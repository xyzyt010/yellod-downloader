use serde::{Serialize, Deserialize};
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::sync::{Semaphore, RwLock, Mutex};
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use chrono::Utc;
use uuid::Uuid;
use dashmap::{DashMap, DashSet};
use crate::db::{Database, Job};
use crate::aria2::Aria2Client;
use crate::resolver::Resolver;
use crate::ws::WsHub;
use crate::config::AppConfig;
use crate::net::{self, RouteCache};

/// Shared progress state of a running download.
///
/// The bytes on disk are the source of truth: they work with every
/// downloader (yt-dlp's own one, aria2c through the external downloader,
/// fragmented streams) and never jump backwards when a second format
/// starts. yt-dlp's own counters are used as a more precise floor and to
/// learn the expected total size.
#[derive(Default)]
struct ProgressHint {
    /// Expected total size in bytes (0 = unknown).
    total: AtomicU64,
    /// Bytes downloaded so far.
    bytes: AtomicU64,
    /// Last known speed in bytes per second.
    speed: AtomicU64,
    /// yt-dlp's own byte counter.
    dlp_bytes: AtomicU64,
    /// Set once yt-dlp starts post-processing (merging / converting).
    processing: AtomicBool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ActiveJob {
    pub id: String,
    pub gid: Option<String>,
    pub source_url: String,
    pub media_type: String,
    pub quality: String,
    pub language: Option<String>,
    pub tool: String,
    pub status: String,
    pub progress: f64,
    pub speed: f64,
    pub route: String,
}

/// Everything needed to start a job (kept separate so the API layer can
/// pass UI-supplied metadata such as title/thumbnail through).
#[derive(Debug, Clone, Default)]
pub struct JobSpec {
    pub url: String,
    pub media_type: String,
    pub quality: String,
    pub language: Option<String>,
    pub tool: Option<String>,
    /// "auto" (default) | "direct" | "proxy"
    pub connection: Option<String>,
    pub title: Option<String>,
    pub thumbnail: Option<String>,
    /// Embed the cover art/thumbnail into the file itself (mp3 album art,
    /// mp4 cover frame). Off by default so plain downloads stay untouched.
    pub embed_thumbnail: bool,
}

pub struct QueueManager {
    pub db: Arc<Database>,
    pub aria2: Arc<Aria2Client>,
    pub resolver: Arc<Resolver>,
    pub ws_hub: Arc<WsHub>,
    pub config: Arc<AppConfig>,
    pub active_jobs: Arc<RwLock<HashMap<String, ActiveJob>>>,
    pub semaphore: Arc<Semaphore>,
    pub route_cache: Arc<RouteCache>,
    /// Running yt-dlp child processes, so cancel can actually kill them.
    children: Arc<DashMap<String, Arc<Mutex<tokio::process::Child>>>>,
    cancelled: Arc<DashSet<String>>,
    pub js_runtime: Option<&'static str>,
    pub aria2c: bool,
    pub proxy_url: String,
    pub proxy_mode: String,
    /// Tor control access for circuit rotation when an exit is bot-walled.
    tor: net::TorControl,
    /// Downloader binaries found on this machine (ffmpeg/aria2/CLI engines).
    downloaders: crate::clidownload::Downloaders,
}

impl QueueManager {
    pub fn new(
        db: Arc<Database>,
        aria2: Arc<Aria2Client>,
        resolver: Arc<Resolver>,
        ws_hub: Arc<WsHub>,
        config: Arc<AppConfig>,
        route_cache: Arc<RouteCache>,
        tor: net::TorControl,
    ) -> Self {
        let max_concurrent = config.max_concurrent_downloads;
        let proxy_url = config.proxy_url.trim().to_string();
        let proxy_mode = config.proxy_mode.trim().to_lowercase();
        Self {
            db,
            aria2,
            resolver,
            ws_hub,
            config,
            active_jobs: Arc::new(RwLock::new(HashMap::new())),
            semaphore: Arc::new(Semaphore::new(max_concurrent)),
            route_cache,
            children: Arc::new(DashMap::new()),
            cancelled: Arc::new(DashSet::new()),
            js_runtime: net::detect_js_runtime(),
            aria2c: net::have_aria2c(),
            proxy_url,
            proxy_mode: if proxy_mode.is_empty() { "never".into() } else { proxy_mode },
            tor,
            downloaders: crate::clidownload::Downloaders::detect(),
        }
    }

    pub async fn enqueue(&self, spec: &JobSpec) -> Result<String, String> {
        let id = Uuid::new_v4().to_string();
        let effective_tool = match spec.tool.as_deref() {
            Some(t) if !t.is_empty() => t.to_string(),
            _ => crate::router::detect_tool_full(&spec.url, false),
        };

        let title = spec.title.clone().filter(|t| !t.trim().is_empty());
        let thumbnail = spec.thumbnail.clone().filter(|t| !t.trim().is_empty());

        let job = Job {
            id: id.clone(),
            source_url: spec.url.clone(),
            title: title.clone(),
            thumbnail_url: thumbnail.clone(),
            media_type: spec.media_type.clone(),
            quality: spec.quality.clone(),
            language: spec.language.clone(),
            tool: effective_tool.clone(),
            status: "queued".to_string(),
            progress: 0.0,
            file_path: None,
            file_size: None,
            error_message: None,
            created_at: Utc::now().timestamp(),
            completed_at: None,
            route: None,
        };

        self.db.insert_job(&job).map_err(|e| e.to_string())?;

        let active = ActiveJob {
            id: id.clone(),
            gid: None,
            source_url: spec.url.clone(),
            media_type: spec.media_type.clone(),
            quality: spec.quality.clone(),
            language: spec.language.clone(),
            tool: effective_tool.clone(),
            status: "queued".to_string(),
            progress: 0.0,
            speed: 0.0,
            route: "auto".to_string(),
        };

        self.active_jobs.write().await.insert(id.clone(), active);
        self.ws_hub.broadcast_status(&id, "queued", 0.0, 0.0, 0.0);

        let qm_id = id.clone();
        let spec = spec.clone();
        let qm_self = Arc::new(self.clone_inner());
        let tool = effective_tool;

        tokio::spawn(async move {
            let _permit = qm_self.semaphore.acquire().await.unwrap();
            qm_self.process_job(&qm_id, &spec, &tool).await;
        });

        Ok(id)
    }

    async fn process_job(&self, id: &str, spec: &JobSpec, tool: &str) {
        self.cancelled.remove(id);
        self.db.update_job_status(id, "downloading", 0.0, None, None, None).ok();
        self.update_active(id, "downloading", 0.0, 0.0).await;
        self.ws_hub.broadcast_status(id, "downloading", 0.0, 0.0, 0.0);

        let result = match tool {
            "ytdlp" => match self.process_ytdlp(id, spec).await {
                Ok(path) => Ok((None, Some(path))),
                Err(e) => Err(e),
            },
            "aria2" => self.process_aria2_direct(id, &spec.url).await,
            _ => Err("Unknown tool".into()),
        };

        let was_cancelled = self.cancelled.remove(id).is_some();

        match result {
            Ok((gid, file_path)) if !was_cancelled => {
                if let Some(ref gid) = gid {
                    self.active_jobs.write().await.entry(id.to_string()).and_modify(|j| j.gid = Some(gid.clone()));
                    self.poll_aria2_progress(id, gid, file_path.as_deref()).await;
                } else {
                    let fp = file_path.as_deref();
                    let fsize = fp.and_then(|p| std::fs::metadata(p).ok()).map(|m| m.len() as i64);
                    if let Some(name) = fp
                        .and_then(|p| std::path::Path::new(p).file_name())
                        .and_then(|n| n.to_str())
                    {
                        let _ = self.db.set_title_if_empty(id, name);
                    }
                    self.db.update_job_status(id, "done", 100.0, fp, fsize, None).ok();
                    self.ws_hub.broadcast_status(id, "done", 100.0, 0.0, 0.0);
                }
            }
            _ => {
                // Failed or cancelled: never leave partial files behind.
                let job_dir = std::path::PathBuf::from(&self.config.output_dir).join(id);
                let _ = std::fs::remove_dir_all(&job_dir);
                if was_cancelled {
                    self.db.update_job_status(id, "cancelled", 0.0, None, None, None).ok();
                    self.ws_hub.broadcast_status(id, "cancelled", 0.0, 0.0, 0.0);
                } else {
                    let e = match result {
                        Err(e) => e,
                        Ok(_) => "Download cancelled".to_string(),
                    };
                    self.db.update_job_status(id, "error", 0.0, None, None, Some(&e)).ok();
                    self.ws_hub.broadcast_error(id, &e);
                }
            }
        }

        self.active_jobs.write().await.remove(id);
    }

    // ------------------------------------------------------------- yt-dlp
    async fn process_ytdlp(&self, id: &str, spec: &JobSpec) -> Result<String, String> {
        let host = net::host_of(&spec.url);
        let proxy_available = !self.proxy_url.is_empty();
        let override_mode = spec.connection.as_deref();

        let effective_mode = match override_mode {
            Some(o) if !o.is_empty() && o != "auto" => o.to_string(),
            _ => self.proxy_mode.clone(),
        };
        let mut first_proxy =
            self.route_cache.use_proxy(&host, &self.proxy_mode, override_mode, proxy_available);
        // YouTube bot-walls datacenter IPs on direct attempts and flags many
        // Tor exits, so unless direct is *known* to work for this host, start
        // through the proxy instead of burning a doomed first attempt. The
        // auto fallback below still covers both directions.
        if !first_proxy
            && proxy_available
            && effective_mode == "auto"
            && net::is_youtube(&spec.url)
            && self.route_cache.lookup(&host) != Some(true)
        {
            first_proxy = true;
        }
        let mut attempts: Vec<bool> = vec![first_proxy];
        // In auto mode, transparently retry the other way once (direct → Tor
        // when a site blocks this server, Tor → direct when the proxy fails).
        if effective_mode == "auto" && proxy_available {
            attempts.push(!first_proxy);
        }

        // Extra proxied attempts with a fresh Tor circuit. A bot-walled exit
        // IP fails for *every* player client, so a retry only helps if the
        // exit itself changes (Tor ignores NEWNYM more often than ~10s, the
        // cooldown is handled inside TorControl).
        const MAX_CIRCUIT_ROTATIONS: usize = 2;
        let mut rotations = 0usize;

        let mut last_error = String::new();
        let mut last_proxy = attempts.first().copied().unwrap_or(false);
        let mut idx = 0usize;
        while let Some(&use_proxy) = attempts.get(idx) {
            if self.cancelled.contains(id) {
                return Err("cancelled".into());
            }
            // Announce which route is in use so the UI can show it while
            // the download runs (and when auto falls back to the other way).
            let attempt_label = net::route_label(use_proxy, &self.proxy_url);
            self.announce_route(id, attempt_label).await;

            // Direct downloads use aria2c (multiple connections); if that
            // trips over a rate limit, the same route is retried once with
            // yt-dlp's own downloader before we consider the route broken.
            let use_aria2c = self.aria2c && !use_proxy;
            let first = self.run_ytdlp_attempt(id, spec, use_proxy, use_aria2c).await;
            let result = match first {
                Err(e) if use_aria2c && net::looks_like_rate_limit(&e) => {
                    tracing::info!("job {}: aria2c failed ({}), retrying with yt-dlp's downloader", id, e);
                    self.run_ytdlp_attempt(id, spec, use_proxy, false).await
                }
                other => other,
            };

            match result {
                Ok(path) => {
                    if self.cancelled.contains(id) {
                        return Err("cancelled".into());
                    }
                    let route = net::route_label(use_proxy, &self.proxy_url);
                    self.route_cache.mark(&host, !use_proxy);
                    let _ = self.db.set_job_route(id, route);
                    self.active_jobs.write().await.entry(id.to_string()).and_modify(|j| j.route = route.to_string());
                    return Ok(path);
                }
                Err(e) => {
                    if self.cancelled.contains(id) {
                        return Err("cancelled".into());
                    }
                    // Remember hosts that need the proxy (and forget hosts
                    // where direct worked), so the next job starts fast.
                    if !use_proxy && net::looks_like_route_error(&e) {
                        self.route_cache.mark(&host, false);
                    }
                    // Route failure *through* the proxy: this exit IP is
                    // likely flagged - swap the circuit and queue one more
                    // proxied attempt (bounded, so a dead proxy can't loop).
                    if use_proxy
                        && self.tor.enabled()
                        && net::looks_like_route_error(&e)
                        && rotations < MAX_CIRCUIT_ROTATIONS
                    {
                        rotations += 1;
                        tracing::info!(
                            "job {}: route failure via proxy, requesting a new Tor circuit",
                            id
                        );
                        if let Err(ce) = self.tor.newnym().await {
                            tracing::warn!("job {}: tor circuit rotation failed: {}", id, ce);
                        }
                        // Retry the proxy immediately (fresh circuit); the
                        // direct attempt stays as the last resort.
                        attempts.insert(idx + 1, true);
                    }
                    last_error = e;
                    last_proxy = use_proxy;
                    if idx + 1 < attempts.len() {
                        let next = if attempts[idx + 1] { "the proxy" } else { "a direct connection" };
                        tracing::info!("job {}: attempt failed, retrying via {}", id, next);
                    }
                }
            }
            idx += 1;
        }

        // ---- standalone CLI fallback (ffmpeg/aria2 engines, N_m3u8DL-RE,
        // lux, you-get) ----------------------------------------------------
        // yt-dlp failed on every route/attempt. Give the standalone engines
        // a chance before declaring the job dead: N_m3u8DL-RE handles
        // HLS/DASH manifests that yt-dlp's downloader chokes on and works
        // through Tor; lux/you-get are complete alternative engines.
        if !self.config.cli_downloaders.trim().is_empty() {
            let out_dir = std::path::PathBuf::from(&self.config.output_dir).join(id);
            let route = if last_proxy {
                crate::clidownload::Route::Proxy
            } else {
                crate::clidownload::Route::Direct
            };
            tracing::info!(
                "job {}: yt-dlp failed on every route, trying CLI downloaders (route {:?})",
                id,
                route
            );
            match self
                .downloaders
                .run_fallbacks(&self.config.cli_downloaders, &spec.url, &out_dir, route, &self.proxy_url)
                .await
            {
                Ok(path) => {
                    let _ = self.db.set_job_route(id, net::route_label(route == crate::clidownload::Route::Proxy && !self.proxy_url.is_empty(), &self.proxy_url));
                    self.db.update_job_status(id, "done", 100.0, Some(&path), None, None).ok();
                    return Ok(path);
                }
                Err(e) => {
                    tracing::info!("job {}: CLI downloaders also failed: {}", id, e);
                    // Keep yt-dlp's (more specific) message as the user error.
                }
            }
        }

        Err(last_error)
    }

    /// Record + push the route ("direct" | "tor" | "proxy") that a job is
    /// using, so the UI shows it while the download is running and it
    /// survives a page reload before the job has finished.
    async fn announce_route(&self, id: &str, label: &str) {
        let _ = self.db.set_job_route(id, label);
        self.active_jobs
            .write()
            .await
            .entry(id.to_string())
            .and_modify(|j| j.route = label.to_string());
        self.ws_hub.broadcast_route(id, label);
    }

    /// One yt-dlp run. `use_proxy` decides direct vs configured proxy.
    async fn run_ytdlp_attempt(
        &self,
        id: &str,
        spec: &JobSpec,
        use_proxy: bool,
        // Kept for the caller's rate-limit retry logic (aria2c splits, so it
        // trips rate limits that a native downloader does not).
        _use_aria2c: bool,
    ) -> Result<String, String> {
        let binary = if self.config.ytdlp_binary_path.is_empty() { "yt-dlp" } else { &self.config.ytdlp_binary_path };
        let out_dir = std::path::PathBuf::from(&self.config.output_dir).join(id);
        std::fs::create_dir_all(&out_dir).ok();

        let before: std::collections::HashSet<String> = std::fs::read_dir(&out_dir)
            .map(|rd| rd.filter_map(|e| e.ok()).map(|e| e.file_name().to_string_lossy().to_string()).collect())
            .unwrap_or_default();

        let mut cmd = tokio::process::Command::new(binary);
        cmd.arg("--no-warnings")
            .arg("--no-playlist")
            .arg("--newline")
            .arg("--no-simulate")
            // Expected size of the selected format(s), printed before the
            // transfer starts. This is what makes a real percentage possible
            // even when the downloader itself reports no progress.
            .arg("--print")
            // One comma-separated template: several --print fields arrive
            // joined on a single line (see the parser below). Cover art of
            // the media itself is saved on the job when it was queued
            // without metadata (covered pastes, playlists, API calls),
            // so history shows the photo.
            .arg("before_dl:DLSIZE|%(filesize)s|%(filesize_approx)s,before_dl:THUMB|%(thumbnail)s")
            .arg("--progress-template")
            .arg("download:DLP|%(progress.downloaded_bytes)s|%(progress.total_bytes)s|%(progress.total_bytes_estimate)s|%(progress.speed)s|%(progress.eta)s")
            .arg("--socket-timeout").arg("20")
            .arg("--retries").arg("3")
            .arg("--fragment-retries").arg("3")
            .arg("--user-agent").arg("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
            .arg("-o").arg(format!("{}/%(title)s.%(ext)s", out_dir.display()));

        if let Some(rt) = self.js_runtime {
            cmd.arg("--js-runtimes").arg(rt);
        }
        if self.config.ytdlp_force_ipv4 && !use_proxy {
            cmd.arg("-4");
        }

        // Per-route downloader strategy: aria2c for direct http (multi
        // connection, resume), ffmpeg for HLS/DASH manifests, and yt-dlp's
        // own downloader through the proxy because aria2c has no SOCKS.
        let route = if use_proxy { crate::clidownload::Route::Proxy } else { crate::clidownload::Route::Direct };
        for a in self.downloaders.ytdlp_args(route, &self.proxy_url) {
            cmd.arg(a);
        }
        if use_proxy {
            cmd.arg("--proxy").arg(&self.proxy_url);
            cmd.arg("--concurrent-fragments").arg("4");
        } else {
            cmd.arg("--concurrent-fragments").arg("8");
        }

        let cookies_path = std::path::PathBuf::from(&self.config.cookies_path);
        if cookies_path.exists() {
            cmd.arg("--cookies").arg(&cookies_path);
        }

        // YouTube: pick player clients that survive the "confirm you're not
        // a bot" wall (the default web clients fail from nearly any server
        // IP, direct or via Tor). See net::apply_yt_clients.
        net::apply_yt_clients(&mut cmd, &spec.url, &self.config.yt_clients);

        if spec.media_type == "mp3" {
            // Only fetch the audio stream - never download video just to throw it away.
            cmd.arg("-f").arg("ba/b");
            cmd.arg("-x").arg("--audio-format").arg("mp3");
            if let Some(q) = spec.quality.strip_suffix("kbps") {
                // Accept only "<digits>kbps"; anything else ("360p", a
                // reused job's odd value, an API caller's garbage) is
                // treated as "best" so yt-dlp never gets an unparsable one.
                if !q.is_empty() && q.chars().all(|c| c.is_ascii_digit()) {
                    cmd.arg("--audio-quality").arg(format!("{}K", q));
                } else {
                    cmd.arg("--audio-quality").arg("0");
                }
            } else {
                cmd.arg("--audio-quality").arg("0");
            }
        } else {
            let fmt = if spec.quality == "best" {
                // H.264 first: it plays everywhere (built-in player, TVs,
                // phones, editors); AV1/VP9 only as a fallback.
                "bv*[ext=mp4][vcodec^=avc1]+ba[ext=m4a]/bv*[ext=mp4]+ba[ext=m4a]/bv*+ba/b".to_string()
            } else {
                // Only a plain "<digits>p" becomes a height selector; any
                // other value (odd quality strings from reused jobs or an
                // API caller) falls back to 1080p instead of producing an
                // unparsable yt-dlp filter.
                let h = spec
                    .quality
                    .strip_suffix('p')
                    .filter(|h| !h.is_empty() && h.chars().all(|c| c.is_ascii_digit()))
                    .unwrap_or("1080")
                    .to_string();
                format!(
                    "bv*[height<={h}][ext=mp4][vcodec^=avc1]+ba[ext=m4a]\
                     /bv*[height<={h}][ext=mp4]+ba[ext=m4a]\
                     /bv*[height<={h}]+ba/b[height<={h}]/bv*+ba/b",
                    h = h
                )
            };
            cmd.arg("-f").arg(&fmt).arg("--merge-output-format").arg("mp4");
        }

        // Cover art: when the user ticks "include cover photo", let yt-dlp
        // fetch the thumbnail and embed it (mp3 album art / mp4 cover frame).
        // ffmpeg handles the muxing, which is already required for mp3 anyway.
        if spec.embed_thumbnail {
            // yt-dlp downloads the thumbnail itself for embedding; no separate
            // file is written to disk, so the "final file" picker is unaffected.
            cmd.arg("--embed-thumbnail");
            // Normalise webp/png thumbnails to jpg so every muxer accepts them.
            cmd.arg("--convert-thumbnails").arg("jpg");
        }

        cmd.arg(&spec.url);
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());

        let mut child = cmd.spawn().map_err(|e| {
            let _ = std::fs::remove_dir_all(&out_dir);
            format!("yt-dlp failed to start: {}", e)
        })?;

        let stdout = child.stdout.take();
        let stderr = child.stderr.take();
        let child = Arc::new(Mutex::new(child));
        self.children.insert(id.to_string(), child.clone());

        let stderr_handle = stderr.map(|se| {
            tokio::spawn(async move {
                let mut reader = BufReader::new(se);
                let mut buf = String::new();
                use tokio::io::AsyncReadExt;
                let _ = reader.read_to_string(&mut buf).await;
                buf
            })
        });

        // ---- progress tracking -------------------------------------------
        // The bytes on disk are the source of truth: they work with every
        // downloader and never jump backwards when a second format starts.
        let hint = Arc::new(ProgressHint::default());
        // For direct/generic URLs yt-dlp prints no expected size (DLSIZE is
        // NA) and aria2c reports none either, so the percentage would have no
        // denominator and sit at 0% until the file is done. Ask the server for
        // the Content-Length up front - that is the same number aria2c aims
        // for, so the bar tracks the transfer accurately.
        {
            let url = spec.url.clone();
            let hint = hint.clone();
            tokio::spawn(async move {
                let len = head_content_length(&url).await;
                if let Some(len) = len {
                    hint.total.store(len, Ordering::Relaxed);
                }
            });
        }
        let stop_watch = Arc::new(AtomicBool::new(false));
        let watcher = {
            let stop = stop_watch.clone();
            let hint = hint.clone();
            let job_id = id.to_string();
            let dir = out_dir.clone();
            let db = self.db.clone();
            let ws = self.ws_hub.clone();
            let active = self.active_jobs.clone();
            tokio::spawn(async move {
                let mut last_bytes = 0u64;
                let mut last_ts = std::time::Instant::now();
                let mut last_push = std::time::Instant::now() - std::time::Duration::from_secs(2);
                let mut last_db = std::time::Instant::now() - std::time::Duration::from_secs(5);
                let mut last_db_pct = -5.0f64;
                while !stop.load(Ordering::Relaxed) {
                    tokio::time::sleep(std::time::Duration::from_millis(500)).await;
                    if stop.load(Ordering::Relaxed) {
                        break;
                    }
                    let disk = dir_bytes(&dir);
                    let now = std::time::Instant::now();
                    let dt = now.duration_since(last_ts).as_secs_f64().max(0.01);
                    let speed = disk.saturating_sub(last_bytes) as f64 / dt;
                    last_bytes = disk;
                    last_ts = now;
                    if speed > 0.0 {
                        hint.speed.store(speed as u64, Ordering::Relaxed);
                    }
                    // yt-dlp's own counter is more precise while it streams.
                    let done = disk.max(hint.dlp_bytes.load(Ordering::Relaxed));
                    if done == 0 {
                        continue;
                    }
                    hint.bytes.store(done, Ordering::Relaxed);
                    let total = hint.total.load(Ordering::Relaxed);
                    let pct = if total > 0 {
                        (done as f64 / total as f64 * 100.0).clamp(0.0, 99.5)
                    } else {
                        0.0
                    };
                    let speed = hint.speed.load(Ordering::Relaxed) as f64;
                    let eta = if total > 0 && speed > 0.0 {
                        ((total.saturating_sub(done)) as f64 / speed).max(0.0)
                    } else {
                        0.0
                    };
                    let status = if hint.processing.load(Ordering::Relaxed) { "processing" } else { "downloading" };
                    if now.duration_since(last_push).as_millis() >= 300 {
                        last_push = now;
                        active.write().await.entry(job_id.clone()).and_modify(|j| {
                            j.status = status.to_string();
                            j.progress = pct;
                            j.speed = speed;
                        });
                        ws.broadcast_progress(&job_id, status, pct, speed, eta, done);
                    }
                    if now.duration_since(last_db).as_secs() >= 2 || (pct - last_db_pct).abs() >= 5.0 {
                        db.update_job_status(&job_id, status, pct, None, None, None).ok();
                        last_db = now;
                        last_db_pct = pct;
                    }
                }
            })
        };

        if let Some(out) = stdout {
            let mut lines = BufReader::new(out).lines();
            let mut announced_processing = false;
            while let Ok(Some(line)) = lines.next_line().await {
                if let Some(rest) = line.strip_prefix("DLP|") {
                    if let Some((downloaded, total, speed, eta)) = parse_progress(rest) {
                        hint.dlp_bytes.store(downloaded as u64, Ordering::Relaxed);
                        if total > 0.0 && hint.total.load(Ordering::Relaxed) == 0 {
                            // Only use yt-dlp's total when the expected size
                            // was not printed before the transfer.
                            hint.total.store(total as u64, Ordering::Relaxed);
                        }
                        if speed > 0.0 {
                            hint.speed.store(speed as u64, Ordering::Relaxed);
                        }
                        let _ = eta;
                    }
                } else if line.contains("before_dl:") || line.starts_with("DLSIZE|") {
                    // yt-dlp prints the combined template on one line, with
                    // the later fields prefixed by "before_dl:" - split on
                    // that marker (a bare comma would be wrong: URLs may
                    // contain commas).
                    for seg in line.split("before_dl:") {
                        let seg = seg.trim_start_matches(',').trim();
                        if let Some(rest) = seg.strip_prefix("DLSIZE|") {
                            // Expected size of the selected format(s) - the
                            // total for the percentage (summed when several
                            // formats are downloaded one after the other).
                            let size = parse_expected_size(rest);
                            if size > 0 {
                                hint.total.fetch_add(size, Ordering::Relaxed);
                            }
                        } else if let Some(rest) = seg.strip_prefix("THUMB|") {
                            // Cover photo - stored on the job when it was
                            // queued without one (multi-link pastes,
                            // playlists, API calls) so history shows it.
                            if rest.starts_with("http") {
                                let _ = self.db.set_thumbnail_if_empty(id, rest.trim());
                            }
                        }
                    }
                } else if is_postprocessing_line(&line) {
                    // Merging / converting: the transfer is done, ffmpeg is
                    // working - tell the UI instead of freezing the bar.
                    if !announced_processing {
                        announced_processing = true;
                        hint.processing.store(true, Ordering::Relaxed);
                        let pct = hint.bytes.load(Ordering::Relaxed) as f64;
                        let total = hint.total.load(Ordering::Relaxed);
                        let _ = total;
                        self.update_active(id, "processing", pct, 0.0).await;
                        self.ws_hub.broadcast_status(id, "processing", pct, 0.0, 0.0);
                    }
                }
            }
        }

        let status = {
            let mut guard = child.lock().await;
            guard.wait().await
        };
        stop_watch.store(true, Ordering::Relaxed);
        let _ = watcher.await;
        self.children.remove(id);

        let stderr_text = match stderr_handle {
            Some(handle) => handle.await.unwrap_or_default(),
            None => String::new(),
        };

        if self.cancelled.contains(id) {
            let _ = std::fs::remove_dir_all(&out_dir);
            return Err("cancelled".into());
        }

        let status = status.map_err(|e| {
            let _ = std::fs::remove_dir_all(&out_dir);
            format!("yt-dlp process error: {}", e)
        })?;

        if !status.success() {
            let _ = std::fs::remove_dir_all(&out_dir);
            return Err(cli_failure_message(&stderr_text, status));
        }

        // Keep only the final media file (no partials, no sidecars).
        let after: std::collections::HashSet<String> = std::fs::read_dir(&out_dir)
            .map(|rd| rd.filter_map(|e| e.ok()).map(|e| e.file_name().to_string_lossy().to_string()).collect())
            .unwrap_or_default();
        let mut new_files: Vec<String> = after.difference(&before).cloned().collect();
        // Skip partials, downloader sidecars and any stray thumbnails/metadata
        // so only the real media file is ever treated as the result.
        new_files.retain(|f| {
            let lower = f.to_lowercase();
            !lower.ends_with(".part")
                && !lower.ends_with(".ytdl")
                && !lower.ends_with(".aria2")
                && !lower.ends_with(".jpg")
                && !lower.ends_with(".jpeg")
                && !lower.ends_with(".png")
                && !lower.ends_with(".webp")
                && !lower.ends_with(".json")
                && !lower.ends_with(".description")
        });
        new_files.sort();
        let file_path = new_files.first().map(|f| out_dir.join(f).to_string_lossy().to_string());

        match file_path {
            Some(fp) => {
                if let Ok(rd) = std::fs::read_dir(&out_dir) {
                    for e in rd.flatten() {
                        let p = e.path();
                        if p.to_string_lossy() != fp {
                            std::fs::remove_file(&p).ok();
                        }
                    }
                }
                Ok(fp)
            }
            None => {
                let _ = std::fs::remove_dir_all(&out_dir);
                Err("yt-dlp finished but produced no output file".into())
            }
        }
    }

    // ------------------------------------------------------------- aria2
    async fn process_aria2_direct(&self, id: &str, url: &str) -> Result<(Option<String>, Option<String>), String> {
        // Per-job directory so concurrent jobs don't share a folder.
        let out_dir = std::path::PathBuf::from(&self.config.output_dir).join(id);
        std::fs::create_dir_all(&out_dir).ok();
        let out_dir_str = out_dir.to_string_lossy().to_string();
        // aria2 always goes direct (or through an HTTP proxy when one is
        // configured, it cannot do SOCKS) — announce that from the start.
        self.announce_route(id, self.aria2_route_label()).await;
        let gid = self.add_to_aria2(url, &out_dir_str).await?;
        Ok((Some(gid), None))
    }

    /// aria2c only understands HTTP(S) proxies; with our default Tor SOCKS
    /// proxy it runs direct.
    fn aria2_route_label(&self) -> &'static str {
        if self.config.proxy_url.starts_with("http://") || self.config.proxy_url.starts_with("https://") {
            "proxy"
        } else {
            "direct"
        }
    }

    async fn add_to_aria2(&self, url: &str, out_dir: &str) -> Result<String, String> {
        let dir = std::fs::canonicalize(out_dir)
            .unwrap_or_else(|_| std::path::PathBuf::from(out_dir));
        let mut options = serde_json::json!({
            "dir": dir.to_string_lossy().to_string(),
            "max-connection-per-server": "16",
            "split": "16",
            "min-split-size": "1M",
            "allow-overwrite": "true",
            "auto-file-renaming": "false",
            "user-agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        });
        // aria2c only supports HTTP proxies; never pass a SOCKS proxy URL to it.
        if self.config.proxy_url.starts_with("http://") || self.config.proxy_url.starts_with("https://") {
            if let Some(obj) = options.as_object_mut() {
                obj.insert("all-proxy".to_string(), serde_json::json!(self.config.proxy_url.clone()));
            }
        }

        self.aria2.add_uri(vec![url.to_string()], options).await
    }

    async fn poll_aria2_progress(&self, id: &str, gid: &str, _file_path: Option<&str>) {
        let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(300));
        loop {
            interval.tick().await;

            if self.cancelled.contains(id) {
                let _ = self.aria2.remove(gid).await;
                self.cancelled.remove(id);
                let job_dir = std::path::PathBuf::from(&self.config.output_dir).join(id);
                let _ = std::fs::remove_dir_all(&job_dir);
                self.db.update_job_status(id, "cancelled", 0.0, None, None, None).ok();
                self.ws_hub.broadcast_status(id, "cancelled", 0.0, 0.0, 0.0);
                break;
            }

            match self.aria2.tell_status(gid).await {
                Ok(status) => {
                    let progress = status.progress();
                    let speed = status.download_speed as f64;

                    self.update_active(id, &status.status, progress * 100.0, speed).await;
                    self.db.update_job_status(id, &status.status, progress * 100.0, None, Some(status.total_length as i64), None).ok();
                    self.ws_hub.broadcast_status(id, &status.status, progress * 100.0, speed, 0.0);

                    if status.status == "complete" || status.status == "error" || status.status == "removed" {
                        if status.status == "complete" {
                            let files_result = self.aria2.call("aria2.tellStatus", vec![
                                serde_json::json!(gid),
                                serde_json::json!(["status", "totalLength", "completedLength", "downloadSpeed", "files"]),
                            ]).await;

                            if let Ok(files_val) = files_result {
                                if let Some(files) = files_val.get("files").and_then(|v| v.as_array()) {
                                    if let Some(first_file) = files.first() {
                                        let path = first_file.get("path").and_then(|v| v.as_str()).unwrap_or("");
                                        let fsize = files_val.get("completedLength").and_then(|v| v.as_str()).and_then(|s| s.parse::<i64>().ok()).unwrap_or(0);
                                        if let Some(name) = std::path::Path::new(path).file_name().and_then(|n| n.to_str()) {
                                            let _ = self.db.set_title_if_empty(id, name);
                                        }
                                        let _ = self.db.set_job_route(id, self.aria2_route_label());
                                        self.db.update_job_status(id, "done", 100.0, Some(path), Some(fsize), None).ok();
                                    }
                                }
                            }
                            self.ws_hub.broadcast_status(id, "done", 100.0, 0.0, 0.0);
                        } else {
                            // Failed or cancelled: remove any partial files for this job.
                            let job_dir = std::path::PathBuf::from(&self.config.output_dir).join(id);
                            let _ = std::fs::remove_dir_all(&job_dir);
                            let message = status.error_message.clone().unwrap_or_else(|| {
                                status.error_code
                                    .as_ref()
                                    .map(|code| format!("aria2 {} (code {})", status.status, code))
                                    .unwrap_or_else(|| format!("aria2 {}", status.status))
                            });
                            self.db.update_job_status(id, "error", progress * 100.0, None, Some(status.total_length as i64), Some(&message)).ok();
                            self.ws_hub.broadcast_error(id, &message);
                        }
                        break;
                    }
                }
                Err(_) => {
                    break;
                }
            }
        }
    }

    async fn update_active(&self, id: &str, status: &str, progress: f64, speed: f64) {
        self.active_jobs.write().await.entry(id.to_string()).and_modify(|j| {
            j.status = status.to_string();
            j.progress = progress;
            j.speed = speed;
        });
    }

    pub async fn cancel_job(&self, id: &str) -> Result<(), String> {
        self.cancelled.insert(id.to_string());

        // Kill a running yt-dlp process (if any) so a cancel is instant.
        if let Some((_, child)) = self.children.remove(id) {
            let mut child = child.lock().await;
            let _ = child.start_kill();
        }

        if let Some(active) = self.active_jobs.read().await.get(id) {
            if let Some(gid) = &active.gid {
                let _ = self.aria2.remove(gid).await;
            }
        }

        self.db.update_job_status(id, "cancelled", 0.0, None, None, None).map_err(|e| e.to_string())?;
        self.active_jobs.write().await.remove(id);
        self.ws_hub.broadcast_status(id, "cancelled", 0.0, 0.0, 0.0);
        Ok(())
    }

    fn clone_inner(&self) -> QueueManager {
        QueueManager {
            db: self.db.clone(),
            aria2: self.aria2.clone(),
            resolver: self.resolver.clone(),
            ws_hub: self.ws_hub.clone(),
            config: self.config.clone(),
            active_jobs: self.active_jobs.clone(),
            semaphore: self.semaphore.clone(),
            route_cache: self.route_cache.clone(),
            children: self.children.clone(),
            cancelled: self.cancelled.clone(),
            js_runtime: self.js_runtime,
            aria2c: self.aria2c,
            proxy_url: self.proxy_url.clone(),
            proxy_mode: self.proxy_mode.clone(),
            tor: self.tor.clone(),
            downloaders: crate::clidownload::Downloaders::detect(),
        }
    }
}

/// Parse one `DLP|downloaded|total|estimate|speed|eta` progress line.
/// Returns (downloaded_bytes, total_bytes, speed_bytes_per_sec, eta_seconds).
fn parse_progress(rest: &str) -> Option<(f64, f64, f64, f64)> {
    let mut parts = rest.split('|');
    let num = |v: Option<&str>| -> f64 {
        v.and_then(|x| x.trim().parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v >= 0.0)
            .unwrap_or(0.0)
    };
    let downloaded = num(parts.next());
    let total = num(parts.next()).max(num(parts.next()));
    let speed = num(parts.next());
    let eta = num(parts.next());

    if downloaded <= 0.0 && total <= 0.0 {
        return None;
    }
    Some((downloaded, total, speed, eta))
}

/// Parse the `DLSIZE|filesize|filesize_approx` line that yt-dlp prints
/// before the transfer starts. Returns the expected size in bytes (0 when
/// the size is unknown).
fn parse_expected_size(rest: &str) -> u64 {
    let mut parts = rest.split('|');
    let parse = |v: Option<&str>| -> u64 {
        v.and_then(|x| x.trim().parse::<f64>().ok())
            .filter(|v| v.is_finite() && *v > 0.0)
            .unwrap_or(0.0) as u64
    };
    let exact = parse(parts.next());
    let approx = parse(parts.next());
    if exact > 0 {
        exact
    } else {
        approx
    }
}

/// HEAD the source URL and read its Content-Length, so a direct download has
/// a denominator for the percentage even when yt-dlp cannot report a size.
async fn head_content_length(url: &str) -> Option<u64> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(8))
        .build()
        .ok()?;
    let resp = client
        .head(url)
        .header(
            reqwest::header::USER_AGENT,
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        )
        .send()
        .await
        .ok()?;
    // Note: `resp.content_length()` is not usable here. For a HEAD response
    // reqwest reports the body length, which is zero by definition, so it
    // yields Some(0) and hides the size we actually want. The size of the
    // resource lives in the Content-Length header, so read that directly.
    resp.headers()
        .get(reqwest::header::CONTENT_LENGTH)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|len| *len > 0)
}

/// True for yt-dlp's post-processor lines (merging, converting, fixing up).
fn is_postprocessing_line(line: &str) -> bool {
    const MARKERS: [&str; 6] = [
        "[Merger]",
        "[ExtractAudio]",
        "[EmbedSubtitle]",
        "[EmbedThumbnail]",
        "[Metadata]",
        "[FixupM3u8]",
    ];
    MARKERS.iter().any(|m| line.starts_with(m))
}

/// Turn a failed yt-dlp run into one message a user can actually read.
///
/// yt-dlp prints its reason on an "ERROR: ..." line, but Python often prints
/// a long traceback after it; the old code showed the traceback's tail, which
/// looked like the error itself. Prefer the "ERROR:" line, then the last
/// meaningful non-traceback line, and only then the exit status.
pub(crate) fn cli_failure_message(
    stderr_text: &str,
    status: std::process::ExitStatus,
) -> String {
    let lines: Vec<&str> = stderr_text
        .lines()
        .map(str::trim_end)
        .filter(|l| !l.trim().is_empty())
        .collect();

    let pick = |needle: Option<&str>| -> Option<String> {
        lines
            .iter()
            .rev()
            .find(|l| match needle {
                Some(n) => l.contains(n),
                None => {
                    // Skip traceback / progress noise - what is left is the
                    // human-readable reason.
                    let t = l.trim_start();
                    !t.starts_with("File \"")
                        && !t.starts_with("Traceback")
                        && !t.starts_with("^")
                        && !t.starts_with("[download]")
                        && !t.starts_with("During handling")
                        && !t.starts_with("The above exception")
                }
            })
            .map(|l| {
                // yt-dlp prefixes its own messages: "ERROR: [youtube] ID: ...".
                let t = l.trim_start();
                let msg = t.strip_prefix("ERROR:").unwrap_or(t).trim();
                // Drop the bracketed extractor name and the id that follows.
                let rest = msg.trim_start_matches(|c| c != ']').trim_start_matches(']');
                let mut msg = rest.trim().to_string();
                // ... then the "ID: " part is often noise too ("jNQXAC9IVRw: ").
                if let Some(colon) = msg.find(": ") {
                    let head = &msg[..colon];
                    let wordish = !head.contains(' ') && head.len() <= 20;
                    if wordish {
                        msg = msg[colon + 2..].trim().to_string();
                    }
                }
                msg
            })
    };

    let message = pick(Some("ERROR:"))
        .or_else(|| pick(None))
        .unwrap_or_else(|| format!("yt-dlp exited with {}", status));

    // One or two sentences, never a wall of text; the full log stays on the
    // server (journalctl).
    let mut message = message.replace('\n', " · ");
    if message.chars().count() > 400 {
        message = message.chars().take(400).collect();
    }
    if message.is_empty() {
        format!("yt-dlp exited with {}", status)
    } else {
        message
    }
}

/// Bytes really written in the job folder - that is how much has been
/// downloaded so far (works with every downloader, also while a second
/// format or the merge step is running).
///
/// aria2c pre-allocates its target sparsely, so the file's apparent size is
/// the full expected length from the first moment. Reporting that would show
/// 100% instantly and then stand still, which is exactly the frozen bar we are
/// fixing. Allocated blocks grow only as data actually lands on disk, so they
/// track the transfer on every route.
fn dir_bytes(dir: &std::path::Path) -> u64 {
    let mut total = 0u64;
    if let Ok(rd) = std::fs::read_dir(dir) {
        for e in rd.flatten() {
            let name = e.file_name().to_string_lossy().to_string();
            // Partials and downloader sidecars are not finished media.
            if name.ends_with(".ytdl") || name.ends_with(".aria2") {
                continue;
            }
            if let Ok(md) = e.metadata() {
                if md.is_file() {
                    total += file_bytes(&md);
                }
            }
        }
    }
    total
}

/// Bytes a file really occupies. Falls back to the apparent size when the
/// platform cannot report allocated blocks.
fn file_bytes(md: &std::fs::Metadata) -> u64 {
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if md.blocks() > 0 {
            return md.blocks() * 512;
        }
    }
    md.len()
}
