use serde::{Serialize, Deserialize};
use std::sync::Arc;
use tokio::sync::{Semaphore, RwLock};
use std::collections::HashMap;
use chrono::Utc;
use uuid::Uuid;
use crate::db::{Database, Job};
use crate::aria2::Aria2Client;
use crate::resolver::Resolver;
use crate::ws::WsHub;
use crate::config::AppConfig;

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
}

pub struct QueueManager {
    pub db: Arc<Database>,
    pub aria2: Arc<Aria2Client>,
    pub resolver: Arc<Resolver>,
    pub ws_hub: Arc<WsHub>,
    pub config: Arc<AppConfig>,
    pub active_jobs: Arc<RwLock<HashMap<String, ActiveJob>>>,
    pub semaphore: Arc<Semaphore>,
}

impl QueueManager {
    pub fn new(
        db: Arc<Database>,
        aria2: Arc<Aria2Client>,
        resolver: Arc<Resolver>,
        ws_hub: Arc<WsHub>,
        config: Arc<AppConfig>,
    ) -> Self {
        let max_concurrent = config.max_concurrent_downloads;
        Self {
            db,
            aria2,
            resolver,
            ws_hub,
            config,
            active_jobs: Arc::new(RwLock::new(HashMap::new())),
            semaphore: Arc::new(Semaphore::new(max_concurrent)),
        }
    }

    pub async fn enqueue(&self, source_url: &str, media_type: &str, quality: &str, language: Option<&str>, tool: Option<&str>) -> Result<String, String> {
        let id = Uuid::new_v4().to_string();
        let effective_tool = match tool {
            Some(t) => t.to_string(),
            None => crate::router::detect_tool_full(source_url, false),
        };

        let job = Job {
            id: id.clone(),
            source_url: source_url.to_string(),
            title: None,
            thumbnail_url: None,
            media_type: media_type.to_string(),
            quality: quality.to_string(),
            language: language.map(|l| l.to_string()),
            tool: effective_tool.clone(),
            status: "queued".to_string(),
            progress: 0.0,
            file_path: None,
            file_size: None,
            error_message: None,
            created_at: Utc::now().timestamp(),
            completed_at: None,
        };

        self.db.insert_job(&job).map_err(|e| e.to_string())?;

        let active = ActiveJob {
            id: id.clone(),
            gid: None,
            source_url: source_url.to_string(),
            media_type: media_type.to_string(),
            quality: quality.to_string(),
            language: language.map(|l| l.to_string()),
            tool: effective_tool.clone(),
            status: "queued".to_string(),
            progress: 0.0,
            speed: 0.0,
        };

        self.active_jobs.write().await.insert(id.clone(), active);
        self.ws_hub.broadcast_status(&id, "queued", 0.0, 0.0, 0.0);

        let qm_id = id.clone();
        let qm_url = source_url.to_string();
        let qm_mt = media_type.to_string();
        let qm_q = quality.to_string();
        let qm_lang = language.map(|l| l.to_string());
        let qm_tool = effective_tool;
        let qm_self = Arc::new(self.clone_inner());

        tokio::spawn(async move {
            let _permit = qm_self.semaphore.acquire().await.unwrap();
            qm_self.process_job(&qm_id, &qm_url, &qm_mt, &qm_q, qm_lang.as_deref(), &qm_tool).await;
        });

        Ok(id)
    }

    async fn process_job(&self, id: &str, url: &str, media_type: &str, quality: &str, language: Option<&str>, tool: &str) {
        self.db.update_job_status(id, "downloading", 0.0, None, None, None).ok();
        self.update_active(id, "downloading", 0.0, 0.0).await;
        self.ws_hub.broadcast_status(id, "downloading", 0.0, 0.0, 0.0);

        let result = match tool {
            "ytdlp" => self.process_ytdlp(id, url, media_type, quality, language).await,
            "aria2" => self.process_aria2_direct(id, url).await,
            _ => Err("Unknown tool".into()),
        };

        match result {
            Ok((gid, file_path)) => {
                if let Some(ref gid) = gid {
                    self.active_jobs.write().await.entry(id.to_string()).and_modify(|j| j.gid = Some(gid.clone()));
                    self.poll_aria2_progress(id, gid, file_path.as_deref()).await;
                } else {
                    let fp = file_path.as_deref();
                    let fsize = fp.and_then(|p| std::fs::metadata(p).ok()).map(|m| m.len() as i64);
                    let title = fp.and_then(|p| std::path::Path::new(p).file_name()).and_then(|n| n.to_str()).map(|s| s.to_string());
                    if let Some(ref t) = title {
                        self.db.conn.lock().unwrap().execute("UPDATE jobs SET title=?1 WHERE id=?2", rusqlite::params![t, id]).ok();
                    }
                    self.db.update_job_status(id, "done", 100.0, fp, fsize, None).ok();
                    self.ws_hub.broadcast_status(id, "done", 100.0, 0.0, 0.0);
                }
            }
            Err(e) => {
                self.db.update_job_status(id, "error", 0.0, None, None, Some(&e)).ok();
                self.ws_hub.broadcast_error(id, &e);
            }
        }

        self.active_jobs.write().await.remove(id);
    }

    async fn process_ytdlp(&self, id: &str, url: &str, media_type: &str, quality: &str, language: Option<&str>) -> Result<(Option<String>, Option<String>), String> {
        self.process_ytdlp_fallback(id, url, media_type, quality, language).await
    }

    async fn process_ytdlp_fallback(&self, id: &str, url: &str, media_type: &str, quality: &str, language: Option<&str>) -> Result<(Option<String>, Option<String>), String> {
        let binary = if self.config.ytdlp_binary_path.is_empty() { "yt-dlp" } else { &self.config.ytdlp_binary_path };
        // Each job gets its own output directory so concurrent downloads
        // (or other jobs) can never be mixed up in the snapshot diff below.
        let out_dir = std::path::PathBuf::from(&self.config.output_dir).join(id);
        std::fs::create_dir_all(&out_dir).ok();

        // Snapshot directory before download
        let before: std::collections::HashSet<String> = std::fs::read_dir(&out_dir)
            .map(|rd| rd.filter_map(|e| e.ok()).map(|e| e.file_name().to_string_lossy().to_string()).collect())
            .unwrap_or_default();

        let mut cmd = tokio::process::Command::new(binary);
        cmd.arg("--js-runtimes").arg("deno")
            .arg("--user-agent").arg("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
            .arg("--no-warnings")
            .arg("--no-playlist")
            .arg("-o").arg(format!("{}/%(title)s.%(ext)s", out_dir.display()));

        // aria2c only accepts HTTP proxies, not SOCKS ones. When going through
        // e.g. Tor (socks5h://...), use yt-dlp's native downloader instead.
        let aria2_usable = self.config.proxy_url.is_empty()
            || self.config.proxy_url.starts_with("http://")
            || self.config.proxy_url.starts_with("https://");
        if aria2_usable {
            let mut aria2_args = String::from("aria2c:-x 16 -s 16 -k 1M");
            if !self.config.proxy_url.is_empty() {
                aria2_args.push_str(&format!(" --all-proxy={}", self.config.proxy_url));
            }
            cmd.arg("--external-downloader").arg("aria2c")
                .arg("--external-downloader-args").arg(aria2_args);
        }

        if !self.config.proxy_url.is_empty() {
            cmd.arg("--proxy").arg(&self.config.proxy_url);
        }

        let cookies_path = std::path::PathBuf::from(&self.config.cookies_path);
        if cookies_path.exists() {
            cmd.arg("--cookies").arg(&cookies_path);
        }

        if media_type == "mp3" {
            cmd.arg("-x").arg("--audio-format").arg("mp3");
            if let Some(q) = quality.strip_suffix("kbps") {
                cmd.arg("--audio-quality").arg(format!("{}K", q));
            } else {
                cmd.arg("--audio-quality").arg("0");
            }
        } else {
            let fmt = if quality == "best" {
                "bv*+ba/best".to_string()
            } else {
                let h = quality.trim_end_matches('p');
                format!("bv*[height<={}]+ba/b[height<={}]/best", h, h)
            };
            cmd.arg("-f").arg(&fmt).arg("--merge-output-format").arg("mp4");
        }

        if let Some(lang) = language {
            cmd.arg("--sub-lang").arg(lang);
        }

        if self.config.ytdlp_force_ipv4 {
            cmd.arg("-4");
        }

        cmd.arg(url);

        let output = match cmd.output().await {
            Ok(o) => o,
            Err(e) => {
                let _ = std::fs::remove_dir_all(&out_dir);
                return Err(format!("yt-dlp download failed: {}", e));
            }
        };

        if !output.status.success() {
            let _ = std::fs::remove_dir_all(&out_dir);
            return Err(format!("yt-dlp error: {}", String::from_utf8_lossy(&output.stderr)));
        }

        // Find new file in the directory
        tokio::time::sleep(tokio::time::Duration::from_millis(500)).await;
        let after: std::collections::HashSet<String> = std::fs::read_dir(&out_dir)
            .map(|rd| rd.filter_map(|e| e.ok()).map(|e| e.file_name().to_string_lossy().to_string()).collect())
            .unwrap_or_default();

        let new_files: Vec<String> = after.difference(&before).cloned().collect();
        let file_path = new_files.first().map(|f| out_dir.join(f).to_string_lossy().to_string());
        match file_path {
            Some(fp) => {
                // Only keep the final media file: remove any leftovers
                // (partial downloads, intermediates, metadata files, ...).
                if let Ok(rd) = std::fs::read_dir(&out_dir) {
                    for e in rd.flatten() {
                        let p = e.path();
                        if p.to_string_lossy() != fp {
                            std::fs::remove_file(&p).ok();
                        }
                    }
                }
                Ok((None, Some(fp)))
            }
            None => {
                let _ = std::fs::remove_dir_all(&out_dir);
                Err("yt-dlp finished but produced no output file".into())
            }
        }
    }

    async fn process_aria2_direct(&self, id: &str, url: &str) -> Result<(Option<String>, Option<String>), String> {
        // Per-job directory so concurrent jobs don't share a folder.
        let out_dir = std::path::PathBuf::from(&self.config.output_dir).join(id);
        std::fs::create_dir_all(&out_dir).ok();
        let out_dir_str = out_dir.to_string_lossy().to_string();
        let gid = self.add_to_aria2(url, &out_dir_str).await?;
        Ok((Some(gid), None))
    }

    async fn add_to_aria2(&self, url: &str, out_dir: &str) -> Result<String, String> {
        let dir = std::fs::canonicalize(out_dir)
            .unwrap_or_else(|_| std::path::PathBuf::from(out_dir));
        let mut options = serde_json::json!({
            "dir": dir.to_string_lossy().to_string(),
            "max-connection-per-server": "1",
            "split": "1",
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
        let mut interval = tokio::time::interval(tokio::time::Duration::from_millis(200));
        loop {
            interval.tick().await;

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
                                        let title = std::path::Path::new(path)
                                            .file_name()
                                            .and_then(|n| n.to_str())
                                            .map(|s| s.to_string());
                                        if let Some(t) = title {
                                            self.db.conn.lock().unwrap().execute("UPDATE jobs SET title=?1 WHERE id=?2", rusqlite::params![t, id]).ok();
                                        }
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
        }
    }
}
