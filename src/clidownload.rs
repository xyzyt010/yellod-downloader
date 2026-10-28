//! Downloader strategy + a fallback chain of standalone CLI downloaders.
//!
//! yt-dlp drives almost every job, but it cannot use its own multi-connection
//! engine (aria2c) through a SOCKS5/Tor proxy, and some streams fail with its
//! native downloader no matter what. This module adds the two pieces that make
//! downloads resilient:
//!
//! 1. **Per-protocol `--downloader` selection** for the yt-dlp command line
//!    (aria2c for plain http/ftp when direct, ffmpeg for HLS/DASH, native for
//!    proxied routes). See [`Downloaders::ytdlp_args`].
//!
//! 2. **CLI fallback chain** ([`Downloaders::run_fallbacks`]): when every
//!    yt-dlp attempt has failed, these standalone downloaders are tried in
//!    order. Each is probed at startup and skipped when not installed:
//!      - `N_m3u8DL-RE` — HLS/DASH specialist with real SOCKS5 (Tor) support
//!      - `lux`         — fast standalone engine (direct only)
//!      - `you-get`     — Python-based engine (http proxy only)

use std::collections::HashSet;
use std::path::Path;
use tokio::process::Command;

/// Which network route a job is using right now.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Route {
    /// Direct connection.
    Direct,
    /// Through the configured proxy (e.g. Tor).
    Proxy,
}

/// Downloader binaries found on this machine.
pub struct Downloaders {
    pub aria2c: bool,
    pub ffmpeg: bool,
    pub curl: bool,
    pub wget: bool,
    /// Standalone engine binaries (paths when installed).
    pub n_m3u8: Option<String>,
    pub lux: Option<String>,
    pub you_get: Option<String>,
}

impl Downloaders {
    /// Probe the PATH for every supported downloader.
    pub fn detect() -> Self {
        let bin = |name: &str| -> Option<String> {
            // `which` is not always installed; fall back to a PATH walk.
            if let Ok(out) = std::process::Command::new("which").arg(name).output() {
                if out.status.success() {
                    if let Some(p) = String::from_utf8_lossy(&out.stdout).lines().next() {
                        let p = p.trim();
                        if !p.is_empty() && Path::new(p).exists() {
                            return Some(p.to_string());
                        }
                    }
                }
            }
            std::env::var_os("PATH")
                .map(|paths| {
                    std::env::split_paths(&paths)
                        .map(|dir| dir.join(name))
                        .find(|candidate| candidate.is_file())
                        .map(|candidate| candidate.to_string_lossy().to_string())
                })
                .flatten()
        };
        Self {
            aria2c: bin("aria2c").is_some(),
            ffmpeg: bin("ffmpeg").is_some(),
            curl: bin("curl").is_some(),
            wget: bin("wget").is_some(),
            n_m3u8: bin("N_m3u8DL-RE"),
            lux: bin("lux"),
            you_get: bin("you-get"),
        }
    }

    /// A short one-line summary for the startup log.
    pub fn summary(&self) -> String {
        let present: Vec<&str> = [
            ("ffmpeg", self.ffmpeg),
            ("aria2", self.aria2c),
            ("curl", self.curl),
            ("wget", self.wget),
        ]
        .iter()
        .filter(|(_, on)| *on)
        .map(|(n, _)| *n)
        .collect();
        let cli: Vec<&str> = [
            ("N_m3u8DL-RE", self.n_m3u8.is_some()),
            ("lux", self.lux.is_some()),
            ("you-get", self.you_get.is_some()),
        ]
        .iter()
        .filter(|(_, on)| *on)
        .map(|(n, _)| *n)
        .collect();
        format!(
            "yt-dlp-deep={}, cli=[{}]",
            present.join("+"),
            cli.join(", ")
        )
    }

    /// `--downloader` / `--downloader-args` flags for a yt-dlp invocation.
    ///
    /// - `Route::Direct`: aria2c takes over plain http/ftp transfers (multiple
    ///   connections, resume); HLS/DASH stays with yt-dlp's native fragment
    ///   downloader, and ffmpeg is configured as a *protocol* fallback in case
    ///   the native path chokes on a manifest.
    /// - `Route::Proxy`: aria2c cannot speak SOCKS5, so it must never be used
    ///   through Tor. ffmpeg can follow an http proxy, and yt-dlp passes its
    ///   proxy setting down to it.
    pub fn ytdlp_args(&self, route: Route, proxy_url: &str) -> Vec<String> {
        let mut args: Vec<String> = Vec::new();

        match route {
            Route::Direct => {
                if self.aria2c {
                    args.push("--downloader".into());
                    args.push("aria2c".into());
                    args.push("--downloader-args".into());
                    args.push(
                        "aria2c:-x 8 -s 8 -k 1M --max-tries=5 --retry-wait=2 \
                         --allow-overwrite=false"
                            .into(),
                    );
                    // Keep ffmpeg out of the way for http (aria2c already has
                    // multiple connections), but as the HLS/DASH fallback when
                    // the native downloader fails on the manifest.
                    if self.ffmpeg {
                        args.push("--downloader".into());
                        args.push("dash,m3u8:ffmpeg".into());
                        args.push("--downloader-args".into());
                        args.push("ffmpeg:-loglevel error".into());
                    }
                } else if self.ffmpeg {
                    args.push("--downloader".into());
                    args.push("http,https,dash,m3u8:ffmpeg".into());
                    args.push("--downloader-args".into());
                    args.push("ffmpeg:-loglevel error".into());
                }
            }
            Route::Proxy => {
                // Tor: no aria2c. yt-dlp's native downloader handles the
                // fragments, with ffmpeg available for manifests it cannot.
                if self.ffmpeg {
                    args.push("--downloader".into());
                    args.push("dash,m3u8:ffmpeg".into());
                    args.push("--downloader-args".into());
                    // ffmpeg honours the proxy yt-dlp passes down.
                    args.push("ffmpeg:-loglevel error".into());
                }
                let _ = proxy_url;
            }
        }

        args
    }

    /// Run the standalone CLI downloaders, in order, after yt-dlp failed.
    ///
    /// Returns the path of the first file one of them produced inside
    /// `out_dir`, or an error string when none worked. All of them are given a
    /// short timeout each; a hung engine must never wedge the queue.
    pub async fn run_fallbacks(
        &self,
        chain: &str,
        url: &str,
        out_dir: &Path,
        route: Route,
        proxy_url: &str,
    ) -> Result<String, String> {
        let wanted: HashSet<String> = chain
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();

        let mut errors: Vec<String> = Vec::new();

        // ---- N_m3u8DL-RE (HLS/DASH, works via Tor) ---------------------
        if wanted.contains("N_m3u8DL-RE") {
            if let Some(bin) = &self.n_m3u8 {
                let mut cmd = Command::new(bin);
                cmd.arg(url)
                    .arg("--save-dir")
                    .arg(out_dir)
                    .arg("--auto-select")
                    .arg("--no-log")
                    .arg("--thread-count")
                    .arg("8")
                    // Binary merge keeps the muxed output byte-identical to
                    // the source streams (no ffmpeg re-encode), which is much
                    // faster; ffmpeg merging only happens for incompatible
                    // stream combinations.
                    .arg("--binary-merge")
                    // Keep the temporary segments until the very end so a
                    // crash can be resumed instead of losing the whole
                    // download.
                    .arg("--del-after-done=false");
                match route {
                    Route::Direct => {}
                    Route::Proxy => {
                        if !proxy_url.is_empty() {
                            // The config already uses socks5h:// form.
                            cmd.arg("--custom-proxy").arg(proxy_url);
                        }
                    }
                }
                match run_with_timeout(&mut cmd, std::time::Duration::from_secs(1800)).await {
                    Ok(0) => {
                        if let Some(f) = newest_media_file(out_dir) {
                            return Ok(f);
                        }
                        errors.push("N_m3u8DL-RE: exited cleanly but no file".into());
                    }
                    Ok(code) => errors.push(format!("N_m3u8DL-RE: exited with {}", code)),
                    Err(e) => errors.push(format!("N_m3u8DL-RE: {}", e)),
                }
            }
        }

        // ---- lux (direct only - no SOCKS support) -----------------------
        if wanted.contains("lux") {
            if let Some(bin) = &self.lux {
                if route == Route::Direct {
                    let mut cmd = Command::new(bin);
                    cmd.arg("-o")
                        .arg(out_dir)
                        .arg("--info=false")
                        .arg(url);
                    match run_with_timeout(&mut cmd, std::time::Duration::from_secs(1800)).await {
                        Ok(0) => {
                            if let Some(f) = newest_media_file(out_dir) {
                                return Ok(f);
                            }
                            errors.push("lux: exited cleanly but no file".into());
                        }
                        Ok(code) => errors.push(format!("lux: exited with {}", code)),
                        Err(e) => errors.push(format!("lux: {}", e)),
                    }
                }
            }
        }

        // ---- you-get (http proxy only) ----------------------------------
        if wanted.contains("you-get") {
            if let Some(bin) = &self.you_get {
                let mut cmd = Command::new(bin);
                cmd.arg("-o").arg(out_dir).arg("--no-caption");
                if route == Route::Proxy && !proxy_url.is_empty() {
                    // you-get speaks http(s) proxies only.
                    if let Some(http) = to_http_proxy(proxy_url) {
                        cmd.arg("-x").arg(http);
                    }
                }
                cmd.arg(url);
                match run_with_timeout(&mut cmd, std::time::Duration::from_secs(1800)).await {
                    Ok(0) => {
                        if let Some(f) = newest_media_file(out_dir) {
                            return Ok(f);
                        }
                        errors.push("you-get: exited cleanly but no file".into());
                    }
                    Ok(code) => errors.push(format!("you-get: exited with {}", code)),
                    Err(e) => errors.push(format!("you-get: {}", e)),
                }
            }
        }

        Err(if errors.is_empty() {
            "no CLI fallback downloader is configured/installed".into()
        } else {
            errors.join("; ")
        })
    }
}

/// Best http-form proxy for engines that only speak that (you-get).
/// A socks5h:// proxy cannot be translated, so `None` is returned.
fn to_http_proxy(proxy_url: &str) -> Option<String> {
    let p = proxy_url.trim();
    if p.starts_with("http://") || p.starts_with("https://") {
        Some(p.to_string())
    } else {
        None
    }
}

/// Run a command to completion, killing it if it exceeds `limit`.
/// Returns the exit code (or an error string when it could not be waited on).
async fn run_with_timeout(cmd: &mut Command, limit: std::time::Duration) -> Result<i32, String> {
    cmd.stdout(std::process::Stdio::null());
    cmd.stderr(std::process::Stdio::null());
    let program = cmd
        .as_std()
        .get_program()
        .to_string_lossy()
        .to_string();
    let mut child = cmd
        .spawn()
        .map_err(|e| format!("{}: {}", program, e))?;

    match tokio::time::timeout(limit, child.wait()).await {
        Ok(Ok(status)) => Ok(status.code().unwrap_or(-1)),
        Ok(Err(e)) => Err(e.to_string()),
        Err(_) => {
            // Hung engine: kill it and its children so the queue moves on.
            let _ = child.start_kill();
            let _ = child.wait().await;
            Err("timed out".into())
        }
    }
}

/// The most recently written media-looking file in `dir` (used to find what a
/// CLI downloader produced, since their naming differs from ours).
fn newest_media_file(dir: &Path) -> Option<String> {
    let mut best: Option<(std::time::SystemTime, String)> = None;
    let entries = std::fs::read_dir(dir).ok()?;
    for e in entries.flatten() {
        let name = e.file_name().to_string_lossy().to_string();
        let lower = name.to_lowercase();
        if lower.ends_with(".part")
            || lower.ends_with(".tmp")
            || lower.ends_with(".ytdl")
            || lower.ends_with(".json")
            || lower.ends_with(".jpg")
            || lower.ends_with(".png")
            || lower.ends_with(".webp")
        {
            continue;
        }
        let is_media = [".mp4", ".m4v", ".mkv", ".webm", ".mov", ".mp3", ".m4a",
                        ".aac", ".flac", ".opus", ".ogg", ".wav", ".ts", ".vtt", ".srt"]
            .iter()
            .any(|ext| lower.ends_with(ext));
        if !is_media {
            continue;
        }
        let mtime = e.metadata().ok()?.modified().ok()?;
        if best.as_ref().map(|(t, _)| mtime > *t).unwrap_or(true) {
            best = Some((mtime, e.path().to_string_lossy().to_string()));
        }
    }
    best.map(|(_, p)| p)
}
