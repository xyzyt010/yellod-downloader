use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::RwLock;
use crate::api::{ResolveRequest, ResolveResponse, FormatInfo};

type Cache = Arc<RwLock<HashMap<String, CachedResolve>>>;

struct CachedResolve {
    response: ResolveResponse,
    inserted_at: std::time::Instant,
}

pub struct Resolver {
    cache: Cache,
    ttl: Duration,
    ytdlp_binary_path: String,
    ytdlp_force_ipv4: bool,
    proxy_url: String,
    cookies_path: String,
}

impl Resolver {
    pub fn new(ytdlp_binary_path: String, ytdlp_force_ipv4: bool, proxy_url: String, cookies_path: String) -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
            ttl: Duration::from_secs(600),
            ytdlp_binary_path,
            ytdlp_force_ipv4,
            proxy_url,
            cookies_path,
        }
    }

    fn ytdlp_binary(&self) -> &str {
        if self.ytdlp_binary_path.is_empty() { "yt-dlp" } else { &self.ytdlp_binary_path }
    }

    fn apply_ytdlp_base(&self, cmd: &mut tokio::process::Command) {
        cmd.arg("--js-runtimes").arg("deno")
            .arg("--user-agent").arg("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36")
            .arg("--no-warnings");
        if self.ytdlp_force_ipv4 {
            cmd.arg("-4");
        }
        if !self.proxy_url.is_empty() {
            cmd.arg("--proxy").arg(&self.proxy_url);
        }
        let cookies_path = std::path::PathBuf::from(&self.cookies_path);
        if !self.cookies_path.is_empty() && cookies_path.exists() {
            cmd.arg("--cookies").arg(&cookies_path);
        }
    }

    pub async fn resolve(&self, req: &ResolveRequest) -> Result<ResolveResponse, String> {
        let cache_key = format!("{}:{}", req.url, "");
        {
            let cache = self.cache.read().await;
            if let Some(cached) = cache.get(&cache_key) {
                if cached.inserted_at.elapsed() < self.ttl {
                    return Ok(cached.response.clone());
                }
            }
        }

        let recommended_tool = crate::router::detect_tool_full(&req.url, false);

        let response = if recommended_tool == "aria2" {
            ResolveResponse {
                title: req.url.split('/').last().unwrap_or("direct").to_string(),
                thumbnail: None,
                duration: None,
                formats: vec![FormatInfo {
                    quality: "direct".into(),
                    media_type: "mp4".into(),
                    filesize: None,
                }],
                languages: vec!["en".into()],
                recommended_tool: "aria2".into(),
                extractor: "direct".into(),
            }
        } else {
            self.resolve_ytdlp(&req.url).await?
        };

        {
            let mut cache = self.cache.write().await;
            cache.insert(cache_key, CachedResolve {
                response: response.clone(),
                inserted_at: std::time::Instant::now(),
            });
        }

        Ok(response)
    }

    async fn resolve_ytdlp(&self, url: &str) -> Result<ResolveResponse, String> {
        let mut cmd = tokio::process::Command::new(self.ytdlp_binary());
        self.apply_ytdlp_base(&mut cmd);
        cmd.arg("-J").arg(url);

        let output = cmd.output().await
            .map_err(|e| format!("yt-dlp failed to start: {}", e))?;

        if !output.status.success() {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(format!("yt-dlp error: {}", stderr));
        }

        let info: serde_json::Value = serde_json::from_slice(&output.stdout)
            .map_err(|e| format!("yt-dlp JSON parse error: {}", e))?;

        let title = info.get("title")
            .and_then(|v| v.as_str())
            .unwrap_or("Unknown")
            .to_string();

        let thumbnail = info.get("thumbnail")
            .and_then(|v| v.as_str())
            .map(|s| s.to_string());

        let duration = info.get("duration")
            .and_then(|v| v.as_f64())
            .map(|d| d as u64);

        let mut formats = Vec::new();
        let mut qualities_set = std::collections::HashSet::new();

        if let Some(formats_arr) = info.get("formats").and_then(|v| v.as_array()) {
            for fmt in formats_arr {
                let height = fmt.get("height").and_then(|v| v.as_u64());
                let abr = fmt.get("abr").and_then(|v| v.as_f64());
                let vcodec = fmt.get("vcodec").and_then(|v| v.as_str()).unwrap_or("none");
                let acodec = fmt.get("acodec").and_then(|v| v.as_str()).unwrap_or("none");

                if vcodec != "none" {
                    if let Some(h) = height {
                        let q = format!("{}p", h);
                        if qualities_set.insert(q.clone()) {
                            formats.push(FormatInfo {
                                quality: q,
                                media_type: "mp4".into(),
                                filesize: fmt.get("filesize").and_then(|v| v.as_u64()),
                            });
                        }
                    }
                }
                if acodec != "none" {
                    if let Some(br) = abr {
                        let q = format!("{}kbps", br as u64);
                        if qualities_set.insert(q.clone()) {
                            formats.push(FormatInfo {
                                quality: q,
                                media_type: "mp3".into(),
                                filesize: fmt.get("filesize").and_then(|v| v.as_u64()),
                            });
                        }
                    }
                }
            }
        }

        if formats.is_empty() {
            formats.push(FormatInfo { quality: "best".into(), media_type: "mp4".into(), filesize: None });
            formats.push(FormatInfo { quality: "320kbps".into(), media_type: "mp3".into(), filesize: None });
        }

        let mut languages = vec!["en".into()];
        if let Some(req_subs) = info.get("requested_subtitles") {
            if let Some(obj) = req_subs.as_object() {
                languages = obj.keys().cloned().collect();
                if languages.is_empty() {
                    languages = vec!["en".into()];
                }
            }
        }

        let extractor = info.get("extractor_key")
            .and_then(|v| v.as_str())
            .unwrap_or("Unknown")
            .to_string();

        Ok(ResolveResponse {
            title,
            thumbnail,
            duration,
            formats,
            languages,
            recommended_tool: "ytdlp".into(),
            extractor,
        })
    }

    pub async fn resolve_direct_url(&self, url: &str, media_type: &str, quality: &str) -> Result<String, String> {
        let mut cmd = tokio::process::Command::new(self.ytdlp_binary());
        self.apply_ytdlp_base(&mut cmd);

        if media_type == "mp3" {
            cmd.arg("-f").arg("bestaudio")
                .arg("-g")
                .arg("--audio-format").arg("mp3");
        } else {
            let format_str = if quality == "best" {
                "bestvideo+bestaudio/best".to_string()
            } else {
                let h = quality.trim_end_matches('p');
                format!("bestvideo[height<={}]+bestaudio/best[height<={}]", h, h)
            };
            cmd.arg("-f").arg(&format_str).arg("-g");
        }

        cmd.arg(url);

        let output = cmd.output().await.map_err(|e| format!("yt-dlp -g failed: {}", e))?;
        if !output.status.success() {
            return Err(String::from_utf8_lossy(&output.stderr).to_string());
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        Ok(stdout.lines().next().unwrap_or("").trim().to_string())
    }
}
