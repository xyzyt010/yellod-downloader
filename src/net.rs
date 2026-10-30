use std::sync::Arc;
use std::time::{Duration, Instant};

/// Decide whether a request should start through the proxy.
///
/// mode: global setting ("auto" | "always" | "never")
/// override: per-request choice ("auto" | "direct" | "proxy")
pub fn start_with_proxy(
    mode: &str,
    override_mode: Option<&str>,
    proxy_available: bool,
) -> bool {
    let effective = match override_mode {
        Some(o) if !o.is_empty() && o != "auto" => o,
        _ => mode,
    };
    match effective {
        "direct" | "never" | "off" | "none" => false,
        "proxy" | "always" | "tor" | "on" => proxy_available,
        // auto: always start direct, then fall back to the proxy. "Auto
        // (direct → Tor)" must mean the same thing on every job, not "Tor
        // because the previous attempt needed it"; callers queue the other
        // route when the first attempt fails.
        _ => false,
    }
}

/// True when `url` points at a YouTube property. YouTube bot-walls yt-dlp's
/// default web clients from datacenter IPs, so these URLs need an explicit
/// player-client choice (see [`apply_yt_clients`]).
pub fn is_youtube(url: &str) -> bool {
    let host = host_of(url).to_lowercase();
    host == "youtu.be"
        || host == "youtube.com"
        || host.ends_with(".youtube.com")
        || host.ends_with(".youtubekids.com")
        || host.ends_with(".youtube-nocookie.com")
}

/// Append `--extractor-args "youtube:player_client=..."` for YouTube URLs.
///
/// The default web clients ("default"/"web") hit "Sign in to confirm you're
/// not a bot" from most server IPs no matter which route is used. The TV and
/// Android clients authenticate differently and keep working; yt-dlp merges
/// the formats of every listed client, and a client that fails does not abort
/// extraction as long as another one answers. An empty `clients` value leaves
/// yt-dlp's defaults untouched (deployments that work without this can opt
/// out with `DOWNLOADER_YT_CLIENTS=""`).
pub fn apply_yt_clients(cmd: &mut tokio::process::Command, url: &str, clients: &str) {
    let clients = clients.trim();
    if clients.is_empty() || !is_youtube(url) {
        return;
    }
    cmd.arg("--extractor-args")
        .arg(format!("youtube:player_client={}", clients));
}

/// Tor control connection used to request a fresh circuit (`SIGNAL NEWNYM`).
///
/// YouTube bot-walls individual Tor exit IPs: extraction fails for *every*
/// player client until a different exit is used. When a proxied attempt dies
/// with a route-looking error, callers rotate the circuit and retry, which
/// usually lands on an unflagged exit. Non-fatal by design — if the control
/// socket, cookie or permissions are missing, `newnym` just returns an error
/// and the normal attempt sequence continues.
#[derive(Clone)]
pub struct TorControl {
    socket: String,
    cookie: String,
    password: String,
    /// Cooldown bookkeeping: Tor only honours NEWNYM every ~10 seconds.
    last: Arc<tokio::sync::Mutex<Option<Instant>>>,
}

impl TorControl {
    pub fn from_config(cfg: &crate::config::AppConfig) -> Self {
        Self {
            socket: cfg.tor_control_socket.trim().to_string(),
            cookie: cfg.tor_control_cookie.trim().to_string(),
            password: cfg.tor_control_password.clone(),
            last: Arc::new(tokio::sync::Mutex::new(None)),
        }
    }

    pub fn enabled(&self) -> bool {
        !self.socket.is_empty()
    }

    /// Ask Tor for a new circuit. Waits out Tor's ~10s NEWNYM cooldown first,
    /// so back-to-back rotations actually change the exit.
    pub async fn newnym(&self) -> Result<(), String> {
        if !self.enabled() {
            return Err("tor control socket not configured".into());
        }
        // Held across the awaits on purpose: concurrent jobs must not fire
        // NEWNYM simultaneously (Tor would silently ignore the extras), and
        // the cooldown must be measured from the last *successful* rotation.
        let mut guard = self.last.lock().await;
        if let Some(at) = *guard {
            let cooldown = Duration::from_secs(11);
            if let Some(remaining) = cooldown.checked_sub(at.elapsed()) {
                tokio::time::sleep(remaining).await;
            }
        }
        let result = self.signal_newnym().await;
        if result.is_ok() {
            *guard = Some(Instant::now());
        }
        result
    }

    async fn signal_newnym(&self) -> Result<(), String> {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let mut stream = tokio::net::UnixStream::connect(&self.socket)
            .await
            .map_err(|e| format!("tor control connect ({}): {}", self.socket, e))?;

        let auth = if !self.password.is_empty() {
            let escaped = self.password.replace('\\', "\\\\").replace('"', "\\\"");
            format!("AUTHENTICATE \"{}\"", escaped)
        } else {
            let cookie = tokio::fs::read(&self.cookie)
                .await
                .map_err(|e| format!("tor control cookie ({}): {}", self.cookie, e))?;
            let hex: String = cookie.iter().map(|b| format!("{:02x}", b)).collect();
            format!("AUTHENTICATE {}", hex)
        };
        stream
            .write_all(format!("{}\r\nSIGNAL NEWNYM\r\nQUIT\r\n", auth).as_bytes())
            .await
            .map_err(|e| format!("tor control write: {}", e))?;

        let mut response = String::new();
        let mut buf = [0u8; 1024];
        loop {
            let n = match tokio::time::timeout(Duration::from_secs(5), stream.read(&mut buf)).await {
                Ok(Ok(n)) => n,
                Ok(Err(e)) => return Err(format!("tor control read: {}", e)),
                Err(_) => return Err("tor control read: timeout".into()),
            };
            if n == 0 {
                break;
            }
            response.push_str(&String::from_utf8_lossy(&buf[..n]));
            if response.contains("250 closing connection") {
                break;
            }
        }

        let failed = response
            .lines()
            .any(|l| l.starts_with('5') || l.starts_with("650"));
        if failed || !response.contains("250 OK") {
            return Err(format!(
                "tor control rejected: {}",
                response.trim().replace('\r', " ")
            ));
        }
        Ok(())
    }
}

/// A failure that suggests the network route is the problem (blocked IP,
/// bot wall, timeout, proxy trouble) rather than the URL itself.
pub fn looks_like_route_error(error: &str) -> bool {
    let lower = error.to_lowercase();
    [
        "sign in to confirm",
        "not a bot",
        "login_required",
        "youtube.login",
        "http error 403",
        "http error 429",
        "too many requests",
        "unable to download",
        "timed out",
        "timeout",
        "connection refused",
        "connection reset",
        "connection aborted",
        "network is unreachable",
        "temporary failure",
        "name or service not known",
        "ssl",
        "tls",
        "proxy",
        "geo",
        "blocked",
        "forbidden",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

/// A rate limit / CDN refusal that another downloader may not hit.
pub fn looks_like_rate_limit(error: &str) -> bool {
    let lower = error.to_lowercase();
    [
        "http error 429",
        "too many requests",
        "errorcode=22",
        "download aborted",
        "http error 403",
        "aria2",
    ]
    .iter()
    .any(|needle| lower.contains(needle))
}

/// Detect the first usable JavaScript runtime for yt-dlp (YouTube needs one
/// to solve signatures). Reusing what the system already has avoids extra
/// installs.
pub fn detect_js_runtime() -> Option<&'static str> {
    for rt in ["deno", "node", "bun"] {
        let ok = std::process::Command::new(rt)
            .arg("--version")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .status()
            .map(|s| s.success())
            .unwrap_or(false);
        if ok {
            return Some(rt);
        }
    }
    None
}

/// Whether aria2c can be used as an external downloader (fast multi
/// connection mode) when no SOCKS proxy is in play.
pub fn have_aria2c() -> bool {
    std::process::Command::new("aria2c")
        .arg("--version")
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

pub fn host_of(url: &str) -> String {
    url::Url::parse(url)
        .ok()
        .and_then(|u| u.host_str().map(|h| h.to_string()))
        .unwrap_or_default()
}

/// Friendly name for the route a job used, for badges in the UI.
pub fn route_label(use_proxy: bool, proxy_url: &str) -> &'static str {
    if !use_proxy {
        "direct"
    } else if proxy_url.contains("9050") {
        "tor"
    } else {
        "proxy"
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Cookie/password auth + SIGNAL NEWNYM against a real local Tor. Skips
    /// itself on machines without a control socket (CI) so the suite stays
    /// portable.
    #[tokio::test]
    async fn tor_control_newnym_roundtrip() {
        let cfg = crate::config::AppConfig::default();
        let tor = TorControl::from_config(&cfg);
        if !tor.enabled() || (!cfg.tor_control_cookie.is_empty() && cfg.tor_control_password.is_empty()
            && !std::path::Path::new(&cfg.tor_control_cookie).exists())
        {
            eprintln!("skipping: no usable tor control socket on this machine");
            return;
        }
        tor.newnym().await.expect("first NEWNYM must be accepted");

        // A second rotation must wait out Tor's ~10s cooldown and then still
        // succeed (that is what back-to-back job retries rely on).
        let started = Instant::now();
        tor.newnym().await.expect("second NEWNYM after cooldown");
        assert!(
            started.elapsed() >= Duration::from_secs(10),
            "cooldown not honoured: {:?}",
            started.elapsed()
        );
    }

    #[test]
    fn youtube_detection() {
        assert!(is_youtube("https://www.youtube.com/watch?v=abc"));
        assert!(is_youtube("https://youtu.be/abc"));
        assert!(is_youtube("https://m.youtube.com/watch?v=abc"));
        assert!(!is_youtube("https://example.com/youtube/watch?v=abc"));
        assert!(!is_youtube("https://notyoutube.com/watch?v=abc"));
    }

    #[test]
    fn yt_clients_only_applied_to_youtube() {
        let mut cmd = tokio::process::Command::new("true");
        apply_yt_clients(&mut cmd, "https://example.com/v.mp4", "tv_simply");
        let dry = cmd.as_std().get_args().count();
        assert_eq!(dry, 0, "non-YouTube URLs must not get extractor args");

        let mut cmd = tokio::process::Command::new("true");
        apply_yt_clients(&mut cmd, "https://youtu.be/x", "tv_simply,android");
        let args: Vec<String> = cmd
            .as_std()
            .get_args()
            .map(|a| a.to_string_lossy().into_owned())
            .collect();
        assert_eq!(
            args,
            vec![
                "--extractor-args".to_string(),
                "youtube:player_client=tv_simply,android".to_string()
            ]
        );

        let mut cmd = tokio::process::Command::new("true");
        apply_yt_clients(&mut cmd, "https://youtu.be/x", "  ");
        assert_eq!(cmd.as_std().get_args().count(), 0, "empty list = opt out");
    }
}
