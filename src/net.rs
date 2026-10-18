use dashmap::DashMap;
use std::time::{Duration, Instant};

/// Remembers, per host, whether a direct download recently worked or the
/// host had to go through the configured proxy (e.g. Tor). Entries expire
/// after `ttl`, so blocked/unblocked networks are re-learned automatically.
pub struct RouteCache {
    map: DashMap<String, (bool, Instant)>,
    ttl: Duration,
}

impl RouteCache {
    pub fn new() -> Self {
        Self {
            map: DashMap::new(),
            ttl: Duration::from_secs(1800),
        }
    }

    /// Some(true) = direct recently worked, Some(false) = proxy required.
    pub fn lookup(&self, host: &str) -> Option<bool> {
        if host.is_empty() {
            return None;
        }
        let entry = self.map.get(host)?;
        let (direct_ok, at) = *entry.value();
        if at.elapsed() < self.ttl {
            Some(direct_ok)
        } else {
            None
        }
    }

    pub fn mark(&self, host: &str, direct_ok: bool) {
        if host.is_empty() {
            return;
        }
        self.map.insert(host.to_string(), (direct_ok, Instant::now()));
    }

    /// Decide whether this request should start through the proxy.
    ///
    /// mode: global setting ("auto" | "always" | "never")
    /// override: per-request choice ("auto" | "direct" | "proxy")
    pub fn use_proxy(
        &self,
        host: &str,
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
            // auto: use the proxy only when the host is known to need it;
            // first contact tries direct (and falls back on failure).
            _ => {
                if !proxy_available {
                    return false;
                }
                matches!(self.lookup(host), Some(false))
            }
        }
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
