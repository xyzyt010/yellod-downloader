use serde::Deserialize;

#[derive(Debug, Clone, Deserialize)]
pub struct AppConfig {
    #[serde(default = "default_bind")]
    pub bind_address: String,
    #[serde(default = "default_max_ws")]
    pub max_ws_clients: usize,
    #[serde(default)]
    pub enable_http3: bool,
    #[serde(default = "default_output_dir")]
    pub output_dir: String,
    #[serde(default = "default_file_ttl")]
    pub file_ttl_hours: u64,
    #[serde(default = "default_max_file_size")]
    pub max_file_size_mb: u64,
    #[serde(default = "default_max_concurrent")]
    pub max_concurrent_downloads: usize,
    #[serde(default = "default_max_concurrent_resolves")]
    pub max_concurrent_cold_resolves: usize,
    #[serde(default = "default_job_timeout")]
    pub job_timeout_minutes: u64,
    #[serde(default)]
    pub redis_url: String,
    #[serde(default = "default_aria2_rpc")]
    pub aria2_rpc_url: String,
    #[serde(default)]
    pub aria2_disable_ipv6: bool,
    #[serde(default = "default_aria2_connections")]
    pub aria2_max_connection_per_server: u16,
    #[serde(default = "default_aria2_split")]
    pub aria2_split: u16,
    #[serde(default)]
    pub ytdlp_binary_path: String,
    #[serde(default)]
    pub ytdlp_force_ipv4: bool,
    /// Comma-separated YouTube player clients for yt-dlp
    /// (`youtube:player_client=…`). The default web clients get bot-walled
    /// from server IPs; TV/Android clients keep working. Empty = yt-dlp default.
    #[serde(default = "default_yt_clients")]
    pub yt_clients: String,
    #[serde(default)]
    pub auth_enabled: bool,
    #[serde(default)]
    pub auth_shared_secret: String,
    #[serde(default = "default_proxy_url")]
    pub proxy_url: String,
    #[serde(default = "default_proxy_mode")]
    pub proxy_mode: String,
    /// Tor control socket (for `SIGNAL NEWNYM` circuit rotation when an exit
    /// IP gets bot-walled). Empty = rotation disabled.
    #[serde(default = "default_tor_control_socket")]
    pub tor_control_socket: String,
    /// Tor control auth: password wins over the cookie file when set.
    #[serde(default = "default_tor_control_cookie")]
    pub tor_control_cookie: String,
    #[serde(default = "default_tor_control_password")]
    pub tor_control_password: String,
    /// Fallback chain of standalone CLI downloaders, tried in this order
    /// when yt-dlp cannot fetch the media. Comma-separated; empty disables.
    #[serde(default = "default_cli_downloaders")]
    pub cli_downloaders: String,
    #[serde(default = "default_cookies_path")]
    pub cookies_path: String,
    #[serde(default = "default_db_path")]
    pub db_path: String,
}

impl Default for AppConfig {
    fn default() -> Self {
        Self {
            bind_address: default_bind(),
            max_ws_clients: default_max_ws(),
            enable_http3: false,
            output_dir: default_output_dir(),
            file_ttl_hours: default_file_ttl(),
            max_file_size_mb: default_max_file_size(),
            max_concurrent_downloads: default_max_concurrent(),
            max_concurrent_cold_resolves: default_max_concurrent_resolves(),
            job_timeout_minutes: default_job_timeout(),
            redis_url: String::new(),
            aria2_rpc_url: default_aria2_rpc(),
            aria2_disable_ipv6: true,
            aria2_max_connection_per_server: 16,
            aria2_split: 16,
            ytdlp_binary_path: default_ytdlp_path(),
            ytdlp_force_ipv4: true,
            yt_clients: default_yt_clients(),
            auth_enabled: false,
            auth_shared_secret: String::new(),
            proxy_url: default_proxy_url(),
            proxy_mode: default_proxy_mode(),
            tor_control_socket: default_tor_control_socket(),
            tor_control_cookie: default_tor_control_cookie(),
            tor_control_password: default_tor_control_password(),
            cli_downloaders: default_cli_downloaders(),
            cookies_path: default_cookies_path(),
            db_path: default_db_path(),
        }
    }
}

fn default_bind() -> String { std::env::var("DOWNLOADER_BIND_ADDRESS").unwrap_or_else(|_| "0.0.0.0:5555".into()) }
fn default_max_ws() -> usize { 200 }
fn default_output_dir() -> String { std::env::var("DOWNLOADER_OUTPUT_DIR").unwrap_or_else(|_| "./downloads".into()) }
fn default_file_ttl() -> u64 { 24 }
fn default_max_file_size() -> u64 { 2048 }
fn default_max_concurrent() -> usize { 4 }
fn default_max_concurrent_resolves() -> usize { 4 }
fn default_job_timeout() -> u64 { 30 }
fn default_aria2_rpc() -> String { "http://127.0.0.1:6800/jsonrpc".into() }
fn default_aria2_connections() -> u16 { 16 }
fn default_aria2_split() -> u16 { 16 }
fn default_ytdlp_path() -> String {
    if let Ok(path) = std::env::var("DOWNLOADER_YTDLP_BINARY") {
        if !path.is_empty() {
            return path;
        }
    }
    if std::path::Path::new("/usr/local/bin/yt-dlp").exists() {
        "/usr/local/bin/yt-dlp".into()
    } else {
        String::new()
    }
}

fn default_cookies_path() -> String {
    std::env::var("DOWNLOADER_COOKIES_PATH").unwrap_or_else(|_| "./cookies.txt".into())
}

/// Player clients yt-dlp uses for YouTube. Verified working from datacenter
/// IPs (via Tor): tv_simply exposes the full DASH manifest up to 1080p60,
/// android/android_vr as fallbacks. Override with DOWNLOADER_YT_CLIENTS
/// (set empty to fall back to yt-dlp's own defaults).
fn default_yt_clients() -> String {
    std::env::var("DOWNLOADER_YT_CLIENTS")
        .unwrap_or_else(|_| "tv_simply,android_vr,android".into())
}

/// Extra CLI downloaders tried after yt-dlp itself fails, in order.
/// Comma-separated; each is probed at startup and skipped when not installed.
/// An empty value disables the CLI fallback chain.
///
/// - `N_m3u8DL-RE` is an HLS/DASH specialist with real SOCKS5 (Tor) support -
///   the dependable one.
/// - `lux` / `you-get` are complete alternative engines, but only work without
///   a proxy (neither speaks SOCKS5) and both are unreliable on current
///   YouTube, so they are opt-in (set this variable to add them back).
fn default_cli_downloaders() -> String {
    std::env::var("DOWNLOADER_CLI_DOWNLOADERS")
        .unwrap_or_else(|_| "N_m3u8DL-RE".into())
}

/// Tor control socket used to rotate circuits (`SIGNAL NEWNYM`) when the
/// current exit IP is bot-walled. Auto-detects the standard Debian path;
/// set DOWNLOADER_TOR_CONTROL_SOCKET="" to disable rotation entirely.
fn default_tor_control_socket() -> String {
    if let Ok(v) = std::env::var("DOWNLOADER_TOR_CONTROL_SOCKET") {
        return v;
    }
    if std::path::Path::new("/run/tor/control").exists() {
        "/run/tor/control".into()
    } else {
        String::new()
    }
}

fn default_tor_control_cookie() -> String {
    if let Ok(v) = std::env::var("DOWNLOADER_TOR_CONTROL_COOKIE") {
        return v;
    }
    if std::path::Path::new("/run/tor/control.authcookie").exists() {
        "/run/tor/control.authcookie".into()
    } else {
        String::new()
    }
}

fn default_tor_control_password() -> String {
    std::env::var("DOWNLOADER_TOR_CONTROL_PASSWORD").unwrap_or_default()
}

fn default_db_path() -> String {
    std::env::var("DOWNLOADER_DB_PATH").unwrap_or_else(|_| "./downloader.db".into())
}

fn default_proxy_url() -> String { std::env::var("DOWNLOADER_PROXY_URL").unwrap_or_default() }

/// How the proxy is used: "auto" (direct when it works, proxy when the site
/// blocks this server), "always", or "never". Defaults to "auto" when a
/// proxy URL is configured, otherwise "never".
fn default_proxy_mode() -> String {
    if let Ok(mode) = std::env::var("DOWNLOADER_PROXY_MODE") {
        let mode = mode.trim().to_lowercase();
        if matches!(mode.as_str(), "auto" | "always" | "never") {
            return mode;
        }
        if !mode.is_empty() {
            return "auto".into();
        }
    }
    if std::env::var("DOWNLOADER_PROXY_URL").map(|u| !u.trim().is_empty()).unwrap_or(false) {
        "auto".into()
    } else {
        "never".into()
    }
}
