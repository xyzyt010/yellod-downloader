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
    #[serde(default)]
    pub auth_enabled: bool,
    #[serde(default)]
    pub auth_shared_secret: String,
    #[serde(default = "default_proxy_url")]
    pub proxy_url: String,
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
            auth_enabled: false,
            auth_shared_secret: String::new(),
            proxy_url: default_proxy_url(),
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

fn default_db_path() -> String {
    std::env::var("DOWNLOADER_DB_PATH").unwrap_or_else(|_| "./downloader.db".into())
}

fn default_proxy_url() -> String { std::env::var("DOWNLOADER_PROXY_URL").unwrap_or_default() }
