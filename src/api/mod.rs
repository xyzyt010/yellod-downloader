pub mod handlers;

use serde::{Serialize, Deserialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolveRequest {
    pub url: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlaylistEntry {
    pub url: String,
    pub title: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ResolveResponse {
    pub title: String,
    pub thumbnail: Option<String>,
    pub duration: Option<u64>,
    pub formats: Vec<FormatInfo>,
    pub languages: Vec<String>,
    pub recommended_tool: String,
    pub extractor: String,
    /// Set when the URL is a playlist/collection (flat listing).
    #[serde(default)]
    pub entries: Vec<PlaylistEntry>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FormatInfo {
    pub quality: String,
    pub media_type: String,
    pub filesize: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadRequest {
    pub url: String,
    #[serde(rename = "type")]
    pub media_type: String,
    pub quality: String,
    pub language: Option<String>,
    pub tool: Option<String>,
    /// "auto" (default) | "direct" | "proxy" — network route for this job.
    #[serde(default)]
    pub connection: Option<String>,
    /// Optional display metadata supplied by the UI (from /api/resolve).
    #[serde(default)]
    pub title: Option<String>,
    #[serde(default)]
    pub thumbnail: Option<String>,
    /// Embed the cover art/thumbnail into the file (mp3 album art / mp4 cover).
    #[serde(default)]
    pub embed_thumbnail: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DownloadResponse {
    pub job_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchDownloadRequest {
    pub items: Vec<DownloadRequest>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BatchDownloadResponse {
    pub job_ids: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Preferences {
    pub lang: Option<String>,
    #[serde(rename = "type")]
    pub media_type: Option<String>,
    pub quality: Option<String>,
    pub tool: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WsMessage {
    pub job_id: Option<String>,
    pub status: Option<String>,
    pub progress: Option<f64>,
    pub speed: Option<f64>,
    pub eta: Option<f64>,
}
