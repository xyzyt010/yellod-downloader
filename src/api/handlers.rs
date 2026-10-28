use axum::{
    extract::{State, WebSocketUpgrade, Multipart, ws::{Message, WebSocket}},
    response::{IntoResponse, Response},
    Json,
    http::{StatusCode, header, HeaderValue},
    body::Body,
};
use std::sync::Arc;
use crate::config::AppConfig;
use crate::db::Database;
use crate::queue::QueueManager;
use crate::ws::WsHub;
use crate::api::*;

pub struct AppState {
    pub db: Arc<Database>,
    pub queue: Arc<QueueManager>,
    pub ws_hub: Arc<WsHub>,
    pub config: Arc<AppConfig>,
    pub resolver: Arc<crate::resolver::Resolver>,
}

fn compact_error(error: &str) -> String {
    let compact = error
        .lines()
        .map(str::trim)
        .filter(|line| !line.is_empty())
        .take(12)
        .collect::<Vec<_>>()
        .join("\n");

    if compact.is_empty() {
        "Unknown error".to_string()
    } else {
        compact
    }
}

fn friendly_error(error: &str) -> String {
    let compact = compact_error(error);
    let lower = error.to_lowercase();

    if lower.contains("sign in to confirm")
        || lower.contains("not a bot")
        || lower.contains("login_required")
        || lower.contains("youtube.login")
    {
        format!(
            "YouTube is blocking downloads from this server/IP. yt-dlp and Cobalt report that YouTube requires sign-in/authentication for public videos from this host. Add valid YouTube account cookies from a browser session, or route downloads through an unblocked/residential proxy. Original error:\n{}",
            compact
        )
    } else {
        compact
    }
}

pub async fn resolve(
    State(state): State<Arc<AppState>>,
    Json(req): Json<ResolveRequest>,
) -> Result<Json<ResolveResponse>, (StatusCode, String)> {
    state.resolver.resolve(&req).await
        .map(Json)
        .map_err(|e| {
            tracing::error!("Resolve error: {}", e);
            (StatusCode::UNPROCESSABLE_ENTITY, friendly_error(&e))
        })
}

pub async fn download(
    State(state): State<Arc<AppState>>,
    Json(req): Json<DownloadRequest>,
) -> Result<Json<DownloadResponse>, (StatusCode, String)> {
    let job_id = state.queue.enqueue(&spec_from(&req)).await
        .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, friendly_error(&e)))?;

    Ok(Json(DownloadResponse { job_id }))
}

fn spec_from(req: &DownloadRequest) -> crate::queue::JobSpec {
    crate::queue::JobSpec {
        url: req.url.clone(),
        media_type: req.media_type.clone(),
        quality: req.quality.clone(),
        language: req.language.clone(),
        tool: req.tool.clone(),
        connection: req.connection.clone(),
        title: req.title.clone(),
        thumbnail: req.thumbnail.clone(),
        embed_thumbnail: req.embed_thumbnail,
    }
}

pub async fn batch_download(
    State(state): State<Arc<AppState>>,
    Json(req): Json<BatchDownloadRequest>,
) -> Result<Json<BatchDownloadResponse>, (StatusCode, String)> {
    let mut job_ids = Vec::new();
    for item in &req.items {
        let job_id = state.queue.enqueue(&spec_from(item)).await
            .map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, friendly_error(&e)))?;
        job_ids.push(job_id);
    }
    Ok(Json(BatchDownloadResponse { job_ids }))
}

pub async fn get_job(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Result<Json<crate::db::Job>, StatusCode> {
    match state.db.get_job(&id) {
        Ok(Some(job)) => Ok(Json(job)),
        Ok(None) => Err(StatusCode::NOT_FOUND),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

/// Current version + (cached) latest GitHub release, so the UI can tell the
/// user when an update is available.
pub async fn version(State(state): State<Arc<AppState>>) -> Json<serde_json::Value> {
    let current = env!("CARGO_PKG_VERSION");
    let latest = latest_release("xyzyt010/yellod-downloader").await;
    let update_available = match latest.as_deref() {
        Some(tag) => is_newer(tag, current),
        None => false,
    };
    Json(serde_json::json!({
        "name": "yellod-downloader",
        "version": current,
        "latest": latest,
        "update_available": update_available,
        "proxy_mode": state.queue.proxy_mode,
        "proxy_configured": !state.queue.proxy_url.is_empty(),
        "js_runtime": state.queue.js_runtime,
        "aria2c": state.queue.aria2c,
    }))
}

fn normalize_tag(tag: &str) -> &str {
    tag.trim().trim_start_matches('v')
}

/// True when `latest` is a *newer* release than the running `current`.
///
/// The GitHub lookup is cached for an hour, so right after a deploy the
/// cache can still hold the previous tag - comparing with "!=" would then
/// report a bogus "update available". Only a genuinely newer tag counts.
fn is_newer(latest: &str, current: &str) -> bool {
    fn parts(tag: &str) -> Vec<u64> {
        normalize_tag(tag)
            .split(['.', '-'])
            .map(|p| p.parse::<u64>().unwrap_or(0))
            .collect()
    }
    let l = parts(latest);
    let c = parts(current);
    for i in 0..l.len().max(c.len()) {
        let lv = l.get(i).copied().unwrap_or(0);
        let cv = c.get(i).copied().unwrap_or(0);
        if lv != cv {
            return lv > cv;
        }
    }
    false
}

/// GitHub "latest release" lookup, cached for an hour (and quiet on failure).
async fn latest_release(repo: &str) -> Option<String> {
    use std::sync::{Mutex, OnceLock};
    use std::time::Instant;
    static CACHE: OnceLock<Mutex<Option<(Instant, Option<String>)>>> = OnceLock::new();
    let cache = CACHE.get_or_init(|| Mutex::new(None));

    {
        let guard = cache.lock().unwrap();
        if let Some((at, value)) = guard.as_ref() {
            if at.elapsed().as_secs() < 3600 {
                return value.clone();
            }
        }
    }

    let fetched = async {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(6))
            .user_agent("yellod-downloader")
            .build()
            .ok()?;
        let resp = client
            .get(format!("https://api.github.com/repos/{}/releases/latest", repo))
            .send()
            .await
            .ok()?;
        if !resp.status().is_success() {
            return None;
        }
        let body: serde_json::Value = resp.json().await.ok()?;
        body.get("tag_name").and_then(|v| v.as_str()).map(|s| s.to_string())
    }
    .await;

    let mut guard = cache.lock().unwrap();
    *guard = Some((Instant::now(), fetched.clone()));
    fetched
}

pub async fn list_jobs(
    State(state): State<Arc<AppState>>,
) -> Result<Json<Vec<crate::db::Job>>, StatusCode> {
    let jobs = state.db.list_jobs(200).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(jobs))
}

pub async fn get_job_file(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Result<Response, (StatusCode, String)> {
    let job = state.db.get_job(&id)
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "Database error".to_string()))?
        .ok_or((StatusCode::NOT_FOUND, "Job not found".to_string()))?;

    if job.status != "done" {
        return Err((StatusCode::CONFLICT, "This job did not finish successfully, so there is no file to download.".to_string()));
    }

    let path = job.file_path
        .ok_or((StatusCode::NOT_FOUND, "No file is associated with this job.".to_string()))?;

    let file = tokio::fs::File::open(&path).await
        .map_err(|_| (StatusCode::NOT_FOUND, "The file no longer exists on the server.".to_string()))?;
    let metadata = file.metadata().await
        .map_err(|_| (StatusCode::NOT_FOUND, "The file no longer exists on the server.".to_string()))?;

    let filename = std::path::Path::new(&path)
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("download")
        .replace(['"', '\r', '\n'], "_");

    let body = Body::from_stream(tokio_util::io::ReaderStream::new(file));
    let mut response = Response::new(body);
    let headers = response.headers_mut();
    headers.insert(header::CONTENT_TYPE, HeaderValue::from_static("application/octet-stream"));
    if let Ok(value) = HeaderValue::from_str(&format!("attachment; filename=\"{}\"", filename)) {
        headers.insert(header::CONTENT_DISPOSITION, value);
    }
    headers.insert(header::CONTENT_LENGTH, HeaderValue::from_str(&metadata.len().to_string()).unwrap());
    headers.insert(header::HeaderName::from_static("x-content-type-options"), HeaderValue::from_static("nosniff"));
    Ok(response)
}

/// Guess a browser-friendly content-type from the file extension so the
/// inline <video>/<audio> player can render the file without a download.
fn content_type_for(path: &str) -> &'static str {
    match std::path::Path::new(path)
        .extension()
        .and_then(|e| e.to_str())
        .map(|e| e.to_ascii_lowercase())
        .unwrap_or_default()
        .as_str()
    {
        "mp4" | "m4v" | "mov" => "video/mp4",
        "webm" => "video/webm",
        "mkv" => "video/x-matroska",
        "mp3" => "audio/mpeg",
        "m4a" => "audio/mp4",
        "opus" => "audio/opus",
        "ogg" => "audio/ogg",
        "wav" => "audio/wav",
        "flac" => "audio/flac",
        _ => "application/octet-stream",
    }
}

/// Inline media streaming for the built-in player. Unlike `/file` (which forces
/// a download with `Content-Disposition: attachment`), this serves the file
/// inline and honours HTTP Range requests so video/audio seeking works.
pub async fn stream_job_file(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
    headers: axum::http::HeaderMap,
) -> Result<Response, (StatusCode, String)> {
    let job = state.db.get_job(&id)
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "Database error".to_string()))?
        .ok_or((StatusCode::NOT_FOUND, "Job not found".to_string()))?;

    if job.status != "done" {
        return Err((StatusCode::CONFLICT, "This job did not finish successfully.".to_string()));
    }
    let path = job.file_path
        .ok_or((StatusCode::NOT_FOUND, "No file is associated with this job.".to_string()))?;

    let mut file = tokio::fs::File::open(&path).await
        .map_err(|_| (StatusCode::NOT_FOUND, "The file no longer exists on the server.".to_string()))?;
    let total = file.metadata().await
        .map_err(|_| (StatusCode::NOT_FOUND, "The file no longer exists on the server.".to_string()))?
        .len();

    let filename = std::path::Path::new(&path)
        .file_name().and_then(|n| n.to_str()).unwrap_or("media")
        .replace(['"', '\r', '\n'], "_");
    let ctype = content_type_for(&path);

    // Parse a single-range `Range: bytes=start-end` header (the only form the
    // browser media element sends). Anything else -> serve the whole file.
    let mut start = 0u64;
    let mut end = total.saturating_sub(1);
    let mut partial = false;
    if let Some(rv) = headers.get(header::RANGE).and_then(|v| v.to_str().ok()) {
        if let Some(spec) = rv.trim().strip_prefix("bytes=") {
            let spec = spec.split(',').next().unwrap_or("").trim();
            let (s, e) = spec.split_once('-').unwrap_or((spec, ""));
            let ok = if s.is_empty() {
                // suffix form: last N bytes
                if let Ok(n) = e.parse::<u64>() {
                    let n = n.min(total);
                    start = total.saturating_sub(n);
                    end = total.saturating_sub(1);
                    partial = total > 0;
                    true
                } else { false }
            } else if let Ok(sv) = s.parse::<u64>() {
                start = sv.min(total.saturating_sub(1));
                if !e.is_empty() {
                    end = e.parse::<u64>().map(|v| v.min(total.saturating_sub(1))).unwrap_or(total.saturating_sub(1));
                } else {
                    end = total.saturating_sub(1);
                }
                partial = true;
                true
            } else { false };
            if !ok {
                start = 0;
                end = total.saturating_sub(1);
                partial = false;
            }
        }
    }

    let len = end.saturating_sub(start).saturating_add(1);
    use tokio::io::{AsyncReadExt, AsyncSeekExt};
    file.seek(std::io::SeekFrom::Start(start)).await
        .map_err(|_| (StatusCode::INTERNAL_SERVER_ERROR, "Seek failed".to_string()))?;
    let limited = file.take(len);

    let body = Body::from_stream(tokio_util::io::ReaderStream::new(limited));
    let mut response = Response::new(body);
    if partial {
        *response.status_mut() = StatusCode::PARTIAL_CONTENT;
    }
    let h = response.headers_mut();
    h.insert(header::CONTENT_TYPE, HeaderValue::from_static(ctype));
    h.insert(header::ACCEPT_RANGES, HeaderValue::from_static("bytes"));
    // Inline (not attachment) so the browser plays it instead of downloading.
    if let Ok(value) = HeaderValue::from_str(&format!("inline; filename=\"{}\"", filename)) {
        h.insert(header::CONTENT_DISPOSITION, value);
    }
    h.insert(header::CONTENT_LENGTH, HeaderValue::from_str(&len.to_string()).unwrap());
    if partial {
        if let Ok(v) = HeaderValue::from_str(&format!("bytes {}-{}/{}", start, end, total)) {
            h.insert(header::CONTENT_RANGE, v);
        }
    }
    Ok(response)
}

/// True when `candidate` points inside the configured output directory.
/// Used so history deletion can never remove files outside the downloads folder.
fn is_within_output_dir(output_root: &std::path::Path, candidate: &std::path::Path) -> bool {
    let absolute = |p: &std::path::Path| -> std::path::PathBuf {
        if p.is_absolute() {
            p.to_path_buf()
        } else {
            std::env::current_dir()
                .map(|cwd| cwd.join(p))
                .unwrap_or_else(|_| p.to_path_buf())
        }
    };
    absolute(candidate).starts_with(absolute(output_root))
}

/// Delete a job's file and its per-job folder from disk (best effort).
fn remove_job_artifacts(state: &AppState, id: &str, file_path: Option<&str>) {
    let output_root = std::path::PathBuf::from(&state.config.output_dir);
    if let Some(fp) = file_path {
        let path = std::path::Path::new(fp);
        if is_within_output_dir(&output_root, path) {
            let _ = std::fs::remove_file(path);
        }
    }
    let _ = std::fs::remove_dir_all(output_root.join(id));
}

pub async fn delete_job(
    State(state): State<Arc<AppState>>,
    axum::extract::Path(id): axum::extract::Path<String>,
) -> Result<StatusCode, StatusCode> {
    let _ = state.queue.cancel_job(&id).await;
    let file_path = state.db.get_job(&id).ok().flatten().and_then(|j| j.file_path);
    remove_job_artifacts(&state, &id, file_path.as_deref());
    state.db.delete_job(&id).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn delete_jobs(
    State(state): State<Arc<AppState>>,
    axum::extract::Json(ids): axum::extract::Json<Vec<String>>,
) -> Result<StatusCode, StatusCode> {
    for id in &ids {
        let _ = state.queue.cancel_job(id).await;
        let file_path = state.db.get_job(id).ok().flatten().and_then(|j| j.file_path);
        remove_job_artifacts(&state, id, file_path.as_deref());
    }
    state.db.delete_jobs(&ids).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn delete_all_jobs(
    State(state): State<Arc<AppState>>,
) -> Result<StatusCode, StatusCode> {
    if let Ok(jobs) = state.db.list_jobs(1_000_000) {
        for job in &jobs {
            let _ = state.queue.cancel_job(&job.id).await;
            remove_job_artifacts(&state, &job.id, job.file_path.as_deref());
        }
    }
    state.db.delete_all_jobs().map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(StatusCode::NO_CONTENT)
}

pub async fn health() -> &'static str {
    "ok"
}

pub async fn ws_handler(
    ws: WebSocketUpgrade,
    State(state): State<Arc<AppState>>,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| handle_ws(socket, state))
}

async fn handle_ws(mut socket: WebSocket, state: Arc<AppState>) {
    let mut rx = state.ws_hub.subscribe();

    while let Ok(update) = rx.recv().await {
        let msg = serde_json::to_string(&update).unwrap_or_default();
        if socket.send(Message::Text(msg.into())).await.is_err() {
            break;
        }
    }
}

pub async fn upload_cookies(
    State(state): State<Arc<AppState>>,
    mut multipart: Multipart,
) -> Result<Json<serde_json::Value>, StatusCode> {
    while let Some(field) = multipart.next_field().await.map_err(|_| StatusCode::BAD_REQUEST)? {
        let name = field.name().unwrap_or("file").to_string();
        if name == "cookies" || name == "file" {
            let data = field.bytes().await.map_err(|_| StatusCode::BAD_REQUEST)?;
            let path = std::path::PathBuf::from(&state.config.cookies_path);
            if let Some(parent) = path.parent() {
                if !parent.as_os_str().is_empty() {
                    std::fs::create_dir_all(parent).ok();
                }
            }
            tokio::fs::write(&path, &data).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
            #[cfg(unix)]
            {
                use std::os::unix::fs::PermissionsExt;
                let _ = std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600));
            }
            tracing::info!("Cookies uploaded: {} bytes", data.len());
            return Ok(Json(serde_json::json!({
                "ok": true,
                "message": format!("Cookies uploaded successfully ({} bytes)", data.len())
            })));
        }
    }
    Err(StatusCode::BAD_REQUEST)
}

pub async fn check_cookies(
    State(state): State<Arc<AppState>>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let path = std::path::PathBuf::from(&state.config.cookies_path);
    if path.exists() {
        let content = tokio::fs::read_to_string(&path).await.unwrap_or_default();
        let line_count = content.lines().count();
        let has_sid = content.contains("SID");
        Ok(Json(serde_json::json!({
            "exists": true,
            "lines": line_count,
            "has_session": has_sid
        })))
    } else {
        Ok(Json(serde_json::json!({
            "exists": false,
            "lines": 0,
            "has_session": false
        })))
    }
}
