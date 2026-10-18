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
    let tool = req.tool.as_deref();
    let job_id = state.queue.enqueue(
        &req.url,
        &req.media_type,
        &req.quality,
        req.language.as_deref(),
        tool,
    ).await.map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, friendly_error(&e)))?;

    Ok(Json(DownloadResponse { job_id }))
}

pub async fn batch_download(
    State(state): State<Arc<AppState>>,
    Json(req): Json<BatchDownloadRequest>,
) -> Result<Json<BatchDownloadResponse>, (StatusCode, String)> {
    let mut job_ids = Vec::new();
    for item in req.items {
        let job_id = state.queue.enqueue(
            &item.url,
            &item.media_type,
            &item.quality,
            item.language.as_deref(),
            item.tool.as_deref(),
        ).await.map_err(|e| (StatusCode::INTERNAL_SERVER_ERROR, friendly_error(&e)))?;
        job_ids.push(job_id);
    }
    Ok(Json(BatchDownloadResponse { job_ids }))
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
