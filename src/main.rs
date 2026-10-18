mod api;
mod router;
mod resolver;
mod queue;
mod db;
mod ws;
mod aria2;
mod config;

use axum::{
    routing::{get, post, delete},
    Router,
    response::Html,
};
use std::sync::Arc;
use tower_http::cors::{CorsLayer, Any};
use tower_http::compression::CompressionLayer;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env().add_directive("downloader=info".parse().unwrap()))
        .init();

    let cfg = config::AppConfig::default();
    tracing::info!("Starting downloader on {}", cfg.bind_address);

    let db = Arc::new(db::Database::new(&cfg.db_path).expect("Failed to open SQLite DB"));
    sweep_orphan_job_dirs(&db, &cfg.output_dir);
    let aria2_client = Arc::new(aria2::Aria2Client::new(cfg.aria2_rpc_url.clone()));
    let ws_hub = Arc::new(ws::WsHub::new());
    let resolver = Arc::new(resolver::Resolver::new(
        cfg.ytdlp_binary_path.clone(),
        cfg.ytdlp_force_ipv4,
        cfg.proxy_url.clone(),
        cfg.cookies_path.clone(),
    ));
    let config = Arc::new(cfg.clone());

    let queue = Arc::new(queue::QueueManager::new(
        db.clone(),
        aria2_client.clone(),
        resolver.clone(),
        ws_hub.clone(),
        config.clone(),
    ));

    let state = Arc::new(api::handlers::AppState {
        db: db.clone(),
        queue: queue.clone(),
        ws_hub: ws_hub.clone(),
        config: config.clone(),
        resolver: resolver.clone(),
    });

    let api_routes = Router::new()
        .route("/api/resolve", post(api::handlers::resolve))
        .route("/api/download", post(api::handlers::download))
        .route("/api/download/batch", post(api::handlers::batch_download))
        .route("/api/jobs", get(api::handlers::list_jobs))
        .route("/api/jobs/{id}/file", get(api::handlers::get_job_file))
        .route("/api/jobs/{id}", delete(api::handlers::delete_job))
        .route("/api/jobs/delete/batch", post(api::handlers::delete_jobs))
        .route("/api/jobs/delete/all", post(api::handlers::delete_all_jobs))
        .route("/api/upload-cookies", post(api::handlers::upload_cookies))
        .route("/api/cookies", get(api::handlers::check_cookies))
        .route("/ws", get(api::handlers::ws_handler));

    let app = Router::new()
        .route("/", get(serve_index))
        .route("/healthz", get(api::handlers::health))
        .merge(api_routes)
        .layer(CompressionLayer::new().br(true))
        .layer(CorsLayer::new().allow_origin(Any).allow_methods(Any).allow_headers(Any))
        .with_state(state);

    let addr: std::net::SocketAddr = cfg.bind_address.parse().expect("Invalid bind address");
    tracing::info!("Listening on {}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    axum::serve(listener, app).await.unwrap();
}

async fn serve_index() -> Html<&'static str> {
    Html(include_str!("assets/index.html"))
}

/// Remove UUID-named job directories under the output folder that have no
/// matching database entry (left behind by deletions or crashes). Non-UUID
/// folders are never touched, so unrelated data is safe.
fn sweep_orphan_job_dirs(db: &db::Database, output_dir: &str) {
    let root = std::path::PathBuf::from(output_dir);
    let known: std::collections::HashSet<String> = db
        .all_job_ids()
        .unwrap_or_default()
        .into_iter()
        .collect();
    if let Ok(read_dir) = std::fs::read_dir(&root) {
        for entry in read_dir.flatten() {
            let name = entry.file_name().to_string_lossy().to_string();
            let path = entry.path();
            if path.is_dir()
                && uuid::Uuid::parse_str(&name).is_ok()
                && !known.contains(&name)
            {
                if std::fs::remove_dir_all(&path).is_ok() {
                    tracing::info!("Removed orphaned job directory {}", name);
                }
            }
        }
    }
}
