use tokio::sync::broadcast;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct WsJobUpdate {
    pub job_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub progress: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub speed: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub eta: Option<f64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
    /// "direct" | "tor" | "proxy" — present when the job's network route is
    /// (re-)decided, so the UI can show it while the download is running.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub route: Option<String>,
    /// Bytes downloaded so far (for the "x of y" line in the UI).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub bytes: Option<u64>,
}

pub struct WsHub {
    tx: broadcast::Sender<WsJobUpdate>,
}

impl WsHub {
    pub fn new() -> Self {
        let (tx, _) = broadcast::channel(256);
        Self { tx }
    }

    pub fn broadcast_status(&self, job_id: &str, status: &str, progress: f64, speed: f64, eta: f64) {
        let update = WsJobUpdate {
            job_id: job_id.to_string(),
            status: Some(status.to_string()),
            progress: Some(progress),
            speed: Some(speed),
            eta: Some(eta),
            error_message: None,
            route: None,
            bytes: None,
        };
        let _ = self.tx.send(update);
    }

    /// Progress of a running download, including how many bytes are on disk.
    pub fn broadcast_progress(
        &self,
        job_id: &str,
        status: &str,
        progress: f64,
        speed: f64,
        eta: f64,
        bytes: u64,
    ) {
        let update = WsJobUpdate {
            job_id: job_id.to_string(),
            status: Some(status.to_string()),
            progress: Some(progress),
            speed: Some(speed),
            eta: Some(eta),
            error_message: None,
            route: None,
            bytes: Some(bytes),
        };
        let _ = self.tx.send(update);
    }

    /// Push a route decision ("direct" | "tor" | "proxy") for a running job.
    pub fn broadcast_route(&self, job_id: &str, route: &str) {
        let update = WsJobUpdate {
            job_id: job_id.to_string(),
            status: None,
            progress: None,
            speed: None,
            eta: None,
            error_message: None,
            route: Some(route.to_string()),
            bytes: None,
        };
        let _ = self.tx.send(update);
    }

    pub fn broadcast_error(&self, job_id: &str, error_message: &str) {
        let update = WsJobUpdate {
            job_id: job_id.to_string(),
            status: Some("error".to_string()),
            progress: Some(0.0),
            speed: Some(0.0),
            eta: Some(0.0),
            error_message: Some(error_message.to_string()),
            route: None,
            bytes: None,
        };
        let _ = self.tx.send(update);
    }

    pub fn subscribe(&self) -> broadcast::Receiver<WsJobUpdate> {
        self.tx.subscribe()
    }
}
