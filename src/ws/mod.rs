use tokio::sync::broadcast;
use serde::Serialize;

#[derive(Debug, Clone, Serialize)]
pub struct WsJobUpdate {
    pub job_id: String,
    pub status: String,
    pub progress: f64,
    pub speed: f64,
    pub eta: f64,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error_message: Option<String>,
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
            status: status.to_string(),
            progress,
            speed,
            eta,
            error_message: None,
        };
        let _ = self.tx.send(update);
    }

    pub fn broadcast_error(&self, job_id: &str, error_message: &str) {
        let update = WsJobUpdate {
            job_id: job_id.to_string(),
            status: "error".to_string(),
            progress: 0.0,
            speed: 0.0,
            eta: 0.0,
            error_message: Some(error_message.to_string()),
        };
        let _ = self.tx.send(update);
    }

    pub fn subscribe(&self) -> broadcast::Receiver<WsJobUpdate> {
        self.tx.subscribe()
    }
}
