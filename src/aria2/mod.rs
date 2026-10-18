use serde_json::{Value, json};
use std::sync::Arc;
use tokio::sync::Mutex;

pub struct Aria2Client {
    rpc_url: String,
    client: reqwest::Client,
    next_id: Arc<Mutex<u64>>,
}

impl Aria2Client {
    pub fn new(rpc_url: String) -> Self {
        Self {
            rpc_url,
            client: reqwest::Client::new(),
            next_id: Arc::new(Mutex::new(1)),
        }
    }

    pub async fn call(&self, method: &str, params: Vec<Value>) -> Result<Value, String> {
        let mut id = self.next_id.lock().await;
        *id += 1;
        let request_id = *id;
        drop(id);

        let body = json!({
            "jsonrpc": "2.0",
            "id": request_id,
            "method": method,
            "params": params,
        });

        let resp = self.client.post(&self.rpc_url)
            .header("Content-Type", "application/json")
            .json(&body)
            .send()
            .await
            .map_err(|e| format!("aria2 RPC error: {}", e))?;

        let result: Value = resp.json().await
            .map_err(|e| format!("aria2 RPC parse error: {}", e))?;

        if let Some(error) = result.get("error") {
            return Err(format!("aria2 error: {}", error));
        }

        Ok(result.get("result").cloned().unwrap_or(Value::Null))
    }

    pub async fn add_uri(&self, uris: Vec<String>, options: Value) -> Result<String, String> {
        let result = self.call("aria2.addUri", vec![json!(uris), options]).await?;
        result.as_str().map(|s| s.to_string()).ok_or_else(|| "No GID returned".into())
    }

    pub async fn tell_status(&self, gid: &str) -> Result<Aria2Status, String> {
        let keys = vec!["status", "totalLength", "completedLength", "downloadSpeed", "connections", "errorCode", "errorMessage"];
        let result = self.call("aria2.tellStatus", vec![json!(gid), json!(keys)]).await?;

        Ok(Aria2Status {
            status: result.get("status").and_then(|v| v.as_str()).unwrap_or("unknown").to_string(),
            total_length: result.get("totalLength").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0),
            completed_length: result.get("completedLength").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0),
            download_speed: result.get("downloadSpeed").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0),
            connections: result.get("connections").and_then(|v| v.as_str()).and_then(|s| s.parse().ok()).unwrap_or(0),
            error_code: result.get("errorCode").and_then(|v| v.as_str()).map(|s| s.to_string()),
            error_message: result.get("errorMessage").and_then(|v| v.as_str()).map(|s| s.to_string()),
        })
    }

    pub async fn remove(&self, gid: &str) -> Result<String, String> {
        let result = self.call("aria2.remove", vec![json!(gid)]).await?;
        result.as_str().map(|s| s.to_string()).ok_or_else(|| "No result".into())
    }

    pub async fn remove_download_result(&self, gid: &str) -> Result<String, String> {
        let result = self.call("aria2.removeDownloadResult", vec![json!(gid)]).await?;
        result.as_str().map(|s| s.to_string()).ok_or_else(|| "No result".into())
    }

    pub async fn get_global_stat(&self) -> Result<Value, String> {
        self.call("aria2.getGlobalStat", vec![]).await
    }
}

#[derive(Debug, Clone)]
pub struct Aria2Status {
    pub status: String,
    pub total_length: u64,
    pub completed_length: u64,
    pub download_speed: u64,
    pub connections: u64,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
}

impl Aria2Status {
    pub fn progress(&self) -> f64 {
        if self.total_length == 0 { 0.0 } else { self.completed_length as f64 / self.total_length as f64 }
    }
}
