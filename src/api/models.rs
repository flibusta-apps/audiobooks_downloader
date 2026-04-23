use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Deserialize)]
pub struct CreateDownloadRequest {
    pub magnet_uri: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct CreateDownloadResponse {
    pub id: Uuid,
    pub status: &'static str,
    pub created_at: DateTime<Utc>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DownloadStatsResponse {
    pub downloaded_bytes: i64,
    pub total_bytes: Option<i64>,
    pub progress: f64,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DownloadResponse {
    pub id: Uuid,
    pub name: Option<String>,
    pub status: String,
    pub created_at: DateTime<Utc>,
    pub metadata_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub expires_at: Option<DateTime<Utc>>,
    pub error: Option<String>,
    pub stats: DownloadStatsResponse,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DownloadFilesResponse {
    pub download_id: Uuid,
    pub metadata_ready: bool,
    pub files: Vec<DownloadFileResponse>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DownloadFileResponse {
    pub index: i64,
    pub path: String,
    pub size_bytes: i64,
    pub mime_type: String,
    pub available: bool,
}
