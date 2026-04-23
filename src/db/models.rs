use chrono::{DateTime, Utc};
use sqlx::FromRow;
use uuid::Uuid;

#[derive(Debug, Clone, FromRow)]
pub struct DownloadRow {
    pub id: Uuid,
    pub magnet_uri: String,
    pub torrent_name: Option<String>,
    pub status: String,
    pub storage_dir: String,
    pub total_bytes: Option<i64>,
    pub downloaded_bytes: i64,
    pub error_message: Option<String>,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
    pub metadata_at: Option<DateTime<Utc>>,
    pub completed_at: Option<DateTime<Utc>>,
    pub expires_at: Option<DateTime<Utc>>,
}

#[derive(Debug, Clone, FromRow)]
pub struct DownloadFileRow {
    pub download_id: Uuid,
    pub file_index: i64,
    pub relative_path: String,
    pub size_bytes: i64,
    pub mime_type: String,
}

#[derive(Debug, Clone)]
pub struct NewDownload {
    pub id: Uuid,
    pub magnet_uri: String,
    pub status: String,
    pub storage_dir: String,
    pub created_at: DateTime<Utc>,
    pub updated_at: DateTime<Utc>,
}

#[derive(Debug, Clone)]
pub struct NewDownloadFile {
    pub file_index: i64,
    pub relative_path: String,
    pub size_bytes: i64,
    pub mime_type: String,
}
