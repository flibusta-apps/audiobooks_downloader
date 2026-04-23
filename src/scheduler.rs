use std::path::PathBuf;

use async_trait::async_trait;
use uuid::Uuid;

use crate::error::AppError;

#[derive(Clone, Debug)]
pub struct ScheduledDownload {
    pub id: Uuid,
    pub magnet_uri: String,
    pub storage_dir: PathBuf,
}

#[async_trait]
pub trait DownloadScheduler: Send + Sync {
    async fn schedule(&self, job: ScheduledDownload) -> Result<(), AppError>;
    async fn cancel(&self, id: Uuid) -> Result<bool, AppError>;
}

pub struct NoopScheduler;

#[async_trait]
impl DownloadScheduler for NoopScheduler {
    async fn schedule(&self, _job: ScheduledDownload) -> Result<(), AppError> {
        Ok(())
    }

    async fn cancel(&self, _id: Uuid) -> Result<bool, AppError> {
        Ok(false)
    }
}
