use std::sync::Arc;

use crate::{config::Config, db::DownloadRepo, scheduler::DownloadScheduler};

#[derive(Clone)]
pub struct AppState {
    pub config: Arc<Config>,
    pub repo: DownloadRepo,
    pub scheduler: Arc<dyn DownloadScheduler>,
}
