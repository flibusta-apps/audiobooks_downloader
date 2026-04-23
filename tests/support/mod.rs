use std::{
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
};

use async_trait::async_trait;
use audiobooks_downloader::{
    app::build_app,
    config::Config,
    db::{
        connect, migrate,
        models::{NewDownload, NewDownloadFile},
        DownloadRepo,
    },
    domain::DownloadStatus,
    error::AppError,
    scheduler::{DownloadScheduler, ScheduledDownload},
    state::AppState,
    storage::build_file_path,
};
use chrono::{DateTime, Utc};
use sqlx::SqlitePool;
use tempfile::{tempdir, TempDir};
use tower_http::normalize_path::NormalizePath;
use uuid::Uuid;

#[derive(Default)]
struct SchedulerState {
    scheduled: Vec<ScheduledDownload>,
    cancelled: Vec<Uuid>,
}

#[derive(Clone, Default)]
pub struct RecordingScheduler {
    state: Arc<Mutex<SchedulerState>>,
}

impl RecordingScheduler {
    #[allow(dead_code)]
    pub async fn scheduled_count(&self) -> usize {
        self.state.lock().unwrap().scheduled.len()
    }

    #[allow(dead_code)]
    pub async fn was_cancelled(&self, id: Uuid) -> bool {
        self.state.lock().unwrap().cancelled.contains(&id)
    }
}

#[async_trait]
impl DownloadScheduler for RecordingScheduler {
    async fn schedule(&self, job: ScheduledDownload) -> Result<(), AppError> {
        self.state.lock().unwrap().scheduled.push(job);
        Ok(())
    }

    async fn cancel(&self, id: Uuid) -> Result<bool, AppError> {
        self.state.lock().unwrap().cancelled.push(id);
        Ok(true)
    }
}

pub struct TestApp {
    #[allow(dead_code)]
    pub app: NormalizePath<axum::routing::RouterIntoService<axum::body::Body>>,
    pub repo: DownloadRepo,
    pub pool: SqlitePool,
    #[allow(dead_code)]
    pub scheduler: Arc<RecordingScheduler>,
    pub storage_root: PathBuf,
    pub _dir: TempDir,
}

pub async fn spawn() -> TestApp {
    TestApp::spawn().await
}

impl TestApp {
    pub async fn spawn() -> Self {
        let dir = tempdir().unwrap();
        let database_path = dir.path().join("download-api.sqlite");
        let database_url = format!("sqlite://{}?mode=rwc", database_path.display());
        let storage_root = dir.path().join("storage");

        let pool = connect(&database_url).await.unwrap();
        migrate(&pool).await.unwrap();
        tokio::fs::create_dir_all(&storage_root).await.unwrap();

        let repo = DownloadRepo::new(pool.clone());
        let scheduler = Arc::new(RecordingScheduler::default());
        let state = AppState {
            config: Arc::new(Config {
                bind_addr: "127.0.0.1:0".parse().unwrap(),
                database_url,
                storage_root: storage_root.clone(),
                api_key: "test-api-key".to_string(),
                download_ttl_secs: 86_400,
                cleanup_interval_secs: 60,
            }),
            repo: repo.clone(),
            scheduler: scheduler.clone(),
        };

        Self {
            app: build_app(state),
            repo,
            pool,
            scheduler,
            storage_root,
            _dir: dir,
        }
    }

    pub async fn insert_pending_download(&self) -> Uuid {
        let id = Uuid::now_v7();
        let now = Utc::now();
        let storage_dir = self.storage_root.join(id.to_string());

        tokio::fs::create_dir_all(&storage_dir).await.unwrap();
        self.repo
            .insert_download(NewDownload {
                id,
                magnet_uri: "magnet:?xt=urn:btih:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_string(),
                status: DownloadStatus::PendingMetadata.as_db().to_string(),
                storage_dir: storage_dir.to_string_lossy().into_owned(),
                created_at: now,
                updated_at: now,
            })
            .await
            .unwrap();

        id
    }

    #[allow(dead_code)]
    pub async fn insert_download_with_file(
        &self,
        status: DownloadStatus,
        relative_path: &str,
        contents: &[u8],
        expires_at: Option<DateTime<Utc>>,
    ) -> Uuid {
        let id = self.insert_pending_download().await;
        let now = Utc::now();

        self.repo
            .mark_metadata_ready(
                id,
                "Example Book",
                contents.len() as i64,
                now,
                &[NewDownloadFile {
                    file_index: 0,
                    relative_path: relative_path.to_string(),
                    size_bytes: contents.len() as i64,
                    mime_type: "audio/mpeg".to_string(),
                }],
            )
            .await
            .unwrap();

        let storage_dir = self.storage_root.join(id.to_string());
        let file_path = build_file_path(Path::new(&storage_dir), relative_path).unwrap();
        let parent = file_path.parent().unwrap();

        tokio::fs::create_dir_all(parent).await.unwrap();
        tokio::fs::write(&file_path, contents).await.unwrap();

        sqlx::query(
            "UPDATE downloads SET status = ?, completed_at = ?, expires_at = ?, updated_at = ? WHERE id = ?",
        )
        .bind(status.as_db())
        .bind((status == DownloadStatus::Completed).then_some(now))
        .bind(expires_at)
        .bind(now)
        .bind(id)
        .execute(&self.pool)
        .await
        .unwrap();

        id
    }
}
