use std::{collections::HashMap, sync::Arc, time::Duration};

use anyhow::Context;
use async_trait::async_trait;
use chrono::Utc;
use librqbit::{
    api::TorrentIdOrHash, AddTorrent, AddTorrentOptions, AddTorrentResponse, ManagedTorrent,
    Session,
};
use tokio::{
    sync::{oneshot, RwLock},
    task::JoinHandle,
};
use uuid::Uuid;

use crate::{
    db::{models::NewDownloadFile, DownloadRepo},
    domain::compute_expires_at,
    error::AppError,
    scheduler::{DownloadScheduler, ScheduledDownload},
};

pub struct ActiveDownload {
    pub session: Arc<Session>,
    pub torrent_id: usize,
    pub task: JoinHandle<()>,
}

#[derive(Clone)]
pub struct LibrqbitScheduler {
    repo: DownloadRepo,
    ttl_secs: i64,
    active: Arc<RwLock<HashMap<Uuid, ActiveDownload>>>,
}

impl LibrqbitScheduler {
    pub fn new(repo: DownloadRepo, ttl_secs: i64) -> Self {
        Self {
            repo,
            ttl_secs,
            active: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

fn metadata_files(handle: &ManagedTorrent) -> anyhow::Result<(String, i64, Vec<NewDownloadFile>)> {
    handle.with_metadata(|metadata| {
        let name = metadata
            .name
            .clone()
            .or_else(|| {
                metadata
                    .info
                    .name
                    .as_ref()
                    .and_then(|value| std::str::from_utf8(value.as_ref()).ok())
                    .map(ToOwned::to_owned)
            })
            .unwrap_or_else(|| "torrent".to_string());

        let total_bytes = i64::try_from(metadata.lengths.total_length())?;
        let files = metadata
            .file_infos
            .iter()
            .enumerate()
            .map(|(index, file)| {
                Ok(NewDownloadFile {
                    file_index: i64::try_from(index)?,
                    relative_path: file.relative_filename.to_string_lossy().into_owned(),
                    size_bytes: i64::try_from(file.len)?,
                    mime_type: mime_guess::from_path(&file.relative_filename)
                        .first_or_octet_stream()
                        .essence_str()
                        .to_string(),
                })
            })
            .collect::<anyhow::Result<Vec<_>>>()?;

        Ok((name, total_bytes, files))
    })?
}

async fn run_download_job(
    repo: DownloadRepo,
    ttl_secs: i64,
    job: ScheduledDownload,
    session: Arc<Session>,
    handle: Arc<ManagedTorrent>,
) {
    let result: anyhow::Result<()> = async {
        handle.wait_until_initialized().await?;

        let metadata_at = Utc::now();
        let (torrent_name, total_bytes, files) = metadata_files(&handle)?;
        repo.mark_metadata_ready(job.id, &torrent_name, total_bytes, metadata_at, &files)
            .await?;

        loop {
            let stats = handle.stats();
            repo.update_progress(
                job.id,
                i64::try_from(stats.progress_bytes)?,
                Some(i64::try_from(stats.total_bytes)?),
            )
            .await?;

            if stats.finished {
                break;
            }

            tokio::time::sleep(Duration::from_secs(1)).await;
        }

        handle.wait_until_completed().await?;

        let completed_at = Utc::now();
        let expires_at = compute_expires_at(completed_at, ttl_secs);
        let stats = handle.stats();
        repo.mark_completed(
            job.id,
            completed_at,
            expires_at,
            i64::try_from(stats.progress_bytes)?,
        )
        .await?;

        let _ = session.pause(&handle).await;

        Ok(())
    }
    .await;

    if let Err(error) = result {
        let _ = repo.mark_failed(job.id, &error.to_string()).await;
    }
}

#[async_trait]
impl DownloadScheduler for LibrqbitScheduler {
    async fn schedule(&self, job: ScheduledDownload) -> Result<(), AppError> {
        let session = Session::new(job.storage_dir.clone())
            .await
            .map_err(|error| AppError::Internal(error.to_string()))?;

        let response = session
            .add_torrent(
                AddTorrent::from_url(job.magnet_uri.clone()),
                Some(AddTorrentOptions {
                    overwrite: true,
                    output_folder: Some(job.storage_dir.display().to_string()),
                    ..Default::default()
                }),
            )
            .await
            .map_err(|error| AppError::Internal(error.to_string()))?;

        let (torrent_id, handle) = match response {
            AddTorrentResponse::Added(torrent_id, handle)
            | AddTorrentResponse::AlreadyManaged(torrent_id, handle) => (torrent_id, handle),
            AddTorrentResponse::ListOnly(_) => {
                return Err(AppError::Internal(
                    "torrent unexpectedly added in list-only mode".to_string(),
                ));
            }
        };

        let repo = self.repo.clone();
        let ttl_secs = self.ttl_secs;
        let active = Arc::clone(&self.active);
        let active_id = job.id;
        let task_session = Arc::clone(&session);
        let task_handle = handle.clone();
        let task_job = job.clone();
        let (start_tx, start_rx) = oneshot::channel();
        let task = tokio::spawn(async move {
            let _ = start_rx.await;
            run_download_job(repo, ttl_secs, task_job, task_session, task_handle).await;
            active.write().await.remove(&active_id);
        });

        self.active.write().await.insert(
            job.id,
            ActiveDownload {
                session,
                torrent_id,
                task,
            },
        );
        let _ = start_tx.send(());

        Ok(())
    }

    async fn cancel(&self, id: Uuid) -> Result<bool, AppError> {
        let active = self.active.write().await.remove(&id);
        let Some(active) = active else {
            return Ok(false);
        };

        let _ = active
            .session
            .delete(TorrentIdOrHash::from(active.torrent_id), true)
            .await
            .context("error deleting active torrent");
        active.task.abort();

        Ok(true)
    }
}
