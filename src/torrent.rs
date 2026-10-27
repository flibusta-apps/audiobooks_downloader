use std::{
    collections::HashMap,
    future::Future,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use chrono::Utc;
use librqbit::{
    api::TorrentIdOrHash, AddTorrent, AddTorrentOptions, AddTorrentResponse, ManagedTorrent,
    Session, SessionOptions, TorrentStatsState,
};
use tokio::{
    sync::Semaphore,
    task::JoinHandle,
    time::{interval, timeout_at, Instant, MissedTickBehavior},
};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error, info, warn};
use uuid::Uuid;

use crate::{
    config::Config,
    db::{models::NewDownloadFile, DownloadRepo},
    domain::compute_expires_at,
    error::AppError,
    scheduler::{DownloadScheduler, ScheduledDownload},
};

const CANCEL_WAIT: Duration = Duration::from_secs(15);
const PROGRESS_TICK: Duration = Duration::from_secs(1);

#[derive(Clone, Debug)]
pub struct SchedulerSettings {
    pub ttl_secs: i64,
    pub max_concurrent: usize,
    pub metadata_timeout: Duration,
    pub no_progress_timeout: Duration,
    pub max_torrent_bytes: Option<u64>,
}

impl SchedulerSettings {
    pub fn from_config(config: &Config) -> Self {
        Self {
            ttl_secs: config.download_ttl_secs,
            max_concurrent: config.max_concurrent_downloads,
            metadata_timeout: Duration::from_secs(config.metadata_timeout_secs),
            no_progress_timeout: Duration::from_secs(config.no_progress_timeout_secs),
            max_torrent_bytes: config.max_torrent_bytes,
        }
    }
}

struct ActiveDownload {
    token: CancellationToken,
    task: JoinHandle<()>,
}

struct Inner {
    repo: DownloadRepo,
    session: Arc<Session>,
    settings: SchedulerSettings,
    semaphore: Arc<Semaphore>,
    active: Mutex<HashMap<Uuid, ActiveDownload>>,
    shutdown: CancellationToken,
}

/// Runs torrents on one shared librqbit session.
#[derive(Clone)]
pub struct LibrqbitScheduler {
    inner: Arc<Inner>,
}

enum Step<T> {
    Done(T),
    Cancelled,
    Shutdown,
    TimedOut,
}

enum Outcome {
    Completed,
    Failed(&'static str),
    Cancelled,
    Shutdown,
}

impl LibrqbitScheduler {
    /// Creates the single shared session. DHT state is not persisted (the
    /// default would write into the user's config dir) and nothing is
    /// listening for incoming connections.
    pub async fn new(repo: DownloadRepo, config: &Config) -> anyhow::Result<Self> {
        let settings = SchedulerSettings::from_config(config);
        let session = Session::new_with_opts(
            config.storage_root.clone(),
            SessionOptions {
                disable_dht_persistence: true,
                enable_upnp_port_forwarding: false,
                ..Default::default()
            },
        )
        .await?;

        Ok(Self {
            inner: Arc::new(Inner {
                repo,
                session,
                semaphore: Arc::new(Semaphore::new(settings.max_concurrent)),
                settings,
                active: Mutex::new(HashMap::new()),
                shutdown: CancellationToken::new(),
            }),
        })
    }
}

/// Returns `None` for files whose names are not valid UTF-8; those are skipped
/// instead of being stored under a lossy (wrong) path.
fn metadata_files(
    handle: &ManagedTorrent,
) -> anyhow::Result<(String, i64, u64, Vec<NewDownloadFile>)> {
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

        let total = metadata.lengths.total_length();
        let total_bytes = i64::try_from(total)?;
        let mut files = Vec::new();
        for (index, file) in metadata.file_infos.iter().enumerate() {
            let Some(relative_path) = file.relative_filename.to_str() else {
                warn!(file_index = index, "skipping file with non-UTF-8 name");
                continue;
            };
            files.push(NewDownloadFile {
                file_index: i64::try_from(index)?,
                relative_path: relative_path.to_owned(),
                size_bytes: i64::try_from(file.len)?,
                mime_type: mime_guess::from_path(&file.relative_filename)
                    .first_or_octet_stream()
                    .essence_str()
                    .to_string(),
            });
        }

        Ok((name, total_bytes, total, files))
    })?
}

impl Inner {
    /// Waits for `fut`, but gives up on cancellation, shutdown or deadline.
    async fn guarded<F: Future>(
        &self,
        cancel: &CancellationToken,
        deadline: Option<Instant>,
        fut: F,
    ) -> Step<F::Output> {
        let timed = async {
            match deadline {
                Some(deadline) => timeout_at(deadline, fut).await.map_err(|_| ()),
                None => Ok(fut.await),
            }
        };

        tokio::select! {
            biased;
            _ = cancel.cancelled() => Step::Cancelled,
            _ = self.shutdown.cancelled() => Step::Shutdown,
            result = timed => match result {
                Ok(value) => Step::Done(value),
                Err(()) => Step::TimedOut,
            },
        }
    }

    async fn drive(
        &self,
        job: &ScheduledDownload,
        cancel: &CancellationToken,
        torrent_id: &mut Option<usize>,
    ) -> Outcome {
        // Queued downloads stay `pending_metadata` until a permit is free.
        let _permit = match self
            .guarded(cancel, None, self.semaphore.clone().acquire_owned())
            .await
        {
            Step::Done(Ok(permit)) => permit,
            Step::Done(Err(_)) | Step::Shutdown => return Outcome::Shutdown,
            Step::Cancelled => return Outcome::Cancelled,
            Step::TimedOut => unreachable!("no deadline"),
        };

        let deadline = Instant::now() + self.settings.metadata_timeout;

        let add = self.session.add_torrent(
            AddTorrent::from_url(job.magnet_uri.clone()),
            Some(AddTorrentOptions {
                overwrite: true,
                output_folder: Some(job.storage_dir.display().to_string()),
                ..Default::default()
            }),
        );
        let response = match self.guarded(cancel, Some(deadline), add).await {
            Step::Done(Ok(response)) => response,
            Step::Done(Err(err)) => {
                error!(download_id = %job.id, error = %err, "add_torrent failed");
                return Outcome::Failed("failed to start download");
            }
            Step::Cancelled => return Outcome::Cancelled,
            Step::Shutdown => return Outcome::Shutdown,
            Step::TimedOut => return Outcome::Failed("metadata resolution timed out"),
        };

        let handle = match response {
            AddTorrentResponse::Added(id, handle) => {
                *torrent_id = Some(id);
                handle
            }
            AddTorrentResponse::AlreadyManaged(..) | AddTorrentResponse::ListOnly(_) => {
                error!(download_id = %job.id, "torrent unexpectedly already managed or list-only");
                return Outcome::Failed("failed to start download");
            }
        };

        match self
            .guarded(cancel, Some(deadline), handle.wait_until_initialized())
            .await
        {
            Step::Done(Ok(())) => {}
            Step::Done(Err(err)) => {
                error!(download_id = %job.id, error = %err, "torrent initialization failed");
                return Outcome::Failed("failed to initialize download");
            }
            Step::Cancelled => return Outcome::Cancelled,
            Step::Shutdown => return Outcome::Shutdown,
            Step::TimedOut => return Outcome::Failed("metadata resolution timed out"),
        }

        let (name, total_bytes, total_u64, files) = match metadata_files(&handle) {
            Ok(value) => value,
            Err(err) => {
                error!(download_id = %job.id, error = %err, "invalid torrent metadata");
                return Outcome::Failed("invalid torrent metadata");
            }
        };

        if let Some(max) = self.settings.max_torrent_bytes {
            if total_u64 > max {
                warn!(download_id = %job.id, total_u64, max, "torrent exceeds size limit");
                return Outcome::Failed("torrent exceeds maximum allowed size");
            }
        }

        if let Err(err) = self
            .repo
            .mark_metadata_ready(job.id, &name, total_bytes, Utc::now(), &files)
            .await
        {
            error!(download_id = %job.id, error = %err, "failed to store metadata");
            return Outcome::Failed("internal error");
        }

        let mut ticker = interval(PROGRESS_TICK);
        ticker.set_missed_tick_behavior(MissedTickBehavior::Skip);
        let mut last_written: i64 = 0;
        let mut last_change = Instant::now();

        loop {
            match self.guarded(cancel, None, ticker.tick()).await {
                Step::Done(_) => {}
                Step::Cancelled => return Outcome::Cancelled,
                Step::Shutdown => return Outcome::Shutdown,
                Step::TimedOut => unreachable!("no deadline"),
            }

            let stats = handle.stats();

            if let Some(err) = &stats.error {
                error!(download_id = %job.id, error = %err, "torrent reported an error");
                return Outcome::Failed("download failed");
            }
            if matches!(stats.state, TorrentStatsState::Error) {
                error!(download_id = %job.id, "torrent entered error state");
                return Outcome::Failed("download failed");
            }

            let progress = match i64::try_from(stats.progress_bytes) {
                Ok(value) => value,
                Err(_) => return Outcome::Failed("internal error"),
            };

            if progress != last_written {
                if let Err(err) = self.repo.update_progress(job.id, progress, None).await {
                    error!(download_id = %job.id, error = %err, "failed to store progress");
                    return Outcome::Failed("internal error");
                }
                last_written = progress;
                last_change = Instant::now();
            }

            if stats.finished {
                break;
            }
            if matches!(stats.state, TorrentStatsState::Paused) {
                return Outcome::Failed("download stopped unexpectedly");
            }
            if last_change.elapsed() >= self.settings.no_progress_timeout {
                warn!(download_id = %job.id, "download made no progress");
                return Outcome::Failed("download made no progress");
            }
        }

        let completed_at = Utc::now();
        let Some(expires_at) = compute_expires_at(completed_at, self.settings.ttl_secs) else {
            error!(download_id = %job.id, "invalid ttl");
            return Outcome::Failed("internal error");
        };
        let downloaded = i64::try_from(handle.stats().progress_bytes).unwrap_or(total_bytes);
        if let Err(err) = self
            .repo
            .mark_completed(job.id, completed_at, expires_at, downloaded)
            .await
        {
            error!(download_id = %job.id, error = %err, "failed to mark completed");
            return Outcome::Failed("internal error");
        }

        Outcome::Completed
    }

    async fn run(self: Arc<Self>, job: ScheduledDownload, cancel: CancellationToken) {
        let mut torrent_id = None;
        let outcome = self.drive(&job, &cancel, &mut torrent_id).await;

        let delete_files = match &outcome {
            Outcome::Completed => {
                info!(download_id = %job.id, "download completed");
                Some(false)
            }
            Outcome::Failed(message) => {
                if let Err(err) = self.repo.mark_failed(job.id, message).await {
                    error!(download_id = %job.id, error = %err, "failed to mark failed");
                }
                Some(true)
            }
            Outcome::Cancelled => Some(true),
            // Leave everything in place; startup recovery handles it.
            Outcome::Shutdown => None,
        };

        if let (Some(delete_files), Some(id)) = (delete_files, torrent_id) {
            if let Err(err) = self
                .session
                .delete(TorrentIdOrHash::Id(id), delete_files)
                .await
            {
                warn!(download_id = %job.id, error = %err, "failed to remove torrent from session");
            }
        }

        self.active
            .lock()
            .expect("active lock poisoned")
            .remove(&job.id);
    }
}

#[async_trait]
impl DownloadScheduler for LibrqbitScheduler {
    async fn schedule(&self, job: ScheduledDownload) -> Result<(), AppError> {
        if self.inner.shutdown.is_cancelled() {
            return Err(AppError::Internal("scheduler is shutting down".to_string()));
        }

        let token = CancellationToken::new();
        let id = job.id;
        // The task removes itself from `active` under this same lock, so it
        // cannot finish before it has been registered.
        let mut active = self.inner.active.lock().expect("active lock poisoned");
        let task = tokio::spawn(Arc::clone(&self.inner).run(job, token.clone()));
        active.insert(id, ActiveDownload { token, task });

        Ok(())
    }

    async fn cancel(&self, id: Uuid) -> Result<bool, AppError> {
        let entry = self
            .inner
            .active
            .lock()
            .expect("active lock poisoned")
            .remove(&id);
        let Some(entry) = entry else {
            return Ok(false);
        };

        entry.token.cancel();
        if tokio::time::timeout(CANCEL_WAIT, entry.task).await.is_err() {
            warn!(download_id = %id, "timed out waiting for cancelled download to stop");
        }

        Ok(true)
    }

    async fn shutdown(&self) {
        self.inner.shutdown.cancel();
        let entries: Vec<ActiveDownload> = self
            .inner
            .active
            .lock()
            .expect("active lock poisoned")
            .drain()
            .map(|(_, entry)| entry)
            .collect();

        for entry in entries {
            if tokio::time::timeout(CANCEL_WAIT, entry.task).await.is_err() {
                debug!("download task did not stop in time during shutdown");
            }
        }

        self.inner.session.stop().await;
    }
}
