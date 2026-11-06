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

/// HTTP tracker redirects and response sizes cannot be constrained in librqbit
/// 8.1.1. Strip them before either metadata resolution or the real add.
fn tracker_policy(magnet: &str, allow_http: bool) -> String {
    if allow_http {
        return magnet.to_owned();
    }
    let Ok(mut uri) = url::Url::parse(magnet) else {
        return magnet.to_owned();
    };
    let pairs: Vec<(String, String)> = uri
        .query_pairs()
        .filter(|(key, value)| {
            key != "tr" || url::Url::parse(value).is_ok_and(|tracker| tracker.scheme() == "udp")
        })
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect();
    uri.query_pairs_mut().clear().extend_pairs(pairs);
    uri.into()
}

#[derive(Clone, Debug)]
pub struct SchedulerSettings {
    pub ttl_secs: i64,
    pub max_concurrent: usize,
    pub metadata_timeout: Duration,
    pub no_progress_timeout: Duration,
    pub max_torrent_bytes: Option<u64>,
    pub max_torrent_files: usize,
}

impl SchedulerSettings {
    pub fn from_config(config: &Config) -> Self {
        Self {
            ttl_secs: config.download_ttl_secs,
            max_concurrent: config.max_concurrent_downloads,
            metadata_timeout: Duration::from_secs(config.metadata_timeout_secs),
            no_progress_timeout: Duration::from_secs(config.no_progress_timeout_secs),
            max_torrent_bytes: config.max_torrent_bytes,
            max_torrent_files: config.max_torrent_files,
        }
    }
}

struct ActiveDownload {
    token: CancellationToken,
    task: tokio::sync::Mutex<Option<JoinHandle<()>>>,
}

impl ActiveDownload {
    async fn cancel(&self, wait: Duration) -> Result<(), AppError> {
        // Keep the handle on timeout and serialize retries for this download.
        let mut task = self.task.lock().await;
        self.token.cancel();
        if let Some(handle) = task.as_mut() {
            match tokio::time::timeout(wait, handle).await {
                Ok(Ok(())) => *task = None,
                Ok(Err(_)) | Err(_) => return Err(AppError::CancellationInProgress),
            }
        }
        Ok(())
    }
}

struct Inner {
    repo: DownloadRepo,
    session: Arc<Session>,
    settings: SchedulerSettings,
    semaphore: Arc<Semaphore>,
    active: Mutex<HashMap<Uuid, Arc<ActiveDownload>>>,
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
        if config.allow_http_trackers {
            warn!("HTTP trackers enabled: redirects and unbounded tracker responses cannot be restricted by librqbit; use network egress filtering");
        }
        let session = Session::new_with_opts(
            config.storage_root.clone(),
            SessionOptions {
                disable_dht_persistence: true,
                enable_upnp_port_forwarding: false,
                socks_proxy_url: config.torrent_socks_proxy_url.clone(),
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
        let files = downloadable_files(&metadata.file_infos)?;

        Ok((name, total_bytes, total, files))
    })?
}

fn downloadable_files(
    file_infos: &[librqbit::file_info::FileInfo],
) -> anyhow::Result<Vec<NewDownloadFile>> {
    let mut files = Vec::new();
    for (index, file) in file_infos.iter().enumerate() {
        if file.attrs.padding {
            continue;
        }
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

    Ok(files)
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
        torrent_id: &Mutex<Option<usize>>,
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

        let magnet_uri = tracker_policy(&job.magnet_uri, job.allow_http_trackers);
        let preflight = self.session.add_torrent(
            AddTorrent::from_url(magnet_uri),
            Some(AddTorrentOptions {
                list_only: true,
                output_folder: Some(job.storage_dir.display().to_string()),
                ..Default::default()
            }),
        );
        let metadata = match self.guarded(cancel, Some(deadline), preflight).await {
            Step::Done(Ok(AddTorrentResponse::ListOnly(metadata))) => metadata,
            Step::Done(Ok(_)) => return Outcome::Failed("unexpected metadata response"),
            Step::Done(Err(err)) => {
                warn!(download_id = %job.id, error = %err, "metadata resolution failed");
                return Outcome::Failed("invalid torrent metadata");
            }
            Step::Cancelled => return Outcome::Cancelled,
            Step::Shutdown => return Outcome::Shutdown,
            Step::TimedOut => return Outcome::Failed("metadata resolution timed out"),
        };
        // List-only does not create payload files. Reuse exactly the checked bytes
        // rather than resolving the magnet again (no metadata TOCTOU).
        if metadata.info.piece_length == 0 {
            return Outcome::Failed("invalid torrent piece length");
        }
        let lengths = match metadata.info.iter_file_lengths() {
            Ok(lengths) => lengths,
            Err(_) => return Outcome::Failed("invalid torrent file list"),
        };
        let mut count = 0_usize;
        let mut total = 0_u64;
        for length in lengths {
            count += 1;
            let Some(sum) = total.checked_add(length) else {
                return Outcome::Failed("torrent size overflow");
            };
            total = sum;
            if count > self.settings.max_torrent_files {
                return Outcome::Failed("torrent exceeds maximum file count");
            }
        }
        if total > i64::MAX as u64
            || self
                .settings
                .max_torrent_bytes
                .is_some_and(|max| total > max)
        {
            return Outcome::Failed("torrent exceeds maximum allowed size");
        }

        let add = self.session.add_torrent(
            AddTorrent::from_bytes(metadata.torrent_bytes),
            Some(AddTorrentOptions {
                overwrite: true,
                output_folder: Some(job.storage_dir.display().to_string()),
                initial_peers: Some(metadata.seen_peers),
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
                *torrent_id.lock().unwrap_or_else(|err| err.into_inner()) = Some(id);
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
        let worker_inner = Arc::clone(&self);
        let worker_job = job.clone();
        let torrent_id = Arc::new(Mutex::new(None));
        let worker_torrent_id = Arc::clone(&torrent_id);
        let worker_cancel = cancel.clone();
        let worker = tokio::spawn(async move {
            worker_inner
                .drive(&worker_job, &worker_cancel, &worker_torrent_id)
                .await
        });
        let outcome = match worker.await {
            Ok(result) => result,
            Err(err) => {
                error!(download_id = %job.id, error = %err, panic = err.is_panic(), "download worker terminated");
                Outcome::Failed("download worker panicked or terminated unexpectedly")
            }
        };
        let torrent_id = *torrent_id.lock().unwrap_or_else(|err| err.into_inner());

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
            while let Err(err) = self
                .session
                .delete(TorrentIdOrHash::Id(id), delete_files)
                .await
            {
                warn!(download_id = %job.id, error = %err, "failed to remove torrent from session");
                if !matches!(outcome, Outcome::Cancelled) {
                    break;
                }
                // Do not confirm cancellation while the torrent may still write.
                tokio::time::sleep(PROGRESS_TICK).await;
            }
        }

        let mut active = self.active.lock().expect("active lock poisoned");
        // A cancelled entry must remain discoverable until cancel confirms join.
        if !cancel.is_cancelled() {
            active.remove(&job.id);
        }
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
        active.insert(
            id,
            Arc::new(ActiveDownload {
                token,
                task: tokio::sync::Mutex::new(Some(task)),
            }),
        );

        Ok(())
    }

    async fn cancel(&self, id: Uuid) -> Result<bool, AppError> {
        let entry = self
            .inner
            .active
            .lock()
            .expect("active lock poisoned")
            .get(&id)
            .cloned();
        let Some(entry) = entry else {
            return Ok(false);
        };

        entry.cancel(CANCEL_WAIT).await?;
        self.inner
            .active
            .lock()
            .expect("active lock poisoned")
            .remove(&id);

        Ok(true)
    }

    async fn shutdown(&self) {
        self.inner.shutdown.cancel();
        let entries: Vec<Arc<ActiveDownload>> = self
            .inner
            .active
            .lock()
            .expect("active lock poisoned")
            .drain()
            .map(|(_, entry)| entry)
            .collect();

        for entry in entries {
            let mut task = entry.task.lock().await;
            if let Some(handle) = task.as_mut() {
                if tokio::time::timeout(CANCEL_WAIT, handle).await.is_err() {
                    debug!("download task did not stop in time during shutdown");
                }
            }
        }

        self.inner.session.stop().await;
    }
}

#[cfg(test)]
mod tests {
    use super::tracker_policy;

    #[test]
    fn padding_files_are_excluded_without_renumbering_real_files() {
        let file = |path: &str, padding| {
            let mut file = librqbit::file_info::FileInfo {
                relative_filename: path.into(),
                offset_in_torrent: 0,
                piece_range: 0..1,
                attrs: Default::default(),
                len: 10,
            };
            file.attrs.padding = padding;
            file
        };
        let files = super::downloadable_files(&[
            file("first.mp3", false),
            file(".pad/10", true),
            file("last.mp3", false),
        ])
        .unwrap();
        assert_eq!(files.len(), 2);
        assert_eq!(
            (files[0].file_index, files[0].relative_path.as_str()),
            (0, "first.mp3")
        );
        assert_eq!(
            (files[1].file_index, files[1].relative_path.as_str()),
            (2, "last.mp3")
        );
    }

    #[tokio::test]
    async fn cancellation_timeout_keeps_handle_for_serialized_retries() {
        let (release, stopped) = tokio::sync::oneshot::channel();
        let entry = std::sync::Arc::new(super::ActiveDownload {
            token: tokio_util::sync::CancellationToken::new(),
            task: tokio::sync::Mutex::new(Some(tokio::spawn(async move {
                let _ = stopped.await;
            }))),
        });
        assert!(matches!(
            entry.cancel(std::time::Duration::ZERO).await,
            Err(crate::error::AppError::CancellationInProgress)
        ));
        assert!(entry.token.is_cancelled());
        assert!(entry.task.lock().await.is_some());
        release.send(()).unwrap();
        let other = entry.clone();
        let (first, second) = tokio::join!(
            entry.cancel(std::time::Duration::from_secs(1)),
            other.cancel(std::time::Duration::from_secs(1))
        );
        first.unwrap();
        second.unwrap();
        assert!(entry.task.lock().await.is_none());
    }

    #[test]
    fn secure_policy_strips_http_and_https_but_preserves_udp_and_hash() {
        let magnet = "magnet:?xt=urn:btih:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa&tr=http%3A%2F%2Fa.example%2Fannounce&tr=https%3A%2F%2Fb.example%2Fannounce&tr=udp%3A%2F%2Fc.example%3A80&dn=Book";
        let result = url::Url::parse(&tracker_policy(magnet, false)).unwrap();
        let trackers: Vec<_> = result
            .query_pairs()
            .filter(|(k, _)| k == "tr")
            .map(|(_, v)| v.into_owned())
            .collect();
        assert_eq!(trackers, ["udp://c.example:80"]);
        assert!(result
            .query_pairs()
            .any(|(k, v)| k == "xt" && v == "urn:btih:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"));
        assert!(result.query_pairs().any(|(k, v)| k == "dn" && v == "Book"));
    }

    #[test]
    fn opt_in_preserves_http_trackers_unchanged() {
        let magnet = "magnet:?xt=urn:btih:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa&tr=https%3A%2F%2Fa.example%2Fannounce";
        assert_eq!(tracker_policy(magnet, true), magnet);
    }

    #[test]
    fn secure_policy_allows_trackerless_magnets() {
        let magnet = "magnet:?xt=urn:btih:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";
        let result = url::Url::parse(&tracker_policy(magnet, false)).unwrap();
        let original = url::Url::parse(magnet).unwrap();
        assert_eq!(
            result.query_pairs().collect::<Vec<_>>(),
            original.query_pairs().collect::<Vec<_>>()
        );
    }
}
