use std::path::Path;

use chrono::{Duration, Utc};
use tracing::warn;

use crate::{
    db::DownloadRepo,
    error::AppError,
    storage::{delete_dir_if_exists, resolve_stored_download_dir},
};

const RESTART_MESSAGE: &str = "service restarted before download completed";

pub async fn run_startup_recovery(
    repo: &DownloadRepo,
    storage_root: &Path,
) -> Result<(), AppError> {
    for row in repo.list_active_downloads().await? {
        repo.mark_interrupted(row.id, RESTART_MESSAGE).await?;

        let storage_dir = match resolve_stored_download_dir(
            storage_root,
            Path::new(&row.storage_dir),
        )
        .await
        {
            Ok(storage_dir) => storage_dir,
            Err(error) => {
                warn!(download_id = %row.id, error = %error, "startup recovery ignored invalid storage_dir");
                continue;
            }
        };

        if let Err(error) = delete_dir_if_exists(&storage_dir).await {
            // The row stays `interrupted`, so retention cleanup retries later.
            warn!(download_id = %row.id, error = %error, "startup recovery failed to delete download dir");
        }
    }

    for row in repo.list_completed_downloads().await? {
        let storage_dir = match resolve_stored_download_dir(
            storage_root,
            Path::new(&row.storage_dir),
        )
        .await
        {
            Ok(storage_dir) => storage_dir,
            Err(error) => {
                warn!(download_id = %row.id, error = %error, "startup recovery marked completed row expired due to invalid storage_dir");
                repo.mark_expired(row.id).await?;
                continue;
            }
        };

        if !tokio::fs::try_exists(&storage_dir)
            .await
            .map_err(|error| AppError::Storage(error.to_string()))?
        {
            repo.mark_expired(row.id).await?;
        }
    }

    Ok(())
}

pub async fn expire_due_downloads(
    repo: &DownloadRepo,
    storage_root: &Path,
) -> Result<usize, AppError> {
    let mut expired = 0;

    for row in repo.list_expired_candidates(Utc::now()).await? {
        let storage_dir = match resolve_stored_download_dir(
            storage_root,
            Path::new(&row.storage_dir),
        )
        .await
        {
            Ok(storage_dir) => storage_dir,
            Err(error) => {
                warn!(download_id = %row.id, error = %error, "ttl cleanup skipped invalid storage_dir");
                continue;
            }
        };

        if let Err(error) = delete_dir_if_exists(&storage_dir).await {
            warn!(download_id = %row.id, error = %error, "ttl cleanup delete failed");
            continue;
        }

        if let Err(error) = repo.mark_expired(row.id).await {
            warn!(download_id = %row.id, error = %error, "ttl cleanup mark_expired failed");
            continue;
        }

        expired += 1;
    }

    Ok(expired)
}

/// Deletes failed/interrupted downloads (directory and row) once they have
/// been idle for longer than `ttl_secs`. Rows whose directory cannot be
/// removed are kept and retried on the next run.
pub async fn cleanup_stale_failed_downloads(
    repo: &DownloadRepo,
    storage_root: &Path,
    ttl_secs: i64,
) -> Result<usize, AppError> {
    let Some(cutoff) =
        Duration::try_seconds(ttl_secs).and_then(|ttl| Utc::now().checked_sub_signed(ttl))
    else {
        return Ok(0);
    };
    let mut removed = 0;

    for row in repo.list_stale_failed(cutoff).await? {
        match resolve_stored_download_dir(storage_root, Path::new(&row.storage_dir)).await {
            Ok(storage_dir) => {
                if let Err(error) = delete_dir_if_exists(&storage_dir).await {
                    warn!(download_id = %row.id, error = %error, "failed download cleanup could not delete dir; will retry");
                    continue;
                }
            }
            Err(error) => {
                warn!(download_id = %row.id, error = %error, "failed download cleanup ignored invalid storage_dir");
            }
        }

        if let Err(error) = repo.delete_download(row.id).await {
            warn!(download_id = %row.id, error = %error, "failed download cleanup could not delete row");
            continue;
        }

        removed += 1;
    }

    Ok(removed)
}
