use axum::{
    body::Body,
    extract::{Path, State},
    http::{header, Request, StatusCode},
    response::Response,
    Json,
};
use chrono::Utc;
use serde::Serialize;
use tokio_util::io::ReaderStream;
use tower::ServiceExt;
use tower_http::services::ServeFile;
use uuid::Uuid;

use crate::{
    api::models::{
        CreateDownloadRequest, CreateDownloadResponse, DownloadFileResponse, DownloadFilesResponse,
        DownloadResponse, DownloadStatsResponse,
    },
    db::models::{DownloadFileRow, DownloadRow, NewDownload},
    domain::DownloadStatus,
    error::AppError,
    magnet::sanitize_magnet,
    scheduler::ScheduledDownload,
    state::AppState,
    storage::{delete_dir_if_exists, resolve_stored_download_dir, resolve_stored_file_path},
};

#[derive(Serialize)]
pub struct HealthzResponse {
    pub status: &'static str,
}

pub async fn healthz() -> Json<HealthzResponse> {
    Json(HealthzResponse { status: "ok" })
}

pub async fn create_download(
    State(state): State<AppState>,
    Json(payload): Json<CreateDownloadRequest>,
) -> Result<(StatusCode, Json<CreateDownloadResponse>), AppError> {
    // Never hand the raw URI to librqbit: see `magnet` module docs.
    let magnet = sanitize_magnet(
        &payload.magnet_uri,
        state.config.tracker_allowlist.as_deref(),
    )
    .await?;

    let id = Uuid::now_v7();
    let now = Utc::now();
    let storage_dir = state.config.storage_root.join(id.to_string());

    tokio::fs::create_dir_all(&storage_dir)
        .await
        .map_err(|error| AppError::Storage(error.to_string()))?;

    let existing = match state
        .repo
        .insert_download_dedup(NewDownload {
            id,
            magnet_uri: magnet.uri.clone(),
            status: DownloadStatus::PendingMetadata.as_db().to_string(),
            storage_dir: storage_dir.to_string_lossy().into_owned(),
            info_hash: Some(magnet.info_hash.clone()),
            created_at: now,
            updated_at: now,
        })
        .await
    {
        Ok(existing) => existing,
        Err(error) => {
            let _ = tokio::fs::remove_dir_all(&storage_dir).await;
            return Err(error);
        }
    };

    if let Some(existing) = existing {
        let _ = tokio::fs::remove_dir_all(&storage_dir).await;
        let status = effective_download_status(&existing)?;
        return Ok((
            StatusCode::OK,
            Json(CreateDownloadResponse {
                id: existing.id,
                status: status.as_db(),
                created_at: existing.created_at,
            }),
        ));
    }

    if let Err(error) = state
        .scheduler
        .schedule(ScheduledDownload {
            id,
            magnet_uri: magnet.uri,
            storage_dir: storage_dir.clone(),
        })
        .await
    {
        let _ = state.repo.delete_download(id).await;
        let _ = tokio::fs::remove_dir_all(&storage_dir).await;
        return Err(error);
    }

    Ok((
        StatusCode::ACCEPTED,
        Json(CreateDownloadResponse {
            id,
            status: DownloadStatus::PendingMetadata.as_db(),
            created_at: now,
        }),
    ))
}

pub async fn get_download(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<DownloadResponse>, AppError> {
    let row = state
        .repo
        .get_download(id)
        .await?
        .ok_or(AppError::NotFound)?;
    let status = effective_download_status(&row)?;
    let progress = match row.total_bytes {
        Some(total_bytes) if total_bytes > 0 => row.downloaded_bytes as f64 / total_bytes as f64,
        _ => 0.0,
    };

    Ok(Json(DownloadResponse {
        id: row.id,
        name: row.torrent_name,
        status: status.as_db().to_string(),
        created_at: row.created_at,
        metadata_at: row.metadata_at,
        completed_at: row.completed_at,
        expires_at: row.expires_at,
        error: row.error_message,
        stats: DownloadStatsResponse {
            downloaded_bytes: row.downloaded_bytes,
            total_bytes: row.total_bytes,
            progress,
        },
    }))
}

pub async fn delete_download(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<StatusCode, AppError> {
    let row = state
        .repo
        .get_download(id)
        .await?
        .ok_or(AppError::NotFound)?;

    let storage_dir = resolve_stored_download_dir(
        &state.config.storage_root,
        std::path::Path::new(&row.storage_dir),
    )
    .await?;

    // Stop the torrent first so nothing writes into the directory afterwards.
    let _ = state.scheduler.cancel(id).await;
    delete_dir_if_exists(&storage_dir).await?;
    state.repo.delete_download(id).await?;

    Ok(StatusCode::NO_CONTENT)
}

pub async fn list_files(
    State(state): State<AppState>,
    Path(id): Path<Uuid>,
) -> Result<Json<DownloadFilesResponse>, AppError> {
    let row = state
        .repo
        .get_download(id)
        .await?
        .ok_or(AppError::NotFound)?;
    let files = state.repo.list_files(id).await?;
    let available = effective_download_status(&row)? == DownloadStatus::Completed;

    Ok(Json(DownloadFilesResponse {
        download_id: row.id,
        metadata_ready: row.metadata_at.is_some(),
        files: files
            .into_iter()
            .map(|file| DownloadFileResponse {
                index: file.file_index,
                path: file.relative_path,
                size_bytes: file.size_bytes,
                mime_type: file.mime_type,
                available,
            })
            .collect(),
    }))
}

async fn load_ready_file(
    state: &AppState,
    id: Uuid,
    file_index: i64,
) -> Result<(DownloadRow, DownloadFileRow), AppError> {
    let row = state
        .repo
        .get_download(id)
        .await?
        .ok_or(AppError::NotFound)?;

    match effective_download_status(&row)? {
        DownloadStatus::Completed => {}
        DownloadStatus::Expired => return Err(AppError::Gone),
        _ => return Err(AppError::DownloadNotReady),
    }

    let file = state
        .repo
        .get_file(id, file_index)
        .await?
        .ok_or(AppError::NotFound)?;

    Ok((row, file))
}

fn effective_download_status(row: &DownloadRow) -> Result<DownloadStatus, AppError> {
    let status = DownloadStatus::from_db(&row.status)
        .map_err(|error| AppError::Internal(error.to_string()))?;

    if status == DownloadStatus::Completed && row.expires_at.is_some_and(|ts| ts <= Utc::now()) {
        return Ok(DownloadStatus::Expired);
    }

    Ok(status)
}

pub async fn download_file(
    State(state): State<AppState>,
    Path((id, file_index)): Path<(Uuid, i64)>,
) -> Result<Response<Body>, AppError> {
    let (download, file) = load_ready_file(&state, id, file_index).await?;
    let path = resolve_stored_file_path(
        &state.config.storage_root,
        std::path::Path::new(&download.storage_dir),
        &file.relative_path,
    )
    .await?;
    let handle = tokio::fs::File::open(&path)
        .await
        .map_err(|error| AppError::Storage(error.to_string()))?;
    let metadata = handle
        .metadata()
        .await
        .map_err(|error| AppError::Storage(error.to_string()))?;
    let body = Body::from_stream(ReaderStream::new(handle));

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, &file.mime_type)
        .header(header::CONTENT_LENGTH, metadata.len().to_string())
        .header(
            header::CONTENT_DISPOSITION,
            format!(
                "attachment; filename=\"{}\"",
                safe_download_filename(&file.relative_path)
            ),
        )
        .body(body)
        .map_err(|error| AppError::Internal(error.to_string()))
}

pub async fn stream_file(
    State(state): State<AppState>,
    Path((id, file_index)): Path<(Uuid, i64)>,
    request: Request<Body>,
) -> Result<axum::response::Response, AppError> {
    let (download, file) = load_ready_file(&state, id, file_index).await?;
    let path = resolve_stored_file_path(
        &state.config.storage_root,
        std::path::Path::new(&download.storage_dir),
        &file.relative_path,
    )
    .await?;

    tokio::fs::File::open(&path)
        .await
        .map_err(|error| AppError::Storage(error.to_string()))?;

    ServeFile::new(path)
        .oneshot(request)
        .await
        .map(|response| response.map(Body::new))
        .map_err(|error| AppError::Internal(error.to_string()))
}

fn safe_download_filename(relative_path: &str) -> String {
    std::path::Path::new(relative_path)
        .file_name()
        .and_then(|name| name.to_str())
        .map(|name| {
            let sanitized: String = name
                .chars()
                .map(|ch| match ch {
                    ' '..='~' if ch != '"' && ch != '\\' => ch,
                    _ => '_',
                })
                .collect();

            if sanitized.is_empty() {
                "download.bin".to_string()
            } else {
                sanitized
            }
        })
        .unwrap_or_else(|| "download.bin".to_string())
}
