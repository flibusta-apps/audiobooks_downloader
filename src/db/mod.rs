pub mod models;

use std::str::FromStr;

use chrono::{DateTime, Utc};
use sqlx::sqlite::SqliteConnectOptions;
use sqlx::{sqlite::SqlitePoolOptions, SqlitePool};
use uuid::Uuid;

use crate::{domain::DownloadStatus, error::AppError};

use self::models::{DownloadFileRow, DownloadRow, NewDownload, NewDownloadFile};

pub async fn connect(database_url: &str) -> Result<SqlitePool, AppError> {
    let options = SqliteConnectOptions::from_str(database_url)
        .map_err(AppError::from)?
        .foreign_keys(true);

    SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(options)
        .await
        .map_err(AppError::from)
}

pub async fn migrate(pool: &SqlitePool) -> Result<(), AppError> {
    sqlx::migrate!("./migrations")
        .run(pool)
        .await
        .map_err(|error| AppError::Internal(error.to_string()))
}

#[derive(Clone)]
pub struct DownloadRepo {
    pool: SqlitePool,
}

impl DownloadRepo {
    pub fn new(pool: SqlitePool) -> Self {
        Self { pool }
    }

    pub async fn insert_download(&self, new: NewDownload) -> Result<(), AppError> {
        sqlx::query(
            "INSERT INTO downloads (id, magnet_uri, torrent_name, status, storage_dir, total_bytes, downloaded_bytes, error_message, created_at, updated_at, metadata_at, completed_at, expires_at)
             VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
        )
        .bind(new.id)
        .bind(new.magnet_uri)
        .bind(Option::<String>::None)
        .bind(new.status)
        .bind(new.storage_dir)
        .bind(Option::<i64>::None)
        .bind(0_i64)
        .bind(Option::<String>::None)
        .bind(new.created_at)
        .bind(new.updated_at)
        .bind(Option::<chrono::DateTime<chrono::Utc>>::None)
        .bind(Option::<chrono::DateTime<chrono::Utc>>::None)
        .bind(Option::<chrono::DateTime<chrono::Utc>>::None)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn get_download(&self, id: Uuid) -> Result<Option<DownloadRow>, AppError> {
        sqlx::query_as::<_, DownloadRow>(
            "SELECT id, magnet_uri, torrent_name, status, storage_dir, total_bytes, downloaded_bytes, error_message, created_at, updated_at, metadata_at, completed_at, expires_at
             FROM downloads
             WHERE id = ?",
        )
        .bind(id)
        .fetch_optional(&self.pool)
        .await
        .map_err(AppError::from)
    }

    pub async fn list_active_downloads(&self) -> Result<Vec<DownloadRow>, AppError> {
        sqlx::query_as::<_, DownloadRow>(
            "SELECT id, magnet_uri, torrent_name, status, storage_dir, total_bytes, downloaded_bytes, error_message, created_at, updated_at, metadata_at, completed_at, expires_at
             FROM downloads
             WHERE status IN (?, ?)
             ORDER BY created_at ASC",
        )
        .bind(DownloadStatus::PendingMetadata.as_db())
        .bind(DownloadStatus::Downloading.as_db())
        .fetch_all(&self.pool)
        .await
        .map_err(AppError::from)
    }

    pub async fn list_completed_downloads(&self) -> Result<Vec<DownloadRow>, AppError> {
        sqlx::query_as::<_, DownloadRow>(
            "SELECT id, magnet_uri, torrent_name, status, storage_dir, total_bytes, downloaded_bytes, error_message, created_at, updated_at, metadata_at, completed_at, expires_at
             FROM downloads
             WHERE status = ?
             ORDER BY completed_at ASC, created_at ASC",
        )
        .bind(DownloadStatus::Completed.as_db())
        .fetch_all(&self.pool)
        .await
        .map_err(AppError::from)
    }

    pub async fn list_expired_candidates(
        &self,
        now: DateTime<Utc>,
    ) -> Result<Vec<DownloadRow>, AppError> {
        sqlx::query_as::<_, DownloadRow>(
            "SELECT id, magnet_uri, torrent_name, status, storage_dir, total_bytes, downloaded_bytes, error_message, created_at, updated_at, metadata_at, completed_at, expires_at
             FROM downloads
             WHERE status = ? AND expires_at IS NOT NULL AND expires_at <= ?
             ORDER BY expires_at ASC, created_at ASC",
        )
        .bind(DownloadStatus::Completed.as_db())
        .bind(now)
        .fetch_all(&self.pool)
        .await
        .map_err(AppError::from)
    }

    pub async fn delete_download(&self, id: Uuid) -> Result<(), AppError> {
        sqlx::query("DELETE FROM downloads WHERE id = ?")
            .bind(id)
            .execute(&self.pool)
            .await?;

        Ok(())
    }

    pub async fn mark_metadata_ready(
        &self,
        id: Uuid,
        torrent_name: &str,
        total_bytes: i64,
        metadata_at: DateTime<Utc>,
        files: &[NewDownloadFile],
    ) -> Result<(), AppError> {
        let mut tx = self.pool.begin().await?;

        sqlx::query(
            "UPDATE downloads
             SET torrent_name = ?, total_bytes = ?, metadata_at = ?, updated_at = ?, status = ?
             WHERE id = ?",
        )
        .bind(torrent_name)
        .bind(total_bytes)
        .bind(metadata_at)
        .bind(metadata_at)
        .bind(DownloadStatus::Downloading.as_db())
        .bind(id)
        .execute(&mut *tx)
        .await?;

        for file in files {
            sqlx::query(
                "INSERT INTO download_files (download_id, file_index, relative_path, size_bytes, mime_type)
                 VALUES (?, ?, ?, ?, ?)",
            )
            .bind(id)
            .bind(file.file_index)
            .bind(&file.relative_path)
            .bind(file.size_bytes)
            .bind(&file.mime_type)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn update_progress(
        &self,
        id: Uuid,
        downloaded_bytes: i64,
        total_bytes: Option<i64>,
    ) -> Result<(), AppError> {
        let now = Utc::now();

        sqlx::query(
            "UPDATE downloads
             SET downloaded_bytes = ?, total_bytes = COALESCE(?, total_bytes), updated_at = ?
             WHERE id = ?",
        )
        .bind(downloaded_bytes)
        .bind(total_bytes)
        .bind(now)
        .bind(id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn mark_completed(
        &self,
        id: Uuid,
        completed_at: DateTime<Utc>,
        expires_at: DateTime<Utc>,
        downloaded_bytes: i64,
    ) -> Result<(), AppError> {
        sqlx::query(
            "UPDATE downloads
             SET status = ?, downloaded_bytes = ?, completed_at = ?, expires_at = ?, updated_at = ?, error_message = NULL
             WHERE id = ?",
        )
        .bind(DownloadStatus::Completed.as_db())
        .bind(downloaded_bytes)
        .bind(completed_at)
        .bind(expires_at)
        .bind(completed_at)
        .bind(id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn mark_failed(&self, id: Uuid, message: &str) -> Result<(), AppError> {
        let now = Utc::now();

        sqlx::query(
            "UPDATE downloads
             SET status = ?, error_message = ?, updated_at = ?
             WHERE id = ?",
        )
        .bind(DownloadStatus::Failed.as_db())
        .bind(message)
        .bind(now)
        .bind(id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn mark_interrupted(&self, id: Uuid, message: &str) -> Result<(), AppError> {
        let now = Utc::now();

        sqlx::query(
            "UPDATE downloads
             SET status = ?, error_message = ?, updated_at = ?
             WHERE id = ?",
        )
        .bind(DownloadStatus::Interrupted.as_db())
        .bind(message)
        .bind(now)
        .bind(id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn mark_expired(&self, id: Uuid) -> Result<(), AppError> {
        let now = Utc::now();

        sqlx::query(
            "UPDATE downloads
             SET status = ?, error_message = NULL, updated_at = ?
             WHERE id = ?",
        )
        .bind(DownloadStatus::Expired.as_db())
        .bind(now)
        .bind(id)
        .execute(&self.pool)
        .await?;

        Ok(())
    }

    pub async fn insert_files(
        &self,
        download_id: Uuid,
        files: &[NewDownloadFile],
    ) -> Result<(), AppError> {
        let mut tx = self.pool.begin().await?;

        for file in files {
            sqlx::query(
                "INSERT INTO download_files (download_id, file_index, relative_path, size_bytes, mime_type)
                 VALUES (?, ?, ?, ?, ?)",
            )
            .bind(download_id)
            .bind(file.file_index)
            .bind(&file.relative_path)
            .bind(file.size_bytes)
            .bind(&file.mime_type)
            .execute(&mut *tx)
            .await?;
        }

        tx.commit().await?;
        Ok(())
    }

    pub async fn list_files(&self, download_id: Uuid) -> Result<Vec<DownloadFileRow>, AppError> {
        sqlx::query_as::<_, DownloadFileRow>(
            "SELECT download_id, file_index, relative_path, size_bytes, mime_type
             FROM download_files
             WHERE download_id = ?
             ORDER BY file_index ASC",
        )
        .bind(download_id)
        .fetch_all(&self.pool)
        .await
        .map_err(AppError::from)
    }

    pub async fn get_file(
        &self,
        download_id: Uuid,
        file_index: i64,
    ) -> Result<Option<DownloadFileRow>, AppError> {
        sqlx::query_as::<_, DownloadFileRow>(
            "SELECT download_id, file_index, relative_path, size_bytes, mime_type
             FROM download_files
             WHERE download_id = ? AND file_index = ?",
        )
        .bind(download_id)
        .bind(file_index)
        .fetch_optional(&self.pool)
        .await
        .map_err(AppError::from)
    }
}
