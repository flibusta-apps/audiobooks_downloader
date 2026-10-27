mod support;

use audiobooks_downloader::{
    domain::DownloadStatus,
    maintenance::{expire_due_downloads, run_startup_recovery},
};
use chrono::{Duration, Utc};
use sqlx::query;
use std::path::Path;

#[tokio::test]
async fn startup_recovery_marks_active_jobs_interrupted() {
    let app = support::spawn().await;
    let id = app.insert_pending_download().await;

    app.repo
        .mark_metadata_ready(id, "Example Book", 1024, Utc::now(), &[])
        .await
        .unwrap();

    let storage_dir = app.storage_root.join(id.to_string());
    assert!(tokio::fs::try_exists(&storage_dir).await.unwrap());

    run_startup_recovery(&app.repo, &app.storage_root)
        .await
        .unwrap();

    let row = app.repo.get_download(id).await.unwrap().unwrap();

    assert_eq!(row.status, DownloadStatus::Interrupted.as_db());
    assert_eq!(
        row.error_message.as_deref(),
        Some("service restarted before download completed")
    );
    assert!(!tokio::fs::try_exists(&storage_dir).await.unwrap());
}

#[tokio::test]
async fn ttl_expiration_marks_finished_jobs_expired_and_removes_storage_dirs() {
    let app = support::spawn().await;
    let id = app
        .insert_download_with_file(
            DownloadStatus::Completed,
            "Book/01.mp3",
            b"audio-data",
            Some(Utc::now() - Duration::seconds(5)),
        )
        .await;

    let storage_dir = app.storage_root.join(id.to_string());
    assert!(tokio::fs::try_exists(&storage_dir).await.unwrap());
    assert!(
        tokio::fs::try_exists(&Path::new(&storage_dir).join("Book/01.mp3"))
            .await
            .unwrap()
    );

    let expired = expire_due_downloads(&app.repo, &app.storage_root)
        .await
        .unwrap();
    let row = app.repo.get_download(id).await.unwrap().unwrap();

    assert_eq!(expired, 1);
    assert_eq!(row.status, DownloadStatus::Expired.as_db());
    assert!(!tokio::fs::try_exists(&storage_dir).await.unwrap());
}

#[tokio::test]
async fn startup_recovery_expires_completed_rows_with_missing_storage() {
    let app = support::spawn().await;
    let id = app
        .insert_download_with_file(
            DownloadStatus::Completed,
            "Book/01.mp3",
            b"audio-data",
            Some(Utc::now() + Duration::seconds(60)),
        )
        .await;

    let storage_dir = app.storage_root.join(id.to_string());
    tokio::fs::remove_dir_all(&storage_dir).await.unwrap();

    run_startup_recovery(&app.repo, &app.storage_root)
        .await
        .unwrap();

    let row = app.repo.get_download(id).await.unwrap().unwrap();
    assert_eq!(row.status, DownloadStatus::Expired.as_db());
}

#[tokio::test]
async fn ttl_expiration_skips_rows_with_storage_dirs_outside_storage_root() {
    let app = support::spawn().await;
    let id = app
        .insert_download_with_file(
            DownloadStatus::Completed,
            "Book/01.mp3",
            b"audio-data",
            Some(Utc::now() - Duration::seconds(5)),
        )
        .await;
    let outside = tempfile::tempdir().unwrap();
    let outside_path = outside.path().join("keep-me");
    tokio::fs::create_dir_all(&outside_path).await.unwrap();

    query("UPDATE downloads SET storage_dir = ? WHERE id = ?")
        .bind(outside_path.to_string_lossy().into_owned())
        .bind(id)
        .execute(&app.pool)
        .await
        .unwrap();

    let expired = expire_due_downloads(&app.repo, &app.storage_root)
        .await
        .unwrap();
    let row = app.repo.get_download(id).await.unwrap().unwrap();

    assert_eq!(expired, 0);
    assert_eq!(row.status, DownloadStatus::Completed.as_db());
    assert!(tokio::fs::try_exists(&outside_path).await.unwrap());
}

#[tokio::test]
async fn stale_failed_downloads_are_removed_with_their_directories() {
    let app = support::spawn().await;
    let id = app.insert_pending_download().await;
    app.repo.mark_failed(id, "boom").await.unwrap();
    let dir = app.storage_root.join(id.to_string());

    let kept = audiobooks_downloader::maintenance::cleanup_stale_failed_downloads(
        &app.repo,
        &app.storage_root,
        3600,
    )
    .await
    .unwrap();
    assert_eq!(kept, 0);
    assert!(dir.exists());

    tokio::time::sleep(std::time::Duration::from_millis(1100)).await;
    let removed = audiobooks_downloader::maintenance::cleanup_stale_failed_downloads(
        &app.repo,
        &app.storage_root,
        1,
    )
    .await
    .unwrap();

    assert_eq!(removed, 1);
    assert!(!dir.exists());
    assert!(app.repo.get_download(id).await.unwrap().is_none());
}
