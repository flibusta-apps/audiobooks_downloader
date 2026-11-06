use audiobooks_downloader::{
    db::{
        connect, migrate,
        models::{NewDownload, NewDownloadFile},
        DownloadRepo,
    },
    domain::DownloadStatus,
};
use chrono::Utc;
use sqlx::query;
use tempfile::tempdir;
use uuid::Uuid;

#[tokio::test]
async fn dedup_and_queue_admission_are_atomic() {
    use audiobooks_downloader::{db::InsertDownloadResult, error::AppError};
    let dir = tempdir().unwrap();
    let pool = connect(&format!(
        "sqlite://{}",
        dir.path().join("admission.sqlite").display()
    ))
    .await
    .unwrap();
    migrate(&pool).await.unwrap();
    let repo = DownloadRepo::new(pool);
    let new = |hash: &str| NewDownload {
        id: Uuid::now_v7(),
        magnet_uri: format!("magnet:?xt=urn:btih:{hash}"),
        status: DownloadStatus::PendingMetadata.as_db().into(),
        storage_dir: dir.path().display().to_string(),
        info_hash: Some(hash.into()),
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    let first = new("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
    let first_id = first.id;
    assert!(matches!(
        repo.insert_download_dedup(first, 1).await.unwrap(),
        InsertDownloadResult::Inserted
    ));
    let duplicate = repo
        .insert_download_dedup(new("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), 1)
        .await
        .unwrap();
    assert!(matches!(duplicate, InsertDownloadResult::Existing(row) if row.id == first_id));
    let rejected = new("bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb");
    let rejected_id = rejected.id;
    assert!(matches!(
        repo.insert_download_dedup(rejected, 1).await,
        Err(AppError::QueueFull)
    ));
    assert!(repo.get_download(rejected_id).await.unwrap().is_none());
    repo.mark_failed(first_id, "test failure").await.unwrap();
    assert!(matches!(
        repo.insert_download_dedup(new("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"), 1)
            .await
            .unwrap(),
        InsertDownloadResult::Inserted
    ));
}

#[tokio::test]
async fn concurrent_dedup_admission_returns_one_insert_and_one_existing() {
    use audiobooks_downloader::db::InsertDownloadResult;
    let dir = tempdir().unwrap();
    let pool = connect(&format!(
        "sqlite://{}",
        dir.path().join("race.sqlite").display()
    ))
    .await
    .unwrap();
    migrate(&pool).await.unwrap();
    let repo = DownloadRepo::new(pool);
    let new = || NewDownload {
        id: Uuid::now_v7(),
        magnet_uri: "magnet:?xt=urn:btih:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into(),
        status: DownloadStatus::PendingMetadata.as_db().into(),
        storage_dir: dir.path().display().to_string(),
        info_hash: Some("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".into()),
        created_at: Utc::now(),
        updated_at: Utc::now(),
    };
    let (a, b) = tokio::join!(
        repo.insert_download_dedup(new(), 1),
        repo.insert_download_dedup(new(), 1)
    );
    match (a.unwrap(), b.unwrap()) {
        (InsertDownloadResult::Inserted, InsertDownloadResult::Existing(row))
        | (InsertDownloadResult::Existing(row), InsertDownloadResult::Inserted) => {
            assert!(repo.get_download(row.id).await.unwrap().is_some());
        }
        _ => panic!("expected exactly one insert and one existing row"),
    }
}

#[tokio::test]
async fn repository_roundtrips_download_and_files() {
    let temp_dir = tempdir().unwrap();
    let database_path = temp_dir.path().join("repository.sqlite");
    let database_url = format!("sqlite://{}?mode=rwc", database_path.display());

    let pool = connect(&database_url).await.unwrap();
    migrate(&pool).await.unwrap();

    let repo = DownloadRepo::new(pool.clone());
    let download_id = Uuid::now_v7();
    let now = Utc::now();

    repo.insert_download(NewDownload {
        id: download_id,
        magnet_uri: "magnet:?xt=urn:btih:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_string(),
        status: DownloadStatus::PendingMetadata.as_db().to_string(),
        storage_dir: temp_dir.path().to_string_lossy().into_owned(),
        info_hash: None,
        created_at: now,
        updated_at: now,
    })
    .await
    .unwrap();

    repo.insert_files(
        download_id,
        &[
            NewDownloadFile {
                file_index: 2,
                relative_path: "Book/03.mp3".to_string(),
                size_bytes: 3072,
                mime_type: "audio/mpeg".to_string(),
            },
            NewDownloadFile {
                file_index: 0,
                relative_path: "Book/01.mp3".to_string(),
                size_bytes: 1024,
                mime_type: "audio/mpeg".to_string(),
            },
            NewDownloadFile {
                file_index: 1,
                relative_path: "Book/02.mp3".to_string(),
                size_bytes: 2048,
                mime_type: "audio/mpeg".to_string(),
            },
        ],
    )
    .await
    .unwrap();

    let download = repo.get_download(download_id).await.unwrap().unwrap();
    let files = repo.list_files(download_id).await.unwrap();

    assert_eq!(download.id, download_id);
    assert_eq!(files.len(), 3);
    assert_eq!(files[0].file_index, 0);
    assert_eq!(files[0].relative_path, "Book/01.mp3");
    assert_eq!(files[1].file_index, 1);
    assert_eq!(files[1].relative_path, "Book/02.mp3");
    assert_eq!(files[2].file_index, 2);
    assert_eq!(files[2].relative_path, "Book/03.mp3");

    query("DELETE FROM downloads WHERE id = ?")
        .bind(download_id)
        .execute(&pool)
        .await
        .unwrap();

    assert!(repo.get_download(download_id).await.unwrap().is_none());
    assert!(repo.list_files(download_id).await.unwrap().is_empty());
}

#[tokio::test]
async fn connect_creates_missing_database_file_on_first_run() {
    let temp_dir = tempdir().unwrap();
    let database_path = temp_dir.path().join("fresh.db");
    let database_url = format!("sqlite://{}", database_path.display());

    let pool = connect(&database_url).await.unwrap();
    migrate(&pool).await.unwrap();

    assert!(database_path.exists());
}
