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
