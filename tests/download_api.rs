mod support;

use audiobooks_downloader::{
    api::models::{DownloadFilesResponse, DownloadResponse},
    db::models::NewDownloadFile,
    domain::DownloadStatus,
};
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use chrono::Utc;
use http_body_util::BodyExt;
use sqlx::query;
use tempfile::tempdir;
use tower::ServiceExt;

use self::support::spawn;

#[tokio::test]
async fn creating_a_download_returns_accepted_and_schedules_the_job() {
    let test_app = spawn().await;

    let response = test_app
        .app
        .clone()
        .oneshot(
            Request::post("/api/v1/downloads")
                .header("x-api-key", "test-api-key")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"magnet_uri":"magnet:?xt=urn:btih:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::ACCEPTED);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let payload: serde_json::Value = serde_json::from_slice(&body).unwrap();
    let download_id = uuid::Uuid::parse_str(payload["id"].as_str().unwrap()).unwrap();
    let row = test_app
        .repo
        .get_download(download_id)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(payload["status"], "pending_metadata");
    assert_eq!(row.status, "pending_metadata");
    assert_eq!(test_app.scheduler.scheduled_count().await, 1);
}

#[tokio::test]
async fn get_download_returns_current_status_for_pending_download() {
    let test_app = spawn().await;
    let download_id = test_app.insert_pending_download().await;

    let response = test_app
        .app
        .clone()
        .oneshot(
            Request::get(format!("/api/v1/downloads/{download_id}"))
                .header("x-api-key", "test-api-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let payload: DownloadResponse = serde_json::from_slice(&body).unwrap();

    assert_eq!(payload.id, download_id);
    assert_eq!(payload.status, "pending_metadata");
    assert_eq!(payload.stats.downloaded_bytes, 0);
    assert_eq!(payload.stats.total_bytes, None);
    assert_eq!(payload.stats.progress, 0.0);
}

#[tokio::test]
async fn list_files_reports_metadata_not_ready_before_metadata_exists() {
    let test_app = spawn().await;
    let download_id = test_app.insert_pending_download().await;

    let response = test_app
        .app
        .clone()
        .oneshot(
            Request::get(format!("/api/v1/downloads/{download_id}/files"))
                .header("x-api-key", "test-api-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let payload: DownloadFilesResponse = serde_json::from_slice(&body).unwrap();

    assert_eq!(payload.download_id, download_id);
    assert!(!payload.metadata_ready);
    assert!(payload.files.is_empty());
}

#[tokio::test]
async fn list_files_reports_metadata_ready_and_unavailable_while_downloading() {
    let test_app = spawn().await;
    let download_id = test_app.insert_pending_download().await;

    test_app
        .repo
        .mark_metadata_ready(
            download_id,
            "Example Book",
            4096,
            Utc::now(),
            &[NewDownloadFile {
                file_index: 0,
                relative_path: "Example Book/Chapter 01.mp3".to_string(),
                size_bytes: 4096,
                mime_type: "audio/mpeg".to_string(),
            }],
        )
        .await
        .unwrap();

    let response = test_app
        .app
        .clone()
        .oneshot(
            Request::get(format!("/api/v1/downloads/{download_id}/files"))
                .header("x-api-key", "test-api-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let payload: DownloadFilesResponse = serde_json::from_slice(&body).unwrap();

    assert_eq!(payload.download_id, download_id);
    assert!(payload.metadata_ready);
    assert_eq!(payload.files.len(), 1);
    assert_eq!(payload.files[0].path, "Example Book/Chapter 01.mp3");
    assert_eq!(payload.files[0].mime_type, "audio/mpeg");
    assert!(!payload.files[0].available);
}

#[tokio::test]
async fn list_files_reports_available_when_download_is_completed() {
    let test_app = spawn().await;
    let download_id = test_app.insert_pending_download().await;
    let completed_at = Utc::now();

    test_app
        .repo
        .mark_metadata_ready(
            download_id,
            "Example Book",
            4096,
            completed_at,
            &[NewDownloadFile {
                file_index: 0,
                relative_path: "Example Book/Chapter 01.mp3".to_string(),
                size_bytes: 4096,
                mime_type: "audio/mpeg".to_string(),
            }],
        )
        .await
        .unwrap();

    query("UPDATE downloads SET status = ?, completed_at = ?, updated_at = ? WHERE id = ?")
        .bind(DownloadStatus::Completed.as_db())
        .bind(completed_at)
        .bind(completed_at)
        .bind(download_id)
        .execute(&test_app.pool)
        .await
        .unwrap();

    let response = test_app
        .app
        .clone()
        .oneshot(
            Request::get(format!("/api/v1/downloads/{download_id}/files"))
                .header("x-api-key", "test-api-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);

    let body = response.into_body().collect().await.unwrap().to_bytes();
    let payload: DownloadFilesResponse = serde_json::from_slice(&body).unwrap();

    assert_eq!(payload.download_id, download_id);
    assert!(payload.metadata_ready);
    assert_eq!(payload.files.len(), 1);
    assert_eq!(payload.files[0].path, "Example Book/Chapter 01.mp3");
    assert_eq!(payload.files[0].mime_type, "audio/mpeg");
    assert!(payload.files[0].available);
}

#[tokio::test]
async fn completed_download_past_expiration_is_reported_consistently_as_expired() {
    let test_app = spawn().await;
    let download_id = test_app.insert_pending_download().await;
    let completed_at = Utc::now() - chrono::Duration::hours(2);
    let expires_at = Utc::now() - chrono::Duration::hours(1);

    test_app
        .repo
        .mark_metadata_ready(
            download_id,
            "Example Book",
            4096,
            completed_at,
            &[NewDownloadFile {
                file_index: 0,
                relative_path: "Example Book/Chapter 01.mp3".to_string(),
                size_bytes: 4096,
                mime_type: "audio/mpeg".to_string(),
            }],
        )
        .await
        .unwrap();

    query(
        "UPDATE downloads SET status = ?, completed_at = ?, expires_at = ?, updated_at = ? WHERE id = ?",
    )
    .bind(DownloadStatus::Completed.as_db())
    .bind(completed_at)
    .bind(expires_at)
    .bind(Utc::now())
    .bind(download_id)
    .execute(&test_app.pool)
    .await
    .unwrap();

    let get_response = test_app
        .app
        .clone()
        .oneshot(
            Request::get(format!("/api/v1/downloads/{download_id}"))
                .header("x-api-key", "test-api-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(get_response.status(), StatusCode::OK);

    let get_body = get_response.into_body().collect().await.unwrap().to_bytes();
    let get_payload: DownloadResponse = serde_json::from_slice(&get_body).unwrap();
    assert_eq!(get_payload.status, "expired");

    let list_response = test_app
        .app
        .clone()
        .oneshot(
            Request::get(format!("/api/v1/downloads/{download_id}/files"))
                .header("x-api-key", "test-api-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(list_response.status(), StatusCode::OK);

    let list_body = list_response
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    let list_payload: DownloadFilesResponse = serde_json::from_slice(&list_body).unwrap();
    assert_eq!(list_payload.files.len(), 1);
    assert!(!list_payload.files[0].available);
}

#[tokio::test]
async fn delete_download_removes_the_row_and_calls_cancel() {
    let test_app = spawn().await;
    let download_id = test_app.insert_pending_download().await;

    let response = test_app
        .app
        .clone()
        .oneshot(
            Request::delete(format!("/api/v1/downloads/{download_id}"))
                .header("x-api-key", "test-api-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::NO_CONTENT);
    assert!(test_app
        .repo
        .get_download(download_id)
        .await
        .unwrap()
        .is_none());
    assert!(test_app.scheduler.was_cancelled(download_id).await);
}

#[tokio::test]
async fn delete_download_rejects_tampered_storage_dirs_outside_storage_root() {
    let test_app = spawn().await;
    let download_id = test_app.insert_pending_download().await;
    let outside = tempdir().unwrap();
    let outside_path = outside.path().join("keep-me");

    tokio::fs::create_dir_all(&outside_path).await.unwrap();

    query("UPDATE downloads SET storage_dir = ? WHERE id = ?")
        .bind(outside_path.to_string_lossy().into_owned())
        .bind(download_id)
        .execute(&test_app.pool)
        .await
        .unwrap();

    let response = test_app
        .app
        .clone()
        .oneshot(
            Request::delete(format!("/api/v1/downloads/{download_id}"))
                .header("x-api-key", "test-api-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    assert!(test_app
        .repo
        .get_download(download_id)
        .await
        .unwrap()
        .is_some());
    assert!(tokio::fs::try_exists(&outside_path).await.unwrap());
}

async fn post_magnet(test_app: &support::TestApp, magnet: &str) -> (StatusCode, serde_json::Value) {
    let response = test_app
        .app
        .clone()
        .oneshot(
            Request::post("/api/v1/downloads")
                .header("x-api-key", "test-api-key")
                .header("content-type", "application/json")
                .body(Body::from(
                    serde_json::json!({ "magnet_uri": magnet }).to_string(),
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();

    (status, serde_json::from_slice(&body).unwrap_or_default())
}

#[tokio::test]
async fn magnet_with_select_only_is_rejected_with_400() {
    let test_app = spawn().await;

    let (status, _) = post_magnet(
        &test_app,
        "magnet:?xt=urn:btih:cccccccccccccccccccccccccccccccccccccccc&so=0-18446744073709551615",
    )
    .await;

    assert_eq!(status, StatusCode::BAD_REQUEST);
    assert_eq!(test_app.scheduler.scheduled_count().await, 0);
}

#[tokio::test]
async fn duplicate_info_hash_returns_existing_download_without_rescheduling() {
    let test_app = spawn().await;
    let magnet = "magnet:?xt=urn:btih:dddddddddddddddddddddddddddddddddddddddd";

    let (first_status, first) = post_magnet(&test_app, magnet).await;
    let (second_status, second) =
        post_magnet(&test_app, &format!("{magnet}&tr=udp://8.8.8.8:80")).await;

    assert_eq!(first_status, StatusCode::ACCEPTED);
    assert_eq!(second_status, StatusCode::OK);
    assert_eq!(first["id"], second["id"]);
    assert_eq!(test_app.scheduler.scheduled_count().await, 1);
}
