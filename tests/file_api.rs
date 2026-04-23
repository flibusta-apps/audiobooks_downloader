mod support;

use audiobooks_downloader::domain::DownloadStatus;
use axum::{
    body::Body,
    http::{header, Request, StatusCode},
};
use chrono::{Duration, Utc};
use http_body_util::BodyExt;
use serde_json::Value;
use sqlx::query;
use tower::ServiceExt;

use self::support::spawn;

#[tokio::test]
async fn stream_endpoint_supports_byte_ranges_for_completed_files() {
    let test_app = spawn().await;
    let download_id = test_app
        .insert_download_with_file(
            DownloadStatus::Completed,
            "Example Book/Chapter 01.mp3",
            b"abcdefghij",
            Some(Utc::now() + Duration::hours(1)),
        )
        .await;

    let response = test_app
        .app
        .clone()
        .oneshot(
            Request::get(format!("/api/v1/downloads/{download_id}/files/0/stream"))
                .header("x-api-key", "test-api-key")
                .header(header::RANGE, "bytes=2-5")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::PARTIAL_CONTENT);
    assert_eq!(response.headers()[header::CONTENT_RANGE], "bytes 2-5/10");

    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(&body[..], b"cdef");
}

#[tokio::test]
async fn file_endpoint_returns_conflict_while_download_is_not_finished() {
    let test_app = spawn().await;
    let download_id = test_app
        .insert_download_with_file(
            DownloadStatus::Downloading,
            "Example Book/Chapter 01.mp3",
            b"abcdefghij",
            None,
        )
        .await;

    let response = test_app
        .app
        .clone()
        .oneshot(
            Request::get(format!("/api/v1/downloads/{download_id}/files/0"))
                .header("x-api-key", "test-api-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::CONFLICT);
}

#[tokio::test]
async fn file_endpoint_returns_gone_for_expired_downloads() {
    let test_app = spawn().await;
    let download_id = test_app
        .insert_download_with_file(
            DownloadStatus::Expired,
            "Example Book/Chapter 01.mp3",
            b"abcdefghij",
            Some(Utc::now() - Duration::hours(1)),
        )
        .await;

    let response = test_app
        .app
        .clone()
        .oneshot(
            Request::get(format!("/api/v1/downloads/{download_id}/files/0"))
                .header("x-api-key", "test-api-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::GONE);
}

#[tokio::test]
async fn file_endpoint_returns_gone_for_completed_downloads_past_expiration() {
    let test_app = spawn().await;
    let download_id = test_app
        .insert_download_with_file(
            DownloadStatus::Completed,
            "Example Book/Chapter 01.mp3",
            b"abcdefghij",
            Some(Utc::now() - Duration::hours(1)),
        )
        .await;

    let response = test_app
        .app
        .clone()
        .oneshot(
            Request::get(format!("/api/v1/downloads/{download_id}/files/0"))
                .header("x-api-key", "test-api-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::GONE);
}

#[tokio::test]
async fn file_endpoint_sanitizes_content_disposition_filename() {
    let test_app = spawn().await;
    let download_id = test_app
        .insert_download_with_file(
            DownloadStatus::Completed,
            "Example Book/ch\"apt\u{e9}r 01.mp3",
            b"abcdefghij",
            Some(Utc::now() + Duration::hours(1)),
        )
        .await;

    let response = test_app
        .app
        .clone()
        .oneshot(
            Request::get(format!("/api/v1/downloads/{download_id}/files/0"))
                .header("x-api-key", "test-api-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()[header::CONTENT_DISPOSITION],
        "attachment; filename=\"ch_apt_r 01.mp3\""
    );
}

#[tokio::test]
async fn file_endpoint_uses_on_disk_content_length() {
    let test_app = spawn().await;
    let download_id = test_app
        .insert_download_with_file(
            DownloadStatus::Completed,
            "Example Book/Chapter 01.mp3",
            b"abcdefghij",
            Some(Utc::now() + Duration::hours(1)),
        )
        .await;

    query("UPDATE download_files SET size_bytes = ? WHERE download_id = ? AND file_index = ?")
        .bind(999_i64)
        .bind(download_id)
        .bind(0_i64)
        .execute(&test_app.pool)
        .await
        .unwrap();

    let response = test_app
        .app
        .clone()
        .oneshot(
            Request::get(format!("/api/v1/downloads/{download_id}/files/0"))
                .header("x-api-key", "test-api-key")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()[header::CONTENT_LENGTH], "10");
}

#[tokio::test]
async fn missing_storage_maps_to_storage_error_for_file_and_stream_routes() {
    let test_app = spawn().await;
    let download_id = test_app
        .insert_download_with_file(
            DownloadStatus::Completed,
            "Example Book/Chapter 01.mp3",
            b"abcdefghij",
            Some(Utc::now() + Duration::hours(1)),
        )
        .await;

    let storage_dir = test_app.storage_root.join(download_id.to_string());
    tokio::fs::remove_file(storage_dir.join("Example Book/Chapter 01.mp3"))
        .await
        .unwrap();

    for path in [
        format!("/api/v1/downloads/{download_id}/files/0"),
        format!("/api/v1/downloads/{download_id}/files/0/stream"),
    ] {
        let response = test_app
            .app
            .clone()
            .oneshot(
                Request::get(path)
                    .header("x-api-key", "test-api-key")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);

        let body = response.into_body().collect().await.unwrap().to_bytes();
        let payload: Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(payload["error"]["code"], "storage_error");
    }
}
