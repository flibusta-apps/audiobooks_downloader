use std::sync::Arc;

use audiobooks_downloader::{
    app::build_app,
    config::Config,
    db::{connect, migrate, DownloadRepo},
    scheduler::NoopScheduler,
    state::AppState,
};
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use tempfile::tempdir;
use tower::ServiceExt;

#[tokio::test]
async fn healthz_is_public_and_download_creation_requires_api_key() {
    let temp_dir = tempdir().unwrap();
    let database_path = temp_dir.path().join("healthz-and-auth.sqlite");
    let database_url = format!("sqlite://{}?mode=rwc", database_path.display());

    let pool = connect(&database_url).await.unwrap();
    migrate(&pool).await.unwrap();

    let state = AppState {
        config: Arc::new(Config {
            bind_addr: "127.0.0.1:0".parse().unwrap(),
            database_url,
            storage_root: temp_dir.path().join("storage"),
            api_key: "test-api-key".to_string(),
            download_ttl_secs: 86_400,
            cleanup_interval_secs: 60,
            max_concurrent_downloads: 3,
            metadata_timeout_secs: 600,
            no_progress_timeout_secs: 600,
            max_torrent_bytes: None,
            tracker_allowlist: None,
        }),
        repo: DownloadRepo::new(pool),
        scheduler: Arc::new(NoopScheduler),
    };

    let app = build_app(state);

    let healthz_response = app
        .clone()
        .oneshot(Request::get("/healthz").body(Body::empty()).unwrap())
        .await
        .unwrap();

    assert_eq!(healthz_response.status(), StatusCode::OK);

    let create_response = app
        .clone()
        .oneshot(
            Request::post("/api/v1/downloads")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(create_response.status(), StatusCode::UNAUTHORIZED);

    let authorized_create_response = app
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

    assert_eq!(authorized_create_response.status(), StatusCode::ACCEPTED);

    let trailing_slash_create_response = app
        .oneshot(
            Request::post("/api/v1/downloads/")
                .header("x-api-key", "test-api-key")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"magnet_uri":"magnet:?xt=urn:btih:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(
        trailing_slash_create_response.status(),
        StatusCode::ACCEPTED
    );
}
