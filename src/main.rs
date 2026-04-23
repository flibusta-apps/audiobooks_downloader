use std::{sync::Arc, time::Duration};

use axum::ServiceExt;
use audiobooks_downloader::{
    app::build_app,
    config::Config,
    db::{connect, migrate, DownloadRepo},
    maintenance::{expire_due_downloads, run_startup_recovery},
    state::AppState,
    torrent::LibrqbitScheduler,
};
use dotenvy::dotenv;
use tracing_subscriber::{fmt, EnvFilter};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    match dotenv() {
        Ok(_) => {}
        Err(error) if error.not_found() => {}
        Err(error) => return Err(error.into()),
    }

    fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("warn")),
        )
        .init();

    let config = Arc::new(Config::from_env()?);
    let pool = connect(&config.database_url).await?;

    migrate(&pool).await?;

    let repo = DownloadRepo::new(pool);
    run_startup_recovery(&repo, &config.storage_root).await?;

    let cleaner_repo = repo.clone();
    let cleaner_storage_root = config.storage_root.clone();
    let cleanup_interval_secs = config.cleanup_interval_secs;
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(cleanup_interval_secs));
        loop {
            interval.tick().await;
            if let Err(error) = expire_due_downloads(&cleaner_repo, &cleaner_storage_root).await {
                eprintln!("cleanup task failed: {error}");
            }
        }
    });

    let state = AppState {
        config: Arc::clone(&config),
        repo: repo.clone(),
        scheduler: Arc::new(LibrqbitScheduler::new(repo, config.download_ttl_secs)),
    };

    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    axum::serve(listener, build_app(state).into_make_service()).await?;

    Ok(())
}
