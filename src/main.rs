use std::{sync::Arc, time::Duration};

use audiobooks_downloader::{
    app::build_app,
    config::Config,
    db::{connect, migrate, DownloadRepo},
    maintenance::{cleanup_stale_failed_downloads, expire_due_downloads, run_startup_recovery},
    scheduler::DownloadScheduler,
    state::AppState,
    storage::acquire_instance_lock,
    torrent::LibrqbitScheduler,
};
use axum::ServiceExt;
use dotenvy::dotenv;
use tracing::{error, info};
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
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let config = Arc::new(Config::from_env()?);

    // Held until exit; guarantees a single instance owns storage and recovery.
    let _instance_lock = acquire_instance_lock(&config.storage_root)?;

    let pool = connect(&config.database_url).await?;

    migrate(&pool).await?;

    let repo = DownloadRepo::new(pool);
    run_startup_recovery(&repo, &config.storage_root).await?;

    let cleaner_repo = repo.clone();
    let cleaner_storage_root = config.storage_root.clone();
    let cleanup_interval_secs = config.cleanup_interval_secs;
    let failed_ttl_secs = config.download_ttl_secs;
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_secs(cleanup_interval_secs));
        loop {
            interval.tick().await;
            if let Err(error) = expire_due_downloads(&cleaner_repo, &cleaner_storage_root).await {
                error!(%error, "cleanup task failed");
            }
            if let Err(error) = cleanup_stale_failed_downloads(
                &cleaner_repo,
                &cleaner_storage_root,
                failed_ttl_secs,
            )
            .await
            {
                error!(%error, "failed-download cleanup task failed");
            }
        }
    });

    let scheduler = Arc::new(LibrqbitScheduler::new(repo.clone(), &config).await?);
    let state = AppState {
        config: Arc::clone(&config),
        repo,
        scheduler: scheduler.clone(),
    };

    let listener = tokio::net::TcpListener::bind(config.bind_addr).await?;
    info!(addr = %listener.local_addr()?, "listening");
    axum::serve(listener, build_app(state).into_make_service())
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    info!("shutting down");
    scheduler.shutdown().await;

    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        let _ = tokio::signal::ctrl_c().await;
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(_) => std::future::pending::<()>().await,
        }
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {}
        _ = terminate => {}
    }
}
