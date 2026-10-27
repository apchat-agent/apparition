use appview_rs::{
    backfill::{
        AtriumRepoVerifier, BackfillWorker, HostRateLimiter, HttpRepoSource, RepoBackfiller,
    },
    config::Config,
    identity::{HttpIdentityBackend, IdentityResolver},
    ingest::{Ingestor, JetstreamConsumer, SqliteStorage},
    init_tracing, router,
};
use std::{error::Error, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};
use tokio::net::TcpListener;
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = Config::load()?;
    init_tracing(&config.log_filter)?;
    let address: SocketAddr = config.listen.parse()?;
    let storage = Arc::new(SqliteStorage::open(
        PathBuf::from(&config.data_dir).join("appview.db"),
    )?);
    let ingestor = Ingestor::new(storage.clone(), config.scope_mode);
    let consumer = JetstreamConsumer::new(ingestor, Duration::from_secs(1));
    let jetstream_urls = config.jetstream_urls.clone();
    let resolver = IdentityResolver::new(HttpIdentityBackend::default(), Duration::from_secs(1800));
    let backfiller = RepoBackfiller::new(
        storage.clone(),
        HttpRepoSource::default(),
        AtriumRepoVerifier,
        HostRateLimiter::new(Duration::from_millis(500)),
    );
    let worker = BackfillWorker::new(storage, resolver, backfiller, Duration::from_secs(5));

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let ingestion_shutdown = shutdown_rx.clone();
    let ingestion_task = tokio::spawn(async move {
        if let Err(error) = consumer.run(&jetstream_urls, ingestion_shutdown).await {
            error!(%error, "Jetstream consumer stopped");
        }
    });
    let backfill_task = tokio::spawn(async move {
        if let Err(error) = worker.run(shutdown_rx).await {
            error!(%error, "backfill worker stopped");
        }
    });

    let listener = TcpListener::bind(address).await?;
    info!(
        %address,
        data_dir = %config.data_dir,
        scope = ?config.scope_mode,
        "starting appview-rs"
    );
    axum::serve(listener, router())
        .with_graceful_shutdown(shutdown_signal())
        .await?;
    let _ = shutdown_tx.send(true);
    let _ = tokio::time::timeout(Duration::from_secs(3), async {
        let _ = ingestion_task.await;
        let _ = backfill_task.await;
    })
    .await;
    Ok(())
}

async fn shutdown_signal() {
    let ctrl_c = async {
        tokio::signal::ctrl_c()
            .await
            .expect("failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
            .expect("failed to install SIGTERM handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }
}
