use appview_rs::{
    config::Config,
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
    let ingestor = Ingestor::new(storage, config.scope_mode);
    let consumer = JetstreamConsumer::new(ingestor, Duration::from_secs(1));
    let jetstream_urls = config.jetstream_urls.clone();
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let ingestion_task = tokio::spawn(async move {
        if let Err(error) = consumer.run(&jetstream_urls, shutdown_rx).await {
            error!(%error, "Jetstream consumer stopped");
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
    if tokio::time::timeout(Duration::from_secs(3), ingestion_task)
        .await
        .is_err()
    {
        info!("aborting delayed Jetstream shutdown");
    }
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
