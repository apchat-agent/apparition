use appview_rs::{config::Config, init_tracing, router};
use std::{error::Error, net::SocketAddr};
use tokio::net::TcpListener;
use tracing::info;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = Config::load()?;
    init_tracing(&config.log_filter)?;
    let address: SocketAddr = config.listen.parse()?;
    let listener = TcpListener::bind(address).await?;
    info!(%address, data_dir = %config.data_dir, "starting appview-rs");
    axum::serve(listener, router())
        .with_graceful_shutdown(shutdown_signal())
        .await?;
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
