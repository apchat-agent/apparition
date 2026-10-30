use appview_rs::{
    api::api_router_with_https,
    backfill::{
        AtriumRepoVerifier, BackfillWorker, HostRateLimiter, HttpRepoSource, RepoBackfiller,
    },
    config::{Config, OAuthMode, identity_cache_path},
    identity::{HandleRefreshWorker, HttpIdentityBackend, IdentityResolver, SqliteIdentityCache},
    ingest::{Ingestor, JetstreamConsumer, SqliteStorage},
    init_tracing,
    jwt::ServiceJwtVerifier,
    oauth::{OAuthPublic, ProfileRefreshWorker, SqliteOAuthStore, WebShutdown},
    router_with_oauth_flow_and_shutdown,
};
use std::{
    error::Error, future::IntoFuture, net::SocketAddr, path::PathBuf, sync::Arc, time::Duration,
};
use tokio::net::TcpListener;
use tracing::{error, info};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let config = Config::load()?;
    init_tracing(&config.log_filter)?;
    let listen_address: SocketAddr = config.listen.parse()?;
    let listener = TcpListener::bind(listen_address).await?;
    let address = listener.local_addr()?;
    let (database_path, oauth_database_path) = appview_rs::config::database_paths(&config.data_dir);
    let storage = Arc::new(SqliteStorage::open(&database_path)?);
    let identity_cache = Arc::new(SqliteIdentityCache::open(identity_cache_path(
        &config.data_dir,
    ))?);
    let oauth_store = SqliteOAuthStore::open(&oauth_database_path)?;
    let oauth_key_path = PathBuf::from(&config.data_dir).join("oauth-client-key.json");
    let oauth = match config.oauth_mode {
        OAuthMode::Confidential => OAuthPublic::load_or_create(&config.public_url, oauth_key_path)?,
        OAuthMode::Loopback => OAuthPublic::load_or_create_loopback(address, oauth_key_path)?,
    };
    let oauth_identity_resolver = Arc::new(
        IdentityResolver::new(HttpIdentityBackend::default(), Duration::from_secs(1800))
            .with_persistent_cache(identity_cache.clone()),
    );
    let oauth_flow = oauth
        .atrium_flow(oauth_identity_resolver, oauth_store)
        .map_err(std::io::Error::other)?;
    let profile_refresh_worker =
        ProfileRefreshWorker::new(oauth_flow.clone(), storage.clone(), Duration::from_secs(2));
    let ingestor = Ingestor::new(storage.clone(), config.scope_mode)
        .with_identity_cache(identity_cache.clone());
    let consumer = JetstreamConsumer::new(ingestor, Duration::from_secs(1))
        .with_idle_timeout(Duration::from_secs(config.jetstream_idle_timeout_secs))
        .with_event_timeout(Duration::from_secs(config.jetstream_event_timeout_secs))
        .with_stale_cursor_threshold(Duration::from_secs(
            config.jetstream_stale_cursor_threshold_secs,
        ));
    let jetstream_urls = config.jetstream_urls.clone();
    let resolver = IdentityResolver::new(HttpIdentityBackend::default(), Duration::from_secs(1800))
        .with_persistent_cache(identity_cache.clone());
    let jwt_resolver = Arc::new(
        IdentityResolver::new(HttpIdentityBackend::default(), Duration::from_secs(1800))
            .with_persistent_cache(identity_cache.clone()),
    );
    let jwt = Arc::new(ServiceJwtVerifier::new(
        jwt_resolver,
        config.appview_did.clone(),
    ));
    let web_shutdown = WebShutdown::new();
    let app_router = router_with_oauth_flow_and_shutdown(
        oauth,
        oauth_flow.clone(),
        storage.clone(),
        web_shutdown.clone(),
    )
    .merge(api_router_with_https(
        storage.clone(),
        jwt,
        config.appview_did.clone(),
        config.public_url.starts_with("https://"),
    ));
    let backfiller = RepoBackfiller::new(
        storage.clone(),
        HttpRepoSource::default(),
        AtriumRepoVerifier,
        HostRateLimiter::new(Duration::from_millis(500)),
    )
    .with_records_per_second(config.backfill_records_per_second);
    let worker = BackfillWorker::new(
        storage.clone(),
        resolver,
        backfiller,
        Duration::from_secs(5),
    );
    let handle_refresh_worker = HandleRefreshWorker::new(
        storage.clone(),
        IdentityResolver::new(HttpIdentityBackend::default(), Duration::from_secs(1800))
            .with_persistent_cache(identity_cache),
        Duration::from_secs(1800),
        Duration::from_secs(30),
    );

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let ingestion_shutdown = shutdown_rx.clone();
    let ingestion_task = tokio::spawn(async move {
        if let Err(error) = consumer.run(&jetstream_urls, ingestion_shutdown).await {
            error!(%error, "Jetstream consumer stopped");
        }
    });
    let backfill_shutdown = shutdown_rx.clone();
    // Backfill can be paused independently when its storage or network load is too
    // high; live ingest keeps running.
    let backfill_paused = std::env::var_os("APPVIEW_BACKFILL_PAUSED").is_some();
    if backfill_paused {
        info!("repository backfill paused by APPVIEW_BACKFILL_PAUSED");
    } else if !config.backfill_enabled {
        info!("repository backfill disabled; set APPVIEW_BACKFILL_ENABLED=true to enable it");
    }
    let backfill_enabled = config.backfill_enabled;
    let backfill_task = tokio::spawn(async move {
        if backfill_paused || !backfill_enabled {
            return;
        }
        if let Err(error) = worker.run(backfill_shutdown).await {
            error!(%error, "backfill worker stopped");
        }
    });
    let handle_refresh_shutdown = shutdown_rx.clone();
    let handle_refresh_task = tokio::spawn(async move {
        if let Err(error) = handle_refresh_worker.run(handle_refresh_shutdown).await {
            error!(%error, "verified handle refresh worker stopped");
        }
    });
    let profile_refresh_shutdown = shutdown_rx.clone();
    let profile_refresh_task = tokio::spawn(async move {
        if let Err(error) = profile_refresh_worker.run(profile_refresh_shutdown).await {
            error!(%error, "author profile refresh worker stopped");
        }
    });

    info!(
        %address,
        data_dir = %config.data_dir,
        scope = ?config.scope_mode,
        oauth_mode = ?config.oauth_mode,
        "starting appview-rs"
    );
    if std::env::args().any(|arg| arg == "--dev") {
        let login_url = match config.oauth_mode {
            OAuthMode::Confidential => format!("{}/login", config.public_url.trim_end_matches('/')),
            OAuthMode::Loopback => format!("http://{address}/login"),
        };
        info!(%login_url, "development login URL");
    }
    let signal_web_shutdown = web_shutdown.clone();
    let signal_worker_shutdown = shutdown_tx.clone();
    let (shutdown_started_tx, mut shutdown_started_rx) = tokio::sync::oneshot::channel();
    let mut server = Box::pin(
        axum::serve(
            listener,
            app_router.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(async move {
            shutdown_signal().await;
            signal_web_shutdown.shutdown();
            let _ = signal_worker_shutdown.send(true);
            let _ = shutdown_started_tx.send(());
        })
        .into_future(),
    );
    let server_finished = tokio::select! {
        result = &mut server => {
            result?;
            true
        }
        _ = &mut shutdown_started_rx => false,
    };
    let shutdown_result = tokio::time::timeout(Duration::from_secs(5), async {
        if !server_finished {
            (&mut server).await?;
        }
        let _ = ingestion_task.await;
        let _ = backfill_task.await;
        let _ = handle_refresh_task.await;
        let _ = profile_refresh_task.await;
        Ok::<(), std::io::Error>(())
    })
    .await;
    match shutdown_result {
        Ok(result) => result?,
        Err(_) => {
            let connections_cut = web_shutdown.active_notification_streams();
            error!(
                connections_cut,
                "shutdown deadline reached; forcing process exit"
            );
        }
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
