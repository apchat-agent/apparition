pub mod api;
pub mod backfill;
pub mod config;
pub mod http_security;
pub mod identity;
pub mod ingest;
pub mod jwt;
pub mod net;
pub mod oauth;

use axum::{Json, Router, routing::get};
use serde::Serialize;
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

pub fn router() -> Router {
    let oauth = oauth::OAuthPublic::new("https://appview.example")
        .expect("default OAuth public origin is valid");
    router_with_oauth(oauth)
}

pub fn router_with_oauth(oauth: oauth::OAuthPublic) -> Router {
    let public_https = oauth.public_url().starts_with("https://");
    http_security::protect(
        Router::new()
            .route("/xrpc/_health", get(health))
            .route("/healthz", get(health))
            .merge(oauth.router()),
        public_https,
    )
}

pub fn router_with_oauth_flow<F, S>(
    oauth: oauth::OAuthPublic,
    flow: F,
    storage: std::sync::Arc<S>,
) -> Router
where
    F: oauth::OAuthFlow,
    S: ingest::Storage + 'static,
{
    let public_https = oauth.public_url().starts_with("https://");
    http_security::protect(
        Router::new()
            .route("/xrpc/_health", get(health))
            .route("/healthz", get(health))
            .merge(oauth.router_with_flow(flow, storage)),
        public_https,
    )
}

#[derive(Serialize)]
struct HealthResponse {
    status: &'static str,
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

pub fn init_tracing(filter: &str) -> Result<(), tracing_subscriber::util::TryInitError> {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(filter));
    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt::layer().json())
        .try_init()
}
