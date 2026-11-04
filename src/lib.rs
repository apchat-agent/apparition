pub mod api;
pub mod backfill;
pub mod config;
pub mod feeds;
pub mod http_security;
pub mod identity;
pub mod ingest;
pub mod jwt;
pub mod link_card;
pub mod net;
pub mod oauth;
pub mod profile_counts;
pub mod recent_posts;
pub mod search;
pub mod timing_console;

use axum::{Json, Router, extract::State, routing::get};
use serde::Serialize;
use tracing_subscriber::{EnvFilter, fmt, layer::SubscriberExt, util::SubscriberInitExt};

pub fn router() -> Router {
    let oauth = oauth::OAuthPublic::new("https://appview.example")
        .expect("default OAuth public origin is valid");
    router_with_oauth(oauth)
}

pub fn router_with_oauth(oauth: oauth::OAuthPublic) -> Router {
    let public_https = oauth.public_url().starts_with("https://");
    http_security::protect_with_picture_body_limit(
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
    router_with_oauth_flow_and_shutdown(oauth, flow, storage, oauth::WebShutdown::new())
}

pub fn router_with_oauth_flow_and_shutdown<F, S>(
    oauth: oauth::OAuthPublic,
    flow: F,
    storage: std::sync::Arc<S>,
    shutdown: oauth::WebShutdown,
) -> Router
where
    F: oauth::OAuthFlow,
    S: ingest::Storage + 'static,
{
    let public_https = oauth.public_url().starts_with("https://");
    http_security::protect_with_picture_body_limit(
        Router::new()
            .route("/xrpc/_health", get(health))
            .route("/healthz", get(health))
            .merge(oauth.router_with_flow_and_shutdown(flow, storage, shutdown)),
        public_https,
    )
}

#[derive(Serialize)]
pub struct HealthResponse {
    status: &'static str,
}

#[derive(Serialize)]
pub struct JetstreamHealthResponse {
    status: &'static str,
    jetstream_state: String,
    supervisor_restarts: u64,
    last_event_us: Option<u64>,
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse { status: "ok" })
}

pub async fn health_with_status(
    State(status): State<std::sync::Arc<ingest::JetstreamStatus>>,
) -> Json<JetstreamHealthResponse> {
    let last_event_us = status
        .last_event_us
        .load(std::sync::atomic::Ordering::Relaxed);
    Json(JetstreamHealthResponse {
        status: "ok",
        jetstream_state: status.state_name().to_owned(),
        supervisor_restarts: status.restarts.load(std::sync::atomic::Ordering::Relaxed),
        last_event_us: if last_event_us == 0 {
            None
        } else {
            Some(last_event_us)
        },
    })
}

#[derive(Serialize)]
pub struct JetstreamStorageHealthResponse {
    status: &'static str,
    jetstream_state: String,
    supervisor_restarts: u64,
    last_event_us: Option<u64>,
    post_sort_backfill_remaining: Option<u64>,
    post_sort_backfill_failures: u64,
}

pub struct HealthState<S: ingest::Storage> {
    pub storage: std::sync::Arc<S>,
    pub status: std::sync::Arc<ingest::JetstreamStatus>,
}

impl<S: ingest::Storage> Clone for HealthState<S> {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            status: self.status.clone(),
        }
    }
}

pub async fn health_with_storage_and_status<S: ingest::Storage>(
    State(state): State<HealthState<S>>,
) -> Json<JetstreamStorageHealthResponse> {
    let last_event_us = state
        .status
        .last_event_us
        .load(std::sync::atomic::Ordering::Relaxed);
    let remaining = state.storage.post_sort_backfill_remaining().ok();
    let failures = state.storage.post_sort_backfill_failures();
    Json(JetstreamStorageHealthResponse {
        status: "ok",
        jetstream_state: state.status.state_name().to_owned(),
        supervisor_restarts: state
            .status
            .restarts
            .load(std::sync::atomic::Ordering::Relaxed),
        last_event_us: if last_event_us == 0 {
            None
        } else {
            Some(last_event_us)
        },
        post_sort_backfill_remaining: remaining,
        post_sort_backfill_failures: failures,
    })
}

pub fn init_tracing(filter: &str) -> Result<(), tracing_subscriber::util::TryInitError> {
    let env_filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(filter));
    tracing_subscriber::registry()
        .with(env_filter)
        .with(fmt::layer().json())
        .try_init()
}
