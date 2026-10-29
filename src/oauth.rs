use crate::identity::{HttpIdentityBackend, IdentityResolver};
use crate::{
    api::profile_view_from_storage,
    feeds::{
        FeedClient, FeedSkeletonPage, FeedTimeline, FeedTimelineTiming, ResolvedFeed,
        SavedFeedPreference, parse_saved_feed_preferences,
    },
    ingest::{
        IndexedNotification, IndexedRecord, MemoryStorage, NotificationCursor, ProfileIndexEntry,
        RecordCursor, Storage, StorageError,
    },
    link_card::{LinkCard, LinkCardFetcher, LinkThumbnail},
    net::SafeHttpClient,
};
use atrium_api::{
    agent::{Agent, SessionManager},
    types::string::{AtIdentifier, Datetime, Did},
};
use atrium_common::store::Store;
use atrium_identity::{
    did::{CommonDidResolver, CommonDidResolverConfig, DEFAULT_PLC_DIRECTORY_URL},
    handle::{AtprotoHandleResolver, AtprotoHandleResolverConfig, DnsTxtResolver},
};
use atrium_oauth::{
    AtprotoClientMetadata, AtprotoLocalhostClientMetadata, AuthMethod, AuthorizeOptions, GrantType,
    KnownScope, OAuthClient, OAuthClientConfig, OAuthResolverConfig, Scope,
    store::{
        session::{Session, SessionStore},
        state::{InternalStateData, StateStore},
    },
};
use atrium_xrpc::error::XrpcErrorKind;
use atrium_xrpc::{
    Error as XrpcClientError, InputDataOrBytes, OutputDataOrBytes, XrpcClient, XrpcRequest,
};
use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{
        DefaultBodyLimit, Form, FromRequest, Multipart, Path as AxumPath, Query, Request, State,
    },
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{
        Html, IntoResponse, Redirect, Response,
        sse::{Event, Sse},
    },
    routing::{get, post},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use futures_util::{FutureExt, StreamExt, stream};
use hkdf::Hkdf;
use hmac::{Hmac, Mac};
use jose_jwk::{Ec, EcCurves, Jwk, Key, Parameters};
use p256::{SecretKey, elliptic_curve::sec1::ToEncodedPoint};
use rand::{RngCore, thread_rng};
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::Sha256;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::{
    collections::{HashMap, HashSet},
    fs,
    future::Future,
    io::Write,
    net::SocketAddr,
    path::{Component, Path, PathBuf},
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
};
use thiserror::Error;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::io::AsyncWriteExt;

const METADATA_PATH: &str = "/.well-known/oauth-client-metadata.json";
const CALLBACK_PATH: &str = "/oauth/callback";
const SESSION_COOKIE: &str = "appview_session";
const LOGIN_COOKIE: &str = "appview_login";
const SESSION_TTL_SECONDS: u64 = 30 * 24 * 60 * 60;
const LOGIN_COOKIE_TTL_SECONDS: u64 = 10 * 60;
const MAX_RENDERED_REMOTE_CHARS: usize = 4096;
const PROFILE_MAX_AGE: Duration = Duration::from_secs(24 * 60 * 60);
const PROFILE_NEGATIVE_TTL: Duration = Duration::from_secs(60 * 60);
const PAGE_PROFILE_BUDGET: Duration = Duration::from_millis(1500);
const PAGE_PROFILE_CONCURRENCY: usize = 8;
const PAGE_QUOTE_CONCURRENCY: usize = 16;
const WEB_STYLESHEET_URL: &str = "/static/app.css?v=m13";
const WEB_SCRIPT_URL: &str = "/static/app.js?v=js11";
const HLS_SCRIPT_URL: &str = "/static/vendor/hls.min.js?v=1.7.3";
const ALT_MODEL_DOWNLOAD_BYTES: u64 = 264_713_119;
const IMMUTABLE_CACHE: &str = "public, max-age=31536000, immutable";
const SAVED_FEED_CACHE_PREFIX: &str = "saved-feed-preferences:";
const VIDEO_SERVICE_DID: &str = "did:web:video.bsky.app";
const VIDEO_SERVICE_URL: &str = "https://video.bsky.app";
const REQUESTED_SCOPE: &str = "atproto repo:app.bsky.feed.post?action=create repo:app.bsky.feed.like?action=create&action=delete repo:app.bsky.feed.repost?action=create&action=delete blob:image/* rpc:app.bsky.actor.getPreferences?aud=* rpc:app.bsky.feed.getFeedSkeleton?aud=* blob:video/* rpc:app.bsky.video.getUploadLimits?aud=did:web:video.bsky.app rpc:app.bsky.video.uploadVideo?aud=did:web:video.bsky.app rpc:app.bsky.video.getJobStatus?aud=did:web:video.bsky.app";
const BLOB_SCOPE: &str = "blob:image/*";
const VIDEO_BLOB_SCOPE: &str = "blob:video/*";
const VIDEO_SCOPES: [&str; 4] = [
    VIDEO_BLOB_SCOPE,
    "rpc:app.bsky.video.getUploadLimits?aud=did:web:video.bsky.app",
    "rpc:app.bsky.video.uploadVideo?aud=did:web:video.bsky.app",
    "rpc:app.bsky.video.getJobStatus?aud=did:web:video.bsky.app",
];
const WRITE_SCOPES: [&str; 3] = [
    "repo:app.bsky.feed.post?action=create",
    "repo:app.bsky.feed.like?action=create&action=delete",
    "repo:app.bsky.feed.repost?action=create&action=delete",
];
const FEED_SCOPES: [&str; 2] = [
    "rpc:app.bsky.actor.getPreferences?aud=*",
    "rpc:app.bsky.feed.getFeedSkeleton?aud=*",
];
const MAX_VIDEO_BYTES: u64 = 300_000_000;
const MAX_VIDEO_DURATION_SECONDS: f64 = 600.0;
const MAX_VIDEO_ALT_CHARS: usize = 2000;
const MAX_VIDEO_REQUEST_BODY_BYTES: usize = MAX_VIDEO_BYTES as usize + 1024 * 1024;
const VIDEO_JOB_POLL_INTERVAL: Duration = Duration::from_secs(2);
const VIDEO_JOB_MAX_POLLS: usize = 300;
type HmacSha256 = Hmac<Sha256>;

fn saved_feed_cache_key(did: &str) -> String {
    format!("{SAVED_FEED_CACHE_PREFIX}{did}")
}

fn unix_time_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or_default()
}

#[derive(Debug, Error)]
pub enum OAuthConfigError {
    #[error("OAuth client key I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("OAuth client key is invalid: {0}")]
    Jwk(#[from] serde_json::Error),
    #[error("public URL must be an origin URL without credentials, path, query, or fragment")]
    InvalidPublicUrl,
    #[error("OAuth private key file does not contain a P-256 signing key")]
    InvalidPrivateKey,
    #[error("loopback OAuth requires a loopback listen address")]
    InvalidLoopbackAddress,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OAuthClientMode {
    Confidential,
    Loopback,
}

#[derive(Clone)]
pub struct OAuthPublic {
    public_url: String,
    private_key: Jwk,
    metadata: Value,
    mode: OAuthClientMode,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SessionPermissions {
    write: bool,
    feed: bool,
    blob: bool,
    video: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OAuthCallback {
    pub code: String,
    pub state: String,
    pub issuer: Option<String>,
    pub scope: Option<String>,
    pub browser_binding: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OAuthAccount {
    pub did: String,
    pub verified_handle: Option<String>,
    pub granted_scope: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PdsCreatedRecord {
    pub uri: String,
    pub cid: String,
    pub record: Value,
    pub time_us: u64,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostStrongRef {
    pub uri: String,
    pub cid: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PostImage {
    pub bytes: Vec<u8>,
    pub mime_type: String,
    pub alt: String,
    pub width: u32,
    pub height: u32,
}

#[derive(Debug)]
pub struct PostVideo {
    pub path: PathBuf,
    pub mime_type: String,
    pub alt: String,
    pub width: u32,
    pub height: u32,
    pub duration_seconds: f64,
    pub size_bytes: u64,
    pub filename: String,
}

impl Drop for PostVideo {
    fn drop(&mut self) {
        if let Err(error) = fs::remove_file(&self.path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(path = %self.path.display(), %error, "could not remove temporary video upload");
        }
    }
}

fn validate_video_properties(
    size_bytes: u64,
    duration_seconds: f64,
    mime_type: &str,
) -> Result<(), String> {
    if size_bytes == 0 || size_bytes > MAX_VIDEO_BYTES {
        return Err("Videos must be no larger than 300,000,000 bytes.".to_owned());
    }
    if !duration_seconds.is_finite()
        || duration_seconds <= 0.0
        || duration_seconds > MAX_VIDEO_DURATION_SECONDS
    {
        return Err("Videos can be up to 10 minutes long.".to_owned());
    }
    if !matches!(mime_type, "video/mp4" | "video/webm" | "video/quicktime") {
        return Err("Choose an MP4, WebM, or MOV video.".to_owned());
    }
    Ok(())
}

fn validate_video_upload_limits(limits: &Value, upload_size: u64) -> Result<(), String> {
    if limits.get("canUpload").and_then(Value::as_bool) != Some(true) {
        return Err(limits
            .get("message")
            .and_then(Value::as_str)
            .filter(|message| !message.trim().is_empty())
            .unwrap_or("This account cannot upload videos right now.")
            .to_owned());
    }
    if limits
        .get("remainingDailyVideos")
        .and_then(Value::as_i64)
        .is_some_and(|remaining| remaining <= 0)
    {
        return Err("The daily video upload limit has been reached.".to_owned());
    }
    if limits
        .get("remainingDailyBytes")
        .and_then(Value::as_u64)
        .is_some_and(|remaining| remaining < upload_size)
    {
        return Err("The daily video upload data limit has been reached.".to_owned());
    }
    Ok(())
}

fn build_video_post_record(
    text: &str,
    blob: Value,
    alt: &str,
    width: u32,
    height: u32,
    reply: Option<Value>,
    quote: Option<PostStrongRef>,
) -> Value {
    let video_embed = json!({
        "$type":"app.bsky.embed.video",
        "video":blob,
        "alt":alt,
        "aspectRatio":{"width":width,"height":height}
    });
    let embed = if let Some(quote) = quote {
        json!({
            "$type":"app.bsky.embed.recordWithMedia",
            "record":{"$type":"app.bsky.embed.record","record":quote},
            "media":video_embed
        })
    } else {
        video_embed
    };
    let mut record = json!({
        "$type":"app.bsky.feed.post",
        "text":text,
        "createdAt":Datetime::now(),
        "embed":embed
    });
    if let Some(reply) = reply {
        record["reply"] = reply;
    }
    record
}

fn job_status_value(response: &Value) -> &Value {
    response.get("jobStatus").unwrap_or(response)
}

async fn poll_video_job<F>(
    job_id: &str,
    interval: Duration,
    max_polls: usize,
    mut fetch_status: F,
) -> Result<Value, String>
where
    F: FnMut(String) -> Pin<Box<dyn Future<Output = Result<Value, String>> + Send>>,
{
    for attempt in 0..max_polls {
        let response = fetch_status(job_id.to_owned()).await?;
        let status = job_status_value(&response);
        match status
            .get("state")
            .and_then(Value::as_str)
            .unwrap_or_default()
        {
            "JOB_STATE_COMPLETED" => {
                return status
                    .get("blob")
                    .cloned()
                    .ok_or_else(|| "video service completed without returning a blob".to_owned());
            }
            "JOB_STATE_FAILED" => {
                let message = status
                    .get("message")
                    .or_else(|| status.get("error"))
                    .and_then(Value::as_str)
                    .filter(|message| !message.trim().is_empty())
                    .unwrap_or("Video processing failed.");
                return Err(message.to_owned());
            }
            _ if attempt + 1 < max_polls => tokio::time::sleep(interval).await,
            _ => return Err("Video processing did not finish in time.".to_owned()),
        }
    }
    Err("Video processing did not finish in time.".to_owned())
}

async fn read_video_service_response(response: reqwest::Response) -> Result<Value, String> {
    const MAX_RESPONSE_BYTES: usize = 64 * 1024;
    let status = response.status();
    let mut response = response;
    let mut bytes = Vec::new();
    while let Some(chunk) = response
        .chunk()
        .await
        .map_err(|error| format!("video service response could not be read: {error}"))?
    {
        if bytes.len().saturating_add(chunk.len()) > MAX_RESPONSE_BYTES {
            return Err("video service returned an unexpectedly large response".to_owned());
        }
        bytes.extend_from_slice(&chunk);
    }
    let value: Value = serde_json::from_slice(&bytes)
        .map_err(|_| format!("video service returned invalid JSON ({status})"))?;
    let completed_conflict = status == StatusCode::CONFLICT
        && job_status_value(&value)
            .get("state")
            .and_then(Value::as_str)
            == Some("JOB_STATE_COMPLETED");
    if !status.is_success() && !completed_conflict {
        let message = value
            .get("message")
            .or_else(|| value.get("error"))
            .and_then(Value::as_str)
            .unwrap_or("request failed");
        return Err(format!(
            "video service request failed ({status}): {message}"
        ));
    }
    Ok(value)
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PostExternal {
    pub uri: String,
    pub title: String,
    pub description: String,
    pub thumbnail: Option<LinkThumbnail>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FetchedAuthorProfile {
    pub profile: Option<IndexedRecord>,
    pub verified_handle: Option<String>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PinnedFeedTabs {
    pub feeds: Vec<ResolvedFeed>,
    pub preferences_ms: u64,
    pub tabs_ms: u64,
}

pub trait OAuthFlow: Clone + Send + Sync + 'static {
    fn authorize<'a>(
        &'a self,
        login: &'a str,
        browser_binding: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<String, String>> + Send + 'a>>;
    fn callback<'a>(
        &'a self,
        params: OAuthCallback,
    ) -> Pin<Box<dyn Future<Output = Result<OAuthAccount, String>> + Send + 'a>>;
    fn logout<'a>(
        &'a self,
        did: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;
    fn create_post<'a>(
        &'a self,
        _did: &'a str,
        _text: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<PdsCreatedRecord, String>> + Send + 'a>> {
        Box::pin(async { Err("post action is not available".to_owned()) })
    }
    fn create_reply<'a>(
        &'a self,
        _did: &'a str,
        _text: &'a str,
        _root: PostStrongRef,
        _parent: PostStrongRef,
    ) -> Pin<Box<dyn Future<Output = Result<PdsCreatedRecord, String>> + Send + 'a>> {
        Box::pin(async { Err("reply action is not available".to_owned()) })
    }
    fn create_quote<'a>(
        &'a self,
        _did: &'a str,
        _text: &'a str,
        _quoted: PostStrongRef,
    ) -> Pin<Box<dyn Future<Output = Result<PdsCreatedRecord, String>> + Send + 'a>> {
        Box::pin(async { Err("quote action is not available".to_owned()) })
    }
    fn create_post_with_images<'a>(
        &'a self,
        _did: &'a str,
        _text: &'a str,
        _images: Vec<PostImage>,
    ) -> Pin<Box<dyn Future<Output = Result<PdsCreatedRecord, String>> + Send + 'a>> {
        Box::pin(async { Err("image post action is not available".to_owned()) })
    }
    fn create_post_with_external<'a>(
        &'a self,
        _did: &'a str,
        _text: &'a str,
        _external: PostExternal,
    ) -> Pin<Box<dyn Future<Output = Result<PdsCreatedRecord, String>> + Send + 'a>> {
        Box::pin(async { Err("link card post action is not available".to_owned()) })
    }
    fn create_post_with_video<'a>(
        &'a self,
        _did: &'a str,
        _text: &'a str,
        _video: PostVideo,
        _reply: Option<Value>,
        _quote: Option<PostStrongRef>,
    ) -> Pin<Box<dyn Future<Output = Result<PdsCreatedRecord, String>> + Send + 'a>> {
        Box::pin(async { Err("video post action is not available".to_owned()) })
    }
    fn create_reply_with_images<'a>(
        &'a self,
        _did: &'a str,
        _text: &'a str,
        _root: PostStrongRef,
        _parent: PostStrongRef,
        _images: Vec<PostImage>,
    ) -> Pin<Box<dyn Future<Output = Result<PdsCreatedRecord, String>> + Send + 'a>> {
        Box::pin(async { Err("image reply action is not available".to_owned()) })
    }
    fn create_reply_with_external<'a>(
        &'a self,
        _did: &'a str,
        _text: &'a str,
        _root: PostStrongRef,
        _parent: PostStrongRef,
        _external: PostExternal,
    ) -> Pin<Box<dyn Future<Output = Result<PdsCreatedRecord, String>> + Send + 'a>> {
        Box::pin(async { Err("link card reply action is not available".to_owned()) })
    }
    fn create_quote_with_images<'a>(
        &'a self,
        _did: &'a str,
        _text: &'a str,
        _quoted: PostStrongRef,
        _images: Vec<PostImage>,
    ) -> Pin<Box<dyn Future<Output = Result<PdsCreatedRecord, String>> + Send + 'a>> {
        Box::pin(async { Err("image quote action is not available".to_owned()) })
    }
    fn create_quote_with_external<'a>(
        &'a self,
        _did: &'a str,
        _text: &'a str,
        _quoted: PostStrongRef,
        _external: PostExternal,
    ) -> Pin<Box<dyn Future<Output = Result<PdsCreatedRecord, String>> + Send + 'a>> {
        Box::pin(async { Err("link card quote action is not available".to_owned()) })
    }
    fn fetch_post<'a>(
        &'a self,
        _uri: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<IndexedRecord, String>> + Send + 'a>> {
        Box::pin(async { Err("post fetch is not available".to_owned()) })
    }
    fn fetch_author_profile<'a>(
        &'a self,
        _did: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<FetchedAuthorProfile, String>> + Send + 'a>> {
        Box::pin(async { Err("profile fetch is not available".to_owned()) })
    }
    fn resolve_author_handle<'a>(
        &'a self,
        _did: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<String>, String>> + Send + 'a>> {
        Box::pin(async { Ok(None) })
    }
    fn like<'a>(
        &'a self,
        _did: &'a str,
        _uri: &'a str,
        _cid: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<PdsCreatedRecord, String>> + Send + 'a>> {
        Box::pin(async { Err("like action is not available".to_owned()) })
    }
    fn delete_like<'a>(
        &'a self,
        _did: &'a str,
        _like_uri: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async { Err("unlike action is not available".to_owned()) })
    }
    fn repost<'a>(
        &'a self,
        _did: &'a str,
        _uri: &'a str,
        _cid: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<PdsCreatedRecord, String>> + Send + 'a>> {
        Box::pin(async { Err("repost action is not available".to_owned()) })
    }
    fn timeline<'a>(
        &'a self,
        _did: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Value>, String>> + Send + 'a>> {
        Box::pin(async { Err("timeline is not available".to_owned()) })
    }
    fn pinned_feeds<'a>(
        &'a self,
        _did: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResolvedFeed>, String>> + Send + 'a>> {
        Box::pin(async { Ok(Vec::new()) })
    }
    /// Return feed tab data that is already available without waiting for network resolution.
    /// Production flows override this to use persisted metadata and refresh it in the background.
    fn pinned_feed_tabs<'a>(
        &'a self,
        did: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<PinnedFeedTabs, String>> + Send + 'a>> {
        Box::pin(async move {
            let started = std::time::Instant::now();
            Ok(PinnedFeedTabs {
                feeds: self.pinned_feeds(did).await?,
                preferences_ms: 0,
                tabs_ms: started.elapsed().as_millis() as u64,
            })
        })
    }

    fn feed_timeline<'a>(
        &'a self,
        _did: &'a str,
        uri: &'a str,
        _cursor: Option<&'a str>,
        _storage: &'a dyn Storage,
    ) -> Pin<Box<dyn Future<Output = Result<FeedTimeline, String>> + Send + 'a>> {
        Box::pin(async move {
            Ok(FeedTimeline {
                generator: ResolvedFeed {
                    uri: uri.to_owned(),
                    display_name: "Feed".to_owned(),
                    description: None,
                    avatar: None,
                    service_did: String::new(),
                    endpoint: String::new(),
                },
                timeline: crate::feeds::HydratedFeedPage {
                    feed: Vec::new(),
                    cursor: None,
                    dropped_count: 0,
                },
                timing: FeedTimelineTiming::default(),
            })
        })
    }
}

#[derive(Clone)]
pub struct DisabledOAuthFlow;

impl OAuthFlow for DisabledOAuthFlow {
    fn authorize<'a>(
        &'a self,
        _: &'a str,
        _: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<String, String>> + Send + 'a>> {
        Box::pin(async { Err("OAuth flow is not configured".to_owned()) })
    }
    fn callback<'a>(
        &'a self,
        _: OAuthCallback,
    ) -> Pin<Box<dyn Future<Output = Result<OAuthAccount, String>> + Send + 'a>> {
        Box::pin(async { Err("OAuth flow is not configured".to_owned()) })
    }
    fn logout<'a>(
        &'a self,
        _: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async { Err("OAuth flow is not configured".to_owned()) })
    }
}

pub struct OAuthWebState<F, S> {
    pub public: OAuthPublic,
    pub flow: F,
    pub storage: Arc<S>,
    pub alt_model_dir: Option<PathBuf>,
    link_card_fetcher: LinkCardFetcher,
    feed_page_cache: FeedPageCache,
    notification_streams: Arc<Mutex<HashMap<String, usize>>>,
    shutdown: WebShutdown,
}

/// Coordinates web request shutdown and counts active notification streams.
#[derive(Clone)]
pub struct WebShutdown {
    sender: tokio::sync::watch::Sender<bool>,
    notification_streams: Arc<AtomicUsize>,
}

impl WebShutdown {
    pub fn new() -> Self {
        let (sender, _) = tokio::sync::watch::channel(false);
        Self {
            sender,
            notification_streams: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn shutdown(&self) {
        self.sender.send_replace(true);
    }

    pub fn subscribe(&self) -> tokio::sync::watch::Receiver<bool> {
        self.sender.subscribe()
    }

    pub fn active_notification_streams(&self) -> usize {
        self.notification_streams.load(Ordering::Relaxed)
    }
}

impl Default for WebShutdown {
    fn default() -> Self {
        Self::new()
    }
}

const MAX_NOTIFICATION_STREAMS_PER_VIEWER: usize = 4;

struct NotificationStreamGuard {
    registry: Arc<Mutex<HashMap<String, usize>>>,
    active_streams: Arc<AtomicUsize>,
    did: String,
}

impl NotificationStreamGuard {
    fn acquire(
        registry: Arc<Mutex<HashMap<String, usize>>>,
        active_streams: Arc<AtomicUsize>,
        did: &str,
    ) -> Option<Self> {
        let mut streams = registry.lock().ok()?;
        let count = streams.entry(did.to_owned()).or_default();
        if *count >= MAX_NOTIFICATION_STREAMS_PER_VIEWER {
            return None;
        }
        *count += 1;
        active_streams.fetch_add(1, Ordering::Relaxed);
        Some(Self {
            registry: registry.clone(),
            active_streams,
            did: did.to_owned(),
        })
    }
}

impl Drop for NotificationStreamGuard {
    fn drop(&mut self) {
        self.active_streams.fetch_sub(1, Ordering::Relaxed);
        if let Ok(mut streams) = self.registry.lock()
            && let Some(count) = streams.get_mut(&self.did)
        {
            *count = count.saturating_sub(1);
            if *count == 0 {
                streams.remove(&self.did);
            }
        }
    }
}

impl OAuthPublic {
    /// Create a metadata document with a fresh key. Production should use
    /// `load_or_create` so the confidential client key survives restarts.
    pub fn new(public_url: &str) -> Result<Self, OAuthConfigError> {
        Self::from_key(public_url, create_key())
    }

    /// Load the confidential signing key or create it once with owner-only
    /// permissions. Authorization servers use the published key to verify
    /// client assertions; rotating this key invalidates in-flight sessions.
    pub fn load_or_create(
        public_url: &str,
        key_path: impl AsRef<Path>,
    ) -> Result<Self, OAuthConfigError> {
        validate_origin(public_url)?;
        let key = load_or_create_key(key_path.as_ref())?;
        Self::from_key(public_url, key)
    }

    /// Construct the public localhost OAuth client for a loopback listener.
    pub fn new_loopback(listen: SocketAddr) -> Result<Self, OAuthConfigError> {
        Self::from_loopback(listen, create_key())
    }

    /// Load the persistent local cookie-signing key for loopback OAuth.
    /// The key is not published or used for OAuth client authentication.
    pub fn load_or_create_loopback(
        listen: SocketAddr,
        key_path: impl AsRef<Path>,
    ) -> Result<Self, OAuthConfigError> {
        let key = load_or_create_key(key_path.as_ref())?;
        Self::from_loopback(listen, key)
    }

    pub fn metadata(&self) -> &Value {
        &self.metadata
    }

    pub fn private_key(&self) -> &Jwk {
        &self.private_key
    }

    pub fn public_url(&self) -> &str {
        &self.public_url
    }

    pub fn atrium_flow(
        &self,
        identity_resolver: Arc<IdentityResolver<HttpIdentityBackend>>,
        store: SqliteOAuthStore,
    ) -> Result<AtriumOAuthFlow, String> {
        AtriumOAuthFlow::new(
            self,
            identity_resolver,
            store,
            DEFAULT_PLC_DIRECTORY_URL,
            SafeHttpClient::new(),
        )
    }

    /// Construct the production flow against an injectable PLC directory.
    /// This allows offline tests to use a local fake PDS and authorization server.
    pub fn atrium_flow_with_plc_directory<B: crate::identity::IdentityBackend + 'static>(
        &self,
        identity_resolver: Arc<IdentityResolver<B>>,
        store: SqliteOAuthStore,
        plc_directory_url: &str,
    ) -> Result<AtriumOAuthFlow<B>, String> {
        let http_test_hosts = reqwest::Url::parse(plc_directory_url)
            .ok()
            .filter(|url| url.scheme() == "http")
            .and_then(|url| url.host_str().map(str::to_owned))
            .into_iter()
            .collect::<Vec<_>>();
        AtriumOAuthFlow::new(
            self,
            identity_resolver,
            store,
            plc_directory_url,
            SafeHttpClient::new().with_http_test_hosts(http_test_hosts),
        )
    }

    pub fn router(&self) -> Router {
        self.router_with_flow(DisabledOAuthFlow, Arc::new(MemoryStorage::default()))
    }

    pub fn router_with_flow<F, S>(&self, flow: F, storage: Arc<S>) -> Router
    where
        F: OAuthFlow,
        S: Storage + 'static,
    {
        self.router_with_flow_and_alt_model_dir(flow, storage, None)
    }

    pub fn router_with_flow_and_alt_model_dir<F, S>(
        &self,
        flow: F,
        storage: Arc<S>,
        alt_model_dir: Option<PathBuf>,
    ) -> Router
    where
        F: OAuthFlow,
        S: Storage + 'static,
    {
        self.router_with_flow_and_alt_model_dir_and_shutdown(
            flow,
            storage,
            alt_model_dir,
            WebShutdown::new(),
        )
    }

    pub fn router_with_flow_and_shutdown<F, S>(
        &self,
        flow: F,
        storage: Arc<S>,
        shutdown: WebShutdown,
    ) -> Router
    where
        F: OAuthFlow,
        S: Storage + 'static,
    {
        self.router_with_flow_and_alt_model_dir_and_shutdown(flow, storage, None, shutdown)
    }

    pub fn router_with_flow_and_alt_model_dir_and_shutdown<F, S>(
        &self,
        flow: F,
        storage: Arc<S>,
        alt_model_dir: Option<PathBuf>,
        shutdown: WebShutdown,
    ) -> Router
    where
        F: OAuthFlow,
        S: Storage + 'static,
    {
        let state = Arc::new(OAuthWebState {
            public: self.clone(),
            flow,
            storage,
            alt_model_dir,
            link_card_fetcher: LinkCardFetcher::new(),
            shutdown,
            feed_page_cache: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            notification_streams: Arc::new(Mutex::new(HashMap::new())),
        });
        let regular_routes = Router::new()
            .route(METADATA_PATH, get(metadata::<F, S>))
            .route("/static/app.css", get(app_stylesheet))
            .route("/static/app.js", get(app_script))
            .route("/static/alt-model-worker.js", get(alt_model_worker))
            .route("/static/vendor/{*path}", get(vendor_asset))
            .route("/static/models/{*path}", get(model_asset::<F, S>))
            .route("/", get(root))
            .route("/login", get(login))
            .route("/oauth/authorize", post(authorize::<F, S>))
            .route("/oauth/callback", get(callback::<F, S>))
            .route("/logout", post(logout::<F, S>))
            .route("/app", get(app_home::<F, S>))
            .route("/alt-test", get(alt_test_page::<F, S>))
            .route("/feeds", get(feeds_page::<F, S>))
            .route("/app/feed", get(feed_timeline_page::<F, S>))
            .route("/app/thread", get(thread_page::<F, S>))
            .route("/notifications", get(notifications_page::<F, S>))
            .route(
                "/notifications/unread-count",
                get(notification_unread_count::<F, S>),
            )
            .route("/notifications/stream", get(notification_stream::<F, S>))
            .route("/profile", get(profile_page::<F, S>))
            .route("/thread", get(thread_page::<F, S>))
            .route("/compose", get(quote_composer::<F, S>))
            .route("/link-preview", post(link_preview::<F, S>))
            .route("/post", post(write_post::<F, S>))
            .route("/reply", post(write_reply::<F, S>))
            .route("/quote", post(write_quote::<F, S>))
            .route("/like", post(write_like::<F, S>))
            .route("/repost", post(write_repost::<F, S>))
            .layer(DefaultBodyLimit::max(MAX_POST_BODY_BYTES))
            .with_state(state.clone());
        let video_routes = Router::new()
            .route("/video", post(upload_video::<F, S>))
            .layer(DefaultBodyLimit::max(MAX_VIDEO_REQUEST_BODY_BYTES))
            .with_state(state);
        regular_routes.merge(video_routes)
    }

    fn from_key(public_url: &str, private_key: Jwk) -> Result<Self, OAuthConfigError> {
        validate_origin(public_url)?;
        validate_private_key(&private_key)?;
        let mut public_key = private_key.clone();
        if let Key::Ec(ec) = &mut public_key.key {
            ec.d = None;
        }
        let client_id = format!("{public_url}{METADATA_PATH}");
        let redirect_uri = format!("{public_url}{CALLBACK_PATH}");
        let metadata = json!({
            "client_id": client_id,
            "client_uri": public_url,
            "application_type": "web",
            "redirect_uris": [redirect_uri],
            "grant_types": ["authorization_code", "refresh_token"],
            "scope": REQUESTED_SCOPE,
            "token_endpoint_auth_method": "private_key_jwt",
            "token_endpoint_auth_signing_alg": "ES256",
            "dpop_bound_access_tokens": true,
            "jwks": {"keys": [public_key]}
        });
        Ok(Self {
            public_url: public_url.to_owned(),
            private_key,
            metadata,
            mode: OAuthClientMode::Confidential,
        })
    }

    fn from_loopback(listen: SocketAddr, private_key: Jwk) -> Result<Self, OAuthConfigError> {
        if !listen.ip().is_loopback() {
            return Err(OAuthConfigError::InvalidLoopbackAddress);
        }
        validate_private_key(&private_key)?;
        let origin = format!("http://{listen}");
        let redirect_uri = format!("{origin}{CALLBACK_PATH}");
        let query = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("redirect_uri", &redirect_uri)
            .append_pair("scope", REQUESTED_SCOPE)
            .finish();
        let client_id = format!("http://localhost?{query}");
        let metadata = json!({
            "client_id": client_id,
            "client_name": "Development client",
            "application_type": "native",
            "redirect_uris": [redirect_uri],
            "grant_types": ["authorization_code", "refresh_token"],
            "response_types": ["code"],
            "scope": REQUESTED_SCOPE,
            "token_endpoint_auth_method": "none",
            "dpop_bound_access_tokens": true
        });
        Ok(Self {
            public_url: origin,
            private_key,
            metadata,
            mode: OAuthClientMode::Loopback,
        })
    }

    fn secure_cookies(&self) -> bool {
        self.mode == OAuthClientMode::Confidential && self.public_url.starts_with("https://")
    }

    fn issue_session_cookie(
        &self,
        did: &str,
        session_id: &str,
        generation: u64,
        permissions: SessionPermissions,
    ) -> String {
        let expires = unix_time().saturating_add(SESSION_TTL_SECONDS);
        let payload = format!(
            "{did}\n{expires}\n{session_id}\n{generation}\n{}\n{}\n{}\n{}",
            permissions.write, permissions.feed, permissions.blob, permissions.video
        );
        let encoded = URL_SAFE_NO_PAD.encode(payload.as_bytes());
        let mut mac = HmacSha256::new_from_slice(&self.cookie_key())
            .expect("HMAC accepts keys of any length");
        mac.update(b"session:");
        mac.update(encoded.as_bytes());
        format!(
            "{encoded}.{}",
            URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
        )
    }

    fn issue_login_cookie(&self, binding: &str) -> String {
        let expires = unix_time().saturating_add(LOGIN_COOKIE_TTL_SECONDS);
        let payload = URL_SAFE_NO_PAD.encode(format!("{binding}\n{expires}").as_bytes());
        let mut mac = HmacSha256::new_from_slice(&self.cookie_key())
            .expect("HMAC accepts keys of any length");
        mac.update(b"login:");
        mac.update(payload.as_bytes());
        format!(
            "{payload}.{}",
            URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
        )
    }

    fn verify_login_cookie(&self, token: &str) -> Option<String> {
        let (payload, signature) = token.split_once('.')?;
        let signature = URL_SAFE_NO_PAD.decode(signature).ok()?;
        let mut mac = HmacSha256::new_from_slice(&self.cookie_key()).ok()?;
        mac.update(b"login:");
        mac.update(payload.as_bytes());
        mac.verify_slice(&signature).ok()?;
        let decoded = URL_SAFE_NO_PAD.decode(payload).ok()?;
        let value = std::str::from_utf8(&decoded).ok()?;
        let (binding, expires) = value.split_once('\n')?;
        if binding.is_empty() || expires.parse::<u64>().ok()? <= unix_time() {
            return None;
        }
        Some(binding.to_owned())
    }

    fn verify_session_cookie(
        &self,
        token: &str,
    ) -> Option<(String, String, u64, bool, bool, bool, bool)> {
        let (payload, signature) = token.split_once('.')?;
        let signature = URL_SAFE_NO_PAD.decode(signature).ok()?;
        let mut mac = HmacSha256::new_from_slice(&self.cookie_key()).ok()?;
        mac.update(b"session:");
        mac.update(payload.as_bytes());
        mac.verify_slice(&signature).ok()?;
        let decoded = URL_SAFE_NO_PAD.decode(payload).ok()?;
        let value = std::str::from_utf8(&decoded).ok()?;
        let (did, session) = value.split_once('\n')?;
        let (expires, session) = session.split_once('\n')?;
        let (session_id, generation) = session.split_once('\n')?;
        let (generation, permissions) = generation.split_once('\n')?;
        let (write_access, permissions) = permissions
            .split_once('\n')
            .unwrap_or((permissions, "false\nfalse"));
        let (feed_access, permissions) = permissions
            .split_once('\n')
            .unwrap_or((permissions, "false\nfalse"));
        let (blob_access, video_access) = permissions
            .split_once('\n')
            .unwrap_or((permissions, "false"));
        if expires.parse::<u64>().ok()? <= unix_time()
            || session_id.is_empty()
            || !(did.starts_with("did:plc:") || did.starts_with("did:web:"))
        {
            return None;
        }
        Some((
            did.to_owned(),
            session_id.to_owned(),
            generation.parse().ok()?,
            write_access.parse().ok()?,
            feed_access.parse().ok()?,
            blob_access.parse().ok()?,
            video_access.parse().ok()?,
        ))
    }

    fn cookie_key(&self) -> Vec<u8> {
        let serialized =
            serde_json::to_value(&self.private_key).expect("the validated JWK is serializable");
        let secret = URL_SAFE_NO_PAD
            .decode(
                serialized
                    .get("d")
                    .and_then(Value::as_str)
                    .unwrap_or_default(),
            )
            .expect("the validated JWK has a valid private key");
        let hkdf = Hkdf::<Sha256>::new(Some(b"appview-cookie-hmac-v1"), &secret);
        let mut derived = [0u8; 32];
        hkdf.expand(b"web-session-cookie", &mut derived)
            .expect("a 32-byte HKDF output is valid for SHA-256");
        derived.to_vec()
    }
}

type OAuthDidResolver = CommonDidResolver<SafeHttpClient>;
type OAuthHandleResolver = AtprotoHandleResolver<CloudflareDnsTxtResolver, SafeHttpClient>;
type AppOAuthClient = OAuthClient<
    SqliteOAuthStore,
    SqliteOAuthStore,
    OAuthDidResolver,
    OAuthHandleResolver,
    SafeHttpClient,
>;
type SavedFeedCache =
    Arc<tokio::sync::Mutex<HashMap<String, (tokio::time::Instant, Vec<SavedFeedPreference>)>>>;
type SavedFeedRefreshes = Arc<tokio::sync::Mutex<HashSet<String>>>;
// Keyed by (viewer DID, service DID, XRPC method).
type ServiceAuthCache =
    Arc<tokio::sync::Mutex<HashMap<(String, String, String), (tokio::time::Instant, String)>>>;
// Keyed by (viewer DID, feed URI, cursor); switching between feed tabs reuses a page for a minute.
type FeedPageCache = Arc<
    tokio::sync::Mutex<HashMap<(String, String, String), (tokio::time::Instant, FeedTimeline)>>,
>;
type FeedSkeletonCache = Arc<
    tokio::sync::Mutex<HashMap<(String, String, String), (tokio::time::Instant, FeedSkeletonPage)>>,
>;
const SERVICE_AUTH_LIFETIME_SECS: u64 = 30 * 60;
const FEED_PAGE_CACHE_SECS: u64 = 60;

pub struct AtriumOAuthFlow<B: crate::identity::IdentityBackend = HttpIdentityBackend> {
    client: Arc<AppOAuthClient>,
    identity_resolver: Arc<IdentityResolver<B>>,
    store: SqliteOAuthStore,
    feed_client: Arc<FeedClient<B>>,
    http_client: SafeHttpClient,
    video_http_client: reqwest::Client,
    saved_feed_cache: SavedFeedCache,
    saved_feed_refreshes: SavedFeedRefreshes,
    service_auth_cache: ServiceAuthCache,
    feed_page_cache: FeedPageCache,
    feed_skeleton_cache: FeedSkeletonCache,
}

impl<B: crate::identity::IdentityBackend> Clone for AtriumOAuthFlow<B> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            identity_resolver: self.identity_resolver.clone(),
            store: self.store.clone(),
            feed_client: self.feed_client.clone(),
            http_client: self.http_client.clone(),
            video_http_client: self.video_http_client.clone(),
            saved_feed_cache: self.saved_feed_cache.clone(),
            saved_feed_refreshes: self.saved_feed_refreshes.clone(),
            service_auth_cache: self.service_auth_cache.clone(),
            feed_page_cache: self.feed_page_cache.clone(),
            feed_skeleton_cache: self.feed_skeleton_cache.clone(),
        }
    }
}

#[derive(Deserialize, Serialize)]
struct ExpectedOAuthIdentity {
    did: String,
    handle: Option<String>,
    browser_binding: String,
}

#[derive(Debug, Error)]
pub enum OAuthStoreError {
    #[error("OAuth session database error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("OAuth session data encoding error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("OAuth session database lock was poisoned")]
    Poisoned,
    #[error("OAuth session database I/O error: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Clone)]
pub struct SqliteOAuthStore {
    connection: Arc<Mutex<Connection>>,
}

impl SqliteOAuthStore {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, OAuthStoreError> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        crate::ingest::ensure_private_database_file(path)?;
        let connection = Connection::open(path)?;
        let migration_started = std::time::Instant::now();
        connection.execute_batch(
            "PRAGMA journal_mode = WAL;
             CREATE TABLE IF NOT EXISTS web_cache (
                 cache_key TEXT PRIMARY KEY,
                 value_json TEXT NOT NULL,
                 stored_at_ms INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS oauth_states (
                 state TEXT PRIMARY KEY,
                 value_json TEXT NOT NULL,
                 created_at INTEGER NOT NULL DEFAULT (unixepoch())
             );
             CREATE TABLE IF NOT EXISTS oauth_sessions (
                 did TEXT PRIMARY KEY,
                 value_json TEXT NOT NULL
             );",
        )?;
        tracing::info!(
            elapsed_ms = migration_started.elapsed().as_millis() as u64,
            "OAuth cache schema migration complete"
        );
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
        })
    }

    pub(crate) fn web_cache_get(&self, key: &str) -> Result<Option<(String, i64)>, String> {
        self.connection
            .lock()
            .map_err(|_| "OAuth cache database lock is poisoned".to_owned())?
            .query_row(
                "SELECT value_json, stored_at_ms FROM web_cache WHERE cache_key=?1",
                [key],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|error| error.to_string())
    }

    pub(crate) fn web_cache_set(&self, key: &str, value_json: &str) -> Result<(), String> {
        let stored_at_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
            .unwrap_or_default();
        self.connection
            .lock()
            .map_err(|_| "OAuth cache database lock is poisoned".to_owned())?
            .execute(
                "INSERT INTO web_cache(cache_key,value_json,stored_at_ms) VALUES(?1,?2,?3)
                 ON CONFLICT(cache_key) DO UPDATE SET value_json=excluded.value_json,stored_at_ms=excluded.stored_at_ms",
                params![key, value_json, stored_at_ms],
            )
            .map_err(|error| error.to_string())?;
        Ok(())
    }

    fn get_json<T: for<'de> Deserialize<'de>>(
        &self,
        table: &'static str,
        key_column: &'static str,
        key: &str,
        state_ttl: bool,
    ) -> Result<Option<T>, OAuthStoreError> {
        let connection = self
            .connection
            .lock()
            .map_err(|_| OAuthStoreError::Poisoned)?;
        let sql = if state_ttl {
            format!(
                "SELECT value_json FROM {table} WHERE {key_column}=?1 AND created_at>=unixepoch()-600"
            )
        } else {
            format!("SELECT value_json FROM {table} WHERE {key_column}=?1")
        };
        connection
            .query_row(&sql, [key], |row| row.get::<_, String>(0))
            .optional()?
            .map(|json| serde_json::from_str(&json).map_err(Into::into))
            .transpose()
    }

    fn set_json<T: serde::Serialize>(
        &self,
        table: &'static str,
        key_column: &'static str,
        key: &str,
        value: &T,
        update_timestamp: bool,
    ) -> Result<(), OAuthStoreError> {
        let value_json = serde_json::to_string(value)?;
        let sql = if update_timestamp {
            format!(
                "INSERT INTO {table}({key_column},value_json,created_at) VALUES(?1,?2,unixepoch()) ON CONFLICT({key_column}) DO UPDATE SET value_json=excluded.value_json,created_at=unixepoch()"
            )
        } else {
            format!(
                "INSERT INTO {table}({key_column},value_json) VALUES(?1,?2) ON CONFLICT({key_column}) DO UPDATE SET value_json=excluded.value_json"
            )
        };
        self.connection
            .lock()
            .map_err(|_| OAuthStoreError::Poisoned)?
            .execute(&sql, params![key, value_json])?;
        Ok(())
    }

    fn delete_key(
        &self,
        table: &'static str,
        key_column: &'static str,
        key: &str,
    ) -> Result<(), OAuthStoreError> {
        let sql = format!("DELETE FROM {table} WHERE {key_column}=?1");
        self.connection
            .lock()
            .map_err(|_| OAuthStoreError::Poisoned)?
            .execute(&sql, [key])?;
        Ok(())
    }

    fn clear_table(&self, table: &'static str) -> Result<(), OAuthStoreError> {
        self.connection
            .lock()
            .map_err(|_| OAuthStoreError::Poisoned)?
            .execute(&format!("DELETE FROM {table}"), [])?;
        Ok(())
    }
}

impl Store<String, InternalStateData> for SqliteOAuthStore {
    type Error = OAuthStoreError;

    async fn get(&self, key: &String) -> Result<Option<InternalStateData>, Self::Error> {
        self.get_json("oauth_states", "state", key, true)
    }

    async fn set(&self, key: String, value: InternalStateData) -> Result<(), Self::Error> {
        self.set_json("oauth_states", "state", &key, &value, true)
    }

    async fn del(&self, key: &String) -> Result<(), Self::Error> {
        self.delete_key("oauth_states", "state", key)
    }

    async fn clear(&self) -> Result<(), Self::Error> {
        self.clear_table("oauth_states")
    }
}

impl StateStore for SqliteOAuthStore {}

impl Store<Did, Session> for SqliteOAuthStore {
    type Error = OAuthStoreError;

    async fn get(&self, key: &Did) -> Result<Option<Session>, Self::Error> {
        self.get_json("oauth_sessions", "did", key.as_ref(), false)
    }

    async fn set(&self, key: Did, value: Session) -> Result<(), Self::Error> {
        self.set_json("oauth_sessions", "did", key.as_ref(), &value, false)
    }

    async fn del(&self, key: &Did) -> Result<(), Self::Error> {
        self.delete_key("oauth_sessions", "did", key.as_ref())
    }

    async fn clear(&self) -> Result<(), Self::Error> {
        self.clear_table("oauth_sessions")
    }
}

impl SessionStore for SqliteOAuthStore {}

impl<B: crate::identity::IdentityBackend + 'static> AtriumOAuthFlow<B> {
    fn new(
        public: &OAuthPublic,
        identity_resolver: Arc<IdentityResolver<B>>,
        store: SqliteOAuthStore,
        plc_directory_url: &str,
        guarded_client: SafeHttpClient,
    ) -> Result<Self, String> {
        let feed_client = Arc::new(FeedClient::new_with_store(
            Arc::clone(&identity_resolver),
            guarded_client.clone(),
            store.clone(),
        ));
        let retained_http_client = guarded_client.clone();
        let http_client = Arc::new(guarded_client);
        let redirect_uri = public.metadata["redirect_uris"][0]
            .as_str()
            .ok_or_else(|| "OAuth client metadata has no redirect URI".to_owned())?
            .to_owned();
        let make_resolver = || OAuthResolverConfig {
            did_resolver: CommonDidResolver::new(CommonDidResolverConfig {
                plc_directory_url: plc_directory_url.to_owned(),
                http_client: http_client.clone(),
            }),
            handle_resolver: AtprotoHandleResolver::new(AtprotoHandleResolverConfig {
                dns_txt_resolver: CloudflareDnsTxtResolver {
                    client: (*http_client).clone(),
                },
                http_client: http_client.clone(),
            }),
            authorization_server_metadata: Default::default(),
            protected_resource_metadata: Default::default(),
        };
        let client = match public.mode {
            OAuthClientMode::Loopback => OAuthClient::new(OAuthClientConfig {
                client_metadata: AtprotoLocalhostClientMetadata {
                    redirect_uris: Some(vec![redirect_uri]),
                    scopes: Some(requested_scopes()),
                },
                keys: None,
                state_store: store.clone(),
                session_store: store.clone(),
                resolver: make_resolver(),
                http_client: (*http_client).clone(),
            }),
            OAuthClientMode::Confidential => {
                let metadata = AtprotoClientMetadata {
                    client_id: public.metadata["client_id"]
                        .as_str()
                        .ok_or_else(|| "OAuth client metadata has no client_id".to_owned())?
                        .to_owned(),
                    client_uri: Some(public.public_url.clone()),
                    redirect_uris: vec![redirect_uri],
                    token_endpoint_auth_method: AuthMethod::PrivateKeyJwt,
                    grant_types: vec![GrantType::AuthorizationCode, GrantType::RefreshToken],
                    scopes: requested_scopes(),
                    jwks_uri: None,
                    token_endpoint_auth_signing_alg: Some("ES256".to_owned()),
                };
                OAuthClient::new(OAuthClientConfig {
                    client_metadata: metadata,
                    keys: Some(vec![public.private_key.clone()]),
                    state_store: store.clone(),
                    session_store: store.clone(),
                    resolver: make_resolver(),
                    http_client: (*http_client).clone(),
                })
            }
        }
        .map_err(|error| error.to_string())?;
        Ok(Self {
            client: Arc::new(client),
            identity_resolver,
            store,
            feed_client,
            http_client: retained_http_client,
            video_http_client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .map_err(|error| error.to_string())?,
            saved_feed_cache: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            saved_feed_refreshes: Arc::new(tokio::sync::Mutex::new(HashSet::new())),
            service_auth_cache: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            feed_page_cache: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            feed_skeleton_cache: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        })
    }

    async fn saved_feed_preferences(&self, did: &str) -> Result<Vec<SavedFeedPreference>, String> {
        if let Some((expires, preferences)) = self.saved_feed_cache.lock().await.get(did).cloned()
            && expires > tokio::time::Instant::now()
        {
            return Ok(preferences);
        }
        let did = did
            .parse::<Did>()
            .map_err(|_| "viewer DID is invalid".to_owned())?;
        let session = self
            .client
            .restore(&did)
            .await
            .map_err(|_| "OAuth session could not be restored".to_owned())?;
        let request = XrpcRequest {
            method: http::Method::GET,
            nsid: "app.bsky.actor.getPreferences".to_owned(),
            parameters: Some(json!({})),
            input: None,
            encoding: None,
        };
        let response: OutputDataOrBytes<Value> = session
            .send_xrpc::<Value, Value, Value, Value>(&request)
            .await
            .map_err(|error| {
                log_pds_xrpc_failure("app.bsky.actor.getPreferences", &error);
                "PDS getPreferences request failed".to_owned()
            })?;
        let response = match response {
            OutputDataOrBytes::Data(value) => value,
            OutputDataOrBytes::Bytes(_) => {
                return Err("PDS getPreferences returned a non-JSON response".to_owned());
            }
        };
        let preferences = parse_saved_feed_preferences(&response);
        let cache_key = saved_feed_cache_key(&did);
        if let Ok(value_json) = serde_json::to_string(&preferences)
            && let Err(error) = self.store.web_cache_set(&cache_key, &value_json)
        {
            tracing::warn!(%error, "could not persist saved feed preferences");
        }
        let mut cache = self.saved_feed_cache.lock().await;
        cache.retain(|_, (expires, _)| *expires > tokio::time::Instant::now());
        if cache.len() >= 4096
            && let Some(oldest) = cache.keys().next().cloned()
        {
            cache.remove(&oldest);
        }
        cache.insert(
            did.to_string(),
            (
                tokio::time::Instant::now() + Duration::from_secs(5 * 60),
                preferences.clone(),
            ),
        );
        Ok(preferences)
    }

    async fn saved_feed_preferences_for_tabs(&self, did: &str) -> Vec<SavedFeedPreference> {
        if let Some((expires, preferences)) = self.saved_feed_cache.lock().await.get(did).cloned()
            && expires > tokio::time::Instant::now()
        {
            return preferences;
        }
        let mut cached_preferences = None;
        let mut refresh = true;
        if let Ok(Some((json, stored_at_ms))) = self.store.web_cache_get(&saved_feed_cache_key(did))
            && let Ok(preferences) = serde_json::from_str::<Vec<SavedFeedPreference>>(&json)
        {
            let age_ms = unix_time_ms().saturating_sub(stored_at_ms).max(0) as u64;
            refresh = age_ms > 60 * 60 * 1000;
            cached_preferences = Some(preferences);
        }
        if refresh {
            let mut pending = self.saved_feed_refreshes.lock().await;
            if pending.insert(did.to_owned()) {
                drop(pending);
                let flow = self.clone();
                let did = did.to_owned();
                tokio::spawn(async move {
                    let _ = flow.saved_feed_preferences(&did).await;
                    flow.saved_feed_refreshes.lock().await.remove(&did);
                });
            }
        }
        cached_preferences.unwrap_or_default()
    }

    async fn service_auth_token(
        &self,
        viewer_did: &str,
        service_did: &str,
        lexicon: &str,
    ) -> Result<String, String> {
        let key = (
            viewer_did.to_owned(),
            service_did.to_owned(),
            lexicon.to_owned(),
        );
        if let Some((expires, token)) = self.service_auth_cache.lock().await.get(&key).cloned()
            && expires > tokio::time::Instant::now()
        {
            return Ok(token);
        }
        let token = self
            .fetch_service_auth_token(viewer_did, service_did, lexicon)
            .await?;
        let mut cache = self.service_auth_cache.lock().await;
        cache.retain(|_, (expires, _)| *expires > tokio::time::Instant::now());
        if cache.len() < 4096 {
            // Reuse until a minute before the expiry we asked for.
            cache.insert(
                key,
                (
                    tokio::time::Instant::now()
                        + Duration::from_secs(SERVICE_AUTH_LIFETIME_SECS - 60),
                    token.clone(),
                ),
            );
        }
        Ok(token)
    }

    async fn fetch_service_auth_token(
        &self,
        viewer_did: &str,
        service_did: &str,
        lexicon: &str,
    ) -> Result<String, String> {
        let did = viewer_did
            .parse::<Did>()
            .map_err(|_| "viewer DID is invalid".to_owned())?;
        let session = self
            .client
            .restore(&did)
            .await
            .map_err(|_| "OAuth session could not be restored".to_owned())?;
        let exp = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| "system clock is before the epoch".to_owned())?
            .as_secs()
            + SERVICE_AUTH_LIFETIME_SECS;
        let request = XrpcRequest {
            method: http::Method::GET,
            nsid: "com.atproto.server.getServiceAuth".to_owned(),
            parameters: Some(json!({"aud":service_did,"lxm":lexicon,"exp":exp})),
            input: None,
            encoding: None,
        };
        let response: OutputDataOrBytes<Value> = session
            .send_xrpc::<Value, Value, Value, Value>(&request)
            .await
            .map_err(|error| {
                log_pds_xrpc_failure("com.atproto.server.getServiceAuth", &error);
                "PDS getServiceAuth request failed".to_owned()
            })?;
        match response {
            OutputDataOrBytes::Data(value) => value
                .get("token")
                .and_then(Value::as_str)
                .filter(|token| !token.is_empty())
                .map(str::to_owned)
                .ok_or_else(|| "PDS getServiceAuth returned no token".to_owned()),
            OutputDataOrBytes::Bytes(_) => {
                Err("PDS getServiceAuth returned a non-JSON response".to_owned())
            }
        }
    }

    async fn video_service_json(
        &self,
        viewer_did: &str,
        lexicon: &str,
        parameters: &[(&str, &str)],
    ) -> Result<Value, String> {
        let token = self
            .service_auth_token(viewer_did, VIDEO_SERVICE_DID, lexicon)
            .await?;
        let mut url = reqwest::Url::parse(VIDEO_SERVICE_URL)
            .map_err(|error| format!("video service URL is invalid: {error}"))?;
        url.set_path(&format!("/xrpc/{lexicon}"));
        url.query_pairs_mut()
            .extend_pairs(parameters.iter().copied());
        let response = self
            .video_http_client
            .get(url)
            .bearer_auth(token)
            .send()
            .await
            .map_err(|error| format!("video service {lexicon} request failed: {error}"))?;
        read_video_service_response(response).await
    }

    async fn upload_video_blob(&self, did: &str, video: &PostVideo) -> Result<Value, String> {
        let limits = self
            .video_service_json(did, "app.bsky.video.getUploadLimits", &[])
            .await?;
        validate_video_upload_limits(&limits, video.size_bytes)?;

        let token = self
            .service_auth_token(did, VIDEO_SERVICE_DID, "app.bsky.video.uploadVideo")
            .await?;
        let mut url = reqwest::Url::parse(VIDEO_SERVICE_URL)
            .map_err(|error| format!("video service URL is invalid: {error}"))?;
        url.set_path("/xrpc/app.bsky.video.uploadVideo");
        url.query_pairs_mut()
            .append_pair("did", did)
            .append_pair("name", &video.filename);
        let file = tokio::fs::File::open(&video.path)
            .await
            .map_err(|error| format!("temporary video file could not be reopened: {error}"))?;
        let body = reqwest::Body::wrap_stream(tokio_util::io::ReaderStream::new(file));
        let response = self
            .video_http_client
            .post(url)
            .bearer_auth(token)
            .header(header::CONTENT_TYPE, &video.mime_type)
            .header(header::CONTENT_LENGTH, video.size_bytes)
            .body(body)
            .send()
            .await
            .map_err(|error| format!("video service upload request failed: {error}"))?;
        let initial = read_video_service_response(response).await?;
        let initial_status = job_status_value(&initial);
        let state = initial_status
            .get("state")
            .and_then(Value::as_str)
            .unwrap_or_default();
        let job_id = initial_status
            .get("jobId")
            .and_then(Value::as_str)
            .ok_or_else(|| "video service upload returned no job ID".to_owned())?;
        if state == "JOB_STATE_COMPLETED" {
            return initial_status
                .get("blob")
                .cloned()
                .ok_or_else(|| "video service completed without returning a blob".to_owned());
        }
        poll_video_job(
            job_id,
            VIDEO_JOB_POLL_INTERVAL,
            VIDEO_JOB_MAX_POLLS,
            |job_id| {
                let service = self.clone();
                let did = did.to_owned();
                Box::pin(async move {
                    service
                        .video_service_json(
                            &did,
                            "app.bsky.video.getJobStatus",
                            &[("jobId", job_id.as_str())],
                        )
                        .await
                })
            },
        )
        .await
    }

    async fn create_record(
        &self,
        did: &str,
        collection: &str,
        record: Value,
    ) -> Result<PdsCreatedRecord, String> {
        let indexed_record = record.clone();
        let time_us = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros()
            .min(u64::MAX as u128) as u64;
        let did = did.parse::<Did>().map_err(|error| error.to_string())?;
        let session = self
            .client
            .restore(&did)
            .await
            .map_err(|error| error.to_string())?;
        let agent = Agent::new(session);
        let input = atrium_api::com::atproto::repo::create_record::InputData {
            collection: collection
                .parse()
                .map_err(|error| format!("invalid collection: {error}"))?,
            record: serde_json::from_value(record).map_err(|error| error.to_string())?,
            repo: AtIdentifier::Did(did),
            rkey: None,
            swap_commit: None,
            validate: None,
        }
        .into();
        let output = agent
            .api
            .com
            .atproto
            .repo
            .create_record(input)
            .await
            .map_err(|error| format!("PDS createRecord failed: {error:?}"))?;
        let cid = serde_json::to_value(output.cid.clone())
            .map_err(|error| error.to_string())?
            .as_str()
            .ok_or_else(|| "PDS createRecord returned an invalid CID".to_owned())?
            .to_owned();
        Ok(PdsCreatedRecord {
            uri: output.uri.to_string(),
            cid,
            record: indexed_record,
            time_us,
        })
    }

    async fn create_post_with_images(
        &self,
        did: &str,
        text: &str,
        images: Vec<PostImage>,
        reply: Option<Value>,
        quote: Option<PostStrongRef>,
    ) -> Result<PdsCreatedRecord, String> {
        let parsed_did = did.parse::<Did>().map_err(|error| error.to_string())?;
        let session = self
            .client
            .restore(&parsed_did)
            .await
            .map_err(|error| error.to_string())?;
        let mut blobs = Vec::with_capacity(images.len());
        for image in &images {
            let request = XrpcRequest {
                method: http::Method::POST,
                nsid: "com.atproto.repo.uploadBlob".to_owned(),
                parameters: None,
                input: Some(InputDataOrBytes::Bytes(image.bytes.clone())),
                encoding: Some(image.mime_type.clone()),
            };
            let response: OutputDataOrBytes<Value> = session
                .send_xrpc::<(), Vec<u8>, Value, Value>(&request)
                .await
                .map_err(|error| {
                    log_pds_xrpc_failure("com.atproto.repo.uploadBlob", &error);
                    format!("PDS uploadBlob failed: {error:?}")
                })?;
            let blob = match response {
                OutputDataOrBytes::Data(value) => value
                    .get("blob")
                    .cloned()
                    .filter(|blob| blob.get("ref").is_some())
                    .ok_or_else(|| "PDS uploadBlob returned no blob reference".to_owned())?,
                OutputDataOrBytes::Bytes(_) => {
                    return Err("PDS uploadBlob returned a non-JSON response".to_owned());
                }
            };
            blobs.push(blob);
        }

        let image_records = images
            .iter()
            .zip(blobs)
            .map(|(image, blob)| {
                json!({
                    "image":blob,
                    "alt":image.alt,
                    "aspectRatio":{"width":image.width,"height":image.height}
                })
            })
            .collect::<Vec<_>>();
        let images_embed = json!({
            "$type":"app.bsky.embed.images",
            "images":image_records
        });
        let embed = if let Some(quote) = quote {
            json!({
                "$type":"app.bsky.embed.recordWithMedia",
                "record":{
                    "$type":"app.bsky.embed.record",
                    "record":quote
                },
                "media":images_embed
            })
        } else {
            images_embed
        };
        let mut record = json!({
            "$type":"app.bsky.feed.post",
            "text":text,
            "createdAt":Datetime::now(),
            "embed":embed
        });
        if let Some(reply) = reply {
            record["reply"] = reply;
        }
        self.create_record(did, "app.bsky.feed.post", record).await
    }

    async fn create_post_with_external(
        &self,
        did: &str,
        text: &str,
        external: PostExternal,
        reply: Option<Value>,
        quote: Option<PostStrongRef>,
    ) -> Result<PdsCreatedRecord, String> {
        let uri =
            url::Url::parse(&external.uri).map_err(|_| "link card URL is invalid".to_owned())?;
        if !matches!(uri.scheme(), "http" | "https")
            || !uri.username().is_empty()
            || uri.password().is_some()
        {
            return Err("link card URL must use http or https".to_owned());
        }

        let parsed_did = did.parse::<Did>().map_err(|error| error.to_string())?;
        let session = self
            .client
            .restore(&parsed_did)
            .await
            .map_err(|error| error.to_string())?;
        let thumb = if let Some(thumbnail) = external.thumbnail.as_ref() {
            let request = XrpcRequest {
                method: http::Method::POST,
                nsid: "com.atproto.repo.uploadBlob".to_owned(),
                parameters: None,
                input: Some(InputDataOrBytes::Bytes(thumbnail.bytes.clone())),
                encoding: Some(thumbnail.mime_type.clone()),
            };
            let response: OutputDataOrBytes<Value> = session
                .send_xrpc::<(), Vec<u8>, Value, Value>(&request)
                .await
                .map_err(|error| {
                    log_pds_xrpc_failure("com.atproto.repo.uploadBlob", &error);
                    format!("PDS uploadBlob failed: {error:?}")
                })?;
            Some(match response {
                OutputDataOrBytes::Data(value) => value
                    .get("blob")
                    .cloned()
                    .filter(|blob| blob.get("ref").is_some())
                    .ok_or_else(|| "PDS uploadBlob returned no blob reference".to_owned())?,
                OutputDataOrBytes::Bytes(_) => {
                    return Err("PDS uploadBlob returned a non-JSON response".to_owned());
                }
            })
        } else {
            None
        };
        let external_embed = external_embed_value(&external, thumb);
        let embed = if let Some(quote) = quote {
            json!({
                "$type":"app.bsky.embed.recordWithMedia",
                "record":{"$type":"app.bsky.embed.record","record":quote},
                "media":external_embed
            })
        } else {
            external_embed
        };
        let mut record = json!({
            "$type":"app.bsky.feed.post",
            "text":text,
            "createdAt":Datetime::now(),
            "embed":embed
        });
        if let Some(reply) = reply {
            record["reply"] = reply;
        }
        self.create_record(did, "app.bsky.feed.post", record).await
    }

    async fn create_post_with_video(
        &self,
        did: &str,
        text: &str,
        video: PostVideo,
        reply: Option<Value>,
        quote: Option<PostStrongRef>,
    ) -> Result<PdsCreatedRecord, String> {
        let blob = self.upload_video_blob(did, &video).await?;
        let record = build_video_post_record(
            text,
            blob,
            &video.alt,
            video.width,
            video.height,
            reply,
            quote,
        );
        self.create_record(did, "app.bsky.feed.post", record).await
    }

    async fn delete_like_record(&self, did: &str, like_uri: &str) -> Result<(), String> {
        let collection = "app.bsky.feed.like";
        let prefix = format!("at://{did}/{collection}/");
        let rkey = like_uri
            .strip_prefix(&prefix)
            .filter(|rkey| !rkey.is_empty() && !rkey.contains('/'))
            .ok_or_else(|| "like URI does not belong to this account".to_owned())?;
        let did = did.parse::<Did>().map_err(|error| error.to_string())?;
        let session = self
            .client
            .restore(&did)
            .await
            .map_err(|error| error.to_string())?;
        let agent = Agent::new(session);
        let input = atrium_api::com::atproto::repo::delete_record::InputData {
            collection: collection
                .parse()
                .map_err(|error| format!("invalid collection: {error}"))?,
            repo: AtIdentifier::Did(did),
            rkey: rkey
                .parse()
                .map_err(|error| format!("invalid record key: {error}"))?,
            swap_commit: None,
            swap_record: None,
        }
        .into();
        agent
            .api
            .com
            .atproto
            .repo
            .delete_record(input)
            .await
            .map(|_| ())
            .map_err(|error| format!("PDS deleteRecord failed: {error:?}"))
    }
}

impl<B: crate::identity::IdentityBackend + 'static> OAuthFlow for AtriumOAuthFlow<B> {
    fn authorize<'a>(
        &'a self,
        login: &'a str,
        browser_binding: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<String, String>> + Send + 'a>> {
        Box::pin(async move {
            let identity = if login.starts_with("did:") {
                self.identity_resolver
                    .resolve_did(login)
                    .await
                    .map_err(|error| error.to_string())?
            } else {
                self.identity_resolver
                    .resolve_handle(login)
                    .await
                    .map_err(|error| error.to_string())?
            };
            let state = serde_json::to_string(&ExpectedOAuthIdentity {
                did: identity.did,
                handle: identity.handle,
                browser_binding: browser_binding.to_owned(),
            })
            .map_err(|error| error.to_string())?;
            self.client
                .authorize(
                    login,
                    AuthorizeOptions {
                        state: Some(state),
                        scopes: requested_scopes(),
                        ..Default::default()
                    },
                )
                .await
                .map_err(|error| error.to_string())
        })
    }

    fn callback<'a>(
        &'a self,
        params: OAuthCallback,
    ) -> Pin<Box<dyn Future<Output = Result<OAuthAccount, String>> + Send + 'a>> {
        Box::pin(async move {
            let (session, app_state) = self
                .client
                .callback(atrium_oauth::CallbackParams {
                    code: params.code,
                    state: Some(params.state),
                    iss: params.issuer,
                })
                .await
                .map_err(|error| error.to_string())?;
            let expected: ExpectedOAuthIdentity = serde_json::from_str(
                app_state
                    .as_deref()
                    .ok_or_else(|| "OAuth state has no expected identity".to_owned())?,
            )
            .map_err(|error| error.to_string())?;
            let did = session
                .did()
                .await
                .ok_or_else(|| "OAuth session has no subject DID".to_owned())?;
            let did = did.as_ref().to_owned();
            let parsed_did = did.parse::<Did>().map_err(|error| error.to_string())?;
            if params.browser_binding.as_deref() != Some(expected.browser_binding.as_str()) {
                let _ = self.client.revoke(&parsed_did).await;
                let _ = self.store.del(&parsed_did).await;
                return Err("OAuth browser binding does not match the authorization state".into());
            }
            let stored_session = self
                .store
                .get(&parsed_did)
                .await
                .map_err(|error| error.to_string())?
                .ok_or_else(|| "OAuth callback did not persist its session".to_owned())?;
            let granted_scope = stored_session.token_set.scope;
            if !has_atproto_scope(granted_scope.as_deref()) {
                let _ = self.client.revoke(&parsed_did).await;
                let _ = self.store.del(&parsed_did).await;
                return Err("OAuth token response did not grant the atproto scope".into());
            }
            if did != expected.did {
                let _ = self.client.revoke(&parsed_did).await;
                let _ = self.store.del(&parsed_did).await;
                return Err("OAuth subject DID does not match the verified login identity".into());
            }
            Ok(OAuthAccount {
                did,
                verified_handle: expected.handle,
                granted_scope,
            })
        })
    }

    fn logout<'a>(
        &'a self,
        did: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move {
            let did = did.parse::<Did>().map_err(|error| error.to_string())?;
            self.client
                .revoke(&did)
                .await
                .map_err(|error| error.to_string())
        })
    }

    fn create_post<'a>(
        &'a self,
        did: &'a str,
        text: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<PdsCreatedRecord, String>> + Send + 'a>> {
        Box::pin(async move {
            self.create_record(
                did,
                "app.bsky.feed.post",
                json!({
                    "$type":"app.bsky.feed.post",
                    "text":text,
                    "createdAt":Datetime::now()
                }),
            )
            .await
        })
    }

    fn create_reply<'a>(
        &'a self,
        did: &'a str,
        text: &'a str,
        root: PostStrongRef,
        parent: PostStrongRef,
    ) -> Pin<Box<dyn Future<Output = Result<PdsCreatedRecord, String>> + Send + 'a>> {
        Box::pin(async move {
            self.create_record(
                did,
                "app.bsky.feed.post",
                json!({
                    "$type":"app.bsky.feed.post",
                    "text":text,
                    "createdAt":Datetime::now(),
                    "reply":{"root":root,"parent":parent}
                }),
            )
            .await
        })
    }

    fn create_quote<'a>(
        &'a self,
        did: &'a str,
        text: &'a str,
        quoted: PostStrongRef,
    ) -> Pin<Box<dyn Future<Output = Result<PdsCreatedRecord, String>> + Send + 'a>> {
        Box::pin(async move {
            self.create_record(
                did,
                "app.bsky.feed.post",
                json!({
                    "$type":"app.bsky.feed.post",
                    "text":text,
                    "createdAt":Datetime::now(),
                    "embed":{
                        "$type":"app.bsky.embed.record",
                        "record":quoted
                    }
                }),
            )
            .await
        })
    }

    fn create_post_with_images<'a>(
        &'a self,
        did: &'a str,
        text: &'a str,
        images: Vec<PostImage>,
    ) -> Pin<Box<dyn Future<Output = Result<PdsCreatedRecord, String>> + Send + 'a>> {
        Box::pin(async move {
            AtriumOAuthFlow::create_post_with_images(self, did, text, images, None, None).await
        })
    }

    fn create_post_with_external<'a>(
        &'a self,
        did: &'a str,
        text: &'a str,
        external: PostExternal,
    ) -> Pin<Box<dyn Future<Output = Result<PdsCreatedRecord, String>> + Send + 'a>> {
        Box::pin(async move {
            AtriumOAuthFlow::create_post_with_external(self, did, text, external, None, None).await
        })
    }

    fn create_post_with_video<'a>(
        &'a self,
        did: &'a str,
        text: &'a str,
        video: PostVideo,
        reply: Option<Value>,
        quote: Option<PostStrongRef>,
    ) -> Pin<Box<dyn Future<Output = Result<PdsCreatedRecord, String>> + Send + 'a>> {
        Box::pin(async move {
            AtriumOAuthFlow::create_post_with_video(self, did, text, video, reply, quote).await
        })
    }

    fn create_reply_with_images<'a>(
        &'a self,
        did: &'a str,
        text: &'a str,
        root: PostStrongRef,
        parent: PostStrongRef,
        images: Vec<PostImage>,
    ) -> Pin<Box<dyn Future<Output = Result<PdsCreatedRecord, String>> + Send + 'a>> {
        Box::pin(async move {
            AtriumOAuthFlow::create_post_with_images(
                self,
                did,
                text,
                images,
                Some(json!({"root":root,"parent":parent})),
                None,
            )
            .await
        })
    }

    fn create_reply_with_external<'a>(
        &'a self,
        did: &'a str,
        text: &'a str,
        root: PostStrongRef,
        parent: PostStrongRef,
        external: PostExternal,
    ) -> Pin<Box<dyn Future<Output = Result<PdsCreatedRecord, String>> + Send + 'a>> {
        Box::pin(async move {
            AtriumOAuthFlow::create_post_with_external(
                self,
                did,
                text,
                external,
                Some(json!({"root":root,"parent":parent})),
                None,
            )
            .await
        })
    }

    fn create_quote_with_images<'a>(
        &'a self,
        did: &'a str,
        text: &'a str,
        quoted: PostStrongRef,
        images: Vec<PostImage>,
    ) -> Pin<Box<dyn Future<Output = Result<PdsCreatedRecord, String>> + Send + 'a>> {
        Box::pin(async move {
            AtriumOAuthFlow::create_post_with_images(self, did, text, images, None, Some(quoted))
                .await
        })
    }

    fn create_quote_with_external<'a>(
        &'a self,
        did: &'a str,
        text: &'a str,
        quoted: PostStrongRef,
        external: PostExternal,
    ) -> Pin<Box<dyn Future<Output = Result<PdsCreatedRecord, String>> + Send + 'a>> {
        Box::pin(async move {
            AtriumOAuthFlow::create_post_with_external(
                self,
                did,
                text,
                external,
                None,
                Some(quoted),
            )
            .await
        })
    }

    fn fetch_post<'a>(
        &'a self,
        uri: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<IndexedRecord, String>> + Send + 'a>> {
        Box::pin(async move { self.feed_client.fetch_post(uri).await })
    }

    fn fetch_author_profile<'a>(
        &'a self,
        did: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<FetchedAuthorProfile, String>> + Send + 'a>> {
        Box::pin(async move {
            let document = self
                .identity_resolver
                .resolve_did_document(did)
                .await
                .map_err(|error| format!("author identity resolution failed: {error}"))?;
            let pds_endpoint = document
                .pds_endpoint()
                .ok_or_else(|| "author DID document has no valid PDS endpoint".to_owned())?;
            let verified_handle = self
                .identity_resolver
                .verified_handle_for_document(&document)
                .await
                .ok()
                .flatten();
            let mut url =
                reqwest::Url::parse(&format!("{pds_endpoint}/xrpc/com.atproto.repo.getRecord"))
                    .map_err(|_| "author PDS URL is invalid".to_owned())?;
            url.query_pairs_mut()
                .append_pair("repo", did)
                .append_pair("collection", "app.bsky.actor.profile")
                .append_pair("rkey", "self");
            let mut headers = http::HeaderMap::new();
            headers.insert(
                http::header::ACCEPT,
                http::HeaderValue::from_static("application/json"),
            );
            let response = self
                .http_client
                .get(&url, headers, 256 * 1024)
                .await
                .map_err(|error| format!("author PDS profile request failed: {error}"))?;
            let status = response.status();
            if status == http::StatusCode::NOT_FOUND {
                return Ok(FetchedAuthorProfile {
                    profile: None,
                    verified_handle,
                });
            }
            if !status.is_success() {
                return Err(format!("author PDS returned HTTP {status}"));
            }
            let value: Value = serde_json::from_slice(response.body())
                .map_err(|_| "author PDS returned invalid profile JSON".to_owned())?;
            let uri = value
                .get("uri")
                .and_then(Value::as_str)
                .ok_or_else(|| "author PDS profile has no URI".to_owned())?;
            let expected_uri = format!("at://{did}/app.bsky.actor.profile/self");
            if uri != expected_uri {
                return Err("author PDS returned a different profile record".to_owned());
            }
            let profile_record = value
                .get("value")
                .cloned()
                .ok_or_else(|| "author PDS profile has no record value".to_owned())?;
            let cid = value.get("cid").and_then(Value::as_str).map(str::to_owned);
            let time_us = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros()
                .min(u64::MAX as u128) as u64;
            Ok(FetchedAuthorProfile {
                profile: Some(IndexedRecord {
                    did: did.to_owned(),
                    collection: "app.bsky.actor.profile".to_owned(),
                    rkey: "self".to_owned(),
                    uri: expected_uri,
                    cid,
                    time_us,
                    record: profile_record,
                }),
                verified_handle,
            })
        })
    }

    fn resolve_author_handle<'a>(
        &'a self,
        did: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<String>, String>> + Send + 'a>> {
        Box::pin(async move {
            let document = self
                .identity_resolver
                .resolve_did_document(did)
                .await
                .map_err(|error| format!("author identity resolution failed: {error}"))?;
            self.identity_resolver
                .verified_handle_for_document(&document)
                .await
                .map_err(|error| format!("author handle verification failed: {error}"))
        })
    }

    fn like<'a>(
        &'a self,
        did: &'a str,
        uri: &'a str,
        cid: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<PdsCreatedRecord, String>> + Send + 'a>> {
        Box::pin(async move {
            self.create_record(
                did,
                "app.bsky.feed.like",
                json!({
                    "$type":"app.bsky.feed.like",
                    "subject":{"uri":uri,"cid":cid},
                    "createdAt":Datetime::now()
                }),
            )
            .await
        })
    }

    fn delete_like<'a>(
        &'a self,
        did: &'a str,
        like_uri: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move { self.delete_like_record(did, like_uri).await })
    }

    fn repost<'a>(
        &'a self,
        did: &'a str,
        uri: &'a str,
        cid: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<PdsCreatedRecord, String>> + Send + 'a>> {
        Box::pin(async move {
            self.create_record(
                did,
                "app.bsky.feed.repost",
                json!({
                    "$type":"app.bsky.feed.repost",
                    "subject":{"uri":uri,"cid":cid},
                    "createdAt":Datetime::now()
                }),
            )
            .await
        })
    }

    fn timeline<'a>(
        &'a self,
        did: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Value>, String>> + Send + 'a>> {
        Box::pin(async move {
            let did = did.parse::<Did>().map_err(|error| error.to_string())?;
            let session = self
                .client
                .restore(&did)
                .await
                .map_err(|error| error.to_string())?;
            let agent = Agent::new(session);
            let params = atrium_api::app::bsky::feed::get_timeline::ParametersData {
                algorithm: None,
                cursor: None,
                limit: 25u8.try_into().ok(),
            }
            .into();
            let output = agent
                .api
                .app
                .bsky
                .feed
                .get_timeline(params)
                .await
                .map_err(|error| format!("PDS getTimeline failed: {error:?}"))?;
            output
                .feed
                .iter()
                .map(|item| serde_json::to_value(item).map_err(|error| error.to_string()))
                .collect()
        })
    }

    fn pinned_feeds<'a>(
        &'a self,
        did: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResolvedFeed>, String>> + Send + 'a>> {
        Box::pin(async move {
            let started = std::time::Instant::now();
            let preferences = self.saved_feed_preferences(did).await?;
            let preferences_ms = started.elapsed().as_millis() as u64;
            let resolutions = futures_util::future::join_all(
                preferences
                    .iter()
                    .filter(|item| item.feed_type == "feed" && item.pinned)
                    .map(|preference| self.feed_client.resolve_feed(&preference.value)),
            )
            .await;
            let requested = resolutions.len();
            let feeds = resolutions.into_iter().flatten().collect::<Vec<_>>();
            tracing::debug!(
                pinned = requested,
                resolved = feeds.len(),
                preferences_ms,
                duration_ms = started.elapsed().as_millis() as u64,
                "pinned feed tabs loaded"
            );
            Ok(feeds)
        })
    }

    fn pinned_feed_tabs<'a>(
        &'a self,
        did: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<PinnedFeedTabs, String>> + Send + 'a>> {
        Box::pin(async move {
            let preferences_started = std::time::Instant::now();
            let preferences = self.saved_feed_preferences_for_tabs(did).await;
            let preferences_ms = preferences_started.elapsed().as_millis() as u64;
            let uris = preferences
                .into_iter()
                .filter(|item| item.feed_type == "feed" && item.pinned)
                .map(|item| item.value)
                .collect::<Vec<_>>();
            let tabs_started = std::time::Instant::now();
            let tabs = futures_util::future::join_all(
                uris.iter().map(|uri| self.feed_client.feed_for_tab(uri)),
            )
            .await;
            Ok(PinnedFeedTabs {
                feeds: tabs,
                preferences_ms,
                tabs_ms: tabs_started.elapsed().as_millis() as u64,
            })
        })
    }

    fn feed_timeline<'a>(
        &'a self,
        did: &'a str,
        uri: &'a str,
        cursor: Option<&'a str>,
        storage: &'a dyn Storage,
    ) -> Pin<Box<dyn Future<Output = Result<FeedTimeline, String>> + Send + 'a>> {
        Box::pin(async move {
            let key = (
                did.to_owned(),
                uri.to_owned(),
                cursor.unwrap_or_default().to_owned(),
            );
            if let Some((expires, page)) = self.feed_page_cache.lock().await.get(&key).cloned()
                && expires > tokio::time::Instant::now()
            {
                let mut page = page;
                page.timing = FeedTimelineTiming {
                    cache_hit: true,
                    ..FeedTimelineTiming::default()
                };
                return Ok(page);
            }
            let resolve_started = std::time::Instant::now();
            let generator = self.feed_client.resolve_feed(uri).await?;
            let resolve_ms = resolve_started.elapsed().as_millis() as u64;
            let skeleton_started = std::time::Instant::now();
            let mut skeleton_cache_hit = false;
            let mut auth_ms = 0;
            let skeleton = if let Some((expires, skeleton)) =
                self.feed_skeleton_cache.lock().await.get(&key).cloned()
                && expires > tokio::time::Instant::now()
            {
                skeleton_cache_hit = true;
                skeleton
            } else {
                let service_jwt = if did.is_empty() {
                    None
                } else {
                    let auth_started = std::time::Instant::now();
                    let token = self
                        .service_auth_token(
                            did,
                            &generator.service_did,
                            "app.bsky.feed.getFeedSkeleton",
                        )
                        .await
                        .ok();
                    auth_ms = auth_started.elapsed().as_millis() as u64;
                    token
                };
                let skeleton = self
                    .feed_client
                    .get_skeleton(&generator, cursor, service_jwt.as_deref())
                    .await?;
                let mut cache = self.feed_skeleton_cache.lock().await;
                cache.retain(|_, (expires, _)| *expires > tokio::time::Instant::now());
                if cache.len() < 1024 {
                    cache.insert(
                        key.clone(),
                        (
                            tokio::time::Instant::now() + Duration::from_secs(FEED_PAGE_CACHE_SECS),
                            skeleton.clone(),
                        ),
                    );
                }
                skeleton
            };
            let skeleton_ms = skeleton_started.elapsed().as_millis() as u64;
            let hydration_started = std::time::Instant::now();
            let timeline = self.feed_client.hydrate_skeleton(skeleton, storage).await?;
            tracing::debug!(
                feed = uri,
                resolve_ms,
                auth_ms,
                skeleton_ms,
                hydrate_ms = hydration_started.elapsed().as_millis() as u64,
                posts = timeline.feed.len(),
                dropped = timeline.dropped_count,
                "feed page loaded"
            );
            let page = FeedTimeline {
                generator,
                timeline,
                timing: FeedTimelineTiming {
                    session_token_refresh_ms: auth_ms,
                    timeline_fetch_live_ms: resolve_ms.saturating_add(skeleton_ms),
                    skeleton_hydration_ms: hydration_started.elapsed().as_millis() as u64,
                    cache_hit: false,
                    skeleton_cache_hit,
                },
            };
            let mut cache = self.feed_page_cache.lock().await;
            cache.retain(|_, (expires, _)| *expires > tokio::time::Instant::now());
            if cache.len() < 1024 {
                cache.insert(
                    key,
                    (
                        tokio::time::Instant::now() + Duration::from_secs(FEED_PAGE_CACHE_SECS),
                        page.clone(),
                    ),
                );
            }
            Ok(page)
        })
    }
}

#[derive(Clone)]
struct CloudflareDnsTxtResolver {
    client: SafeHttpClient,
}

impl DnsTxtResolver for CloudflareDnsTxtResolver {
    async fn resolve(
        &self,
        query: &str,
    ) -> Result<Vec<String>, Box<dyn std::error::Error + Send + Sync + 'static>> {
        #[derive(Deserialize)]
        struct DohResponse {
            #[serde(default, rename = "Answer")]
            answers: Vec<DohAnswer>,
        }
        #[derive(Deserialize)]
        struct DohAnswer {
            data: String,
        }
        let mut url = reqwest::Url::parse("https://cloudflare-dns.com/dns-query")?;
        url.query_pairs_mut()
            .append_pair("name", query)
            .append_pair("type", "TXT");
        let mut headers = http::HeaderMap::new();
        headers.insert(
            http::header::ACCEPT,
            http::HeaderValue::from_static("application/dns-json"),
        );
        let response = self
            .client
            .get(&url, headers, 64 * 1024)
            .await
            .map_err(std::io::Error::other)?;
        if !response.status().is_success() {
            return Err(std::io::Error::other("DNS service returned an error status").into());
        }
        let response = serde_json::from_slice::<DohResponse>(response.body())?;
        Ok(response
            .answers
            .into_iter()
            .map(|answer| answer.data.trim_matches('"').to_owned())
            .collect())
    }
}

async fn metadata<F, S>(State(state): State<Arc<OAuthWebState<F, S>>>) -> Json<Value>
where
    F: OAuthFlow,
    S: Storage + 'static,
{
    Json(state.public.metadata.clone())
}

async fn alt_test_page<F, S>(
    State(state): State<Arc<OAuthWebState<F, S>>>,
    headers: HeaderMap,
) -> Response
where
    F: OAuthFlow,
    S: Storage + 'static,
{
    if verified_cookie_did(&state.public, &headers, state.storage.as_ref()).is_none() {
        return Redirect::to("/login").into_response();
    }
    let model_status = if state.alt_model_dir.is_some() {
        format!(
            "Model files: {ALT_MODEL_DOWNLOAD_BYTES} bytes. The first run downloads them from this AppView; the photo stays in this browser."
        )
    } else {
        "On-device captioning is not configured on this AppView.".to_owned()
    };
    let content = format!(
        "<section class=\"alt-test-page\"><h1>On-device caption test</h1><p>Choose a photo and run a caption test. The image is processed in this browser and is never uploaded.</p><p data-alt-model-size>{model_status}</p><form data-alt-test><label for=\"alt-test-image\">Photo to caption</label><input id=\"alt-test-image\" type=\"file\" accept=\"image/*\" data-alt-test-image required><button type=\"submit\" data-alt-test-run{}>Run caption test</button></form><progress data-alt-test-progress max=\"{ALT_MODEL_DOWNLOAD_BYTES}\" value=\"0\" hidden></progress><p data-alt-test-status role=\"status\" aria-live=\"polite\">No image has been processed.</p><dl><dt>Backend</dt><dd data-alt-test-backend>—</dd><dt>Download bytes</dt><dd data-alt-test-bytes>—</dd><dt>Model load</dt><dd data-alt-test-load-ms>—</dd><dt>Caption time</dt><dd data-alt-test-caption-ms>—</dd><dt>Caption</dt><dd data-alt-test-caption>—</dd></dl></section>",
        if state.alt_model_dir.is_some() {
            ""
        } else {
            " disabled"
        }
    );
    Html(page_shell("On-device caption test", &content, true)).into_response()
}

/// Optional progressive enhancement (like toggles in place). Every action still works as a plain form without it.
async fn app_script() -> Response {
    let mut response = include_str!("../static/app.js").into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/javascript; charset=utf-8"),
    );
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("public, max-age=31536000, immutable"),
    );
    response
}

async fn alt_model_worker() -> Response {
    static_asset_response(
        include_bytes!("../static/alt-model-worker.js"),
        "text/javascript; charset=utf-8",
    )
}

async fn vendor_asset(AxumPath(path): AxumPath<String>) -> Response {
    let (bytes, content_type): (&'static [u8], &'static str) = match path.as_str() {
        "transformers-4.3.2.min.js" => (
            include_bytes!("../static/vendor/transformers-4.3.2.min.js"),
            "text/javascript; charset=utf-8",
        ),
        "hls.min.js" => (
            include_bytes!("../static/vendor/hls.min.js"),
            "text/javascript; charset=utf-8",
        ),
        "ort-wasm-simd-threaded.mjs" => (
            include_bytes!("../static/vendor/ort-wasm-simd-threaded.mjs"),
            "text/javascript; charset=utf-8",
        ),
        "ort-wasm-simd-threaded.wasm" => (
            include_bytes!("../static/vendor/ort-wasm-simd-threaded.wasm"),
            "application/wasm",
        ),
        "ort-wasm-simd-threaded.asyncify.mjs" => (
            include_bytes!("../static/vendor/ort-wasm-simd-threaded.asyncify.mjs"),
            "text/javascript; charset=utf-8",
        ),
        "ort-wasm-simd-threaded.asyncify.wasm" => (
            include_bytes!("../static/vendor/ort-wasm-simd-threaded.asyncify.wasm"),
            "application/wasm",
        ),
        "ort-wasm-simd-threaded.jsep.mjs" => (
            include_bytes!("../static/vendor/ort-wasm-simd-threaded.jsep.mjs"),
            "text/javascript; charset=utf-8",
        ),
        "ort-wasm-simd-threaded.jsep.wasm" => (
            include_bytes!("../static/vendor/ort-wasm-simd-threaded.jsep.wasm"),
            "application/wasm",
        ),
        "LICENSE-transformers-js" => (
            include_bytes!("../static/vendor/LICENSE-transformers-js"),
            "text/plain; charset=utf-8",
        ),
        "LICENSE-hls.js" => (
            include_bytes!("../static/vendor/LICENSE-hls.js"),
            "text/plain; charset=utf-8",
        ),
        "LICENSE-onnxruntime-web" => (
            include_bytes!("../static/vendor/LICENSE-onnxruntime-web"),
            "text/plain; charset=utf-8",
        ),
        _ => return StatusCode::NOT_FOUND.into_response(),
    };
    static_asset_response(bytes, content_type)
}

fn static_asset_response(bytes: &'static [u8], content_type: &'static str) -> Response {
    let mut response = Response::new(Body::from(Bytes::from_static(bytes)));
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    response.headers_mut().insert(
        header::CONTENT_LENGTH,
        HeaderValue::from(bytes.len() as u64),
    );
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static(IMMUTABLE_CACHE),
    );
    response
}

async fn model_asset<F, S>(
    State(state): State<Arc<OAuthWebState<F, S>>>,
    AxumPath(path): AxumPath<String>,
) -> Response
where
    F: OAuthFlow,
    S: Storage + 'static,
{
    use tokio::io::AsyncReadExt;

    let Some(model_root) = state.alt_model_dir.as_deref() else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let relative = Path::new(&path);
    if relative
        .components()
        .any(|component| !matches!(component, Component::Normal(_)))
        || relative.components().next() != Some(Component::Normal("alt-caption-v1".as_ref()))
    {
        return StatusCode::NOT_FOUND.into_response();
    }
    let Ok(root) = tokio::fs::canonicalize(model_root).await else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let Ok(target) = tokio::fs::canonicalize(root.join(relative)).await else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if !target.starts_with(&root) {
        return StatusCode::NOT_FOUND.into_response();
    }
    let Ok(metadata) = tokio::fs::metadata(&target).await else {
        return StatusCode::NOT_FOUND.into_response();
    };
    if !metadata.is_file() {
        return StatusCode::NOT_FOUND.into_response();
    }
    let Ok(file) = tokio::fs::File::open(&target).await else {
        return StatusCode::NOT_FOUND.into_response();
    };
    let content_type = match target.extension().and_then(|value| value.to_str()) {
        Some("json") => "application/json",
        Some("txt") => "text/plain; charset=utf-8",
        Some("onnx") | Some("bin") => "application/octet-stream",
        Some("wasm") => "application/wasm",
        _ => "application/octet-stream",
    };
    let body_stream = stream::try_unfold(file, |mut file| async move {
        let mut buffer = vec![0; 64 * 1024];
        let read = file.read(&mut buffer).await?;
        if read == 0 {
            Ok::<_, std::io::Error>(None)
        } else {
            buffer.truncate(read);
            Ok(Some((Bytes::from(buffer), file)))
        }
    });
    let mut response = Response::new(Body::from_stream(body_stream));
    response
        .headers_mut()
        .insert(header::CONTENT_TYPE, HeaderValue::from_static(content_type));
    response
        .headers_mut()
        .insert(header::CONTENT_LENGTH, HeaderValue::from(metadata.len()));
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static(IMMUTABLE_CACHE),
    );
    response
}

async fn app_stylesheet() -> Response {
    let mut response = include_str!("../static/app.css").into_response();
    response.headers_mut().insert(
        header::CONTENT_TYPE,
        HeaderValue::from_static("text/css; charset=utf-8"),
    );
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("public, max-age=31536000, immutable"),
    );
    response
}

fn page_shell(title: &str, content: &str, authenticated: bool) -> String {
    page_shell_with_feeds(title, content, authenticated, &[], None, false)
}

fn page_shell_with_feeds(
    title: &str,
    content: &str,
    authenticated: bool,
    feeds: &[ResolvedFeed],
    active_feed: Option<&str>,
    following_active: bool,
) -> String {
    page_shell_with_feeds_and_badge(
        title,
        content,
        authenticated,
        feeds,
        active_feed,
        following_active,
        0,
    )
}

fn page_shell_with_feeds_and_badge(
    title: &str,
    content: &str,
    authenticated: bool,
    feeds: &[ResolvedFeed],
    active_feed: Option<&str>,
    following_active: bool,
    unread_count: usize,
) -> String {
    page_shell_with_options(
        title,
        content,
        authenticated,
        PageShellOptions {
            feeds,
            active_feed,
            following_active,
            show_feed_tabs: true,
            mobile_navigation_override: None,
            unread_count,
        },
    )
}

struct PageShellOptions<'a> {
    feeds: &'a [ResolvedFeed],
    active_feed: Option<&'a str>,
    following_active: bool,
    show_feed_tabs: bool,
    mobile_navigation_override: Option<&'a str>,
    unread_count: usize,
}

fn page_shell_with_options(
    title: &str,
    content: &str,
    authenticated: bool,
    options: PageShellOptions<'_>,
) -> String {
    let PageShellOptions {
        feeds,
        active_feed,
        following_active,
        show_feed_tabs,
        mobile_navigation_override,
        unread_count,
    } = options;
    let navigation = if authenticated {
        format!(
            "<nav class=\"primary-nav\" aria-label=\"Main navigation\"><a href=\"/app\">{}<span>Home</span></a><a href=\"/feeds\">{}<span>Feeds</span></a><a class=\"notification-nav-link\" href=\"/notifications\">{}<span>Notifications</span>{}</a><a href=\"/profile\">{}<span>Profile</span></a><a class=\"compose-link\" href=\"/app#new-post\">{}<span>New post</span></a><form action=\"/logout\" method=\"post\"><button type=\"submit\">Sign out</button></form></nav>",
            icon_svg("home"),
            icon_svg("feeds"),
            icon_svg("notifications"),
            notification_badge_markup(unread_count),
            icon_svg("profile"),
            icon_svg("compose")
        )
    } else {
        "<nav class=\"primary-nav\" aria-label=\"Main navigation\"><a href=\"/login\">Sign in</a></nav>".to_owned()
    };
    let tabs = if authenticated && show_feed_tabs {
        render_feed_tabs(feeds, active_feed, following_active)
    } else {
        String::new()
    };
    let right_sidebar = if authenticated {
        render_right_sidebar(feeds)
    } else {
        String::new()
    };
    let mobile_navigation = if authenticated {
        mobile_navigation_override
            .map(str::to_owned)
            .unwrap_or_else(|| {
                format!(
                    "<nav class=\"mobile-nav\" aria-label=\"Mobile navigation\"><a href=\"/app\">{}<span>Home</span></a><a href=\"/feeds\">{}<span>Feeds</span></a><a class=\"notification-nav-link\" href=\"/notifications\">{}<span>Alerts</span>{}</a><a href=\"/profile\">{}<span>Profile</span></a></nav><a class=\"mobile-compose\" href=\"/app#new-post\" aria-label=\"New post\">{}</a>",
                    icon_svg("home"),
                    icon_svg("feeds"),
                    icon_svg("notifications"),
                    notification_badge_markup(unread_count),
                    icon_svg("profile"),
                    icon_svg("compose")
                )
            })
    } else {
        String::new()
    };
    let layout = if authenticated {
        "app-layout"
    } else {
        "app-layout guest-layout"
    };
    format!(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1, viewport-fit=cover\"><title>{}</title><link rel=\"stylesheet\" href=\"{}\"><script src=\"{}\" defer></script><script src=\"{}\" defer></script></head><body>{}<a class=\"skip-link\" href=\"#main-content\">Skip to content</a><div class=\"{layout}\"><aside class=\"left-sidebar\"><a class=\"brand\" href=\"/app\">AppView</a>{navigation}</aside><div class=\"center-column\"><header class=\"center-sticky\">{tabs}</header><main id=\"main-content\" class=\"page-content\">{content}</main></div>{right_sidebar}</div>{mobile_navigation}</body></html>",
        html_escape(title),
        WEB_STYLESHEET_URL,
        HLS_SCRIPT_URL,
        WEB_SCRIPT_URL,
        icon_sprite()
    )
}

fn notification_badge_markup(unread_count: usize) -> String {
    let hidden = if unread_count == 0 { " hidden" } else { "" };
    let display = if unread_count > 9 {
        "9+".to_owned()
    } else {
        unread_count.to_string()
    };
    format!(
        "<span class=\"notification-badge\" data-notification-badge aria-label=\"{unread_count} unread notifications\" aria-live=\"polite\"{hidden}>{display}</span>"
    )
}

fn unread_badge_count<S: Storage>(storage: &S, did: &str) -> usize {
    storage
        .unread_notification_count(did)
        .unwrap_or_else(|error| {
            tracing::warn!(%error, %did, "could not read notification badge count");
            0
        })
}

fn render_feed_tabs(
    feeds: &[ResolvedFeed],
    active_feed: Option<&str>,
    following_active: bool,
) -> String {
    let following_current = if following_active {
        " aria-current=\"page\""
    } else {
        ""
    };
    let mut tabs = format!(
        "<nav class=\"feed-tabs\" aria-label=\"Feed tabs\"><a class=\"feed-tab{}\" href=\"/app\"{following_current}>Following</a>",
        if following_active { " is-active" } else { "" }
    );
    for feed in feeds {
        let active = active_feed == Some(feed.uri.as_str());
        let current = if active { " aria-current=\"page\"" } else { "" };
        tabs.push_str(&format!(
            "<a class=\"feed-tab{}\" href=\"{}\"{current}>{}</a>",
            if active { " is-active" } else { "" },
            feed_link_href(feed),
            html_escape(&feed.display_name)
        ));
    }
    tabs.push_str("</nav>");
    tabs
}

fn render_right_sidebar(feeds: &[ResolvedFeed]) -> String {
    let feed_links = if feeds.is_empty() {
        "<li><a href=\"/feeds\">Explore saved feeds</a></li>".to_owned()
    } else {
        feeds
            .iter()
            .map(|feed| {
                format!(
                    "<li><a href=\"{}\">{}</a></li>",
                    feed_link_href(feed),
                    html_escape(&feed.display_name)
                )
            })
            .collect::<String>()
    };
    format!(
        "<aside class=\"right-sidebar\"><form class=\"search-form\" role=\"search\"><label class=\"sr-only\" for=\"site-search\">Search</label><span aria-hidden=\"true\">{}</span><input id=\"site-search\" type=\"search\" placeholder=\"Search\" disabled></form><section class=\"sidebar-section\"><h2>Feeds</h2><ul>{feed_links}</ul><a class=\"sidebar-more\" href=\"/feeds\">All feeds</a></section></aside>",
        icon_svg("search")
    )
}

async fn pinned_feeds_for_layout<F: OAuthFlow>(
    flow: &F,
    did: &str,
    has_permission: bool,
) -> PinnedFeedTabs {
    if !has_permission {
        return PinnedFeedTabs::default();
    }
    match flow.pinned_feed_tabs(did).await {
        Ok(tabs) => tabs,
        Err(error) => {
            tracing::warn!(%error, "could not load pinned feed tabs");
            PinnedFeedTabs::default()
        }
    }
}

#[derive(Default)]
struct PageProfileHydration {
    authors_needed: usize,
    from_index: usize,
    fetched_live: usize,
    timed_out: usize,
}

#[derive(Clone, Debug)]
enum QuoteRenderState {
    Found(IndexedRecord),
    NotFound,
    Unavailable,
    Pending,
    Other(Option<IndexedRecord>),
}

#[derive(Clone, Debug)]
struct QuotedRecordRef {
    uri: String,
    unavailable: bool,
}

type QuoteRenderIndex = HashMap<String, QuoteRenderState>;

struct PageContentHydration {
    quote_index: QuoteRenderIndex,
    profile_hydration_ms: u64,
    quote_hydration_ms: u64,
}

type PageParentIndex = HashMap<String, Option<IndexedRecord>>;

#[derive(Default)]
struct ThreadAncestorStats {
    from_index: usize,
    live: usize,
    timed_out: usize,
}

#[derive(Clone, Copy)]
enum AncestorSource {
    Index,
    Live,
}

struct WebPageTiming {
    page: &'static str,
    started: std::time::Instant,
    session_token_refresh_ms: u64,
    preferences_ms: u64,
    tabs_ms: u64,
    timeline_fetch_index_ms: u64,
    timeline_fetch_live_ms: u64,
    parent_hydration_ms: u64,
    profile_hydration_ms: u64,
    quote_hydration_ms: u64,
    render_ms: u64,
    ancestors_from_index: usize,
    ancestors_live: usize,
    ancestors_timed_out: usize,
}

impl WebPageTiming {
    fn new(page: &'static str) -> Self {
        Self {
            page,
            started: std::time::Instant::now(),
            session_token_refresh_ms: 0,
            preferences_ms: 0,
            tabs_ms: 0,
            timeline_fetch_index_ms: 0,
            timeline_fetch_live_ms: 0,
            parent_hydration_ms: 0,
            profile_hydration_ms: 0,
            quote_hydration_ms: 0,
            render_ms: 0,
            ancestors_from_index: 0,
            ancestors_live: 0,
            ancestors_timed_out: 0,
        }
    }
}

impl Drop for WebPageTiming {
    fn drop(&mut self) {
        tracing::info!(
            page = self.page,
            session_token_refresh_ms = self.session_token_refresh_ms,
            preferences_ms = self.preferences_ms,
            tabs_ms = self.tabs_ms,
            timeline_fetch_index_ms = self.timeline_fetch_index_ms,
            timeline_fetch_live_ms = self.timeline_fetch_live_ms,
            parent_hydration_ms = self.parent_hydration_ms,
            profile_hydration_ms = self.profile_hydration_ms,
            quote_hydration_ms = self.quote_hydration_ms,
            render_ms = self.render_ms,
            ancestors_from_index = self.ancestors_from_index,
            ancestors_live = self.ancestors_live,
            ancestors_timed_out = self.ancestors_timed_out,
            total_ms = self.started.elapsed().as_millis() as u64,
            "web page phases"
        );
    }
}

pub struct ProfileRefreshWorker<F, S> {
    flow: F,
    storage: Arc<S>,
    poll_interval: Duration,
}

impl<F: OAuthFlow, S: Storage + 'static> ProfileRefreshWorker<F, S> {
    pub fn new(flow: F, storage: Arc<S>, poll_interval: Duration) -> Self {
        Self {
            flow,
            storage,
            poll_interval: poll_interval.max(Duration::from_millis(1)),
        }
    }

    pub async fn refresh_once(&self) -> Result<usize, StorageError> {
        let pending = self
            .storage
            .pending_profile_fetches(PAGE_PROFILE_CONCURRENCY)?;
        let mut completed = 0;
        let now_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis()
            .min(i64::MAX as u128) as i64;
        for did in pending {
            let status = self.storage.profile_index_snapshot()?.get(&did).copied();
            let fresh = status.is_some_and(|entry| {
                let age = now_ms.saturating_sub(entry.fetched_at_ms).max(0);
                (entry.has_profile && age < PROFILE_MAX_AGE.as_millis() as i64)
                    || (!entry.has_profile && age < PROFILE_NEGATIVE_TTL.as_millis() as i64)
            });
            if !fresh {
                match self.flow.fetch_author_profile(&did).await {
                    Ok(fetched) => {
                        if let Some(profile) = fetched.profile {
                            let expected_uri = format!("at://{did}/app.bsky.actor.profile/self");
                            if profile.did == did
                                && profile.collection == "app.bsky.actor.profile"
                                && profile.rkey == "self"
                                && profile.uri == expected_uri
                            {
                                self.storage.put_record(&profile)?;
                            } else {
                                self.storage.set_profile_index_entry(
                                    &did,
                                    ProfileIndexEntry {
                                        has_profile: false,
                                        fetched_at_ms: now_ms,
                                    },
                                )?;
                            }
                        } else {
                            self.storage.set_profile_index_entry(
                                &did,
                                ProfileIndexEntry {
                                    has_profile: false,
                                    fetched_at_ms: now_ms,
                                },
                            )?;
                        }
                        if let Some(handle) = fetched.verified_handle {
                            self.storage.set_verified_handle(&did, &handle)?;
                        } else {
                            self.storage.queue_handle_refresh(&did)?;
                        }
                    }
                    Err(error) => {
                        tracing::debug!(%did, %error, "background author profile fetch failed");
                        self.storage.set_profile_index_entry(
                            &did,
                            ProfileIndexEntry {
                                has_profile: false,
                                fetched_at_ms: now_ms,
                            },
                        )?;
                        self.storage.queue_handle_refresh(&did)?;
                    }
                }
            }
            self.storage.complete_profile_fetch(&did)?;
            completed += 1;
        }
        Ok(completed)
    }

    pub async fn run(
        &self,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Result<(), StorageError> {
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            if let Err(error) = self.refresh_once().await {
                tracing::warn!(%error, "background author profile fetch failed");
            }
            tokio::select! {
                _ = tokio::time::sleep(self.poll_interval) => {},
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
            }
        }
    }
}

async fn hydrate_page_profiles<F: OAuthFlow, S: Storage + 'static>(
    flow: &F,
    storage: Arc<S>,
    author_order: impl IntoIterator<Item = String>,
    deadline: tokio::time::Instant,
) -> Result<PageProfileHydration, StorageError> {
    let mut seen = HashSet::new();
    let authors = author_order
        .into_iter()
        .filter(|did| did.starts_with("did:") && seen.insert(did.clone()))
        .collect::<Vec<_>>();
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64;
    let index = storage.profile_index_snapshot()?;
    let mut hydration = PageProfileHydration {
        authors_needed: authors.len(),
        ..PageProfileHydration::default()
    };
    let mut candidates = Vec::new();
    for did in &authors {
        let status = index.get(did);
        let age_ms = status.map_or(i64::MAX, |entry| {
            now_ms.saturating_sub(entry.fetched_at_ms).max(0)
        });
        let profile_is_fresh = status
            .is_some_and(|entry| entry.has_profile && age_ms < PROFILE_MAX_AGE.as_millis() as i64);
        let profile_is_negative_cached = status.is_some_and(|entry| {
            !entry.has_profile && age_ms < PROFILE_NEGATIVE_TTL.as_millis() as i64
        });
        if profile_is_fresh {
            hydration.from_index += 1;
        }
        let profile_needs_fetch = !profile_is_fresh && !profile_is_negative_cached;
        let has_handle = storage.verified_handle(did)?.is_some();
        let handle_is_invalid = storage.handle_is_invalid(did)?;
        let handle_needs_fetch = !has_handle && !handle_is_invalid;
        if profile_needs_fetch || handle_needs_fetch {
            candidates.push((did.clone(), profile_needs_fetch, handle_needs_fetch));
        }
    }

    let candidate_dids = candidates
        .iter()
        .map(|(did, _, _)| did.clone())
        .collect::<Vec<_>>();
    let handle_candidate_dids = candidates
        .iter()
        .filter(|(_, _, needs_handle)| *needs_handle)
        .map(|(did, _, _)| did.clone())
        .collect::<HashSet<_>>();
    let flow = flow.clone();
    let mut fetches = futures_util::stream::iter(candidates.into_iter().map(
        move |(did, profile_needs_fetch, handle_needs_fetch)| {
            let flow = flow.clone();
            async move {
                let profile = if profile_needs_fetch {
                    Some(flow.fetch_author_profile(&did).await)
                } else {
                    None
                };
                let mut handle = profile
                    .as_ref()
                    .and_then(|result| result.as_ref().ok())
                    .and_then(|fetched| fetched.verified_handle.clone());
                let handle_result = if handle_needs_fetch && handle.is_none() {
                    Some(flow.resolve_author_handle(&did).await)
                } else {
                    None
                };
                if let Some(Ok(resolved)) = &handle_result {
                    handle = resolved.clone();
                }
                (did, profile, handle, handle_result)
            }
        },
    ))
    .buffer_unordered(PAGE_PROFILE_CONCURRENCY);
    let mut completed = HashSet::new();
    let mut unresolved_handle_set = HashSet::new();
    let storage_for_fetch = storage.clone();
    let fetch_result = tokio::time::timeout_at(deadline, async {
        while let Some((did, profile_result, handle, handle_result)) = fetches.next().await {
            completed.insert(did.clone());
            if let Some(result) = profile_result {
                match result {
                    Ok(fetched) => {
                        hydration.fetched_live += 1;
                        if let Some(profile) = fetched.profile {
                            let expected_uri = format!("at://{did}/app.bsky.actor.profile/self");
                            if profile.did != did
                                || profile.collection != "app.bsky.actor.profile"
                                || profile.rkey != "self"
                                || profile.uri != expected_uri
                            {
                                storage_for_fetch.set_profile_index_entry(
                                    &did,
                                    ProfileIndexEntry {
                                        has_profile: false,
                                        fetched_at_ms: now_ms,
                                    },
                                )?;
                            } else {
                                storage_for_fetch.put_record(&profile)?;
                            }
                        } else {
                            storage_for_fetch.set_profile_index_entry(
                                &did,
                                ProfileIndexEntry {
                                    has_profile: false,
                                    fetched_at_ms: now_ms,
                                },
                            )?;
                        }
                    }
                    Err(error) => {
                        tracing::debug!(%did, %error, "live author profile fetch failed");
                        storage_for_fetch.set_profile_index_entry(
                            &did,
                            ProfileIndexEntry {
                                has_profile: false,
                                fetched_at_ms: now_ms,
                            },
                        )?;
                    }
                }
            }
            if let Some(handle) = handle {
                storage_for_fetch.set_verified_handle(&did, &handle)?;
            } else if handle_result.is_some() && !storage_for_fetch.handle_is_invalid(&did)? {
                unresolved_handle_set.insert(did.clone());
            }
        }
        Ok::<(), StorageError>(())
    })
    .await;
    match fetch_result {
        Ok(result) => result?,
        Err(_) => {
            hydration.timed_out = candidate_dids
                .iter()
                .filter(|did| !completed.contains(*did))
                .count();
            let timed_out_authors = candidate_dids
                .iter()
                .filter(|did| !completed.contains(*did))
                .cloned()
                .collect::<Vec<_>>();
            let timed_out_profiles = candidates_profile_fetches(&authors, &index, now_ms);
            let timed_out_profiles = timed_out_profiles
                .into_iter()
                .filter(|did| timed_out_authors.contains(did))
                .collect::<Vec<_>>();
            storage.enqueue_profile_fetches(&timed_out_profiles)?;
            unresolved_handle_set.extend(
                timed_out_authors
                    .into_iter()
                    .filter(|did| handle_candidate_dids.contains(did)),
            );
        }
    }
    let unresolved_handles = authors
        .iter()
        .filter(|did| unresolved_handle_set.contains(*did))
        .cloned()
        .collect::<Vec<_>>();
    storage.queue_handle_refresh_ordered(&unresolved_handles)?;
    tracing::debug!(
        authors_needed = hydration.authors_needed,
        from_index = hydration.from_index,
        fetched_live = hydration.fetched_live,
        timed_out = hydration.timed_out,
        "hydrated page author profiles"
    );
    Ok(hydration)
}

fn candidates_profile_fetches(
    authors: &[String],
    index: &HashMap<String, ProfileIndexEntry>,
    now_ms: i64,
) -> Vec<String> {
    authors
        .iter()
        .filter(|did| {
            let status = index.get(*did);
            let age_ms = status.map_or(i64::MAX, |entry| {
                now_ms.saturating_sub(entry.fetched_at_ms).max(0)
            });
            !status.is_some_and(|entry| {
                (entry.has_profile && age_ms < PROFILE_MAX_AGE.as_millis() as i64)
                    || (!entry.has_profile && age_ms < PROFILE_NEGATIVE_TTL.as_millis() as i64)
            })
        })
        .cloned()
        .collect()
}

async fn hydrate_page_content<F: OAuthFlow, S: Storage + 'static>(
    flow: &F,
    storage: Arc<S>,
    page_records: &[&IndexedRecord],
    leading_authors: impl IntoIterator<Item = String>,
    deadline: tokio::time::Instant,
) -> Result<PageContentHydration, StorageError> {
    let mut refs = Vec::new();
    let mut seen_refs = HashSet::new();
    for record in page_records {
        if let Some(embed) = record.record.get("embed")
            && let Some(reference) = quoted_record_ref(embed)
            && seen_refs.insert(reference.uri.clone())
        {
            refs.push(reference);
        }
    }

    let mut authors = leading_authors.into_iter().collect::<Vec<_>>();
    for (record, quote_ref) in page_records.iter().zip(
        page_records
            .iter()
            .map(|record| record.record.get("embed").and_then(quoted_record_ref)),
    ) {
        authors.push(record.did.clone());
        if let Some(quote_ref) = quote_ref
            && !quote_ref.unavailable
            && let Some(did) = post_uri_did(&quote_ref.uri)
        {
            authors.push(did.to_owned());
        }
    }

    let quote_storage = storage.clone();
    let profile_flow = flow.clone();
    let profile_task = tokio::spawn(async move {
        let result = hydrate_page_profiles(&profile_flow, storage, authors, deadline).await;
        if let Err(error) = &result {
            tracing::warn!(%error, "background page profile hydration failed");
        }
        result
    });
    tokio::task::yield_now().await;
    let profile_fast_path = async move {
        let started = std::time::Instant::now();
        match profile_task.await {
            Ok(result) => result.map(|_| started.elapsed().as_millis() as u64),
            Err(error) => {
                tracing::warn!(%error, "page profile hydration task failed");
                Ok(started.elapsed().as_millis() as u64)
            }
        }
    };
    let quote_hydration = async {
        let started = std::time::Instant::now();
        let quotes = hydrate_page_quotes(flow, quote_storage.as_ref(), refs, deadline).await;
        (quotes, started.elapsed().as_millis() as u64)
    };
    let (quote_hydration, profile_hydration_ms) = tokio::join!(quote_hydration, profile_fast_path);
    Ok(PageContentHydration {
        quote_index: quote_hydration.0?,
        profile_hydration_ms: profile_hydration_ms?,
        quote_hydration_ms: quote_hydration.1,
    })
}

async fn hydrate_page_parents<F: OAuthFlow, S: Storage>(
    flow: &F,
    storage: &S,
    page_records: &[&IndexedRecord],
    deadline: tokio::time::Instant,
) -> Result<PageParentIndex, StorageError> {
    let mut parents = PageParentIndex::new();
    let mut parent_uris = page_records
        .iter()
        .filter_map(|record| {
            record
                .record
                .pointer("/reply/parent/uri")
                .and_then(Value::as_str)
                .filter(|uri| post_uri_did(uri).is_some())
                .map(str::to_owned)
        })
        .collect::<Vec<_>>();
    parent_uris.sort_unstable();
    parent_uris.dedup();

    for depth in 0..=2 {
        if parent_uris.is_empty() || tokio::time::Instant::now() >= deadline {
            break;
        }
        hydrate_parent_uri_batch(flow, storage, &parent_uris, &mut parents, deadline).await?;
        if depth == 2 {
            break;
        }
        parent_uris = parent_uris
            .iter()
            .filter_map(|uri| parents.get(uri).and_then(Option::as_ref))
            .filter_map(|record| {
                record
                    .record
                    .pointer("/reply/parent/uri")
                    .and_then(Value::as_str)
                    .filter(|uri| post_uri_did(uri).is_some())
                    .filter(|uri| !parents.contains_key(*uri))
                    .map(str::to_owned)
            })
            .collect();
        parent_uris.sort_unstable();
        parent_uris.dedup();
    }
    Ok(parents)
}

async fn hydrate_parent_uri_batch<F: OAuthFlow, S: Storage>(
    flow: &F,
    storage: &S,
    parent_uris: &[String],
    parents: &mut PageParentIndex,
    deadline: tokio::time::Instant,
) -> Result<(), StorageError> {
    for uri in parent_uris {
        parents.entry(uri.clone()).or_insert(None);
    }
    for uri in parent_uris {
        if let Some(record) = storage.record_by_uri("app.bsky.feed.post", uri)? {
            parents.insert(uri.clone(), Some(record));
        }
    }

    let missing = parent_uris
        .iter()
        .filter(|uri| parents.get(*uri).is_some_and(Option::is_none))
        .cloned()
        .collect::<Vec<_>>();
    let flow = flow.clone();
    let mut fetches = stream::iter(missing.into_iter().map(|uri| {
        let flow = flow.clone();
        async move {
            let fetched = flow.fetch_post(&uri).await;
            (uri, fetched)
        }
    }))
    .buffer_unordered(PAGE_QUOTE_CONCURRENCY);
    let _ = tokio::time::timeout_at(deadline, async {
        while let Some((uri, fetched)) = fetches.next().await {
            if let Ok(record) = fetched
                && record.uri == uri
                && record.collection == "app.bsky.feed.post"
            {
                parents.insert(uri, Some(record));
            }
        }
    })
    .await;
    Ok(())
}

fn queue_missing_parent_authors<S: Storage>(
    storage: &S,
    parents: &PageParentIndex,
) -> Result<(), StorageError> {
    let now_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64;
    let profile_index = storage.profile_index_snapshot()?;
    let mut queued = HashSet::new();
    for parent in parents.values().flatten() {
        let profile_status = profile_index.get(&parent.did);
        let age_ms = profile_status.map_or(i64::MAX, |entry| {
            now_ms.saturating_sub(entry.fetched_at_ms).max(0)
        });
        let profile_is_fresh = profile_status
            .is_some_and(|entry| entry.has_profile && age_ms < PROFILE_MAX_AGE.as_millis() as i64);
        let handle_is_known = storage.verified_handle(&parent.did)?.is_some();
        let handle_is_invalid = storage.handle_is_invalid(&parent.did)?;
        if !profile_is_fresh || (!handle_is_known && !handle_is_invalid) {
            queued.insert(parent.did.clone());
        }
    }
    if !queued.is_empty() {
        let mut dids = queued.into_iter().collect::<Vec<_>>();
        dids.sort();
        storage.enqueue_profile_fetches(&dids)?;
    }
    Ok(())
}

fn quoted_record_ref(embed: &Value) -> Option<QuotedRecordRef> {
    match embed.get("$type").and_then(Value::as_str) {
        Some("app.bsky.embed.record") | Some("app.bsky.embed.record#view") => {}
        Some("app.bsky.embed.recordWithMedia") | Some("app.bsky.embed.recordWithMedia#view") => {}
        _ => return None,
    }
    let record = embed.get("record")?;
    let reference = record.get("record").unwrap_or(record);
    let uri = reference.get("uri")?.as_str()?.to_owned();
    let type_name = reference.get("$type").and_then(Value::as_str).unwrap_or("");
    Some(QuotedRecordRef {
        uri,
        unavailable: ["#viewBlocked", "#viewDetached", "#viewNotFound"]
            .iter()
            .any(|view| type_name.ends_with(view)),
    })
}

fn at_uri_collection(uri: &str) -> Option<&str> {
    let mut parts = uri.strip_prefix("at://")?.split('/');
    let did = parts.next()?;
    let collection = parts.next()?;
    let rkey = parts.next()?;
    (did.starts_with("did:")
        && !collection.is_empty()
        && !rkey.is_empty()
        && parts.next().is_none())
    .then_some(collection)
}

async fn hydrate_page_quotes<F: OAuthFlow, S: Storage>(
    flow: &F,
    storage: &S,
    refs: Vec<QuotedRecordRef>,
    deadline: tokio::time::Instant,
) -> Result<QuoteRenderIndex, StorageError> {
    let mut result = HashMap::new();
    let mut missing_posts = Vec::new();
    for reference in refs {
        let state = if reference.unavailable {
            QuoteRenderState::Unavailable
        } else if let Some(collection) = at_uri_collection(&reference.uri) {
            if collection == "app.bsky.feed.post" {
                match storage.record_by_uri(collection, &reference.uri)? {
                    Some(record) => QuoteRenderState::Found(record),
                    None => {
                        missing_posts.push(reference.uri.clone());
                        QuoteRenderState::Pending
                    }
                }
            } else {
                QuoteRenderState::Other(storage.record_by_uri(collection, &reference.uri)?)
            }
        } else {
            QuoteRenderState::NotFound
        };
        result.insert(reference.uri, state);
    }

    let flow = flow.clone();
    let mut fetches = stream::iter(missing_posts.into_iter().map(|uri| {
        let flow = flow.clone();
        async move {
            let fetched = flow.fetch_post(&uri).await;
            (uri, fetched)
        }
    }))
    .buffer_unordered(PAGE_QUOTE_CONCURRENCY);
    let fetch_result = tokio::time::timeout_at(deadline, async {
        while let Some((uri, fetched)) = fetches.next().await {
            let state = match fetched {
                Ok(record) if record.uri == uri && record.collection == "app.bsky.feed.post" => {
                    QuoteRenderState::Found(record)
                }
                Ok(_) | Err(_) => QuoteRenderState::NotFound,
            };
            result.insert(uri, state);
        }
    })
    .await;
    if fetch_result.is_err() {
        tracing::debug!(
            pending = result
                .values()
                .filter(|state| matches!(state, QuoteRenderState::Pending))
                .count(),
            "quoted post hydration reached page budget"
        );
    }
    Ok(result)
}

fn icon_content(name: &str) -> &'static str {
    match name {
        "home" => "<path d=\"m3 10 9-7 9 7\"/><path d=\"M5 9v11h14V9M9 20v-6h6v6\"/>",
        "feeds" => {
            "<path d=\"M4 5h16M4 12h16M4 19h16\"/><circle cx=\"7\" cy=\"5\" r=\"1\"/><circle cx=\"17\" cy=\"12\" r=\"1\"/><circle cx=\"10\" cy=\"19\" r=\"1\"/>"
        }
        "notifications" => "<path d=\"M18 9a6 6 0 0 0-12 0c0 7-3 7-3 9h18c0-2-3-2-3-9M10 21h4\"/>",
        "profile" => "<circle cx=\"12\" cy=\"8\" r=\"4\"/><path d=\"M4 21a8 8 0 0 1 16 0\"/>",
        "follow" => {
            "<circle cx=\"9\" cy=\"8\" r=\"3.5\"/><path d=\"M2.5 20a6.5 6.5 0 0 1 13 0M19 8v8M15 12h8\"/>"
        }
        "compose" => "<path d=\"M12 5v14M5 12h14\"/>",
        "search" => "<circle cx=\"10.8\" cy=\"10.8\" r=\"6.8\"/><path d=\"m16 16 5 5\"/>",
        "reply" => "<path d=\"M10 8 5 12l5 4\"/><path d=\"M5 12h8a6 6 0 0 1 6 6v1\"/>",
        "repost" => {
            "<path d=\"m16 3 4 4-4 4\"/><path d=\"M4 11V9a2 2 0 0 1 2-2h14M8 21l-4-4 4-4\"/><path d=\"M20 13v2a2 2 0 0 1-2 2H4\"/>"
        }
        "like" => {
            "<path d=\"M20.8 8.8c0 5.2-8.8 10.2-8.8 10.2S3.2 14 3.2 8.8A4.8 4.8 0 0 1 12 6.1a4.8 4.8 0 0 1 8.8 2.7Z\"/>"
        }
        "share" => {
            "<circle cx=\"18\" cy=\"5\" r=\"3\"/><circle cx=\"6\" cy=\"12\" r=\"3\"/><circle cx=\"18\" cy=\"19\" r=\"3\"/><path d=\"m8.7 10.7 6.6-4.4M8.7 13.3l6.6 4.4\"/>"
        }
        "more" => {
            "<circle cx=\"5\" cy=\"12\" r=\"1\"/><circle cx=\"12\" cy=\"12\" r=\"1\"/><circle cx=\"19\" cy=\"12\" r=\"1\"/>"
        }
        _ => "",
    }
}

fn icon_sprite() -> String {
    const ICONS: [&str; 12] = [
        "home",
        "feeds",
        "notifications",
        "profile",
        "follow",
        "compose",
        "search",
        "reply",
        "repost",
        "like",
        "share",
        "more",
    ];
    let symbols = ICONS
        .iter()
        .map(|name| {
            format!(
                "<symbol id=\"i-{name}\" viewBox=\"0 0 24 24\" stroke=\"currentColor\" stroke-width=\"1.8\" stroke-linecap=\"round\" stroke-linejoin=\"round\">{}</symbol>",
                icon_content(name)
            )
        })
        .collect::<String>();
    format!(
        "<svg class=\"icon-sprite\" aria-hidden=\"true\" focusable=\"false\" xmlns=\"http://www.w3.org/2000/svg\">{symbols}</svg>"
    )
}

fn icon_svg(name: &str) -> String {
    if icon_content(name).is_empty() {
        return String::new();
    }
    format!(
        "<svg class=\"icon\" aria-hidden=\"true\" focusable=\"false\"><use href=\"#i-{name}\"></use></svg>"
    )
}

/// The bare origin goes to the app, which sends a visitor without a session on to `/login`.
async fn root() -> Redirect {
    Redirect::to("/app")
}

/// Trim, drop a leading `@`, and lowercase handles (case-insensitive in atproto; phone keyboards
/// capitalise the first letter). DIDs are passed through unchanged.
fn normalize_login(typed: &str) -> String {
    let login = typed.trim();
    let login = login.strip_prefix('@').unwrap_or(login);
    if login.starts_with("did:") {
        login.to_owned()
    } else {
        login.to_ascii_lowercase()
    }
}

const HANDLE_COOKIE: &str = "appview_handle";

fn handle_shaped(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 253
        && value
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || matches!(c, '.' | '-' | ':' | '_'))
}

/// Remembers the last handle typed so the sign-in form comes back filled in.
fn remember_handle_cookie(handle: &str, secure: bool) -> Option<String> {
    handle_shaped(handle).then(|| {
        format!(
            "{HANDLE_COOKIE}={handle}; Path=/; HttpOnly; SameSite=Lax; Max-Age=31536000{}",
            if secure { "; Secure" } else { "" }
        )
    })
}

async fn login(headers: HeaderMap) -> Html<String> {
    let value = cookie_value_named(&headers, HANDLE_COOKIE)
        .filter(|handle| handle_shaped(handle))
        .map(|handle| format!(" value=\"{}\"", html_escape(handle)))
        .unwrap_or_default();
    Html(page_shell(
        "Sign in",
        &format!(
            "<section class=\"login-card\"><p class=\"eyebrow\">Your network, your view</p><h1>Sign in to AppView</h1><p class=\"intro\">Use your AT Protocol handle or DID to continue securely with your account provider.</p><form action=\"/oauth/authorize\" method=\"post\" class=\"login-form\"><label for=\"handle\">AT Protocol handle or DID</label><input id=\"handle\" name=\"handle\" autocomplete=\"username\" autocapitalize=\"none\" placeholder=\"you.bsky.social\"{value} required><button type=\"submit\">Continue</button></form></section>"
        ),
        false,
    ))
}

async fn authorize<F, S>(
    State(state): State<Arc<OAuthWebState<F, S>>>,
    headers: HeaderMap,
    Form(form): Form<HashMap<String, String>>,
) -> Response
where
    F: OAuthFlow,
    S: Storage + 'static,
{
    if !same_origin_request(&headers, &state.public.public_url) {
        return oauth_error(
            StatusCode::FORBIDDEN,
            "authorization must start from this origin",
        );
    }
    if form
        .get("handle")
        .is_none_or(|handle| handle.trim().is_empty())
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error":"invalid_request","message":"handle is required"})),
        )
            .into_response();
    }
    let mut binding_bytes = [0u8; 32];
    thread_rng().fill_bytes(&mut binding_bytes);
    let browser_binding = URL_SAFE_NO_PAD.encode(binding_bytes);
    let authorize_result = std::panic::AssertUnwindSafe(state.flow.authorize(
        &normalize_login(form.get("handle").expect("validated above")),
        &browser_binding,
    ))
    .catch_unwind()
    .await;
    match authorize_result {
        Ok(Ok(url)) if valid_redirect_url(&url) => {
            let mut response = Redirect::to(&url).into_response();
            let cookie = format!(
                "{LOGIN_COOKIE}={}; Path=/; HttpOnly; SameSite=Lax; Max-Age={LOGIN_COOKIE_TTL_SECONDS}{}",
                state.public.issue_login_cookie(&browser_binding),
                if state.public.secure_cookies() {
                    "; Secure"
                } else {
                    ""
                },
            );
            response.headers_mut().insert(
                header::SET_COOKIE,
                HeaderValue::from_str(&cookie).expect("login cookie token is header safe"),
            );
            if let Some(remember) = remember_handle_cookie(
                &normalize_login(form.get("handle").expect("validated above")),
                state.public.secure_cookies(),
            ) {
                response.headers_mut().append(
                    header::SET_COOKIE,
                    HeaderValue::from_str(&remember).expect("handle-shaped values are header safe"),
                );
            }
            response
        }
        Ok(Ok(_)) => oauth_error(
            StatusCode::BAD_GATEWAY,
            "invalid authorization endpoint URL",
        ),
        Ok(Err(error)) => {
            // The browser gets a generic message; the operator needs the real cause.
            tracing::warn!(%error, "oauth sign-in could not be started");
            oauth_error(
                StatusCode::BAD_REQUEST,
                "Sign-in could not be started. Please try again.",
            )
        }
        Err(_) => oauth_error_page(
            StatusCode::BAD_GATEWAY,
            "Sign-in could not be completed. Please start again.",
        ),
    }
}

async fn callback<F, S>(
    State(state): State<Arc<OAuthWebState<F, S>>>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response
where
    F: OAuthFlow,
    S: Storage + 'static,
{
    if query.get("code").is_none_or(|code| code.trim().is_empty())
        || query
            .get("state")
            .is_none_or(|state| state.trim().is_empty())
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error":"invalid_request","message":"code and state are required"})),
        )
            .into_response();
    }
    let browser_binding = cookie_value_named(&headers, LOGIN_COOKIE)
        .and_then(|value| state.public.verify_login_cookie(value));
    let Some(browser_binding) = browser_binding else {
        tracing::warn!(
            cookie_headers = headers.get_all(header::COOKIE).iter().count(),
            login_cookie_present = cookie_value_named(&headers, LOGIN_COOKIE).is_some(),
            "oauth callback without a valid browser binding cookie"
        );
        return oauth_error(
            StatusCode::BAD_REQUEST,
            "OAuth callback is missing its browser binding cookie",
        );
    };
    let scope = query.get("scope").cloned();
    let params = OAuthCallback {
        code: query.get("code").expect("validated above").clone(),
        state: query.get("state").expect("validated above").clone(),
        issuer: query.get("iss").cloned(),
        scope,
        browser_binding: Some(browser_binding),
    };
    tracing::info!(issuer = ?params.issuer, "oauth callback received");
    let callback_result = std::panic::AssertUnwindSafe(state.flow.callback(params))
        .catch_unwind()
        .await;
    let account = match callback_result {
        Ok(Ok(account)) => account,
        Ok(Err(error)) => {
            // The browser gets a generic message; the operator needs the real cause.
            tracing::warn!(%error, "oauth callback could not be completed");
            return oauth_error(
                StatusCode::BAD_REQUEST,
                "Sign-in could not be completed. Please start again.",
            );
        }
        Err(_) => {
            tracing::warn!("oauth callback panicked");
            return oauth_error_page(
                StatusCode::BAD_GATEWAY,
                "Sign-in could not be completed. Please start again.",
            );
        }
    };
    if !has_atproto_scope(account.granted_scope.as_deref()) {
        tracing::warn!(scope = ?account.granted_scope, "oauth callback without atproto scope");
        return oauth_error(
            StatusCode::BAD_REQUEST,
            "OAuth token response must grant the atproto scope",
        );
    }
    let mut session_id_bytes = [0u8; 32];
    thread_rng().fill_bytes(&mut session_id_bytes);
    let session_id = URL_SAFE_NO_PAD.encode(session_id_bytes);
    let storage = Arc::clone(&state.storage);
    let storage_did = account.did.clone();
    let storage_handle = account.verified_handle.clone();
    let storage_session_id = session_id.clone();
    let generation = match tokio::task::spawn_blocking(move || {
        storage.register_user(&storage_did)?;
        if let Some(handle) = storage_handle.as_deref() {
            storage.set_verified_handle(&storage_did, handle)?;
        }
        storage.rotate_web_session(&storage_did, &storage_session_id)
    })
    .await
    {
        Ok(Ok(generation)) => generation,
        Ok(Err(error)) => {
            tracing::warn!(%error, "oauth callback could not persist the user session");
            return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string());
        }
        Err(error) => {
            tracing::warn!(%error, "oauth callback storage task failed");
            return oauth_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "storage operation failed",
            );
        }
    };
    let mut response = Redirect::to("/app").into_response();
    let write_access = has_all_write_scopes(account.granted_scope.as_deref());
    let feed_access = has_all_feed_scopes(account.granted_scope.as_deref());
    let blob_access = has_blob_scope(account.granted_scope.as_deref());
    let video_access = has_all_video_scopes(account.granted_scope.as_deref());
    let cookie = format!(
        "{SESSION_COOKIE}={}; Path=/; HttpOnly; SameSite=Lax; Max-Age={SESSION_TTL_SECONDS}{}",
        state.public.issue_session_cookie(
            &account.did,
            &session_id,
            generation,
            SessionPermissions {
                write: write_access,
                feed: feed_access,
                blob: blob_access,
                video: video_access,
            },
        ),
        if state.public.secure_cookies() {
            "; Secure"
        } else {
            ""
        },
    );
    response.headers_mut().insert(
        header::SET_COOKIE,
        HeaderValue::from_str(&cookie).expect("cookie token is header safe"),
    );
    response.headers_mut().append(
        header::SET_COOKIE,
        HeaderValue::from_str(&clear_login_cookie(&state.public))
            .expect("login cookie clearing header is valid"),
    );
    response
}

async fn logout<F, S>(State(state): State<Arc<OAuthWebState<F, S>>>, headers: HeaderMap) -> Response
where
    F: OAuthFlow,
    S: Storage + 'static,
{
    if !same_origin_request(&headers, &state.public.public_url) {
        return oauth_error(StatusCode::FORBIDDEN, "request must come from this origin");
    }
    let Some((did, session_id, _, _, _, _, _)) =
        verified_cookie_session(&state.public, &headers, state.storage.as_ref())
    else {
        return clear_session_cookie(
            Redirect::to("/login").into_response(),
            state.public.secure_cookies(),
        );
    };
    let storage = Arc::clone(&state.storage);
    let storage_did = did.clone();
    let storage_session_id = session_id.clone();
    match tokio::task::spawn_blocking(move || {
        storage.rotate_web_session(&storage_did, &storage_session_id)
    })
    .await
    {
        Ok(Ok(_)) => {}
        Ok(Err(error)) => {
            return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string());
        }
        Err(error) => {
            tracing::warn!(%error, "logout storage task failed while invalidating the session");
            return oauth_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "storage operation failed",
            );
        }
    }
    let remote_result = state.flow.logout(&did).await;
    let storage = Arc::clone(&state.storage);
    let storage_did = did.clone();
    match tokio::task::spawn_blocking(move || storage.unregister_user(&storage_did)).await {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string());
        }
        Err(error) => {
            tracing::warn!(%error, "logout storage task failed while unregistering the user");
            return oauth_error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "storage operation failed",
            );
        }
    }
    let response = clear_session_cookie(
        Redirect::to("/login").into_response(),
        state.public.secure_cookies(),
    );
    if remote_result.is_err() {
        return response;
    }
    response
}

async fn app_home<F, S>(
    State(state): State<Arc<OAuthWebState<F, S>>>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response
where
    F: OAuthFlow,
    S: Storage + 'static,
{
    let mut page_timing = WebPageTiming::new("app");
    let session_started = std::time::Instant::now();
    let Some(did) = verified_cookie_did(&state.public, &headers, state.storage.as_ref()) else {
        return Redirect::to("/login").into_response();
    };
    page_timing.session_token_refresh_ms = session_started.elapsed().as_millis() as u64;
    let tabs = pinned_feeds_for_layout(
        &state.flow,
        &did,
        verified_cookie_feed_access(&state.public, &headers, state.storage.as_ref()),
    )
    .await;
    page_timing.preferences_ms = tabs.preferences_ms;
    page_timing.tabs_ms = tabs.tabs_ms;
    let pinned_feeds = tabs.feeds;
    let limit = query
        .get("limit")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(50)
        .clamp(1, 100);
    let cursor = query
        .get("cursor")
        .and_then(|cursor| RecordCursor::parse(cursor));
    let timeline_started = std::time::Instant::now();
    let timeline_page = match state
        .storage
        .timeline_posts(&did, cursor.as_ref(), limit + 1)
    {
        Ok(records) => records,
        Err(error) => return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    page_timing.timeline_fetch_index_ms = timeline_started.elapsed().as_millis() as u64;
    let has_more = timeline_page.len() > limit;
    let timeline = timeline_page.iter().take(limit).collect::<Vec<_>>();
    let content_deadline = tokio::time::Instant::now() + PAGE_PROFILE_BUDGET;
    let parent_started = std::time::Instant::now();
    let parents = match hydrate_page_parents(
        &state.flow,
        state.storage.as_ref(),
        &timeline,
        content_deadline,
    )
    .await
    {
        Ok(parents) => parents,
        Err(error) => return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    page_timing.parent_hydration_ms = parent_started.elapsed().as_millis() as u64;
    let quote_index = match hydrate_page_content(
        &state.flow,
        state.storage.clone(),
        &timeline,
        std::iter::empty(),
        content_deadline,
    )
    .await
    {
        Ok(hydration) => {
            page_timing.profile_hydration_ms = hydration.profile_hydration_ms;
            page_timing.quote_hydration_ms = hydration.quote_hydration_ms;
            hydration.quote_index
        }
        Err(error) => return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    if let Err(error) = queue_missing_parent_authors(state.storage.as_ref(), &parents) {
        return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string());
    }
    let render_started = std::time::Instant::now();
    let write_forms_enabled =
        verified_cookie_write_access(&state.public, &headers, state.storage.as_ref());
    let posts = render_timeline_post_items(
        state.storage.as_ref(),
        &timeline,
        &vec![String::new(); timeline.len()],
        &parents,
        &did,
        write_forms_enabled,
        &quote_index,
    );
    let posts = if posts.is_empty() {
        "<li class=\"empty-state\">No indexed posts yet.</li>".to_owned()
    } else {
        posts
    };
    let indexing_status = if timeline.is_empty() {
        let (total, done) = match state.storage.backfill_progress(&did) {
            Ok(progress) => progress,
            Err(error) => {
                return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string());
            }
        };
        let failed = match state.storage.backfill_failure_count(&did) {
            Ok(failed) => failed,
            Err(error) => {
                return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string());
            }
        };
        if total > done {
            let account_word = if total == 1 { "account" } else { "accounts" };
            format!(
                "<p role=\"status\">Indexing {total} {account_word}, {done} done, {failed} failed. The timeline will fill as repositories finish.</p>"
            )
        } else {
            let account_word = if total == 1 { "account" } else { "accounts" };
            format!(
                "<p role=\"status\">Indexed {done} of {total} {account_word}; no posts are available yet.</p>"
            )
        }
    } else {
        String::new()
    };
    let read_only_message = if write_forms_enabled {
        String::new()
    } else {
        "<p role=\"status\">Read-only: write permissions were not granted, so posting, liking, and reposting are disabled.</p>".to_owned()
    };
    let post_form = if write_forms_enabled {
        format!(
            "<form class=\"composer image-composer\" action=\"/post\" method=\"post\" enctype=\"multipart/form-data\" data-image-composer><label for=\"new-post\">Write a post</label><textarea id=\"new-post\" name=\"text\" rows=\"3\" maxlength=\"300\" required></textarea>{}{}{}<button type=\"submit\">Post</button><p class=\"composer-error\" data-composer-error role=\"alert\" hidden></p></form>",
            render_image_picker(
                verified_cookie_blob_access(&state.public, &headers, state.storage.as_ref()),
                "post-images"
            ),
            render_video_picker(
                verified_cookie_video_access(&state.public, &headers, state.storage.as_ref()),
                "post-video",
            ),
            render_link_picker("post-link")
        )
    } else {
        String::new()
    };
    let older = if has_more {
        timeline
            .last()
            .map(|last| {
                let cursor = RecordCursor {
                    time_us: last.time_us,
                    uri: last.uri.clone(),
                }
                .encode();
                let encoded =
                    url::form_urlencoded::byte_serialize(cursor.as_bytes()).collect::<String>();
                format!(
                    "<a class=\"pagination-link\" rel=\"next\" href=\"/app?limit={limit}&amp;cursor={encoded}\">Older</a>"
                )
            })
            .unwrap_or_default()
    } else {
        String::new()
    };
    let response = Html(page_shell_with_feeds_and_badge(
        "Timeline",
        &format!(
            "<header class=\"page-heading\"><p class=\"eyebrow\">Your feed</p><h1>Timeline</h1><p class=\"page-subtitle\">Posts from accounts in your local view.</p></header>{read_only_message}{indexing_status}{post_form}<ol class=\"post-list\">{posts}</ol>{older}",
        ),
        true,
        &pinned_feeds,
        None,
        true,
        unread_badge_count(state.storage.as_ref(), &did),
    ))
    .into_response();
    page_timing.render_ms = render_started.elapsed().as_millis() as u64;
    response
}

async fn feeds_page<F, S>(
    State(state): State<Arc<OAuthWebState<F, S>>>,
    headers: HeaderMap,
) -> Response
where
    F: OAuthFlow,
    S: Storage + 'static,
{
    let Some(did) = verified_cookie_did(&state.public, &headers, state.storage.as_ref()) else {
        return Redirect::to("/login").into_response();
    };
    if !verified_cookie_feed_access(&state.public, &headers, state.storage.as_ref()) {
        return feed_reauthorization_page();
    }
    let feeds = match state.flow.pinned_feed_tabs(&did).await {
        Ok(tabs) => tabs.feeds,
        Err(error) => return oauth_error(StatusCode::BAD_GATEWAY, &error),
    };
    let entries = feeds.iter().map(render_feed_card).collect::<String>();
    let entries = if entries.is_empty() {
        "<li class=\"empty-state\">No pinned custom feeds were found in your account preferences.</li>".to_owned()
    } else {
        entries
    };
    Html(page_shell_with_feeds_and_badge(
        "Feeds",
        &format!(
            "<header class=\"page-heading\"><p class=\"eyebrow\">Your saved algorithms</p><h1>Feeds</h1><p class=\"page-subtitle\">Pinned custom feeds from your account.</p></header><ol class=\"feed-list\">{entries}</ol>"
        ),
        true,
        &feeds,
        None,
        false,
        unread_badge_count(state.storage.as_ref(), &did),
    ))
    .into_response()
}

async fn feed_timeline_page<F, S>(
    State(state): State<Arc<OAuthWebState<F, S>>>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response
where
    F: OAuthFlow,
    S: Storage + 'static,
{
    let mut page_timing = WebPageTiming::new("feed");
    let session_started = std::time::Instant::now();
    let Some(did) = verified_cookie_did(&state.public, &headers, state.storage.as_ref()) else {
        return Redirect::to("/login").into_response();
    };
    page_timing.session_token_refresh_ms = session_started.elapsed().as_millis() as u64;
    if !verified_cookie_feed_access(&state.public, &headers, state.storage.as_ref()) {
        return feed_reauthorization_page();
    }
    let Some(uri) = query.get("uri").filter(|uri| !uri.is_empty()) else {
        return oauth_error(StatusCode::BAD_REQUEST, "feed uri is required");
    };
    let page_key = (
        did.clone(),
        uri.to_owned(),
        query.get("cursor").cloned().unwrap_or_default(),
    );
    let timeline_started = std::time::Instant::now();
    let page = if let Some((expires, page)) =
        state.feed_page_cache.lock().await.get(&page_key).cloned()
        && expires > tokio::time::Instant::now()
    {
        let mut page = page;
        page.timing = FeedTimelineTiming {
            cache_hit: true,
            ..FeedTimelineTiming::default()
        };
        page
    } else {
        match state
            .flow
            .feed_timeline(
                &did,
                uri,
                query.get("cursor").map(String::as_str),
                state.storage.as_ref(),
            )
            .await
        {
            Ok(page) => page,
            Err(error) => return oauth_error(StatusCode::BAD_GATEWAY, &error),
        }
    };
    if !page.timing.cache_hit {
        let mut cache = state.feed_page_cache.lock().await;
        cache.retain(|_, (expires, _)| *expires > tokio::time::Instant::now());
        if cache.len() < 1024 {
            cache.insert(
                page_key,
                (
                    tokio::time::Instant::now() + Duration::from_secs(FEED_PAGE_CACHE_SECS),
                    page.clone(),
                ),
            );
        }
    }
    page_timing.session_token_refresh_ms = page
        .timing
        .session_token_refresh_ms
        .saturating_add(page_timing.session_token_refresh_ms);
    page_timing.timeline_fetch_live_ms = page.timing.timeline_fetch_live_ms;
    if page.timing.cache_hit {
        page_timing.timeline_fetch_index_ms = timeline_started.elapsed().as_millis() as u64;
    }
    let feed_records = page
        .timeline
        .feed
        .iter()
        .map(|post| &post.record)
        .collect::<Vec<_>>();
    let content_deadline = tokio::time::Instant::now() + PAGE_PROFILE_BUDGET;
    let parent_started = std::time::Instant::now();
    let parents = match hydrate_page_parents(
        &state.flow,
        state.storage.as_ref(),
        &feed_records,
        content_deadline,
    )
    .await
    {
        Ok(parents) => parents,
        Err(error) => return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    page_timing.parent_hydration_ms = parent_started.elapsed().as_millis() as u64;
    let quote_index = match hydrate_page_content(
        &state.flow,
        state.storage.clone(),
        &feed_records,
        std::iter::empty(),
        content_deadline,
    )
    .await
    {
        Ok(hydration) => {
            page_timing.profile_hydration_ms = hydration.profile_hydration_ms;
            page_timing.quote_hydration_ms = hydration.quote_hydration_ms;
            hydration.quote_index
        }
        Err(error) => return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    if let Err(error) = queue_missing_parent_authors(state.storage.as_ref(), &parents) {
        return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string());
    }
    let render_started = std::time::Instant::now();
    let write_forms_enabled =
        verified_cookie_write_access(&state.public, &headers, state.storage.as_ref());
    let feed_preludes = page
        .timeline
        .feed
        .iter()
        .map(|post| {
            post.reposted_by
                .as_deref()
                .map(|reposter| {
                    format!(
                        "<p class=\"feed-repost\">Reposted by @{}</p>",
                        html_escape(reposter)
                    )
                })
                .unwrap_or_default()
        })
        .collect::<Vec<_>>();
    let posts = render_timeline_post_items(
        state.storage.as_ref(),
        &feed_records,
        &feed_preludes,
        &parents,
        &did,
        write_forms_enabled,
        &quote_index,
    );
    let posts = if posts.is_empty() {
        "<li class=\"empty-state\">This feed has no available posts on this page.</li>".to_owned()
    } else {
        posts
    };
    let skipped = if page.timeline.dropped_count > 0 {
        format!(
            "<p role=\"status\">Skipped {} feed item(s) that could not be hydrated.</p>",
            page.timeline.dropped_count
        )
    } else {
        String::new()
    };
    let older = page
        .timeline
        .cursor
        .as_deref()
        .map(|cursor| {
            let uri = form_encoded(&page.generator.uri);
            let cursor = form_encoded(cursor);
            format!(
                "<a class=\"pagination-link\" rel=\"next\" href=\"/app/feed?uri={uri}&amp;cursor={cursor}\">Load more</a>"
            )
        })
        .unwrap_or_default();
    let tabs = pinned_feeds_for_layout(&state.flow, &did, true).await;
    page_timing.preferences_ms = tabs.preferences_ms;
    page_timing.tabs_ms = tabs.tabs_ms;
    let response = Html(page_shell_with_feeds_and_badge(
        &page.generator.display_name,
        &format!(
            "{}{}<ol class=\"post-list\">{posts}</ol>{older}",
            render_feed_heading(&page.generator),
            skipped
        ),
        true,
        &tabs.feeds,
        Some(&page.generator.uri),
        false,
        unread_badge_count(state.storage.as_ref(), &did),
    ))
    .into_response();
    page_timing.render_ms = render_started.elapsed().as_millis() as u64;
    response
}

#[derive(Clone, Debug, PartialEq)]
struct WebNotificationGroup {
    reason: String,
    reason_subject: Option<String>,
    events: Vec<IndexedNotification>,
}

const NOTIFICATION_GROUP_WINDOW_US: u64 = 48 * 60 * 60 * 1_000_000;

fn group_web_notifications(
    notifications: &[IndexedNotification],
    last_seen_us: u64,
) -> Vec<WebNotificationGroup> {
    let mut ordered = notifications.to_vec();
    ordered.sort_by(|left, right| {
        right
            .record
            .time_us
            .cmp(&left.record.time_us)
            .then_with(|| right.record.uri.cmp(&left.record.uri))
            .then_with(|| right.reason.cmp(&left.reason))
    });

    let mut groups = Vec::<WebNotificationGroup>::new();
    let mut active = HashMap::<(String, Option<String>, bool), usize>::new();
    for notification in ordered {
        let can_group = match notification.reason.as_str() {
            "like" | "repost" => notification.reason_subject.is_some(),
            "follow" => true,
            _ => false,
        };
        if !can_group {
            groups.push(WebNotificationGroup {
                reason: notification.reason.clone(),
                reason_subject: notification.reason_subject.clone(),
                events: vec![notification],
            });
            continue;
        }

        let unread = notification.record.time_us > last_seen_us;
        let key = (
            notification.reason.clone(),
            notification.reason_subject.clone(),
            unread,
        );
        let existing = active.get(&key).copied().filter(|index| {
            groups[*index].events.first().is_some_and(|newest| {
                newest
                    .record
                    .time_us
                    .saturating_sub(notification.record.time_us)
                    <= NOTIFICATION_GROUP_WINDOW_US
            })
        });
        if let Some(index) = existing {
            groups[index].events.push(notification);
        } else {
            let index = groups.len();
            groups.push(WebNotificationGroup {
                reason: notification.reason.clone(),
                reason_subject: notification.reason_subject.clone(),
                events: vec![notification],
            });
            active.insert(key, index);
        }
    }
    groups.sort_by(|left, right| {
        let left = &left.events[0];
        let right = &right.events[0];
        right
            .record
            .time_us
            .cmp(&left.record.time_us)
            .then_with(|| right.record.uri.cmp(&left.record.uri))
            .then_with(|| right.reason.cmp(&left.reason))
    });
    groups
}

fn notification_group_page<'a>(
    groups: &'a [WebNotificationGroup],
    before: Option<&NotificationCursor>,
    limit: usize,
) -> (Vec<&'a WebNotificationGroup>, bool) {
    let matching = groups
        .iter()
        .filter(|group| {
            let Some(cursor) = before else {
                return true;
            };
            let newest = &group.events[0];
            newest.record.time_us < cursor.time_us
                || (newest.record.time_us == cursor.time_us
                    && (newest.record.uri < cursor.uri
                        || (newest.record.uri == cursor.uri && newest.reason < cursor.reason)))
        })
        .collect::<Vec<_>>();
    let has_more = matching.len() > limit;
    (matching.into_iter().take(limit).collect(), has_more)
}

fn notification_group_cursor(group: &WebNotificationGroup) -> NotificationCursor {
    let newest = &group.events[0];
    NotificationCursor {
        time_us: newest.record.time_us,
        uri: newest.record.uri.clone(),
        reason: newest.reason.clone(),
    }
}

fn unread_count_event(count: usize) -> Event {
    Event::default().event("unread").data(count.to_string())
}

fn notification_count_stream<S: Storage + 'static>(
    receiver: crate::ingest::NotificationSubscription,
    initial_count: usize,
    storage: Arc<S>,
    did: String,
    heartbeat_interval: Duration,
    shutdown: tokio::sync::watch::Receiver<bool>,
    guard: NotificationStreamGuard,
) -> impl futures_util::Stream<Item = Result<Event, std::convert::Infallible>> + Send + 'static {
    let heartbeat_interval = heartbeat_interval.max(Duration::from_millis(1));
    let heartbeat = tokio::time::interval_at(
        tokio::time::Instant::now() + heartbeat_interval,
        heartbeat_interval,
    );
    stream::unfold(
        (
            Some(initial_count),
            receiver,
            heartbeat,
            storage,
            did,
            shutdown,
            guard,
        ),
        |(initial, mut receiver, mut heartbeat, storage, did, mut shutdown, _guard)| async move {
            if *shutdown.borrow() {
                return None;
            }
            if let Some(count) = initial {
                return Some((
                    Ok(unread_count_event(count)),
                    (None, receiver, heartbeat, storage, did, shutdown, _guard),
                ));
            }
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        None
                    } else {
                        Some((
                            Ok(Event::default().comment("heartbeat")),
                            (None, receiver, heartbeat, storage, did, shutdown, _guard),
                        ))
                    }
                },
                _ = heartbeat.tick() => Some((
                    Ok(Event::default().comment("heartbeat")),
                    (None, receiver, heartbeat, storage, did, shutdown, _guard),
                )),
                result = receiver.recv() => match result {
                    Ok(count) => Some((
                        Ok(unread_count_event(count)),
                        (None, receiver, heartbeat, storage, did, shutdown, _guard),
                    )),
                    Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                        let count = storage.unread_notification_count(&did).unwrap_or_default();
                        Some((
                            Ok(unread_count_event(count)),
                            (None, receiver, heartbeat, storage, did, shutdown, _guard),
                        ))
                    }
                    Err(tokio::sync::broadcast::error::RecvError::Closed) => None,
                }
            }
        },
    )
}

async fn notification_unread_count<F, S>(
    State(state): State<Arc<OAuthWebState<F, S>>>,
    headers: HeaderMap,
) -> Response
where
    F: OAuthFlow,
    S: Storage + 'static,
{
    if !same_origin_event_request(&headers, &state.public.public_url) {
        return oauth_error(StatusCode::FORBIDDEN, "request must be same-origin");
    }
    let Some(did) = verified_cookie_did(&state.public, &headers, state.storage.as_ref()) else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    match state.storage.unread_notification_count(&did) {
        Ok(count) => Json(json!({"count":count})).into_response(),
        Err(error) => oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    }
}

async fn notification_stream<F, S>(
    State(state): State<Arc<OAuthWebState<F, S>>>,
    headers: HeaderMap,
) -> Response
where
    F: OAuthFlow,
    S: Storage + 'static,
{
    if !same_origin_event_request(&headers, &state.public.public_url) {
        return oauth_error(StatusCode::FORBIDDEN, "request must be same-origin");
    }
    let Some(did) = verified_cookie_did(&state.public, &headers, state.storage.as_ref()) else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let Some(guard) = NotificationStreamGuard::acquire(
        state.notification_streams.clone(),
        state.shutdown.notification_streams.clone(),
        &did,
    ) else {
        return oauth_error(
            StatusCode::TOO_MANY_REQUESTS,
            "too many notification streams for this viewer",
        );
    };
    let receiver = state.storage.subscribe_notification_counts(&did);
    let initial_count = match state.storage.unread_notification_count(&did) {
        Ok(count) => count,
        Err(error) => return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    let events = notification_count_stream(
        receiver,
        initial_count,
        state.storage.clone(),
        did,
        Duration::from_secs(25),
        state.shutdown.subscribe(),
        guard,
    );
    let mut response = Sse::new(events).into_response();
    response.headers_mut().insert(
        header::CACHE_CONTROL,
        HeaderValue::from_static("no-cache, no-transform"),
    );
    response.headers_mut().insert(
        header::HeaderName::from_static("x-accel-buffering"),
        HeaderValue::from_static("no"),
    );
    response
}

fn same_origin_event_request(headers: &HeaderMap, public_url: &str) -> bool {
    if headers
        .get("sec-fetch-site")
        .is_some_and(|site| site.to_str().ok() != Some("same-origin"))
    {
        return false;
    }
    let Some(origin) = headers.get(header::ORIGIN) else {
        return true;
    };
    let Ok(origin_value) = origin.to_str() else {
        return false;
    };
    let Ok(origin) = reqwest::Url::parse(origin_value) else {
        return false;
    };
    let Ok(expected) = reqwest::Url::parse(public_url) else {
        return false;
    };
    origin.username().is_empty()
        && origin.password().is_none()
        && origin.path() == "/"
        && origin.query().is_none()
        && origin.fragment().is_none()
        && origin.origin() == expected.origin()
}

async fn notifications_page<F, S>(
    State(state): State<Arc<OAuthWebState<F, S>>>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response
where
    F: OAuthFlow,
    S: Storage + 'static,
{
    let mut page_timing = WebPageTiming::new("notifications");
    let session_started = std::time::Instant::now();
    let Some(did) = verified_cookie_did(&state.public, &headers, state.storage.as_ref()) else {
        return Redirect::to("/login").into_response();
    };
    page_timing.session_token_refresh_ms = session_started.elapsed().as_millis() as u64;
    let tabs = pinned_feeds_for_layout(
        &state.flow,
        &did,
        verified_cookie_feed_access(&state.public, &headers, state.storage.as_ref()),
    )
    .await;
    page_timing.preferences_ms = tabs.preferences_ms;
    page_timing.tabs_ms = tabs.tabs_ms;
    let limit = query
        .get("limit")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(50)
        .clamp(1, 100);
    let before = query
        .get("cursor")
        .and_then(|cursor| NotificationCursor::parse(cursor));
    let last_seen_us = match state.storage.notification_last_seen(&did) {
        Ok(time_us) => time_us,
        Err(error) => return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    let now_us = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros()
        .min(u64::MAX as u128) as u64;
    if let Err(error) = state.storage.mark_notifications_seen(&did, now_us) {
        return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string());
    }
    let timeline_started = std::time::Instant::now();
    let notices = match state.storage.notifications_for(&did, None, usize::MAX) {
        Ok(notices) => notices,
        Err(error) => return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    page_timing.timeline_fetch_index_ms = timeline_started.elapsed().as_millis() as u64;
    let groups = group_web_notifications(&notices, last_seen_us);
    let (visible, has_more) = notification_group_page(&groups, before.as_ref(), limit);
    let notification_records = visible
        .iter()
        .map(|group| &group.events[0].record)
        .collect::<Vec<_>>();
    let mut leading_authors = visible
        .iter()
        .flat_map(|group| {
            group
                .events
                .iter()
                .take(5)
                .map(|event| event.record.did.clone())
        })
        .collect::<Vec<_>>();
    for group in &visible {
        if let Some(uri) = group.reason_subject.as_deref()
            && let Ok(Some(post)) = state.storage.record_by_uri("app.bsky.feed.post", uri)
        {
            leading_authors.push(post.did);
        }
    }
    let content_deadline = tokio::time::Instant::now() + PAGE_PROFILE_BUDGET;
    let parent_started = std::time::Instant::now();
    let parents = match hydrate_page_parents(
        &state.flow,
        state.storage.as_ref(),
        &notification_records,
        content_deadline,
    )
    .await
    {
        Ok(parents) => parents,
        Err(error) => return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    page_timing.parent_hydration_ms = parent_started.elapsed().as_millis() as u64;
    let quote_index = match hydrate_page_content(
        &state.flow,
        state.storage.clone(),
        &notification_records,
        leading_authors,
        content_deadline,
    )
    .await
    {
        Ok(hydration) => {
            page_timing.profile_hydration_ms = hydration.profile_hydration_ms;
            page_timing.quote_hydration_ms = hydration.quote_hydration_ms;
            hydration.quote_index
        }
        Err(error) => return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    if let Err(error) = queue_missing_parent_authors(state.storage.as_ref(), &parents) {
        return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string());
    }
    let render_started = std::time::Instant::now();
    let items = visible
        .iter()
        .map(|group| {
            render_web_notification_group(state.storage.as_ref(), group, &quote_index, &parents)
        })
        .collect::<String>();
    let items = if items.is_empty() {
        "<li class=\"empty-state\">No notifications yet.</li>".to_owned()
    } else {
        items
    };
    let older = if has_more {
        visible.last().map(|group| {
                let cursor = notification_group_cursor(group).encode();
                let cursor =
                    url::form_urlencoded::byte_serialize(cursor.as_bytes()).collect::<String>();
                format!(
                    "<a class=\"pagination-link\" rel=\"next\" href=\"/notifications?cursor={}\">Older</a>",
                    html_escape(&cursor)
                )
            })
            .unwrap_or_default()
    } else {
        String::new()
    };
    let response = Html(page_shell_with_feeds_and_badge(
        "Notifications",
        &format!(
            "<header class=\"page-heading\"><p class=\"eyebrow\">What’s happening</p><h1>Notifications</h1><p class=\"page-subtitle\">Recent activity around your posts and profile.</p></header><ol class=\"notification-list\">{items}</ol>{older}"
        ),
        true,
        &tabs.feeds,
        None,
        false,
        unread_badge_count(state.storage.as_ref(), &did),
    ))
    .into_response();
    page_timing.render_ms = render_started.elapsed().as_millis() as u64;
    response
}

fn render_web_notification_group<S: Storage>(
    storage: &S,
    group: &WebNotificationGroup,
    quote_index: &QuoteRenderIndex,
    parents: &PageParentIndex,
) -> String {
    if !matches!(group.reason.as_str(), "like" | "repost" | "follow") {
        return render_web_notification(storage, &group.events[0], quote_index, parents);
    }

    let newest = &group.events[0];
    let author = profile_view_from_storage(storage, &newest.record.did)
        .unwrap_or_else(|_| json!({"did":newest.record.did,"handle":newest.record.did}));
    let handle = profile_handle(&author, &newest.record.did);
    let display_name = author
        .get("displayName")
        .and_then(Value::as_str)
        .filter(|name| !name.is_empty())
        .unwrap_or(&handle);
    let author_uri = format!(
        "/profile?did={}",
        url::form_urlencoded::byte_serialize(newest.record.did.as_bytes()).collect::<String>()
    );
    let avatars = group
        .events
        .iter()
        .take(5)
        .map(|event| {
            let profile = profile_view_from_storage(storage, &event.record.did)
                .unwrap_or_else(|_| json!({"did":event.record.did,"handle":event.record.did}));
            avatar_markup(&profile, "notification-avatar")
        })
        .collect::<String>();
    let count = group.events.len().saturating_sub(1);
    let others = if count == 0 {
        String::new()
    } else {
        format!(" and {count} other{}", if count == 1 { "" } else { "s" })
    };
    let (icon, action) = match group.reason.as_str() {
        "like" => ("like", "liked your post"),
        "repost" => ("repost", "reposted your post"),
        _ => ("follow", "started following you"),
    };
    let icon = icon_svg(icon).replace(
        "class=\"icon\"",
        &format!("class=\"icon notification-type-icon notification-{icon}-icon\""),
    );
    let age = notification_age(newest.record.time_us);
    let snippet = if matches!(group.reason.as_str(), "like" | "repost") {
        group
            .reason_subject
            .as_deref()
            .and_then(|uri| storage.record_by_uri("app.bsky.feed.post", uri).ok().flatten())
            .and_then(|post| {
                let text = post.record.get("text").and_then(Value::as_str)?;
                let text = text.chars().take(300).collect::<String>();
                let target = url::form_urlencoded::byte_serialize(
                    group.reason_subject.as_deref()?.as_bytes(),
                )
                .collect::<String>();
                Some(format!(
                    "<a class=\"notification-subject-snippet\" href=\"/app/thread?uri={target}\">{}</a>",
                    html_escape(&text)
                ))
            })
            .unwrap_or_default()
    } else {
        String::new()
    };

    format!(
        "<li class=\"notification-item notification-group-item\"><article class=\"notification-group\"><div class=\"notification-group-icon\">{icon}</div><div class=\"notification-group-content\"><div class=\"notification-group-avatars\">{avatars}</div><p class=\"notification-summary\"><a class=\"notification-name\" href=\"{}\">{}</a>{others} {action} <span class=\"notification-age\">· {age}</span></p>{snippet}</div></article></li>",
        html_escape(&author_uri),
        html_escape(display_name)
    )
}

fn notification_age(time_us: u64) -> String {
    let seconds = unix_time().saturating_sub(time_us / 1_000_000);
    match seconds {
        0..=59 => "now".to_owned(),
        60..=3_599 => format!("{}m", seconds / 60),
        3_600..=86_399 => format!("{}h", seconds / 3_600),
        _ => format!("{}d", seconds / 86_400),
    }
}

fn render_web_notification<S: Storage>(
    storage: &S,
    notification: &IndexedNotification,
    quote_index: &QuoteRenderIndex,
    parents: &PageParentIndex,
) -> String {
    let record = &notification.record;
    let author = profile_view_from_storage(storage, &record.did)
        .unwrap_or_else(|_| json!({"did":record.did,"handle":record.did}));
    let handle = profile_handle(&author, &record.did);
    let author_uri = format!(
        "/profile?did={}",
        url::form_urlencoded::byte_serialize(record.did.as_bytes()).collect::<String>()
    );
    let reason = match notification.reason.as_str() {
        "like" => "liked your post",
        "repost" => "reposted your post",
        "reply" => "replied to your post",
        "mention" => "mentioned you",
        "quote" => "quoted your post",
        "follow" => "started following you",
        _ => "interacted with you",
    };
    let text = record
        .record
        .get("text")
        .and_then(Value::as_str)
        .unwrap_or("");
    let text = if text.is_empty() {
        String::new()
    } else {
        format!("<p>{}</p>", html_escape(text))
    };
    let parent = render_notification_parent(storage, record, parents);
    let embed = record
        .record
        .get("embed")
        .map(|embed| render_web_embed_with_quotes(storage, &record.did, embed, quote_index, 0))
        .unwrap_or_default();
    let target_uri = notification
        .reason_subject
        .as_deref()
        .unwrap_or(&record.uri);
    let thread_link = if target_uri.starts_with("at://") {
        let encoded =
            url::form_urlencoded::byte_serialize(target_uri.as_bytes()).collect::<String>();
        format!(
            "<a href=\"/app/thread?uri={}\">View thread</a>",
            html_escape(&encoded)
        )
    } else {
        String::new()
    };
    format!(
        "<li class=\"notification-item\" data-post-kind=\"notification\" data-post-uri=\"{}\"><article class=\"notification-card\"><header class=\"notification-heading\">{}<div><a class=\"author-name\" href=\"{}\">@{}</a><span class=\"notification-reason\">{reason}</span></div></header>{parent}{text}{embed}{thread_link}</article></li>",
        html_escape(&record.uri),
        avatar_markup(&author, "avatar"),
        html_escape(&author_uri),
        html_escape(&handle)
    )
}

async fn profile_page<F, S>(
    State(state): State<Arc<OAuthWebState<F, S>>>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response
where
    F: OAuthFlow,
    S: Storage + 'static,
{
    let mut page_timing = WebPageTiming::new("profile");
    let session_started = std::time::Instant::now();
    let Some(did) = verified_cookie_did(&state.public, &headers, state.storage.as_ref()) else {
        return Redirect::to("/login").into_response();
    };
    page_timing.session_token_refresh_ms = session_started.elapsed().as_millis() as u64;
    let tabs = pinned_feeds_for_layout(
        &state.flow,
        &did,
        verified_cookie_feed_access(&state.public, &headers, state.storage.as_ref()),
    )
    .await;
    page_timing.preferences_ms = tabs.preferences_ms;
    page_timing.tabs_ms = tabs.tabs_ms;
    let subject = query.get("did").map(String::as_str).unwrap_or(&did);
    let limit = query
        .get("limit")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(50)
        .clamp(1, 100);
    let cursor = query
        .get("cursor")
        .and_then(|cursor| RecordCursor::parse(cursor));
    let timeline_started = std::time::Instant::now();
    let records = match state
        .storage
        .author_posts(subject, cursor.as_ref(), limit + 1, true)
    {
        Ok(records) => records,
        Err(error) => return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    page_timing.timeline_fetch_index_ms = timeline_started.elapsed().as_millis() as u64;
    let has_more = records.len() > limit;
    let records = records.iter().take(limit).collect::<Vec<_>>();
    let content_deadline = tokio::time::Instant::now() + PAGE_PROFILE_BUDGET;
    let parent_started = std::time::Instant::now();
    let parents = match hydrate_page_parents(
        &state.flow,
        state.storage.as_ref(),
        &records,
        content_deadline,
    )
    .await
    {
        Ok(parents) => parents,
        Err(error) => return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    page_timing.parent_hydration_ms = parent_started.elapsed().as_millis() as u64;
    let quote_index = match hydrate_page_content(
        &state.flow,
        state.storage.clone(),
        &records,
        std::iter::once(subject.to_owned()).chain(records.iter().map(|record| record.did.clone())),
        content_deadline,
    )
    .await
    {
        Ok(hydration) => {
            page_timing.profile_hydration_ms = hydration.profile_hydration_ms;
            page_timing.quote_hydration_ms = hydration.quote_hydration_ms;
            hydration.quote_index
        }
        Err(error) => return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    if let Err(error) = queue_missing_parent_authors(state.storage.as_ref(), &parents) {
        return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string());
    }
    let render_started = std::time::Instant::now();
    let profile = match profile_view_from_storage(state.storage.as_ref(), subject) {
        Ok(profile) => profile,
        Err(error) => return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    let handle = profile_handle(&profile, subject);
    let display_name = profile
        .get("displayName")
        .and_then(Value::as_str)
        .unwrap_or(&handle);
    let write_forms_enabled =
        verified_cookie_write_access(&state.public, &headers, state.storage.as_ref());
    let posts = render_timeline_post_items(
        state.storage.as_ref(),
        &records,
        &vec![String::new(); records.len()],
        &parents,
        &did,
        write_forms_enabled,
        &quote_index,
    );
    let posts = if posts.is_empty() {
        "<li class=\"empty-state\">No indexed posts yet.</li>".to_owned()
    } else {
        posts
    };
    let older = if has_more {
        records
            .last()
            .map(|last| {
                let cursor = RecordCursor {
                    time_us: last.time_us,
                    uri: last.uri.clone(),
                }
                .encode();
                let encoded_did =
                    url::form_urlencoded::byte_serialize(subject.as_bytes()).collect::<String>();
                let encoded_cursor =
                    url::form_urlencoded::byte_serialize(cursor.as_bytes()).collect::<String>();
                format!(
                    "<a class=\"pagination-link\" rel=\"next\" href=\"/profile?did={encoded_did}&amp;limit={limit}&amp;cursor={encoded_cursor}\">Older</a>"
                )
            })
            .unwrap_or_default()
    } else {
        String::new()
    };
    let response = Html(page_shell_with_feeds_and_badge(
        display_name,
        &format!(
            "<section class=\"profile-card\">{}<div><h1>{}</h1><p class=\"profile-handle\">@{}</p></div></section><header class=\"section-heading\"><h2>Posts</h2></header><ol class=\"post-list\">{posts}</ol>{older}",
            avatar_markup(&profile, "profile-avatar"),
            html_escape(display_name),
            html_escape(&handle)
        ),
        true,
        &tabs.feeds,
        None,
        false,
        unread_badge_count(state.storage.as_ref(), &did),
    ))
    .into_response();
    page_timing.render_ms = render_started.elapsed().as_millis() as u64;
    response
}

async fn thread_page<F, S>(
    State(state): State<Arc<OAuthWebState<F, S>>>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response
where
    F: OAuthFlow,
    S: Storage + 'static,
{
    let mut page_timing = WebPageTiming::new("thread");
    let session_started = std::time::Instant::now();
    let Some(viewer_did) = verified_cookie_did(&state.public, &headers, state.storage.as_ref())
    else {
        return Redirect::to("/login").into_response();
    };
    page_timing.session_token_refresh_ms = session_started.elapsed().as_millis() as u64;
    let tabs = pinned_feeds_for_layout(
        &state.flow,
        &viewer_did,
        verified_cookie_feed_access(&state.public, &headers, state.storage.as_ref()),
    )
    .await;
    page_timing.preferences_ms = tabs.preferences_ms;
    page_timing.tabs_ms = tabs.tabs_ms;
    let page_deadline = tokio::time::Instant::now() + PAGE_PROFILE_BUDGET;
    let Some(uri) = query.get("uri") else {
        return oauth_error(StatusCode::BAD_REQUEST, "uri is required");
    };
    let record_started = std::time::Instant::now();
    let (record, record_was_live) = match load_post_record_with_source_until(
        &state.flow,
        state.storage.as_ref(),
        uri,
        page_deadline,
    )
    .await
    {
        Ok(Some(record)) => record,
        Ok(None) => return oauth_error(StatusCode::NOT_FOUND, "post not found"),
        Err(error) => return oauth_error(StatusCode::BAD_GATEWAY, &error),
    };
    if record_was_live {
        page_timing.timeline_fetch_live_ms += record_started.elapsed().as_millis() as u64;
    } else {
        page_timing.timeline_fetch_index_ms += record_started.elapsed().as_millis() as u64;
    }
    let write_forms_enabled =
        verified_cookie_write_access(&state.public, &headers, state.storage.as_ref());
    let parent_height = query
        .get("parentHeight")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(80)
        .min(6);
    let depth = query
        .get("depth")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(3)
        .min(6);
    let ancestors_started = std::time::Instant::now();
    let (ancestors, ancestor_stats) = match thread_ancestors(
        &state.flow,
        state.storage.as_ref(),
        &record,
        parent_height,
        page_deadline,
    )
    .await
    {
        Ok(ancestors) => ancestors,
        Err(error) => return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error),
    };
    page_timing.ancestors_from_index = ancestor_stats.from_index;
    page_timing.ancestors_live = ancestor_stats.live;
    page_timing.ancestors_timed_out = ancestor_stats.timed_out;
    let ancestors_ms = ancestors_started.elapsed().as_millis() as u64;
    if ancestor_stats.live > 0 || ancestor_stats.timed_out > 0 {
        page_timing.timeline_fetch_live_ms += ancestors_ms;
    } else {
        page_timing.timeline_fetch_index_ms += ancestors_ms;
    }
    let mut thread_reply_records = Vec::new();
    let mut author_reply_budget = 100;
    if let Err(error) = collect_thread_reply_records(
        state.storage.as_ref(),
        &record,
        depth,
        &mut author_reply_budget,
        &mut thread_reply_records,
    ) {
        return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string());
    }
    let page_records = ancestors
        .iter()
        .chain(std::iter::once(&record))
        .chain(thread_reply_records.iter())
        .collect::<Vec<_>>();
    let quote_index = match hydrate_page_content(
        &state.flow,
        state.storage.clone(),
        &page_records,
        std::iter::empty(),
        page_deadline,
    )
    .await
    {
        Ok(hydration) => {
            page_timing.profile_hydration_ms = hydration.profile_hydration_ms;
            page_timing.quote_hydration_ms = hydration.quote_hydration_ms;
            hydration.quote_index
        }
        Err(error) => return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    let render_started = std::time::Instant::now();
    let ancestors_html = ancestors
        .iter()
        .enumerate()
        .map(|(index, ancestor)| {
            format!(
                "<li class=\"post-item\">{}</li>",
                render_web_post(
                    state.storage.as_ref(),
                    ancestor,
                    &viewer_did,
                    write_forms_enabled,
                    false,
                    index < 6,
                    &quote_index,
                )
            )
        })
        .collect::<String>();
    let mut reply_budget = 100;
    let thread_continuations = thread_reply_records
        .windows(2)
        .filter(|pair| pair[0].did == pair[1].did)
        .map(|pair| pair[0].uri.clone())
        .collect::<HashSet<_>>();
    let reply_options = ThreadReplyRenderOptions {
        viewer_did: &viewer_did,
        write_forms_enabled,
        thread_continuations: &thread_continuations,
        quote_index: &quote_index,
    };
    let replies_html = match render_thread_replies(
        state.storage.as_ref(),
        &record,
        &reply_options,
        depth,
        &mut reply_budget,
    ) {
        Ok(replies) => replies,
        Err(error) => return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    let read_only_message = if write_forms_enabled {
        String::new()
    } else {
        "<p role=\"status\">Read-only: write permissions were not granted.</p>".to_owned()
    };
    let reply_composer = if write_forms_enabled {
        render_reply_composer(
            state.storage.as_ref(),
            &record,
            verified_cookie_blob_access(&state.public, &headers, state.storage.as_ref()),
            verified_cookie_video_access(&state.public, &headers, state.storage.as_ref()),
        )
    } else {
        String::new()
    };
    let reply_bar = if write_forms_enabled {
        render_thread_reply_bar(state.storage.as_ref(), &viewer_did)
    } else {
        String::new()
    };
    let badge_count = unread_badge_count(state.storage.as_ref(), &viewer_did);
    let mobile_navigation =
        render_thread_mobile_navigation(state.storage.as_ref(), &viewer_did, badge_count);
    let response = Html(page_shell_with_options(
        "Thread",
        &format!(
            "<header class=\"thread-topbar\"><a href=\"/app\" aria-label=\"Back to timeline\">←</a><h1>Post</h1></header>{read_only_message}<section class=\"thread-ancestors\" aria-label=\"Earlier posts in this thread\"><ol class=\"post-list\">{ancestors_html}</ol></section><section class=\"thread-focus\" aria-label=\"Selected post\">{}</section><section class=\"thread-replies\" aria-label=\"Replies\">{replies_html}</section>{reply_composer}{reply_bar}",
            render_thread_focus(
                state.storage.as_ref(),
                &record,
                &viewer_did,
                write_forms_enabled,
                true,
                &quote_index,
            )
        ),
        true,
        PageShellOptions {
            feeds: &tabs.feeds,
            active_feed: None,
            following_active: false,
            show_feed_tabs: false,
            mobile_navigation_override: Some(&mobile_navigation),
            unread_count: badge_count,
        },
    ))
    .into_response();
    page_timing.render_ms = render_started.elapsed().as_millis() as u64;
    response
}

async fn quote_composer<F, S>(
    State(state): State<Arc<OAuthWebState<F, S>>>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response
where
    F: OAuthFlow,
    S: Storage + 'static,
{
    let Some(viewer_did) = verified_cookie_did(&state.public, &headers, state.storage.as_ref())
    else {
        return Redirect::to("/login").into_response();
    };
    let Some(quote_uri) = query.get("quote_uri") else {
        return oauth_error(StatusCode::BAD_REQUEST, "quote_uri is required");
    };
    if post_uri_did(quote_uri).is_none() {
        return oauth_error(StatusCode::BAD_REQUEST, "quote_uri is not a post URI");
    }
    let quoted = match load_post_record(&state.flow, state.storage.as_ref(), quote_uri).await {
        Ok(Some(record)) => record,
        Ok(None) => return oauth_error(StatusCode::NOT_FOUND, "quoted post not found"),
        Err(error) => return oauth_error(StatusCode::BAD_GATEWAY, &error),
    };
    if quoted.cid.as_deref().is_none_or(str::is_empty) {
        return oauth_error(StatusCode::BAD_REQUEST, "quoted post has no CID");
    }
    if let Err(error) = hydrate_page_profiles(
        &state.flow,
        state.storage.clone(),
        std::iter::once(quoted.did.clone()),
        tokio::time::Instant::now() + PAGE_PROFILE_BUDGET,
    )
    .await
    {
        return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string());
    }
    let write_forms_enabled =
        verified_cookie_write_access(&state.public, &headers, state.storage.as_ref());
    let composer = if write_forms_enabled {
        format!(
            "<form class=\"composer quote-composer image-composer\" action=\"/quote\" method=\"post\" enctype=\"multipart/form-data\" data-image-composer><label for=\"quote-text\">Write a quote post</label><input type=\"hidden\" name=\"quote_uri\" value=\"{}\"><textarea id=\"quote-text\" name=\"text\" rows=\"4\" maxlength=\"300\" required></textarea>{}{}{}{}<button type=\"submit\">Post</button><p class=\"composer-error\" data-composer-error role=\"alert\" hidden></p></form>",
            html_escape(&quoted.uri),
            render_image_picker(
                verified_cookie_blob_access(&state.public, &headers, state.storage.as_ref()),
                "quote-images"
            ),
            render_video_picker(
                verified_cookie_video_access(&state.public, &headers, state.storage.as_ref()),
                "quote-video",
            ),
            render_link_picker("quote-link"),
            render_quote_preview(state.storage.as_ref(), &quoted)
        )
    } else {
        format!(
            "<p role=\"status\">Read-only: write permissions were not granted.</p>{}",
            render_quote_preview(state.storage.as_ref(), &quoted)
        )
    };
    let tabs = pinned_feeds_for_layout(
        &state.flow,
        &viewer_did,
        verified_cookie_feed_access(&state.public, &headers, state.storage.as_ref()),
    )
    .await;
    Html(page_shell_with_feeds_and_badge(
        "Quote post",
        &format!("<header class=\"page-heading\"><h1>Quote post</h1></header>{composer}"),
        true,
        &tabs.feeds,
        None,
        false,
        unread_badge_count(state.storage.as_ref(), &viewer_did),
    ))
    .into_response()
}

fn render_quote_preview<S: Storage>(storage: &S, quoted: &IndexedRecord) -> String {
    let profile = profile_view_from_storage(storage, &quoted.did)
        .unwrap_or_else(|_| json!({"did":quoted.did,"handle":quoted.did}));
    let handle = profile_handle(&profile, &quoted.did);
    let display_name = profile
        .get("displayName")
        .and_then(Value::as_str)
        .unwrap_or(&handle);
    let text = quoted
        .record
        .get("text")
        .and_then(Value::as_str)
        .unwrap_or("");
    let embed = quoted
        .record
        .get("embed")
        .map(|embed| render_web_embed(storage, &quoted.did, embed))
        .unwrap_or_default();
    let author = format!(
        "<span class=\"quoted-author\"><strong>{}</strong> <span>@{}</span></span>",
        html_escape(display_name),
        html_escape(&handle)
    );
    format!(
        "<article class=\"quoted-post-preview\" aria-label=\"Quoted post\"><header>{}{}</header><p class=\"post-text\">{}</p>{embed}</article>",
        avatar_markup(&profile, "small-avatar"),
        author,
        render_post_text(text)
    )
}

async fn thread_ancestors<F: OAuthFlow, S: Storage>(
    flow: &F,
    storage: &S,
    record: &IndexedRecord,
    parent_height: usize,
    deadline: tokio::time::Instant,
) -> Result<(Vec<IndexedRecord>, ThreadAncestorStats), String> {
    let mut stats = ThreadAncestorStats::default();
    let mut ancestors = Vec::new();
    let mut visited = HashSet::from([record.uri.clone()]);
    let mut parent_uri = record
        .record
        .pointer("/reply/parent/uri")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let root_uri = record
        .record
        .pointer("/reply/root/uri")
        .and_then(Value::as_str)
        .map(str::to_owned);
    let mut initial_uris = Vec::new();
    if let Some(parent) = parent_uri
        .as_ref()
        .filter(|uri| post_uri_did(uri).is_some())
    {
        initial_uris.push(parent.clone());
    }
    if let Some(root) = root_uri.as_ref().filter(|uri| post_uri_did(uri).is_some())
        && !initial_uris.contains(root)
    {
        initial_uris.push(root.clone());
    }
    let mut records_by_uri = HashMap::new();
    let mut source_by_uri = HashMap::new();
    let mut missing = Vec::new();
    for uri in initial_uris {
        match storage
            .record_by_uri("app.bsky.feed.post", &uri)
            .map_err(|error| error.to_string())?
        {
            Some(local) => {
                records_by_uri.insert(uri.clone(), local);
                source_by_uri.insert(uri, AncestorSource::Index);
            }
            None => missing.push(uri),
        }
    }
    let attempted_initial = missing.iter().cloned().collect::<HashSet<_>>();
    let flow_for_initial = flow.clone();
    let mut completed = HashSet::new();
    let mut initial_fetches = stream::iter(missing.iter().cloned().map(move |uri| {
        let flow = flow_for_initial.clone();
        async move {
            let fetched = flow.fetch_post(&uri).await;
            (uri, fetched)
        }
    }))
    .buffer_unordered(PAGE_QUOTE_CONCURRENCY);
    let initial_result = tokio::time::timeout_at(deadline, async {
        while let Some((uri, fetched)) = initial_fetches.next().await {
            completed.insert(uri.clone());
            if let Ok(parent) = fetched
                && parent.uri == uri
                && parent.collection == "app.bsky.feed.post"
            {
                records_by_uri.insert(uri.clone(), parent);
                source_by_uri.insert(uri, AncestorSource::Live);
            }
        }
    })
    .await;
    if initial_result.is_err() {
        stats.timed_out += missing
            .iter()
            .filter(|uri| !completed.contains(*uri))
            .count();
    }

    for _ in 0..parent_height {
        let Some(uri) = parent_uri else { break };
        if tokio::time::Instant::now() >= deadline {
            break;
        }
        if !visited.insert(uri.clone()) {
            break;
        }
        let parent = match records_by_uri.remove(&uri) {
            Some(parent) => {
                match source_by_uri.remove(&uri) {
                    Some(AncestorSource::Index) => stats.from_index += 1,
                    Some(AncestorSource::Live) => stats.live += 1,
                    None => {}
                }
                parent
            }
            None if attempted_initial.contains(&uri) => break,
            None => match storage
                .record_by_uri("app.bsky.feed.post", &uri)
                .map_err(|error| error.to_string())?
            {
                Some(parent) => {
                    stats.from_index += 1;
                    parent
                }
                None => match tokio::time::timeout_at(deadline, flow.fetch_post(&uri)).await {
                    Err(_) => {
                        stats.timed_out += 1;
                        break;
                    }
                    Ok(Ok(parent))
                        if parent.uri == uri && parent.collection == "app.bsky.feed.post" =>
                    {
                        stats.live += 1;
                        parent
                    }
                    Ok(Ok(_)) | Ok(Err(_)) => break,
                },
            },
        };
        let next_parent_uri = parent
            .record
            .pointer("/reply/parent/uri")
            .and_then(Value::as_str)
            .map(str::to_owned);
        let is_root = root_uri.as_deref() == Some(uri.as_str());
        ancestors.push(parent);
        if is_root {
            break;
        }
        parent_uri = next_parent_uri;
    }
    ancestors.reverse();
    Ok((ancestors, stats))
}

async fn load_post_record<F: OAuthFlow, S: Storage>(
    flow: &F,
    storage: &S,
    uri: &str,
) -> Result<Option<IndexedRecord>, String> {
    load_post_record_until(
        flow,
        storage,
        uri,
        tokio::time::Instant::now() + Duration::from_secs(10),
    )
    .await
}

async fn load_post_record_until<F: OAuthFlow, S: Storage>(
    flow: &F,
    storage: &S,
    uri: &str,
    deadline: tokio::time::Instant,
) -> Result<Option<IndexedRecord>, String> {
    Ok(
        load_post_record_with_source_until(flow, storage, uri, deadline)
            .await?
            .map(|(record, _)| record),
    )
}

async fn load_post_record_with_source_until<F: OAuthFlow, S: Storage>(
    flow: &F,
    storage: &S,
    uri: &str,
    deadline: tokio::time::Instant,
) -> Result<Option<(IndexedRecord, bool)>, String> {
    if let Some(record) = storage
        .record_by_uri("app.bsky.feed.post", uri)
        .map_err(|error| error.to_string())?
    {
        return Ok(Some((record, false)));
    }
    let record = tokio::time::timeout_at(deadline, flow.fetch_post(uri))
        .await
        .map_err(|_| "post fetch timed out".to_owned())??;
    if record.uri != uri || record.collection != "app.bsky.feed.post" {
        return Err("post fetch returned a different record".to_owned());
    }
    Ok(Some((record, true)))
}

fn render_reply_composer<S: Storage>(
    storage: &S,
    parent: &IndexedRecord,
    blob_access: bool,
    video_access: bool,
) -> String {
    let profile = profile_view_from_storage(storage, &parent.did)
        .unwrap_or_else(|_| json!({"did":parent.did,"handle":parent.did}));
    let handle = profile_handle(&profile, &parent.did);
    format!(
        "<form id=\"reply-composer\" class=\"composer reply-composer image-composer\" action=\"/reply\" method=\"post\" enctype=\"multipart/form-data\" data-image-composer><p class=\"reply-context\">Replying to @{}</p><input type=\"hidden\" name=\"parent_uri\" value=\"{}\"><label for=\"reply-text\">Write a reply</label><textarea id=\"reply-text\" name=\"text\" rows=\"3\" maxlength=\"300\" required></textarea>{}{}{}<button type=\"submit\">Reply</button><p class=\"composer-error\" data-composer-error role=\"alert\" hidden></p></form>",
        html_escape(&handle),
        html_escape(&parent.uri),
        render_image_picker(blob_access, "reply-images"),
        render_video_picker(video_access, "reply-video"),
        render_link_picker("reply-link")
    )
}

fn render_image_picker(blob_access: bool, id: &str) -> String {
    if !blob_access {
        return "<p class=\"picture-permission-hint\" role=\"status\"><a href=\"/login\">Log in again to enable pictures</a></p>".to_owned();
    }
    format!(
        "<fieldset class=\"image-picker\"><legend>Add pictures</legend><label for=\"{id}\">Choose up to 4 images</label><input id=\"{id}\" type=\"file\" name=\"images\" accept=\"image/*\" multiple data-image-input><div class=\"image-previews\" data-image-previews></div><p class=\"image-picker-status\" data-image-status role=\"status\"></p></fieldset>"
    )
}

fn render_video_picker(video_access: bool, id: &str) -> String {
    if !video_access {
        return "<p class=\"video-permission-hint\" role=\"status\"><a href=\"/login\">sign in again to post videos</a></p>".to_owned();
    }
    format!(
        "<fieldset class=\"video-picker\"><legend>Add a video</legend><label for=\"{id}\">Choose one MP4, WebM, or MOV video (up to 300 MB and 10 minutes)</label><input id=\"{id}\" type=\"file\" name=\"video\" accept=\"video/mp4,video/webm,video/quicktime,.mp4,.webm,.mov\" data-video-input><video class=\"video-picker-preview\" data-video-preview muted controls playsinline preload=\"metadata\" hidden></video><p class=\"video-picker-status\" data-video-summary role=\"status\" aria-live=\"polite\">No video selected.</p><label class=\"video-alt-label\" for=\"{id}-alt\">Video alt text (optional)<textarea id=\"{id}-alt\" name=\"video_alt\" maxlength=\"2000\" rows=\"2\" data-video-alt></textarea></label><button type=\"button\" data-video-remove hidden>Remove video</button><progress data-video-progress max=\"100\" value=\"0\" hidden></progress><p class=\"video-upload-status\" data-video-upload-status role=\"status\" aria-live=\"polite\"></p></fieldset>"
    )
}

fn render_link_picker(id: &str) -> String {
    format!(
        "<fieldset class=\"link-picker\"><legend>Add a webpage</legend><label for=\"{id}\">Webpage URL</label><div class=\"link-picker-controls\"><input id=\"{id}\" name=\"link_url\" type=\"url\" inputmode=\"url\" placeholder=\"https://example.com/article\" data-link-input><button type=\"button\" data-link-preview>Preview link</button></div><p class=\"link-picker-status\" data-link-status role=\"status\" aria-live=\"polite\"></p><div class=\"link-preview\" data-link-preview-card hidden></div><p class=\"composer-notice\" data-link-image-note hidden>Pictures will be attached to the post; the webpage link will be omitted.</p></fieldset>"
    )
}

fn render_thread_reply_bar<S: Storage>(storage: &S, viewer_did: &str) -> String {
    let profile = profile_view_from_storage(storage, viewer_did)
        .unwrap_or_else(|_| json!({"did":viewer_did,"handle":viewer_did}));
    format!(
        "<aside class=\"thread-reply-bar\" aria-label=\"Mobile reply bar\">{}<a href=\"#reply-composer\">Write your reply</a></aside>",
        avatar_markup(&profile, "small-avatar")
    )
}

fn render_thread_mobile_navigation<S: Storage>(
    storage: &S,
    viewer_did: &str,
    unread_count: usize,
) -> String {
    let profile = profile_view_from_storage(storage, viewer_did)
        .unwrap_or_else(|_| json!({"did":viewer_did,"handle":viewer_did}));
    format!(
        "<nav class=\"mobile-nav\" aria-label=\"Mobile navigation\"><a href=\"/app\">{}<span>Home</span></a><button class=\"mobile-search\" type=\"button\" aria-label=\"Mobile search\" disabled>{}<span>Search</span></button><a href=\"/feeds\">{}<span>Feeds</span></a><a class=\"notification-nav-link\" href=\"/notifications\">{}<span>Notifications</span>{}</a><a href=\"/profile\" aria-label=\"Profile\">{}</a></nav><a class=\"mobile-compose\" href=\"/app#new-post\" aria-label=\"New post\">{}</a>",
        icon_svg("home"),
        icon_svg("search"),
        icon_svg("feeds"),
        icon_svg("notifications"),
        notification_badge_markup(unread_count),
        avatar_markup(&profile, "mobile-profile-avatar"),
        icon_svg("compose")
    )
}

fn render_thread_focus<S: Storage>(
    storage: &S,
    record: &IndexedRecord,
    viewer_did: &str,
    write_forms_enabled: bool,
    eager_avatar: bool,
    quote_index: &QuoteRenderIndex,
) -> String {
    let profile = profile_view_from_storage(storage, &record.did)
        .unwrap_or_else(|_| json!({"did":record.did,"handle":record.did}));
    let handle = profile_handle(&profile, &record.did);
    let display_name = profile
        .get("displayName")
        .and_then(Value::as_str)
        .unwrap_or(&handle);
    let profile_uri = format!("/profile?did={}", form_encoded(&record.did));
    let text = record
        .record
        .get("text")
        .and_then(Value::as_str)
        .unwrap_or("");
    let text = if text.chars().count() > MAX_RENDERED_REMOTE_CHARS {
        text.chars()
            .take(MAX_RENDERED_REMOTE_CHARS)
            .collect::<String>()
    } else {
        text.to_owned()
    };
    let text = render_post_text(&text);
    let time = record
        .record
        .get("createdAt")
        .and_then(Value::as_str)
        .map(thread_date_label)
        .unwrap_or_else(|| "time unavailable".to_owned());
    let threadgate = storage
        .get_record(&record.did, "app.bsky.feed.threadgate", &record.rkey)
        .ok()
        .flatten();
    let reply_policy = if threadgate.is_none() {
        " · Everybody can reply"
    } else {
        ""
    };
    let interactions = storage
        .post_interaction_summary(&record.uri, Some(viewer_did))
        .unwrap_or_default();
    let stats = [
        (interactions.repost_count, "repost", "reposts"),
        (interactions.quote_count, "quote", "quotes"),
        (interactions.like_count, "like", "likes"),
    ]
    .into_iter()
    .filter(|(count, _, _)| *count > 0)
    .map(|(count, singular, plural)| {
        format!(
            "<span><strong>{count}</strong> {}</span>",
            if count == 1 { singular } else { plural }
        )
    })
    .collect::<Vec<_>>()
    .join(" ");
    let thread_uri = format!("/app/thread?uri={}", form_encoded(&record.uri));
    let uri = html_escape(&record.uri);
    let cid = html_escape(record.cid.as_deref().unwrap_or(""));
    let reply_action = format!(
        "<a class=\"post-action\" href=\"{}\" aria-label=\"Reply\">{}<span class=\"sr-only\">Reply</span>{}</a>",
        html_escape(&thread_uri),
        icon_svg("reply"),
        action_count_markup(interactions.reply_count)
    );
    let repost_action = if write_forms_enabled {
        format!(
            "<details class=\"repost-menu\"><summary class=\"post-action\" aria-label=\"Repost menu\">{}<span class=\"sr-only\">Repost</span>{}</summary><div class=\"repost-menu-options\"><form class=\"post-action-form\" action=\"/repost\" method=\"post\"><input type=\"hidden\" name=\"uri\" value=\"{uri}\"><input type=\"hidden\" name=\"cid\" value=\"{cid}\"><button class=\"post-action\" type=\"submit\" aria-label=\"Repost by @{}\">Repost</button></form><a href=\"/compose?quote_uri={}\">Quote post</a></div></details>",
            icon_svg("repost"),
            action_count_markup(interactions.repost_count),
            html_escape(&handle),
            form_encoded(&record.uri)
        )
    } else {
        format!(
            "<span class=\"post-action action-readonly\" aria-label=\"Repost count\">{}{}</span>",
            icon_svg("repost"),
            action_count_markup(interactions.repost_count)
        )
    };
    let like_action = if write_forms_enabled {
        let liked = interactions.viewer_like.is_some();
        let intent = interactions
            .viewer_like
            .as_ref()
            .map(|like| {
                format!(
                    "<input type=\"hidden\" name=\"intent\" value=\"delete\"><input type=\"hidden\" name=\"like_uri\" value=\"{}\">",
                    html_escape(like)
                )
            })
            .unwrap_or_default();
        let like_aria = if liked {
            format!("Unlike post by @{}", html_escape(&handle))
        } else {
            format!("Like post by @{}", html_escape(&handle))
        };
        format!(
            "<form class=\"post-action-form\" action=\"/like\" method=\"post\"><input type=\"hidden\" name=\"uri\" value=\"{uri}\"><input type=\"hidden\" name=\"cid\" value=\"{cid}\">{intent}<button class=\"post-action{}\" type=\"submit\" aria-label=\"{like_aria}\" aria-pressed=\"{liked}\">{}<span class=\"sr-only\">Like</span>{}</button></form>",
            if liked { " is-liked" } else { "" },
            icon_svg("like"),
            action_count_markup(interactions.like_count)
        )
    } else {
        format!(
            "<span class=\"post-action action-readonly{}\" aria-label=\"Like count\">{}{}</span>",
            if interactions.viewer_like.is_some() {
                " is-liked"
            } else {
                ""
            },
            icon_svg("like"),
            action_count_markup(interactions.like_count)
        )
    };
    let share_action = format!(
        "<a class=\"post-action\" href=\"{}\" aria-label=\"Share or copy link\">{}<span>Share</span></a>",
        html_escape(&thread_uri),
        icon_svg("share")
    );
    let more_action = format!(
        "<details class=\"thread-more-menu\"><summary class=\"post-action\" aria-label=\"More post actions\">{}</summary><div class=\"repost-menu-options\"><a href=\"{}\">Copy link</a><a href=\"{}\">View profile</a></div></details>",
        icon_svg("more"),
        html_escape(&thread_uri),
        html_escape(&profile_uri)
    );
    let embed = record
        .record
        .get("embed")
        .map(|embed| render_web_embed_with_quotes(storage, &record.did, embed, quote_index, 0))
        .unwrap_or_default();
    format!(
        "<article class=\"post-card thread-post-focus\"><header class=\"thread-focus-author\">{}<div class=\"thread-focus-names\"><a class=\"author-name\" href=\"{}\">{}</a><a class=\"author-handle\" href=\"{}\">@{}</a></div></header><p class=\"thread-focus-text\">{text}</p><p class=\"thread-focus-meta\">{time}{reply_policy}</p><hr><div class=\"thread-stats\" aria-label=\"Post statistics\">{stats}</div><hr><div class=\"thread-focus-actions\">{reply_action}{repost_action}{like_action}{share_action}{more_action}</div>{embed}</article>",
        avatar_markup_with_priority(&profile, "focus-avatar", eager_avatar),
        html_escape(&profile_uri),
        html_escape(display_name),
        html_escape(&profile_uri),
        html_escape(&handle)
    )
}

fn thread_date_label(created_at: &str) -> String {
    let Ok(created_at) = OffsetDateTime::parse(created_at, &Rfc3339) else {
        return "time unavailable".to_owned();
    };
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    format!(
        "{:02}:{:02} · {} {} {}",
        created_at.hour(),
        created_at.minute(),
        created_at.day(),
        MONTHS[created_at.month() as usize - 1],
        created_at.year()
    )
}

fn collect_thread_reply_records<S: Storage>(
    storage: &S,
    record: &IndexedRecord,
    depth: usize,
    budget: &mut usize,
    output: &mut Vec<IndexedRecord>,
) -> Result<(), StorageError> {
    if depth == 0 || *budget == 0 {
        return Ok(());
    }
    let replies = storage.replies_to(&record.uri, (*budget).min(100))?;
    for reply in replies {
        if *budget == 0 {
            break;
        }
        *budget -= 1;
        output.push(reply.clone());
        collect_thread_reply_records(storage, &reply, depth - 1, budget, output)?;
    }
    Ok(())
}

struct ThreadReplyRenderOptions<'a> {
    viewer_did: &'a str,
    write_forms_enabled: bool,
    thread_continuations: &'a HashSet<String>,
    quote_index: &'a QuoteRenderIndex,
}

fn render_thread_replies<S: Storage>(
    storage: &S,
    record: &IndexedRecord,
    options: &ThreadReplyRenderOptions<'_>,
    depth: usize,
    budget: &mut usize,
) -> Result<String, StorageError> {
    if depth == 0 || *budget == 0 {
        return Ok(String::new());
    }
    let replies = storage.replies_to(&record.uri, (*budget).min(100))?;
    if replies.is_empty() {
        return Ok(String::new());
    }
    let mut html = String::from("<ol>");
    for reply in &replies {
        if *budget == 0 {
            break;
        }
        *budget -= 1;
        let nested = render_thread_replies(storage, reply, options, depth - 1, budget)?;
        let continues = options.thread_continuations.contains(&reply.uri);
        html.push_str(&format!(
            "<li class=\"post-item thread-reply-item{}\">{}{nested}</li>",
            if continues { " thread-continues" } else { "" },
            render_web_post(
                storage,
                reply,
                options.viewer_did,
                options.write_forms_enabled,
                false,
                false,
                options.quote_index
            )
        ));
    }
    html.push_str("</ol>");
    Ok(html)
}

const MAX_POST_IMAGES: usize = 4;
const MAX_IMAGE_BYTES: usize = 1_000_000;
const MAX_IMAGE_ALT_CHARS: usize = 2000;
const MAX_POST_BODY_BYTES: usize = 5 * 1024 * 1024;

struct PendingPostImage {
    bytes: Vec<u8>,
    mime_type: String,
}

struct PostSubmission {
    text: String,
    parent_uri: Option<String>,
    quote_uri: Option<String>,
    link_url: Option<String>,
    images: Vec<PostImage>,
}

struct VideoSubmission {
    text: String,
    parent_uri: Option<String>,
    quote_uri: Option<String>,
    video: PostVideo,
}

struct TemporaryVideoFile(Option<PathBuf>);

impl TemporaryVideoFile {
    fn take_path(&mut self) -> PathBuf {
        self.0.take().expect("temporary video path is present")
    }
}

impl Drop for TemporaryVideoFile {
    fn drop(&mut self) {
        if let Some(path) = self.0.take()
            && let Err(error) = fs::remove_file(&path)
            && error.kind() != std::io::ErrorKind::NotFound
        {
            tracing::warn!(path = %path.display(), %error, "could not remove incomplete video upload");
        }
    }
}

fn create_temporary_video_file() -> Result<(TemporaryVideoFile, tokio::fs::File), String> {
    for _ in 0..4 {
        let mut nonce = [0u8; 18];
        thread_rng().fill_bytes(&mut nonce);
        let path = std::env::temp_dir().join(format!(
            "appview-video-{}-{}",
            std::process::id(),
            URL_SAFE_NO_PAD.encode(nonce)
        ));
        let mut options = fs::OpenOptions::new();
        options.write(true).create_new(true);
        #[cfg(unix)]
        {
            use std::os::unix::fs::OpenOptionsExt;
            options.mode(0o600);
        }
        match options.open(&path) {
            Ok(file) => {
                return Ok((
                    TemporaryVideoFile(Some(path)),
                    tokio::fs::File::from_std(file),
                ));
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
            Err(error) => {
                return Err(format!(
                    "temporary video file could not be created: {error}"
                ));
            }
        }
    }
    Err("temporary video file could not be created".to_owned())
}

async fn parse_video_submission<F, S>(
    request: Request,
    state: &Arc<OAuthWebState<F, S>>,
) -> Result<VideoSubmission, (StatusCode, String)>
where
    F: OAuthFlow,
    S: Storage + 'static,
{
    let content_type = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_ascii_lowercase();
    if !content_type.starts_with("multipart/form-data") {
        return Err((
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "Use a multipart form to upload a video.".to_owned(),
        ));
    }
    let mut multipart = Multipart::from_request(request, state).await.map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            "The video form could not be read.".to_owned(),
        )
    })?;
    let mut text = String::new();
    let mut parent_uri = None;
    let mut quote_uri = None;
    let mut alt = String::new();
    let mut duration = None;
    let mut width = None;
    let mut height = None;
    let mut video_file: Option<(TemporaryVideoFile, String, u64, Vec<u8>)> = None;
    while let Some(mut field) = multipart.next_field().await.map_err(|_| {
        (
            StatusCode::BAD_REQUEST,
            "The video form could not be read.".to_owned(),
        )
    })? {
        let name = field.name().unwrap_or_default().to_owned();
        if name == "video" {
            if video_file.is_some() {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "Choose one video per post.".to_owned(),
                ));
            }
            let mime_type = field
                .content_type()
                .unwrap_or_default()
                .trim()
                .to_ascii_lowercase();
            let (temporary, mut file) = create_temporary_video_file()
                .map_err(|message| (StatusCode::INTERNAL_SERVER_ERROR, message))?;
            let mut size = 0u64;
            let mut prefix = Vec::with_capacity(16);
            while let Some(chunk) = field.chunk().await.map_err(|_| {
                (
                    StatusCode::BAD_REQUEST,
                    "The video file could not be read.".to_owned(),
                )
            })? {
                size = size.saturating_add(chunk.len() as u64);
                if size > MAX_VIDEO_BYTES {
                    return Err((
                        StatusCode::PAYLOAD_TOO_LARGE,
                        "Videos must be no larger than 300,000,000 bytes.".to_owned(),
                    ));
                }
                if prefix.len() < 16 {
                    prefix.extend_from_slice(&chunk[..chunk.len().min(16 - prefix.len())]);
                }
                file.write_all(&chunk).await.map_err(|error| {
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        format!("Video could not be saved temporarily: {error}"),
                    )
                })?;
            }
            file.flush().await.map_err(|error| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Video could not be saved temporarily: {error}"),
                )
            })?;
            drop(file);
            video_file = Some((temporary, mime_type, size, prefix));
        } else {
            const MAX_VIDEO_FORM_FIELD_BYTES: usize = 8192;
            let mut value = Vec::new();
            while let Some(chunk) = field.chunk().await.map_err(|_| {
                (
                    StatusCode::BAD_REQUEST,
                    "A video form field could not be read.".to_owned(),
                )
            })? {
                if value.len().saturating_add(chunk.len()) > MAX_VIDEO_FORM_FIELD_BYTES {
                    return Err((
                        StatusCode::BAD_REQUEST,
                        "A video form field is too long.".to_owned(),
                    ));
                }
                value.extend_from_slice(&chunk);
            }
            let value = String::from_utf8(value).map_err(|_| {
                (
                    StatusCode::BAD_REQUEST,
                    "A video form field is invalid.".to_owned(),
                )
            })?;
            match name.as_str() {
                "text" => text = value,
                "parent_uri" => parent_uri = Some(value),
                "quote_uri" => quote_uri = Some(value),
                "video_alt" => alt = value,
                "video_duration" => duration = value.parse::<f64>().ok(),
                "video_width" => width = value.parse::<u32>().ok(),
                "video_height" => height = value.parse::<u32>().ok(),
                _ => {}
            }
        }
    }
    let Some((mut temporary, mime_type, size_bytes, prefix)) = video_file else {
        return Err((
            StatusCode::BAD_REQUEST,
            "Choose a video to post.".to_owned(),
        ));
    };
    let duration_seconds = duration.ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            "Video duration could not be read. Choose the video again.".to_owned(),
        )
    })?;
    validate_video_properties(size_bytes, duration_seconds, &mime_type)
        .map_err(|message| (StatusCode::BAD_REQUEST, message))?;
    if alt.chars().count() > MAX_VIDEO_ALT_CHARS {
        return Err((
            StatusCode::BAD_REQUEST,
            "Video alt text must be no more than 2,000 characters.".to_owned(),
        ));
    }
    let (Some(width), Some(height)) = (
        width.filter(|value| (1..=10_000).contains(value)),
        height.filter(|value| (1..=10_000).contains(value)),
    ) else {
        return Err((
            StatusCode::BAD_REQUEST,
            "Video dimensions could not be read. Choose the video again.".to_owned(),
        ));
    };
    if !video_content_matches(&prefix, &mime_type) {
        return Err((
            StatusCode::BAD_REQUEST,
            "The video contents do not match the declared video type.".to_owned(),
        ));
    }
    let extension = video_extension(&mime_type);
    let stored_path = temporary.0.as_deref().ok_or_else(|| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            "Video temporary file was lost.".to_owned(),
        )
    })?;
    let filename = format!(
        "{}.{}",
        stored_path
            .file_name()
            .unwrap_or_default()
            .to_string_lossy(),
        extension
    );
    let path = temporary.take_path();
    let video = PostVideo {
        path,
        mime_type,
        alt,
        width,
        height,
        duration_seconds,
        size_bytes,
        filename,
    };
    Ok(VideoSubmission {
        text,
        parent_uri,
        quote_uri,
        video,
    })
}

fn video_content_matches(prefix: &[u8], mime_type: &str) -> bool {
    match mime_type {
        "video/mp4" | "video/quicktime" => prefix.len() >= 12 && &prefix[4..8] == b"ftyp",
        "video/webm" => prefix.starts_with(&[0x1a, 0x45, 0xdf, 0xa3]),
        _ => false,
    }
}

fn video_extension(mime_type: &str) -> &'static str {
    match mime_type {
        "video/webm" => "webm",
        "video/quicktime" => "mov",
        _ => "mp4",
    }
}

async fn parse_post_submission<F, S>(
    request: Request,
    state: &Arc<OAuthWebState<F, S>>,
) -> Result<PostSubmission, (StatusCode, String)>
where
    F: OAuthFlow,
    S: Storage + 'static,
{
    let content_type = request
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_ascii_lowercase();
    if content_type.starts_with("multipart/form-data") {
        let mut multipart = Multipart::from_request(request, state)
            .await
            .map_err(|_| {
                (
                    StatusCode::BAD_REQUEST,
                    "The multipart post form could not be read. Check the image selection and try again."
                        .to_owned(),
                )
            })?;
        let mut text = String::new();
        let mut parent_uri = None;
        let mut quote_uri = None;
        let mut link_url = None;
        let mut pending_images = Vec::new();
        let mut alts = Vec::new();
        let mut widths = Vec::new();
        let mut heights = Vec::new();
        while let Some(field) = multipart.next_field().await.map_err(|_| {
            (
                StatusCode::BAD_REQUEST,
                "The multipart post form could not be read. Check the image selection and try again."
                    .to_owned(),
            )
        })? {
            let name = field.name().unwrap_or_default().to_owned();
            if name == "video" {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "Enable JavaScript to validate and upload videos.".to_owned(),
                ));
            }
            if name == "images" {
                let filename = field.file_name().unwrap_or_default().to_owned();
                let mime_type = field.content_type().unwrap_or_default().to_owned();
                let bytes = field.bytes().await.map_err(|_| {
                    (
                        StatusCode::BAD_REQUEST,
                        "An image could not be read from the form.".to_owned(),
                    )
                })?;
                if filename.is_empty() && bytes.is_empty() {
                    continue;
                }
                if pending_images.len() == MAX_POST_IMAGES {
                    return Err((
                        StatusCode::BAD_REQUEST,
                        "Choose no more than 4 images per post.".to_owned(),
                    ));
                }
                pending_images.push(PendingPostImage {
                    bytes: bytes.to_vec(),
                    mime_type,
                });
            } else {
                let value = field.text().await.map_err(|_| {
                    (
                        StatusCode::BAD_REQUEST,
                        "A post form field could not be read.".to_owned(),
                    )
                })?;
                match name.as_str() {
                    "text" => text = value,
                    "parent_uri" => parent_uri = Some(value),
                    "quote_uri" => quote_uri = Some(value),
                    "link_url" => link_url = Some(value),
                    "alts" => alts.push(value),
                    "widths" => widths.push(value),
                    "heights" => heights.push(value),
                    _ => {}
                }
            }
        }
        let images = validate_post_images(pending_images, alts, widths, heights)?;
        Ok(PostSubmission {
            text,
            parent_uri,
            quote_uri,
            link_url,
            images,
        })
    } else if content_type.starts_with("application/x-www-form-urlencoded") {
        let Form(form) = Form::<HashMap<String, String>>::from_request(request, state)
            .await
            .map_err(|_| {
                (
                    StatusCode::BAD_REQUEST,
                    "The post form could not be read.".to_owned(),
                )
            })?;
        Ok(PostSubmission {
            text: form.get("text").cloned().unwrap_or_default(),
            parent_uri: form.get("parent_uri").cloned(),
            quote_uri: form.get("quote_uri").cloned(),
            link_url: form.get("link_url").cloned(),
            images: Vec::new(),
        })
    } else {
        Err((
            StatusCode::UNSUPPORTED_MEDIA_TYPE,
            "Use a post form with text and optional image files.".to_owned(),
        ))
    }
}

fn validate_post_images(
    pending: Vec<PendingPostImage>,
    alts: Vec<String>,
    widths: Vec<String>,
    heights: Vec<String>,
) -> Result<Vec<PostImage>, (StatusCode, String)> {
    if pending.is_empty() {
        return Ok(Vec::new());
    }
    if pending.len() > MAX_POST_IMAGES {
        return Err((
            StatusCode::BAD_REQUEST,
            "Choose no more than 4 images per post.".to_owned(),
        ));
    }
    if alts.len() > pending.len() || widths.len() != pending.len() || heights.len() != pending.len()
    {
        return Err((
            StatusCode::BAD_REQUEST,
            "Each image needs an alt text value and image dimensions. Reload the form and try again."
                .to_owned(),
        ));
    }
    pending
        .into_iter()
        .enumerate()
        .map(|(index, image)| {
            let mime_type = image.mime_type.trim().to_ascii_lowercase();
            if !matches!(mime_type.as_str(), "image/jpeg" | "image/png" | "image/webp" | "image/gif") {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "Images must be JPEG, PNG, WebP, or GIF files.".to_owned(),
                ));
            }
            if image.bytes.len() > MAX_IMAGE_BYTES {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "Each image must be no larger than 1,000,000 bytes.".to_owned(),
                ));
            }
            if image_content_type(&image.bytes) != Some(mime_type.as_str()) {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "The image contents do not match the declared image type.".to_owned(),
                ));
            }
            let alt = alts.get(index).cloned().unwrap_or_default();
            if alt.chars().count() > MAX_IMAGE_ALT_CHARS {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "Image alt text must be no more than 2,000 characters.".to_owned(),
                ));
            }
            let width = widths[index].parse::<u32>().ok().filter(|value| (1..=2000).contains(value));
            let height = heights[index].parse::<u32>().ok().filter(|value| (1..=2000).contains(value));
            let (Some(width), Some(height)) = (width, height) else {
                return Err((
                    StatusCode::BAD_REQUEST,
                    "Image dimensions must be between 1 and 2,000 pixels. Enable JavaScript to prepare images for posting."
                        .to_owned(),
                ));
            };
            Ok(PostImage {
                bytes: image.bytes,
                mime_type,
                alt,
                width,
                height,
            })
        })
        .collect()
}

fn image_content_type(bytes: &[u8]) -> Option<&'static str> {
    if bytes.starts_with(&[0xff, 0xd8, 0xff]) {
        Some("image/jpeg")
    } else if bytes.starts_with(b"\x89PNG\r\n\x1a\n") {
        Some("image/png")
    } else if bytes.starts_with(b"GIF87a") || bytes.starts_with(b"GIF89a") {
        Some("image/gif")
    } else if bytes.len() >= 12 && bytes.starts_with(b"RIFF") && &bytes[8..12] == b"WEBP" {
        Some("image/webp")
    } else {
        None
    }
}

fn post_external_from_card(card: LinkCard) -> PostExternal {
    PostExternal {
        uri: card.uri,
        title: card.title,
        description: card.description,
        thumbnail: card.thumbnail,
    }
}

fn selected_link_url(has_images: bool, link_url: Option<&str>) -> Option<&str> {
    if has_images {
        None
    } else {
        link_url.map(str::trim).filter(|value| !value.is_empty())
    }
}

async fn upload_video<F, S>(
    State(state): State<Arc<OAuthWebState<F, S>>>,
    request: Request,
) -> Response
where
    F: OAuthFlow,
    S: Storage + 'static,
{
    let headers = request.headers().clone();
    if !same_origin_request(&headers, &state.public.public_url) {
        return oauth_error(StatusCode::FORBIDDEN, "request must come from this origin");
    }
    let Some(did) = verified_cookie_did(&state.public, &headers, state.storage.as_ref()) else {
        return Redirect::to("/login").into_response();
    };
    if !verified_cookie_write_access(&state.public, &headers, state.storage.as_ref()) {
        return oauth_error(StatusCode::FORBIDDEN, "write permissions were not granted");
    }
    if !verified_cookie_video_access(&state.public, &headers, state.storage.as_ref()) {
        return oauth_error(StatusCode::FORBIDDEN, "sign in again to post videos");
    }
    let submission = match parse_video_submission(request, &state).await {
        Ok(submission) => submission,
        Err((status, message)) => return oauth_error(status, &message),
    };
    let text = submission.text.trim();
    if text.is_empty() || text.chars().count() > 300 {
        return oauth_error(
            StatusCode::BAD_REQUEST,
            "post text must contain 1 to 300 characters",
        );
    }
    if submission.parent_uri.is_some() && submission.quote_uri.is_some() {
        return oauth_error(
            StatusCode::BAD_REQUEST,
            "A video post cannot be both a reply and a quote.",
        );
    }

    let mut reply = None;
    let mut quote = None;
    let mut redirect = "/app".to_owned();
    if let Some(parent_uri) = submission.parent_uri.as_deref() {
        if post_uri_did(parent_uri).is_none() {
            return oauth_error(StatusCode::BAD_REQUEST, "parent_uri is not a post URI");
        }
        let parent = match load_post_record(&state.flow, state.storage.as_ref(), parent_uri).await {
            Ok(Some(record)) => record,
            Ok(None) => return oauth_error(StatusCode::NOT_FOUND, "parent post not found"),
            Err(error) => return oauth_error(StatusCode::BAD_GATEWAY, &error),
        };
        let Some(parent_cid) = parent.cid.as_deref().filter(|cid| !cid.is_empty()) else {
            return oauth_error(StatusCode::BAD_REQUEST, "parent post has no CID");
        };
        let parent_ref = PostStrongRef {
            uri: parent.uri.clone(),
            cid: parent_cid.to_owned(),
        };
        let root_ref = if let Some(root) = parent.record.pointer("/reply/root") {
            let (Some(uri), Some(cid)) = (
                root.get("uri").and_then(Value::as_str),
                root.get("cid").and_then(Value::as_str),
            ) else {
                return oauth_error(
                    StatusCode::BAD_REQUEST,
                    "parent reply has no root reference",
                );
            };
            PostStrongRef {
                uri: uri.to_owned(),
                cid: cid.to_owned(),
            }
        } else {
            parent_ref.clone()
        };
        reply = Some(json!({"root":root_ref,"parent":parent_ref}));
    }
    if let Some(quote_uri) = submission.quote_uri.as_deref() {
        if post_uri_did(quote_uri).is_none() {
            return oauth_error(StatusCode::BAD_REQUEST, "quote_uri is not a post URI");
        }
        let quoted = match load_post_record(&state.flow, state.storage.as_ref(), quote_uri).await {
            Ok(Some(record)) => record,
            Ok(None) => return oauth_error(StatusCode::NOT_FOUND, "quoted post not found"),
            Err(error) => return oauth_error(StatusCode::BAD_GATEWAY, &error),
        };
        let Some(cid) = quoted.cid.as_deref().filter(|cid| !cid.is_empty()) else {
            return oauth_error(StatusCode::BAD_REQUEST, "quoted post has no CID");
        };
        quote = Some(PostStrongRef {
            uri: quoted.uri,
            cid: cid.to_owned(),
        });
    }

    let created = state
        .flow
        .create_post_with_video(&did, text, submission.video, reply, quote)
        .await;
    let created = match created {
        Ok(created) => created,
        Err(error) => {
            tracing::warn!(%error, "video upload or processing failed before record creation");
            return oauth_error(StatusCode::BAD_GATEWAY, &error);
        }
    };
    if submission.parent_uri.is_some() {
        redirect = format!("/app/thread?uri={}", form_encoded(&created.uri));
    }
    match persist_created_record(state.storage.clone(), &did, "app.bsky.feed.post", created).await {
        Ok(()) => Redirect::to(&redirect).into_response(),
        Err(error) => oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error),
    }
}

fn external_embed_value(external: &PostExternal, thumb_blob: Option<Value>) -> Value {
    let mut value = json!({
        "$type":"app.bsky.embed.external#external",
        "uri":external.uri,
        "title":external.title,
        "description":external.description
    });
    if let Some(thumb_blob) = thumb_blob {
        value["thumb"] = thumb_blob;
    }
    json!({
        "$type":"app.bsky.embed.external",
        "external":value
    })
}

#[derive(Deserialize)]
struct LinkPreviewRequest {
    url: String,
}

async fn link_preview<F, S>(
    State(state): State<Arc<OAuthWebState<F, S>>>,
    headers: HeaderMap,
    Form(input): Form<LinkPreviewRequest>,
) -> Response
where
    F: OAuthFlow,
    S: Storage + 'static,
{
    if !same_origin_request(&headers, &state.public.public_url) {
        return oauth_error(StatusCode::FORBIDDEN, "request must come from this origin");
    }
    if verified_cookie_did(&state.public, &headers, state.storage.as_ref()).is_none() {
        return Redirect::to("/login").into_response();
    }
    if !verified_cookie_write_access(&state.public, &headers, state.storage.as_ref()) {
        return oauth_error(StatusCode::FORBIDDEN, "write permissions were not granted");
    }
    match state.link_card_fetcher.fetch(&input.url).await {
        Ok(card) => Json(card.preview()).into_response(),
        Err(error) => oauth_error(StatusCode::BAD_REQUEST, &error),
    }
}

async fn write_post<F, S>(
    State(state): State<Arc<OAuthWebState<F, S>>>,
    request: Request,
) -> Response
where
    F: OAuthFlow,
    S: Storage + 'static,
{
    let headers = request.headers().clone();
    if !same_origin_request(&headers, &state.public.public_url) {
        return oauth_error(StatusCode::FORBIDDEN, "request must come from this origin");
    }
    let Some(did) = verified_cookie_did(&state.public, &headers, state.storage.as_ref()) else {
        return Redirect::to("/login").into_response();
    };
    if !verified_cookie_write_access(&state.public, &headers, state.storage.as_ref()) {
        return oauth_error(StatusCode::FORBIDDEN, "write permissions were not granted");
    }
    let submission = match parse_post_submission(request, &state).await {
        Ok(submission) => submission,
        Err((status, message)) => return oauth_error(status, &message),
    };
    if !submission.images.is_empty()
        && !verified_cookie_blob_access(&state.public, &headers, state.storage.as_ref())
    {
        return oauth_error(
            StatusCode::FORBIDDEN,
            "Log in again to enable pictures before posting images.",
        );
    }
    let text = submission.text.trim();
    if text.is_empty() || text.chars().count() > 300 {
        return oauth_error(
            StatusCode::BAD_REQUEST,
            "post text must contain 1 to 300 characters",
        );
    }
    let has_images = !submission.images.is_empty();
    let external =
        if let Some(link_url) = selected_link_url(has_images, submission.link_url.as_deref()) {
            match state.link_card_fetcher.fetch(link_url).await {
                Ok(card) => Some(post_external_from_card(card)),
                Err(error) => return oauth_error(StatusCode::BAD_REQUEST, &error),
            }
        } else {
            None
        };
    if external
        .as_ref()
        .is_some_and(|link| link.thumbnail.is_some())
        && !verified_cookie_blob_access(&state.public, &headers, state.storage.as_ref())
    {
        return oauth_error(
            StatusCode::FORBIDDEN,
            "Log in again to enable webpage thumbnail uploads.",
        );
    }
    let has_external = external.is_some();
    let created = if has_images {
        state
            .flow
            .create_post_with_images(&did, text, submission.images)
            .await
    } else if let Some(external) = external {
        state
            .flow
            .create_post_with_external(&did, text, external)
            .await
    } else {
        state.flow.create_post(&did, text).await
    };
    match created {
        Ok(created) => {
            match persist_created_record(state.storage.clone(), &did, "app.bsky.feed.post", created)
                .await
            {
                Ok(()) => Redirect::to("/app").into_response(),
                Err(error) => oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error),
            }
        }
        Err(error) => image_or_text_action_error(error, has_images || has_external),
    }
}

fn image_or_text_action_error(error: String, has_media: bool) -> Response {
    if !has_media {
        return oauth_error(StatusCode::BAD_GATEWAY, &error);
    }
    tracing::warn!(%error, "post media upload failed before record creation");
    (
        StatusCode::BAD_GATEWAY,
        Json(json!({
            "error":"ImagePostFailed",
            "message":"The picture or webpage card upload failed. No post was created. Please try again."
        })),
    )
        .into_response()
}

async fn write_quote<F, S>(
    State(state): State<Arc<OAuthWebState<F, S>>>,
    request: Request,
) -> Response
where
    F: OAuthFlow,
    S: Storage + 'static,
{
    let headers = request.headers().clone();
    if !same_origin_request(&headers, &state.public.public_url) {
        return oauth_error(StatusCode::FORBIDDEN, "request must come from this origin");
    }
    let Some(did) = verified_cookie_did(&state.public, &headers, state.storage.as_ref()) else {
        return Redirect::to("/login").into_response();
    };
    if !verified_cookie_write_access(&state.public, &headers, state.storage.as_ref()) {
        return oauth_error(StatusCode::FORBIDDEN, "write permissions were not granted");
    }
    let submission = match parse_post_submission(request, &state).await {
        Ok(submission) => submission,
        Err((status, message)) => return oauth_error(status, &message),
    };
    if !submission.images.is_empty()
        && !verified_cookie_blob_access(&state.public, &headers, state.storage.as_ref())
    {
        return oauth_error(
            StatusCode::FORBIDDEN,
            "Log in again to enable pictures before posting images.",
        );
    }
    let text = submission.text.trim();
    if text.is_empty() || text.chars().count() > 300 {
        return oauth_error(
            StatusCode::BAD_REQUEST,
            "quote text must contain 1 to 300 characters",
        );
    }
    let Some(quote_uri) = submission
        .quote_uri
        .as_deref()
        .filter(|uri| post_uri_did(uri).is_some())
    else {
        return oauth_error(StatusCode::BAD_REQUEST, "quote_uri is required");
    };
    let quoted = match load_post_record(&state.flow, state.storage.as_ref(), quote_uri).await {
        Ok(Some(record)) => record,
        Ok(None) => return oauth_error(StatusCode::NOT_FOUND, "quoted post not found"),
        Err(error) => return oauth_error(StatusCode::BAD_GATEWAY, &error),
    };
    let Some(cid) = quoted.cid.as_deref().filter(|cid| !cid.is_empty()) else {
        return oauth_error(StatusCode::BAD_REQUEST, "quoted post has no CID");
    };
    let quoted_ref = PostStrongRef {
        uri: quoted.uri,
        cid: cid.to_owned(),
    };
    let has_images = !submission.images.is_empty();
    let external =
        if let Some(link_url) = selected_link_url(has_images, submission.link_url.as_deref()) {
            match state.link_card_fetcher.fetch(link_url).await {
                Ok(card) => Some(post_external_from_card(card)),
                Err(error) => return oauth_error(StatusCode::BAD_REQUEST, &error),
            }
        } else {
            None
        };
    if external
        .as_ref()
        .is_some_and(|link| link.thumbnail.is_some())
        && !verified_cookie_blob_access(&state.public, &headers, state.storage.as_ref())
    {
        return oauth_error(
            StatusCode::FORBIDDEN,
            "Log in again to enable webpage thumbnail uploads.",
        );
    }
    let has_external = external.is_some();
    let created = if has_images {
        state
            .flow
            .create_quote_with_images(&did, text, quoted_ref, submission.images)
            .await
    } else if let Some(external) = external {
        state
            .flow
            .create_quote_with_external(&did, text, quoted_ref, external)
            .await
    } else {
        state.flow.create_quote(&did, text, quoted_ref).await
    };
    let created = match created {
        Ok(created) => created,
        Err(error) => return image_or_text_action_error(error, has_images || has_external),
    };
    match persist_created_record(state.storage.clone(), &did, "app.bsky.feed.post", created).await {
        Ok(()) => Redirect::to("/app").into_response(),
        Err(error) => oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error),
    }
}

async fn write_reply<F, S>(
    State(state): State<Arc<OAuthWebState<F, S>>>,
    request: Request,
) -> Response
where
    F: OAuthFlow,
    S: Storage + 'static,
{
    let headers = request.headers().clone();
    if !same_origin_request(&headers, &state.public.public_url) {
        return oauth_error(StatusCode::FORBIDDEN, "request must come from this origin");
    }
    let Some(did) = verified_cookie_did(&state.public, &headers, state.storage.as_ref()) else {
        return Redirect::to("/login").into_response();
    };
    if !verified_cookie_write_access(&state.public, &headers, state.storage.as_ref()) {
        return oauth_error(StatusCode::FORBIDDEN, "write permissions were not granted");
    }
    let submission = match parse_post_submission(request, &state).await {
        Ok(submission) => submission,
        Err((status, message)) => return oauth_error(status, &message),
    };
    if !submission.images.is_empty()
        && !verified_cookie_blob_access(&state.public, &headers, state.storage.as_ref())
    {
        return oauth_error(
            StatusCode::FORBIDDEN,
            "Log in again to enable pictures before posting images.",
        );
    }
    let text = submission.text.trim();
    if text.is_empty() || text.chars().count() > 300 {
        return oauth_error(
            StatusCode::BAD_REQUEST,
            "reply text must contain 1 to 300 characters",
        );
    }
    let Some(parent_uri) = submission
        .parent_uri
        .as_deref()
        .filter(|uri| !uri.trim().is_empty())
    else {
        return oauth_error(StatusCode::BAD_REQUEST, "parent_uri is required");
    };
    let parent = match load_post_record(&state.flow, state.storage.as_ref(), parent_uri).await {
        Ok(Some(parent)) => parent,
        Ok(None) => return oauth_error(StatusCode::NOT_FOUND, "parent post not found"),
        Err(error) => return oauth_error(StatusCode::BAD_GATEWAY, &error),
    };
    let Some(parent_cid) = parent.cid.as_deref().filter(|cid| !cid.is_empty()) else {
        return oauth_error(StatusCode::BAD_REQUEST, "parent post has no CID");
    };
    let parent_ref = PostStrongRef {
        uri: parent.uri.clone(),
        cid: parent_cid.to_owned(),
    };
    let root_ref = if let Some(root) = parent
        .record
        .get("reply")
        .and_then(|reply| reply.get("root"))
    {
        let (Some(uri), Some(cid)) = (
            root.get("uri").and_then(Value::as_str),
            root.get("cid").and_then(Value::as_str),
        ) else {
            return oauth_error(
                StatusCode::BAD_REQUEST,
                "parent reply has no root reference",
            );
        };
        if uri.is_empty() || cid.is_empty() {
            return oauth_error(
                StatusCode::BAD_REQUEST,
                "parent reply has no root reference",
            );
        }
        PostStrongRef {
            uri: uri.to_owned(),
            cid: cid.to_owned(),
        }
    } else {
        parent_ref.clone()
    };
    let has_images = !submission.images.is_empty();
    let external =
        if let Some(link_url) = selected_link_url(has_images, submission.link_url.as_deref()) {
            match state.link_card_fetcher.fetch(link_url).await {
                Ok(card) => Some(post_external_from_card(card)),
                Err(error) => return oauth_error(StatusCode::BAD_REQUEST, &error),
            }
        } else {
            None
        };
    if external
        .as_ref()
        .is_some_and(|link| link.thumbnail.is_some())
        && !verified_cookie_blob_access(&state.public, &headers, state.storage.as_ref())
    {
        return oauth_error(
            StatusCode::FORBIDDEN,
            "Log in again to enable webpage thumbnail uploads.",
        );
    }
    let has_external = external.is_some();
    let created = if has_images {
        state
            .flow
            .create_reply_with_images(&did, text, root_ref, parent_ref, submission.images)
            .await
    } else if let Some(external) = external {
        state
            .flow
            .create_reply_with_external(&did, text, root_ref, parent_ref, external)
            .await
    } else {
        state
            .flow
            .create_reply(&did, text, root_ref, parent_ref)
            .await
    };
    let created = match created {
        Ok(created) => created,
        Err(error) => return image_or_text_action_error(error, has_images || has_external),
    };
    let redirect = format!("/app/thread?uri={}", form_encoded(&created.uri));
    match persist_created_record(state.storage.clone(), &did, "app.bsky.feed.post", created).await {
        Ok(()) => Redirect::to(&redirect).into_response(),
        Err(error) => oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error),
    }
}

async fn persist_created_record<S: Storage + 'static>(
    storage: Arc<S>,
    did: &str,
    collection: &str,
    created: PdsCreatedRecord,
) -> Result<(), String> {
    let prefix = format!("at://{did}/{collection}/");
    let rkey = created
        .uri
        .strip_prefix(&prefix)
        .filter(|rkey| !rkey.is_empty() && !rkey.contains('/'))
        .ok_or_else(|| {
            "PDS createRecord returned a URI outside the expected repo/collection".to_owned()
        })?
        .to_owned();
    let record = IndexedRecord {
        did: did.to_owned(),
        collection: collection.to_owned(),
        rkey,
        uri: created.uri,
        cid: Some(created.cid),
        time_us: created.time_us,
        record: created.record,
    };
    tokio::task::spawn_blocking(move || storage.put_record(&record))
        .await
        .map_err(|error| format!("index write task failed: {error}"))?
        .map_err(|error| format!("could not index PDS record: {error}"))
}

async fn remove_deleted_record<S: Storage + 'static>(
    storage: Arc<S>,
    did: &str,
    collection: &str,
    uri: &str,
) -> Result<(), String> {
    let prefix = format!("at://{did}/{collection}/");
    let rkey = uri
        .strip_prefix(&prefix)
        .filter(|rkey| !rkey.is_empty() && !rkey.contains('/'))
        .ok_or_else(|| "PDS deleteRecord URI is outside the expected repo/collection".to_owned())?
        .to_owned();
    let did = did.to_owned();
    let collection = collection.to_owned();
    tokio::task::spawn_blocking(move || storage.delete_record(&did, &collection, &rkey))
        .await
        .map_err(|error| format!("index delete task failed: {error}"))?
        .map_err(|error| format!("could not remove deleted PDS record: {error}"))
}

async fn write_like<F, S>(
    State(state): State<Arc<OAuthWebState<F, S>>>,
    headers: HeaderMap,
    Form(form): Form<HashMap<String, String>>,
) -> Response
where
    F: OAuthFlow,
    S: Storage + 'static,
{
    if !same_origin_request(&headers, &state.public.public_url) {
        return oauth_error(StatusCode::FORBIDDEN, "request must come from this origin");
    }
    let Some(did) = verified_cookie_did(&state.public, &headers, state.storage.as_ref()) else {
        return Redirect::to("/login").into_response();
    };
    if !verified_cookie_write_access(&state.public, &headers, state.storage.as_ref()) {
        return oauth_error(StatusCode::FORBIDDEN, "write permissions were not granted");
    }
    let (Some(uri), Some(cid)) = (form.get("uri"), form.get("cid")) else {
        return oauth_error(StatusCode::BAD_REQUEST, "uri and cid are required");
    };
    if !uri.starts_with("at://") || cid.is_empty() {
        return oauth_error(StatusCode::BAD_REQUEST, "uri and cid are invalid");
    }
    if form.get("intent").is_some_and(|intent| intent == "delete") {
        let summary = match state.storage.post_interaction_summary(uri, Some(&did)) {
            Ok(summary) => summary,
            Err(error) => {
                return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string());
            }
        };
        let Some(like_uri) = summary.viewer_like else {
            return oauth_error(
                StatusCode::BAD_REQUEST,
                "like record was not found in the local index",
            );
        };
        return match state.flow.delete_like(&did, &like_uri).await {
            Ok(()) => match remove_deleted_record(
                state.storage.clone(),
                &did,
                "app.bsky.feed.like",
                &like_uri,
            )
            .await
            {
                Ok(()) => like_response(
                    &headers,
                    &state.public.public_url,
                    state.storage.as_ref(),
                    &did,
                    uri,
                ),
                Err(error) => oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error),
            },
            Err(error) => oauth_error(StatusCode::BAD_GATEWAY, &error),
        };
    }
    match state.flow.like(&did, uri, cid).await {
        Ok(created) => {
            match persist_created_record(state.storage.clone(), &did, "app.bsky.feed.like", created)
                .await
            {
                Ok(()) => like_response(
                    &headers,
                    &state.public.public_url,
                    state.storage.as_ref(),
                    &did,
                    uri,
                ),
                Err(error) => oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error),
            }
        }
        Err(error) => oauth_error(StatusCode::BAD_GATEWAY, &error),
    }
}

async fn write_repost<F, S>(
    State(state): State<Arc<OAuthWebState<F, S>>>,
    headers: HeaderMap,
    Form(form): Form<HashMap<String, String>>,
) -> Response
where
    F: OAuthFlow,
    S: Storage + 'static,
{
    if !same_origin_request(&headers, &state.public.public_url) {
        return oauth_error(StatusCode::FORBIDDEN, "request must come from this origin");
    }
    let Some(did) = verified_cookie_did(&state.public, &headers, state.storage.as_ref()) else {
        return Redirect::to("/login").into_response();
    };
    if !verified_cookie_write_access(&state.public, &headers, state.storage.as_ref()) {
        return oauth_error(StatusCode::FORBIDDEN, "write permissions were not granted");
    }
    let (Some(uri), Some(cid)) = (form.get("uri"), form.get("cid")) else {
        return oauth_error(StatusCode::BAD_REQUEST, "uri and cid are required");
    };
    if !uri.starts_with("at://") || cid.is_empty() {
        return oauth_error(StatusCode::BAD_REQUEST, "uri and cid are invalid");
    }
    match state.flow.repost(&did, uri, cid).await {
        Ok(created) => match persist_created_record(
            state.storage.clone(),
            &did,
            "app.bsky.feed.repost",
            created,
        )
        .await
        {
            Ok(()) => return_to_post(&headers, &state.public.public_url, uri),
            Err(error) => oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error),
        },
        Err(error) => oauth_error(StatusCode::BAD_GATEWAY, &error),
    }
}

fn verified_cookie_did<S: Storage>(
    public: &OAuthPublic,
    headers: &HeaderMap,
    storage: &S,
) -> Option<String> {
    verified_cookie_session(public, headers, storage).map(|(did, _, _, _, _, _, _)| did)
}

fn verified_cookie_write_access<S: Storage>(
    public: &OAuthPublic,
    headers: &HeaderMap,
    storage: &S,
) -> bool {
    verified_cookie_session(public, headers, storage)
        .is_some_and(|(_, _, _, write_access, _, _, _)| write_access)
}

fn verified_cookie_blob_access<S: Storage>(
    public: &OAuthPublic,
    headers: &HeaderMap,
    storage: &S,
) -> bool {
    verified_cookie_session(public, headers, storage)
        .is_some_and(|(_, _, _, _, _, blob_access, _)| blob_access)
}

fn verified_cookie_video_access<S: Storage>(
    public: &OAuthPublic,
    headers: &HeaderMap,
    storage: &S,
) -> bool {
    verified_cookie_session(public, headers, storage)
        .is_some_and(|(_, _, _, _, _, _, video_access)| video_access)
}

fn verified_cookie_feed_access<S: Storage>(
    public: &OAuthPublic,
    headers: &HeaderMap,
    storage: &S,
) -> bool {
    verified_cookie_session(public, headers, storage)
        .is_some_and(|(_, _, _, _, feed_access, _, _)| feed_access)
}

fn verified_cookie_session<S: Storage>(
    public: &OAuthPublic,
    headers: &HeaderMap,
    storage: &S,
) -> Option<(String, String, u64, bool, bool, bool, bool)> {
    let (did, session_id, generation, write_access, feed_access, blob_access, video_access) =
        cookie_value(headers).and_then(|value| public.verify_session_cookie(value))?;
    (storage
        .web_session_generation(&did, &session_id)
        .ok()
        .flatten()
        == Some(generation))
    .then_some((
        did,
        session_id,
        generation,
        write_access,
        feed_access,
        blob_access,
        video_access,
    ))
}

/// The like script asks with `x-appview-fetch: 1` and gets the new state as JSON instead of a page.
fn like_response<S: Storage>(
    headers: &HeaderMap,
    public_url: &str,
    storage: &S,
    did: &str,
    uri: &str,
) -> Response {
    if headers
        .get("x-appview-fetch")
        .is_some_and(|value| value.as_bytes() == b"1")
    {
        let summary = storage
            .post_interaction_summary(uri, Some(did))
            .unwrap_or_default();
        return Json(json!({
            "liked": summary.viewer_like.is_some(),
            "likeCount": summary.like_count,
        }))
        .into_response();
    }
    return_to_post(headers, public_url, uri)
}

/// Fragment id of a post's card, so a like or repost can return to the same place on the page.
fn post_anchor(uri: &str) -> String {
    let id = uri
        .trim_start_matches("at://")
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '-' })
        .collect::<String>();
    format!("post-{id}")
}

/// After a like or repost, go back to the page the form was on (same origin, under /app), scrolled to the
/// post, instead of the top of the home timeline.
fn return_to_post(headers: &HeaderMap, public_url: &str, uri: &str) -> Response {
    let anchor = post_anchor(uri);
    let back = headers
        .get(header::REFERER)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| reqwest::Url::parse(value).ok())
        .zip(reqwest::Url::parse(public_url).ok())
        .filter(|(referer, public)| {
            referer.scheme() == public.scheme()
                && referer.host_str() == public.host_str()
                && referer.port_or_known_default() == public.port_or_known_default()
                && referer.path().starts_with("/app")
        })
        .map(|(referer, _)| match referer.query() {
            Some(query) => format!("{}?{query}", referer.path()),
            None => referer.path().to_owned(),
        })
        .unwrap_or_else(|| "/app".to_owned());
    Redirect::to(&format!("{back}#{anchor}")).into_response()
}

fn same_origin_request(headers: &HeaderMap, public_url: &str) -> bool {
    if headers
        .get("sec-fetch-site")
        .is_some_and(|site| site.to_str().ok() != Some("same-origin"))
    {
        return false;
    }
    let Some(origin) = headers
        .get(header::ORIGIN)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| reqwest::Url::parse(value).ok())
    else {
        return false;
    };
    let Ok(expected) = reqwest::Url::parse(public_url) else {
        return false;
    };
    origin.username().is_empty()
        && origin.password().is_none()
        && origin.path() == "/"
        && origin.query().is_none()
        && origin.fragment().is_none()
        && origin.origin() == expected.origin()
}

fn cookie_value(headers: &HeaderMap) -> Option<&str> {
    cookie_value_named(headers, SESSION_COOKIE)
}

fn cookie_value_named<'a>(headers: &'a HeaderMap, expected_name: &str) -> Option<&'a str> {
    headers
        .get_all(header::COOKIE)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(';'))
        .filter_map(|part| part.trim().split_once('='))
        .find_map(|(name, value)| (name == expected_name).then_some(value))
}

fn clear_login_cookie(public: &OAuthPublic) -> String {
    format!(
        "{LOGIN_COOKIE}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0{}",
        if public.secure_cookies() {
            "; Secure"
        } else {
            ""
        },
    )
}

fn clear_session_cookie(mut response: Response, secure: bool) -> Response {
    response.headers_mut().insert(
        header::SET_COOKIE,
        HeaderValue::from_str(&format!(
            "{SESSION_COOKIE}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0{}",
            if secure { "; Secure" } else { "" }
        ))
        .expect("session cookie clearing header is valid"),
    );
    response
}

fn oauth_error(status: StatusCode, message: &str) -> Response {
    let message = match status {
        StatusCode::INTERNAL_SERVER_ERROR => "internal server error",
        StatusCode::BAD_GATEWAY => "the request could not be completed",
        _ => message,
    };
    (
        status,
        Json(json!({"error":"OAuthError","message":message})),
    )
        .into_response()
}

fn oauth_error_page(status: StatusCode, message: &str) -> Response {
    (
        status,
        Html(page_shell(
            "Sign-in error",
            &format!(
                "<section class=\"error-card\"><p class=\"eyebrow\">Something went wrong</p><h1>Sign-in error</h1><p>{}</p><a class=\"primary-link\" href=\"/login\">Try again</a></section>",
                html_escape(message)
            ),
            false,
        )),
    )
        .into_response()
}

fn feed_reauthorization_page() -> Response {
    Html(page_shell(
        "Allow feeds",
        "<section class=\"error-card\"><p class=\"eyebrow\">Permission update</p><h1>Sign in again to allow feeds</h1><p>Your existing sign-in does not include permission to load saved feeds. Sign in again and approve the updated access.</p><a class=\"primary-link\" href=\"/login\">Sign in again</a></section>",
        false,
    ))
    .into_response()
}

fn valid_redirect_url(value: &str) -> bool {
    let Ok(url) = reqwest::Url::parse(value) else {
        return false;
    };
    let local = matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "::1"));
    (url.scheme() == "https" || (local && url.scheme() == "http"))
        && url.username().is_empty()
        && url.password().is_none()
}

fn html_escape(value: &str) -> String {
    let mut escaped = String::with_capacity(value.len().min(MAX_RENDERED_REMOTE_CHARS * 2));
    for (index, character) in value.chars().enumerate() {
        if index == MAX_RENDERED_REMOTE_CHARS {
            escaped.push('…');
            break;
        }
        match character {
            '&' => escaped.push_str("&amp;"),
            '<' => escaped.push_str("&lt;"),
            '>' => escaped.push_str("&gt;"),
            '"' => escaped.push_str("&quot;"),
            '\'' => escaped.push_str("&#39;"),
            character => escaped.push(character),
        }
    }
    escaped
}

fn render_feed_card(feed: &ResolvedFeed) -> String {
    let href = feed_link_href(feed);
    let avatar = feed_avatar_markup(feed, 56, 56);
    format!(
        "<li class=\"feed-item\"><a class=\"feed-card\" href=\"{href}\">{avatar}<h2>{}</h2><p>{}</p></a></li>",
        html_escape(&feed.display_name),
        html_escape(feed.description.as_deref().unwrap_or("")),
    )
}

fn render_feed_heading(feed: &ResolvedFeed) -> String {
    let href = feed_link_href(feed);
    let avatar = feed_avatar_markup(feed, 72, 72);
    format!(
        "<header class=\"page-heading feed-heading\">{avatar}<p class=\"eyebrow\"><a href=\"/feeds\">Feeds</a></p><h1>{}</h1><p class=\"page-subtitle\">{}</p><a class=\"feed-address-link\" href=\"{href}\">Feed address</a></header>",
        html_escape(&feed.display_name),
        html_escape(feed.description.as_deref().unwrap_or("")),
    )
}

fn feed_link_href(feed: &ResolvedFeed) -> String {
    format!("/app/feed?uri={}", html_escape(&form_encoded(&feed.uri)))
}

fn form_encoded(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}

fn feed_avatar_markup(feed: &ResolvedFeed, width: u16, height: u16) -> String {
    let Some(avatar) = feed.avatar.as_deref() else {
        return String::new();
    };
    let Some(avatar) = avatar_thumbnail_url(avatar) else {
        return String::new();
    };
    format!(
        "<img class=\"feed-avatar\" src=\"{}\" alt=\"\" width=\"{width}\" height=\"{height}\" loading=\"lazy\" decoding=\"async\">",
        html_escape(&avatar)
    )
}

#[cfg(test)]
mod feed_html_tests {
    use super::{render_feed_card, render_feed_heading};
    use crate::feeds::ResolvedFeed;

    #[test]
    fn generator_text_and_uri_are_escaped_on_both_feed_pages() {
        let feed = ResolvedFeed {
            uri: "at://did:plc:feed/app.bsky.feed.generator/x\"><script>alert(1)</script>".into(),
            display_name: "<img src=x onerror=alert(1)>".into(),
            description: Some("<script>feed text</script> & more".into()),
            avatar: Some("javascript:alert(1)".into()),
            service_did: "did:plc:generator".into(),
            endpoint: "https://feed.example".into(),
        };
        for html in [render_feed_card(&feed), render_feed_heading(&feed)] {
            assert!(!html.contains("<script>"));
            assert!(!html.contains("<img src=x"));
            assert!(html.contains("&lt;script&gt;"));
            assert!(html.contains("&lt;img src=x onerror=alert(1)&gt;"));
        }
    }
}

fn profile_handle(profile: &Value, did: &str) -> String {
    profile
        .get("handle")
        .and_then(Value::as_str)
        .filter(|handle| !handle.is_empty() && *handle != "handle.invalid")
        .unwrap_or(did)
        .to_owned()
}

fn safe_avatar_url(profile: &Value) -> Option<&str> {
    let avatar = profile.get("avatar").and_then(Value::as_str)?;
    let url = url::Url::parse(avatar).ok()?;
    (url.scheme() == "https"
        && url.host_str() == Some("cdn.bsky.app")
        && url.username().is_empty()
        && url.password().is_none())
    .then_some(avatar)
}

fn avatar_thumbnail_url(avatar: &str) -> Option<String> {
    let mut url = url::Url::parse(avatar).ok()?;
    if url.scheme() != "https"
        || url.host_str() != Some("cdn.bsky.app")
        || !url.username().is_empty()
        || url.password().is_some()
    {
        return None;
    }
    let mut segments = url.path_segments()?.map(str::to_owned).collect::<Vec<_>>();
    if segments.len() == 5 && segments[..3] == ["img", "avatar", "plain"] {
        segments[1] = "avatar_thumbnail".to_owned();
        if !segments[4].ends_with("@jpeg") {
            segments[4].push_str("@jpeg");
        }
        url.path_segments_mut().ok()?.clear().extend(segments);
        return Some(url.into());
    }
    if segments.len() == 5
        && segments[..3] == ["img", "avatar_thumbnail", "plain"]
        && !segments[4].ends_with("@jpeg")
    {
        segments[4].push_str("@jpeg");
        url.path_segments_mut().ok()?.clear().extend(segments);
    }
    Some(url.into())
}

fn avatar_dimensions(class_name: &str) -> (u16, u16) {
    match class_name {
        "small-avatar" => (32, 32),
        "profile-avatar" => (80, 80),
        "focus-avatar" => (56, 56),
        "mobile-profile-avatar" => (25, 25),
        _ => (44, 44),
    }
}

fn avatar_markup(profile: &Value, class_name: &str) -> String {
    avatar_markup_with_priority(profile, class_name, false)
}

fn avatar_markup_with_priority(profile: &Value, class_name: &str, eager: bool) -> String {
    if let Some(avatar) = safe_avatar_url(profile).and_then(avatar_thumbnail_url) {
        let (width, height) = avatar_dimensions(class_name);
        let loading = if eager { "eager" } else { "lazy" };
        format!(
            "<img class=\"{class_name}\" src=\"{}\" alt=\"\" width=\"{width}\" height=\"{height}\" loading=\"{loading}\" decoding=\"async\">",
            html_escape(&avatar)
        )
    } else {
        format!("<span class=\"{class_name} avatar-placeholder\" aria-hidden=\"true\"></span>")
    }
}

fn relative_time(created_at: &str) -> String {
    let Ok(created_at) = OffsetDateTime::parse(created_at, &Rfc3339) else {
        return "time unavailable".to_owned();
    };
    let seconds = OffsetDateTime::now_utc().unix_timestamp() - created_at.unix_timestamp();
    if seconds.unsigned_abs() < 60 {
        return if seconds < 0 {
            "soon".to_owned()
        } else {
            "now".to_owned()
        };
    }
    let (amount, unit) = match seconds.unsigned_abs() {
        value if value < 60 * 60 => (value / 60, "minute"),
        value if value < 24 * 60 * 60 => (value / (60 * 60), "hour"),
        value if value < 7 * 24 * 60 * 60 => (value / (24 * 60 * 60), "day"),
        value if value < 30 * 24 * 60 * 60 => (value / (7 * 24 * 60 * 60), "week"),
        value if value < 365 * 24 * 60 * 60 => (value / (30 * 24 * 60 * 60), "month"),
        value => (value / (365 * 24 * 60 * 60), "year"),
    };
    let unit = if amount == 1 {
        unit.to_owned()
    } else {
        format!("{unit}s")
    };
    if seconds < 0 {
        format!("in {amount} {unit}")
    } else {
        format!("{amount} {unit} ago")
    }
}

fn render_post_text(text: &str) -> String {
    let Some((separator_start, separator)) = text
        .char_indices()
        .rev()
        .find(|(_, character)| character.is_whitespace())
    else {
        return if is_thread_number(text) {
            format!(
                "<span class=\"thread-number-pill\">{}</span>",
                html_escape(text)
            )
        } else {
            html_escape(text)
        };
    };
    let number_start = separator_start + separator.len_utf8();
    let number = &text[number_start..];
    if !is_thread_number(number) {
        return html_escape(text);
    }
    format!(
        "{}<span class=\"thread-number-pill\">{}</span>",
        html_escape(&text[..number_start]),
        html_escape(number)
    )
}

fn is_thread_number(value: &str) -> bool {
    value.split_once('/').is_some_and(|(part, total)| {
        !part.is_empty()
            && !total.is_empty()
            && part.bytes().all(|byte| byte.is_ascii_digit())
            && total.bytes().all(|byte| byte.is_ascii_digit())
    })
}

fn action_count_markup(count: usize) -> String {
    if count == 0 {
        String::new()
    } else {
        format!("<span class=\"action-count\">{count}</span>")
    }
}

fn render_web_post<S: Storage>(
    storage: &S,
    record: &IndexedRecord,
    viewer_did: &str,
    write_forms_enabled: bool,
    link_text_to_thread: bool,
    eager_avatar: bool,
    quote_index: &QuoteRenderIndex,
) -> String {
    let profile = profile_view_from_storage(storage, &record.did)
        .unwrap_or_else(|_| json!({"did":record.did,"handle":record.did}));
    let handle = profile_handle(&profile, &record.did);
    let display_name = profile
        .get("displayName")
        .and_then(Value::as_str)
        .unwrap_or(&handle);
    let profile_uri = format!(
        "/profile?did={}",
        url::form_urlencoded::byte_serialize(record.did.as_bytes()).collect::<String>()
    );
    let author = format!(
        "{}<div class=\"post-author\"><a class=\"author-name\" href=\"{}\">{}</a><a class=\"author-handle\" href=\"{}\">@{}</a></div>",
        avatar_markup_with_priority(&profile, "avatar", eager_avatar),
        html_escape(&profile_uri),
        html_escape(display_name),
        html_escape(&profile_uri),
        html_escape(&handle),
    );
    let time = record
        .record
        .get("createdAt")
        .and_then(Value::as_str)
        .map(|created_at| {
            format!(
                "<span class=\"sr-only\" aria-hidden=\"true\"><time datetime=\"{}\">{}</time></span><span class=\"relative-time\">{}</span>",
                html_escape(created_at),
                html_escape(created_at),
                relative_time(created_at)
            )
        })
        .unwrap_or_default();
    let text = record
        .record
        .get("text")
        .and_then(Value::as_str)
        .unwrap_or("");
    let text = render_post_text(text);
    let text = if link_text_to_thread {
        let thread_uri = format!(
            "/app/thread?uri={}",
            url::form_urlencoded::byte_serialize(record.uri.as_bytes()).collect::<String>()
        );
        format!(
            "<a class=\"post-text-link\" href=\"{}\">{}</a>",
            html_escape(&thread_uri),
            text
        )
    } else {
        text
    };
    let interactions = storage
        .post_interaction_summary(&record.uri, Some(viewer_did))
        .unwrap_or_default();
    let thread_uri = format!(
        "/app/thread?uri={}",
        url::form_urlencoded::byte_serialize(record.uri.as_bytes()).collect::<String>()
    );
    let reply_action = format!(
        "<a class=\"post-action\" href=\"{}\" aria-label=\"Open replies\">{}<span class=\"sr-only\">Reply</span>{}</a>",
        html_escape(&thread_uri),
        icon_svg("reply"),
        action_count_markup(interactions.reply_count)
    );
    let uri = html_escape(&record.uri);
    let cid = html_escape(record.cid.as_deref().unwrap_or(""));
    let (repost_action, like_action) = if write_forms_enabled {
        let (like_intent, like_label) = if interactions.viewer_like.is_some() {
            (
                "<input type=\"hidden\" name=\"intent\" value=\"delete\">",
                "Liked",
            )
        } else {
            ("", "Like")
        };
        let repost_label = if interactions.viewer_repost.is_some() {
            "Reposted"
        } else {
            "Repost"
        };
        let like_aria = if interactions.viewer_like.is_some() {
            format!("Unlike post by @{}", html_escape(&handle))
        } else {
            format!("Like post by @{}", html_escape(&handle))
        };
        let quote_href = format!("/compose?quote_uri={}", form_encoded(&record.uri));
        let repost = format!(
            "<details class=\"repost-menu\"><summary class=\"post-action\" aria-label=\"Repost menu\">{}<span class=\"sr-only\">{repost_label}</span>{}</summary><div class=\"repost-menu-options\"><form class=\"post-action-form\" action=\"/repost\" method=\"post\"><input type=\"hidden\" name=\"uri\" value=\"{uri}\"><input type=\"hidden\" name=\"cid\" value=\"{cid}\"><button class=\"post-action\" type=\"submit\" aria-label=\"Repost by @{}\">Repost</button></form><a href=\"{}\">Quote post</a></div></details>",
            icon_svg("repost"),
            action_count_markup(interactions.repost_count),
            html_escape(&handle),
            html_escape(&quote_href)
        );
        let liked = interactions.viewer_like.is_some();
        let like = format!(
            "<form class=\"post-action-form\" action=\"/like\" method=\"post\"><input type=\"hidden\" name=\"uri\" value=\"{uri}\"><input type=\"hidden\" name=\"cid\" value=\"{cid}\">{like_intent}<button class=\"post-action{}\" type=\"submit\" aria-label=\"{like_aria}\" aria-pressed=\"{liked}\">{}<span class=\"sr-only\">{like_label}</span>{}</button></form>",
            if liked { " is-liked" } else { "" },
            icon_svg("like"),
            action_count_markup(interactions.like_count)
        );
        (repost, like)
    } else {
        (
            format!(
                "<span class=\"post-action action-readonly\" aria-label=\"Repost count\">{}{}</span>",
                icon_svg("repost"),
                action_count_markup(interactions.repost_count)
            ),
            format!(
                "<span class=\"post-action action-readonly{}\" aria-label=\"Like count\">{}{}</span>",
                if interactions.viewer_like.is_some() {
                    " is-liked"
                } else {
                    ""
                },
                icon_svg("like"),
                action_count_markup(interactions.like_count)
            ),
        )
    };
    let share_action = format!(
        "<a class=\"post-action\" href=\"{}\" aria-label=\"Share or copy link\">{}<span class=\"sr-only\">Share</span></a>",
        html_escape(&thread_uri),
        icon_svg("share")
    );
    let more_action = format!(
        "<details class=\"post-more-menu\"><summary class=\"post-action\" aria-label=\"More post actions\">{}</summary><div class=\"repost-menu-options\"><a href=\"{}\">Copy link</a><a href=\"{}\">View profile</a></div></details>",
        icon_svg("more"),
        html_escape(&thread_uri),
        html_escape(&profile_uri)
    );
    let actions = format!("{reply_action}{repost_action}{like_action}{share_action}{more_action}");
    let embed = record
        .record
        .get("embed")
        .map(|embed| render_web_embed_with_quotes(storage, &record.did, embed, quote_index, 0))
        .unwrap_or_default();
    let anchor = post_anchor(&record.uri);
    format!(
        "<article class=\"post-card\" id=\"{anchor}\" data-post-kind=\"post\" data-post-uri=\"{uri}\"><header class=\"post-header\">{author}{time}</header><p class=\"post-text\">{text}</p>{embed}<div class=\"post-actions\" aria-label=\"Post actions\">{actions}</div></article>"
    )
}

fn short_did(did: &str) -> String {
    let tail = did.rsplit(':').next().unwrap_or(did);
    if tail.len() <= 12 {
        did.to_owned()
    } else {
        format!("did:…{}", &tail[tail.len() - 8..])
    }
}

fn parent_display_identity<S: Storage>(storage: &S, did: &str) -> (Value, String, String) {
    let profile =
        profile_view_from_storage(storage, did).unwrap_or_else(|_| json!({"did":did,"handle":did}));
    let cached_handle = profile_handle(&profile, did);
    let handle = if cached_handle == did {
        short_did(did)
    } else {
        cached_handle
    };
    let display_name = profile
        .get("displayName")
        .and_then(Value::as_str)
        .filter(|name| !name.is_empty())
        .unwrap_or(&handle)
        .to_owned();
    (profile, handle, display_name)
}

fn render_reply_parent_card<S: Storage>(storage: &S, parent: &IndexedRecord) -> String {
    let (profile, handle, display_name) = parent_display_identity(storage, &parent.did);
    let thread_uri = format!("/app/thread?uri={}", form_encoded(&parent.uri));
    let text = parent
        .record
        .get("text")
        .and_then(Value::as_str)
        .unwrap_or("");
    let uri = html_escape(&parent.uri);
    format!(
        "<a class=\"reply-parent-card\" href=\"{}\" aria-label=\"Open parent post by @{}\" data-post-kind=\"parent\" data-post-uri=\"{uri}\"><header class=\"reply-parent-header\">{}<span class=\"reply-parent-author\"><strong>{}</strong><span>@{}</span></span></header><p class=\"reply-parent-text\">{}</p></a>",
        html_escape(&thread_uri),
        html_escape(&handle),
        avatar_markup(&profile, "reply-parent-avatar"),
        html_escape(&display_name),
        html_escape(&handle),
        render_post_text(text),
    )
}

fn render_reply_parent_fallback<S: Storage>(storage: &S, parent_uri: &str) -> String {
    let thread_uri = format!("/app/thread?uri={}", form_encoded(parent_uri));
    let Some(did) = post_uri_did(parent_uri) else {
        return "<p class=\"reply-parent-fallback\">Reply to a deleted post</p>".to_owned();
    };
    let (_, handle, _) = parent_display_identity(storage, did);
    format!(
        "<p class=\"reply-parent-fallback\">Reply to <a href=\"{}\">@{}</a></p>",
        html_escape(&thread_uri),
        html_escape(&handle)
    )
}

struct TimelinePostItemOptions<'a> {
    prelude: &'a str,
    previous_uri: Option<&'a str>,
    parents: &'a PageParentIndex,
    viewer_did: &'a str,
    write_forms_enabled: bool,
    eager_avatar: bool,
    quote_index: &'a QuoteRenderIndex,
}

#[derive(Default)]
struct TimelinePostGrouping {
    groups_by_newest: HashMap<usize, Vec<usize>>,
    grouped_indices: HashSet<usize>,
}

fn timeline_post_grouping(
    records: &[&IndexedRecord],
    parents: &PageParentIndex,
) -> TimelinePostGrouping {
    const MAX_THREAD_SPAN_US: u64 = 24 * 60 * 60 * 1_000_000;

    let indices = records
        .iter()
        .enumerate()
        .map(|(index, record)| (record.uri.as_str(), index))
        .collect::<HashMap<_, _>>();
    let mut children = HashMap::<usize, Vec<usize>>::new();
    let mut has_page_parent = HashSet::new();
    for (index, record) in records.iter().enumerate() {
        if let Some(parent_index) = record
            .record
            .pointer("/reply/parent/uri")
            .and_then(Value::as_str)
            .and_then(|uri| indices.get(uri).copied())
        {
            children.entry(parent_index).or_default().push(index);
            has_page_parent.insert(index);
        }
    }

    let mut grouping = TimelinePostGrouping::default();
    for root in 0..records.len() {
        if has_page_parent.contains(&root) || grouping.grouped_indices.contains(&root) {
            continue;
        }
        let mut chain = vec![root];
        let mut current = root;
        loop {
            let Some(next) = children.get(&current).filter(|next| next.len() == 1) else {
                break;
            };
            current = next[0];
            if chain.contains(&current) {
                break;
            }
            chain.push(current);
        }
        if chain.len() < 2 {
            continue;
        }
        let oldest = chain
            .iter()
            .map(|index| records[*index].time_us)
            .min()
            .unwrap_or_default();
        let mut oldest = oldest;
        let mut parent_uri = records[root]
            .record
            .pointer("/reply/parent/uri")
            .and_then(Value::as_str)
            .filter(|uri| post_uri_did(uri).is_some());
        let mut visited_parents = HashSet::new();
        let mut complete_ancestry = true;
        while let Some(uri) = parent_uri {
            if !visited_parents.insert(uri) {
                break;
            }
            match parents.get(uri) {
                Some(Some(parent)) => {
                    oldest = oldest.min(parent.time_us);
                    parent_uri = parent
                        .record
                        .pointer("/reply/parent/uri")
                        .and_then(Value::as_str)
                        .filter(|uri| post_uri_did(uri).is_some());
                }
                Some(None) => break,
                None => {
                    complete_ancestry = false;
                    break;
                }
            }
        }
        if !complete_ancestry {
            continue;
        }
        let newest = chain
            .iter()
            .map(|index| records[*index].time_us)
            .max()
            .unwrap_or_default();
        if newest.saturating_sub(oldest) > MAX_THREAD_SPAN_US {
            continue;
        }
        let newest_index = chain
            .iter()
            .copied()
            .max_by_key(|index| (records[*index].time_us, std::cmp::Reverse(*index)))
            .unwrap_or(root);
        let mut oldest_first = chain;
        oldest_first.sort_by_key(|index| (records[*index].time_us, records[*index].uri.as_str()));
        grouping
            .grouped_indices
            .extend(oldest_first.iter().copied());
        grouping.groups_by_newest.insert(newest_index, oldest_first);
    }
    grouping
}

fn render_timeline_post_items<S: Storage>(
    storage: &S,
    records: &[&IndexedRecord],
    preludes: &[String],
    parents: &PageParentIndex,
    viewer_did: &str,
    write_forms_enabled: bool,
    quote_index: &QuoteRenderIndex,
) -> String {
    let grouping = timeline_post_grouping(records, parents);
    let mut html = String::new();
    for (index, record) in records.iter().enumerate() {
        if let Some(group) = grouping.groups_by_newest.get(&index) {
            html.push_str("<li class=\"post-item thread-group\" aria-label=\"Reply thread\">");
            let self_thread = group.first().is_some_and(|first| {
                group
                    .iter()
                    .all(|member| records[*member].did == records[*first].did)
            });
            let visible = if self_thread && group.len() > 10 {
                group
                    .iter()
                    .take(2)
                    .chain(group.iter().rev().take(3).rev())
                    .copied()
                    .collect::<Vec<_>>()
            } else if !self_thread && group.len() > 3 {
                group
                    .iter()
                    .take(1)
                    .chain(group.iter().rev().take(2).rev())
                    .copied()
                    .collect::<Vec<_>>()
            } else {
                group.clone()
            };
            for (position, member_index) in visible.iter().enumerate() {
                if position > 0 {
                    html.push_str(
                        "<span class=\"reply-connector thread-group-connector\" aria-hidden=\"true\"></span>",
                    );
                }
                html.push_str(
                    preludes
                        .get(*member_index)
                        .map(String::as_str)
                        .unwrap_or(""),
                );
                html.push_str(&render_web_post(
                    storage,
                    records[*member_index],
                    viewer_did,
                    write_forms_enabled,
                    true,
                    *member_index < 6,
                    quote_index,
                ));
            }
            let skipped = group.len().saturating_sub(visible.len());
            if skipped > 0 {
                let root_uri = &records[group[0]].uri;
                let thread_uri = format!("/app/thread?uri={}", form_encoded(root_uri));
                let label = if self_thread {
                    format!("{skipped} more in this thread")
                } else {
                    format!("{skipped} more replies")
                };
                html.push_str(&format!(
                    "<a class=\"thread-more\" href=\"{}\">{label}</a>",
                    html_escape(&thread_uri)
                ));
            }
            html.push_str("</li>");
        } else if grouping.grouped_indices.contains(&index) {
            continue;
        } else {
            html.push_str(&render_timeline_post_item(
                storage,
                record,
                TimelinePostItemOptions {
                    prelude: preludes.get(index).map(String::as_str).unwrap_or(""),
                    previous_uri: index
                        .checked_sub(1)
                        .map(|previous| records[previous].uri.as_str()),
                    parents,
                    viewer_did,
                    write_forms_enabled,
                    eager_avatar: index < 6,
                    quote_index,
                },
            ));
        }
    }
    html
}

fn render_timeline_post_item<S: Storage>(
    storage: &S,
    record: &IndexedRecord,
    options: TimelinePostItemOptions<'_>,
) -> String {
    let article = render_web_post(
        storage,
        record,
        options.viewer_did,
        options.write_forms_enabled,
        true,
        options.eager_avatar,
        options.quote_index,
    );
    let Some(parent_uri) = record
        .record
        .pointer("/reply/parent/uri")
        .and_then(Value::as_str)
    else {
        return format!("<li class=\"post-item\">{}{article}</li>", options.prelude);
    };
    if options.previous_uri == Some(parent_uri) {
        return format!(
            "<li class=\"post-item reply-connected-to-previous\">{}<span class=\"reply-connector reply-connector-previous\" aria-hidden=\"true\"></span>{article}</li>",
            options.prelude
        );
    }
    let parent = options.parents.get(parent_uri).and_then(Option::as_ref);
    let parent_markup = parent
        .map(|parent| render_reply_parent_card(storage, parent))
        .unwrap_or_else(|| render_reply_parent_fallback(storage, parent_uri));
    format!(
        "<li class=\"post-item reply-with-parent\">{}<div class=\"reply-parent-slot\">{parent_markup}<span class=\"reply-connector\" aria-hidden=\"true\"></span></div>{article}</li>",
        options.prelude
    )
}

fn render_notification_parent<S: Storage>(
    storage: &S,
    record: &IndexedRecord,
    parents: &PageParentIndex,
) -> String {
    let Some(parent_uri) = record
        .record
        .pointer("/reply/parent/uri")
        .and_then(Value::as_str)
    else {
        return String::new();
    };
    let parent = parents.get(parent_uri).and_then(Option::as_ref);
    let markup = parent
        .map(|parent| render_reply_parent_card(storage, parent))
        .unwrap_or_else(|| render_reply_parent_fallback(storage, parent_uri));
    format!(
        "<div class=\"notification-reply-parent\">{markup}<span class=\"reply-connector\" aria-hidden=\"true\"></span></div>"
    )
}

fn post_uri_did(uri: &str) -> Option<&str> {
    let mut parts = uri.strip_prefix("at://")?.split('/');
    let did = parts.next()?;
    let collection = parts.next()?;
    let rkey = parts.next()?;
    if !(did.starts_with("did:plc:") || did.starts_with("did:web:")) {
        return None;
    }
    (collection == "app.bsky.feed.post" && !rkey.is_empty() && parts.next().is_none())
        .then_some(did)
}

fn render_web_embed<S: Storage>(storage: &S, author_did: &str, embed: &Value) -> String {
    render_web_embed_with_quotes(storage, author_did, embed, &QuoteRenderIndex::new(), 0)
}

fn render_web_embed_with_quotes<S: Storage>(
    storage: &S,
    author_did: &str,
    embed: &Value,
    quote_index: &QuoteRenderIndex,
    quote_depth: usize,
) -> String {
    if quote_depth > 0
        && embed.get("$type").and_then(Value::as_str) == Some("app.bsky.embed.recordWithMedia")
    {
        return render_web_quote(storage, embed, quote_index, quote_depth);
    }
    match embed.get("$type").and_then(Value::as_str) {
        Some("app.bsky.embed.images") => render_web_images(author_did, embed),
        Some("app.bsky.embed.video") | Some("app.bsky.embed.video#view") => {
            render_web_video(author_did, embed)
        }
        Some("app.bsky.embed.external") => render_web_external(author_did, embed),
        Some("app.bsky.embed.record") | Some("app.bsky.embed.record#view") => {
            render_web_quote(storage, embed, quote_index, quote_depth)
        }
        Some("app.bsky.embed.recordWithMedia") => {
            let media = embed
                .get("media")
                .map(|media| {
                    render_web_embed_with_quotes(
                        storage,
                        author_did,
                        media,
                        quote_index,
                        quote_depth,
                    )
                })
                .unwrap_or_default();
            let quote = embed
                .get("record")
                .map(|record| render_web_quote(storage, record, quote_index, quote_depth))
                .unwrap_or_default();
            format!("<section class=\"record-with-media\">{media}{quote}</section>")
        }
        _ => String::new(),
    }
}

fn render_web_video(author_did: &str, embed: &Value) -> String {
    let cid = embed.get("cid").and_then(Value::as_str).or_else(|| {
        embed
            .pointer("/video/ref/$link")
            .or_else(|| embed.pointer("/video/ref"))
            .and_then(Value::as_str)
    });
    let canonical_url = |asset: &str| {
        let did = url::form_urlencoded::byte_serialize(author_did.as_bytes()).collect::<String>();
        let cid = url::form_urlencoded::byte_serialize(cid?.as_bytes()).collect::<String>();
        Some(format!("{VIDEO_SERVICE_URL}/watch/{did}/{cid}/{asset}"))
    };
    let playlist = embed
        .get("playlist")
        .and_then(Value::as_str)
        .and_then(approved_video_cdn_url)
        .or_else(|| canonical_url("playlist.m3u8"));
    let Some(playlist) = playlist else {
        return String::new();
    };
    let thumbnail = embed
        .get("thumbnail")
        .and_then(Value::as_str)
        .and_then(approved_video_cdn_url)
        .or_else(|| canonical_url("thumbnail.jpg"));
    let alt = embed.get("alt").and_then(Value::as_str).unwrap_or_default();
    let (dimensions, ratio_class) = embed
        .get("aspectRatio")
        .and_then(|ratio| {
            let width = ratio.get("width")?.as_u64()?;
            let height = ratio.get("height")?.as_u64()?;
            (width > 0 && height > 0 && width <= 10_000 && height <= 10_000)
                .then(|| (format!(" width=\"{width}\" height=\"{height}\""), ""))
        })
        .unwrap_or_else(|| (String::new(), " video-default-ratio"));
    let poster = thumbnail
        .as_deref()
        .map(|url| format!(" poster=\"{}\"", html_escape(url)))
        .unwrap_or_default();
    format!(
        "<figure class=\"post-video{ratio_class}\"><video controls muted playsinline preload=\"none\" data-video-playlist=\"{}\" aria-label=\"{}\"{poster}{dimensions}></video><a class=\"video-playlist-link\" href=\"{}\" rel=\"nofollow noopener noreferrer\">Open video playlist</a></figure>",
        html_escape(&playlist),
        html_escape(alt),
        html_escape(&playlist)
    )
}

fn approved_video_cdn_url(value: &str) -> Option<String> {
    let url = reqwest::Url::parse(value).ok()?;
    (url.scheme() == "https"
        && url.host_str() == Some("video.bsky.app")
        && url.username().is_empty()
        && url.password().is_none()
        && url.path().starts_with("/watch/"))
    .then(|| value.to_owned())
}

fn render_web_images(author_did: &str, embed: &Value) -> String {
    let Some(images) = embed.get("images").and_then(Value::as_array) else {
        return String::new();
    };
    let images = images
        .iter()
        .filter_map(|image| {
            let cid = image
                .pointer("/image/ref/$link")
                .or_else(|| image.pointer("/image/ref"))
                .and_then(Value::as_str)?;
            let alt = image.get("alt").and_then(Value::as_str).unwrap_or("");
            Some(format!(
                "<img src=\"https://cdn.bsky.app/img/feed_fullsize/plain/{}/{}\" alt=\"{}\" loading=\"lazy\">",
                html_escape(author_did),
                html_escape(cid),
                html_escape(alt)
            ))
        })
        .collect::<String>();
    if images.is_empty() {
        String::new()
    } else {
        format!("<div class=\"post-images\">{images}</div>")
    }
}

fn render_web_external(author_did: &str, embed: &Value) -> String {
    let Some(external) = embed.get("external") else {
        return String::new();
    };
    let uri = external.get("uri").and_then(Value::as_str).unwrap_or("");
    let title = external.get("title").and_then(Value::as_str).unwrap_or(uri);
    let description = external
        .get("description")
        .and_then(Value::as_str)
        .unwrap_or("");
    let thumb_src = external.get("thumb").and_then(|thumb| {
        if let Some(uri) = thumb.as_str() {
            return safe_external_url(uri).then(|| uri.to_owned());
        }
        let cid = thumb
            .pointer("/ref/$link")
            .or_else(|| thumb.pointer("/ref"))
            .and_then(Value::as_str)?;
        (!author_did.is_empty() && !cid.is_empty()).then(|| {
            format!(
                "https://cdn.bsky.app/img/feed_thumbnail/plain/{}/{cid}",
                html_escape(author_did)
            )
        })
    });
    if uri.is_empty() && title.is_empty() && description.is_empty() {
        return String::new();
    }
    let title = html_escape(title);
    let uri_markup = if safe_external_url(uri) {
        format!(
            "<a href=\"{}\" rel=\"nofollow noopener noreferrer ugc\">{}</a>",
            html_escape(uri),
            html_escape(uri)
        )
    } else {
        html_escape(uri)
    };
    let thumb_markup = thumb_src
        .map(|src| {
            format!(
                "<img src=\"{}\" alt=\"\" loading=\"lazy\">",
                html_escape(&src)
            )
        })
        .unwrap_or_default();
    format!(
        "<aside class=\"external-card\">{thumb_markup}<p>{title}</p><p>{}</p><p>{uri_markup}</p></aside>",
        html_escape(description)
    )
}

fn safe_external_url(uri: &str) -> bool {
    url::Url::parse(uri).is_ok_and(|url| matches!(url.scheme(), "http" | "https"))
}

fn render_web_quote<S: Storage>(
    storage: &S,
    embed: &Value,
    quote_index: &QuoteRenderIndex,
    quote_depth: usize,
) -> String {
    let Some(reference) = quoted_record_ref(embed) else {
        return String::new();
    };
    let uri = reference.uri.as_str();
    let Some(collection) = at_uri_collection(uri) else {
        return String::new();
    };
    let encoded_uri = form_encoded(uri);
    if quote_depth > 0 {
        return format!(
            "<a class=\"quoted-nested-link\" href=\"/app/thread?uri={encoded_uri}\">Quoted post</a>"
        );
    }
    if reference.unavailable {
        return format!(
            "<a class=\"quoted-post-card quoted-post-status\" href=\"/app/thread?uri={encoded_uri}\">Post unavailable</a>"
        );
    }
    if collection != "app.bsky.feed.post" {
        let indexed = quote_index.get(uri).and_then(|state| match state {
            QuoteRenderState::Other(record) => record.clone(),
            _ => None,
        });
        let cached = indexed.or_else(|| storage.record_by_uri(collection, uri).ok().flatten());
        let record_name = cached
            .as_ref()
            .and_then(|record| {
                ["displayName", "name", "handle"]
                    .iter()
                    .find_map(|key| record.record.get(*key).and_then(Value::as_str))
            })
            .unwrap_or_else(|| quote_collection_name(collection));
        return format!(
            "<a class=\"quoted-post-card quoted-record-type\" href=\"{}\"><strong>{}</strong><span>{}</span></a>",
            html_escape(uri),
            html_escape(record_name),
            html_escape(quote_collection_name(collection))
        );
    }
    let state = quote_index.get(uri).cloned().unwrap_or_else(|| {
        storage
            .record_by_uri(collection, uri)
            .ok()
            .flatten()
            .map(QuoteRenderState::Found)
            .unwrap_or(QuoteRenderState::Pending)
    });
    let quoted = match state {
        QuoteRenderState::Found(record) => record,
        QuoteRenderState::Unavailable => {
            return format!(
                "<a class=\"quoted-post-card quoted-post-status\" href=\"/app/thread?uri={encoded_uri}\">Post unavailable</a>"
            );
        }
        QuoteRenderState::NotFound => {
            return format!(
                "<a class=\"quoted-post-card quoted-post-status\" href=\"/app/thread?uri={encoded_uri}\">Post not found</a>"
            );
        }
        QuoteRenderState::Pending | QuoteRenderState::Other(_) => {
            return format!(
                "<a class=\"quoted-post-card quoted-post-status\" href=\"/app/thread?uri={encoded_uri}\">Quoted post</a>"
            );
        }
    };
    let profile = profile_view_from_storage(storage, &quoted.did)
        .unwrap_or_else(|_| json!({"did":quoted.did,"handle":quoted.did}));
    let handle = profile_handle(&profile, &quoted.did);
    let display_name = profile
        .get("displayName")
        .and_then(Value::as_str)
        .unwrap_or(&handle);
    let text = quoted
        .record
        .get("text")
        .and_then(Value::as_str)
        .unwrap_or("");
    let created_at = quoted.record.get("createdAt").and_then(Value::as_str);
    let time = created_at
        .map(|created_at| {
            format!(
                "<time datetime=\"{}\"> · {}</time>",
                html_escape(created_at),
                html_escape(&relative_time_short(created_at))
            )
        })
        .unwrap_or_default();
    let nested = quoted
        .record
        .get("embed")
        .map(|embed| {
            render_web_embed_with_quotes(storage, &quoted.did, embed, quote_index, quote_depth + 1)
        })
        .unwrap_or_default();
    format!(
        "<article class=\"quoted-post-card\"><a class=\"quoted-card-main\" href=\"/app/thread?uri={encoded_uri}\" aria-label=\"Open quoted post thread\">{}<div class=\"quoted-card-body\"><header><strong>{}</strong><span class=\"quoted-card-handle\">@{}</span>{time}</header><p>{}</p></div></a>{nested}</article>",
        avatar_markup(&profile, "small-avatar"),
        html_escape(display_name),
        html_escape(&handle),
        render_post_text(text)
    )
}

fn quote_collection_name(collection: &str) -> &'static str {
    match collection {
        "app.bsky.feed.generator" => "Feed generator",
        "app.bsky.graph.list" => "List",
        "app.bsky.graph.starterpack" => "Starter pack",
        _ => "Record",
    }
}

fn relative_time_short(created_at: &str) -> String {
    let Ok(created_at) = OffsetDateTime::parse(created_at, &Rfc3339) else {
        return "now".to_owned();
    };
    let seconds = (OffsetDateTime::now_utc().unix_timestamp() - created_at.unix_timestamp()).max(0);
    let (amount, unit) = match seconds {
        value if value < 60 => (value, "s"),
        value if value < 60 * 60 => (value / 60, "m"),
        value if value < 24 * 60 * 60 => (value / (60 * 60), "h"),
        value if value < 7 * 24 * 60 * 60 => (value / (24 * 60 * 60), "d"),
        value if value < 30 * 24 * 60 * 60 => (value / (7 * 24 * 60 * 60), "w"),
        value if value < 365 * 24 * 60 * 60 => (value / (30 * 24 * 60 * 60), "mo"),
        value => (value / (365 * 24 * 60 * 60), "y"),
    };
    format!("{amount}{unit}")
}

fn has_atproto_scope(scope: Option<&str>) -> bool {
    scope.is_some_and(|scope| scope.split_whitespace().any(|value| value == "atproto"))
}

fn log_pds_xrpc_failure(method: &'static str, error: &XrpcClientError<Value>) {
    let (status, error_code, error_message) = match error {
        XrpcClientError::XrpcResponse(response) => {
            let (code, message) = response
                .error
                .as_ref()
                .map(xrpc_error_details)
                .unwrap_or((None, None));
            (Some(response.status), code, message)
        }
        _ => (None, None, None),
    };
    tracing::warn!(
        method,
        status = ?status,
        error_code = ?error_code,
        error_message = ?error_message,
        "PDS XRPC request failed"
    );
}

fn xrpc_error_details(error: &XrpcErrorKind<Value>) -> (Option<&str>, Option<&str>) {
    match error {
        XrpcErrorKind::Custom(value) => (
            value.get("error").and_then(Value::as_str),
            value.get("message").and_then(Value::as_str),
        ),
        XrpcErrorKind::Undefined(value) => (value.error.as_deref(), value.message.as_deref()),
    }
}

fn has_all_write_scopes(scope: Option<&str>) -> bool {
    let Some(scope) = scope else {
        return false;
    };
    let granted = scope.split_whitespace().collect::<HashSet<_>>();
    WRITE_SCOPES
        .iter()
        .all(|required| granted.contains(required))
}

fn has_all_feed_scopes(scope: Option<&str>) -> bool {
    let Some(scope) = scope else {
        return false;
    };
    let granted = scope.split_whitespace().collect::<HashSet<_>>();
    FEED_SCOPES
        .iter()
        .all(|required| granted.contains(required))
}

fn has_blob_scope(scope: Option<&str>) -> bool {
    scope.is_some_and(|scope| scope.split_whitespace().any(|value| value == BLOB_SCOPE))
}

fn has_all_video_scopes(scope: Option<&str>) -> bool {
    let Some(scope) = scope else {
        return false;
    };
    let granted = scope.split_whitespace().collect::<HashSet<_>>();
    VIDEO_SCOPES
        .iter()
        .all(|required| granted.contains(required))
}

fn requested_scopes() -> Vec<Scope> {
    std::iter::once(Scope::Known(KnownScope::Atproto))
        .chain(
            WRITE_SCOPES
                .iter()
                .map(|scope| Scope::Unknown((*scope).to_owned())),
        )
        .chain(std::iter::once(Scope::Unknown(BLOB_SCOPE.to_owned())))
        .chain(
            FEED_SCOPES
                .iter()
                .map(|scope| Scope::Unknown((*scope).to_owned())),
        )
        .chain(
            VIDEO_SCOPES
                .iter()
                .map(|scope| Scope::Unknown((*scope).to_owned())),
        )
        .collect()
}

fn unix_time() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn load_or_create_key(path: &Path) -> Result<Jwk, OAuthConfigError> {
    match fs::read(path) {
        Ok(contents) => Ok(serde_json::from_slice(&contents)?),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            if let Some(parent) = path.parent() {
                fs::create_dir_all(parent)?;
            }
            let key = create_key();
            let contents = serde_json::to_vec(&key)?;
            match create_key_file(path) {
                Ok(mut file) => file.write_all(&contents)?,
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                    return load_or_create_key(path);
                }
                Err(error) => return Err(error.into()),
            }
            Ok(key)
        }
        Err(error) => Err(error.into()),
    }
}

fn validate_private_key(private_key: &Jwk) -> Result<(), OAuthConfigError> {
    if matches!(
        &private_key.key,
        Key::Ec(Ec {
            crv: EcCurves::P256,
            d: Some(_),
            ..
        })
    ) {
        Ok(())
    } else {
        Err(OAuthConfigError::InvalidPrivateKey)
    }
}

fn create_key() -> Jwk {
    let secret = SecretKey::random(&mut thread_rng());
    let public = secret.public_key().to_encoded_point(false);
    let value = json!({
        "kty": "EC",
        "crv": "P-256",
        "x": URL_SAFE_NO_PAD.encode(public.x().expect("uncompressed P-256 key has x")),
        "y": URL_SAFE_NO_PAD.encode(public.y().expect("uncompressed P-256 key has y")),
        "d": URL_SAFE_NO_PAD.encode(secret.to_bytes()),
        "alg": "ES256",
        "kid": "appview-oauth-1",
        "use": "sig"
    });
    let mut key: Jwk = serde_json::from_value(value).expect("generated P-256 JWK is valid");
    key.prm = Parameters {
        alg: Some(jose_jwk::jose_jwa::Algorithm::Signing(
            jose_jwk::jose_jwa::Signing::Es256,
        )),
        kid: Some("appview-oauth-1".to_owned()),
        cls: Some(jose_jwk::Class::Signing),
        ..Default::default()
    };
    key
}

fn validate_origin(origin: &str) -> Result<(), OAuthConfigError> {
    let parsed = reqwest::Url::parse(origin).map_err(|_| OAuthConfigError::InvalidPublicUrl)?;
    let local = matches!(parsed.host_str(), Some("localhost" | "127.0.0.1" | "::1"));
    if (parsed.scheme() != "https" && !(local && parsed.scheme() == "http"))
        || parsed.host_str().is_none()
        || !parsed.username().is_empty()
        || parsed.password().is_some()
        || !matches!(parsed.path(), "" | "/")
        || parsed.query().is_some()
        || parsed.fragment().is_some()
    {
        return Err(OAuthConfigError::InvalidPublicUrl);
    }
    Ok(())
}

#[cfg(unix)]
fn create_key_file(path: &Path) -> Result<fs::File, std::io::Error> {
    use std::os::unix::fs::OpenOptionsExt;
    fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::identity::{DidDocument, DidService, IdentityBackend, ResolutionError};
    use crate::ingest::IndexedRecord;
    use atrium_oauth::store::session::Session;
    use axum::http::Uri;
    use http_body_util::BodyExt;
    use std::collections::HashMap;
    use tokio::net::TcpListener;
    use tower::ServiceExt;
    use tracing::instrument::WithSubscriber;
    use tracing_subscriber::fmt::MakeWriter;

    #[test]
    fn external_post_record_has_the_atproto_shape_with_optional_thumbnail() {
        let external = PostExternal {
            uri: "https://example.test/article".to_owned(),
            title: "Example title".to_owned(),
            description: "Short description".to_owned(),
            thumbnail: None,
        };
        let without_thumb = external_embed_value(&external, None);
        assert_eq!(without_thumb["$type"], "app.bsky.embed.external");
        assert_eq!(
            without_thumb["external"],
            json!({
                "$type":"app.bsky.embed.external#external",
                "uri":"https://example.test/article",
                "title":"Example title",
                "description":"Short description"
            })
        );
        assert!(without_thumb["external"].get("thumb").is_none());

        let blob =
            json!({"$type":"blob","ref":{"$link":"bafk-thumb"},"mimeType":"image/png","size":3});
        let with_thumb = external_embed_value(&external, Some(blob.clone()));
        assert_eq!(with_thumb["external"]["thumb"], blob);
        assert_eq!(
            selected_link_url(true, Some("https://example.test/article")),
            None,
            "pictures take precedence when both are selected"
        );
        assert_eq!(
            selected_link_url(false, Some(" https://example.test/article ")),
            Some("https://example.test/article")
        );
    }

    #[test]
    fn video_embed_renders_playlist_alt_text_and_aspect_ratio() {
        let storage = MemoryStorage::default();
        let rendered = render_web_embed(
            &storage,
            "did:plc:av31author",
            &json!({
                "$type":"app.bsky.embed.video#view",
                "playlist":"https://video.bsky.app/watch/did%3Aplc%3Aav31author/bafk-video/playlist.m3u8",
                "thumbnail":"https://video.bsky.app/watch/did%3Aplc%3Aav31author/bafk-video/thumbnail.jpg",
                "alt":"A red bicycle rides past a park",
                "aspectRatio":{"width":1920,"height":1080}
            }),
        );
        assert!(rendered.contains("<video"), "{rendered}");
        assert!(rendered.contains("playlist.m3u8"), "{rendered}");
        assert!(
            rendered.contains("A red bicycle rides past a park"),
            "{rendered}"
        );
        assert!(rendered.contains("width=\"1920\""), "{rendered}");
        assert!(rendered.contains("height=\"1080\""), "{rendered}");
        assert!(rendered.contains("thumbnail.jpg"), "{rendered}");
        assert!(rendered.contains("controls"), "{rendered}");
        assert!(rendered.contains("muted"), "{rendered}");
        assert!(!rendered.contains("autoplay"), "{rendered}");
    }

    #[test]
    fn video_oauth_scopes_are_explicit_and_old_scopes_do_not_enable_video() {
        for scope in [
            "blob:video/*",
            "rpc:app.bsky.video.getUploadLimits?aud=did:web:video.bsky.app",
            "rpc:app.bsky.video.uploadVideo?aud=did:web:video.bsky.app",
            "rpc:app.bsky.video.getJobStatus?aud=did:web:video.bsky.app",
        ] {
            assert!(
                REQUESTED_SCOPE
                    .split_whitespace()
                    .any(|value| value == scope)
            );
        }
        let old_scope = "atproto repo:app.bsky.feed.post?action=create blob:image/*";
        assert!(!has_all_video_scopes(Some(old_scope)));
        assert!(has_all_video_scopes(Some(REQUESTED_SCOPE)));
    }

    #[test]
    fn legacy_session_cookie_defaults_to_no_video_permission() {
        let public = OAuthPublic::new("https://appview.example").unwrap();
        let payload = URL_SAFE_NO_PAD.encode(
            format!(
                "did:plc:legacy\n{}\nlegacy-session\n7\ntrue\ntrue\ntrue",
                unix_time().saturating_add(SESSION_TTL_SECONDS)
            )
            .as_bytes(),
        );
        let mut mac = HmacSha256::new_from_slice(&public.cookie_key()).unwrap();
        mac.update(b"session:");
        mac.update(payload.as_bytes());
        let token = format!(
            "{payload}.{}",
            URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
        );

        let session = public.verify_session_cookie(&token).unwrap();
        assert!(session.5, "legacy blob permission remains available");
        assert!(!session.6, "legacy sessions must sign in again for video");
    }

    #[test]
    fn video_file_constraints_reject_oversize_and_overlong_uploads() {
        assert!(validate_video_properties(MAX_VIDEO_BYTES + 1, 30.0, "video/mp4").is_err());
        assert!(
            validate_video_properties(1024, MAX_VIDEO_DURATION_SECONDS + 0.1, "video/mp4").is_err()
        );
        assert!(validate_video_properties(1024, 30.0, "video/webm").is_ok());
    }

    #[test]
    fn daily_upload_limits_are_checked_before_forwarding_video_bytes() {
        assert!(
            validate_video_upload_limits(
                &json!({"canUpload":true,"remainingDailyVideos":1,"remainingDailyBytes":1024}),
                1024
            )
            .is_ok()
        );
        assert!(
            validate_video_upload_limits(
                &json!({"canUpload":true,"remainingDailyVideos":0,"remainingDailyBytes":2048}),
                1024
            )
            .unwrap_err()
            .contains("daily video upload limit")
        );
        assert!(
            validate_video_upload_limits(
                &json!({"canUpload":true,"remainingDailyVideos":1,"remainingDailyBytes":500}),
                1024
            )
            .unwrap_err()
            .contains("daily video upload data limit")
        );
    }

    #[tokio::test]
    async fn video_job_polling_stops_after_completion_and_returns_the_blob() {
        let mut states = [
            json!({"jobStatus":{"jobId":"job-1","state":"JOB_STATE_ENCODING"}}),
            json!({"jobStatus":{"jobId":"job-1","state":"JOB_STATE_COMPLETED","blob":{"ref":{"$link":"bafk-video"},"mimeType":"video/mp4","size":99}}}),
        ]
        .into_iter();
        let blob = poll_video_job("job-1", Duration::ZERO, 3, |_| {
            let state = states.next().expect("poll stops when the job completes");
            Box::pin(async move { Ok(state) })
        })
        .await
        .unwrap();
        assert_eq!(
            blob.pointer("/ref/$link").and_then(Value::as_str),
            Some("bafk-video")
        );
    }

    #[tokio::test]
    async fn video_job_polling_stops_immediately_when_processing_fails() {
        let mut states = [
            json!({"jobStatus":{"jobId":"job-2","state":"JOB_STATE_SCANNING"}}),
            json!({"jobStatus":{"jobId":"job-2","state":"JOB_STATE_FAILED","message":"Video moderation rejected this upload"}}),
        ]
        .into_iter();
        let result = poll_video_job("job-2", Duration::ZERO, 3, |_| {
            let state = states.next().expect("poll stops as soon as the job fails");
            Box::pin(async move { Ok(state) })
        })
        .await;
        assert_eq!(result.unwrap_err(), "Video moderation rejected this upload");
    }

    #[test]
    fn video_post_record_contains_video_blob_alt_ratio_reply_and_quote_shapes() {
        let blob =
            json!({"$type":"blob","ref":{"$link":"bafk-video"},"mimeType":"video/mp4","size":99});
        let base = build_video_post_record(
            "A clip",
            blob.clone(),
            "A river at sunset",
            1920,
            1080,
            None,
            None,
        );
        assert_eq!(base["embed"]["$type"], "app.bsky.embed.video");
        assert_eq!(base["embed"]["video"], blob);
        assert_eq!(base["embed"]["alt"], "A river at sunset");
        assert_eq!(
            base["embed"]["aspectRatio"],
            json!({"width":1920,"height":1080})
        );

        let quoted = PostStrongRef {
            uri: "at://did:plc:author/app.bsky.feed.post/abc".to_owned(),
            cid: "cid-quoted".to_owned(),
        };
        let quote = build_video_post_record(
            "Quote",
            blob.clone(),
            "alt",
            1,
            1,
            None,
            Some(quoted.clone()),
        );
        assert_eq!(quote["embed"]["$type"], "app.bsky.embed.recordWithMedia");
        assert_eq!(quote["embed"]["media"]["$type"], "app.bsky.embed.video");
        assert_eq!(quote["embed"]["record"]["record"]["uri"], quoted.uri);
        let reply = json!({"root":{"uri":"root-uri","cid":"root-cid"},"parent":{"uri":"parent-uri","cid":"parent-cid"}});
        let reply_post =
            build_video_post_record("Reply", blob, "alt", 1, 1, Some(reply.clone()), None);
        assert_eq!(reply_post["reply"], reply);
    }

    #[tokio::test]
    async fn alt_test_page_requires_and_accepts_a_verified_session_cookie() {
        let public = OAuthPublic::new("https://appview.example").unwrap();
        let storage = Arc::new(MemoryStorage::default());
        let did = "did:plc:av27viewer";
        let session_id = "av27-session";
        let generation = storage.rotate_web_session(did, session_id).unwrap();
        let token = public.issue_session_cookie(
            did,
            session_id,
            generation,
            SessionPermissions {
                write: true,
                feed: true,
                blob: true,
                video: true,
            },
        );
        let app = public.router_with_flow(DisabledOAuthFlow, storage);
        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/alt-test")
                    .header("cookie", format!("{SESSION_COOKIE}={token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let page = String::from_utf8(body.to_vec()).unwrap();
        for marker in [
            "data-alt-test",
            "data-alt-test-backend",
            "data-alt-test-bytes",
            "data-alt-test-load-ms",
            "data-alt-test-caption-ms",
            "data-alt-test-caption",
        ] {
            assert!(page.contains(marker), "missing measurement field {marker}");
        }
        assert!(page.contains("never uploaded"));
    }

    const FEED_VIEWER_DID: &str = "did:plc:av23viewer";
    const FEED_CREATOR_DID: &str = "did:plc:av23creator";
    const FEED_SERVICE_DID: &str = "did:plc:av23service";
    const FEED_URI: &str = "at://did:plc:av23creator/app.bsky.feed.generator/av23-authenticated";
    const FAILURE_FEED_URI: &str =
        "at://did:plc:av23creator/app.bsky.feed.generator/av23-auth-failure";
    const PLACEHOLDER_URI: &str = "at://did:plc:av23author/app.bsky.feed.post/placeholder";
    const REAL_POST_URIS: [&str; 3] = [
        "at://did:plc:av23author/app.bsky.feed.post/real-one",
        "at://did:plc:av23author/app.bsky.feed.post/real-two",
        "at://did:plc:av23author/app.bsky.feed.post/real-three",
    ];
    const SERVICE_JWT: &str = "av23-service-jwt";

    #[test]
    fn icon_symbols_leave_fill_to_css_so_liked_hearts_can_be_filled() {
        let sprite = icon_sprite();
        let css = include_str!("../static/app.css");

        assert!(sprite.split("<symbol ").skip(1).all(|symbol| {
            !symbol
                .split('>')
                .next()
                .unwrap_or_default()
                .contains(" fill=")
        }));
        assert!(sprite.contains("<symbol id=\"i-like\""));
        assert!(css.contains(".icon {\n  fill: none;"));
        assert!(css.contains(".post-actions .is-liked .icon {\n  fill: var(--accent);"));
        assert!(css.contains(".thread-focus-actions .is-liked .icon {\n  fill: var(--accent);"));
        assert!(css.contains(".infinite-scroll-status {"));
        assert!(css.contains(".infinite-scroll-retry[hidden] {\n  display: none;"));
        assert!(WEB_STYLESHEET_URL.contains("v=m13"));
        assert!(WEB_SCRIPT_URL.contains("v=js11"));
    }

    #[test]
    fn notification_groups_respect_type_subject_age_and_read_boundary() {
        let notices = vec![
            web_notice("alice", "like", 200 * HOUR_US, Some("post-a")),
            web_notice("bob", "like", 199 * HOUR_US, Some("post-a")),
            web_notice("carol", "like", 152 * HOUR_US, Some("post-a")),
            web_notice("dave", "like", 151 * HOUR_US, Some("post-a")),
            web_notice("ivan", "like", 199 * HOUR_US, Some("post-b")),
            web_notice("erin", "repost", 199 * HOUR_US, Some("post-a")),
            web_notice("frank", "follow", 199 * HOUR_US, None),
            web_notice("grace", "follow", 198 * HOUR_US, None),
            web_notice("heidi", "like", 150 * HOUR_US, Some("post-a")),
        ];

        let groups = group_web_notifications(&notices, 150 * HOUR_US);
        let likes_a = groups
            .iter()
            .filter(|group| {
                group.reason == "like"
                    && group
                        .reason_subject
                        .as_deref()
                        .is_some_and(|subject| subject.ends_with("/post-a"))
            })
            .collect::<Vec<_>>();
        assert_eq!(likes_a.len(), 3);
        assert_eq!(likes_a[0].events.len(), 3);
        assert_eq!(likes_a[1].events.len(), 1);
        assert_eq!(likes_a[2].events.len(), 1);
        assert!(
            groups
                .iter()
                .any(|group| group.reason == "follow" && group.events.len() == 2)
        );
        assert!(
            groups
                .iter()
                .any(|group| group.reason == "repost" && group.events.len() == 1)
        );
        assert!(groups.iter().any(|group| {
            group.reason == "like"
                && group
                    .reason_subject
                    .as_deref()
                    .is_some_and(|subject| subject.ends_with("/post-b"))
        }));
    }

    #[test]
    fn notification_group_pages_keep_a_group_whole_without_duplicates() {
        let notices = vec![
            web_notice("alice", "like", 10 * HOUR_US, Some("post-a")),
            web_notice("bob", "like", 9 * HOUR_US, Some("post-a")),
            web_notice("carol", "like", 8 * HOUR_US, Some("post-b")),
            web_notice("dave", "reply", 7 * HOUR_US, Some("post-c")),
        ];
        let groups = group_web_notifications(&notices, 0);
        let (first, has_more) = notification_group_page(&groups, None, 1);
        assert!(has_more);
        assert_eq!(first[0].events.len(), 2);
        let cursor = notification_group_cursor(first.last().unwrap());
        let (second, has_more) = notification_group_page(&groups, Some(&cursor), 1);
        assert!(has_more);
        assert_eq!(second[0].events.len(), 1);
        assert_ne!(
            first[0].events[0].record.uri,
            second[0].events[0].record.uri
        );
        let cursor = notification_group_cursor(second.last().unwrap());
        let (third, has_more) = notification_group_page(&groups, Some(&cursor), 1);
        assert!(!has_more);
        assert_eq!(third[0].events.len(), 1);
        let all_uris = first
            .iter()
            .chain(&second)
            .chain(&third)
            .flat_map(|group| group.events.iter().map(|event| event.record.uri.clone()))
            .collect::<HashSet<_>>();
        assert_eq!(all_uris.len(), 4);
    }

    #[test]
    fn grouped_like_row_shows_other_count_avatars_and_subject_snippet() {
        let storage = MemoryStorage::default();
        storage
            .set_verified_handle("did:plc:alice", "alice.test")
            .unwrap();
        storage
            .set_verified_handle("did:plc:bob", "bob.test")
            .unwrap();
        let post_uri = "at://did:plc:viewer/app.bsky.feed.post/post-a";
        storage
            .put_record(&IndexedRecord {
                did: "did:plc:viewer".into(),
                collection: "app.bsky.feed.post".into(),
                rkey: "post-a".into(),
                uri: post_uri.into(),
                cid: None,
                time_us: 1,
                record: json!({"text":"subject <preview> text"}),
            })
            .unwrap();
        let notices = vec![
            web_notice("alice", "like", 10 * HOUR_US, Some("post-a")),
            web_notice("bob", "like", 9 * HOUR_US, Some("post-a")),
        ];
        let groups = group_web_notifications(&notices, 0);
        let group = &groups[0];
        let html = render_web_notification_group(&storage, group, &HashMap::new(), &HashMap::new());
        assert!(html.contains("alice.test</a> and 1 other liked your post"));
        assert_eq!(
            html.matches("class=\"notification-avatar avatar-placeholder\"")
                .count(),
            2
        );
        assert!(html.contains("subject &lt;preview&gt; text"));
    }

    #[tokio::test]
    async fn notification_stream_sends_initial_count_and_heartbeat() {
        let storage = Arc::new(MemoryStorage::default());
        let did = "did:plc:stream-test".to_owned();
        let receiver = storage.subscribe_notification_counts(&did);
        let registry = Arc::new(Mutex::new(HashMap::new()));
        let shutdown = WebShutdown::new();
        let guard =
            NotificationStreamGuard::acquire(registry, shutdown.notification_streams.clone(), &did)
                .unwrap();
        let events = notification_count_stream(
            receiver,
            3,
            storage,
            did,
            Duration::from_millis(10),
            shutdown.subscribe(),
            guard,
        );
        let mut body = Sse::new(events).into_response().into_body();
        let initial = tokio::time::timeout(Duration::from_secs(1), body.frame())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .into_data()
            .unwrap();
        assert!(String::from_utf8_lossy(&initial).contains("data: 3"));
        let heartbeat = tokio::time::timeout(Duration::from_secs(1), body.frame())
            .await
            .unwrap()
            .unwrap()
            .unwrap()
            .into_data()
            .unwrap();
        assert!(String::from_utf8_lossy(&heartbeat).contains(": heartbeat"));
    }

    #[test]
    fn notification_badge_hides_zero_and_caps_counts_above_nine() {
        let zero = notification_badge_markup(0);
        assert!(zero.contains("hidden"));
        assert!(zero.contains(">0</span>"));
        let ten = notification_badge_markup(10);
        assert!(!ten.contains(" hidden"));
        assert!(ten.contains(">9+</span>"));
        assert!(ten.contains("10 unread notifications"));
    }

    #[tokio::test]
    async fn timeline_reply_shows_parent_card_and_connector_with_parent_identity() {
        let storage = Arc::new(MemoryStorage::default());
        let viewer = "did:plc:reply-viewer";
        let parent_did = "did:plc:original-author";
        let reply_did = "did:plc:reply-author";
        let parent = test_web_post(parent_did, "original", 1, "Original text", None);
        let reply = test_web_post(reply_did, "reply", 2, "Reply text", Some(&parent.uri));
        storage.put_record(&parent).unwrap();
        storage.put_record(&reply).unwrap();
        storage.set_follow(viewer, reply_did, true).unwrap();
        storage
            .set_verified_handle(parent_did, "original.test")
            .unwrap();
        storage
            .set_verified_handle(reply_did, "reply.test")
            .unwrap();
        put_test_profile(&storage, parent_did, "Original Author");

        let page = request_test_timeline(storage, viewer).await;

        assert!(page.contains("class=\"reply-parent-card\""));
        assert!(page.contains("class=\"reply-connector\""));
        assert!(page.contains("Original text"));
        assert!(page.contains("Original Author"));
        assert!(page.contains("@original.test"));
        assert!(page.contains("Reply text"));
        assert!(page.contains("/app/thread?uri=at%3A%2F%2Fdid%3Aplc%3Aoriginal-author"));
        assert!(page.contains(
            "data-post-kind=\"post\" data-post-uri=\"at://did:plc:reply-author/app.bsky.feed.post/reply\""
        ));
        assert!(page.contains(
            "data-post-kind=\"parent\" data-post-uri=\"at://did:plc:original-author/app.bsky.feed.post/original\""
        ));
    }

    #[tokio::test]
    async fn timeline_groups_three_post_self_thread_without_standalone_duplicates() {
        let storage = Arc::new(MemoryStorage::default());
        let viewer = "did:plc:reply-viewer";
        let author = "did:plc:self-thread-author";
        let first = test_web_post(author, "self-1", 1, "Self thread first", None);
        let second = test_web_post(author, "self-2", 2, "Self thread second", Some(&first.uri));
        let third = test_web_post(author, "self-3", 3, "Self thread third", Some(&second.uri));
        for record in [&first, &second, &third] {
            storage.put_record(record).unwrap();
            storage.set_follow(viewer, author, true).unwrap();
        }

        let page = request_test_timeline(storage, viewer).await;

        assert_eq!(
            page.matches("class=\"post-item thread-group\"").count(),
            1,
            "{page}"
        );
        assert_eq!(page.matches("class=\"post-card\"").count(), 3, "{page}");
        assert_eq!(
            page.matches("class=\"reply-parent-card\"").count(),
            0,
            "{page}"
        );
        for text in [
            "Self thread first",
            "Self thread second",
            "Self thread third",
        ] {
            assert_eq!(page.matches(text).count(), 1, "{page}");
        }
        let first_position = page.find("Self thread first").unwrap();
        let second_position = page.find("Self thread second").unwrap();
        let third_position = page.find("Self thread third").unwrap();
        assert!(first_position < second_position && second_position < third_position);
    }

    #[tokio::test]
    async fn timeline_caps_mixed_author_thread_and_links_to_skipped_replies() {
        let storage = Arc::new(MemoryStorage::default());
        let viewer = "did:plc:reply-viewer";
        let mut records = Vec::new();
        for index in 0..5_u64 {
            let author = format!("did:plc:mixed-author-{index}");
            let parent_uri = records
                .last()
                .map(|record: &IndexedRecord| record.uri.clone());
            let record = test_web_post(
                &author,
                &format!("mixed-{index}"),
                index + 1,
                &format!("Mixed post {index}"),
                parent_uri.as_deref(),
            );
            storage.put_record(&record).unwrap();
            storage.set_follow(viewer, &author, true).unwrap();
            records.push(record);
        }

        let page = request_test_timeline(storage, viewer).await;

        assert_eq!(
            page.matches("class=\"post-item thread-group\"").count(),
            1,
            "{page}"
        );
        assert_eq!(page.matches("class=\"post-card\"").count(), 3, "{page}");
        for shown in ["Mixed post 0", "Mixed post 3", "Mixed post 4"] {
            assert!(page.contains(shown), "missing {shown}: {page}");
        }
        for skipped in ["Mixed post 1", "Mixed post 2"] {
            assert!(!page.contains(skipped), "unexpected {skipped}: {page}");
        }
        assert!(page.contains("2 more replies"), "{page}");
        assert!(page.contains("class=\"thread-more\""), "{page}");
        assert!(page.contains("/app/thread?uri=at%3A%2F%2Fdid%3Aplc%3Amixed-author-0"));
    }

    #[tokio::test]
    async fn timeline_does_not_group_when_its_parent_chain_started_over_twenty_four_hours_earlier()
    {
        let storage = Arc::new(MemoryStorage::default());
        let viewer = "did:plc:reply-viewer";
        let author = "did:plc:long-thread-author";
        let root = test_web_post(author, "long-root", 1_000_000, "Long thread root", None);
        storage.put_record(&root).unwrap();
        storage.set_follow(viewer, author, true).unwrap();
        let mut parent_uri = root.uri.clone();
        for index in 1..=51_u64 {
            let reply = test_web_post(
                author,
                &format!("long-{index}"),
                1_000_000 + 25 * 60 * 60 * 1_000_000 + index * 60 * 1_000_000,
                &format!("Long thread reply {index}"),
                Some(&parent_uri),
            );
            storage.put_record(&reply).unwrap();
            parent_uri = reply.uri.clone();
        }

        let page = request_test_timeline(storage, viewer).await;

        assert!(!page.contains("class=\"post-item thread-group\""), "{page}");
        assert!(page.contains("class=\"reply-parent-card\""), "{page}");
        assert!(!page.contains("Long thread root"), "{page}");
        assert!(page.contains("Long thread reply 2"), "{page}");
        assert!(page.contains("Long thread reply 51"), "{page}");
    }

    #[tokio::test]
    async fn timeline_caps_long_self_thread_and_links_to_the_thread_view() {
        let storage = Arc::new(MemoryStorage::default());
        let viewer = "did:plc:reply-viewer";
        let author = "did:plc:self-thread-long";
        let mut records = Vec::new();
        for index in 0..12_u64 {
            let parent_uri = records
                .last()
                .map(|record: &IndexedRecord| record.uri.clone());
            let record = test_web_post(
                author,
                &format!("self-long-{index}"),
                index + 1,
                &format!("Self long post {index}"),
                parent_uri.as_deref(),
            );
            storage.put_record(&record).unwrap();
            records.push(record);
        }
        storage.set_follow(viewer, author, true).unwrap();

        let page = request_test_timeline(storage, viewer).await;

        assert_eq!(
            page.matches("class=\"post-item thread-group\"").count(),
            1,
            "{page}"
        );
        assert_eq!(page.matches("class=\"post-card\"").count(), 5, "{page}");
        for shown in [
            "Self long post 0",
            "Self long post 1",
            "Self long post 9",
            "Self long post 10",
            "Self long post 11",
        ] {
            assert!(page.contains(shown), "missing {shown}: {page}");
        }
        for skipped in 2..=8 {
            assert!(
                !page.contains(&format!("Self long post {skipped}")),
                "{page}"
            );
        }
        assert!(page.contains("7 more in this thread"), "{page}");
        assert!(page.contains("class=\"thread-more\""), "{page}");
    }

    #[tokio::test]
    async fn timeline_reply_to_unavailable_parent_shows_a_fallback_line() {
        let storage = Arc::new(MemoryStorage::default());
        let viewer = "did:plc:reply-viewer";
        let reply_did = "did:plc:reply-author";
        let reply = test_web_post(
            reply_did,
            "reply-missing-parent",
            2,
            "Reply text",
            Some("at://did:plc:missing-parent/app.bsky.feed.post/gone"),
        );
        storage.put_record(&reply).unwrap();
        storage.set_follow(viewer, reply_did, true).unwrap();

        let page = request_test_timeline(storage, viewer).await;

        assert!(page.contains("Reply to <a"));
        assert!(page.contains("missing-parent"));
        assert!(page.contains("class=\"reply-connector\""));
    }

    #[tokio::test]
    async fn timeline_does_not_duplicate_a_parent_already_in_the_previous_card() {
        let storage = Arc::new(MemoryStorage::default());
        let viewer = "did:plc:reply-viewer";
        let parent_did = "did:plc:original-author";
        let reply_did = "did:plc:reply-author";
        let parent = test_web_post(parent_did, "original", 2, "Original text", None);
        let reply = test_web_post(reply_did, "reply", 1, "Reply text", Some(&parent.uri));
        for record in [&parent, &reply] {
            storage.put_record(record).unwrap();
            storage.set_follow(viewer, &record.did, true).unwrap();
        }

        let page = request_test_timeline(storage, viewer).await;

        assert_eq!(page.matches("Original text").count(), 1);
        assert!(!page.contains("class=\"reply-parent-card\""));
        assert!(page.contains("class=\"reply-connector reply-connector-previous\""));
    }

    #[tokio::test]
    async fn timeline_parent_hydration_uses_one_batch_lookup_for_the_page() {
        let storage = Arc::new(MemoryStorage::default());
        let viewer = "did:plc:reply-viewer";
        for index in 0..25_u64 {
            let parent_did = format!("did:plc:parent-{index}");
            let parent = test_web_post(&parent_did, "parent", 50 - index, "Parent text", None);
            let reply = test_web_post(
                "did:plc:reply-author",
                &format!("reply-{index}"),
                100 + index,
                "Reply text",
                Some(&parent.uri),
            );
            storage.put_record(&parent).unwrap();
            storage.put_record(&reply).unwrap();
            storage.set_follow(viewer, &parent_did, true).unwrap();
            storage
                .set_follow(viewer, "did:plc:reply-author", true)
                .unwrap();
        }

        let page = request_test_timeline(storage.clone(), viewer).await;
        let (single_lookups, batch_lookups) = storage.record_uri_lookup_counts();

        assert_eq!(page.matches("class=\"reply-parent-card\"").count(), 25);
        assert_eq!(
            single_lookups, 0,
            "parent hydration used per-record lookups"
        );
        assert_eq!(batch_lookups, 1, "parents should load in one page lookup");
    }

    #[tokio::test]
    async fn parent_hydration_batches_each_level_and_stops_after_two_extra_levels() {
        let storage = MemoryStorage::default();
        let mut leaves = Vec::new();
        let mut fourth_level_uris = Vec::new();
        for index in 0..25_u64 {
            let did = format!("did:plc:chain-{index}");
            let fourth = test_web_post(&did, "fourth", 1, "Fourth", None);
            let third = test_web_post(&did, "third", 2, "Third", Some(&fourth.uri));
            let second = test_web_post(&did, "second", 3, "Second", Some(&third.uri));
            let first = test_web_post(&did, "first", 4, "First", Some(&second.uri));
            let leaf = test_web_post(&did, "leaf", 5, "Leaf", Some(&first.uri));
            for record in [&fourth, &third, &second, &first] {
                storage.put_record(record).unwrap();
            }
            fourth_level_uris.push(fourth.uri);
            leaves.push(leaf);
        }
        let page_records = leaves.iter().collect::<Vec<_>>();

        let parents = hydrate_page_parents(
            &DisabledOAuthFlow,
            &storage,
            &page_records,
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();
        let (single_lookups, batch_lookups) = storage.record_uri_lookup_counts();

        assert_eq!(
            single_lookups, 0,
            "parent hydration must not look up one URI at a time"
        );
        assert_eq!(
            batch_lookups, 3,
            "one page batch plus at most two ancestor batches"
        );
        assert_eq!(parents.len(), 75, "three parent levels should be hydrated");
        assert!(
            fourth_level_uris
                .iter()
                .all(|uri| !parents.contains_key(uri))
        );
    }

    #[tokio::test]
    async fn parent_author_cache_miss_does_not_block_and_refresh_fills_the_handle() {
        let storage = Arc::new(MemoryStorage::default());
        let viewer = "did:plc:reply-viewer";
        let parent_did = "did:plc:verylongparentidentifier";
        let reply_did = "did:plc:reply-author";
        let parent = test_web_post(parent_did, "original", 1, "Original text", None);
        let reply = test_web_post(reply_did, "reply", 2, "Reply text", Some(&parent.uri));
        storage.put_record(&parent).unwrap();
        storage.put_record(&reply).unwrap();
        storage.set_follow(viewer, reply_did, true).unwrap();
        storage
            .set_verified_handle(reply_did, "reply.test")
            .unwrap();
        put_test_profile(&storage, reply_did, "Reply Author");
        let flow = ParentIdentityTestFlow {
            parent_did: parent_did.to_owned(),
            profile_started: Arc::new(tokio::sync::Notify::new()),
            release_profile: Arc::new(tokio::sync::Notify::new()),
        };

        let page = tokio::time::timeout(
            Duration::from_millis(250),
            request_test_timeline_with_flow(storage.clone(), viewer, flow.clone()),
        )
        .await
        .expect("a cache miss for the parent author must not hold the page open");
        assert!(page.contains("class=\"reply-parent-card\""));
        assert!(page.contains("did:…"));
        assert!(!page.contains("@original.cache.test"));

        let worker =
            ProfileRefreshWorker::new(flow.clone(), storage.clone(), Duration::from_millis(1));
        let refresh = tokio::spawn(async move { worker.refresh_once().await });
        tokio::time::timeout(Duration::from_secs(1), flow.profile_started.notified())
            .await
            .expect("queued parent author should reach the existing profile refresh path");
        assert!(!refresh.is_finished());
        flow.release_profile.notify_one();
        assert_eq!(refresh.await.unwrap().unwrap(), 1);
        assert_eq!(
            storage.verified_handle(parent_did).unwrap().as_deref(),
            Some("original.cache.test")
        );

        let refreshed_page = request_test_timeline_with_flow(storage, viewer, flow).await;
        assert!(refreshed_page.contains("@original.cache.test"));
    }

    fn test_web_post(
        did: &str,
        rkey: &str,
        time_us: u64,
        text: &str,
        parent_uri: Option<&str>,
    ) -> IndexedRecord {
        let mut record =
            json!({"$type":"app.bsky.feed.post","text":text,"createdAt":"2026-10-29T12:00:00Z"});
        if let Some(parent_uri) = parent_uri {
            record["reply"] = json!({
                "root":{"uri":parent_uri,"cid":"parent-cid"},
                "parent":{"uri":parent_uri,"cid":"parent-cid"}
            });
        }
        IndexedRecord {
            did: did.to_owned(),
            collection: "app.bsky.feed.post".to_owned(),
            rkey: rkey.to_owned(),
            uri: format!("at://{did}/app.bsky.feed.post/{rkey}"),
            cid: Some(format!("cid-{rkey}")),
            time_us,
            record,
        }
    }

    fn put_test_profile(storage: &MemoryStorage, did: &str, display_name: &str) {
        storage
            .put_record(&IndexedRecord {
                did: did.to_owned(),
                collection: "app.bsky.actor.profile".to_owned(),
                rkey: "self".to_owned(),
                uri: format!("at://{did}/app.bsky.actor.profile/self"),
                cid: None,
                time_us: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_micros() as u64,
                record: json!({"displayName":display_name}),
            })
            .unwrap();
    }

    async fn request_test_timeline(storage: Arc<MemoryStorage>, viewer: &str) -> String {
        request_test_timeline_with_flow(storage, viewer, DisabledOAuthFlow).await
    }

    async fn request_test_timeline_with_flow<F: OAuthFlow>(
        storage: Arc<MemoryStorage>,
        viewer: &str,
        flow: F,
    ) -> String {
        let public = OAuthPublic::new_loopback("127.0.0.1:3000".parse().unwrap()).unwrap();
        let session_id = "av28-timeline-session";
        let generation = storage.rotate_web_session(viewer, session_id).unwrap();
        let token = public.issue_session_cookie(
            viewer,
            session_id,
            generation,
            SessionPermissions {
                write: false,
                feed: false,
                blob: false,
                video: false,
            },
        );
        let app = public.router_with_flow(flow, storage);
        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .uri("/app")
                    .header("cookie", format!("{SESSION_COOKIE}={token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        String::from_utf8(body.to_vec()).unwrap()
    }

    #[derive(Clone)]
    struct ParentIdentityTestFlow {
        parent_did: String,
        profile_started: Arc<tokio::sync::Notify>,
        release_profile: Arc<tokio::sync::Notify>,
    }

    impl OAuthFlow for ParentIdentityTestFlow {
        fn authorize<'a>(
            &'a self,
            _: &'a str,
            _: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<String, String>> + Send + 'a>> {
            Box::pin(async { Err("test flow does not authorize".to_owned()) })
        }

        fn callback<'a>(
            &'a self,
            _: OAuthCallback,
        ) -> Pin<Box<dyn Future<Output = Result<OAuthAccount, String>> + Send + 'a>> {
            Box::pin(async { Err("test flow does not callback".to_owned()) })
        }

        fn logout<'a>(
            &'a self,
            _: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
            Box::pin(async { Ok(()) })
        }

        fn fetch_author_profile<'a>(
            &'a self,
            did: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<FetchedAuthorProfile, String>> + Send + 'a>>
        {
            let did = did.to_owned();
            let parent_did = self.parent_did.clone();
            let profile_started = self.profile_started.clone();
            let release_profile = self.release_profile.clone();
            Box::pin(async move {
                if did == parent_did {
                    profile_started.notify_one();
                    release_profile.notified().await;
                }
                let profile = IndexedRecord {
                    did: did.clone(),
                    collection: "app.bsky.actor.profile".to_owned(),
                    rkey: "self".to_owned(),
                    uri: format!("at://{did}/app.bsky.actor.profile/self"),
                    cid: None,
                    time_us: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_micros() as u64,
                    record: json!({"displayName":"Original Cache Author"}),
                };
                Ok(FetchedAuthorProfile {
                    profile: Some(profile),
                    verified_handle: Some("original.cache.test".to_owned()),
                })
            })
        }
    }

    const HOUR_US: u64 = 60 * 60 * 1_000_000;

    fn web_notice(
        actor: &str,
        reason: &str,
        time_us: u64,
        reason_subject: Option<&str>,
    ) -> IndexedNotification {
        let collection = match reason {
            "like" => "app.bsky.feed.like",
            "repost" => "app.bsky.feed.repost",
            "follow" => "app.bsky.graph.follow",
            _ => "app.bsky.feed.post",
        };
        let subject_uri = reason_subject
            .map(|subject| format!("at://did:plc:viewer/app.bsky.feed.post/{subject}"));
        IndexedNotification {
            record: IndexedRecord {
                did: format!("did:plc:{actor}"),
                collection: collection.to_owned(),
                rkey: actor.to_owned(),
                uri: format!("at://did:plc:{actor}/{collection}/{actor}"),
                cid: None,
                time_us,
                record: match reason {
                    "like" | "repost" => json!({"subject":{"uri":subject_uri}}),
                    "follow" => json!({"subject":"did:plc:viewer"}),
                    _ => json!({"text":"reply body"}),
                },
            },
            reason: reason.to_owned(),
            reason_subject: subject_uri,
        }
    }

    #[derive(Clone)]
    struct FeedTestIdentity(HashMap<String, DidDocument>);

    impl IdentityBackend for FeedTestIdentity {
        async fn did_document(&self, did: &str) -> Result<Option<DidDocument>, ResolutionError> {
            Ok(self.0.get(did).cloned())
        }

        async fn dns_txt_did(&self, _: &str) -> Result<Option<String>, ResolutionError> {
            Ok(None)
        }

        async fn well_known_did(&self, _: &str) -> Result<Option<String>, ResolutionError> {
            Ok(None)
        }
    }

    #[derive(Default)]
    struct FeedTestState {
        auth_calls: usize,
        skeleton_authorizations: Vec<Option<String>>,
        fail_auth: bool,
    }

    #[derive(Clone, Default)]
    struct FeedTestLogs(Arc<Mutex<Vec<u8>>>);

    struct FeedTestLogWriter(FeedTestLogs);

    impl Write for FeedTestLogWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for FeedTestLogs {
        type Writer = FeedTestLogWriter;

        fn make_writer(&'a self) -> Self::Writer {
            FeedTestLogWriter(self.clone())
        }
    }

    async fn feed_test_flow(
        fail_auth: bool,
    ) -> (
        AtriumOAuthFlow<FeedTestIdentity>,
        Arc<Mutex<FeedTestState>>,
        tokio::task::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let pds = format!("http://127.0.0.1:{}", address.port());
        let service_endpoint = format!("{pds}/fg");
        let state = Arc::new(Mutex::new(FeedTestState {
            fail_auth,
            ..FeedTestState::default()
        }));
        let oauth_metadata = json!({
            "issuer":pds,
            "authorization_endpoint":format!("{pds}/authorize"),
            "token_endpoint":format!("{pds}/token"),
            "scopes_supported":["atproto"],
            "response_types_supported":["code"]
        });
        let auth_state = state.clone();
        let skeleton_state = state.clone();
        let app = Router::new()
            .route(
                "/.well-known/oauth-authorization-server",
                get(move || {
                    let metadata = oauth_metadata.clone();
                    async move { Json(metadata) }
                }),
            )
            .route(
                "/xrpc/com.atproto.server.getServiceAuth",
                get(move || {
                    let state = auth_state.clone();
                    async move {
                        let mut state = state.lock().unwrap();
                        state.auth_calls += 1;
                        if state.fail_auth {
                            (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                Json(json!({"error":"Unavailable"})),
                            )
                        } else {
                            (StatusCode::OK, Json(json!({"token":SERVICE_JWT})))
                        }
                    }
                }),
            )
            .route(
                "/xrpc/com.atproto.repo.getRecord",
                get(|uri: Uri| async move {
                    let query = uri.query().unwrap_or_default();
                    if query.contains("collection=app.bsky.feed.generator") {
                        Json(json!({
                            "uri": FEED_URI,
                            "cid": "bafyreav23generator",
                            "value": {"did": FEED_SERVICE_DID, "displayName": "For you"}
                        }))
                        .into_response()
                    } else {
                        StatusCode::NOT_FOUND.into_response()
                    }
                }),
            )
            .route(
                "/fg/xrpc/app.bsky.feed.getFeedSkeleton",
                get(move |uri: Uri, headers: HeaderMap| {
                    let state = skeleton_state.clone();
                    async move {
                        let authorization = headers
                            .get(header::AUTHORIZATION)
                            .and_then(|value| value.to_str().ok())
                            .map(str::to_owned);
                        state
                            .lock()
                            .unwrap()
                            .skeleton_authorizations
                            .push(authorization.clone());
                        let feed = if authorization.as_deref() == Some("Bearer av23-service-jwt") {
                            REAL_POST_URIS
                                .iter()
                                .map(|post| json!({"post":post}))
                                .collect::<Vec<_>>()
                        } else {
                            vec![json!({
                                "post": PLACEHOLDER_URI,
                                "reason": {"$type":"app.bsky.feed.defs#skeletonReasonPin"}
                            })]
                        };
                        let _ = uri;
                        Json(json!({"feed":feed,"cursor":"next"}))
                    }
                }),
            );
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let docs = HashMap::from([
            (
                FEED_CREATOR_DID.to_owned(),
                DidDocument {
                    id: FEED_CREATOR_DID.to_owned(),
                    also_known_as: vec![],
                    verification_method: vec![],
                    service: vec![DidService {
                        id: "#atproto_pds".into(),
                        type_name: "AtprotoPersonalDataServer".into(),
                        service_endpoint: pds.clone(),
                    }],
                },
            ),
            (
                FEED_SERVICE_DID.to_owned(),
                DidDocument {
                    id: FEED_SERVICE_DID.to_owned(),
                    also_known_as: vec![],
                    verification_method: vec![],
                    service: vec![DidService {
                        id: "#bsky_fg".into(),
                        type_name: "BskyFeedGenerator".into(),
                        service_endpoint,
                    }],
                },
            ),
        ]);
        let identity_resolver = Arc::new(IdentityResolver::new(
            FeedTestIdentity(docs),
            Duration::from_secs(30),
        ));
        let public = OAuthPublic::new_loopback("127.0.0.1:3000".parse().unwrap()).unwrap();
        let store = SqliteOAuthStore::open(":memory:").unwrap();
        let viewer_did: Did = FEED_VIEWER_DID.parse().unwrap();
        store
            .set(
                viewer_did.clone(),
                Session {
                    dpop_key: create_key().key,
                    token_set: serde_json::from_value(json!({
                        "iss":pds,
                        "sub":viewer_did,
                        "aud":pds,
                        "scope":"atproto",
                        "refresh_token":null,
                        "access_token":"viewer-access-token",
                        "token_type":"DPoP",
                        "expires_at":null
                    }))
                    .unwrap(),
                },
            )
            .await
            .unwrap();
        let flow = AtriumOAuthFlow::new(
            &public,
            identity_resolver,
            store,
            &pds,
            SafeHttpClient::new().with_http_test_hosts(["127.0.0.1".to_owned()]),
        )
        .unwrap();
        (flow, state, server)
    }

    fn feed_test_storage() -> MemoryStorage {
        let storage = MemoryStorage::default();
        for (uri, text) in std::iter::once((PLACEHOLDER_URI, "This feed requires authentication"))
            .chain(REAL_POST_URIS.iter().copied().zip([
                "personal post one",
                "personal post two",
                "personal post three",
            ]))
        {
            let rkey = uri.rsplit('/').next().unwrap();
            let did = uri
                .strip_prefix("at://")
                .and_then(|rest| rest.split('/').next())
                .unwrap();
            storage
                .put_record(&IndexedRecord {
                    did: did.to_owned(),
                    collection: "app.bsky.feed.post".into(),
                    rkey: rkey.to_owned(),
                    uri: uri.to_owned(),
                    cid: None,
                    time_us: 1,
                    record: json!({"$type":"app.bsky.feed.post","text":text,"createdAt":"2026-10-28T12:00:00Z"}),
                })
                .unwrap();
        }
        storage
    }

    #[tokio::test]
    async fn authenticated_feed_uses_service_jwt_for_single_skeleton_request() {
        let (flow, state, server) = feed_test_flow(false).await;
        let storage = feed_test_storage();
        let page = flow
            .feed_timeline(FEED_VIEWER_DID, FEED_URI, None, &storage)
            .await
            .unwrap();
        let texts = page
            .timeline
            .feed
            .iter()
            .map(|post| post.record.record["text"].as_str().unwrap())
            .collect::<Vec<_>>();
        assert_eq!(
            texts,
            [
                "personal post one",
                "personal post two",
                "personal post three"
            ]
        );
        let state = state.lock().unwrap();
        assert_eq!(state.skeleton_authorizations.len(), 1);
        assert_eq!(
            state.skeleton_authorizations[0].as_deref(),
            Some("Bearer av23-service-jwt")
        );
        assert_eq!(state.auth_calls, 1);
        server.abort();
    }

    #[tokio::test]
    async fn service_auth_token_is_reused_on_next_feed_page() {
        let (flow, state, server) = feed_test_flow(false).await;
        let storage = feed_test_storage();
        flow.feed_timeline(FEED_VIEWER_DID, FEED_URI, None, &storage)
            .await
            .unwrap();
        flow.feed_timeline(FEED_VIEWER_DID, FEED_URI, Some("next"), &storage)
            .await
            .unwrap();
        let state = state.lock().unwrap();
        assert_eq!(state.auth_calls, 1);
        assert_eq!(state.skeleton_authorizations.len(), 2);
        assert!(
            state
                .skeleton_authorizations
                .iter()
                .all(|authorization| authorization.as_deref() == Some("Bearer av23-service-jwt"))
        );
        server.abort();
    }

    #[tokio::test]
    async fn failed_service_auth_falls_back_anonymously_and_warns_once_per_feed() {
        let (flow, state, server) = feed_test_flow(true).await;
        let storage = feed_test_storage();
        let logs = FeedTestLogs::default();
        for cursor in [None, Some("next")] {
            let subscriber = tracing_subscriber::fmt()
                .without_time()
                .with_ansi(false)
                .with_writer(logs.clone())
                .finish();
            let page = flow
                .feed_timeline(FEED_VIEWER_DID, FAILURE_FEED_URI, cursor, &storage)
                .with_subscriber(subscriber)
                .await
                .unwrap();
            assert_eq!(page.timeline.feed.len(), 1);
        }
        let output = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
        assert_eq!(
            output
                .matches("feed service auth unavailable; requesting skeleton anonymously")
                .count(),
            1
        );
        let state = state.lock().unwrap();
        assert_eq!(state.auth_calls, 2);
        assert_eq!(state.skeleton_authorizations.len(), 2);
        assert!(state.skeleton_authorizations.iter().all(Option::is_none));
        server.abort();
    }

    #[tokio::test]
    async fn logged_out_feed_is_anonymous_without_service_auth() {
        let (flow, state, server) = feed_test_flow(false).await;
        let storage = feed_test_storage();
        let page = flow
            .feed_timeline("", FEED_URI, None, &storage)
            .await
            .unwrap();
        assert_eq!(page.timeline.feed.len(), 1);
        let state = state.lock().unwrap();
        assert_eq!(state.auth_calls, 0);
        assert_eq!(state.skeleton_authorizations.as_slice(), [None]);
        server.abort();
    }

    // HTTP/2 browsers may send each cookie as its own `cookie` header; the
    // sign-in cookie was missed when another cookie came first (2026-10-28).
    #[tokio::test]
    async fn login_page_prefills_the_remembered_handle_safely() {
        let mut headers = HeaderMap::new();
        headers.append(
            header::COOKIE,
            "appview_handle=owner.example.test".parse().unwrap(),
        );
        let Html(body) = login(headers).await;
        assert!(body.contains("value=\"owner.example.test\""), "{body}");

        let mut headers = HeaderMap::new();
        headers.append(
            header::COOKIE,
            "appview_handle=x\"onfocus=alert(1)".parse().unwrap(),
        );
        let Html(body) = login(headers).await;
        assert!(!body.contains("onfocus"), "{body}");
    }

    #[test]
    fn remembered_handle_cookie_only_for_handle_shaped_values() {
        assert!(
            remember_handle_cookie("owner.example.test", true)
                .unwrap()
                .contains("Max-Age=31536000; Secure")
        );
        assert!(remember_handle_cookie("did:plc:abc123", false).is_some());
        assert!(remember_handle_cookie("a;b", true).is_none());
    }

    #[test]
    fn cookie_lookup_reads_every_cookie_header() {
        let mut headers = HeaderMap::new();
        headers.append(header::COOKIE, "appview_session=abc".parse().unwrap());
        headers.append(
            header::COOKIE,
            "other=1; appview_login=xyz".parse().unwrap(),
        );
        assert_eq!(cookie_value_named(&headers, "appview_login"), Some("xyz"));
        assert_eq!(cookie_value_named(&headers, "appview_session"), Some("abc"));
    }

    fn signed_cookie(public: &OAuthPublic, domain: &str, value: &str) -> String {
        let payload = URL_SAFE_NO_PAD.encode(value.as_bytes());
        let mut mac = HmacSha256::new_from_slice(&public.cookie_key()).unwrap();
        mac.update(domain.as_bytes());
        mac.update(payload.as_bytes());
        format!(
            "{payload}.{}",
            URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes())
        )
    }

    #[test]
    fn cookie_hmac_key_is_distinct_from_the_oauth_signing_scalar() {
        let public = OAuthPublic::new("https://appview.example").unwrap();
        let serialized = serde_json::to_value(public.private_key()).unwrap();
        let signing_scalar = URL_SAFE_NO_PAD
            .decode(serialized.get("d").and_then(Value::as_str).unwrap())
            .unwrap();
        assert_ne!(public.cookie_key(), signing_scalar);
    }

    #[test]
    fn expired_login_cookie_is_rejected() {
        let public = OAuthPublic::new("https://appview.example").unwrap();
        let cookie = signed_cookie(
            &public,
            "login:",
            &format!("binding\n{}", unix_time().saturating_sub(1)),
        );
        assert!(public.verify_login_cookie(&cookie).is_none());
    }

    #[test]
    fn expired_session_cookie_is_rejected() {
        let public = OAuthPublic::new("https://appview.example").unwrap();
        let cookie = signed_cookie(
            &public,
            "session:",
            &format!(
                "did:plc:alice\n{}\nsession-one\n1",
                unix_time().saturating_sub(1)
            ),
        );
        assert!(public.verify_session_cookie(&cookie).is_none());
    }
}

#[cfg(not(unix))]
fn create_key_file(path: &Path) -> Result<fs::File, std::io::Error> {
    fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
}
