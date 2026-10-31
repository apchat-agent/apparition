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
    net::{
        OutboundCallSnapshot, OutboundCallTimer, RequestOutboundAccounting, SafeHttpClient,
        current_outbound_call_snapshot, with_request_outbound_accounting,
    },
    profile_counts::{
        LABELLED_APPVIEW_BASE, PUBLIC_APPVIEW_BASE, ProfileCountsClient,
        RELATIONSHIP_CONFIRMATION_TTL, RelationshipClient, apply_profile_counts, now_millis,
        relationship_cache_key,
    },
    timing_console::{
        TIMING_PAGE_LIMIT, TIMING_RECENT_PATH, TimingConsole, TimingRecord, new_timing_id,
        now_unix_ms,
    },
};
use atrium_api::{
    agent::{Agent, CloneWithProxy, Configure, SessionManager},
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
    Error as XrpcClientError, HttpClient, InputDataOrBytes, OutputDataOrBytes, XrpcClient,
    XrpcRequest,
};
use axum::{
    Json, Router,
    body::{Body, Bytes},
    extract::{
        DefaultBodyLimit, Form, FromRequest, Multipart, OriginalUri, Path as AxumPath, Query,
        Request, State,
    },
    http::{HeaderMap, HeaderName, HeaderValue, StatusCode, header},
    middleware::{Next, from_fn, from_fn_with_state},
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
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};
use std::{
    collections::{HashMap, HashSet},
    fs,
    future::Future,
    io::Write,
    net::SocketAddr,
    path::{Path, PathBuf},
    pin::Pin,
    sync::mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TryRecvError},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
    thread::JoinHandle,
};
use thiserror::Error;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};
use tokio::{
    io::AsyncWriteExt,
    sync::{OnceCell, oneshot},
};
use tracing::instrument::WithSubscriber;

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
/// How long a spawned block-list refresh may run. The page waits only PAGE_PROFILE_BUDGET for it,
/// but the refresh itself must be allowed to finish: a 742-member list is ~8 getList pages (~4 s),
/// and cutting it at the page budget meant it never filled the cache (2026-10-30).
const BLOCK_LIST_FETCH_TIMEOUT: Duration = Duration::from_secs(60);
const PAGE_PROFILE_CONCURRENCY: usize = 8;
const PAGE_QUOTE_CONCURRENCY: usize = 16;
const OAUTH_STORE_WRITER_QUEUE: usize = 128;
const OAUTH_STORE_WRITER_BATCH_SIZE: usize = 64;
const OAUTH_STORE_WRITER_BATCH_DELAY: Duration = Duration::from_millis(5);
const OAUTH_STORE_WRITER_METRICS_INTERVAL: Duration = Duration::from_secs(60);
static NEXT_OAUTH_MEMORY_DATABASE_ID: AtomicUsize = AtomicUsize::new(1);
const WEB_STYLESHEET_URL: &str = "/static/app.css?v=m40";
const WEB_SCRIPT_URL: &str = "/static/app.js?v=js24";
const WEB_NOJS_STYLESHEET_URL: &str = "/static/nojs.css?v=m40";
const HLS_SCRIPT_URL: &str = "/static/vendor/hls.min.js?v=1.7.3";
const IMMUTABLE_CACHE: &str = "public, max-age=31536000, immutable";
const SAVED_FEED_CACHE_PREFIX: &str = "saved-feed-preferences:";
const BSKY_MODERATION_LABELER_DID: &str = "did:plc:ar7c4by46qjdydhdevvrndac";
const ACCOUNT_LABEL_PROFILE_BATCH_SIZE: usize = 25;
const ACCOUNT_LABEL_CACHE_TTL: Duration = Duration::from_secs(5 * 60);
const LABELER_PREFERENCE_CACHE_TTL: Duration = Duration::from_secs(5 * 60);
const LABELER_PRESENTATION_CACHE_TTL: Duration = Duration::from_secs(6 * 60 * 60);
const SAVED_FEED_PREFERENCE_CACHE_TTL: Duration = Duration::from_secs(5 * 60);
const OAUTH_SESSION_CACHE_TTL: Duration = Duration::from_secs(30 * 60);
const OAUTH_SESSION_CACHE_CAPACITY: usize = 1024;
const OAUTH_METADATA_CACHE_TTL: Duration = Duration::from_secs(6 * 60 * 60);
const OAUTH_METADATA_RESOLVER_CACHE_TTL: Duration = Duration::from_secs(1);
const VIDEO_SERVICE_DID: &str = "did:web:video.bsky.app";
const VIDEO_SERVICE_URL: &str = "https://video.bsky.app";
const REQUESTED_SCOPE: &str = "atproto repo:app.bsky.feed.post?action=create repo:app.bsky.feed.like?action=create&action=delete repo:app.bsky.feed.repost?action=create&action=delete blob:image/* rpc:app.bsky.actor.getPreferences?aud=* rpc:app.bsky.feed.getFeedSkeleton?aud=* blob:video/* rpc:app.bsky.video.getUploadLimits?aud=* rpc:com.atproto.repo.uploadBlob?aud=* repo:app.bsky.graph.follow?action=create&action=delete repo:app.bsky.graph.block?action=create&action=delete repo:app.bsky.graph.listitem?action=create&action=delete rpc:app.bsky.graph.muteActor?aud=* rpc:app.bsky.graph.unmuteActor?aud=* rpc:app.bsky.graph.getMutes?aud=* rpc:app.bsky.actor.putPreferences?aud=did:web:api.bsky.app%23bsky_appview";
const BLOB_SCOPE: &str = "blob:image/*";
const VIDEO_BLOB_SCOPE: &str = "blob:video/*";
const VIDEO_SCOPES: [&str; 3] = [
    VIDEO_BLOB_SCOPE,
    "rpc:app.bsky.video.getUploadLimits?aud=*",
    "rpc:com.atproto.repo.uploadBlob?aud=*",
];
const FOLLOW_SCOPES: [&str; 1] = ["repo:app.bsky.graph.follow?action=create&action=delete"];
const BLOCK_SCOPES: [&str; 1] = ["repo:app.bsky.graph.block?action=create&action=delete"];
const LIST_SCOPES: [&str; 1] = ["repo:app.bsky.graph.listitem?action=create&action=delete"];
const MUTE_SCOPES: [&str; 3] = [
    "rpc:app.bsky.graph.muteActor?aud=*",
    "rpc:app.bsky.graph.unmuteActor?aud=*",
    "rpc:app.bsky.graph.getMutes?aud=*",
];
/// Removing a dead saved feed rewrites the viewer's preferences (owner, 2026-10-31).
/// The audience must be named exactly: bsky.social grants `aud=*` for putPreferences but its PDS
/// then rejects the call with ScopeMissingError naming this audience (measured 2026-10-31).
const PREFERENCES_SCOPES: [&str; 1] =
    ["rpc:app.bsky.actor.putPreferences?aud=did:web:api.bsky.app%23bsky_appview"];
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

fn muted_actor_cache_key(did: &str) -> String {
    format!("{MUTED_ACTOR_CACHE_PREFIX}{did}")
}

fn listblock_cache_key(did: &str) -> String {
    format!("{LISTBLOCK_CACHE_PREFIX}{did}")
}

fn list_members_cache_key(list_uri: &str) -> String {
    format!("{LIST_MEMBERS_CACHE_PREFIX}{list_uri}")
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
    login_allowlist: Option<Arc<HashSet<String>>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct SessionPermissions {
    write: bool,
    feed: bool,
    blob: bool,
    video: bool,
    follow: bool,
    block: bool,
    mute: bool,
    list: bool,
    /// Position 8 in the cookie: cookies issued before it existed read as false, so those users
    /// stay signed in and are asked to sign in again only when they try to remove a feed.
    preferences: bool,
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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ProfileListChoice {
    pub uri: String,
    pub name: String,
    pub purpose: String,
    pub item_uri: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct AccountLabel {
    pub subject_did: String,
    pub labeler_did: String,
    pub labeler_name: String,
    pub labeler_handle: String,
    pub value: String,
    pub display_name: String,
    pub description: String,
    pub severity: String,
    pub blurs: String,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct PageAccountData {
    pub labels: Vec<AccountLabel>,
    pub profiles: Vec<crate::feeds::PublicProfileView>,
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

fn service_auth_audience_from_url(value: &str) -> Result<String, String> {
    let url = reqwest::Url::parse(value)
        .map_err(|_| "PDS URL could not be parsed for service authorization".to_owned())?;
    if !matches!(url.scheme(), "http" | "https") {
        return Err("PDS URL has an unsupported scheme for service authorization".to_owned());
    }
    let hostname = url
        .host_str()
        .filter(|hostname| !hostname.is_empty())
        .ok_or_else(|| "PDS URL has no host for service authorization".to_owned())?;
    Ok(format!("did:web:{hostname}"))
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

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct BlockListDetail {
    pub uri: String,
    pub name: String,
    pub member_dids: Vec<String>,
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
    fn fetch_public_posts<'a>(
        &'a self,
        uris: &'a [String],
    ) -> Pin<Box<dyn Future<Output = HashMap<String, crate::feeds::FetchedPublicPost>> + Send + 'a>>
    {
        Box::pin(async move {
            futures_util::stream::iter(uris.iter().cloned().map(|uri| async move {
                self.fetch_post(&uri).await.ok().map(|record| {
                    (
                        uri,
                        crate::feeds::FetchedPublicPost {
                            record,
                            author: None,
                        },
                    )
                })
            }))
            .buffer_unordered(PAGE_QUOTE_CONCURRENCY)
            .filter_map(|post| async move { post })
            .collect::<HashMap<_, _>>()
            .await
        })
    }
    fn supports_public_post_batching(&self) -> bool {
        false
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
    fn create_profile_graph_record<'a>(
        &'a self,
        _did: &'a str,
        _collection: &'a str,
        _record: Value,
    ) -> Pin<Box<dyn Future<Output = Result<PdsCreatedRecord, String>> + Send + 'a>> {
        Box::pin(async { Err("profile graph action is not available".to_owned()) })
    }
    fn delete_profile_graph_record<'a>(
        &'a self,
        _did: &'a str,
        _collection: &'a str,
        _uri: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async { Err("profile graph action is not available".to_owned()) })
    }
    fn set_actor_muted<'a>(
        &'a self,
        _did: &'a str,
        _subject: &'a str,
        _muted: bool,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async { Err("mute action is not available".to_owned()) })
    }
    /// Removes a saved feed from the viewer's Bluesky preferences (used for feeds whose creator
    /// deleted them, owner 2026-10-31).
    fn remove_saved_feed<'a>(
        &'a self,
        _did: &'a str,
        _feed_uri: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async { Err("removing saved feeds is not available".to_owned()) })
    }
    fn muted_actor_dids<'a>(
        &'a self,
        _did: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<HashSet<String>, String>> + Send + 'a>> {
        Box::pin(async { Ok(HashSet::new()) })
    }
    /// The at-uris of the lists the viewer subscribed to as block lists
    /// (`app.bsky.graph.listblock` subjects). Uncached raw fetch.
    fn subscribed_block_list_uris<'a>(
        &'a self,
        _did: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<String>, String>> + Send + 'a>> {
        Box::pin(async { Ok(Vec::new()) })
    }
    /// One block list's display name and member DIDs. Uncached raw fetch; the
    /// list usually belongs to another account, so it must not use the viewer's session.
    fn block_list_detail<'a>(
        &'a self,
        _list_uri: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<BlockListDetail, String>> + Send + 'a>> {
        Box::pin(async { Err("block list lookup is not available".to_owned()) })
    }
    /// Handle for the persistent web cache used to keep the block-list caches
    /// across restarts; `None` keeps a flow's block-list cache memory-only.
    fn web_cache_store(&self) -> Option<SqliteOAuthStore> {
        None
    }
    fn profile_list_choices<'a>(
        &'a self,
        _did: &'a str,
        _subject: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ProfileListChoice>, String>> + Send + 'a>> {
        Box::pin(async { Ok(Vec::new()) })
    }
    fn account_labels_for_page<'a>(
        &'a self,
        _viewer_did: &'a str,
        _actor_dids: Vec<String>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<AccountLabel>, String>> + Send + 'a>> {
        Box::pin(async { Ok(Vec::new()) })
    }
    fn page_account_data_for_page<'a>(
        &'a self,
        viewer_did: &'a str,
        actor_dids: Vec<String>,
    ) -> Pin<Box<dyn Future<Output = Result<PageAccountData, String>> + Send + 'a>> {
        Box::pin(async move {
            let labels = self.account_labels_for_page(viewer_did, actor_dids).await?;
            Ok(PageAccountData {
                labels,
                profiles: Vec::new(),
            })
        })
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
                    unavailable: false,
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
    profile_counts: ProfileCountsClient,
    relationships: RelationshipClient,
    link_card_fetcher: LinkCardFetcher,
    feed_page_cache: FeedPageCache,
    block_lists: Arc<BlockListCache>,
    notification_streams: Arc<Mutex<HashMap<String, usize>>>,
    timing_console: Arc<TimingConsole>,
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

    /// Limit sign-in to these already-resolved account DIDs. An empty set keeps
    /// the default open sign-in behavior.
    pub fn with_login_allowlist<I, D>(mut self, allowed_dids: I) -> Self
    where
        I: IntoIterator<Item = D>,
        D: Into<String>,
    {
        let allowed_dids = allowed_dids
            .into_iter()
            .map(Into::into)
            .collect::<HashSet<_>>();
        self.login_allowlist = (!allowed_dids.is_empty()).then(|| Arc::new(allowed_dids));
        self
    }

    fn login_is_allowed(&self, did: &str) -> bool {
        self.login_allowlist
            .as_ref()
            .is_none_or(|allowed_dids| allowed_dids.contains(did))
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
        self.router_with_flow_and_shutdown(flow, storage, WebShutdown::new())
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
        self.router_with_flow_and_shutdown_and_profile_counts(
            flow,
            storage,
            shutdown,
            ProfileCountsClient::local_only(),
        )
    }

    pub fn router_with_flow_and_shutdown_and_profile_counts<F, S>(
        &self,
        flow: F,
        storage: Arc<S>,
        shutdown: WebShutdown,
        profile_counts: ProfileCountsClient,
    ) -> Router
    where
        F: OAuthFlow,
        S: Storage + 'static,
    {
        self.router_with_flow_and_shutdown_and_profile_counts_and_relationships(
            flow,
            storage,
            shutdown,
            profile_counts,
            RelationshipClient::local_only(),
        )
    }

    pub fn router_with_flow_and_shutdown_and_profile_counts_and_relationships<F, S>(
        &self,
        flow: F,
        storage: Arc<S>,
        shutdown: WebShutdown,
        profile_counts: ProfileCountsClient,
        relationships: RelationshipClient,
    ) -> Router
    where
        F: OAuthFlow,
        S: Storage + 'static,
    {
        self.router_with_timing_console(
            flow,
            storage,
            shutdown,
            profile_counts,
            relationships,
            Arc::new(TimingConsole::default()),
        )
    }

    /// Builds the web router with a caller-supplied timing history, so tests can inspect
    /// the ring and drive its once-a-minute metrics line.
    #[cfg(test)]
    pub(crate) fn router_with_flow_and_timing_console<F, S>(
        &self,
        flow: F,
        storage: Arc<S>,
        timing_console: Arc<TimingConsole>,
    ) -> Router
    where
        F: OAuthFlow,
        S: Storage + 'static,
    {
        self.router_with_timing_console(
            flow,
            storage,
            WebShutdown::new(),
            ProfileCountsClient::local_only(),
            RelationshipClient::local_only(),
            timing_console,
        )
    }

    fn router_with_timing_console<F, S>(
        &self,
        flow: F,
        storage: Arc<S>,
        shutdown: WebShutdown,
        profile_counts: ProfileCountsClient,
        relationships: RelationshipClient,
        timing_console: Arc<TimingConsole>,
    ) -> Router
    where
        F: OAuthFlow,
        S: Storage + 'static,
    {
        let state = Arc::new(OAuthWebState {
            public: self.clone(),
            flow,
            storage,
            profile_counts,
            relationships,
            link_card_fetcher: LinkCardFetcher::new(),
            shutdown,
            feed_page_cache: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            block_lists: Arc::new(BlockListCache::default()),
            notification_streams: Arc::new(Mutex::new(HashMap::new())),
            timing_console,
        });
        let regular_routes = Router::new()
            .route(METADATA_PATH, get(metadata::<F, S>))
            .route("/static/app.css", get(app_stylesheet))
            .route("/static/nojs.css", get(nojs_stylesheet))
            .route("/static/app.js", get(app_script))
            .route("/static/vendor/{*path}", get(vendor_asset))
            .route("/", get(root))
            .route("/login", get(login))
            .route("/oauth/authorize", post(authorize::<F, S>))
            .route("/oauth/callback", get(callback::<F, S>))
            .route("/logout", post(logout::<F, S>))
            .route("/app", get(app_home::<F, S>))
            .route("/feeds", get(feeds_page::<F, S>))
            .route("/app/block-lists", get(block_lists_page::<F, S>))
            .route("/app/feed", get(feed_timeline_page::<F, S>))
            .route("/app/thread", get(thread_page::<F, S>))
            .route("/quote-fragment", get(quote_fragment::<F, S>))
            .route("/profile-fragment", get(profile_fragment::<F, S>))
            .route("/notifications", get(notifications_page::<F, S>))
            .route("/timing/recent", get(timing_recent::<F, S>))
            .route(
                "/notifications/unread-count",
                get(notification_unread_count::<F, S>),
            )
            .route("/notifications/stream", get(notification_stream::<F, S>))
            .route("/profile", get(profile_page::<F, S>))
            .route("/profile/block-confirm", get(profile_block_confirm::<F, S>))
            .route("/profile/action", post(profile_action::<F, S>))
            .route("/thread", get(thread_page::<F, S>))
            .route("/compose", get(quote_composer::<F, S>))
            .route("/link-preview", post(link_preview::<F, S>))
            .route("/post", post(write_post::<F, S>))
            .route("/reply", post(write_reply::<F, S>))
            .route("/quote", post(write_quote::<F, S>))
            .route("/like", post(write_like::<F, S>))
            .route("/repost", post(write_repost::<F, S>))
            .route("/feeds/remove", post(remove_saved_feed::<F, S>))
            .layer(DefaultBodyLimit::max(MAX_POST_BODY_BYTES))
            .layer(from_fn(log_web_post_client_errors))
            .layer(from_fn_with_state(
                state.clone(),
                account_web_outbound_calls::<F, S>,
            ))
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
            login_allowlist: None,
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
            login_allowlist: None,
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
            "{did}\n{expires}\n{session_id}\n{generation}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}\n{}",
            permissions.write,
            permissions.feed,
            permissions.blob,
            permissions.video,
            permissions.follow,
            permissions.block,
            permissions.mute,
            permissions.list,
            permissions.preferences
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
    ) -> Option<(String, String, u64, SessionPermissions)> {
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
        let (generation, permission_values) = generation.split_once('\n')?;
        let permission_values = permission_values.split('\n').collect::<Vec<_>>();
        let permission = |index: usize| {
            permission_values
                .get(index)
                .and_then(|value| value.parse().ok())
                .unwrap_or(false)
        };
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
            SessionPermissions {
                write: permission(0),
                feed: permission(1),
                blob: permission(2),
                video: permission(3),
                follow: permission(4),
                block: permission(5),
                mute: permission(6),
                list: permission(7),
                preferences: permission(8),
            },
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
    OAuthMetadataHttpClient,
>;
type RestoredOAuthSession = atrium_oauth::OAuthSession<
    OAuthMetadataHttpClient,
    OAuthDidResolver,
    OAuthHandleResolver,
    SqliteOAuthStore,
>;
type OAuthSessionCache =
    Arc<tokio::sync::Mutex<HashMap<String, (tokio::time::Instant, Arc<RestoredOAuthSession>)>>>;
type OAuthSessionRestores = Arc<
    tokio::sync::Mutex<HashMap<String, Arc<OnceCell<Result<Arc<RestoredOAuthSession>, String>>>>>,
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
type MutedActorCache =
    Arc<tokio::sync::Mutex<HashMap<String, (tokio::time::Instant, HashSet<String>)>>>;
type MutedActorRefreshes = Arc<tokio::sync::Mutex<HashSet<String>>>;
type LabelerPreferenceCache =
    Arc<tokio::sync::Mutex<HashMap<String, (tokio::time::Instant, Vec<String>)>>>;
type LabelerPreferenceRefreshes = Arc<tokio::sync::Mutex<HashSet<String>>>;
type AccountLabelActorCache =
    Arc<tokio::sync::Mutex<HashMap<(String, String), (tokio::time::Instant, AccountActorData)>>>;
type AccountLabelRefreshes = Arc<tokio::sync::Mutex<HashSet<(String, String)>>>;
type LabelerPresentationCache =
    Arc<tokio::sync::Mutex<HashMap<String, (tokio::time::Instant, LabelerPresentation)>>>;
type OAuthMetadataColdRefreshes = Arc<
    tokio::sync::Mutex<HashMap<String, Arc<OnceCell<Result<CachedOAuthMetadataResponse, String>>>>>,
>;

#[derive(Clone)]
struct CachedOAuthMetadataResponse {
    expires: tokio::time::Instant,
    status: StatusCode,
    headers: HeaderMap,
    body: Vec<u8>,
}

#[derive(Clone)]
struct OAuthMetadataHttpClient {
    inner: SafeHttpClient,
    cache: Arc<tokio::sync::Mutex<HashMap<String, CachedOAuthMetadataResponse>>>,
    refreshes: Arc<tokio::sync::Mutex<HashSet<String>>>,
    cold_refreshes: OAuthMetadataColdRefreshes,
    writer_metrics: Arc<OAuthWriterMetrics>,
}

impl OAuthMetadataHttpClient {
    fn new(inner: SafeHttpClient) -> Self {
        Self {
            inner,
            cache: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            refreshes: Arc::new(tokio::sync::Mutex::new(HashSet::new())),
            cold_refreshes: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            writer_metrics: Arc::new(OAuthWriterMetrics::default()),
        }
    }

    fn with_writer_metrics(mut self, metrics: Arc<OAuthWriterMetrics>) -> Self {
        self.writer_metrics = metrics;
        self
    }

    fn metadata_key(request: &http::Request<Vec<u8>>) -> Option<String> {
        let path = request.uri().path();
        (request.method() == http::Method::GET
            && (path.ends_with("/.well-known/oauth-authorization-server")
                || path.ends_with("/.well-known/oauth-protected-resource")))
        .then(|| format!("{} {}", request.method(), request.uri()))
    }

    fn build_response(cached: CachedOAuthMetadataResponse) -> http::Response<Vec<u8>> {
        let mut response = http::Response::builder()
            .status(cached.status)
            .body(cached.body)
            .expect("cached OAuth metadata response has a valid status");
        *response.headers_mut() = cached.headers;
        response
    }

    async fn schedule_refresh(&self, key: String, request: http::Request<Vec<u8>>) {
        let mut refreshes = self.refreshes.lock().await;
        if !refreshes.insert(key.clone()) {
            return;
        }
        drop(refreshes);
        let client = self.inner.clone().background();
        let cache = self.cache.clone();
        let refreshes = self.refreshes.clone();
        tokio::spawn(async move {
            if let Ok(response) = client.send_http(request).await
                && response.status().is_success()
            {
                let (parts, body) = response.into_parts();
                let mut cache = cache.lock().await;
                cache.retain(|_, entry| entry.expires > tokio::time::Instant::now());
                if cache.len() >= 256
                    && !cache.contains_key(&key)
                    && let Some(oldest) = cache.keys().next().cloned()
                {
                    cache.remove(&oldest);
                }
                cache.insert(
                    key.clone(),
                    CachedOAuthMetadataResponse {
                        expires: tokio::time::Instant::now() + OAUTH_METADATA_CACHE_TTL,
                        status: parts.status,
                        headers: parts.headers,
                        body,
                    },
                );
            }
            refreshes.lock().await.remove(&key);
        });
    }
}

impl HttpClient for OAuthMetadataHttpClient {
    async fn send_http(
        &self,
        request: http::Request<Vec<u8>>,
    ) -> Result<http::Response<Vec<u8>>, Box<dyn std::error::Error + Send + Sync + 'static>> {
        let token_issuer = (request.method() == http::Method::POST
            && request
                .body()
                .windows(b"grant_type=".len())
                .any(|w| w == b"grant_type="))
        .then(|| {
            let refresh = request
                .body()
                .windows(b"grant_type=refresh_token".len())
                .any(|w| w == b"grant_type=refresh_token");
            let issuer = match (request.uri().scheme_str(), request.uri().authority()) {
                (Some(scheme), Some(authority)) => format!("{scheme}://{authority}"),
                _ => request.uri().to_string(),
            };
            (issuer, refresh)
        });
        if let Some((issuer, refresh)) = token_issuer {
            let response = self.inner.send_http(request).await?;
            if !response.status().is_success() {
                // Atrium swallows a failed token response inside refresh_token(), so this is
                // the only place the authorization server's own reason can be logged.
                let body = String::from_utf8_lossy(response.body()).to_string();
                let parsed: serde_json::Value =
                    serde_json::from_str(&body).unwrap_or(serde_json::Value::Null);
                let error = parsed
                    .get("error")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("unparsable response");
                let description = parsed
                    .get("error_description")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or("");
                let attempt = OAUTH_TOKEN_REQUEST
                    .try_with(Arc::clone)
                    .unwrap_or_else(|_| {
                        Arc::new(OAuthTokenAttempt {
                            did: "-".to_owned(),
                            grant_rejected: AtomicBool::new(false),
                        })
                    });
                if error == "invalid_grant" {
                    attempt.grant_rejected.store(true, Ordering::Relaxed);
                }
                let did = attempt.did.clone();
                tracing::warn!(
                    did = %did,
                    issuer = %issuer,
                    refresh,
                    status = response.status().as_u16(),
                    error = %error,
                    error_description = %description,
                    body = %body.chars().take(512).collect::<String>(),
                    "OAuth token endpoint rejected the request"
                );
                if refresh {
                    self.writer_metrics.record_refresh_failure();
                }
            }
            return Ok(response);
        }
        let Some(key) = Self::metadata_key(&request) else {
            return self.inner.send_http(request).await;
        };
        if let Some(cached) = self.cache.lock().await.get(&key).cloned() {
            if cached.expires > tokio::time::Instant::now() {
                return Ok(Self::build_response(cached));
            }
            self.schedule_refresh(key, request).await;
            return Ok(Self::build_response(cached));
        }

        let refresh = {
            let mut pending = self.cold_refreshes.lock().await;
            pending
                .entry(key.clone())
                .or_insert_with(|| Arc::new(OnceCell::new()))
                .clone()
        };
        let result = refresh
            .get_or_init(|| async {
                let response = self
                    .inner
                    .send_http(request)
                    .await
                    .map_err(|error| error.to_string())?;
                let (parts, body) = response.into_parts();
                let cached = CachedOAuthMetadataResponse {
                    expires: tokio::time::Instant::now() + OAUTH_METADATA_CACHE_TTL,
                    status: parts.status,
                    headers: parts.headers.clone(),
                    body,
                };
                if parts.status.is_success() {
                    let mut cache = self.cache.lock().await;
                    if cache.len() >= 256
                        && !cache.contains_key(&key)
                        && let Some(oldest) = cache.keys().next().cloned()
                    {
                        cache.remove(&oldest);
                    }
                    cache.insert(key.clone(), cached.clone());
                }
                Ok(cached)
            })
            .await
            .clone();
        self.cold_refreshes.lock().await.remove(&key);
        match result {
            Ok(cached) => Ok(Self::build_response(cached)),
            Err(error) => Err(Box::new(std::io::Error::other(error))),
        }
    }
}

/// Whose token request this is, and whether the authorization server said `invalid_grant`
/// (the refresh token itself is dead, so no retry can revive the session).
struct OAuthTokenAttempt {
    did: String,
    grant_rejected: AtomicBool,
}

tokio::task_local! {
    static OAUTH_TOKEN_REQUEST: Arc<OAuthTokenAttempt>;
}

tokio::task_local! {
    static OAUTH_SESSION_DEAD_DID: Arc<Mutex<Option<String>>>;
}

fn mark_oauth_session_dead(did: &str) {
    let _ = OAUTH_SESSION_DEAD_DID.try_with(|slot| {
        if let Ok(mut slot) = slot.lock() {
            *slot = Some(did.to_owned());
        }
    });
}

fn take_dead_oauth_session() -> Option<String> {
    OAUTH_SESSION_DEAD_DID
        .try_with(|slot| slot.lock().ok().and_then(|mut slot| slot.take()))
        .unwrap_or(None)
}

/// The user's Bluesky sign-in is dead: clear this app's cookie, send them to sign in again,
/// and tell fetch callers with 401 SessionExpired instead of a redirect they cannot read.
fn session_expired_response(public: &OAuthPublic, headers: &HeaderMap) -> Response {
    const MESSAGE: &str = "Your Bluesky sign-in expired; please sign in again.";
    if headers
        .get("x-appview-fetch")
        .is_some_and(|value| value.as_bytes() == b"1")
    {
        return (
            StatusCode::UNAUTHORIZED,
            Json(json!({"error":"SessionExpired","message":MESSAGE})),
        )
            .into_response();
    }
    clear_session_cookie(
        Redirect::to("/login?session_expired=1").into_response(),
        public.secure_cookies(),
    )
}

#[derive(Clone)]
struct SharedOAuthSession {
    inner: Arc<RestoredOAuthSession>,
    did: String,
    store: SqliteOAuthStore,
    cache: OAuthSessionCache,
    restores: OAuthSessionRestores,
}

impl HttpClient for SharedOAuthSession {
    async fn send_http(
        &self,
        request: http::Request<Vec<u8>>,
    ) -> Result<http::Response<Vec<u8>>, Box<dyn std::error::Error + Send + Sync + 'static>> {
        self.inner.send_http(request).await
    }
}

impl XrpcClient for SharedOAuthSession {
    fn base_uri(&self) -> String {
        self.inner.base_uri()
    }

    async fn send_xrpc<P, I, O, E>(
        &self,
        request: &XrpcRequest<P, I>,
    ) -> Result<OutputDataOrBytes<O>, XrpcClientError<E>>
    where
        P: Serialize + Send + Sync,
        I: Serialize + Send + Sync,
        O: serde::de::DeserializeOwned + Send + Sync,
        E: serde::de::DeserializeOwned + Send + Sync + std::fmt::Debug,
    {
        let attempt = Arc::new(OAuthTokenAttempt {
            did: self.did.clone(),
            grant_rejected: AtomicBool::new(false),
        });
        let result = OAUTH_TOKEN_REQUEST
            .scope(attempt.clone(), self.inner.send_xrpc(request))
            .await;
        if let Err(error) = &result
            && format!("{error:?}")
                .to_ascii_lowercase()
                .contains("invalid_token")
        {
            // The PDS rejected this access token. Drop the cached session, mark the stored
            // session expired so the OAuth client's registry really refreshes it, and retry
            // once: the first call after a revocation must recover with the new token instead
            // of handing the stale one back to the caller.
            self.cache.lock().await.remove(&self.did);
            self.restores.lock().await.remove(&self.did);
            if self.expire_stored_session().await {
                let retry = OAUTH_TOKEN_REQUEST
                    .scope(attempt.clone(), self.inner.send_xrpc(request))
                    .await;
                if retry.is_ok() {
                    // The retried call refreshed this session in place; the cache must only
                    // ever hand out this post-refresh session, never the pre-refresh one.
                    self.cache.lock().await.insert(
                        self.did.clone(),
                        (
                            tokio::time::Instant::now() + OAUTH_SESSION_CACHE_TTL,
                            self.inner.clone(),
                        ),
                    );
                } else if attempt.grant_rejected.load(Ordering::Relaxed) {
                    // The authorization server rejected the refresh token itself: this
                    // session cannot be revived, so delete it and tell the request handler
                    // to send the user to sign in again instead of failing the write.
                    tracing::warn!(
                        did = %self.did,
                        error = %format!("{error:?}"),
                        "OAuth session is dead: the authorization server rejected the refresh token"
                    );
                    if let Ok(did) = self.did.parse::<Did>() {
                        let _ = self.store.del(&did).await;
                    }
                    self.cache.lock().await.remove(&self.did);
                    self.restores.lock().await.remove(&self.did);
                    mark_oauth_session_dead(&self.did);
                }
                return retry;
            }
        }
        result
    }
}

impl SharedOAuthSession {
    /// Rewrite the stored session so the OAuth client treats its access token as expired and
    /// the next send exchanges the refresh token instead of replaying a rejected access token.
    async fn expire_stored_session(&self) -> bool {
        let Ok(did) = self.did.parse::<Did>() else {
            return false;
        };
        let Ok(Some(mut session)) = self.store.get(&did).await else {
            return false;
        };
        session.token_set.expires_at = None;
        self.store.set(did, session).await.is_ok()
    }
}

impl SessionManager for SharedOAuthSession {
    async fn did(&self) -> Option<Did> {
        self.inner.did().await
    }
}

impl Configure for SharedOAuthSession {
    fn configure_endpoint(&self, endpoint: String) {
        self.inner.configure_endpoint(endpoint);
    }

    fn configure_labelers_header(&self, labeler_dids: Option<Vec<(Did, bool)>>) {
        self.inner.configure_labelers_header(labeler_dids);
    }

    fn configure_proxy_header(&self, did: Did, service_type: impl AsRef<str>) {
        self.inner.configure_proxy_header(did, service_type);
    }
}

impl CloneWithProxy for SharedOAuthSession {
    fn clone_with_proxy(&self, did: Did, service_type: impl AsRef<str>) -> Self {
        Self {
            inner: Arc::new(self.inner.clone_with_proxy(did, service_type)),
            did: self.did.clone(),
            store: self.store.clone(),
            cache: self.cache.clone(),
            restores: self.restores.clone(),
        }
    }
}

#[derive(Clone, Debug, Default)]
struct LabelerPresentation {
    name: String,
    handle: String,
    definitions: HashMap<String, LabelValuePresentation>,
}

#[derive(Clone, Debug)]
struct LabelValuePresentation {
    name: String,
    description: String,
    severity: String,
    blurs: String,
}
const SERVICE_AUTH_LIFETIME_SECS: u64 = 30 * 60;
const FEED_PAGE_CACHE_SECS: u64 = 60;
const BSKY_APPVIEW_DID: &str = "did:web:api.bsky.app";
const BSKY_APPVIEW_SERVICE: &str = "bsky_appview";
const MUTED_ACTOR_CACHE_TTL: Duration = Duration::from_secs(5 * 60);
const MUTED_ACTOR_CACHE_PREFIX: &str = "muted-actors:";
const LISTBLOCK_CACHE_TTL: Duration = Duration::from_secs(10 * 60);
const LISTBLOCK_CACHE_PREFIX: &str = "list-blocks:";
const LIST_MEMBERS_CACHE_TTL: Duration = Duration::from_secs(60 * 60);
const LIST_MEMBERS_CACHE_PREFIX: &str = "list-members:";

#[derive(Clone, Debug, Default)]
struct AccountActorData {
    labels: Vec<AccountLabel>,
    profile: Option<crate::feeds::PublicProfileView>,
}

pub struct AtriumOAuthFlow<B: crate::identity::IdentityBackend = HttpIdentityBackend> {
    client: Arc<AppOAuthClient>,
    identity_resolver: Arc<IdentityResolver<B>>,
    store: SqliteOAuthStore,
    feed_client: Arc<FeedClient<B>>,
    http_client: SafeHttpClient,
    public_appview_base: String,
    /// Base for `refresh_account_actor_data`'s getProfiles only; see LABELLED_APPVIEW_BASE.
    labelled_appview_base: String,
    video_http_client: reqwest::Client,
    video_service_url: String,
    session_cache: OAuthSessionCache,
    session_restores: OAuthSessionRestores,
    saved_feed_cache: SavedFeedCache,
    saved_feed_refreshes: SavedFeedRefreshes,
    service_auth_cache: ServiceAuthCache,
    feed_page_cache: FeedPageCache,
    feed_skeleton_cache: FeedSkeletonCache,
    muted_actor_cache: MutedActorCache,
    muted_actor_refreshes: MutedActorRefreshes,
    labeler_preference_cache: LabelerPreferenceCache,
    labeler_preference_refreshes: LabelerPreferenceRefreshes,
    account_label_actor_cache: AccountLabelActorCache,
    account_label_refreshes: AccountLabelRefreshes,
    labeler_presentation_cache: LabelerPresentationCache,
}

impl<B: crate::identity::IdentityBackend> Clone for AtriumOAuthFlow<B> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            identity_resolver: self.identity_resolver.clone(),
            store: self.store.clone(),
            feed_client: self.feed_client.clone(),
            http_client: self.http_client.clone(),
            public_appview_base: self.public_appview_base.clone(),
            labelled_appview_base: self.labelled_appview_base.clone(),
            video_http_client: self.video_http_client.clone(),
            video_service_url: self.video_service_url.clone(),
            session_cache: self.session_cache.clone(),
            session_restores: self.session_restores.clone(),
            saved_feed_cache: self.saved_feed_cache.clone(),
            saved_feed_refreshes: self.saved_feed_refreshes.clone(),
            service_auth_cache: self.service_auth_cache.clone(),
            feed_page_cache: self.feed_page_cache.clone(),
            feed_skeleton_cache: self.feed_skeleton_cache.clone(),
            muted_actor_cache: self.muted_actor_cache.clone(),
            muted_actor_refreshes: self.muted_actor_refreshes.clone(),
            labeler_preference_cache: self.labeler_preference_cache.clone(),
            labeler_preference_refreshes: self.labeler_preference_refreshes.clone(),
            account_label_actor_cache: self.account_label_actor_cache.clone(),
            account_label_refreshes: self.account_label_refreshes.clone(),
            labeler_presentation_cache: self.labeler_presentation_cache.clone(),
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
    #[error("OAuth session database writer stopped before replying")]
    WriterStopped,
}

struct EphemeralOAuthDatabase(PathBuf);

impl Drop for EphemeralOAuthDatabase {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
        let _ = fs::remove_file(format!("{}-wal", self.0.display()));
        let _ = fs::remove_file(format!("{}-shm", self.0.display()));
    }
}

type OAuthFireWrite = Box<dyn FnOnce(&Connection) -> Result<(), OAuthStoreError> + Send>;
type OAuthBlockingWrite = Box<dyn FnOnce(&mut Connection) + Send>;

enum OAuthWriterMessage {
    Fire(OAuthFireWrite),
    Blocking(OAuthBlockingWrite),
}

#[derive(Default)]
struct OAuthWriterMetrics {
    queue_depth: AtomicU64,
    queue_depth_max: AtomicU64,
    dropped: AtomicU64,
    batch_count: AtomicU64,
    transaction_count: AtomicU64,
    transaction_total_ns: AtomicU64,
    transaction_max_ns: AtomicU64,
    refresh_failures: AtomicU64,
}

impl OAuthWriterMetrics {
    fn queued(&self) {
        let depth = self.queue_depth.fetch_add(1, Ordering::Relaxed) + 1;
        self.queue_depth_max.fetch_max(depth, Ordering::Relaxed);
    }

    fn dequeued(&self) {
        self.queue_depth.fetch_sub(1, Ordering::Relaxed);
    }

    fn record_batch(&self, elapsed: Duration) {
        let elapsed_ns = elapsed.as_nanos().min(u128::from(u64::MAX)) as u64;
        self.batch_count.fetch_add(1, Ordering::Relaxed);
        self.transaction_count.fetch_add(1, Ordering::Relaxed);
        self.transaction_total_ns
            .fetch_add(elapsed_ns, Ordering::Relaxed);
        self.transaction_max_ns
            .fetch_max(elapsed_ns, Ordering::Relaxed);
    }

    fn record_refresh_failure(&self) {
        self.refresh_failures.fetch_add(1, Ordering::Relaxed);
    }

    fn report(&self) {
        let count = self.transaction_count.swap(0, Ordering::Relaxed);
        let total_ns = self.transaction_total_ns.swap(0, Ordering::Relaxed);
        let max_ns = self.transaction_max_ns.swap(0, Ordering::Relaxed);
        tracing::info!(
            oauth_writer_queue_depth_max = self
                .queue_depth_max
                .swap(self.queue_depth.load(Ordering::Relaxed), Ordering::Relaxed),
            oauth_writer_batch_count = self.batch_count.swap(0, Ordering::Relaxed),
            oauth_writer_transaction_mean_ms = if count == 0 {
                0.0
            } else {
                total_ns as f64 / count as f64 / 1_000_000.0
            },
            oauth_writer_transaction_max_ms = max_ns as f64 / 1_000_000.0,
            oauth_writer_dropped_count = self.dropped.swap(0, Ordering::Relaxed),
            oauth_refresh_failure_count = self.refresh_failures.swap(0, Ordering::Relaxed),
            "OAuth SQLite writer minute metrics"
        );
    }
}

struct OAuthStoreWriter {
    sender: Mutex<Option<SyncSender<OAuthWriterMessage>>>,
    worker: Mutex<Option<JoinHandle<()>>>,
    metrics: Arc<OAuthWriterMetrics>,
}

impl OAuthStoreWriter {
    fn new(connection: Connection) -> Result<Self, OAuthStoreError> {
        let (sender, receiver) = mpsc::sync_channel(OAUTH_STORE_WRITER_QUEUE);
        let metrics = Arc::new(OAuthWriterMetrics::default());
        let worker_metrics = Arc::clone(&metrics);
        let worker = std::thread::Builder::new()
            .name("oauth-sqlite-writer".to_owned())
            .spawn(move || oauth_store_writer_loop(connection, receiver, worker_metrics))?;
        Ok(Self {
            sender: Mutex::new(Some(sender)),
            worker: Mutex::new(Some(worker)),
            metrics,
        })
    }

    async fn call<R: Send + 'static>(
        &self,
        operation: impl FnOnce(&mut Connection) -> Result<R, OAuthStoreError> + Send + 'static,
    ) -> Result<R, OAuthStoreError> {
        let (reply, result) = oneshot::channel();
        let message = OAuthWriterMessage::Blocking(Box::new(move |connection| {
            let _ = reply.send(operation(connection));
        }));
        let sender = self
            .sender
            .lock()
            .map_err(|_| OAuthStoreError::Poisoned)?
            .as_ref()
            .cloned()
            .ok_or(OAuthStoreError::WriterStopped)?;
        self.metrics.queued();
        tokio::task::spawn_blocking(move || sender.send(message))
            .await
            .map_err(|_| OAuthStoreError::WriterStopped)?
            .map_err(|_| OAuthStoreError::WriterStopped)?;
        result.await.map_err(|_| OAuthStoreError::WriterStopped)?
    }

    fn fire(
        &self,
        operation: impl FnOnce(&Connection) -> Result<(), OAuthStoreError> + Send + 'static,
    ) {
        let sender = self
            .sender
            .lock()
            .ok()
            .and_then(|sender| sender.as_ref().cloned());
        let Some(sender) = sender else {
            self.metrics.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        };
        self.metrics.queued();
        if let Err(error) = sender.try_send(OAuthWriterMessage::Fire(Box::new(operation))) {
            self.metrics.dequeued();
            self.metrics.dropped.fetch_add(1, Ordering::Relaxed);
            match error {
                mpsc::TrySendError::Full(_) => {
                    tracing::debug!("OAuth SQLite writer queue full; dropped cache write");
                }
                mpsc::TrySendError::Disconnected(_) => {
                    tracing::warn!("OAuth SQLite writer stopped; dropped cache write");
                }
            }
        }
    }

    async fn flush(&self) -> Result<(), OAuthStoreError> {
        self.call(|_| Ok(())).await
    }

    fn shutdown(&self) {
        if let Ok(mut sender) = self.sender.lock() {
            drop(sender.take());
        }
        if let Ok(mut worker) = self.worker.lock()
            && let Some(worker) = worker.take()
            && worker.join().is_err()
        {
            tracing::error!("OAuth SQLite writer thread panicked during shutdown");
        }
    }
}

impl Drop for OAuthStoreWriter {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn oauth_store_writer_loop(
    mut connection: Connection,
    receiver: Receiver<OAuthWriterMessage>,
    metrics: Arc<OAuthWriterMetrics>,
) {
    let mut next_metrics = Instant::now() + OAUTH_STORE_WRITER_METRICS_INTERVAL;
    let mut pending = None;
    loop {
        let message = if let Some(message) = pending.take() {
            Some(message)
        } else {
            match receiver.recv_timeout(next_metrics.saturating_duration_since(Instant::now())) {
                Ok(message) => Some(message),
                Err(RecvTimeoutError::Timeout) => {
                    metrics.report();
                    next_metrics = Instant::now() + OAUTH_STORE_WRITER_METRICS_INTERVAL;
                    continue;
                }
                Err(RecvTimeoutError::Disconnected) => None,
            }
        };
        let Some(message) = message else {
            break;
        };
        metrics.dequeued();
        match message {
            OAuthWriterMessage::Blocking(operation) => {
                let started = Instant::now();
                operation(&mut connection);
                metrics.record_batch(started.elapsed());
            }
            OAuthWriterMessage::Fire(first) => {
                let mut writes = vec![first];
                let deadline = Instant::now() + OAUTH_STORE_WRITER_BATCH_DELAY;
                while writes.len() < OAUTH_STORE_WRITER_BATCH_SIZE {
                    match receiver.try_recv() {
                        Ok(OAuthWriterMessage::Fire(write)) => {
                            metrics.dequeued();
                            writes.push(write);
                        }
                        Ok(blocking @ OAuthWriterMessage::Blocking(_)) => {
                            pending = Some(blocking);
                            break;
                        }
                        Err(TryRecvError::Empty) if Instant::now() < deadline => {
                            std::thread::sleep(Duration::from_micros(100));
                        }
                        Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
                    }
                }
                let started = Instant::now();
                let mut completions = 0_u64;
                let mut first_error = None;
                match connection.transaction() {
                    Ok(transaction) => {
                        for write in writes {
                            match write(&transaction) {
                                Ok(()) => completions += 1,
                                Err(error) if first_error.is_none() => first_error = Some(error),
                                Err(_) => {}
                            }
                        }
                        if first_error.is_none()
                            && let Err(error) = transaction.commit()
                        {
                            first_error = Some(OAuthStoreError::Sqlite(error));
                        }
                    }
                    Err(error) => first_error = Some(OAuthStoreError::Sqlite(error)),
                }
                if let Some(error) = first_error {
                    tracing::warn!(%error, "OAuth SQLite cache write batch rolled back");
                } else {
                    tracing::trace!(
                        cache_writes = completions,
                        "OAuth SQLite cache writes committed"
                    );
                }
                metrics.record_batch(started.elapsed());
            }
        }
        if Instant::now() >= next_metrics {
            metrics.report();
            next_metrics = Instant::now() + OAUTH_STORE_WRITER_METRICS_INTERVAL;
        }
    }
}

#[derive(Clone)]
pub struct SqliteOAuthStore {
    read_connection: Arc<Mutex<Connection>>,
    writer: Arc<OAuthStoreWriter>,
    _ephemeral_database: Option<Arc<EphemeralOAuthDatabase>>,
}

impl SqliteOAuthStore {
    /// Refreshes the authorization server rejected since the last metrics report.
    pub fn oauth_refresh_failure_count(&self) -> u64 {
        self.writer.metrics.refresh_failures.load(Ordering::Relaxed)
    }

    pub fn open(path: impl AsRef<Path>) -> Result<Self, OAuthStoreError> {
        let path = path.as_ref();
        let is_memory = path == Path::new(":memory:");
        if !is_memory && let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let ephemeral_database = is_memory.then(|| {
            Arc::new(EphemeralOAuthDatabase(std::env::temp_dir().join(format!(
                "appview-rs-oauth-memory-{}-{}.sqlite3",
                std::process::id(),
                NEXT_OAUTH_MEMORY_DATABASE_ID.fetch_add(1, Ordering::Relaxed)
            ))))
        });
        let database_path = ephemeral_database
            .as_ref()
            .map(|database| database.0.as_path())
            .unwrap_or(path);
        crate::ingest::ensure_private_database_file(database_path)?;
        let connection = Connection::open(database_path)?;
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
        let reader = Connection::open(database_path)?;
        reader.execute_batch("PRAGMA busy_timeout = 5000; PRAGMA query_only = ON;")?;
        Ok(Self {
            read_connection: Arc::new(Mutex::new(reader)),
            writer: Arc::new(OAuthStoreWriter::new(connection)?),
            _ephemeral_database: ephemeral_database,
        })
    }

    pub(crate) async fn web_cache_get(&self, key: &str) -> Result<Option<(String, i64)>, String> {
        self.writer
            .flush()
            .await
            .map_err(|error| error.to_string())?;
        self.read_connection
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
        let writer_key = key.to_owned();
        let writer_value = value_json.to_owned();
        self.writer.fire(move |connection| {
            connection.execute(
                "INSERT INTO web_cache(cache_key,value_json,stored_at_ms) VALUES(?1,?2,?3)
                 ON CONFLICT(cache_key) DO UPDATE SET value_json=excluded.value_json,stored_at_ms=excluded.stored_at_ms",
                params![writer_key, writer_value, stored_at_ms],
            )?;
            Ok(())
        });
        Ok(())
    }

    pub(crate) async fn web_cache_delete(&self, key: &str) -> Result<(), String> {
        let writer_key = key.to_owned();
        self.writer
            .call(move |connection| {
                connection.execute("DELETE FROM web_cache WHERE cache_key=?1", [writer_key])?;
                Ok(())
            })
            .await
            .map_err(|error| error.to_string())
    }

    fn get_json<T: for<'de> Deserialize<'de>>(
        &self,
        table: &'static str,
        key_column: &'static str,
        key: &str,
        state_ttl: bool,
    ) -> Result<Option<T>, OAuthStoreError> {
        let connection = self
            .read_connection
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

    async fn set_json<T: serde::Serialize>(
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
        let writer_key = key.to_owned();
        self.writer
            .call(move |connection| {
                connection.execute(&sql, params![writer_key, value_json])?;
                Ok(())
            })
            .await
    }

    async fn delete_key(
        &self,
        table: &'static str,
        key_column: &'static str,
        key: &str,
    ) -> Result<(), OAuthStoreError> {
        let sql = format!("DELETE FROM {table} WHERE {key_column}=?1");
        let writer_key = key.to_owned();
        self.writer
            .call(move |connection| {
                connection.execute(&sql, [writer_key])?;
                Ok(())
            })
            .await
    }

    async fn clear_table(&self, table: &'static str) -> Result<(), OAuthStoreError> {
        self.writer
            .call(move |connection| {
                connection.execute(&format!("DELETE FROM {table}"), [])?;
                Ok(())
            })
            .await
    }
}

impl Store<String, InternalStateData> for SqliteOAuthStore {
    type Error = OAuthStoreError;

    async fn get(&self, key: &String) -> Result<Option<InternalStateData>, Self::Error> {
        self.get_json("oauth_states", "state", key, true)
    }

    async fn set(&self, key: String, value: InternalStateData) -> Result<(), Self::Error> {
        self.set_json("oauth_states", "state", &key, &value, true)
            .await
    }

    async fn del(&self, key: &String) -> Result<(), Self::Error> {
        self.delete_key("oauth_states", "state", key).await
    }

    async fn clear(&self) -> Result<(), Self::Error> {
        self.clear_table("oauth_states").await
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
            .await
    }

    async fn del(&self, key: &Did) -> Result<(), Self::Error> {
        self.delete_key("oauth_sessions", "did", key.as_ref()).await
    }

    async fn clear(&self) -> Result<(), Self::Error> {
        self.clear_table("oauth_sessions").await
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
        Self::new_with_public_appview_base(
            public,
            identity_resolver,
            store,
            plc_directory_url,
            guarded_client,
            PUBLIC_APPVIEW_BASE,
        )
    }

    fn new_with_public_appview_base(
        public: &OAuthPublic,
        identity_resolver: Arc<IdentityResolver<B>>,
        store: SqliteOAuthStore,
        plc_directory_url: &str,
        guarded_client: SafeHttpClient,
        public_appview_base: &str,
    ) -> Result<Self, String> {
        let feed_client = Arc::new(FeedClient::new_with_store(
            Arc::clone(&identity_resolver),
            guarded_client.clone(),
            store.clone(),
        ));
        let retained_http_client = guarded_client.clone();
        let http_client = Arc::new(guarded_client);
        let oauth_http_client = OAuthMetadataHttpClient::new((*http_client).clone())
            .with_writer_metrics(store.writer.metrics.clone());
        let redirect_uri = public.metadata["redirect_uris"][0]
            .as_str()
            .ok_or_else(|| "OAuth client metadata has no redirect URI".to_owned())?
            .to_owned();
        let make_resolver = || {
            let mut resolver = OAuthResolverConfig {
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
            // Atrium's private resolver cache must not hide refreshed transport-cache values.
            resolver.authorization_server_metadata.cache.time_to_live =
                Some(OAUTH_METADATA_RESOLVER_CACHE_TTL);
            resolver.protected_resource_metadata.cache.time_to_live =
                Some(OAUTH_METADATA_RESOLVER_CACHE_TTL);
            resolver
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
                http_client: oauth_http_client.clone(),
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
                    http_client: oauth_http_client.clone(),
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
            public_appview_base: public_appview_base.to_owned(),
            labelled_appview_base: LABELLED_APPVIEW_BASE.to_owned(),
            video_http_client: reqwest::Client::builder()
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .map_err(|error| error.to_string())?,
            video_service_url: VIDEO_SERVICE_URL.to_owned(),
            session_cache: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            session_restores: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            saved_feed_cache: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            saved_feed_refreshes: Arc::new(tokio::sync::Mutex::new(HashSet::new())),
            service_auth_cache: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            feed_page_cache: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            feed_skeleton_cache: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            muted_actor_cache: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            muted_actor_refreshes: Arc::new(tokio::sync::Mutex::new(HashSet::new())),
            labeler_preference_cache: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            labeler_preference_refreshes: Arc::new(tokio::sync::Mutex::new(HashSet::new())),
            account_label_actor_cache: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
            account_label_refreshes: Arc::new(tokio::sync::Mutex::new(HashSet::new())),
            labeler_presentation_cache: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
        })
    }

    /// Point the labelled getProfiles request at a fake server (tests only).
    /// Production always uses LABELLED_APPVIEW_BASE; see its comment for why.
    #[cfg(test)]
    fn with_labelled_appview_base(mut self, base: impl Into<String>) -> Self {
        self.labelled_appview_base = base.into();
        self
    }

    async fn restored_session(&self, did: &Did) -> Result<SharedOAuthSession, String> {
        let key = did.to_string();
        let now = tokio::time::Instant::now();
        if let Some((expires, session)) = self.session_cache.lock().await.get(&key).cloned()
            && expires > now
        {
            return Ok(SharedOAuthSession {
                inner: session,
                did: key,
                store: self.store.clone(),
                cache: self.session_cache.clone(),
                restores: self.session_restores.clone(),
            });
        }

        let restore = {
            let mut pending = self.session_restores.lock().await;
            pending
                .entry(key.clone())
                .or_insert_with(|| Arc::new(OnceCell::new()))
                .clone()
        };
        let result = restore
            .get_or_init(|| async {
                self.client
                    .restore(did)
                    .await
                    .map(Arc::new)
                    .map_err(|error| error.to_string())
            })
            .await
            .clone();

        if let Err(error) = &result
            && error
                .to_ascii_lowercase()
                .contains("session does not exist")
        {
            // The stored session is gone (e.g. deleted as dead by an earlier request): there
            // is nothing to restore, so the user must sign in again rather than see a failure.
            tracing::warn!(
                did = %key,
                error = %error,
                "OAuth session cannot be restored: sending the user to sign in again"
            );
            mark_oauth_session_dead(&key);
        }
        if let Ok(session) = &result {
            let now = tokio::time::Instant::now();
            let mut cache = self.session_cache.lock().await;
            cache.retain(|_, (expires, _)| *expires > now);
            if cache.len() >= OAUTH_SESSION_CACHE_CAPACITY
                && let Some(oldest) = cache.keys().next().cloned()
            {
                cache.remove(&oldest);
            }
            cache.insert(
                key.clone(),
                (now + OAUTH_SESSION_CACHE_TTL, Arc::clone(session)),
            );
        }
        self.session_restores.lock().await.remove(&key);
        result.map(|inner| SharedOAuthSession {
            inner,
            did: key,
            store: self.store.clone(),
            cache: self.session_cache.clone(),
            restores: self.session_restores.clone(),
        })
    }

    async fn evict_restored_session(&self, did: &str) {
        self.session_cache.lock().await.remove(did);
        self.session_restores.lock().await.remove(did);
    }

    fn session_agent(session: &SharedOAuthSession) -> Agent<SharedOAuthSession> {
        Agent::new(session.clone())
    }

    async fn revoke_session(&self, did: &Did) -> Result<(), String> {
        self.evict_restored_session(did.as_ref()).await;
        self.client
            .revoke(did)
            .await
            .map_err(|error| error.to_string())
    }

    async fn cached_saved_feed_preferences(
        &self,
        did: &str,
    ) -> Option<(Vec<SavedFeedPreference>, bool)> {
        let now = tokio::time::Instant::now();
        if let Some((expires, preferences)) = self.saved_feed_cache.lock().await.get(did).cloned() {
            return Some((preferences, expires <= now));
        }
        let Ok(Some((json, stored_at_ms))) =
            self.store.web_cache_get(&saved_feed_cache_key(did)).await
        else {
            return None;
        };
        let Ok(preferences) = serde_json::from_str::<Vec<SavedFeedPreference>>(&json) else {
            return None;
        };
        let age_ms = unix_time_ms().saturating_sub(stored_at_ms).max(0) as u64;
        let ttl_ms = SAVED_FEED_PREFERENCE_CACHE_TTL.as_millis() as u64;
        let stale = age_ms >= ttl_ms;
        let expires = now + Duration::from_millis(ttl_ms.saturating_sub(age_ms));
        let mut cache = self.saved_feed_cache.lock().await;
        cache.retain(|_, (expires, _)| *expires > now);
        if cache.len() >= 4096
            && let Some(oldest) = cache.keys().next().cloned()
        {
            cache.remove(&oldest);
        }
        cache.insert(did.to_owned(), (expires, preferences.clone()));
        Some((preferences, stale))
    }

    async fn refresh_saved_feed_preferences(
        &self,
        did: &str,
    ) -> Result<Vec<SavedFeedPreference>, String> {
        let parsed_did = did
            .parse::<Did>()
            .map_err(|_| "viewer DID is invalid".to_owned())?;
        let session = self.restored_session(&parsed_did).await?;
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
        let cache_key = saved_feed_cache_key(did);
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
            did.to_owned(),
            (
                tokio::time::Instant::now() + SAVED_FEED_PREFERENCE_CACHE_TTL,
                preferences.clone(),
            ),
        );
        Ok(preferences)
    }

    async fn schedule_saved_feed_refresh(&self, did: &str) {
        let mut pending = self.saved_feed_refreshes.lock().await;
        if !pending.insert(did.to_owned()) {
            return;
        }
        drop(pending);
        let flow = self.clone();
        let did = did.to_owned();
        tokio::spawn(async move {
            if let Err(error) = flow.refresh_saved_feed_preferences(&did).await {
                tracing::debug!(%error, %did, "saved feed preferences refresh failed");
            }
            flow.saved_feed_refreshes.lock().await.remove(&did);
        });
    }

    async fn saved_feed_preferences_for_tabs(&self, did: &str) -> Vec<SavedFeedPreference> {
        self.saved_feed_preferences(did).await.unwrap_or_default()
    }

    async fn saved_feed_preferences(&self, did: &str) -> Result<Vec<SavedFeedPreference>, String> {
        if let Some((preferences, stale)) = self.cached_saved_feed_preferences(did).await {
            if stale {
                self.schedule_saved_feed_refresh(did).await;
            }
            return Ok(preferences);
        }

        let mut pending = self.saved_feed_refreshes.lock().await;
        if pending.insert(did.to_owned()) {
            drop(pending);
            let refresh = tokio::time::timeout(
                PAGE_PROFILE_BUDGET,
                self.refresh_saved_feed_preferences(did),
            )
            .await
            .unwrap_or_else(|_| Err("PDS getPreferences request timed out".to_owned()));
            self.saved_feed_refreshes.lock().await.remove(did);
            return refresh;
        }
        drop(pending);

        let deadline = tokio::time::Instant::now() + PAGE_PROFILE_BUDGET;
        loop {
            if let Some((preferences, stale)) = self.cached_saved_feed_preferences(did).await
                && !stale
            {
                return Ok(preferences);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err("PDS getPreferences request timed out".to_owned());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn list_repo_collection(
        &self,
        did: &str,
        collection: &str,
    ) -> Result<Vec<Value>, String> {
        let parsed_did = did
            .parse::<Did>()
            .map_err(|_| "viewer DID is invalid".to_owned())?;
        let session = self.restored_session(&parsed_did).await?;
        let mut records = Vec::new();
        let mut cursor: Option<String> = None;
        for _ in 0..100 {
            let mut parameters = json!({"repo":did,"collection":collection,"limit":100});
            if let Some(value) = cursor.as_deref() {
                parameters["cursor"] = json!(value);
            }
            let request = XrpcRequest {
                method: http::Method::GET,
                nsid: "com.atproto.repo.listRecords".to_owned(),
                parameters: Some(parameters),
                input: None,
                encoding: None,
            };
            let response: OutputDataOrBytes<Value> = session
                .send_xrpc::<Value, Value, Value, Value>(&request)
                .await
                .map_err(|error| {
                    log_pds_xrpc_failure("com.atproto.repo.listRecords", &error);
                    "PDS listRecords request failed".to_owned()
                })?;
            let response = match response {
                OutputDataOrBytes::Data(value) => value,
                OutputDataOrBytes::Bytes(_) => {
                    return Err("PDS listRecords returned a non-JSON response".to_owned());
                }
            };
            let Some(page) = response.get("records").and_then(Value::as_array) else {
                return Err("PDS listRecords returned no records array".to_owned());
            };
            records.extend(page.iter().cloned());
            if records.len() > 10_000 {
                return Err("PDS listRecords returned too many records".to_owned());
            }
            cursor = response
                .get("cursor")
                .and_then(Value::as_str)
                .map(str::to_owned);
            if cursor.is_none() || page.is_empty() {
                return Ok(records);
            }
        }
        Err("PDS listRecords pagination exceeded its safety limit".to_owned())
    }

    /// Resolve a block list owned by any account: its display name and member
    /// DIDs, from the public Bluesky appview's `app.bsky.graph.getList`
    /// (unauthenticated, paginated by the list itself). Not the owner's PDS:
    /// listing the owner's `app.bsky.graph.listitem` collection walks EVERY
    /// list they own; on 2026-10-30 one owner had over 15 000 items of which
    /// 93 of the first 15 000 belonged to the 546-member list, ~150 s a fetch.
    async fn fetch_block_list_members(&self, list_uri: &str) -> Result<BlockListDetail, String> {
        let mut parts = list_uri
            .strip_prefix("at://")
            .ok_or_else(|| "block list uri is invalid".to_owned())?
            .split('/');
        let owner_did = parts.next().unwrap_or_default();
        let collection = parts.next().unwrap_or_default();
        let rkey = parts.next().unwrap_or_default();
        if owner_did.is_empty()
            || collection != "app.bsky.graph.list"
            || rkey.is_empty()
            || parts.next().is_some()
        {
            return Err("block list uri is invalid".to_owned());
        }
        let mut name = None;
        let mut members = HashSet::new();
        let mut cursor: Option<String> = None;
        let mut pages = 0usize;
        loop {
            let mut page_url =
                url::Url::parse(&format!("{PUBLIC_APPVIEW_BASE}app.bsky.graph.getList"))
                    .map_err(|error| format!("block list getList URL is invalid: {error}"))?;
            {
                let mut query = page_url.query_pairs_mut();
                query.append_pair("list", list_uri);
                query.append_pair("limit", "100");
                if let Some(value) = cursor.as_deref() {
                    query.append_pair("cursor", value);
                }
            }
            let response = self
                .http_client
                .get(&page_url, HeaderMap::new(), 2 * 1024 * 1024)
                .await
                .map_err(|error| format!("block list getList request failed: {error}"))?;
            if response.status() == StatusCode::BAD_REQUEST
                && String::from_utf8_lossy(response.body()).contains("List not found")
            {
                // A subscription can outlive its list (the owner deleted it). bsky.app
                // ignores it; cache it as empty so every page does not retry and warn.
                tracing::info!(list = %list_uri, "subscribed block list no longer exists; treating it as empty");
                return Ok(BlockListDetail {
                    uri: list_uri.to_owned(),
                    name: "Deleted list".to_owned(),
                    member_dids: Vec::new(),
                });
            }
            if !response.status().is_success() {
                return Err(format!(
                    "block list getList returned status {}: {}",
                    response.status(),
                    String::from_utf8_lossy(&response.body()[..response.body().len().min(200)])
                ));
            }
            let page: Value = serde_json::from_slice(response.body())
                .map_err(|error| format!("block list getList was invalid JSON: {error}"))?;
            if name.is_none() {
                name = page
                    .pointer("/list/name")
                    .and_then(Value::as_str)
                    .filter(|name| !name.trim().is_empty())
                    .map(str::to_owned);
            }
            let Some(items) = page.get("items").and_then(Value::as_array) else {
                return Err("block list getList returned no items array".to_owned());
            };
            for item in items {
                if let Some(did) = item.pointer("/subject/did").and_then(Value::as_str) {
                    members.insert(did.to_owned());
                }
            }
            pages += 1;
            cursor = page
                .get("cursor")
                .and_then(Value::as_str)
                .filter(|cursor| !cursor.is_empty())
                .map(str::to_owned);
            if cursor.is_none() || items.is_empty() {
                break;
            }
            if pages >= 100 {
                tracing::warn!(
                    list = %list_uri,
                    members = members.len(),
                    "block list member fetch stopped at 100 getList pages; members beyond it stay unknown"
                );
                break;
            }
        }
        tracing::info!(list = %list_uri, members = members.len(), pages, "block list members fetched");
        let mut member_dids = members.into_iter().collect::<Vec<_>>();
        member_dids.sort();
        Ok(BlockListDetail {
            uri: list_uri.to_owned(),
            name: name.unwrap_or_else(|| "Untitled list".to_owned()),
            member_dids,
        })
    }

    async fn fetch_muted_actor_dids(&self, did: &str) -> Result<HashSet<String>, String> {
        did.parse::<Did>()
            .map_err(|_| "viewer DID is invalid".to_owned())?;
        let now = tokio::time::Instant::now();
        if let Some((expires, actors)) = self.muted_actor_cache.lock().await.get(did).cloned() {
            if expires > now {
                return Ok(actors);
            }
            self.schedule_muted_actor_refresh(did).await;
            return Ok(actors);
        }
        if let Ok(Some((json, stored_at_ms))) =
            self.store.web_cache_get(&muted_actor_cache_key(did)).await
            && let Ok(actor_list) = serde_json::from_str::<Vec<String>>(&json)
        {
            let actors = actor_list.into_iter().collect::<HashSet<_>>();
            let age_ms = unix_time_ms().saturating_sub(stored_at_ms).max(0) as u64;
            let ttl_ms = MUTED_ACTOR_CACHE_TTL.as_millis() as u64;
            let stale = age_ms >= ttl_ms;
            let expires = now + Duration::from_millis(ttl_ms.saturating_sub(age_ms));
            let mut cache = self.muted_actor_cache.lock().await;
            cache.retain(|_, (expires, _)| *expires > now);
            if cache.len() < 4096 {
                cache.insert(did.to_owned(), (expires, actors.clone()));
            }
            drop(cache);
            if stale {
                self.schedule_muted_actor_refresh(did).await;
            }
            return Ok(actors);
        }

        let mut pending = self.muted_actor_refreshes.lock().await;
        if !pending.insert(did.to_owned()) {
            drop(pending);
            let deadline = tokio::time::Instant::now() + PAGE_PROFILE_BUDGET;
            loop {
                if let Some((expires, actors)) =
                    self.muted_actor_cache.lock().await.get(did).cloned()
                    && expires > tokio::time::Instant::now()
                {
                    return Ok(actors);
                }
                if tokio::time::Instant::now() >= deadline {
                    return Err("Bluesky getMutes request timed out".to_owned());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        drop(pending);
        let result = self.refresh_muted_actor_dids_with_deadline(did).await;
        self.muted_actor_refreshes.lock().await.remove(did);
        result
    }

    async fn schedule_muted_actor_refresh(&self, did: &str) {
        let mut pending = self.muted_actor_refreshes.lock().await;
        if !pending.insert(did.to_owned()) {
            return;
        }
        drop(pending);
        let flow = self.clone();
        let did = did.to_owned();
        tokio::spawn(async move {
            if let Err(error) = flow.refresh_muted_actor_dids_with_deadline(&did).await {
                tracing::debug!(%error, %did, "muted actor list refresh failed");
            }
            flow.muted_actor_refreshes.lock().await.remove(&did);
        });
    }

    async fn refresh_muted_actor_dids_with_deadline(
        &self,
        did: &str,
    ) -> Result<HashSet<String>, String> {
        tokio::time::timeout(PAGE_PROFILE_BUDGET, self.refresh_muted_actor_dids(did))
            .await
            .map_err(|_| "Bluesky getMutes request timed out".to_owned())?
    }

    async fn refresh_muted_actor_dids(&self, did: &str) -> Result<HashSet<String>, String> {
        let parsed_did = did
            .parse::<Did>()
            .map_err(|_| "viewer DID is invalid".to_owned())?;
        let session = self.restored_session(&parsed_did).await?;
        let proxy_did = BSKY_APPVIEW_DID
            .parse::<Did>()
            .map_err(|_| "Bluesky AppView service DID is invalid".to_owned())?;
        let api = Self::session_agent(&session).api_with_proxy(proxy_did, BSKY_APPVIEW_SERVICE);
        let mut actors = HashSet::new();
        let mut cursor: Option<String> = None;
        for _ in 0..100 {
            let parameters = atrium_api::app::bsky::graph::get_mutes::ParametersData {
                cursor: cursor.clone(),
                limit: None,
            }
            .into();
            let page = api
                .app
                .bsky
                .graph
                .get_mutes(parameters)
                .await
                .map_err(|error| format!("Bluesky getMutes request failed: {error:?}"))?;
            actors.extend(
                page.data
                    .mutes
                    .into_iter()
                    .map(|profile| profile.did.to_string()),
            );
            cursor = page.data.cursor;
            if cursor.is_none() {
                let mut actor_list = actors.iter().cloned().collect::<Vec<_>>();
                actor_list.sort();
                if let Ok(value_json) = serde_json::to_string(&actor_list)
                    && let Err(error) = self
                        .store
                        .web_cache_set(&muted_actor_cache_key(did), &value_json)
                {
                    tracing::warn!(%error, "could not persist muted actor list");
                }
                let mut cache = self.muted_actor_cache.lock().await;
                cache.retain(|_, (expires, _)| *expires > tokio::time::Instant::now());
                if cache.len() >= 4096
                    && let Some(oldest) = cache.keys().next().cloned()
                {
                    cache.remove(&oldest);
                }
                cache.insert(
                    did.to_owned(),
                    (
                        tokio::time::Instant::now() + MUTED_ACTOR_CACHE_TTL,
                        actors.clone(),
                    ),
                );
                return Ok(actors);
            }
        }
        Err("Bluesky getMutes pagination exceeded its safety limit".to_owned())
    }

    async fn labeler_dids_for_viewer(&self, did: &str) -> Result<Vec<String>, String> {
        if let Some((expires, dids)) = self.labeler_preference_cache.lock().await.get(did).cloned()
        {
            if expires > tokio::time::Instant::now() {
                return Ok(dids);
            }
            self.schedule_labeler_preference_refresh(did).await;
            return Ok(dids);
        }
        let mut pending = self.labeler_preference_refreshes.lock().await;
        if !pending.insert(did.to_owned()) {
            drop(pending);
            let deadline = tokio::time::Instant::now() + PAGE_PROFILE_BUDGET;
            loop {
                if let Some((_, dids)) =
                    self.labeler_preference_cache.lock().await.get(did).cloned()
                {
                    return Ok(dids);
                }
                if tokio::time::Instant::now() >= deadline {
                    return Err("PDS getPreferences request timed out".to_owned());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        }
        drop(pending);
        // The refresh runs in a task that owns the pending-set cleanup: a caller
        // dropped by its page deadline must not strand the DID as pending forever.
        let (done, awaited) = oneshot::channel();
        let flow = self.clone();
        let refresh_did = did.to_owned();
        let dispatcher = tracing::dispatcher::get_default(|dispatcher| dispatcher.clone());
        tokio::spawn(
            async move {
                let result = tokio::time::timeout(
                    PAGE_PROFILE_BUDGET,
                    flow.refresh_labeler_preferences(&refresh_did),
                )
                .await
                .unwrap_or_else(|_| Err("PDS getPreferences request timed out".to_owned()));
                flow.labeler_preference_refreshes
                    .lock()
                    .await
                    .remove(&refresh_did);
                let _ = done.send(result);
            }
            .with_subscriber(dispatcher),
        );
        match awaited.await {
            Ok(result) => result,
            Err(_) => Err("PDS getPreferences refresh task ended without a result".to_owned()),
        }
    }

    async fn schedule_labeler_preference_refresh(&self, did: &str) {
        let mut pending = self.labeler_preference_refreshes.lock().await;
        if !pending.insert(did.to_owned()) {
            return;
        }
        drop(pending);
        let flow = self.clone();
        let did = did.to_owned();
        tokio::spawn(async move {
            if let Err(error) =
                tokio::time::timeout(PAGE_PROFILE_BUDGET, flow.refresh_labeler_preferences(&did))
                    .await
                    .unwrap_or_else(|_| Err("PDS getPreferences request timed out".to_owned()))
            {
                tracing::debug!(%error, %did, "labeler preferences refresh failed");
            }
            flow.labeler_preference_refreshes.lock().await.remove(&did);
        });
    }

    async fn refresh_labeler_preferences(&self, did: &str) -> Result<Vec<String>, String> {
        let parsed_did = did
            .parse::<Did>()
            .map_err(|_| "viewer DID is invalid".to_owned())?;
        let session = self.restored_session(&parsed_did).await?;
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
        let mut labelers = vec![BSKY_MODERATION_LABELER_DID.to_owned()];
        if let Some(preferences) = response.get("preferences").and_then(Value::as_array) {
            for preference in preferences {
                let is_labelers_pref = preference
                    .get("$type")
                    .and_then(Value::as_str)
                    .is_some_and(|kind| kind.ends_with("#labelersPref"));
                if !is_labelers_pref {
                    continue;
                }
                if let Some(user_labelers) = preference.get("labelers").and_then(Value::as_array) {
                    labelers.extend(user_labelers.iter().filter_map(|labeler| {
                        let did = labeler.get("did")?.as_str()?;
                        did.parse::<Did>().ok().map(|_| did.to_owned())
                    }));
                }
            }
        }
        let mut seen = HashSet::new();
        labelers.retain(|labeler| seen.insert(labeler.clone()));
        labelers.truncate(20);
        let mut cache = self.labeler_preference_cache.lock().await;
        cache.retain(|_, (expires, _)| *expires > tokio::time::Instant::now());
        if cache.len() >= 4096
            && let Some(oldest) = cache.keys().next().cloned()
        {
            cache.remove(&oldest);
        }
        cache.insert(
            did.to_owned(),
            (
                tokio::time::Instant::now() + LABELER_PREFERENCE_CACHE_TTL,
                labelers.clone(),
            ),
        );
        Ok(labelers)
    }

    /// Fetch a public AppView JSON document, also returning the raw
    /// `atproto-content-labelers` response header when present.
    async fn public_appview_json(
        &self,
        url: url::Url,
        headers: HeaderMap,
    ) -> Result<(Value, Option<String>), String> {
        let response = self.http_client.get(&url, headers, 2 * 1024 * 1024).await?;
        if !response.status().is_success() {
            return Err(format!(
                "Bluesky AppView request failed with status {}",
                response.status()
            ));
        }
        let applied_labelers = response
            .headers()
            .get("atproto-content-labelers")
            .and_then(|value| value.to_str().ok())
            .map(str::to_owned);
        let body = serde_json::from_slice(response.body())
            .map_err(|_| "Bluesky AppView returned invalid JSON".to_owned())?;
        Ok((body, applied_labelers))
    }

    async fn labeler_presentations(
        &self,
        labeler_dids: &[String],
    ) -> Result<HashMap<String, LabelerPresentation>, String> {
        let now = tokio::time::Instant::now();
        let cached = self.labeler_presentation_cache.lock().await;
        let mut presentations = HashMap::new();
        let missing = labeler_dids
            .iter()
            .filter(|did| {
                if let Some((expires, presentation)) = cached.get(*did)
                    && *expires > now
                {
                    presentations.insert((*did).clone(), presentation.clone());
                    false
                } else {
                    true
                }
            })
            .cloned()
            .collect::<Vec<_>>();
        drop(cached);
        if !missing.is_empty() {
            let mut url = url::Url::parse(&format!(
                "{}app.bsky.labeler.getServices",
                self.public_appview_base
            ))
            .map_err(|_| "Bluesky AppView URL is invalid".to_owned())?;
            {
                let mut query = url.query_pairs_mut();
                for did in &missing {
                    query.append_pair("dids", did);
                }
                query.append_pair("detailed", "true");
            }
            let (response, _) = self.public_appview_json(url, HeaderMap::new()).await?;
            let views = response
                .get("views")
                .and_then(Value::as_array)
                .cloned()
                .unwrap_or_default();
            let mut fetched = HashMap::new();
            for view in views {
                let creator = view.get("creator").unwrap_or(&Value::Null);
                let Some(did) = creator.get("did").and_then(Value::as_str) else {
                    continue;
                };
                let name = creator
                    .get("displayName")
                    .and_then(Value::as_str)
                    .filter(|name| !name.trim().is_empty())
                    .or_else(|| creator.get("handle").and_then(Value::as_str))
                    .unwrap_or(did)
                    .to_owned();
                let handle = creator
                    .get("handle")
                    .and_then(Value::as_str)
                    .filter(|handle| !handle.trim().is_empty())
                    .unwrap_or(did)
                    .to_owned();
                let mut definitions = HashMap::new();
                if let Some(values) = view
                    .get("policies")
                    .and_then(|policies| policies.get("labelValueDefinitions"))
                    .and_then(Value::as_array)
                {
                    for definition in values {
                        let Some(identifier) = definition.get("identifier").and_then(Value::as_str)
                        else {
                            continue;
                        };
                        let locales = definition
                            .get("locales")
                            .and_then(Value::as_array)
                            .cloned()
                            .unwrap_or_default();
                        let locale = locales
                            .iter()
                            .find(|locale| locale.get("lang").and_then(Value::as_str) == Some("en"))
                            .or_else(|| locales.first());
                        let default_name = label_value_default(identifier).name;
                        definitions.insert(
                            identifier.to_owned(),
                            LabelValuePresentation {
                                name: locale
                                    .and_then(|locale| locale.get("name"))
                                    .and_then(Value::as_str)
                                    .filter(|name| !name.trim().is_empty())
                                    .unwrap_or(&default_name)
                                    .to_owned(),
                                description: locale
                                    .and_then(|locale| locale.get("description"))
                                    .and_then(Value::as_str)
                                    .unwrap_or_default()
                                    .to_owned(),
                                severity: definition
                                    .get("severity")
                                    .and_then(Value::as_str)
                                    .unwrap_or("none")
                                    .to_owned(),
                                blurs: definition
                                    .get("blurs")
                                    .and_then(Value::as_str)
                                    .unwrap_or("none")
                                    .to_owned(),
                            },
                        );
                    }
                }
                fetched.insert(
                    did.to_owned(),
                    LabelerPresentation {
                        name,
                        handle,
                        definitions,
                    },
                );
            }
            for did in &missing {
                fetched
                    .entry(did.clone())
                    .or_insert_with(|| LabelerPresentation {
                        name: did.clone(),
                        handle: did.clone(),
                        definitions: HashMap::new(),
                    });
            }
            let mut cache = self.labeler_presentation_cache.lock().await;
            cache.retain(|_, (expires, _)| *expires > tokio::time::Instant::now());
            for (did, presentation) in fetched {
                presentations.insert(did.clone(), presentation.clone());
                if cache.len() < 4096 {
                    cache.insert(
                        did,
                        (
                            tokio::time::Instant::now() + LABELER_PRESENTATION_CACHE_TTL,
                            presentation,
                        ),
                    );
                }
            }
        }
        Ok(presentations)
    }

    async fn fetch_account_labels_for_page(
        &self,
        viewer_did: &str,
        actor_dids: Vec<String>,
    ) -> Result<Vec<AccountLabel>, String> {
        Ok(self
            .fetch_page_account_data_for_page(viewer_did, actor_dids)
            .await?
            .labels)
    }

    async fn fetch_page_account_data_for_page(
        &self,
        viewer_did: &str,
        actor_dids: Vec<String>,
    ) -> Result<PageAccountData, String> {
        let mut actors = actor_dids
            .into_iter()
            .filter(|did| did.parse::<Did>().is_ok())
            .collect::<Vec<_>>();
        actors.sort();
        actors.dedup();
        actors.truncate(100);
        if actors.is_empty() {
            return Ok(PageAccountData::default());
        }
        let now = tokio::time::Instant::now();
        let mut stale = Vec::new();
        let mut cold = Vec::new();
        {
            let cache = self.account_label_actor_cache.lock().await;
            for actor in &actors {
                let key = (viewer_did.to_owned(), actor.clone());
                match cache.get(&key) {
                    Some((expires, _)) if *expires <= now => stale.push(actor.clone()),
                    Some(_) => {}
                    None => cold.push(actor.clone()),
                }
            }
        }
        if !stale.is_empty() {
            self.schedule_account_actor_refresh(viewer_did, stale).await;
        }
        if !cold.is_empty() {
            let mut pending = self.account_label_refreshes.lock().await;
            let refresh = cold
                .into_iter()
                .filter(|actor| pending.insert((viewer_did.to_owned(), actor.clone())))
                .collect::<Vec<_>>();
            drop(pending);
            if !refresh.is_empty() {
                // The refresh runs in a task that owns the pending-set cleanup: a
                // page dropped by its deadline must not strand the (viewer, actor)
                // pairs as pending forever; the task still fills the cache and the
                // next page for the same viewer+actor shows the labels.
                let (done, awaited) = oneshot::channel();
                let flow = self.clone();
                let refresh_viewer = viewer_did.to_owned();
                let dispatcher = tracing::dispatcher::get_default(|dispatcher| dispatcher.clone());
                tokio::spawn(async move {
                            let result = tokio::time::timeout(
                        PAGE_PROFILE_BUDGET,
                        flow.refresh_account_actor_data(&refresh_viewer, refresh.clone()),
                    )
                    .await
                    .unwrap_or_else(|_| {
                        Err("Bluesky account profile request timed out".to_owned())
                    });
                    if let Err(error) = &result {
                        tracing::debug!(%error, %refresh_viewer, "account labels refresh failed");
                    }
                    flow.clear_account_actor_refreshes(&refresh_viewer, &refresh)
                        .await;
                    let _ = done.send(result);
                }.with_subscriber(dispatcher));
                match awaited.await {
                    Ok(Ok(())) => {}
                    Ok(Err(error)) => return Err(error),
                    Err(_) => {
                        return Err("account labels refresh task ended without a result".to_owned());
                    }
                }
            }
        }

        let cache = self.account_label_actor_cache.lock().await;
        let mut data = PageAccountData::default();
        for actor in actors {
            if let Some((_, actor_data)) = cache.get(&(viewer_did.to_owned(), actor)) {
                data.labels.extend(actor_data.labels.iter().cloned());
                if let Some(profile) = &actor_data.profile {
                    data.profiles.push(profile.clone());
                }
            }
        }
        Ok(data)
    }

    async fn schedule_account_actor_refresh(&self, viewer_did: &str, actors: Vec<String>) {
        let mut pending = self.account_label_refreshes.lock().await;
        let refresh = actors
            .into_iter()
            .filter(|actor| pending.insert((viewer_did.to_owned(), actor.clone())))
            .collect::<Vec<_>>();
        drop(pending);
        if refresh.is_empty() {
            return;
        }
        let flow = self.clone();
        let viewer_did = viewer_did.to_owned();
        tokio::spawn(async move {
            let result = tokio::time::timeout(
                PAGE_PROFILE_BUDGET,
                flow.refresh_account_actor_data(&viewer_did, refresh.clone()),
            )
            .await
            .unwrap_or_else(|_| Err("Bluesky account profile request timed out".to_owned()));
            if let Err(error) = result {
                tracing::debug!(%error, %viewer_did, "account labels refresh failed");
            }
            flow.clear_account_actor_refreshes(&viewer_did, &refresh)
                .await;
        });
    }

    async fn clear_account_actor_refreshes(&self, viewer_did: &str, actors: &[String]) {
        let mut pending = self.account_label_refreshes.lock().await;
        for actor in actors {
            pending.remove(&(viewer_did.to_owned(), actor.clone()));
        }
    }

    /// Requested labeler DIDs missing from an `atproto-content-labelers` response
    /// header. Entries look like `did:plc:x;redact,did:plc:y`; the DID is what counts.
    fn requested_labelers_not_applied(requested: &[String], applied: Option<&str>) -> Vec<String> {
        let applied_dids: HashSet<&str> = applied
            .unwrap_or_default()
            .split(',')
            .map(|entry| entry.split(';').next().unwrap_or_default().trim())
            .filter(|did| !did.is_empty())
            .collect();
        requested
            .iter()
            .filter(|did| !applied_dids.contains(did.as_str()))
            .cloned()
            .collect()
    }

    async fn refresh_account_actor_data(
        &self,
        viewer_did: &str,
        actors: Vec<String>,
    ) -> Result<(), String> {
        let labeler_dids = self.labeler_dids_for_viewer(viewer_did).await?;
        let accepted_labelers = labeler_dids.join(",");
        let mut requests = Vec::new();
        for batch in actors.chunks(ACCOUNT_LABEL_PROFILE_BATCH_SIZE) {
            // Not public_appview_base: public.api.bsky.app ignores third-party
            // labelers in atproto-accept-labelers. See LABELLED_APPVIEW_BASE.
            let mut url = url::Url::parse(&format!(
                "{}app.bsky.actor.getProfiles",
                self.labelled_appview_base
            ))
            .map_err(|_| "Bluesky AppView URL is invalid".to_owned())?;
            {
                let mut query = url.query_pairs_mut();
                for did in batch {
                    query.append_pair("actors", did);
                }
            }
            let mut headers = HeaderMap::new();
            headers.insert(
                "atproto-accept-labelers",
                HeaderValue::from_str(&accepted_labelers)
                    .map_err(|_| "labeler preference header is invalid".to_owned())?,
            );
            requests.push(self.public_appview_json(url, headers));
        }
        let responses = futures_util::future::join_all(requests).await;
        let mut profiles = HashMap::new();
        let mut account_raw_labels = HashMap::<String, Vec<(String, String)>>::new();
        for response in responses {
            let (response, applied_labelers) = response?;
            let unapplied =
                Self::requested_labelers_not_applied(&labeler_dids, applied_labelers.as_deref());
            if !unapplied.is_empty() {
                tracing::warn!(
                    requested_labelers = %accepted_labelers,
                    applied_labelers = applied_labelers.as_deref().unwrap_or("<missing>"),
                    unapplied_labelers = %unapplied.join(","),
                    "Bluesky getProfiles did not apply every requested labeler"
                );
            }
            if let Some(profile_values) = response.get("profiles").and_then(Value::as_array) {
                for profile in profile_values {
                    let Some(subject_did) = profile.get("did").and_then(Value::as_str) else {
                        continue;
                    };
                    if actors.iter().any(|actor| actor == subject_did)
                        && let Some(profile_view) = crate::feeds::parse_public_profile_view(profile)
                    {
                        profiles.insert(subject_did.to_owned(), profile_view);
                    }
                    for label in profile
                        .get("labels")
                        .and_then(Value::as_array)
                        .into_iter()
                        .flatten()
                    {
                        let (Some(labeler_did), Some(uri), Some(value)) = (
                            label.get("src").and_then(Value::as_str),
                            label.get("uri").and_then(Value::as_str),
                            label.get("val").and_then(Value::as_str),
                        ) else {
                            continue;
                        };
                        if uri == subject_did
                            && label.get("neg").and_then(Value::as_bool) != Some(true)
                            && labeler_dids.iter().any(|accepted| accepted == labeler_did)
                        {
                            account_raw_labels
                                .entry(subject_did.to_owned())
                                .or_default()
                                .push((labeler_did.to_owned(), value.to_owned()));
                        }
                    }
                }
            }
        }
        let presentations = self.labeler_presentations(&labeler_dids).await?;
        let mut fetched = HashMap::new();
        for actor in &actors {
            let labels = account_raw_labels
                .remove(actor)
                .unwrap_or_default()
                .into_iter()
                .map(|(labeler_did, value)| {
                    let presentation = presentations.get(&labeler_did);
                    let global = label_value_default(&value);
                    let definition =
                        presentation.and_then(|presentation| presentation.definitions.get(&value));
                    AccountLabel {
                        subject_did: actor.clone(),
                        labeler_did: labeler_did.clone(),
                        labeler_name: presentation
                            .map(|presentation| presentation.name.clone())
                            .unwrap_or_else(|| labeler_did.clone()),
                        labeler_handle: presentation
                            .map(|presentation| presentation.handle.clone())
                            .unwrap_or_else(|| labeler_did.clone()),
                        value,
                        display_name: definition
                            .map(|definition| definition.name.clone())
                            .unwrap_or(global.name),
                        description: definition
                            .map(|definition| definition.description.clone())
                            .unwrap_or_default(),
                        severity: definition
                            .map(|definition| definition.severity.clone())
                            .unwrap_or(global.severity),
                        blurs: definition
                            .map(|definition| definition.blurs.clone())
                            .unwrap_or(global.blurs),
                    }
                })
                .collect();
            fetched.insert(
                actor.clone(),
                AccountActorData {
                    labels,
                    profile: profiles.remove(actor),
                },
            );
        }
        let now = tokio::time::Instant::now();
        let mut cache = self.account_label_actor_cache.lock().await;
        cache.retain(|_, (expires, _)| *expires > now);
        for (actor, actor_data) in fetched {
            let key = (viewer_did.to_owned(), actor);
            if cache.len() >= 8192
                && !cache.contains_key(&key)
                && let Some(oldest) = cache.keys().next().cloned()
            {
                cache.remove(&oldest);
            }
            cache.insert(key, (now + ACCOUNT_LABEL_CACHE_TTL, actor_data));
        }
        Ok(())
    }

    async fn set_muted_actor(&self, did: &str, subject: &str, muted: bool) -> Result<(), String> {
        let parsed_did = did
            .parse::<Did>()
            .map_err(|_| "viewer DID is invalid".to_owned())?;
        let actor = subject
            .parse::<Did>()
            .map(AtIdentifier::Did)
            .map_err(|_| "profile DID is invalid".to_owned())?;
        let session = self.restored_session(&parsed_did).await?;
        let proxy_did = BSKY_APPVIEW_DID
            .parse::<Did>()
            .map_err(|_| "Bluesky AppView service DID is invalid".to_owned())?;
        let api = Self::session_agent(&session).api_with_proxy(proxy_did, BSKY_APPVIEW_SERVICE);
        if muted {
            api.app
                .bsky
                .graph
                .mute_actor(atrium_api::app::bsky::graph::mute_actor::InputData { actor }.into())
                .await
                .map_err(|error| format!("Bluesky mute request failed: {error:?}"))?;
        } else {
            api.app
                .bsky
                .graph
                .unmute_actor(
                    atrium_api::app::bsky::graph::unmute_actor::InputData { actor }.into(),
                )
                .await
                .map_err(|error| format!("Bluesky unmute request failed: {error:?}"))?;
        }
        self.muted_actor_cache.lock().await.remove(did);
        if let Err(error) = self
            .store
            .web_cache_delete(&muted_actor_cache_key(did))
            .await
        {
            tracing::warn!(%error, "could not invalidate persisted muted actor list");
        }
        Ok(())
    }

    /// putPreferences replaces the WHOLE preference set, so this reads it fresh from the PDS (never
    /// from our cache), removes only the one feed and writes everything else back as it came. A
    /// failed or malformed read writes nothing.
    async fn remove_saved_feed_preference(&self, did: &str, feed_uri: &str) -> Result<(), String> {
        let parsed_did = did
            .parse::<Did>()
            .map_err(|_| "viewer DID is invalid".to_owned())?;
        let session = self.restored_session(&parsed_did).await?;
        let read = XrpcRequest {
            method: http::Method::GET,
            nsid: "app.bsky.actor.getPreferences".to_owned(),
            parameters: Some(json!({})),
            input: None,
            encoding: None,
        };
        let response: OutputDataOrBytes<Value> = session
            .send_xrpc::<Value, Value, Value, Value>(&read)
            .await
            .map_err(|error| {
                log_pds_xrpc_failure("app.bsky.actor.getPreferences", &error);
                format!("PDS getPreferences request failed: {error:?}")
            })?;
        let OutputDataOrBytes::Data(response) = response else {
            return Err("PDS getPreferences returned a non-JSON response".to_owned());
        };
        let preferences = response
            .get("preferences")
            .and_then(Value::as_array)
            .ok_or_else(|| "PDS getPreferences response has no preferences array".to_owned())?;
        let updated = crate::feeds::remove_saved_feed_from_preferences(preferences, feed_uri)
            .ok_or_else(|| "that feed is not in your saved feeds".to_owned())?;
        let write = XrpcRequest {
            method: http::Method::POST,
            nsid: "app.bsky.actor.putPreferences".to_owned(),
            parameters: None,
            input: Some(InputDataOrBytes::Data(json!({ "preferences": updated }))),
            encoding: Some("application/json".to_owned()),
        };
        session
            .send_xrpc::<(), Value, Value, Value>(&write)
            .await
            .map_err(|error| {
                log_pds_xrpc_failure("app.bsky.actor.putPreferences", &error);
                format!("PDS putPreferences request failed: {error:?}")
            })?;
        tracing::info!(
            did,
            feed = feed_uri,
            "removed a saved feed from preferences"
        );
        self.saved_feed_cache.lock().await.remove(did);
        if let Err(error) = self
            .store
            .web_cache_delete(&saved_feed_cache_key(did))
            .await
        {
            tracing::warn!(%error, "could not invalidate persisted saved feed preferences");
        }
        Ok(())
    }

    async fn pds_service_did(&self, viewer_did: &str) -> Result<String, String> {
        let did = viewer_did
            .parse::<Did>()
            .map_err(|_| "viewer DID is invalid".to_owned())?;
        let session = self.restored_session(&did).await?;
        service_auth_audience_from_url(&session.base_uri())
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
        let session = self.restored_session(&did).await?;
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
        self.video_service_json_with_token(&token, lexicon, parameters)
            .await
    }

    async fn video_service_json_with_token(
        &self,
        token: &str,
        lexicon: &str,
        parameters: &[(&str, &str)],
    ) -> Result<Value, String> {
        let mut url = reqwest::Url::parse(&self.video_service_url)
            .map_err(|error| format!("video service URL is invalid: {error}"))?;
        url.set_path(&format!("/xrpc/{lexicon}"));
        url.query_pairs_mut()
            .extend_pairs(parameters.iter().copied());
        let call_timer =
            OutboundCallTimer::new(&axum::http::Method::GET, &url, std::time::Instant::now());
        let response = self
            .video_http_client
            .get(url)
            .bearer_auth(token)
            .send()
            .await
            .map_err(|error| format!("video service {lexicon} request failed: {error}"))?;
        let result = read_video_service_response(response).await;
        drop(call_timer);
        result
    }

    async fn upload_video_blob(&self, did: &str, video: &PostVideo) -> Result<Value, String> {
        let limits = self
            .video_service_json(did, "app.bsky.video.getUploadLimits", &[])
            .await?;
        validate_video_upload_limits(&limits, video.size_bytes)?;

        let pds_service_did = self.pds_service_did(did).await?;
        let token = self
            .service_auth_token(did, &pds_service_did, "com.atproto.repo.uploadBlob")
            .await?;
        let mut url = reqwest::Url::parse(&self.video_service_url)
            .map_err(|error| format!("video service URL is invalid: {error}"))?;
        url.set_path("/xrpc/app.bsky.video.uploadVideo");
        url.query_pairs_mut()
            .append_pair("did", did)
            .append_pair("name", &video.filename);
        let file = tokio::fs::File::open(&video.path)
            .await
            .map_err(|error| format!("temporary video file could not be reopened: {error}"))?;
        let body = reqwest::Body::wrap_stream(tokio_util::io::ReaderStream::new(file));
        let call_timer =
            OutboundCallTimer::new(&axum::http::Method::POST, &url, std::time::Instant::now());
        let response = self
            .video_http_client
            .post(url)
            .bearer_auth(&token)
            .header(header::CONTENT_TYPE, &video.mime_type)
            .header(header::CONTENT_LENGTH, video.size_bytes)
            .body(body)
            .send()
            .await
            .map_err(|error| format!("video service upload request failed: {error}"))?;
        let initial_result = read_video_service_response(response).await;
        drop(call_timer);
        let initial = initial_result?;
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
        let poll_token = token.clone();
        poll_video_job(
            job_id,
            VIDEO_JOB_POLL_INTERVAL,
            VIDEO_JOB_MAX_POLLS,
            |job_id| {
                let service = self.clone();
                let token = poll_token.clone();
                Box::pin(async move {
                    service
                        .video_service_json_with_token(
                            &token,
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
        let session = self.restored_session(&did).await?;
        let agent = Self::session_agent(&session);
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
        let session = self.restored_session(&parsed_did).await?;
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
        let session = self.restored_session(&parsed_did).await?;
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
        self.delete_repo_record(did, "app.bsky.feed.like", like_uri)
            .await
    }

    async fn delete_repo_record(
        &self,
        did: &str,
        collection: &str,
        uri: &str,
    ) -> Result<(), String> {
        let prefix = format!("at://{did}/{collection}/");
        let rkey = uri
            .strip_prefix(&prefix)
            .filter(|rkey| !rkey.is_empty() && !rkey.contains('/'))
            .ok_or_else(|| {
                "record URI does not belong to this account and collection".to_owned()
            })?;
        let did = did.parse::<Did>().map_err(|error| error.to_string())?;
        let session = self.restored_session(&did).await?;
        let agent = Self::session_agent(&session);
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
                let _ = self.revoke_session(&parsed_did).await;
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
                let _ = self.revoke_session(&parsed_did).await;
                let _ = self.store.del(&parsed_did).await;
                return Err("OAuth token response did not grant the atproto scope".into());
            }
            if did != expected.did {
                let _ = self.revoke_session(&parsed_did).await;
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
            self.revoke_session(&did).await
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

    fn fetch_public_posts<'a>(
        &'a self,
        uris: &'a [String],
    ) -> Pin<Box<dyn Future<Output = HashMap<String, crate::feeds::FetchedPublicPost>> + Send + 'a>>
    {
        let uris = uris.to_vec();
        Box::pin(async move { self.feed_client.fetch_posts(&uris).await })
    }

    fn supports_public_post_batching(&self) -> bool {
        true
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

    fn create_profile_graph_record<'a>(
        &'a self,
        did: &'a str,
        collection: &'a str,
        record: Value,
    ) -> Pin<Box<dyn Future<Output = Result<PdsCreatedRecord, String>> + Send + 'a>> {
        Box::pin(async move { self.create_record(did, collection, record).await })
    }

    fn delete_profile_graph_record<'a>(
        &'a self,
        did: &'a str,
        collection: &'a str,
        uri: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move { self.delete_repo_record(did, collection, uri).await })
    }

    fn set_actor_muted<'a>(
        &'a self,
        did: &'a str,
        subject: &'a str,
        muted: bool,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move { self.set_muted_actor(did, subject, muted).await })
    }

    fn remove_saved_feed<'a>(
        &'a self,
        did: &'a str,
        feed_uri: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move { self.remove_saved_feed_preference(did, feed_uri).await })
    }

    fn muted_actor_dids<'a>(
        &'a self,
        did: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<HashSet<String>, String>> + Send + 'a>> {
        Box::pin(async move { self.fetch_muted_actor_dids(did).await })
    }

    fn subscribed_block_list_uris<'a>(
        &'a self,
        did: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<String>, String>> + Send + 'a>> {
        Box::pin(async move {
            let records = self
                .list_repo_collection(did, "app.bsky.graph.listblock")
                .await?;
            let mut uris = records
                .iter()
                .filter_map(|record| {
                    let subject = record.get("value")?.get("subject")?.as_str()?;
                    subject
                        .contains("/app.bsky.graph.list/")
                        .then(|| subject.to_owned())
                })
                .collect::<Vec<_>>();
            uris.sort();
            uris.dedup();
            Ok(uris)
        })
    }

    fn block_list_detail<'a>(
        &'a self,
        list_uri: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<BlockListDetail, String>> + Send + 'a>> {
        Box::pin(async move { self.fetch_block_list_members(list_uri).await })
    }

    fn web_cache_store(&self) -> Option<SqliteOAuthStore> {
        Some(self.store.clone())
    }

    fn profile_list_choices<'a>(
        &'a self,
        did: &'a str,
        subject: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ProfileListChoice>, String>> + Send + 'a>> {
        Box::pin(async move {
            let lists = self
                .list_repo_collection(did, "app.bsky.graph.list")
                .await?;
            let items = self
                .list_repo_collection(did, "app.bsky.graph.listitem")
                .await?;
            let memberships = items
                .iter()
                .filter_map(|item| {
                    let value = item.get("value")?;
                    (value.get("subject").and_then(Value::as_str) == Some(subject))
                        .then(|| {
                            Some((
                                value.get("list")?.as_str()?.to_owned(),
                                item.get("uri")?.as_str()?.to_owned(),
                            ))
                        })
                        .flatten()
                })
                .collect::<HashMap<_, _>>();
            let mut choices = lists
                .iter()
                .filter_map(|list| {
                    let value = list.get("value")?;
                    let purpose = value.get("purpose")?.as_str()?;
                    if !matches!(
                        purpose,
                        "app.bsky.graph.defs#curatelist" | "app.bsky.graph.defs#modlist"
                    ) {
                        return None;
                    }
                    let uri = list.get("uri")?.as_str()?.to_owned();
                    Some(ProfileListChoice {
                        item_uri: memberships.get(&uri).cloned(),
                        uri,
                        name: value
                            .get("name")
                            .and_then(Value::as_str)
                            .unwrap_or("Untitled list")
                            .to_owned(),
                        purpose: purpose.to_owned(),
                    })
                })
                .collect::<Vec<_>>();
            choices.sort_by_key(|choice| choice.name.to_lowercase());
            Ok(choices)
        })
    }

    fn account_labels_for_page<'a>(
        &'a self,
        viewer_did: &'a str,
        actor_dids: Vec<String>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<AccountLabel>, String>> + Send + 'a>> {
        Box::pin(async move {
            self.fetch_account_labels_for_page(viewer_did, actor_dids)
                .await
        })
    }

    fn page_account_data_for_page<'a>(
        &'a self,
        viewer_did: &'a str,
        actor_dids: Vec<String>,
    ) -> Pin<Box<dyn Future<Output = Result<PageAccountData, String>> + Send + 'a>> {
        Box::pin(async move {
            self.fetch_page_account_data_for_page(viewer_did, actor_dids)
                .await
        })
    }

    fn timeline<'a>(
        &'a self,
        did: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Value>, String>> + Send + 'a>> {
        Box::pin(async move {
            let did = did.parse::<Did>().map_err(|error| error.to_string())?;
            let session = self.restored_session(&did).await?;
            let agent = Self::session_agent(&session);
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

async fn vendor_asset(AxumPath(path): AxumPath<String>) -> Response {
    let (bytes, content_type): (&'static [u8], &'static str) = match path.as_str() {
        "hls.min.js" => (
            include_bytes!("../static/vendor/hls.min.js"),
            "text/javascript; charset=utf-8",
        ),
        "LICENSE-hls.js" => (
            include_bytes!("../static/vendor/LICENSE-hls.js"),
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

/// Linked from a `<noscript>` tag in the page head; restores the full composer form to visitors without JavaScript.
async fn nojs_stylesheet() -> Response {
    let mut response = include_str!("../static/nojs.css").into_response();
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
            "<nav class=\"primary-nav\" aria-label=\"Main navigation\"><a href=\"/app\">{}<span>Home</span></a><a href=\"/feeds\">{}<span>Feeds</span></a><a class=\"notification-nav-link\" href=\"/notifications\">{}<span>Notifications</span>{}</a><a href=\"/profile\">{}<span>Profile</span></a><a href=\"/app/block-lists\">{}<span>Block lists</span></a><a class=\"compose-link\" href=\"/app#new-post\">{}<span>New post</span></a><form action=\"/logout\" method=\"post\"><button type=\"submit\">Sign out</button></form></nav>",
            icon_svg("home"),
            icon_svg("feeds"),
            icon_svg("notifications"),
            notification_badge_markup(unread_count),
            icon_svg("profile"),
            icon_svg("block-lists"),
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
    let body_open = match current_request_timing_id() {
        Some(timing_id) => format!("<body data-timing-id=\"{timing_id}\">"),
        None => "<body>".to_owned(),
    };
    format!(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1, viewport-fit=cover\"><title>{}</title><link rel=\"stylesheet\" href=\"{}\"><noscript><link rel=\"stylesheet\" href=\"{}\"></noscript><script src=\"{}\" defer></script><script src=\"{}\" defer></script></head>{}{}<a class=\"skip-link\" href=\"#main-content\">Skip to content</a>{mobile_navigation}<div class=\"{layout}\"><aside class=\"left-sidebar\"><a class=\"brand\" href=\"/app\">AppView</a>{navigation}</aside><div class=\"center-column\"><header class=\"center-sticky\">{tabs}</header><main id=\"main-content\" class=\"page-content\">{content}</main></div>{right_sidebar}</div></body></html>",
        html_escape(title),
        WEB_STYLESHEET_URL,
        WEB_NOJS_STYLESHEET_URL,
        HLS_SCRIPT_URL,
        WEB_SCRIPT_URL,
        body_open,
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

#[derive(Clone, Debug, Default)]
struct QuoteRenderIndex {
    quotes: HashMap<String, QuoteRenderState>,
    account_labels: HashMap<String, Vec<AccountLabel>>,
}

impl QuoteRenderIndex {
    fn new() -> Self {
        Self::default()
    }
}

impl std::ops::Deref for QuoteRenderIndex {
    type Target = HashMap<String, QuoteRenderState>;

    fn deref(&self) -> &Self::Target {
        &self.quotes
    }
}

impl std::ops::DerefMut for QuoteRenderIndex {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.quotes
    }
}

struct PageContentHydration {
    quote_index: QuoteRenderIndex,
    profile_hydration_ms: u64,
    quote_hydration_ms: u64,
    account_labels_ms: u64,
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

/// Response header carrying the request's timing console id.
const TIMING_ID_HEADER: HeaderName = HeaderName::from_static("x-timing-id");

tokio::task_local! {
    static REQUEST_TIMING_ID: String;
    static WEB_PAGE_PHASE_DETAIL: WebPagePhaseDetail;
}

async fn with_request_timing_id<F: Future>(timing_id: String, future: F) -> F::Output {
    REQUEST_TIMING_ID.scope(timing_id, future).await
}

async fn with_web_page_phase_detail<F: Future>(detail: WebPagePhaseDetail, future: F) -> F::Output {
    WEB_PAGE_PHASE_DETAIL.scope(detail, future).await
}

tokio::task_local! {
    static WEB_WRITE_FAILURE: Arc<Mutex<Option<String>>>;
}

async fn with_web_write_failure<F: Future>(
    slot: Arc<Mutex<Option<String>>>,
    future: F,
) -> F::Output {
    WEB_WRITE_FAILURE.scope(slot, future).await
}

/// Keep the first failed write's real error text so the `web write` minute line can carry it.
fn record_web_write_failure(error: &str) {
    let _ = WEB_WRITE_FAILURE.try_with(|slot| {
        if let Ok(mut slot) = slot.lock()
            && slot.is_none()
        {
            *slot = Some(error.to_owned());
        }
    });
}

/// The browser-facing `<short reason>`: the real text collapsed and truncated, or a pointer to
/// the log when the text carries token-shaped or filesystem-shaped secrets (tokens, DPoP keys,
/// refresh tokens and internal paths stay out of responses; the warn line carries the full text).
fn write_reason_for_browser(error: &str) -> String {
    let collapsed = error.split_whitespace().collect::<Vec<_>>().join(" ");
    let lower = collapsed.to_ascii_lowercase();
    const SECRET_MARKERS: [&str; 9] = [
        "access_token",
        "access token",
        "refresh_token",
        "refresh token",
        "client_assertion",
        "dpop_key",
        "/srv/",
        "/var/",
        "/etc/",
    ];
    if SECRET_MARKERS.iter().any(|marker| lower.contains(marker)) {
        return "see the server log for the reason".to_owned();
    }
    if collapsed.chars().count() <= 240 {
        return collapsed;
    }
    let mut shortened = collapsed.chars().take(240).collect::<String>();
    shortened.push('…');
    shortened
}

fn oauth_named_error(status: StatusCode, message: &str) -> Response {
    (
        status,
        Json(json!({"error":"OAuthError","message":message})),
    )
        .into_response()
}

/// The one response every web write handler returns when the write failed: the real error is
/// warned with route + DID, recorded for the `web write` line, and the user gets an error whose
/// message names what failed instead of "the request could not be completed".
fn web_write_response(
    public: &OAuthPublic,
    headers: &HeaderMap,
    route: &str,
    verb: &str,
    did: &str,
    status: StatusCode,
    error: &str,
) -> Response {
    if take_dead_oauth_session().is_some() {
        return session_expired_response(public, headers);
    }
    tracing::warn!(route, did = %did, error = %error, "web write failed");
    record_web_write_failure(error);
    let message = if status == StatusCode::BAD_GATEWAY {
        format!(
            "Bluesky did not accept the {verb}: {}",
            write_reason_for_browser(error)
        )
    } else {
        format!(
            "This appview could not finish the {verb}: {}",
            write_reason_for_browser(error)
        )
    };
    oauth_named_error(status, &message)
}

fn current_request_timing_id() -> Option<String> {
    REQUEST_TIMING_ID.try_with(Clone::clone).ok()
}

/// Where the page handler's `WebPageTiming` drop leaves its phase detail so the timing
/// middleware can store it in the history after the response is built.
#[derive(Clone, Default)]
struct WebPagePhaseDetail(Arc<Mutex<Option<(String, Value)>>>);

impl WebPagePhaseDetail {
    fn set(&self, page: &str, fields: Value) {
        if let Ok(mut slot) = self.0.lock() {
            *slot = Some((page.to_owned(), fields));
        }
    }

    fn take(&self) -> (Option<String>, Option<Value>) {
        self.0
            .lock()
            .ok()
            .and_then(|mut slot| slot.take())
            .map(|(page, fields)| (Some(page), Some(fields)))
            .unwrap_or_default()
    }
}

async fn account_web_outbound_calls<F: OAuthFlow, S: Storage + 'static>(
    State(state): State<Arc<OAuthWebState<F, S>>>,
    request: Request,
    next: Next,
) -> Response {
    let write_route = if request.method() == axum::http::Method::POST {
        match request.uri().path() {
            "/like" => Some("like"),
            "/repost" => Some("repost"),
            "/reply" => Some("reply"),
            "/post" => Some("post"),
            _ => None,
        }
    } else {
        None
    };
    let timing_id = new_timing_id();
    let timing_self_request = request.uri().path() == TIMING_RECENT_PATH;
    let timing_method = request.method().to_string();
    let timing_path = request.uri().path().to_owned();
    let timing_viewer =
        verified_cookie_did(&state.public, request.headers(), state.storage.as_ref());
    let started = std::time::Instant::now();
    let started_at_ms = now_unix_ms();
    let accounting = RequestOutboundAccounting::default();
    let page_phases = WebPagePhaseDetail::default();
    let write_failure = Arc::new(Mutex::new(None::<String>));
    let dead_session = Arc::new(Mutex::new(None::<String>));
    let response = with_request_outbound_accounting(
        accounting.clone(),
        with_web_page_phase_detail(
            page_phases.clone(),
            with_request_timing_id(
                timing_id.clone(),
                with_web_write_failure(
                    write_failure.clone(),
                    OAUTH_SESSION_DEAD_DID.scope(dead_session, next.run(request)),
                ),
            ),
        ),
    )
    .await;
    let mut response = response;
    match HeaderValue::from_str(&timing_id) {
        Ok(value) => {
            response.headers_mut().insert(TIMING_ID_HEADER, value);
        }
        Err(error) => tracing::warn!(
            error = %error,
            timing_id = %timing_id,
            "could not send the X-Timing-Id response header"
        ),
    }
    if !timing_self_request {
        let calls = accounting.snapshot();
        let (page, phases) = page_phases.take();
        state.timing_console.record(TimingRecord {
            id: timing_id.clone(),
            viewer: timing_viewer,
            method: timing_method,
            path: timing_path,
            status: response.status().as_u16(),
            started_at_ms,
            total_ms: started.elapsed().as_millis().min(u64::MAX as u128) as u64,
            page,
            phases,
            calls: calls.calls,
            calls_overflow: calls.calls_overflow,
        });
    }
    if let Some(route) = write_route {
        let calls = accounting.snapshot();
        let failure = write_failure
            .lock()
            .ok()
            .and_then(|slot| slot.clone())
            .unwrap_or_default();
        tracing::info!(
            route,
            total_ms = started.elapsed().as_millis() as u64,
            outbound_call_count = calls.total_calls,
            outbound_call_ms = calls.total_ms,
            dpop_nonce_retries = calls.dpop_nonce_retries,
            dpop_nonce_retry = calls.dpop_nonce_retries > 0,
            outbound_calls = %format_outbound_calls(&calls),
            error = %failure,
            "web write"
        );
    }
    response
}

/// The timing console endpoint: the logged-in viewer's own newest records, newest first.
/// Records of other viewers and records without a viewer are never returned, and this
/// endpoint's own requests are never recorded (it must not fill the history with itself).
async fn timing_recent<F: OAuthFlow, S: Storage + 'static>(
    State(state): State<Arc<OAuthWebState<F, S>>>,
    headers: HeaderMap,
) -> Response {
    let Some(did) = verified_cookie_did(&state.public, &headers, state.storage.as_ref()) else {
        return StatusCode::UNAUTHORIZED.into_response();
    };
    let records = state
        .timing_console
        .recent_for_viewer(&did, TIMING_PAGE_LIMIT);
    (
        [(header::CACHE_CONTROL, HeaderValue::from_static("no-store"))],
        Json(json!({"records": records})),
    )
        .into_response()
}

async fn log_web_post_client_errors(request: Request, next: Next) -> Response {
    let route = match request.uri().path() {
        "/link-preview" => Some("/link-preview"),
        "/post" => Some("/post"),
        _ => None,
    };
    let response = next.run(request).await;
    if let Some(route) = route
        && response.status().is_client_error()
    {
        let status = response.status();
        tracing::warn!(
            route,
            status = status.as_u16(),
            reason = status.canonical_reason().unwrap_or("client error"),
            "web post request rejected"
        );
    }
    response
}

fn format_outbound_calls(calls: &OutboundCallSnapshot) -> String {
    if calls.by_kind.is_empty() {
        return "none".to_owned();
    }
    calls
        .by_kind
        .iter()
        .map(|(kind, (count, duration_ms))| format!("{kind}:{count}/{duration_ms}ms"))
        .collect::<Vec<_>>()
        .join(",")
}

struct WebPageTiming {
    page: &'static str,
    started: std::time::Instant,
    session_token_refresh_ms: u64,
    saved_feeds_ms: u64,
    preferences_ms: u64,
    tabs_ms: u64,
    timeline_fetch_index_ms: u64,
    timeline_fetch_live_ms: u64,
    feed_timeline_ms: u64,
    mutes_ms: u64,
    list_blocks_ms: u64,
    list_blocked_posts: u64,
    parent_hydration_ms: u64,
    content_hydration_ms: u64,
    profile_hydration_ms: u64,
    quote_hydration_ms: u64,
    account_labels_ms: u64,
    profile_counts_ms: u64,
    relationship_ms: u64,
    relationship_requests: u32,
    relationship_rows_written: u32,
    relationship_answer: &'static str,
    profile_action_panel_ms: u64,
    profile_action_mutes_ms: u64,
    profile_list_choices_ms: u64,
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
            saved_feeds_ms: 0,
            preferences_ms: 0,
            tabs_ms: 0,
            timeline_fetch_index_ms: 0,
            timeline_fetch_live_ms: 0,
            feed_timeline_ms: 0,
            mutes_ms: 0,
            list_blocks_ms: 0,
            list_blocked_posts: 0,
            parent_hydration_ms: 0,
            content_hydration_ms: 0,
            profile_hydration_ms: 0,
            quote_hydration_ms: 0,
            account_labels_ms: 0,
            profile_counts_ms: 0,
            relationship_ms: 0,
            relationship_requests: 0,
            relationship_rows_written: 0,
            relationship_answer: "none",
            profile_action_panel_ms: 0,
            profile_action_mutes_ms: 0,
            profile_list_choices_ms: 0,
            render_ms: 0,
            ancestors_from_index: 0,
            ancestors_live: 0,
            ancestors_timed_out: 0,
        }
    }
}

impl Drop for WebPageTiming {
    fn drop(&mut self) {
        let total_ms = self.started.elapsed().as_millis() as u64;
        // The *_hydration_ms, preferences_ms/tabs_ms, and profile action subphase fields are
        // details nested within their enclosing wall-clock phase. Keep those details out of this
        // additive coverage sum because hydration runs its profile, quote, and label futures in
        // parallel.
        let timeline_phase_ms = if self.page == "feed" {
            self.feed_timeline_ms
        } else {
            self.timeline_fetch_index_ms
                .saturating_add(self.timeline_fetch_live_ms)
        };
        let session_phase_ms = if self.page == "feed" {
            0
        } else {
            self.session_token_refresh_ms
        };
        let phase_sum_ms = session_phase_ms
            .saturating_add(self.saved_feeds_ms)
            .saturating_add(timeline_phase_ms)
            .saturating_add(self.mutes_ms)
            .saturating_add(self.list_blocks_ms)
            .saturating_add(self.parent_hydration_ms)
            .saturating_add(self.content_hydration_ms)
            .saturating_add(self.profile_counts_ms)
            .saturating_add(self.relationship_ms)
            .saturating_add(self.profile_action_panel_ms)
            .saturating_add(self.render_ms);
        let phase_gap_ms = total_ms as i64 - phase_sum_ms as i64;
        let calls = current_outbound_call_snapshot();
        tracing::info!(
            page = self.page,
            session_token_refresh_ms = self.session_token_refresh_ms,
            saved_feeds_ms = self.saved_feeds_ms,
            preferences_ms = self.preferences_ms,
            tabs_ms = self.tabs_ms,
            timeline_fetch_index_ms = self.timeline_fetch_index_ms,
            timeline_fetch_live_ms = self.timeline_fetch_live_ms,
            feed_timeline_ms = self.feed_timeline_ms,
            mutes_ms = self.mutes_ms,
            list_blocks_ms = self.list_blocks_ms,
            list_blocked_posts = self.list_blocked_posts,
            parent_hydration_ms = self.parent_hydration_ms,
            content_hydration_ms = self.content_hydration_ms,
            profile_hydration_ms = self.profile_hydration_ms,
            quote_hydration_ms = self.quote_hydration_ms,
            account_labels_ms = self.account_labels_ms,
            profile_counts_ms = self.profile_counts_ms,
            relationship_ms = self.relationship_ms,
            relationship_requests = self.relationship_requests,
            relationship_rows_written = self.relationship_rows_written,
            relationship_answer = self.relationship_answer,
            profile_action_panel_ms = self.profile_action_panel_ms,
            profile_action_mutes_ms = self.profile_action_mutes_ms,
            profile_list_choices_ms = self.profile_list_choices_ms,
            render_ms = self.render_ms,
            ancestors_from_index = self.ancestors_from_index,
            ancestors_live = self.ancestors_live,
            ancestors_timed_out = self.ancestors_timed_out,
            outbound_call_count = calls.total_calls,
            outbound_call_ms = calls.total_ms,
            outbound_calls = %format_outbound_calls(&calls),
            dpop_nonce_retries = calls.dpop_nonce_retries,
            phase_sum_ms,
            phase_gap_ms,
            total_ms,
            "web page phases"
        );
        if let Ok(detail) = WEB_PAGE_PHASE_DETAIL.try_with(Clone::clone) {
            let named: &[(&str, u64)] = &[
                ("session_token_refresh_ms", self.session_token_refresh_ms),
                ("saved_feeds_ms", self.saved_feeds_ms),
                ("preferences_ms", self.preferences_ms),
                ("tabs_ms", self.tabs_ms),
                ("timeline_fetch_index_ms", self.timeline_fetch_index_ms),
                ("timeline_fetch_live_ms", self.timeline_fetch_live_ms),
                ("feed_timeline_ms", self.feed_timeline_ms),
                ("mutes_ms", self.mutes_ms),
                ("list_blocks_ms", self.list_blocks_ms),
                ("list_blocked_posts", self.list_blocked_posts),
                ("parent_hydration_ms", self.parent_hydration_ms),
                ("content_hydration_ms", self.content_hydration_ms),
                ("profile_hydration_ms", self.profile_hydration_ms),
                ("quote_hydration_ms", self.quote_hydration_ms),
                ("account_labels_ms", self.account_labels_ms),
                ("profile_counts_ms", self.profile_counts_ms),
                ("relationship_ms", self.relationship_ms),
                (
                    "relationship_requests",
                    u64::from(self.relationship_requests),
                ),
                (
                    "relationship_rows_written",
                    u64::from(self.relationship_rows_written),
                ),
                ("profile_action_panel_ms", self.profile_action_panel_ms),
                ("profile_action_mutes_ms", self.profile_action_mutes_ms),
                ("profile_list_choices_ms", self.profile_list_choices_ms),
                ("render_ms", self.render_ms),
                ("ancestors_from_index", self.ancestors_from_index as u64),
                ("ancestors_live", self.ancestors_live as u64),
                ("ancestors_timed_out", self.ancestors_timed_out as u64),
            ];
            let mut fields = serde_json::Map::new();
            for (name, value) in named {
                if *value != 0 {
                    fields.insert((*name).to_owned(), json!(*value));
                }
            }
            if self.relationship_answer != "none" {
                fields.insert(
                    "relationship_answer".to_owned(),
                    json!(self.relationship_answer),
                );
            }
            detail.set(self.page, Value::Object(fields));
        }
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
        crate::net::with_background_request_pool(self.refresh_once_inner()).await
    }

    async fn refresh_once_inner(&self) -> Result<usize, StorageError> {
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

fn hydrate_page_profiles<S: Storage>(
    storage: &S,
    author_order: impl IntoIterator<Item = String>,
    public_profiles: Vec<crate::feeds::PublicProfileView>,
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
    let mut profiles = public_profiles
        .into_iter()
        .filter(|profile| seen.contains(&profile.did))
        .map(|profile| (profile.did.clone(), profile))
        .collect::<HashMap<_, _>>();
    let mut profile_fetches = Vec::new();
    let mut handle_refreshes = Vec::new();
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
        if let Some(profile) = profiles.remove(did) {
            if !profile_is_fresh {
                storage.put_record(&crate::feeds::merged_public_profile(storage, &profile)?)?;
                hydration.fetched_live += 1;
            }
        } else if !profile_is_fresh && !profile_is_negative_cached {
            profile_fetches.push(did.clone());
        }
        if storage.verified_handle(did)?.is_none() && !storage.handle_is_invalid(did)? {
            handle_refreshes.push(did.clone());
        }
    }
    if !profile_fetches.is_empty() {
        storage.enqueue_profile_fetches(&profile_fetches)?;
    }
    storage.queue_handle_refresh_ordered(&handle_refreshes)?;
    tracing::debug!(
        authors_needed = hydration.authors_needed,
        from_index = hydration.from_index,
        fetched_live = hydration.fetched_live,
        queued_background = profile_fetches.len(),
        "hydrated page author profiles"
    );
    Ok(hydration)
}

async fn hydrate_page_content<F: OAuthFlow, S: Storage + 'static>(
    flow: &F,
    viewer_did: &str,
    storage: Arc<S>,
    page_records: &[&IndexedRecord],
    quote_records: &[&IndexedRecord],
    leading_authors: impl IntoIterator<Item = String>,
    deadline: tokio::time::Instant,
) -> Result<PageContentHydration, StorageError> {
    let mut refs = Vec::new();
    let mut seen_refs = HashSet::new();
    for record in quote_records {
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

    let label_actor_dids = {
        let mut actors = authors.clone();
        actors.sort();
        actors.dedup();
        actors.truncate(100);
        actors
    };
    let account_flow = flow.clone();
    let viewer_did = viewer_did.to_owned();
    let label_actor_count = label_actor_dids.len();
    let account_data_hydration = async move {
        let started = std::time::Instant::now();
        let data = match tokio::time::timeout_at(
            deadline,
            account_flow.page_account_data_for_page(&viewer_did, label_actor_dids),
        )
        .await
        {
            Ok(Ok(data)) => data,
            Ok(Err(error)) => {
                tracing::warn!(%error, "page account profiles and labels could not be fetched");
                PageAccountData::default()
            }
            Err(_) => {
                tracing::warn!(
                    viewer = %viewer_did,
                    actors = label_actor_count,
                    "account labels timed out before the page deadline"
                );
                PageAccountData::default()
            }
        };
        (data, started.elapsed().as_millis() as u64)
    };
    let quote_storage = storage.clone();
    let quote_hydration = async {
        let started = std::time::Instant::now();
        let quotes = hydrate_page_quotes(flow, quote_storage.as_ref(), refs, deadline).await;
        (quotes, started.elapsed().as_millis() as u64)
    };
    let (quote_hydration, (account_data, account_labels_ms)) =
        tokio::join!(quote_hydration, account_data_hydration);
    let profile_write_started = std::time::Instant::now();
    let profile_result = hydrate_page_profiles(storage.as_ref(), authors, account_data.profiles);
    let profile_hydration_ms =
        account_labels_ms.saturating_add(profile_write_started.elapsed().as_millis() as u64);
    let mut quote_index = quote_hydration.0?;
    for label in account_data.labels {
        quote_index
            .account_labels
            .entry(label.subject_did.clone())
            .or_default()
            .push(label);
    }
    Ok(PageContentHydration {
        quote_index,
        profile_hydration_ms: {
            profile_result?;
            profile_hydration_ms
        },
        quote_hydration_ms: quote_hydration.1,
        account_labels_ms,
    })
}

fn page_records_with_parents<'a>(
    page_records: &[&'a IndexedRecord],
    parents: &'a PageParentIndex,
) -> Vec<&'a IndexedRecord> {
    let mut result = page_records.to_vec();
    let mut seen = page_records
        .iter()
        .map(|record| record.uri.as_str())
        .collect::<HashSet<_>>();
    for parent in parents.values().flatten() {
        if seen.insert(parent.uri.as_str()) {
            result.push(parent);
        }
    }
    result
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

    let mut first_round_uris = parent_uris.clone();
    if flow.supports_public_post_batching() {
        first_round_uris.extend(page_records.iter().filter_map(|record| {
            record
                .record
                .get("embed")
                .and_then(quoted_record_ref)
                .filter(|reference| !reference.unavailable)
                .filter(|reference| at_uri_collection(&reference.uri) == Some("app.bsky.feed.post"))
                .map(|reference| reference.uri)
        }));
    }
    first_round_uris.sort_unstable();
    first_round_uris.dedup();

    for depth in 0..=2 {
        let uris = if depth == 0 {
            &first_round_uris
        } else {
            &parent_uris
        };
        if uris.is_empty() || tokio::time::Instant::now() >= deadline {
            break;
        }
        hydrate_parent_uri_batch(flow, storage, uris, &mut parents, deadline).await?;
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
    let local = storage.records_by_uri("app.bsky.feed.post", parent_uris)?;
    for (uri, record) in local {
        parents.insert(uri, Some(record));
    }

    let missing = parent_uris
        .iter()
        .filter(|uri| parents.get(*uri).is_some_and(Option::is_none))
        .cloned()
        .collect::<Vec<_>>();
    if missing.is_empty() {
        return Ok(());
    }
    if !flow.supports_public_post_batching() {
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
        return Ok(());
    }
    if let Ok(fetched) = tokio::time::timeout_at(deadline, flow.fetch_public_posts(&missing)).await
    {
        let mut public_profiles = Vec::new();
        for uri in missing {
            let Some(post) = fetched.get(&uri) else {
                continue;
            };
            if post.record.uri == uri && post.record.collection == "app.bsky.feed.post" {
                parents.insert(uri, Some(post.record.clone()));
                if let Some(author) = &post.author {
                    public_profiles.push(author.clone());
                }
            }
        }
        crate::feeds::persist_public_profiles(storage, public_profiles)?;
    }
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
    let mut result = QuoteRenderIndex::default();
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

    if !flow.supports_public_post_batching() {
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
                    Ok(record)
                        if record.uri == uri && record.collection == "app.bsky.feed.post" =>
                    {
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
    } else if let Ok(fetched) =
        tokio::time::timeout_at(deadline, flow.fetch_public_posts(&missing_posts)).await
    {
        let mut public_profiles = Vec::new();
        for uri in missing_posts {
            let state = match fetched.get(&uri) {
                Some(post)
                    if post.record.uri == uri && post.record.collection == "app.bsky.feed.post" =>
                {
                    if let Some(author) = &post.author {
                        public_profiles.push(author.clone());
                    }
                    QuoteRenderState::Found(post.record.clone())
                }
                _ => QuoteRenderState::NotFound,
            };
            result.insert(uri, state);
        }
        crate::feeds::persist_public_profiles(storage, public_profiles)?;
    } else {
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
        "image" => {
            "<rect x=\"3\" y=\"4\" width=\"18\" height=\"16\" rx=\"2\"/><circle cx=\"8.5\" cy=\"9\" r=\"1.5\"/><path d=\"m21 15-5-5L5 20\"/>"
        }
        "link" => {
            "<path d=\"M10 13a5 5 0 0 0 7.1 0l3-3A5 5 0 0 0 13 2.9l-1.7 1.7\"/><path d=\"M14 11a5 5 0 0 0-7.1 0l-3 3A5 5 0 0 0 11 21.1l1.7-1.7\"/>"
        }
        "more" => {
            "<circle cx=\"5\" cy=\"12\" r=\"1\"/><circle cx=\"12\" cy=\"12\" r=\"1\"/><circle cx=\"19\" cy=\"12\" r=\"1\"/>"
        }
        "block-lists" => "<circle cx=\"12\" cy=\"12\" r=\"9\"/><path d=\"m5.6 5.6 12.8 12.8\"/>",
        _ => "",
    }
}

fn icon_sprite() -> String {
    const ICONS: [&str; 15] = [
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
        "image",
        "link",
        "block-lists",
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

async fn login(headers: HeaderMap, Query(query): Query<HashMap<String, String>>) -> Html<String> {
    let value = cookie_value_named(&headers, HANDLE_COOKIE)
        .filter(|handle| handle_shaped(handle))
        .map(|handle| format!(" value=\"{}\"", html_escape(handle)))
        .unwrap_or_default();
    let notice = if query
        .get("session_expired")
        .is_some_and(|value| value == "1")
    {
        "<p class=\"intro notice\" role=\"alert\">Your Bluesky sign-in expired; please sign in again.</p>"
    } else {
        ""
    };
    // Read-only by default (owner, 2026-10-31); ?write=1 re-signs the user in with changes allowed.
    let write_checked = if query.get("write").is_some_and(|value| value == "1") {
        " checked"
    } else {
        ""
    };
    Html(page_shell(
        "Sign in",
        &format!(
            "<section class=\"login-card\"><p class=\"eyebrow\">Your network, your view</p><h1>Sign in to AppView</h1>{notice}<p class=\"intro\">Use your AT Protocol handle or DID to continue securely with your account provider.</p><form action=\"/oauth/authorize\" method=\"post\" class=\"login-form\"><label for=\"handle\">AT Protocol handle or DID</label><input id=\"handle\" name=\"handle\" autocomplete=\"username\" autocapitalize=\"none\" placeholder=\"you.bsky.social\"{value} required><label class=\"login-write-option\" for=\"write\"><input type=\"checkbox\" id=\"write\" name=\"write\" value=\"1\"{write_checked}> Allow changes (post, like, follow, edit feeds)</label><button type=\"submit\">Continue</button></form></section>"
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
            return login_retry_page();
        }
    };
    if !state.public.login_is_allowed(&account.did) {
        tracing::info!(did = %account.did, "OAuth account is not in the login allowlist");
        if let Err(error) = state.flow.logout(&account.did).await {
            tracing::warn!(%error, did = %account.did, "could not revoke a disallowed OAuth session");
        }
        return oauth_error_page(StatusCode::FORBIDDEN, "This appview is invite-only.");
    }
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
                follow: has_all_follow_scopes(account.granted_scope.as_deref()),
                block: has_all_block_scopes(account.granted_scope.as_deref()),
                mute: has_all_mute_scopes(account.granted_scope.as_deref()),
                list: has_all_list_scopes(account.granted_scope.as_deref()),
                preferences: has_all_preferences_scopes(account.granted_scope.as_deref()),
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
    let Some((did, session_id, _, _)) =
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

async fn timeline_hidden_actor_dids<F: OAuthFlow, S: Storage>(
    flow: &F,
    storage: &S,
    viewer_did: &str,
) -> Result<HashSet<String>, String> {
    let mut hidden = storage
        .records_for_actor_collection(viewer_did, "app.bsky.graph.block", 10_000)
        .map_err(|error| error.to_string())?
        .into_iter()
        .filter_map(|record| {
            record
                .record
                .get("subject")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .collect::<HashSet<_>>();
    match flow.muted_actor_dids(viewer_did).await {
        Ok(muted) => hidden.extend(muted),
        Err(error) => tracing::warn!(%error, "could not load muted accounts for the timeline"),
    }
    Ok(hidden)
}

#[derive(Clone, Debug)]
struct ListBlockMark {
    list_uri: String,
    list_name: String,
}

/// In-memory + web-cache store for subscribed block lists, mirroring the muted
/// actor cache: serve stale entries and refresh in spawned tasks that own their
/// pending-set cleanup, so a page dropped mid-refresh cannot strand a key.
#[derive(Default)]
struct BlockListCache {
    listblocks: tokio::sync::Mutex<HashMap<String, (tokio::time::Instant, Vec<String>)>>,
    listblocks_pending: tokio::sync::Mutex<HashSet<String>>,
    members: tokio::sync::Mutex<HashMap<String, (tokio::time::Instant, BlockListDetail)>>,
    members_pending: tokio::sync::Mutex<HashSet<String>>,
}

fn remember_block_list_entry<T>(
    memory: &mut HashMap<String, (tokio::time::Instant, T)>,
    key: String,
    value: T,
    expires: tokio::time::Instant,
) {
    let now = tokio::time::Instant::now();
    memory.retain(|_, (expires, _)| *expires > now);
    if memory.len() >= 4096
        && let Some(oldest) = memory.keys().next().cloned()
    {
        memory.remove(&oldest);
    }
    memory.insert(key, (expires, value));
}

async fn persist_block_list_entry(store: &SqliteOAuthStore, key: &str, value_json: &str) {
    if let Err(error) = store.web_cache_set(key, value_json) {
        tracing::warn!(%error, key, "could not persist block list cache entry");
    }
}

fn spawn_listblock_refresh<F: OAuthFlow>(
    flow: F,
    cache: Arc<BlockListCache>,
    did: String,
    done: Option<oneshot::Sender<Result<Vec<String>, String>>>,
) {
    let dispatcher = tracing::dispatcher::get_default(|dispatcher| dispatcher.clone());
    tokio::spawn(
        async move {
            let result = tokio::time::timeout(
                BLOCK_LIST_FETCH_TIMEOUT,
                flow.subscribed_block_list_uris(&did),
            )
            .await
            .unwrap_or_else(|_| Err("block list lookup timed out".to_owned()));
            match &result {
                Ok(uris) => {
                    if let Some(store) = flow.web_cache_store()
                        && let Ok(value_json) = serde_json::to_string(uris)
                    {
                        persist_block_list_entry(&store, &listblock_cache_key(&did), &value_json)
                            .await;
                    }
                    let mut memory = cache.listblocks.lock().await;
                    remember_block_list_entry(
                        &mut memory,
                        did.clone(),
                        uris.clone(),
                        tokio::time::Instant::now() + LISTBLOCK_CACHE_TTL,
                    );
                }
                Err(error) => tracing::warn!(
                    %error,
                    viewer = %did,
                    "subscribed block list lookup failed; keeping the last known list set"
                ),
            }
            cache.listblocks_pending.lock().await.remove(&did);
            if let Some(done) = done {
                let _ = done.send(result);
            }
        }
        .with_subscriber(dispatcher),
    );
}

async fn schedule_listblock_refresh<F: OAuthFlow>(
    flow: &F,
    cache: &Arc<BlockListCache>,
    did: &str,
) {
    let mut pending = cache.listblocks_pending.lock().await;
    if !pending.insert(did.to_owned()) {
        return;
    }
    drop(pending);
    spawn_listblock_refresh(flow.clone(), Arc::clone(cache), did.to_owned(), None);
}

/// The viewer's subscribed block-list URIs, cached like the muted actor list:
/// fresh memory hit, then stale serving with a background refresh, then the
/// persisted web cache, and only then a blocking refresh guarded by the
/// pending set.
async fn viewer_block_list_uris<F: OAuthFlow>(
    flow: &F,
    cache: &Arc<BlockListCache>,
    did: &str,
) -> Result<Vec<String>, String> {
    let now = tokio::time::Instant::now();
    if let Some((expires, uris)) = cache.listblocks.lock().await.get(did).cloned() {
        if expires > now {
            return Ok(uris);
        }
        schedule_listblock_refresh(flow, cache, did).await;
        return Ok(uris);
    }
    if let Some(store) = flow.web_cache_store()
        && let Ok(Some((json, stored_at_ms))) = store.web_cache_get(&listblock_cache_key(did)).await
        && let Ok(uris) = serde_json::from_str::<Vec<String>>(&json)
    {
        let age_ms = unix_time_ms().saturating_sub(stored_at_ms).max(0) as u64;
        let ttl_ms = LISTBLOCK_CACHE_TTL.as_millis() as u64;
        let stale = age_ms >= ttl_ms;
        let expires = now + Duration::from_millis(ttl_ms.saturating_sub(age_ms));
        let mut memory = cache.listblocks.lock().await;
        remember_block_list_entry(
            &mut memory,
            did.to_owned(),
            uris.clone(),
            expires.min(now + LISTBLOCK_CACHE_TTL),
        );
        drop(memory);
        if stale {
            schedule_listblock_refresh(flow, cache, did).await;
        }
        return Ok(uris);
    }
    if !cache.listblocks_pending.lock().await.insert(did.to_owned()) {
        let deadline = tokio::time::Instant::now() + PAGE_PROFILE_BUDGET;
        loop {
            if let Some((_, uris)) = cache.listblocks.lock().await.get(did).cloned() {
                return Ok(uris);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err("block list lookup timed out".to_owned());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    let (done, awaited) = oneshot::channel();
    spawn_listblock_refresh(flow.clone(), Arc::clone(cache), did.to_owned(), Some(done));
    match tokio::time::timeout(PAGE_PROFILE_BUDGET, awaited).await {
        Ok(Ok(result)) => result,
        Ok(Err(_)) => Err("block list refresh task ended without a result".to_owned()),
        Err(_) => Err("block list lookup timed out".to_owned()),
    }
}

fn spawn_members_refresh<F: OAuthFlow>(
    flow: F,
    cache: Arc<BlockListCache>,
    list_uri: String,
    done: Option<oneshot::Sender<Result<BlockListDetail, String>>>,
) {
    let dispatcher = tracing::dispatcher::get_default(|dispatcher| dispatcher.clone());
    tokio::spawn(
        async move {
            let result =
                tokio::time::timeout(BLOCK_LIST_FETCH_TIMEOUT, flow.block_list_detail(&list_uri))
                    .await
                    .unwrap_or_else(|_| Err("block list member fetch timed out".to_owned()));
            match &result {
                Ok(detail) => {
                    if let Some(store) = flow.web_cache_store()
                        && let Ok(value_json) = serde_json::to_string(detail)
                    {
                        persist_block_list_entry(
                            &store,
                            &list_members_cache_key(&list_uri),
                            &value_json,
                        )
                        .await;
                    }
                    let mut memory = cache.members.lock().await;
                    remember_block_list_entry(
                        &mut memory,
                        list_uri.clone(),
                        detail.clone(),
                        tokio::time::Instant::now() + LIST_MEMBERS_CACHE_TTL,
                    );
                }
                Err(error) => tracing::warn!(
                    %error,
                    list = %list_uri,
                    "block list member fetch failed; keeping the last known members"
                ),
            }
            cache.members_pending.lock().await.remove(&list_uri);
            if let Some(done) = done {
                let _ = done.send(result);
            }
        }
        .with_subscriber(dispatcher),
    );
}

async fn schedule_members_refresh<F: OAuthFlow>(
    flow: &F,
    cache: &Arc<BlockListCache>,
    list_uri: &str,
) {
    let mut pending = cache.members_pending.lock().await;
    if !pending.insert(list_uri.to_owned()) {
        return;
    }
    drop(pending);
    spawn_members_refresh(flow.clone(), Arc::clone(cache), list_uri.to_owned(), None);
}

/// One list's name and members, cached per list URI like the muted actor list.
async fn block_list_detail_cached<F: OAuthFlow>(
    flow: &F,
    cache: &Arc<BlockListCache>,
    list_uri: &str,
) -> Result<BlockListDetail, String> {
    let now = tokio::time::Instant::now();
    if let Some((expires, detail)) = cache.members.lock().await.get(list_uri).cloned() {
        if expires > now {
            return Ok(detail);
        }
        schedule_members_refresh(flow, cache, list_uri).await;
        return Ok(detail);
    }
    if let Some(store) = flow.web_cache_store()
        && let Ok(Some((json, stored_at_ms))) =
            store.web_cache_get(&list_members_cache_key(list_uri)).await
        && let Ok(detail) = serde_json::from_str::<BlockListDetail>(&json)
    {
        let age_ms = unix_time_ms().saturating_sub(stored_at_ms).max(0) as u64;
        let ttl_ms = LIST_MEMBERS_CACHE_TTL.as_millis() as u64;
        let stale = age_ms >= ttl_ms;
        let expires = now + Duration::from_millis(ttl_ms.saturating_sub(age_ms));
        let mut memory = cache.members.lock().await;
        remember_block_list_entry(
            &mut memory,
            list_uri.to_owned(),
            detail.clone(),
            expires.min(now + LIST_MEMBERS_CACHE_TTL),
        );
        drop(memory);
        if stale {
            schedule_members_refresh(flow, cache, list_uri).await;
        }
        return Ok(detail);
    }
    if !cache
        .members_pending
        .lock()
        .await
        .insert(list_uri.to_owned())
    {
        let deadline = tokio::time::Instant::now() + PAGE_PROFILE_BUDGET;
        loop {
            if let Some((_, detail)) = cache.members.lock().await.get(list_uri).cloned() {
                return Ok(detail);
            }
            if tokio::time::Instant::now() >= deadline {
                return Err("block list member fetch timed out".to_owned());
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }
    let (done, awaited) = oneshot::channel();
    spawn_members_refresh(
        flow.clone(),
        Arc::clone(cache),
        list_uri.to_owned(),
        Some(done),
    );
    match tokio::time::timeout(PAGE_PROFILE_BUDGET, awaited).await {
        Ok(Ok(result)) => result,
        Ok(Err(_)) => Err("block list member refresh task ended without a result".to_owned()),
        Err(_) => Err("block list member fetch timed out".to_owned()),
    }
}

/// The viewer's subscribed block lists for a page, sorted by list URI. A failed
/// lookup warns and keeps whatever was last known (an empty set on first sight),
/// so it never hides or unhides anything new.
async fn subscribed_block_lists_for_page<F: OAuthFlow>(
    flow: &F,
    cache: &Arc<BlockListCache>,
    did: &str,
) -> Vec<BlockListDetail> {
    let mut uris = match viewer_block_list_uris(flow, cache, did).await {
        Ok(uris) => uris,
        Err(error) => {
            tracing::warn!(
                %error,
                viewer = %did,
                "could not load the viewer's subscribed block lists; rendering without list blocks"
            );
            return Vec::new();
        }
    };
    uris.sort();
    uris.dedup();
    let mut lists = Vec::new();
    for uri in uris {
        match block_list_detail_cached(flow, cache, &uri).await {
            Ok(detail) => lists.push(detail),
            Err(error) => tracing::warn!(
                %error,
                list = %uri,
                viewer = %did,
                "could not load block list members; keeping the last known members"
            ),
        }
    }
    lists
}

/// Map each member DID to its block list, keeping the first list in the stable
/// order sorted by list URI.
fn first_list_block_marks(lists: &[BlockListDetail]) -> HashMap<String, ListBlockMark> {
    let mut ordered = lists.iter().collect::<Vec<_>>();
    ordered.sort_by_key(|list| list.uri.as_str());
    let mut marks = HashMap::new();
    for list in ordered {
        for member in &list.member_dids {
            marks
                .entry(member.clone())
                .or_insert_with(|| ListBlockMark {
                    list_uri: list.uri.clone(),
                    list_name: list.name.clone(),
                });
        }
    }
    marks
}

fn timeline_list_block_attrs(
    list_blocks: &HashMap<String, ListBlockMark>,
    author_dids: impl IntoIterator<Item = impl AsRef<str>>,
) -> String {
    let mut best: Option<&ListBlockMark> = None;
    for did in author_dids {
        if let Some(mark) = list_blocks.get(did.as_ref())
            && best.is_none_or(|current| mark.list_uri < current.list_uri)
        {
            best = Some(mark);
        }
    }
    match best {
        Some(mark) => format!(
            " hidden data-list-block=\"{}\" data-list-block-name=\"{}\"",
            html_escape(&mark.list_uri),
            html_escape(&mark.list_name)
        ),
        None => String::new(),
    }
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
    let saved_feeds_started = std::time::Instant::now();
    let tabs = pinned_feeds_for_layout(
        &state.flow,
        &did,
        verified_cookie_feed_access(&state.public, &headers, state.storage.as_ref()),
    )
    .await;
    page_timing.saved_feeds_ms = saved_feeds_started.elapsed().as_millis() as u64;
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
    let next_cursor_record = timeline_page.get(limit.saturating_sub(1)).cloned();
    let mutes_started = std::time::Instant::now();
    let hidden_actors_result =
        timeline_hidden_actor_dids(&state.flow, state.storage.as_ref(), &did).await;
    page_timing.mutes_ms = mutes_started.elapsed().as_millis() as u64;
    let hidden_actors = match hidden_actors_result {
        Ok(hidden) => hidden,
        Err(error) => return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error),
    };
    let mut page_records = timeline_page.into_iter().take(limit).collect::<Vec<_>>();
    page_records.retain(|record| !hidden_actors.contains(&record.did));
    let list_blocks_started = std::time::Instant::now();
    let block_lists = subscribed_block_lists_for_page(&state.flow, &state.block_lists, &did).await;
    page_timing.list_blocks_ms = list_blocks_started.elapsed().as_millis() as u64;
    let list_blocks = first_list_block_marks(&block_lists);
    page_timing.list_blocked_posts = page_records
        .iter()
        .filter(|record| list_blocks.contains_key(&record.did))
        .count() as u64;
    let timeline = page_records.iter().collect::<Vec<_>>();
    let content_deadline = tokio::time::Instant::now() + PAGE_PROFILE_BUDGET;
    let parent_started = std::time::Instant::now();
    let parent_result = hydrate_page_parents(
        &state.flow,
        state.storage.as_ref(),
        &timeline,
        content_deadline,
    )
    .await;
    page_timing.parent_hydration_ms = parent_started.elapsed().as_millis() as u64;
    let parents = match parent_result {
        Ok(parents) => parents,
        Err(error) => return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    let quote_records = page_records_with_parents(&timeline, &parents);
    let content_started = std::time::Instant::now();
    let content_result = hydrate_page_content(
        &state.flow,
        &did,
        state.storage.clone(),
        &timeline,
        &quote_records,
        std::iter::empty(),
        content_deadline,
    )
    .await;
    page_timing.content_hydration_ms = content_started.elapsed().as_millis() as u64;
    let quote_index = match content_result {
        Ok(hydration) => {
            page_timing.profile_hydration_ms = hydration.profile_hydration_ms;
            page_timing.quote_hydration_ms = hydration.quote_hydration_ms;
            page_timing.account_labels_ms = hydration.account_labels_ms;
            hydration.quote_index
        }
        Err(error) => return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    if let Err(error) = queue_missing_parent_authors(state.storage.as_ref(), &parents) {
        return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string());
    }
    let write_forms_enabled =
        verified_cookie_write_access(&state.public, &headers, state.storage.as_ref());
    let render_started = std::time::Instant::now();
    let posts = render_timeline_post_items(
        state.storage.as_ref(),
        &timeline,
        &vec![String::new(); timeline.len()],
        &parents,
        &TimelineRenderContext {
            viewer_did: &did,
            write_forms_enabled,
            quote_index: &quote_index,
            list_blocks: &list_blocks,
        },
    );
    let posts = if posts.is_empty() {
        "<li class=\"empty-state\">No indexed posts yet.</li>".to_owned()
    } else {
        posts
    };
    let posts_render_ms = render_started.elapsed().as_millis() as u64;
    let indexing_status = if page_records.is_empty() && next_cursor_record.is_none() {
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
    let rest_render_started = std::time::Instant::now();
    let read_only_message = if write_forms_enabled {
        String::new()
    } else {
        "<p role=\"status\">Read-only: write permissions were not granted, so posting, liking, and reposting are disabled.</p>".to_owned()
    };
    let post_form = if write_forms_enabled {
        let blob_access =
            verified_cookie_blob_access(&state.public, &headers, state.storage.as_ref());
        let video_access =
            verified_cookie_video_access(&state.public, &headers, state.storage.as_ref());
        let profile = profile_view_from_storage(state.storage.as_ref(), &did)
            .unwrap_or_else(|_| json!({"did":did,"handle":did}));
        render_composer(ComposerSpec {
            form_attributes: "",
            classes: "media-composer",
            textarea_id: "new-post",
            action: "/post",
            label: "Write a post",
            placeholder: "What's up?",
            submit_label: "Post",
            avatar: &composer_avatar_markup(&profile),
            hidden_fields: "",
            context: "",
            media_picker: render_media_picker(blob_access, video_access, "post-media"),
            link_picker: render_link_picker("new-post-link"),
            trailing: "",
            media_access: blob_access || video_access,
        })
    } else {
        String::new()
    };
    let older = if has_more {
        next_cursor_record
            .as_ref()
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
    page_timing.render_ms = posts_render_ms + rest_render_started.elapsed().as_millis() as u64;
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
        Err(error) => {
            if take_dead_oauth_session().is_some() {
                return session_expired_response(&state.public, &headers);
            }
            return oauth_error(StatusCode::BAD_GATEWAY, &error);
        }
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

fn render_block_list_setting(index: usize, list: &BlockListDetail) -> String {
    let uri = html_escape(&list.uri);
    let input_id = format!("block-list-leak-{index}");
    let member_word = if list.member_dids.len() == 1 {
        "member"
    } else {
        "members"
    };
    format!(
        "<li class=\"block-list-setting\" data-block-list-uri=\"{uri}\"><div class=\"block-list-head\"><span class=\"block-list-name\">{}</span><span class=\"block-list-count\">{} {member_word}</span></div><label for=\"{input_id}\">% of posts let through</label><input id=\"{input_id}\" type=\"range\" min=\"0\" max=\"100\" step=\"1\" value=\"0\" data-list-block-uri=\"{uri}\"></li>",
        html_escape(&list.name),
        list.member_dids.len(),
    )
}

async fn block_lists_page<F, S>(
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
    let tabs = pinned_feeds_for_layout(
        &state.flow,
        &did,
        verified_cookie_feed_access(&state.public, &headers, state.storage.as_ref()),
    )
    .await;
    let lists = subscribed_block_lists_for_page(&state.flow, &state.block_lists, &did).await;
    let entries = lists
        .iter()
        .enumerate()
        .map(|(index, list)| render_block_list_setting(index, list))
        .collect::<String>();
    let entries = if entries.is_empty() {
        "<li class=\"empty-state\">No lists are subscribed as block lists yet. Subscribe to a moderation list as a block list and it will appear here.</li>".to_owned()
    } else {
        entries
    };
    Html(page_shell_with_feeds_and_badge(
        "Block lists",
        &format!(
            "<header class=\"page-heading\"><p class=\"eyebrow\">Your moderation</p><h1>Block lists</h1><p class=\"page-subtitle\">Posts by members of these lists stay hidden. Let a share of their posts through anyway.</p></header><ol class=\"block-list-settings\">{entries}</ol>"
        ),
        true,
        &tabs.feeds,
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
            Err(error) => {
                if take_dead_oauth_session().is_some() {
                    return session_expired_response(&state.public, &headers);
                }
                if crate::feeds::feed_is_gone(&error) {
                    return unavailable_feed_page(
                        uri,
                        verified_cookie_preferences_access(
                            &state.public,
                            &headers,
                            state.storage.as_ref(),
                        ),
                    );
                }
                return oauth_error(StatusCode::BAD_GATEWAY, &error);
            }
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
    page_timing.feed_timeline_ms = timeline_started.elapsed().as_millis() as u64;
    page_timing.session_token_refresh_ms = page
        .timing
        .session_token_refresh_ms
        .saturating_add(page_timing.session_token_refresh_ms);
    page_timing.timeline_fetch_live_ms = page.timing.timeline_fetch_live_ms;
    if page.timing.cache_hit {
        page_timing.timeline_fetch_index_ms = timeline_started.elapsed().as_millis() as u64;
    }
    let mutes_started = std::time::Instant::now();
    let hidden_actors_result =
        timeline_hidden_actor_dids(&state.flow, state.storage.as_ref(), &did).await;
    page_timing.mutes_ms = mutes_started.elapsed().as_millis() as u64;
    let hidden_actors = match hidden_actors_result {
        Ok(hidden) => hidden,
        Err(error) => return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error),
    };
    let feed_items = page
        .timeline
        .feed
        .iter()
        .filter(|post| !hidden_actors.contains(&post.record.did))
        .collect::<Vec<_>>();
    let feed_records = feed_items
        .iter()
        .map(|post| &post.record)
        .collect::<Vec<_>>();
    let list_blocks_started = std::time::Instant::now();
    let block_lists = subscribed_block_lists_for_page(&state.flow, &state.block_lists, &did).await;
    page_timing.list_blocks_ms = list_blocks_started.elapsed().as_millis() as u64;
    let list_blocks = first_list_block_marks(&block_lists);
    page_timing.list_blocked_posts = feed_records
        .iter()
        .filter(|record| list_blocks.contains_key(&record.did))
        .count() as u64;
    let content_deadline = tokio::time::Instant::now() + PAGE_PROFILE_BUDGET;
    let parent_started = std::time::Instant::now();
    let parent_result = hydrate_page_parents(
        &state.flow,
        state.storage.as_ref(),
        &feed_records,
        content_deadline,
    )
    .await;
    page_timing.parent_hydration_ms = parent_started.elapsed().as_millis() as u64;
    let parents = match parent_result {
        Ok(parents) => parents,
        Err(error) => return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    let quote_records = page_records_with_parents(&feed_records, &parents);
    let content_started = std::time::Instant::now();
    let content_result = hydrate_page_content(
        &state.flow,
        &did,
        state.storage.clone(),
        &feed_records,
        &quote_records,
        std::iter::empty(),
        content_deadline,
    )
    .await;
    page_timing.content_hydration_ms = content_started.elapsed().as_millis() as u64;
    let quote_index = match content_result {
        Ok(hydration) => {
            page_timing.profile_hydration_ms = hydration.profile_hydration_ms;
            page_timing.quote_hydration_ms = hydration.quote_hydration_ms;
            page_timing.account_labels_ms = hydration.account_labels_ms;
            hydration.quote_index
        }
        Err(error) => return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    if let Err(error) = queue_missing_parent_authors(state.storage.as_ref(), &parents) {
        return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string());
    }
    let write_forms_enabled =
        verified_cookie_write_access(&state.public, &headers, state.storage.as_ref());
    let render_started = std::time::Instant::now();
    let feed_preludes = feed_items
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
        &TimelineRenderContext {
            viewer_did: &did,
            write_forms_enabled,
            quote_index: &quote_index,
            list_blocks: &list_blocks,
        },
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
    let pre_tabs_render_ms = render_started.elapsed().as_millis() as u64;
    let saved_feeds_started = std::time::Instant::now();
    let tabs = pinned_feeds_for_layout(&state.flow, &did, true).await;
    page_timing.saved_feeds_ms = saved_feeds_started.elapsed().as_millis() as u64;
    page_timing.preferences_ms = tabs.preferences_ms;
    page_timing.tabs_ms = tabs.tabs_ms;
    let shell_render_started = std::time::Instant::now();
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
    page_timing.render_ms = pre_tabs_render_ms + shell_render_started.elapsed().as_millis() as u64;
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
    let saved_feeds_started = std::time::Instant::now();
    let tabs = pinned_feeds_for_layout(
        &state.flow,
        &did,
        verified_cookie_feed_access(&state.public, &headers, state.storage.as_ref()),
    )
    .await;
    page_timing.saved_feeds_ms = saved_feeds_started.elapsed().as_millis() as u64;
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
    let parent_result = hydrate_page_parents(
        &state.flow,
        state.storage.as_ref(),
        &notification_records,
        content_deadline,
    )
    .await;
    page_timing.parent_hydration_ms = parent_started.elapsed().as_millis() as u64;
    let parents = match parent_result {
        Ok(parents) => parents,
        Err(error) => return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    let quote_records = page_records_with_parents(&notification_records, &parents);
    let content_started = std::time::Instant::now();
    let content_result = hydrate_page_content(
        &state.flow,
        &did,
        state.storage.clone(),
        &notification_records,
        &quote_records,
        leading_authors,
        content_deadline,
    )
    .await;
    page_timing.content_hydration_ms = content_started.elapsed().as_millis() as u64;
    let quote_index = match content_result {
        Ok(hydration) => {
            page_timing.profile_hydration_ms = hydration.profile_hydration_ms;
            page_timing.quote_hydration_ms = hydration.quote_hydration_ms;
            page_timing.account_labels_ms = hydration.account_labels_ms;
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
            let fill = author_fill(&profile, &event.record.did);
            avatar_markup_with_priority_and_attrs(
                &profile,
                "notification-avatar",
                false,
                &fill.avatar_attrs,
            )
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

    let name_fill = author_fill(&author, &newest.record.did);
    let name_attrs = format!("{}{}", name_fill.container, name_fill.name);
    format!(
        "<li class=\"notification-item notification-group-item\"><article class=\"notification-group\"><div class=\"notification-group-icon\">{icon}</div><div class=\"notification-group-content\"><div class=\"notification-group-avatars\">{avatars}</div><p class=\"notification-summary\"><a class=\"notification-name\" href=\"{}\"{}>{}</a>{others} {action} <span class=\"notification-age\">· {age}</span></p>{snippet}</div></article></li>",
        html_escape(&author_uri),
        name_attrs,
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
    let fill = author_fill(&author, &record.did);
    let author_attrs = format!("{}{}", fill.container, fill.handle);
    format!(
        "<li class=\"notification-item\" data-post-kind=\"notification\" data-post-uri=\"{}\"><article class=\"notification-card\"><header class=\"notification-heading\">{}<div><a class=\"author-name\" href=\"{}\"{}>@{}</a><span class=\"notification-reason\">{reason}</span></div></header>{parent}{text}{embed}{thread_link}</article></li>",
        html_escape(&record.uri),
        avatar_markup_with_priority_and_attrs(&author, "avatar", false, &fill.avatar_attrs),
        html_escape(&author_uri),
        author_attrs,
        html_escape(&handle)
    )
}

fn label_value_default(value: &str) -> LabelValuePresentation {
    let (name, severity, blurs) = match value {
        "!hide" => ("Hidden", "alert", "content"),
        "!warn" => ("Warning", "inform", "content"),
        "porn" => ("Adult Content", "alert", "media"),
        "sexual" => ("Sexually Suggestive", "inform", "media"),
        "nudity" => ("Nudity", "inform", "media"),
        "graphic-media" => ("Graphic Media", "alert", "media"),
        other => {
            let name = other
                .split('-')
                .map(|part| {
                    let mut chars = part.chars();
                    chars
                        .next()
                        .map(|first| first.to_uppercase().collect::<String>() + chars.as_str())
                        .unwrap_or_default()
                })
                .collect::<Vec<_>>()
                .join(" ");
            return LabelValuePresentation {
                name,
                description: String::new(),
                severity: "none".to_owned(),
                blurs: "none".to_owned(),
            };
        }
    };
    LabelValuePresentation {
        name: name.to_owned(),
        description: String::new(),
        severity: severity.to_owned(),
        blurs: blurs.to_owned(),
    }
}

// The compact icon for one account label, picked from the label value (lowercased) by the first
// matching keyword in this fixed table:
//   "url"                        🔗  the same URL posted repeatedly   (posted-same-url-high)
//   "gap" / "break"              ⏱️  activity without meaningful breaks (no-gap-more-than-one-hours)
//   "metadata" / "profile"       🪪  profile/metadata changes          (high-metadata-changes-ten)
//   "follow"                     👥  follows and unfollows ("unfollow" contains "follow") (monthly-high-unfollows-*)
//   "repl"                       💬  replies and quote-replies         (made-over-hundred-replies-yesterday)
//   "post"                       📝  posting volume                    (made-over-hundred-posts-yesterday)
//   anything else                🏷️  a generic label tag               (spider)
// "url" is checked before "post" so "posted-same-url-*" reads as the link it tracks, and "repl"
// before "post" so reply counts never show the memo icon.
fn label_emoji(value: &str) -> &'static str {
    let value = value.to_ascii_lowercase();
    if value.contains("url") {
        "🔗"
    } else if value.contains("gap") || value.contains("break") {
        "⏱️"
    } else if value.contains("metadata") || value.contains("profile") {
        "🪪"
    } else if value.contains("follow") {
        "👥"
    } else if value.contains("repl") {
        "💬"
    } else if value.contains("post") {
        "📝"
    } else {
        "🏷️"
    }
}

fn account_label_severity_class(severity: &str) -> &'static str {
    match severity {
        "alert" => "severity-alert",
        "inform" => "severity-inform",
        _ => "severity-neutral",
    }
}

fn render_account_label_chips(
    labels_by_actor: &HashMap<String, Vec<AccountLabel>>,
    actor_did: &str,
) -> String {
    let Some(labels) = labels_by_actor
        .get(actor_did)
        .filter(|labels| !labels.is_empty())
    else {
        return String::new();
    };
    let count = format!(
        "{} account label{}",
        labels.len(),
        if labels.len() == 1 { "" } else { "s" }
    );
    let mut summary = String::new();
    for label in labels.iter().take(4) {
        summary.push_str(&format!(
            "<span class=\"account-label-emoji {}\" aria-hidden=\"true\">{}</span>",
            account_label_severity_class(&label.severity),
            label_emoji(&label.value),
        ));
    }
    if let Some(hidden) = labels.len().checked_sub(4) {
        summary.push_str(&format!(
            "<span class=\"account-label-more\">+{hidden}</span>"
        ));
    }
    let mut groups: Vec<(&AccountLabel, Vec<&AccountLabel>)> = Vec::new();
    for label in labels {
        if let Some((_, rows)) = groups
            .iter_mut()
            .find(|(first, _)| first.labeler_did == label.labeler_did)
        {
            rows.push(label);
        } else {
            groups.push((label, vec![label]));
        }
    }
    let grouped = groups.len() > 1;
    let groups_markup = groups
        .iter()
        .map(|(first, rows)| {
            let heading = if grouped {
                format!(
                    "<p class=\"account-label-group-name\">{}</p>",
                    html_escape(&first.labeler_name)
                )
            } else {
                String::new()
            };
            let rows_markup = rows
                .iter()
                .map(|label| {
                    let severity = account_label_severity_class(&label.severity);
                    let description = if label.description.trim().is_empty() {
                        String::new()
                    } else {
                        format!(
                            "<span class=\"account-label-description\">{}</span>",
                            html_escape(&label.description)
                        )
                    };
                    let labeler_uri = format!(
                        "/profile?did={}",
                        url::form_urlencoded::byte_serialize(label.labeler_did.as_bytes())
                            .collect::<String>()
                    );
                    format!(
                        "<li class=\"account-label-row {severity}\"><span class=\"account-label-emoji {severity}\" aria-hidden=\"true\">{}</span><span class=\"account-label-copy\"><span class=\"account-label-name\">{}</span>{description}<a class=\"account-label-by\" href=\"{labeler_uri}\" title=\"@{}\">by {}</a></span></li>",
                        label_emoji(&label.value),
                        html_escape(&label.display_name),
                        html_escape(&label.labeler_handle),
                        html_escape(&label.labeler_name),
                    )
                })
                .collect::<String>();
            format!("<li class=\"account-label-group\">{heading}<ul class=\"account-label-rows\">{rows_markup}</ul></li>")
        })
        .collect::<String>();
    format!(
        "<details class=\"account-labels\"><summary class=\"account-labels-summary\" aria-label=\"{count}\" title=\"{count}\">{summary}</summary><ul class=\"account-label-groups\">{groups_markup}</ul></details>"
    )
}

async fn profile_action_panel<F: OAuthFlow, S: Storage>(
    flow: &F,
    public: &OAuthPublic,
    headers: &HeaderMap,
    storage: &S,
    viewer_did: &str,
    subject_did: &str,
    timing: &mut ProfileActionTiming,
) -> Result<String, String> {
    let projected_following = storage
        .is_following(viewer_did, subject_did)
        .map_err(|error| error.to_string())?;
    // The viewer's own follow records are the source of truth: an emptied or
    // stale `follows` projection must not flip a real follow back to "Follow".
    let following = projected_following
        || storage
            .has_follow_record(viewer_did, subject_did)
            .map_err(|error| error.to_string())?;
    if following && !projected_following {
        tracing::debug!(
            viewer = %viewer_did,
            subject = %subject_did,
            "Follow state answered from the viewer's own follow records; the follows row was missing",
        );
    }
    let blocked = storage
        .records_for_actor_collection(viewer_did, "app.bsky.graph.block", 10_000)
        .map_err(|error| error.to_string())?
        .iter()
        .any(|record| record.record.get("subject").and_then(Value::as_str) == Some(subject_did));
    let follow_access = verified_cookie_follow_access(public, headers, storage);
    let block_access = verified_cookie_block_access(public, headers, storage);
    let mute_access = verified_cookie_mute_access(public, headers, storage);
    let list_access = verified_cookie_list_access(public, headers, storage);
    let mutes_started = std::time::Instant::now();
    let muted_result = if mute_access {
        Some(flow.muted_actor_dids(viewer_did).await)
    } else {
        None
    };
    timing.mutes_ms = mutes_started.elapsed().as_millis() as u64;
    let muted = if let Some(muted_result) = muted_result {
        match muted_result {
            Ok(muted) => muted.contains(subject_did),
            Err(error) => {
                tracing::warn!(%error, "could not load the viewer's muted accounts");
                false
            }
        }
    } else {
        false
    };
    let lists_started = std::time::Instant::now();
    let lists_result = if list_access {
        Some(flow.profile_list_choices(viewer_did, subject_did).await)
    } else {
        None
    };
    timing.list_choices_ms = lists_started.elapsed().as_millis() as u64;
    let lists = if let Some(lists_result) = lists_result {
        match lists_result {
            Ok(lists) => lists,
            Err(error) => {
                tracing::warn!(%error, "could not load the viewer's lists");
                Vec::new()
            }
        }
    } else {
        Vec::new()
    };
    Ok(render_profile_actions(ProfileActionState {
        subject_did,
        following,
        blocked,
        muted,
        follow_access,
        block_access,
        mute_access,
        list_access,
        lists: &lists,
    }))
}

#[derive(Default)]
struct ProfileActionTiming {
    mutes_ms: u64,
    list_choices_ms: u64,
}

struct ProfileActionState<'a> {
    subject_did: &'a str,
    following: bool,
    blocked: bool,
    muted: bool,
    follow_access: bool,
    block_access: bool,
    mute_access: bool,
    list_access: bool,
    lists: &'a [ProfileListChoice],
}

fn render_profile_actions(state: ProfileActionState<'_>) -> String {
    let ProfileActionState {
        subject_did,
        following,
        blocked,
        muted,
        follow_access,
        block_access,
        mute_access,
        list_access,
        lists,
    } = state;
    let follow = if follow_access {
        profile_follow_control(subject_did, following)
    } else {
        permission_reauthorization_link(if following { "unfollow" } else { "follow" })
    };
    let block = if !block_access {
        permission_reauthorization_link(if blocked { "unblock" } else { "block" })
    } else if blocked {
        profile_action_form(subject_did, "unblock", "Unblock account", None)
    } else {
        let did = form_encoded(subject_did);
        format!(
            "<a class=\"profile-menu-item\" href=\"/profile/block-confirm?did={did}\" data-block-confirm aria-haspopup=\"dialog\">Block account</a>"
        )
    };
    let mute = if mute_access {
        if muted {
            profile_action_form(subject_did, "unmute", "Unmute account", None)
        } else {
            profile_action_form(subject_did, "mute", "Mute account", None)
        }
    } else {
        permission_reauthorization_link(if muted { "unmute" } else { "mute" })
    };
    let lists_link = if list_access {
        "<a class=\"profile-menu-item\" href=\"#profile-lists\" data-focus-list>Add to lists...</a>"
            .to_owned()
    } else {
        permission_reauthorization_link("lists")
    };
    let list_control = if !list_access {
        permission_reauthorization_link("lists")
    } else if lists.is_empty() {
        "<p class=\"list-empty\">No lists found in your account.</p>".to_owned()
    } else {
        let selected = &lists[0];
        let selected_member = selected.item_uri.is_some();
        let options = lists
            .iter()
            .enumerate()
            .map(|(index, list)| {
                let check = if list.item_uri.is_some() { " ✓" } else { "" };
                let selected = if index == 0 { " selected" } else { "" };
                format!(
                    "<option value=\"{}\" data-member=\"{}\"{selected}>{}{check}</option>",
                    html_escape(&list.uri),
                    list.item_uri.is_some(),
                    html_escape(&list.name),
                )
            })
            .collect::<String>();
        let action_label = if selected_member { "Remove" } else { "Add" };
        format!(
            "<form class=\"profile-list-row\" id=\"profile-lists\" action=\"/profile/action\" method=\"post\" data-profile-action data-profile-list-form><input type=\"hidden\" name=\"subject\" value=\"{}\"><input type=\"hidden\" name=\"action\" value=\"list_toggle\"><label class=\"sr-only\" for=\"profile-list-select\">Choose a list</label><select id=\"profile-list-select\" name=\"list_uri\" data-profile-list-select>{options}</select><button class=\"profile-list-action\" type=\"submit\">{action_label}</button></form>",
            html_escape(subject_did)
        )
    };
    format!(
        "<section class=\"profile-actions\" aria-label=\"Profile actions\"><div class=\"profile-control-row\">{follow}<details class=\"profile-overflow\"><summary class=\"profile-overflow-trigger\" aria-label=\"More profile actions\"><svg class=\"icon\" aria-hidden=\"true\" focusable=\"false\"><use href=\"#i-more\"></use></svg></summary><div class=\"profile-menu\" role=\"menu\">{lists_link}{mute}{block}</div></details></div><div class=\"profile-list-actions\">{list_control}</div></section>"
    )
}

fn profile_follow_control(subject_did: &str, following: bool) -> String {
    let (action, label, class, aria_label) = if following {
        ("unfollow", "Following", "following", "Unfollow")
    } else {
        ("follow", "Follow", "follow", "Follow")
    };
    format!(
        "<form class=\"profile-primary-action {class}\" action=\"/profile/action\" method=\"post\" data-profile-action><input type=\"hidden\" name=\"subject\" value=\"{}\"><input type=\"hidden\" name=\"action\" value=\"{action}\"><button type=\"submit\" aria-label=\"{aria_label}\">{label}</button></form>",
        html_escape(subject_did)
    )
}

fn permission_reauthorization_link(action: &str) -> String {
    let description = match action {
        "follow" => "follow",
        "unfollow" => "unfollow",
        "block" => "block",
        "unblock" => "unblock",
        "mute" => "mute",
        "unmute" => "unmute",
        _ => "add to lists",
    };
    format!("<a class=\"permission-prompt\" href=\"/login\">Sign in again to {description}</a>")
}

fn profile_action_form(
    subject_did: &str,
    action: &str,
    label: &str,
    extra: Option<(&str, &str)>,
) -> String {
    let extra = extra
        .map(|(name, value)| {
            format!(
                "<input type=\"hidden\" name=\"{}\" value=\"{}\">",
                html_escape(name),
                html_escape(value)
            )
        })
        .unwrap_or_default();
    format!(
        "<form class=\"profile-action-form\" action=\"/profile/action\" method=\"post\" data-profile-action><input type=\"hidden\" name=\"subject\" value=\"{}\"><input type=\"hidden\" name=\"action\" value=\"{}\">{extra}<button type=\"submit\">{}</button></form>",
        html_escape(subject_did),
        html_escape(action),
        html_escape(label)
    )
}

/// Decide whether a profile shows "Follows you" from the viewer's own
/// `follows` rows, confirmed by `app.bsky.graph.getRelationships` before the
/// first render for a pair. The stored rows drive every fallback path: a
/// confirmed pair answers from them, a stale confirmation serves them and
/// refreshes once in the background, and any fetch failure keeps them and only
/// warns. Confirmations live in the persistent `web_cache` keyed per pair so a
/// restart cannot make the user re-confirm.
async fn confirm_follows_you<S>(
    relationships: &RelationshipClient,
    storage: Arc<S>,
    viewer: &str,
    subject: &str,
    timing: &mut WebPageTiming,
) -> Result<bool, String>
where
    S: Storage + 'static,
{
    let started = std::time::Instant::now();
    let stored = storage
        .is_following(subject, viewer)
        .map_err(|error| error.to_string())?;
    let mut answer = stored;
    let mut label = "none";
    if relationships.relationships_available() {
        let key = relationship_cache_key(viewer, subject);
        let confirmed_at_ms = relationships.load_confirmation(viewer, subject).await?;
        let ttl_ms = RELATIONSHIP_CONFIRMATION_TTL.as_millis() as i64;
        let stale = confirmed_at_ms
            .map(|at_ms| now_millis().saturating_sub(at_ms) >= ttl_ms)
            .unwrap_or(false);
        match confirmed_at_ms {
            Some(_) if stale => {
                label = "stale";
                if relationships.begin_refresh(&key) {
                    let relationships = relationships.clone();
                    let storage = storage.clone();
                    let viewer = viewer.to_owned();
                    let subject = subject.to_owned();
                    let dispatcher =
                        tracing::dispatcher::get_default(|dispatcher| dispatcher.clone());
                    // The task owns the in-progress cleanup through the guard, so a
                    // refresh interrupted mid-flight cannot strand the pair.
                    tokio::spawn(
                        async move {
                            let _guard = relationships.refresh_guard(&key);
                            match tokio::time::timeout(
                                relationships.timeout(),
                                relationships.fetch_relationship(&viewer, &subject),
                            )
                            .await
                            {
                                Ok(Ok(state)) => {
                                    match relationships.apply_relationship(
                                        storage.as_ref(),
                                        &viewer,
                                        &subject,
                                        state,
                                    ) {
                                        Ok(rows) => {
                                            relationships
                                                .record_confirmation(&viewer, &subject, state.followed_by);
                                            tracing::debug!(
                                                viewer = %viewer,
                                                subject = %subject,
                                                relationship_rows_written = rows,
                                                "follows-you relationship refreshed in the background"
                                            );
                                        }
                                        Err(error) => tracing::warn!(
                                            viewer = %viewer,
                                            subject = %subject,
                                            error = %error,
                                            "follows-you background relationship rows failed; keeping the stored answer"
                                        ),
                                    }
                                }
                                Ok(Err(error)) => tracing::warn!(
                                    viewer = %viewer,
                                    subject = %subject,
                                    error = %error,
                                    "follows-you background relationship refresh failed; keeping the stored answer"
                                ),
                                Err(_) => tracing::warn!(
                                    viewer = %viewer,
                                    subject = %subject,
                                    "follows-you background relationship refresh timed out; keeping the stored answer"
                                ),
                            }
                        }
                        .with_subscriber(dispatcher),
                    );
                }
            }
            Some(_) => label = "fresh",
            None => {
                timing.relationship_requests += 1;
                match tokio::time::timeout(
                    relationships.timeout(),
                    relationships.fetch_relationship(viewer, subject),
                )
                .await
                {
                    Ok(Ok(state)) => {
                        let rows = relationships.apply_relationship(
                            storage.as_ref(),
                            viewer,
                            subject,
                            state,
                        )?;
                        relationships.record_confirmation(viewer, subject, state.followed_by);
                        timing.relationship_rows_written += rows;
                        label = "confirmed";
                        answer = state.followed_by;
                    }
                    Ok(Err(error)) => {
                        label = "failed";
                        tracing::warn!(
                            viewer = %viewer,
                            subject = %subject,
                            error = %error,
                            "follows-you relationship confirmation failed; serving the stored answer"
                        );
                    }
                    Err(_) => {
                        label = "failed";
                        tracing::warn!(
                            viewer = %viewer,
                            subject = %subject,
                            error = "getRelationships request timed out",
                            "follows-you relationship confirmation timed out; serving the stored answer"
                        );
                    }
                }
            }
        }
    }
    timing.relationship_answer = label;
    timing.relationship_ms = started.elapsed().as_millis() as u64;
    Ok(answer)
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
    let saved_feeds_started = std::time::Instant::now();
    let tabs = pinned_feeds_for_layout(
        &state.flow,
        &did,
        verified_cookie_feed_access(&state.public, &headers, state.storage.as_ref()),
    )
    .await;
    page_timing.saved_feeds_ms = saved_feeds_started.elapsed().as_millis() as u64;
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
    let parent_result = hydrate_page_parents(
        &state.flow,
        state.storage.as_ref(),
        &records,
        content_deadline,
    )
    .await;
    page_timing.parent_hydration_ms = parent_started.elapsed().as_millis() as u64;
    let parents = match parent_result {
        Ok(parents) => parents,
        Err(error) => return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    let quote_records = page_records_with_parents(&records, &parents);
    let content_started = std::time::Instant::now();
    let content_result = hydrate_page_content(
        &state.flow,
        &did,
        state.storage.clone(),
        &records,
        &quote_records,
        std::iter::once(subject.to_owned()).chain(records.iter().map(|record| record.did.clone())),
        content_deadline,
    )
    .await;
    page_timing.content_hydration_ms = content_started.elapsed().as_millis() as u64;
    let quote_index = match content_result {
        Ok(hydration) => {
            page_timing.profile_hydration_ms = hydration.profile_hydration_ms;
            page_timing.quote_hydration_ms = hydration.quote_hydration_ms;
            page_timing.account_labels_ms = hydration.account_labels_ms;
            hydration.quote_index
        }
        Err(error) => return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    if let Err(error) = queue_missing_parent_authors(state.storage.as_ref(), &parents) {
        return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string());
    }
    let mut profile = match profile_view_from_storage(state.storage.as_ref(), subject) {
        Ok(profile) => profile,
        Err(error) => return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    let profile_counts_started = std::time::Instant::now();
    let counts = state.profile_counts.get_counts(subject).await;
    page_timing.profile_counts_ms = profile_counts_started.elapsed().as_millis() as u64;
    if let Some(counts) = counts {
        apply_profile_counts(&mut profile, counts);
    }
    let handle = profile_handle(&profile, subject);
    let display_name = profile
        .get("displayName")
        .and_then(Value::as_str)
        .unwrap_or(&handle);
    let label_chips = render_account_label_chips(&quote_index.account_labels, subject);
    let follows_you = if subject != did {
        confirm_follows_you(
            &state.relationships,
            state.storage.clone(),
            &did,
            subject,
            &mut page_timing,
        )
        .await
    } else {
        Ok(false)
    };
    let follows_you = match follows_you {
        Ok(true) => "<span class=\"follows-you-pill\">Follows you</span>",
        Ok(false) => "",
        Err(error) => return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error),
    };
    let action_html = if subject == did {
        // The mobile nav has no room for a fifth link; the owner asked for it here (2026-10-30).
        "<section class=\"profile-self-links\" aria-label=\"Your settings\"><div class=\"profile-control-row\"><a class=\"profile-block-lists-link\" href=\"/app/block-lists\"><svg class=\"icon\" aria-hidden=\"true\" focusable=\"false\"><use href=\"#i-block-lists\"></use></svg><span>Block lists</span></a></div></section>".to_owned()
    } else {
        let mut action_timing = ProfileActionTiming::default();
        let action_started = std::time::Instant::now();
        let action_result = profile_action_panel(
            &state.flow,
            &state.public,
            &headers,
            state.storage.as_ref(),
            &did,
            subject,
            &mut action_timing,
        )
        .await;
        page_timing.profile_action_panel_ms = action_started.elapsed().as_millis() as u64;
        page_timing.profile_action_mutes_ms = action_timing.mutes_ms;
        page_timing.profile_list_choices_ms = action_timing.list_choices_ms;
        match action_result {
            Ok(html) => html,
            Err(error) => return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error),
        }
    };
    let description = profile
        .get("description")
        .and_then(Value::as_str)
        .filter(|description| !description.trim().is_empty())
        .map(|description| {
            format!(
                "<p class=\"profile-description\">{}</p>",
                render_profile_description(description)
            )
        })
        .unwrap_or_default();
    let stats = [
        profile_stat(&profile, "followersCount", "Followers"),
        profile_stat(&profile, "followsCount", "Following"),
        profile_stat(&profile, "postsCount", "Posts"),
    ]
    .into_iter()
    .flatten()
    .collect::<String>();
    let write_forms_enabled =
        verified_cookie_write_access(&state.public, &headers, state.storage.as_ref());
    let render_started = std::time::Instant::now();
    let no_list_blocks = HashMap::new();
    let posts = render_timeline_post_items(
        state.storage.as_ref(),
        &records,
        &vec![String::new(); records.len()],
        &parents,
        &TimelineRenderContext {
            viewer_did: &did,
            write_forms_enabled,
            quote_index: &quote_index,
            list_blocks: &no_list_blocks,
        },
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
    let content = render_profile_page_content(ProfilePageContent {
        profile: &profile,
        display_name,
        handle: &handle,
        follows_you,
        label_chips: &label_chips,
        action_html: &action_html,
        description: &description,
        stats: &stats,
        posts: &posts,
        older: &older,
    });
    let response = Html(page_shell_with_feeds_and_badge(
        display_name,
        &content,
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

struct ProfilePageContent<'a> {
    profile: &'a Value,
    display_name: &'a str,
    handle: &'a str,
    follows_you: &'a str,
    label_chips: &'a str,
    action_html: &'a str,
    description: &'a str,
    stats: &'a str,
    posts: &'a str,
    older: &'a str,
}

fn render_profile_page_content(page: ProfilePageContent<'_>) -> String {
    let ProfilePageContent {
        profile,
        display_name,
        handle,
        follows_you,
        label_chips,
        action_html,
        description,
        stats,
        posts,
        older,
    } = page;
    let banner = profile_banner_markup(profile);
    format!(
        "<section class=\"profile-header\">{banner}<div class=\"profile-avatar-row\">{}</div><div class=\"profile-card\"><div class=\"profile-identity-copy\"><h1>{}</h1><p class=\"profile-handle\">@{}</p>{follows_you}<div class=\"profile-labels\">{label_chips}</div></div><div class=\"profile-description-stats\">{description}<div class=\"profile-stats\">{stats}</div></div>{action_html}</div></section><header class=\"section-heading\"><h2>Posts</h2></header><ol class=\"post-list\">{posts}</ol>{older}",
        avatar_markup(profile, "profile-avatar"),
        html_escape(display_name),
        html_escape(handle),
    )
}

fn profile_banner_markup(profile: &Value) -> String {
    let banner = profile
        .get("banner")
        .and_then(Value::as_str)
        .and_then(|value| url::Url::parse(value).ok())
        .filter(|url| {
            url.scheme() == "https"
                && url.host_str() == Some("cdn.bsky.app")
                && url.username().is_empty()
                && url.password().is_none()
        });
    if let Some(banner) = banner {
        format!(
            "<div class=\"profile-banner\"><img src=\"{}\" alt=\"\" decoding=\"async\"></div>",
            html_escape(banner.as_str())
        )
    } else {
        "<div class=\"profile-banner\" aria-hidden=\"true\"></div>".to_owned()
    }
}

fn profile_stat(profile: &Value, field: &str, label: &str) -> Option<String> {
    let count = profile.get(field)?.as_u64()?;
    Some(format!("<span><strong>{count}</strong> {label}</span>"))
}

fn render_profile_description(description: &str) -> String {
    let mut remaining = description;
    let mut rendered = String::new();
    while let Some(start) = [remaining.find("https://"), remaining.find("http://")]
        .into_iter()
        .flatten()
        .min()
    {
        rendered.push_str(&html_escape(&remaining[..start]));
        let candidate = &remaining[start..];
        let end = candidate
            .find(char::is_whitespace)
            .unwrap_or(candidate.len());
        let mut link_end = end;
        while candidate[..link_end]
            .chars()
            .last()
            .is_some_and(|character| {
                matches!(character, '.' | ',' | '!' | '?' | ';' | ':' | ')' | ']')
            })
        {
            link_end -= candidate[..link_end]
                .chars()
                .last()
                .unwrap_or_default()
                .len_utf8();
        }
        let link = &candidate[..link_end];
        let parsed = url::Url::parse(link).ok().filter(|url| {
            matches!(url.scheme(), "http" | "https")
                && url.host_str().is_some()
                && url.username().is_empty()
                && url.password().is_none()
        });
        if let Some(parsed) = parsed {
            rendered.push_str(&format!(
                "<a class=\"profile-description-link\" href=\"{}\" rel=\"nofollow noopener noreferrer\">{}</a>",
                html_escape(parsed.as_str()),
                html_escape(link)
            ));
            rendered.push_str(&html_escape(&candidate[link_end..end]));
        } else {
            rendered.push_str(&html_escape(&candidate[..end]));
        }
        remaining = &candidate[end..];
    }
    rendered.push_str(&html_escape(remaining));
    rendered
}

async fn profile_block_confirm<F, S>(
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
    let Some(subject_did) = query.get("did").map(String::as_str) else {
        return oauth_error(StatusCode::BAD_REQUEST, "profile subject is required");
    };
    if subject_did.parse::<Did>().is_err() {
        return oauth_error(StatusCode::BAD_REQUEST, "profile subject DID is invalid");
    }
    if subject_did == viewer_did {
        return oauth_error(
            StatusCode::FORBIDDEN,
            "profile actions are unavailable on your own profile",
        );
    }
    let handle = match profile_view_from_storage(state.storage.as_ref(), subject_did) {
        Ok(profile) => profile_handle(&profile, subject_did),
        Err(error) => return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    let content = if verified_cookie_block_access(&state.public, &headers, state.storage.as_ref()) {
        render_profile_block_confirm_content(&handle, subject_did)
    } else {
        format!(
            "<section class=\"block-confirm-card\"><h1>Block @{}</h1>{}</section>",
            html_escape(&handle),
            permission_reauthorization_link("block")
        )
    };
    Html(page_shell("Confirm block", &content, true)).into_response()
}

fn render_profile_block_confirm_content(handle: &str, subject_did: &str) -> String {
    format!(
        "<section class=\"block-confirm-card\" aria-labelledby=\"block-confirm-title\"><h1 id=\"block-confirm-title\">Block @{}?</h1><p>They won't be able to see your posts, follow you, or message you.</p><form class=\"block-confirm-actions\" action=\"/profile/action\" method=\"post\" data-profile-action><input type=\"hidden\" name=\"subject\" value=\"{}\"><input type=\"hidden\" name=\"action\" value=\"block\"><input type=\"hidden\" name=\"confirm\" value=\"yes\"><a class=\"secondary-button\" href=\"/profile?did={}\">Cancel</a><button type=\"submit\">Block</button></form></section>",
        html_escape(handle),
        html_escape(subject_did),
        form_encoded(subject_did),
    )
}

async fn profile_action<F, S>(
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
    let Some(viewer_did) = verified_cookie_did(&state.public, &headers, state.storage.as_ref())
    else {
        return Redirect::to("/login").into_response();
    };
    let Some(subject_did) = form.get("subject").map(String::as_str) else {
        return oauth_error(StatusCode::BAD_REQUEST, "profile subject is required");
    };
    let Ok(subject) = subject_did.parse::<Did>() else {
        return oauth_error(StatusCode::BAD_REQUEST, "profile subject DID is invalid");
    };
    let subject_did = subject.as_ref();
    if viewer_did == subject_did {
        return oauth_error(
            StatusCode::FORBIDDEN,
            "profile actions are unavailable on your own profile",
        );
    }
    let Some(action) = form.get("action").map(String::as_str) else {
        return oauth_error(StatusCode::BAD_REQUEST, "profile action is required");
    };
    let (allowed, permission_name) = match action {
        "follow" | "unfollow" => (
            verified_cookie_follow_access(&state.public, &headers, state.storage.as_ref()),
            action,
        ),
        "block" | "unblock" => (
            verified_cookie_block_access(&state.public, &headers, state.storage.as_ref()),
            action,
        ),
        "mute" | "unmute" => (
            verified_cookie_mute_access(&state.public, &headers, state.storage.as_ref()),
            action,
        ),
        "list_add" | "list_remove" | "list_toggle" => (
            verified_cookie_list_access(&state.public, &headers, state.storage.as_ref()),
            "lists",
        ),
        _ => return oauth_error(StatusCode::BAD_REQUEST, "unknown profile action"),
    };
    if !allowed {
        return oauth_error(
            StatusCode::FORBIDDEN,
            &format!("Sign in again to {permission_name}"),
        );
    }
    if matches!(action, "block" | "list_remove")
        && form.get("confirm").map(String::as_str) != Some("yes")
    {
        return oauth_error(
            StatusCode::BAD_REQUEST,
            "confirmation is required for this action",
        );
    }
    let result = match action {
        "follow" => {
            let record = json!({
                "$type":"app.bsky.graph.follow",
                "subject":subject_did,
                "createdAt":Datetime::now()
            });
            match state
                .flow
                .create_profile_graph_record(&viewer_did, "app.bsky.graph.follow", record)
                .await
            {
                Ok(created) => match persist_created_record(
                    state.storage.clone(),
                    &viewer_did,
                    "app.bsky.graph.follow",
                    created,
                )
                .await
                {
                    Ok(()) => state
                        .storage
                        .set_follow(&viewer_did, subject_did, true)
                        .map_err(|error| error.to_string()),
                    Err(error) => Err(error),
                },
                Err(error) => Err(error),
            }
        }
        "unfollow" | "unblock" => {
            let (collection, expected_subject) = if action == "unfollow" {
                ("app.bsky.graph.follow", subject_did)
            } else {
                ("app.bsky.graph.block", subject_did)
            };
            let record =
                match state
                    .storage
                    .records_for_actor_collection(&viewer_did, collection, 10_000)
                {
                    Ok(records) => records.into_iter().find(|record| {
                        record.record.get("subject").and_then(Value::as_str)
                            == Some(expected_subject)
                    }),
                    Err(error) => {
                        return web_write_response(
                            &state.public,
                            &headers,
                            "profile action",
                            action,
                            &viewer_did,
                            StatusCode::INTERNAL_SERVER_ERROR,
                            &error.to_string(),
                        );
                    }
                };
            let Some(record) = record else {
                return oauth_error(StatusCode::BAD_REQUEST, "relationship record was not found");
            };
            match state
                .flow
                .delete_profile_graph_record(&viewer_did, collection, &record.uri)
                .await
            {
                Ok(()) => match remove_deleted_record(
                    state.storage.clone(),
                    &viewer_did,
                    collection,
                    &record.uri,
                )
                .await
                {
                    Ok(()) if action == "unfollow" => state
                        .storage
                        .set_follow(&viewer_did, subject_did, false)
                        .map_err(|error| error.to_string()),
                    Ok(()) => Ok(()),
                    Err(error) => Err(error),
                },
                Err(error) => Err(error),
            }
        }
        "block" => {
            let record = json!({
                "$type":"app.bsky.graph.block",
                "subject":subject_did,
                "createdAt":Datetime::now()
            });
            match state
                .flow
                .create_profile_graph_record(&viewer_did, "app.bsky.graph.block", record)
                .await
            {
                Ok(created) => {
                    persist_created_record(
                        state.storage.clone(),
                        &viewer_did,
                        "app.bsky.graph.block",
                        created,
                    )
                    .await
                }
                Err(error) => Err(error),
            }
        }
        "mute" | "unmute" => {
            state
                .flow
                .set_actor_muted(&viewer_did, subject_did, action == "mute")
                .await
        }
        "list_add" => {
            let Some(list_uri) = form.get("list_uri").map(String::as_str) else {
                return oauth_error(StatusCode::BAD_REQUEST, "list URI is required");
            };
            let list_prefix = format!("at://{viewer_did}/app.bsky.graph.list/");
            if !list_uri.starts_with(&list_prefix)
                || list_uri
                    .strip_prefix(&list_prefix)
                    .is_none_or(str::is_empty)
                || list_uri[list_prefix.len()..].contains('/')
            {
                return oauth_error(StatusCode::FORBIDDEN, "list must belong to your account");
            }
            let lists = match state
                .flow
                .profile_list_choices(&viewer_did, subject_did)
                .await
            {
                Ok(lists) => lists,
                Err(error) => {
                    return web_write_response(
                        &state.public,
                        &headers,
                        "profile action",
                        "list action",
                        &viewer_did,
                        StatusCode::BAD_GATEWAY,
                        &error,
                    );
                }
            };
            if !lists.iter().any(|list| list.uri == list_uri) {
                return oauth_error(
                    StatusCode::FORBIDDEN,
                    "list does not belong to your account",
                );
            }
            if lists
                .iter()
                .any(|list| list.uri == list_uri && list.item_uri.is_some())
            {
                return oauth_error(StatusCode::CONFLICT, "account is already in this list");
            }
            let record = json!({
                "$type":"app.bsky.graph.listitem",
                "subject":subject_did,
                "list":list_uri,
                "createdAt":Datetime::now()
            });
            match state
                .flow
                .create_profile_graph_record(&viewer_did, "app.bsky.graph.listitem", record)
                .await
            {
                Ok(created) => {
                    persist_created_record(
                        state.storage.clone(),
                        &viewer_did,
                        "app.bsky.graph.listitem",
                        created,
                    )
                    .await
                }
                Err(error) => Err(error),
            }
        }
        "list_remove" => {
            let Some(item_uri) = form.get("item_uri").map(String::as_str) else {
                return oauth_error(StatusCode::BAD_REQUEST, "list item URI is required");
            };
            let item_prefix = format!("at://{viewer_did}/app.bsky.graph.listitem/");
            if !item_uri.starts_with(&item_prefix)
                || item_uri
                    .strip_prefix(&item_prefix)
                    .is_none_or(str::is_empty)
                || item_uri[item_prefix.len()..].contains('/')
            {
                return oauth_error(
                    StatusCode::FORBIDDEN,
                    "list item must belong to your account",
                );
            }
            let lists = match state
                .flow
                .profile_list_choices(&viewer_did, subject_did)
                .await
            {
                Ok(lists) => lists,
                Err(error) => {
                    return web_write_response(
                        &state.public,
                        &headers,
                        "profile action",
                        "list action",
                        &viewer_did,
                        StatusCode::BAD_GATEWAY,
                        &error,
                    );
                }
            };
            if !lists
                .iter()
                .any(|list| list.item_uri.as_deref() == Some(item_uri))
            {
                return oauth_error(
                    StatusCode::FORBIDDEN,
                    "list item does not match this account",
                );
            }
            match state
                .flow
                .delete_profile_graph_record(&viewer_did, "app.bsky.graph.listitem", item_uri)
                .await
            {
                Ok(()) => {
                    remove_deleted_record(
                        state.storage.clone(),
                        &viewer_did,
                        "app.bsky.graph.listitem",
                        item_uri,
                    )
                    .await
                }
                Err(error) => Err(error),
            }
        }
        "list_toggle" => {
            let Some(list_uri) = form.get("list_uri").map(String::as_str) else {
                return oauth_error(StatusCode::BAD_REQUEST, "list URI is required");
            };
            let list_prefix = format!("at://{viewer_did}/app.bsky.graph.list/");
            if !list_uri.starts_with(&list_prefix)
                || list_uri
                    .strip_prefix(&list_prefix)
                    .is_none_or(str::is_empty)
                || list_uri[list_prefix.len()..].contains('/')
            {
                return oauth_error(StatusCode::FORBIDDEN, "list must belong to your account");
            }
            let lists = match state
                .flow
                .profile_list_choices(&viewer_did, subject_did)
                .await
            {
                Ok(lists) => lists,
                Err(error) => {
                    return web_write_response(
                        &state.public,
                        &headers,
                        "profile action",
                        "list action",
                        &viewer_did,
                        StatusCode::BAD_GATEWAY,
                        &error,
                    );
                }
            };
            let Some(list) = lists.iter().find(|list| list.uri == list_uri) else {
                return oauth_error(
                    StatusCode::FORBIDDEN,
                    "list does not belong to your account",
                );
            };
            if let Some(item_uri) = list.item_uri.as_deref() {
                let item_prefix = format!("at://{viewer_did}/app.bsky.graph.listitem/");
                if !item_uri.starts_with(&item_prefix)
                    || item_uri
                        .strip_prefix(&item_prefix)
                        .is_none_or(str::is_empty)
                    || item_uri[item_prefix.len()..].contains('/')
                {
                    return oauth_error(
                        StatusCode::FORBIDDEN,
                        "list item must belong to your account",
                    );
                }
                match state
                    .flow
                    .delete_profile_graph_record(&viewer_did, "app.bsky.graph.listitem", item_uri)
                    .await
                {
                    Ok(()) => {
                        remove_deleted_record(
                            state.storage.clone(),
                            &viewer_did,
                            "app.bsky.graph.listitem",
                            item_uri,
                        )
                        .await
                    }
                    Err(error) => Err(error),
                }
            } else {
                let record = json!({
                    "$type":"app.bsky.graph.listitem",
                    "subject":subject_did,
                    "list":list_uri,
                    "createdAt":Datetime::now()
                });
                match state
                    .flow
                    .create_profile_graph_record(&viewer_did, "app.bsky.graph.listitem", record)
                    .await
                {
                    Ok(created) => {
                        persist_created_record(
                            state.storage.clone(),
                            &viewer_did,
                            "app.bsky.graph.listitem",
                            created,
                        )
                        .await
                    }
                    Err(error) => Err(error),
                }
            }
        }
        _ => unreachable!("profile action was validated above"),
    };
    if let Err(error) = result {
        return web_write_response(
            &state.public,
            &headers,
            "profile action",
            action,
            &viewer_did,
            StatusCode::BAD_GATEWAY,
            &error,
        );
    }
    if headers
        .get("x-appview-fetch")
        .is_some_and(|value| value.as_bytes() == b"1")
    {
        let mut action_timing = ProfileActionTiming::default();
        return match profile_action_panel(
            &state.flow,
            &state.public,
            &headers,
            state.storage.as_ref(),
            &viewer_did,
            subject_did,
            &mut action_timing,
        )
        .await
        {
            Ok(actions_html) => Json(json!({"actions_html":actions_html})).into_response(),
            Err(error) => web_write_response(
                &state.public,
                &headers,
                "profile action",
                action,
                &viewer_did,
                StatusCode::INTERNAL_SERVER_ERROR,
                &error,
            ),
        };
    }
    Redirect::to(&format!("/profile?did={}", form_encoded(subject_did))).into_response()
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
    let saved_feeds_started = std::time::Instant::now();
    let tabs = pinned_feeds_for_layout(
        &state.flow,
        &viewer_did,
        verified_cookie_feed_access(&state.public, &headers, state.storage.as_ref()),
    )
    .await;
    page_timing.saved_feeds_ms = saved_feeds_started.elapsed().as_millis() as u64;
    page_timing.preferences_ms = tabs.preferences_ms;
    page_timing.tabs_ms = tabs.tabs_ms;
    let page_deadline = tokio::time::Instant::now() + PAGE_PROFILE_BUDGET;
    let Some(uri) = query.get("uri") else {
        return oauth_error(StatusCode::BAD_REQUEST, "uri is required");
    };
    let record_started = std::time::Instant::now();
    let record_result =
        load_post_record_with_source_until(&state.flow, state.storage.as_ref(), uri, page_deadline)
            .await;
    let record_elapsed_ms = record_started.elapsed().as_millis() as u64;
    let (record, record_was_live) = match record_result {
        Ok(Some(record)) => record,
        Ok(None) => {
            page_timing.timeline_fetch_live_ms += record_elapsed_ms;
            return oauth_error(StatusCode::NOT_FOUND, "post not found");
        }
        Err(error) => {
            page_timing.timeline_fetch_live_ms += record_elapsed_ms;
            return oauth_error(StatusCode::BAD_GATEWAY, &error);
        }
    };
    if record_was_live {
        page_timing.timeline_fetch_live_ms += record_elapsed_ms;
    } else {
        page_timing.timeline_fetch_index_ms += record_elapsed_ms;
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
    let ancestors_result = thread_ancestors(
        &state.flow,
        state.storage.as_ref(),
        &record,
        parent_height,
        page_deadline,
    )
    .await;
    let ancestors_ms = ancestors_started.elapsed().as_millis() as u64;
    let (ancestors, ancestor_stats) = match ancestors_result {
        Ok(ancestors) => ancestors,
        Err(error) => {
            page_timing.timeline_fetch_live_ms += ancestors_ms;
            return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error);
        }
    };
    page_timing.ancestors_from_index = ancestor_stats.from_index;
    page_timing.ancestors_live = ancestor_stats.live;
    page_timing.ancestors_timed_out = ancestor_stats.timed_out;
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
    let content_started = std::time::Instant::now();
    let content_result = hydrate_page_content(
        &state.flow,
        &viewer_did,
        state.storage.clone(),
        &page_records,
        &page_records,
        std::iter::empty(),
        page_deadline,
    )
    .await;
    page_timing.content_hydration_ms = content_started.elapsed().as_millis() as u64;
    let quote_index = match content_result {
        Ok(hydration) => {
            page_timing.profile_hydration_ms = hydration.profile_hydration_ms;
            page_timing.quote_hydration_ms = hydration.quote_hydration_ms;
            page_timing.account_labels_ms = hydration.account_labels_ms;
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
            &viewer_did,
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
    let profile_data = match tokio::time::timeout(
        PAGE_PROFILE_BUDGET,
        state
            .flow
            .page_account_data_for_page(&viewer_did, vec![quoted.did.clone()]),
    )
    .await
    {
        Ok(Ok(data)) => data,
        Ok(Err(error)) => {
            tracing::debug!(%error, "quoted author profile batch failed");
            PageAccountData::default()
        }
        Err(_) => PageAccountData::default(),
    };
    if let Err(error) = hydrate_page_profiles(
        state.storage.as_ref(),
        std::iter::once(quoted.did.clone()),
        profile_data.profiles,
    ) {
        return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string());
    }
    let write_forms_enabled =
        verified_cookie_write_access(&state.public, &headers, state.storage.as_ref());
    let composer = if write_forms_enabled {
        let blob_access =
            verified_cookie_blob_access(&state.public, &headers, state.storage.as_ref());
        let video_access =
            verified_cookie_video_access(&state.public, &headers, state.storage.as_ref());
        let profile = profile_view_from_storage(state.storage.as_ref(), &viewer_did)
            .unwrap_or_else(|_| json!({"did":viewer_did,"handle":viewer_did}));
        render_composer(ComposerSpec {
            form_attributes: "",
            classes: "quote-composer media-composer",
            textarea_id: "quote-text",
            action: "/quote",
            label: "Write a quote post",
            placeholder: "What's up?",
            submit_label: "Post",
            avatar: &composer_avatar_markup(&profile),
            hidden_fields: &format!(
                "<input type=\"hidden\" name=\"quote_uri\" value=\"{}\">",
                html_escape(&quoted.uri)
            ),
            context: "",
            media_picker: render_media_picker(blob_access, video_access, "quote-media"),
            link_picker: render_link_picker("quote-text-link"),
            trailing: &render_quote_preview(state.storage.as_ref(), &quoted),
            media_access: blob_access || video_access,
        })
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

async fn quote_fragment<F, S>(
    State(state): State<Arc<OAuthWebState<F, S>>>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response
where
    F: OAuthFlow,
    S: Storage + 'static,
{
    if verified_cookie_did(&state.public, &headers, state.storage.as_ref()).is_none() {
        return (StatusCode::UNAUTHORIZED, "sign in to load this quoted post").into_response();
    }
    let Some(uri) = query.get("uri").filter(|uri| post_uri_did(uri).is_some()) else {
        return oauth_error(StatusCode::BAD_REQUEST, "uri must identify a post");
    };
    let record = match load_post_record(&state.flow, state.storage.as_ref(), uri).await {
        Ok(Some(record)) => record,
        Ok(None) => return (StatusCode::NOT_FOUND, "quoted post not found").into_response(),
        Err(error) => {
            tracing::debug!(%error, uri, "quoted post fragment fetch failed");
            return (StatusCode::BAD_GATEWAY, "quoted post could not be loaded").into_response();
        }
    };
    let mut index = QuoteRenderIndex::default();
    index.insert(uri.clone(), QuoteRenderState::Found(record));
    let embed = json!({"$type":"app.bsky.embed.record","record":{"record":{"uri":uri}}});
    Html(render_web_quote(state.storage.as_ref(), &embed, &index, 0)).into_response()
}

async fn profile_fragment<F, S>(
    State(state): State<Arc<OAuthWebState<F, S>>>,
    headers: HeaderMap,
    OriginalUri(uri): OriginalUri,
) -> Response
where
    F: OAuthFlow,
    S: Storage + 'static,
{
    let Some(viewer_did) = verified_cookie_did(&state.public, &headers, state.storage.as_ref())
    else {
        return (StatusCode::UNAUTHORIZED, "sign in to load these profiles").into_response();
    };
    let mut dids = Vec::new();
    let mut seen = HashSet::new();
    for (key, value) in url::form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes()) {
        if key != "did" {
            continue;
        }
        let value = value.into_owned();
        if !seen.insert(value.clone()) {
            continue;
        }
        if value.parse::<Did>().is_ok() {
            dids.push(value);
            if dids.len() >= ACCOUNT_LABEL_PROFILE_BATCH_SIZE {
                break;
            }
        }
    }
    if dids.is_empty() {
        return Json(json!({"profiles": {}})).into_response();
    }
    let account_data = match state
        .flow
        .page_account_data_for_page(&viewer_did, dids.clone())
        .await
    {
        Ok(data) => data,
        Err(error) => {
            tracing::debug!(%error, "profile fragment account data could not be fetched");
            PageAccountData::default()
        }
    };
    if let Err(error) = hydrate_page_profiles(
        state.storage.as_ref(),
        dids.iter().cloned(),
        account_data.profiles,
    ) {
        tracing::debug!(%error, "profile fragment could not store the fetched profiles");
    }
    let mut profiles = serde_json::Map::new();
    for did in dids {
        let Ok(profile) = profile_view_from_storage(state.storage.as_ref(), &did) else {
            continue;
        };
        let handle = profile_handle(&profile, &did);
        let display_name = profile
            .get("displayName")
            .and_then(Value::as_str)
            .filter(|name| !name.is_empty());
        let avatar = safe_avatar_url(&profile).and_then(avatar_thumbnail_url);
        if handle == did && display_name.is_none() && avatar.is_none() {
            continue;
        }
        let mut entry = serde_json::Map::new();
        if handle != did {
            entry.insert("handle".to_owned(), Value::String(handle));
        }
        if let Some(display_name) = display_name {
            entry.insert(
                "displayName".to_owned(),
                Value::String(display_name.to_owned()),
            );
        }
        if let Some(avatar) = avatar {
            entry.insert("avatar".to_owned(), Value::String(avatar));
        }
        profiles.insert(did, Value::Object(entry));
    }
    Json(json!({"profiles": Value::Object(profiles)})).into_response()
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
        render_post_text(text, record_facets(&quoted.record))
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
    viewer_did: &str,
    parent: &IndexedRecord,
    blob_access: bool,
    video_access: bool,
) -> String {
    let profile = profile_view_from_storage(storage, &parent.did)
        .unwrap_or_else(|_| json!({"did":parent.did,"handle":parent.did}));
    let handle = profile_handle(&profile, &parent.did);
    let viewer = profile_view_from_storage(storage, viewer_did)
        .unwrap_or_else(|_| json!({"did":viewer_did,"handle":viewer_did}));
    render_composer(ComposerSpec {
        form_attributes: "id=\"reply-composer\"",
        classes: "reply-composer media-composer",
        textarea_id: "reply-text",
        action: "/reply",
        label: "Write a reply",
        placeholder: "Write a reply…",
        submit_label: "Reply",
        avatar: &composer_avatar_markup(&viewer),
        hidden_fields: &format!(
            "<input type=\"hidden\" name=\"parent_uri\" value=\"{}\">",
            html_escape(&parent.uri)
        ),
        context: &if author_identity_is_unresolved(&profile, &parent.did) {
            let fill = author_fill(&profile, &parent.did);
            format!(
                "<p class=\"reply-context\"{}>Replying to <span{}>@{}</span></p>",
                fill.container,
                fill.handle,
                html_escape(&handle)
            )
        } else {
            format!(
                "<p class=\"reply-context\">Replying to @{}</p>",
                html_escape(&handle)
            )
        },
        media_picker: render_media_picker(blob_access, video_access, "reply-media"),
        link_picker: render_link_picker("reply-text-link"),
        trailing: "",
        media_access: blob_access || video_access,
    })
}

struct ComposerSpec<'a> {
    form_attributes: &'a str,
    classes: &'a str,
    textarea_id: &'a str,
    action: &'a str,
    label: &'a str,
    placeholder: &'a str,
    submit_label: &'a str,
    avatar: &'a str,
    hidden_fields: &'a str,
    context: &'a str,
    media_picker: String,
    link_picker: String,
    trailing: &'a str,
    media_access: bool,
}

fn render_composer(spec: ComposerSpec<'_>) -> String {
    let ComposerSpec {
        form_attributes,
        classes,
        textarea_id,
        action,
        label,
        placeholder,
        submit_label,
        avatar,
        hidden_fields,
        context,
        media_picker,
        link_picker,
        trailing,
        media_access,
    } = spec;
    let media_button_disabled = if media_access { "" } else { " disabled" };
    format!(
        "<form {form_attributes} class=\"composer {classes}\" action=\"{action}\" method=\"post\" enctype=\"multipart/form-data\" data-media-composer data-compact-composer>{hidden_fields}<div class=\"composer-input-row\">{avatar}<label class=\"sr-only\" for=\"{textarea_id}\">{label}</label><textarea id=\"{textarea_id}\" name=\"text\" rows=\"1\" maxlength=\"300\" placeholder=\"{placeholder}\" required data-compose-input></textarea></div><div class=\"composer-toolbar\" data-compose-toolbar><div class=\"composer-tools\"><button class=\"composer-icon-button\" type=\"button\" data-compose-media aria-label=\"Add photo or video\"{media_button_disabled}>{}</button><button class=\"composer-icon-button\" type=\"button\" data-link-toggle aria-label=\"Add a link\" aria-controls=\"{textarea_id}-link-picker\" aria-expanded=\"false\">{}</button></div><output class=\"composer-character-count\" data-character-count aria-live=\"polite\">0/300</output><button class=\"composer-submit\" type=\"submit\">{submit_label}</button></div><div class=\"composer-fields\">{context}{media_picker}{link_picker}{trailing}</div><p class=\"composer-error\" data-composer-error role=\"alert\" hidden></p></form>",
        icon_svg("image"),
        icon_svg("link"),
    )
}

fn render_media_picker(image_access: bool, video_access: bool, id: &str) -> String {
    if !image_access && !video_access {
        return "<p class=\"picture-permission-hint\" role=\"status\"><a href=\"/login\">Log in again to enable pictures</a></p><p class=\"video-permission-hint\" role=\"status\"><a href=\"/login\">sign in again to post videos</a></p>".to_owned();
    }
    format!(
        "<fieldset class=\"media-picker\" data-media-picker><legend>Add media</legend><input class=\"media-file-input\" id=\"{id}\" type=\"file\" name=\"media\" accept=\"image/*,video/mp4,video/webm,video/quicktime\" multiple data-media-input data-image-access=\"{image_access}\" data-video-access=\"{video_access}\"><label class=\"media-add-button\" for=\"{id}\"><span aria-hidden=\"true\">＋</span> Add media</label><p class=\"media-picker-hint\">Up to 4 pictures, or one video up to 300 MB and 10 minutes.</p><div class=\"media-previews\" data-media-previews></div><p class=\"media-picker-status\" data-media-status role=\"status\" aria-live=\"polite\"></p><label class=\"video-alt-label\" for=\"{id}-alt\" data-video-alt-label hidden>Video alt text (optional)<textarea id=\"{id}-alt\" name=\"video_alt\" maxlength=\"2000\" rows=\"2\" data-video-alt></textarea></label><progress data-video-progress max=\"100\" value=\"0\" hidden></progress><p class=\"video-upload-status\" data-video-upload-status role=\"status\" aria-live=\"polite\"></p></fieldset>"
    )
}

fn render_link_picker(id: &str) -> String {
    format!(
        "<fieldset id=\"{id}-picker\" class=\"link-picker\" data-link-picker><legend>Add a webpage</legend><label for=\"{id}\">Webpage URL</label><div class=\"link-picker-controls\"><input id=\"{id}\" name=\"link_url\" type=\"url\" inputmode=\"url\" placeholder=\"https://example.com/article\" data-link-input><button type=\"button\" data-link-preview>Preview link</button></div><p class=\"link-picker-status\" data-link-status role=\"status\" aria-live=\"polite\"></p><div class=\"link-preview\" data-link-preview-card hidden></div><p class=\"composer-notice\" data-link-image-note hidden>Pictures will be attached to the post; the webpage link will be omitted.</p></fieldset>"
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
    let link_targets = render_post_link_targets(&text, record_facets(&record.record));
    let text = render_post_text(&text, record_facets(&record.record));
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
    let fill = author_fill(&profile, &record.did);
    format!(
        "<article class=\"post-card thread-post-focus\"><header class=\"thread-focus-author\">{}<div class=\"thread-focus-names\"{}><a class=\"author-name\" href=\"{}\"{}>{}</a><a class=\"author-handle\" href=\"{}\"{}>@{}</a></div></header><p class=\"thread-focus-text\">{text}</p>{link_targets}<p class=\"thread-focus-meta\">{time}{reply_policy}</p><hr><div class=\"thread-stats\" aria-label=\"Post statistics\">{stats}</div><hr><div class=\"thread-focus-actions\">{reply_action}{repost_action}{like_action}{share_action}{more_action}</div>{embed}</article>",
        avatar_markup_with_priority_and_attrs(
            &profile,
            "focus-avatar",
            eager_avatar,
            &fill.avatar_attrs,
        ),
        fill.container,
        html_escape(&profile_uri),
        fill.name,
        html_escape(display_name),
        html_escape(&profile_uri),
        fill.handle,
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
            if name == "video" || name == "media" || name == "images" {
                let filename = field.file_name().unwrap_or_default().to_owned();
                let mime_type = field.content_type().unwrap_or_default().to_owned();
                let bytes = field.bytes().await.map_err(|_| {
                    (
                        StatusCode::BAD_REQUEST,
                        "The multipart post form could not be read.".to_owned(),
                    )
                })?;
                if filename.is_empty() && bytes.is_empty() {
                    continue;
                }
                if name == "video" || media_file_is_video(&mime_type, &filename) {
                    return Err((
                        StatusCode::BAD_REQUEST,
                        "Enable JavaScript to validate and upload videos.".to_owned(),
                    ));
                }
                if !matches!(name.as_str(), "media" | "images") {
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
    let has_dimensions = !widths.is_empty() || !heights.is_empty();
    if alts.len() > pending.len()
        || widths.len() != heights.len()
        || (has_dimensions && widths.len() != pending.len())
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
            let dimensions = if has_dimensions {
                widths[index]
                    .parse::<u32>()
                    .ok()
                    .zip(heights[index].parse::<u32>().ok())
            } else {
                image_dimensions(&image.bytes, &mime_type)
            };
            let Some((width, height)) = dimensions
                .filter(|(width, height)| (1..=2000).contains(width) && (1..=2000).contains(height))
            else {
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

fn media_file_is_video(mime_type: &str, filename: &str) -> bool {
    let mime_type = mime_type.trim().to_ascii_lowercase();
    mime_type.starts_with("video/")
        || matches!(
            Path::new(filename)
                .extension()
                .and_then(|extension| extension.to_str())
                .unwrap_or_default()
                .to_ascii_lowercase()
                .as_str(),
            "mp4" | "webm" | "mov"
        )
}

fn image_dimensions(bytes: &[u8], mime_type: &str) -> Option<(u32, u32)> {
    match mime_type {
        "image/png" if bytes.len() >= 24 => Some((
            u32::from_be_bytes(bytes[16..20].try_into().ok()?),
            u32::from_be_bytes(bytes[20..24].try_into().ok()?),
        )),
        "image/gif" if bytes.len() >= 10 => Some((
            u16::from_le_bytes(bytes[6..8].try_into().ok()?) as u32,
            u16::from_le_bytes(bytes[8..10].try_into().ok()?) as u32,
        )),
        "image/jpeg" => jpeg_dimensions(bytes),
        "image/webp" => webp_dimensions(bytes),
        _ => None,
    }
}

fn jpeg_dimensions(bytes: &[u8]) -> Option<(u32, u32)> {
    if !bytes.starts_with(&[0xff, 0xd8]) {
        return None;
    }
    let mut offset = 2;
    while offset + 4 <= bytes.len() {
        if bytes[offset] != 0xff {
            offset += 1;
            continue;
        }
        while offset < bytes.len() && bytes[offset] == 0xff {
            offset += 1;
        }
        let marker = *bytes.get(offset)?;
        offset += 1;
        if matches!(marker, 0xd8 | 0xd9 | 0x01 | 0xd0..=0xd7) {
            continue;
        }
        let length = u16::from_be_bytes(bytes.get(offset..offset + 2)?.try_into().ok()?) as usize;
        if length < 2 || offset + length > bytes.len() {
            return None;
        }
        if matches!(marker, 0xc0..=0xc3 | 0xc5..=0xc7 | 0xc9..=0xcb | 0xcd..=0xcf) {
            let height =
                u16::from_be_bytes(bytes.get(offset + 3..offset + 5)?.try_into().ok()?) as u32;
            let width =
                u16::from_be_bytes(bytes.get(offset + 5..offset + 7)?.try_into().ok()?) as u32;
            return Some((width, height));
        }
        offset += length;
    }
    None
}

fn webp_dimensions(bytes: &[u8]) -> Option<(u32, u32)> {
    if bytes.len() < 20 || !bytes.starts_with(b"RIFF") || &bytes[8..12] != b"WEBP" {
        return None;
    }
    match &bytes[12..16] {
        b"VP8X" if bytes.len() >= 30 => {
            let width =
                u32::from(bytes[24]) | (u32::from(bytes[25]) << 8) | (u32::from(bytes[26]) << 16);
            let height =
                u32::from(bytes[27]) | (u32::from(bytes[28]) << 8) | (u32::from(bytes[29]) << 16);
            Some((width + 1, height + 1))
        }
        b"VP8 " if bytes.len() >= 30 && bytes.get(23..26) == Some(&[0x9d, 0x01, 0x2a]) => {
            let width = u16::from_le_bytes(bytes[26..28].try_into().ok()?) & 0x3fff;
            let height = u16::from_le_bytes(bytes[28..30].try_into().ok()?) & 0x3fff;
            Some((u32::from(width), u32::from(height)))
        }
        b"VP8L" if bytes.len() >= 25 && bytes[20] == 0x2f => {
            let bits = u32::from_le_bytes(bytes[21..25].try_into().ok()?);
            Some(((bits & 0x3fff) + 1, ((bits >> 14) & 0x3fff) + 1))
        }
        _ => None,
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
            Err(error) => {
                return web_write_response(
                    &state.public,
                    &headers,
                    "video",
                    "video post",
                    &did,
                    StatusCode::BAD_GATEWAY,
                    &error,
                );
            }
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
            Err(error) => {
                return web_write_response(
                    &state.public,
                    &headers,
                    "video",
                    "video post",
                    &did,
                    StatusCode::BAD_GATEWAY,
                    &error,
                );
            }
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
            return web_write_response(
                &state.public,
                &headers,
                "video",
                "video post",
                &did,
                StatusCode::BAD_GATEWAY,
                &error,
            );
        }
    };
    if submission.parent_uri.is_some() {
        redirect = format!("/app/thread?uri={}", form_encoded(&created.uri));
    }
    match persist_created_record(state.storage.clone(), &did, "app.bsky.feed.post", created).await {
        Ok(()) => Redirect::to(&redirect).into_response(),
        Err(error) => web_write_response(
            &state.public,
            &headers,
            "video",
            "video post",
            &did,
            StatusCode::INTERNAL_SERVER_ERROR,
            &error,
        ),
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
                Err(error) => web_write_response(
                    &state.public,
                    &headers,
                    "post",
                    "post",
                    &did,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &error,
                ),
            }
        }
        Err(error) => image_or_text_action_error(
            &state.public,
            &headers,
            "post",
            "post",
            &did,
            error,
            has_images || has_external,
        ),
    }
}

fn image_or_text_action_error(
    public: &OAuthPublic,
    headers: &HeaderMap,
    route: &str,
    verb: &str,
    did: &str,
    error: String,
    has_media: bool,
) -> Response {
    if !has_media {
        return web_write_response(
            public,
            headers,
            route,
            verb,
            did,
            StatusCode::BAD_GATEWAY,
            &error,
        );
    }
    tracing::warn!(
        route,
        did = %did,
        error = %error,
        "post media upload failed before record creation"
    );
    record_web_write_failure(&error);
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
        Err(error) => {
            return web_write_response(
                &state.public,
                &headers,
                "quote",
                "quote",
                &did,
                StatusCode::BAD_GATEWAY,
                &error,
            );
        }
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
        Err(error) => {
            return image_or_text_action_error(
                &state.public,
                &headers,
                "quote",
                "quote",
                &did,
                error,
                has_images || has_external,
            );
        }
    };
    match persist_created_record(state.storage.clone(), &did, "app.bsky.feed.post", created).await {
        Ok(()) => Redirect::to("/app").into_response(),
        Err(error) => web_write_response(
            &state.public,
            &headers,
            "quote",
            "quote",
            &did,
            StatusCode::INTERNAL_SERVER_ERROR,
            &error,
        ),
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
        Err(error) => {
            return web_write_response(
                &state.public,
                &headers,
                "reply",
                "reply",
                &did,
                StatusCode::BAD_GATEWAY,
                &error,
            );
        }
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
        Err(error) => {
            return image_or_text_action_error(
                &state.public,
                &headers,
                "reply",
                "reply",
                &did,
                error,
                has_images || has_external,
            );
        }
    };
    let redirect = format!("/app/thread?uri={}", form_encoded(&created.uri));
    match persist_created_record(state.storage.clone(), &did, "app.bsky.feed.post", created).await {
        Ok(()) => Redirect::to(&redirect).into_response(),
        Err(error) => web_write_response(
            &state.public,
            &headers,
            "reply",
            "reply",
            &did,
            StatusCode::INTERNAL_SERVER_ERROR,
            &error,
        ),
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
                return web_write_response(
                    &state.public,
                    &headers,
                    "like",
                    "like",
                    &did,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &error.to_string(),
                );
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
                Err(error) => web_write_response(
                    &state.public,
                    &headers,
                    "like",
                    "like",
                    &did,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &error,
                ),
            },
            Err(error) => web_write_response(
                &state.public,
                &headers,
                "like",
                "like",
                &did,
                StatusCode::BAD_GATEWAY,
                &error,
            ),
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
                Err(error) => web_write_response(
                    &state.public,
                    &headers,
                    "like",
                    "like",
                    &did,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &error,
                ),
            }
        }
        Err(error) => web_write_response(
            &state.public,
            &headers,
            "like",
            "like",
            &did,
            StatusCode::BAD_GATEWAY,
            &error,
        ),
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
            Err(error) => web_write_response(
                &state.public,
                &headers,
                "repost",
                "repost",
                &did,
                StatusCode::INTERNAL_SERVER_ERROR,
                &error,
            ),
        },
        Err(error) => web_write_response(
            &state.public,
            &headers,
            "repost",
            "repost",
            &did,
            StatusCode::BAD_GATEWAY,
            &error,
        ),
    }
}

fn verified_cookie_did<S: Storage>(
    public: &OAuthPublic,
    headers: &HeaderMap,
    storage: &S,
) -> Option<String> {
    verified_cookie_session(public, headers, storage).map(|(did, _, _, _)| did)
}

fn verified_cookie_write_access<S: Storage>(
    public: &OAuthPublic,
    headers: &HeaderMap,
    storage: &S,
) -> bool {
    verified_cookie_session(public, headers, storage)
        .is_some_and(|(_, _, _, permissions)| permissions.write)
}

fn verified_cookie_blob_access<S: Storage>(
    public: &OAuthPublic,
    headers: &HeaderMap,
    storage: &S,
) -> bool {
    verified_cookie_session(public, headers, storage)
        .is_some_and(|(_, _, _, permissions)| permissions.blob)
}

fn verified_cookie_video_access<S: Storage>(
    public: &OAuthPublic,
    headers: &HeaderMap,
    storage: &S,
) -> bool {
    verified_cookie_session(public, headers, storage)
        .is_some_and(|(_, _, _, permissions)| permissions.video)
}

fn verified_cookie_feed_access<S: Storage>(
    public: &OAuthPublic,
    headers: &HeaderMap,
    storage: &S,
) -> bool {
    verified_cookie_session(public, headers, storage)
        .is_some_and(|(_, _, _, permissions)| permissions.feed)
}

fn verified_cookie_preferences_access<S: Storage>(
    public: &OAuthPublic,
    headers: &HeaderMap,
    storage: &S,
) -> bool {
    verified_cookie_session(public, headers, storage)
        .is_some_and(|(_, _, _, permissions)| permissions.preferences)
}

fn verified_cookie_follow_access<S: Storage>(
    public: &OAuthPublic,
    headers: &HeaderMap,
    storage: &S,
) -> bool {
    verified_cookie_session(public, headers, storage)
        .is_some_and(|(_, _, _, permissions)| permissions.follow)
}

fn verified_cookie_block_access<S: Storage>(
    public: &OAuthPublic,
    headers: &HeaderMap,
    storage: &S,
) -> bool {
    verified_cookie_session(public, headers, storage)
        .is_some_and(|(_, _, _, permissions)| permissions.block)
}

fn verified_cookie_mute_access<S: Storage>(
    public: &OAuthPublic,
    headers: &HeaderMap,
    storage: &S,
) -> bool {
    verified_cookie_session(public, headers, storage)
        .is_some_and(|(_, _, _, permissions)| permissions.mute)
}

fn verified_cookie_list_access<S: Storage>(
    public: &OAuthPublic,
    headers: &HeaderMap,
    storage: &S,
) -> bool {
    verified_cookie_session(public, headers, storage)
        .is_some_and(|(_, _, _, permissions)| permissions.list)
}

fn verified_cookie_session<S: Storage>(
    public: &OAuthPublic,
    headers: &HeaderMap,
    storage: &S,
) -> Option<(String, String, u64, SessionPermissions)> {
    let (did, session_id, generation, permissions) =
        cookie_value(headers).and_then(|value| public.verify_session_cookie(value))?;
    if !public.login_is_allowed(&did) {
        if let Err(error) = storage.delete_web_session(&did, &session_id) {
            tracing::warn!(%error, did = %did, "could not remove a disallowed web session");
        }
        return None;
    }
    (storage
        .web_session_generation(&did, &session_id)
        .ok()
        .flatten()
        == Some(generation))
    .then_some((did, session_id, generation, permissions))
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

fn login_retry_page() -> Response {
    Html(page_shell(
        "Login failed",
        "<section class=\"error-card\"><p class=\"eyebrow\">Sign-in error</p><h1>Login failed, try again</h1><p>Your sign-in could not be completed.</p><a class=\"primary-link\" href=\"/login\">Try again</a></section>",
        false,
    ))
    .into_response()
}

/// The page behind an "Unavailable feed" tab: the creator's PDS said the feed is gone, so offer to
/// remove it from the viewer's saved feeds (owner, 2026-10-31), or to sign in again first when the
/// session predates the putPreferences permission.
fn unavailable_feed_page(feed_uri: &str, can_remove: bool) -> Response {
    let action = if can_remove {
        format!(
            "<form method=\"post\" action=\"/feeds/remove\"><input type=\"hidden\" name=\"uri\" value=\"{}\"><button type=\"submit\" class=\"primary-link\">Remove from my saved feeds</button></form>",
            html_escape(feed_uri)
        )
    } else {
        "<a class=\"primary-link\" href=\"/login\">Sign in again to remove this feed</a>".to_owned()
    };
    Html(page_shell(
        "Unavailable feed",
        &format!(
            "<section class=\"error-card\"><p class=\"eyebrow\">Unavailable feed</p><h1>This feed no longer exists</h1><p>Its creator deleted it, or their account is gone, so it cannot be loaded. It is still in your saved feeds.</p><p class=\"feed-uri\">{}</p>{action}</section>",
            html_escape(feed_uri)
        ),
        false,
    ))
    .into_response()
}

async fn remove_saved_feed<F, S>(
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
    let Some(uri) = form
        .get("uri")
        .filter(|uri| uri.starts_with("at://") && uri.contains("/app.bsky.feed.generator/"))
    else {
        return oauth_error(StatusCode::BAD_REQUEST, "a feed generator uri is required");
    };
    if !verified_cookie_preferences_access(&state.public, &headers, state.storage.as_ref()) {
        return unavailable_feed_page(uri, false);
    }
    match state.flow.remove_saved_feed(&did, uri).await {
        Ok(()) => {
            state
                .feed_page_cache
                .lock()
                .await
                .retain(|(viewer, _, _), _| viewer != &did);
            Redirect::to("/app").into_response()
        }
        Err(error) => web_write_response(
            &state.public,
            &headers,
            "feeds/remove",
            "feed removal",
            &did,
            StatusCode::BAD_GATEWAY,
            &error,
        ),
    }
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
mod preferences_scope_tests {
    use super::has_all_preferences_scopes;

    #[test]
    fn preferences_scope_matches_either_fragment_spelling_but_not_the_wildcard() {
        let encoded =
            "atproto rpc:app.bsky.actor.putPreferences?aud=did:web:api.bsky.app%23bsky_appview";
        let decoded =
            "atproto rpc:app.bsky.actor.putPreferences?aud=did:web:api.bsky.app#bsky_appview";
        let wildcard = "atproto rpc:app.bsky.actor.putPreferences?aud=*";
        assert!(has_all_preferences_scopes(Some(encoded)));
        assert!(has_all_preferences_scopes(Some(decoded)));
        assert!(!has_all_preferences_scopes(Some(wildcard)));
    }
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
            unavailable: false,
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

fn author_identity_is_unresolved(profile: &Value, did: &str) -> bool {
    profile_handle(profile, did) == did
}

const PROFILE_NAME_FILL: &str = " data-profile-name";
const PROFILE_HANDLE_FILL: &str = " data-profile-handle";

#[derive(Default)]
struct AuthorFill {
    container: String,
    name: &'static str,
    handle: &'static str,
    avatar_attrs: String,
}

fn author_fill(profile: &Value, did: &str) -> AuthorFill {
    if !author_identity_is_unresolved(profile, did) {
        return AuthorFill::default();
    }
    let container = format!(" data-profile-did=\"{}\"", html_escape(did));
    AuthorFill {
        avatar_attrs: format!("{container} data-profile-avatar"),
        container,
        name: PROFILE_NAME_FILL,
        handle: PROFILE_HANDLE_FILL,
    }
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
    avatar_markup_with_priority_and_attrs(profile, class_name, eager, "")
}

fn avatar_markup_with_priority_and_attrs(
    profile: &Value,
    class_name: &str,
    eager: bool,
    extra_attrs: &str,
) -> String {
    if let Some(avatar) = safe_avatar_url(profile).and_then(avatar_thumbnail_url) {
        let (width, height) = avatar_dimensions(class_name);
        let loading = if eager { "eager" } else { "lazy" };
        format!(
            "<img class=\"{class_name}\" src=\"{}\" alt=\"\" width=\"{width}\" height=\"{height}\" loading=\"{loading}\" decoding=\"async\"{extra_attrs}>",
            html_escape(&avatar)
        )
    } else {
        format!(
            "<span class=\"{class_name} avatar-placeholder\" aria-hidden=\"true\"{extra_attrs}></span>"
        )
    }
}

/// The composer's own avatar sits above the fold on every page, so it renders without the
/// list-oriented loading/decoding hints; it must not shift the lazy-load budget of post lists.
fn composer_avatar_markup(profile: &Value) -> String {
    if let Some(avatar) = safe_avatar_url(profile).and_then(avatar_thumbnail_url) {
        format!(
            "<img class=\"composer-avatar\" src=\"{}\" alt=\"\" width=\"44\" height=\"44\">",
            html_escape(&avatar)
        )
    } else {
        "<span class=\"composer-avatar avatar-placeholder\" aria-hidden=\"true\"></span>".to_owned()
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

struct PostLinkFacet {
    byte_start: usize,
    byte_end: usize,
    uri: String,
}

fn record_facets(record: &Value) -> &[Value] {
    record
        .get("facets")
        .and_then(Value::as_array)
        .map(Vec::as_slice)
        .unwrap_or(&[])
}

fn is_http_facet_uri(uri: &str) -> bool {
    (uri.get(..6)
        .is_some_and(|prefix| prefix.eq_ignore_ascii_case("https:"))
        && uri.get(6..8) == Some("//"))
        || (uri
            .get(..5)
            .is_some_and(|prefix| prefix.eq_ignore_ascii_case("http:"))
            && uri.get(5..7) == Some("//"))
}

fn uri_without_scheme(uri: &str) -> &str {
    for scheme in ["https://", "http://"] {
        if uri.len() >= scheme.len() && uri[..scheme.len()].eq_ignore_ascii_case(scheme) {
            return &uri[scheme.len()..];
        }
    }
    uri
}

fn collect_post_link_facets(text: &str, facets: &[Value]) -> Vec<PostLinkFacet> {
    let mut found: Vec<PostLinkFacet> = Vec::new();
    for facet in facets {
        let Some(uri) = facet
            .get("features")
            .and_then(Value::as_array)
            .and_then(|features| {
                features.iter().find_map(|feature| {
                    (feature.get("$type").and_then(Value::as_str)
                        == Some("app.bsky.richtext.facet#link"))
                    .then(|| feature.get("uri").and_then(Value::as_str))
                    .flatten()
                })
            })
        else {
            continue;
        };
        if !is_http_facet_uri(uri) {
            continue;
        }
        let Some(byte_start) = facet
            .pointer("/index/byteStart")
            .and_then(Value::as_u64)
            .and_then(|value| usize::try_from(value).ok())
        else {
            continue;
        };
        let Some(byte_end) = facet
            .pointer("/index/byteEnd")
            .and_then(Value::as_u64)
            .and_then(|value| usize::try_from(value).ok())
        else {
            continue;
        };
        if byte_start >= byte_end || byte_end > text.len() {
            continue;
        }
        if !text.is_char_boundary(byte_start) || !text.is_char_boundary(byte_end) {
            continue;
        }
        found.push(PostLinkFacet {
            byte_start,
            byte_end,
            uri: uri.to_owned(),
        });
    }
    found.sort_by_key(|link| (link.byte_start, link.byte_end));
    let mut accepted: Vec<PostLinkFacet> = Vec::new();
    for link in found {
        if accepted
            .last()
            .is_some_and(|previous: &PostLinkFacet| link.byte_start < previous.byte_end)
        {
            continue;
        }
        accepted.push(link);
    }
    accepted
}

fn post_link_anchor(uri: &str, label: &str) -> String {
    format!(
        "<a class=\"post-text-link\" href=\"{}\" rel=\"nofollow noopener noreferrer\" target=\"_blank\">{}</a>",
        html_escape(uri),
        html_escape(label),
    )
}

fn render_post_text(text: &str, facets: &[Value]) -> String {
    let links = collect_post_link_facets(text, facets);
    if links.is_empty() {
        return render_post_text_plain(text);
    }
    let mut rendered = String::new();
    let mut position = 0usize;
    for link in &links {
        rendered.push_str(&html_escape(&text[position..link.byte_start]));
        rendered.push_str(&post_link_anchor(
            &link.uri,
            &text[link.byte_start..link.byte_end],
        ));
        position = link.byte_end;
    }
    let tail = &text[position..];
    if let Some((separator_start, separator)) = tail
        .char_indices()
        .rev()
        .find(|(_, character)| character.is_whitespace())
    {
        let number_start = separator_start + separator.len_utf8();
        let number = &tail[number_start..];
        if is_thread_number(number) {
            rendered.push_str(&html_escape(&tail[..number_start]));
            rendered.push_str(&format!(
                "<span class=\"thread-number-pill\">{}</span>",
                html_escape(number)
            ));
            return rendered;
        }
    }
    rendered.push_str(&html_escape(tail));
    rendered
}

fn render_post_link_targets(text: &str, facets: &[Value]) -> String {
    let mut seen: HashSet<String> = HashSet::new();
    let mut rendered = String::new();
    for link in collect_post_link_facets(text, facets) {
        if &text[link.byte_start..link.byte_end] == uri_without_scheme(&link.uri) {
            continue;
        }
        if !seen.insert(link.uri.clone()) {
            continue;
        }
        rendered.push_str(&format!(
            "<p class=\"post-link-targets\"><a class=\"post-link-target\" href=\"{}\" rel=\"nofollow noopener noreferrer\" target=\"_blank\">{}</a></p>",
            html_escape(&link.uri),
            html_escape(&link.uri),
        ));
    }
    rendered
}

fn render_post_text_plain(text: &str) -> String {
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
    let label_chips = render_account_label_chips(&quote_index.account_labels, &record.did);
    let fill = author_fill(&profile, &record.did);
    let author = format!(
        "{}<div class=\"post-author\"{}><a class=\"author-name\" href=\"{}\"{}>{}</a>{label_chips}<a class=\"author-handle\" href=\"{}\"{}>@{}</a></div>",
        avatar_markup_with_priority_and_attrs(&profile, "avatar", eager_avatar, &fill.avatar_attrs,),
        fill.container,
        html_escape(&profile_uri),
        fill.name,
        html_escape(display_name),
        html_escape(&profile_uri),
        fill.handle,
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
    let link_targets = render_post_link_targets(text, record_facets(&record.record));
    let text = render_post_text(text, record_facets(&record.record));
    let text = if link_text_to_thread && !text.contains("<a ") {
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
        "<article class=\"post-card\" id=\"{anchor}\" data-post-kind=\"post\" data-post-uri=\"{uri}\"><header class=\"post-header\">{author}{time}</header><p class=\"post-text\">{text}</p>{link_targets}{embed}<div class=\"post-actions\" aria-label=\"Post actions\">{actions}</div></article>"
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
    let fill = author_fill(&profile, &parent.did);
    format!(
        "<a class=\"reply-parent-card\" href=\"{}\" aria-label=\"Open parent post by @{}\" data-post-kind=\"parent\" data-post-uri=\"{uri}\"><header class=\"reply-parent-header\">{}<span class=\"reply-parent-author\"{}><strong{}>{}</strong><span{}>@{}</span></span></header><p class=\"reply-parent-text\">{}</p></a>",
        html_escape(&thread_uri),
        html_escape(&handle),
        avatar_markup_with_priority_and_attrs(
            &profile,
            "reply-parent-avatar",
            false,
            &fill.avatar_attrs,
        ),
        fill.container,
        fill.name,
        html_escape(&display_name),
        fill.handle,
        html_escape(&handle),
        render_post_text(text, &[]),
    )
}

fn render_reply_parent_fallback<S: Storage>(storage: &S, parent_uri: &str) -> String {
    let thread_uri = format!("/app/thread?uri={}", form_encoded(parent_uri));
    let Some(did) = post_uri_did(parent_uri) else {
        return "<p class=\"reply-parent-fallback\">Reply to a deleted post</p>".to_owned();
    };
    let (profile, handle, _) = parent_display_identity(storage, did);
    let fill = author_fill(&profile, did);
    let link_attrs = format!("{}{}", fill.container, fill.handle);
    format!(
        "<p class=\"reply-parent-fallback\">Reply to <a href=\"{}\"{}>@{}</a></p>",
        html_escape(&thread_uri),
        link_attrs,
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
    list_block_attrs: String,
}

#[derive(Default)]
struct TimelinePostGrouping {
    groups_by_newest: HashMap<usize, TimelinePostGroup>,
    grouped_indices: HashSet<usize>,
}

struct TimelinePostGroup {
    page_members: Vec<usize>,
    ancestors: Vec<IndexedRecord>,
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
        while let Some(next) = children.get(&current).filter(|next| next.len() == 1) {
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
        let mut ancestors_near_to_far = Vec::new();
        while let Some(uri) = parent_uri {
            if !visited_parents.insert(uri) {
                break;
            }
            match parents.get(uri) {
                Some(Some(parent)) => {
                    oldest = oldest.min(parent.time_us);
                    ancestors_near_to_far.push(parent.clone());
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
            .chain(
                ancestors_near_to_far
                    .iter()
                    .map(|ancestor| ancestor.time_us),
            )
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
        grouping.grouped_indices.extend(chain.iter().copied());
        grouping.groups_by_newest.insert(
            newest_index,
            TimelinePostGroup {
                page_members: chain,
                ancestors: ancestors_near_to_far.into_iter().rev().collect(),
            },
        );
    }
    grouping
}

struct TimelineRenderContext<'a> {
    viewer_did: &'a str,
    write_forms_enabled: bool,
    quote_index: &'a QuoteRenderIndex,
    list_blocks: &'a HashMap<String, ListBlockMark>,
}

fn render_timeline_post_items<S: Storage>(
    storage: &S,
    records: &[&IndexedRecord],
    preludes: &[String],
    parents: &PageParentIndex,
    context: &TimelineRenderContext<'_>,
) -> String {
    let grouping = timeline_post_grouping(records, parents);
    let mut html = String::new();
    for (index, record) in records.iter().enumerate() {
        if let Some(group) = grouping.groups_by_newest.get(&index) {
            let mut members = group
                .ancestors
                .iter()
                .map(|record| (record, None))
                .collect::<Vec<_>>();
            members.extend(
                group
                    .page_members
                    .iter()
                    .map(|member| (records[*member], Some(*member))),
            );
            let attrs = timeline_list_block_attrs(
                context.list_blocks,
                members.iter().map(|(member, _)| member.did.as_str()),
            );
            html.push_str(&format!(
                "<li class=\"post-item thread-group\"{attrs} aria-label=\"Reply thread\">"
            ));
            let self_thread = members.first().is_some_and(|(first, _)| {
                members.iter().all(|(member, _)| member.did == first.did)
            });
            let visible = if self_thread && members.len() > 10 {
                (0..members.len())
                    .take(2)
                    .chain((0..members.len()).rev().take(3).rev())
                    .collect::<Vec<_>>()
            } else if !self_thread && members.len() > 3 {
                (0..members.len())
                    .take(1)
                    .chain((0..members.len()).rev().take(2).rev())
                    .collect::<Vec<_>>()
            } else {
                (0..members.len()).collect()
            };
            for (position, member_index) in visible.iter().enumerate() {
                if position > 0 {
                    html.push_str(
                        "<span class=\"reply-connector reply-connector-previous thread-group-connector\" aria-hidden=\"true\"></span>",
                    );
                }
                let (member, page_index) = members[*member_index];
                if let Some(page_index) = page_index {
                    html.push_str(preludes.get(page_index).map(String::as_str).unwrap_or(""));
                }
                html.push_str(&render_web_post(
                    storage,
                    member,
                    context.viewer_did,
                    context.write_forms_enabled,
                    true,
                    page_index.is_none_or(|page_index| page_index < 6),
                    context.quote_index,
                ));
            }
            let skipped = members.len().saturating_sub(visible.len());
            if skipped > 0 {
                let root_uri = &members[0].0.uri;
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
                    viewer_did: context.viewer_did,
                    write_forms_enabled: context.write_forms_enabled,
                    eager_avatar: index < 6,
                    quote_index: context.quote_index,
                    list_block_attrs: timeline_list_block_attrs(
                        context.list_blocks,
                        [record.did.as_str()],
                    ),
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
        return format!(
            "<li class=\"post-item\"{}>{}{article}</li>",
            options.list_block_attrs, options.prelude
        );
    };
    if options.previous_uri == Some(parent_uri) {
        return format!(
            "<li class=\"post-item reply-connected-to-previous\"{}>{}<span class=\"reply-connector reply-connector-previous\" aria-hidden=\"true\"></span>{article}</li>",
            options.list_block_attrs, options.prelude
        );
    }
    let parent = options.parents.get(parent_uri).and_then(Option::as_ref);
    let parent_markup = parent
        .map(|parent| render_reply_parent_card(storage, parent))
        .unwrap_or_else(|| render_reply_parent_fallback(storage, parent_uri));
    format!(
        "<li class=\"post-item reply-with-parent\"{}>{}<div class=\"reply-parent-slot\">{parent_markup}<span class=\"reply-connector\" aria-hidden=\"true\"></span></div>{article}</li>",
        options.list_block_attrs, options.prelude
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
        let handle = post_uri_did(uri)
            .and_then(|did| {
                let profile = profile_view_from_storage(storage, did).ok()?;
                let handle = profile_handle(&profile, did);
                (handle != did).then_some(handle)
            })
            .map(|handle| format!(" · @{}", html_escape(&handle)))
            .unwrap_or_default();
        return format!(
            "<a class=\"quoted-nested-link\" href=\"/app/thread?uri={encoded_uri}\">Quoted post{handle}</a>"
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
                "<div class=\"quoted-post-card quoted-post-deferred\" data-quote-fragment=\"/quote-fragment?uri={encoded_uri}\" data-quote-thread=\"/app/thread?uri={encoded_uri}\"><span>Loading quoted post…</span><a href=\"/app/thread?uri={encoded_uri}\">Open thread</a></div>"
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
    let fill = author_fill(&profile, &quoted.did);
    let label_chips = render_account_label_chips(&quote_index.account_labels, &quoted.did);
    format!(
        "<article class=\"quoted-post-card\"><a class=\"quoted-card-main\" href=\"/app/thread?uri={encoded_uri}\" aria-label=\"Open quoted post thread\">{}<div class=\"quoted-card-body\"><header{}><strong{}>{}</strong><span class=\"quoted-card-handle\"{}>@{}</span>{time}</header><p>{}</p></div></a>{label_chips}{nested}</article>",
        avatar_markup_with_priority_and_attrs(&profile, "small-avatar", false, &fill.avatar_attrs),
        fill.container,
        fill.name,
        html_escape(display_name),
        fill.handle,
        html_escape(&handle),
        render_post_text(text, &[])
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
    // `error` carries the transport failure text (size limit, read failure, timeout...), which status/code/message
    // leave as None for anything that is not an XRPC error response (2026-10-30: getPreferences failed with all three None).
    tracing::warn!(
        method,
        status = ?status,
        error_code = ?error_code,
        error_message = ?error_message,
        error = %error,
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

fn has_all_scopes(scope: Option<&str>, required: &[&str]) -> bool {
    let Some(scope) = scope else {
        return false;
    };
    // An authorization server may echo an audience fragment as `#` or as `%23`; treat them alike.
    let normalize = |s: &str| s.replace("%23", "#");
    let granted = scope
        .split_whitespace()
        .map(normalize)
        .collect::<HashSet<_>>();
    required
        .iter()
        .all(|required| granted.contains(&normalize(required)))
}

fn has_all_follow_scopes(scope: Option<&str>) -> bool {
    has_all_scopes(scope, &FOLLOW_SCOPES)
}

fn has_all_block_scopes(scope: Option<&str>) -> bool {
    has_all_scopes(scope, &BLOCK_SCOPES)
}

fn has_all_mute_scopes(scope: Option<&str>) -> bool {
    has_all_scopes(scope, &MUTE_SCOPES)
}

fn has_all_list_scopes(scope: Option<&str>) -> bool {
    has_all_scopes(scope, &LIST_SCOPES)
}

fn has_all_preferences_scopes(scope: Option<&str>) -> bool {
    has_all_scopes(scope, &PREFERENCES_SCOPES)
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
        .chain(
            FOLLOW_SCOPES
                .iter()
                .map(|scope| Scope::Unknown((*scope).to_owned())),
        )
        .chain(
            BLOCK_SCOPES
                .iter()
                .map(|scope| Scope::Unknown((*scope).to_owned())),
        )
        .chain(
            LIST_SCOPES
                .iter()
                .map(|scope| Scope::Unknown((*scope).to_owned())),
        )
        .chain(
            MUTE_SCOPES
                .iter()
                .map(|scope| Scope::Unknown((*scope).to_owned())),
        )
        .chain(
            PREFERENCES_SCOPES
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
mod oauth_store_writer_tests {
    use super::*;

    #[tokio::test]
    async fn oauth_cache_writer_flushes_pending_writes_when_store_drops() {
        let path = std::env::temp_dir().join(format!(
            "appview-rs-oauth-writer-{}-{}.sqlite3",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let store = SqliteOAuthStore::open(&path).unwrap();
        store
            .web_cache_set("shutdown-cache", r#"{"queued":true}"#)
            .unwrap();
        drop(store);

        let reopened = SqliteOAuthStore::open(&path).unwrap();
        let cached = reopened.web_cache_get("shutdown-cache").await.unwrap();
        assert_eq!(
            cached.as_ref().map(|(value, _)| value.as_str()),
            Some(r#"{"queued":true}"#)
        );
        drop(reopened);
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(format!("{}-wal", path.display()));
        let _ = fs::remove_file(format!("{}-shm", path.display()));
    }
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
    fn write_profile_capture_fixture_when_requested() {
        let Some(output_dir) = std::env::var_os("APPVIEW_CAPTURE_DIR") else {
            return;
        };
        let output_dir = PathBuf::from(output_dir);
        fs::create_dir_all(&output_dir).unwrap();

        let subject_did = "did:plc:riley";
        let mut labels_by_actor = HashMap::new();
        labels_by_actor.insert(
            subject_did.to_owned(),
            vec![AccountLabel {
                subject_did: subject_did.to_owned(),
                labeler_did: "did:plc:careful-labeler".to_owned(),
                labeler_name: "Careful Labeler".to_owned(),
                labeler_handle: "careful.example".to_owned(),
                value: "spider".to_owned(),
                display_name: "Spider Warning".to_owned(),
                description: "This account posted content the labeler flagged as spider-like."
                    .to_owned(),
                severity: "alert".to_owned(),
                blurs: "media".to_owned(),
            }],
        );
        let labels = render_account_label_chips(&labels_by_actor, subject_did);
        let lists = [
            ProfileListChoice {
                uri: "at://did:plc:alice/app.bsky.graph.list/friends".to_owned(),
                name: "Friends".to_owned(),
                purpose: "app.bsky.graph.defs#curatelist".to_owned(),
                item_uri: Some("at://did:plc:alice/app.bsky.graph.listitem/riley".to_owned()),
            },
            ProfileListChoice {
                uri: "at://did:plc:alice/app.bsky.graph.list/reading".to_owned(),
                name: "Reading circle".to_owned(),
                purpose: "app.bsky.graph.defs#curatelist".to_owned(),
                item_uri: None,
            },
        ];
        let actions = render_profile_actions(ProfileActionState {
            subject_did,
            following: true,
            blocked: false,
            muted: false,
            follow_access: true,
            block_access: true,
            mute_access: true,
            list_access: true,
            lists: &lists,
        });
        let display_name = "Riley Morgan";
        let banner_url = "https://cdn.bsky.app/img/banner/plain/fixture@jpeg";
        let variants = [
            ("following", display_name, true),
            ("no-banner", display_name, false),
            (
                "long-name",
                "Riley Morgan writes about gardens, books, careful experiments, and small discoveries throughout the changing seasons",
                true,
            ),
        ];
        for (name, display_name, has_banner) in variants {
            let mut profile = json!({
                "displayName": display_name,
                "handle": "riley.example",
                "avatar": null,
                "banner": banner_url,
                "description": "Field notes, art, and quiet mornings. More at https://example.test/about",
                "followersCount": 128,
                "followsCount": 214,
                "postsCount": 1042
            });
            if !has_banner {
                profile.as_object_mut().unwrap().remove("banner");
            }
            let handle = "riley.example";
            let description = profile["description"]
                .as_str()
                .map(|description| {
                    format!(
                        "<p class=\"profile-description\">{}</p>",
                        render_profile_description(description)
                    )
                })
                .unwrap_or_default();
            let stats = [
                profile_stat(&profile, "followersCount", "Followers"),
                profile_stat(&profile, "followsCount", "Following"),
                profile_stat(&profile, "postsCount", "Posts"),
            ]
            .into_iter()
            .flatten()
            .collect::<String>();
            let content = render_profile_page_content(ProfilePageContent {
                profile: &profile,
                display_name,
                handle,
                follows_you: "<span class=\"follows-you-pill\">Follows you</span>",
                label_chips: &labels,
                action_html: &actions,
                description: &description,
                stats: &stats,
                posts: "<li class=\"empty-state\">No indexed posts yet.</li>",
                older: "",
            });
            let html =
                page_shell_with_feeds_and_badge(display_name, &content, true, &[], None, false, 0)
                    // Keep all browser-fixture traffic local while exercising production markup.
                    .replace(banner_url, "/fixture-banner.svg");
            fs::write(output_dir.join(format!("{name}.html")), html).unwrap();
        }

        let block_confirmation = render_profile_block_confirm_content("riley.example", subject_did);
        let block_confirmation = page_shell("Confirm block", &block_confirmation, true);
        fs::write(output_dir.join("block-confirm.html"), block_confirmation).unwrap();
    }

    #[tokio::test]
    async fn production_account_label_lookup_batches_all_page_authors_and_sends_labeler_header() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let appview_base = format!("http://{address}/xrpc/");
        let requests = Arc::new(Mutex::new(Vec::<(String, Option<String>)>::new()));
        let service_requests = requests.clone();
        let profile_requests = requests.clone();
        let server_app = Router::new()
            .route(
                "/xrpc/app.bsky.actor.getProfiles",
                get(move |uri: Uri, headers: axum::http::HeaderMap| {
                    let requests = profile_requests.clone();
                    async move {
                        requests.lock().unwrap().push((
                            uri.to_string(),
                            headers
                                .get("atproto-accept-labelers")
                                .and_then(|value| value.to_str().ok())
                                .map(str::to_owned),
                        ));
                        Json(json!({"profiles":[
                            {"did":"did:plc:authora","labels":[{"src":"did:plc:customlabeler","uri":"did:plc:authora","val":"spider"}]},
                            {"did":"did:plc:authorb","labels":[{"src":"did:plc:customlabeler","uri":"did:plc:authorb","val":"spider"}]},
                            {"did":"did:plc:authorc","labels":[{"src":"did:plc:customlabeler","uri":"did:plc:authorc","val":"spider"}]}
                        ]}))
                    }
                }),
            )
            .route(
                "/xrpc/app.bsky.labeler.getServices",
                get(move |uri: Uri| {
                    let requests = service_requests.clone();
                    async move {
                        requests.lock().unwrap().push((uri.to_string(), None));
                        Json(json!({"views":[{
                            "creator":{"did":"did:plc:customlabeler","displayName":"Careful Labeler"},
                            "policies":{"labelValueDefinitions":[{
                                "identifier":"spider",
                                "severity":"alert",
                                "blurs":"media",
                                "locales":[{"lang":"en","name":"Spider Warning"}]
                            }]}
                        }]}))
                    }
                }),
            );
        let server = tokio::spawn(async move {
            axum::serve(listener, server_app).await.unwrap();
        });

        let public = OAuthPublic::new("https://appview.example").unwrap();
        let identity = Arc::new(IdentityResolver::new(
            FeedTestIdentity(HashMap::new()),
            Duration::from_secs(30),
        ));
        let flow = AtriumOAuthFlow::new_with_public_appview_base(
            &public,
            identity,
            SqliteOAuthStore::open(":memory:").unwrap(),
            "http://127.0.0.1:1/",
            SafeHttpClient::new().with_http_test_hosts(["127.0.0.1".to_owned()]),
            &appview_base,
        )
        .unwrap()
        // Setup only: this fake server stands in for both appview bases so the
        // labelled getProfiles lands on it too; expectations are unchanged.
        .with_labelled_appview_base(appview_base.clone());
        let viewer = "did:plc:viewer";
        let accepted_labelers = vec![
            BSKY_MODERATION_LABELER_DID.to_owned(),
            "did:plc:customlabeler".to_owned(),
        ];
        flow.labeler_preference_cache.lock().await.insert(
            viewer.to_owned(),
            (
                tokio::time::Instant::now() + Duration::from_secs(60),
                accepted_labelers.clone(),
            ),
        );

        let labels = flow
            .fetch_account_labels_for_page(
                viewer,
                vec![
                    "did:plc:authora".to_owned(),
                    "did:plc:authorb".to_owned(),
                    "did:plc:authorc".to_owned(),
                ],
            )
            .await
            .unwrap();
        assert_eq!(labels.len(), 3, "all fake page authors carry labels");
        assert!(labels.iter().all(|label| {
            label.display_name == "Spider Warning"
                && label.labeler_name == "Careful Labeler"
                && label.severity == "alert"
                && label.blurs == "media"
        }));

        let overlapping = flow
            .fetch_page_account_data_for_page(
                viewer,
                vec!["did:plc:authorb".to_owned(), "did:plc:authorc".to_owned()],
            )
            .await
            .unwrap();
        assert_eq!(overlapping.profiles.len(), 2);
        assert_eq!(overlapping.labels.len(), 2);

        let requests = requests.lock().unwrap();
        let profile_calls = requests
            .iter()
            .filter(|(uri, _)| uri.starts_with("/xrpc/app.bsky.actor.getProfiles?"))
            .collect::<Vec<_>>();
        assert_eq!(
            profile_calls.len(),
            1,
            "production flow should batch authors"
        );
        let query = profile_calls[0].0.split_once('?').unwrap().1;
        let actors = url::form_urlencoded::parse(query.as_bytes())
            .filter_map(|(name, value)| (name == "actors").then(|| value.into_owned()))
            .collect::<Vec<_>>();
        assert_eq!(
            actors,
            ["did:plc:authora", "did:plc:authorb", "did:plc:authorc"]
        );
        let accepted = profile_calls[0].1.as_deref().unwrap();
        assert!(accepted.contains(BSKY_MODERATION_LABELER_DID), "{accepted}");
        assert!(accepted.contains("did:plc:customlabeler"), "{accepted}");
        drop(requests);
        server.abort();
    }

    async fn label_routing_test_flow(
        public_appview_base: String,
        labelled_appview_base: String,
    ) -> (AtriumOAuthFlow<FeedTestIdentity>, String) {
        let public = OAuthPublic::new("https://appview.example").unwrap();
        let identity = Arc::new(IdentityResolver::new(
            FeedTestIdentity(HashMap::new()),
            Duration::from_secs(30),
        ));
        let flow = AtriumOAuthFlow::new_with_public_appview_base(
            &public,
            identity,
            SqliteOAuthStore::open(":memory:").unwrap(),
            "http://127.0.0.1:1/",
            SafeHttpClient::new().with_http_test_hosts(["127.0.0.1".to_owned()]),
            &public_appview_base,
        )
        .unwrap()
        .with_labelled_appview_base(labelled_appview_base);
        let mut preferences = flow.labeler_preference_cache.lock().await;
        for viewer in [
            "did:plc:base-routing-viewer",
            "did:plc:dropped-labeler-viewer",
            "did:plc:applied-labeler-viewer",
        ] {
            preferences.insert(
                viewer.to_owned(),
                (
                    tokio::time::Instant::now() + Duration::from_secs(60),
                    vec![
                        BSKY_MODERATION_LABELER_DID.to_owned(),
                        "did:plc:customlabeler".to_owned(),
                    ],
                ),
            );
        }
        drop(preferences);
        (flow, "did:plc:base-routing-viewer".to_owned())
    }

    async fn label_base_probe_servers() -> (
        String,
        String,
        Arc<Mutex<Vec<String>>>,
        Arc<Mutex<Vec<(String, Option<String>)>>>,
        tokio::task::JoinHandle<()>,
        tokio::task::JoinHandle<()>,
    ) {
        let public_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let public_address = public_listener.local_addr().unwrap();
        let labelled_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let labelled_address = labelled_listener.local_addr().unwrap();

        let public_requests = Arc::new(Mutex::new(Vec::<String>::new()));
        let public_profiles = public_requests.clone();
        let public_services = public_requests.clone();
        let public_app = Router::new()
            .route(
                "/xrpc/app.bsky.actor.getProfiles",
                get(move || {
                    let requests = public_profiles.clone();
                    async move {
                        requests
                            .lock()
                            .unwrap()
                            .push("/xrpc/app.bsky.actor.getProfiles".to_owned());
                        Json(json!({"profiles": []}))
                    }
                }),
            )
            .route(
                "/xrpc/app.bsky.labeler.getServices",
                get(move || {
                    let requests = public_services.clone();
                    async move {
                        requests
                            .lock()
                            .unwrap()
                            .push("/xrpc/app.bsky.labeler.getServices".to_owned());
                        Json(json!({"views":[{
                            "creator":{"did":"did:plc:customlabeler","displayName":"Careful Labeler"},
                            "policies":{"labelValueDefinitions":[{
                                "identifier":"spider",
                                "severity":"alert",
                                "blurs":"media",
                                "locales":[{"lang":"en","name":"Spider Warning"}]
                            }]}
                        }]}))
                    }
                }),
            );
        let public_server = tokio::spawn(async move {
            axum::serve(public_listener, public_app).await.unwrap();
        });

        let labelled_requests = Arc::new(Mutex::new(Vec::<(String, Option<String>)>::new()));
        let labelled_profiles = labelled_requests.clone();
        let labelled_app = Router::new().route(
            "/xrpc/app.bsky.actor.getProfiles",
            get(move |uri: Uri, headers: axum::http::HeaderMap| {
                let requests = labelled_profiles.clone();
                async move {
                    let actors =
                        url::form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
                            .filter(|(name, _)| name == "actors")
                            .map(|(_, value)| value.into_owned())
                            .collect::<Vec<_>>();
                    requests.lock().unwrap().push((
                        uri.to_string(),
                        headers
                            .get("atproto-accept-labelers")
                            .and_then(|value| value.to_str().ok())
                            .map(str::to_owned),
                    ));
                    let profiles = actors
                        .iter()
                        .map(|actor| {
                            json!({
                                "did": actor,
                                "labels": [{
                                    "src": "did:plc:customlabeler",
                                    "uri": actor,
                                    "val": "spider",
                                }]
                            })
                        })
                        .collect::<Vec<_>>();
                    let applied_labelers = if actors
                        .iter()
                        .any(|actor| actor == "did:plc:dropped-label-author")
                    {
                        "did:plc:ar7c4by46qjdydhdevvrndac;redact"
                    } else {
                        "did:plc:ar7c4by46qjdydhdevvrndac;redact,did:plc:customlabeler"
                    };
                    let mut response_headers = axum::http::HeaderMap::new();
                    response_headers.insert(
                        "atproto-content-labelers",
                        axum::http::HeaderValue::from_static(applied_labelers),
                    );
                    (response_headers, Json(json!({"profiles": profiles})))
                }
            }),
        );
        let labelled_server = tokio::spawn(async move {
            axum::serve(labelled_listener, labelled_app).await.unwrap();
        });

        (
            format!("http://{public_address}/xrpc/"),
            format!("http://{labelled_address}/xrpc/"),
            public_requests,
            labelled_requests,
            public_server,
            labelled_server,
        )
    }

    #[tokio::test]
    async fn page_account_labels_fetch_profiles_from_the_labelled_base_not_the_public_one() {
        let (
            public_base,
            labelled_base,
            public_requests,
            labelled_requests,
            public_server,
            labelled_server,
        ) = label_base_probe_servers().await;
        let (flow, viewer) = label_routing_test_flow(public_base, labelled_base).await;

        let labels = flow
            .fetch_account_labels_for_page(
                viewer.as_str(),
                vec!["did:plc:labelled-author".to_owned()],
            )
            .await
            .unwrap();
        assert_eq!(
            labels.len(),
            1,
            "the label returned by the labelled base reaches the page fetch"
        );
        assert_eq!(labels[0].labeler_did, "did:plc:customlabeler");
        assert_eq!(labels[0].display_name, "Spider Warning");

        let rendered = render_account_label_chips(
            &HashMap::from([("did:plc:labelled-author".to_owned(), labels)]),
            "did:plc:labelled-author",
        );
        assert!(
            rendered.contains("<details class=\"account-labels\">"),
            "the fetched label reaches the rendered control: {rendered}"
        );
        assert!(rendered.contains("Spider Warning"), "{rendered}");

        let public_calls = public_requests.lock().unwrap();
        assert!(
            public_calls
                .iter()
                .all(|uri| !uri.starts_with("/xrpc/app.bsky.actor.getProfiles")),
            "getProfiles must not ask the public base: {public_calls:?}"
        );
        assert!(
            public_calls
                .iter()
                .any(|uri| uri.starts_with("/xrpc/app.bsky.labeler.getServices")),
            "labeler presentations still come from the public base: {public_calls:?}"
        );
        drop(public_calls);
        let labelled_calls = labelled_requests.lock().unwrap();
        let profile_calls = labelled_calls
            .iter()
            .filter(|(uri, _)| uri.starts_with("/xrpc/app.bsky.actor.getProfiles?"))
            .collect::<Vec<_>>();
        assert_eq!(
            profile_calls.len(),
            1,
            "one batched getProfiles goes to the labelled base"
        );
        let accepted = profile_calls[0].1.as_deref().unwrap();
        assert!(accepted.contains(BSKY_MODERATION_LABELER_DID), "{accepted}");
        assert!(accepted.contains("did:plc:customlabeler"), "{accepted}");
        drop(labelled_calls);
        public_server.abort();
        labelled_server.abort();
    }

    #[tokio::test]
    async fn dropped_applied_labelers_warn_once_with_requested_and_applied_dids() {
        let (
            public_base,
            labelled_base,
            _public_requests,
            _labelled_requests,
            public_server,
            labelled_server,
        ) = label_base_probe_servers().await;
        let (flow, _) = label_routing_test_flow(public_base, labelled_base).await;

        let warn_logs = FeedTestLogs::default();
        let warn_subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(warn_logs.clone())
            .finish();
        flow.fetch_account_labels_for_page(
            "did:plc:dropped-labeler-viewer",
            vec!["did:plc:dropped-label-author".to_owned()],
        )
        .with_subscriber(warn_subscriber)
        .await
        .unwrap();
        let output = String::from_utf8(warn_logs.0.lock().unwrap().clone()).unwrap();
        let warnings = output
            .lines()
            .filter(|line| {
                line.contains("WARN")
                    && line.contains("Bluesky getProfiles did not apply every requested labeler")
            })
            .collect::<Vec<_>>();
        assert_eq!(warnings.len(), 1, "exactly one warning line: {output}");
        assert!(
            warnings[0].contains("did:plc:customlabeler"),
            "the warning names the dropped labeler: {}",
            warnings[0]
        );
        assert!(
            warnings[0].contains(BSKY_MODERATION_LABELER_DID),
            "the warning names the applied labelers: {}",
            warnings[0]
        );

        let clean_logs = FeedTestLogs::default();
        let clean_subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(clean_logs.clone())
            .finish();
        flow.fetch_account_labels_for_page(
            "did:plc:applied-labeler-viewer",
            vec!["did:plc:labelled-author".to_owned()],
        )
        .with_subscriber(clean_subscriber)
        .await
        .unwrap();
        let clean_output = String::from_utf8(clean_logs.0.lock().unwrap().clone()).unwrap();
        assert!(
            !clean_output.contains("Bluesky getProfiles did not apply every requested labeler"),
            "an applied-labelers header that covers every requested DID stays quiet: {clean_output}"
        );

        public_server.abort();
        labelled_server.abort();
    }

    #[derive(Clone)]
    struct SlowMutesTestFlow;

    impl OAuthFlow for SlowMutesTestFlow {
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

        fn muted_actor_dids<'a>(
            &'a self,
            _: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<HashSet<String>, String>> + Send + 'a>> {
            Box::pin(async {
                tokio::time::sleep(Duration::from_millis(40)).await;
                Ok(HashSet::new())
            })
        }

        fn profile_list_choices<'a>(
            &'a self,
            _: &'a str,
            _: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<ProfileListChoice>, String>> + Send + 'a>>
        {
            Box::pin(async {
                tokio::time::sleep(Duration::from_millis(40)).await;
                Ok(Vec::new())
            })
        }
    }

    #[tokio::test]
    async fn web_page_phase_coverage_accounts_for_a_slow_mutes_wait() {
        let storage = Arc::new(MemoryStorage::default());
        let public = OAuthPublic::new_loopback("127.0.0.1:3000".parse().unwrap()).unwrap();
        let viewer = "did:plc:slow-mutes-viewer";
        let other = "did:plc:slow-list-subject";
        let post = test_web_post(viewer, "phase-coverage", 1, "Fake slow path", None);
        storage.put_record(&post).unwrap();
        let generation = storage
            .rotate_web_session(viewer, "slow-mutes-session")
            .unwrap();
        let token = public.issue_session_cookie(
            viewer,
            "slow-mutes-session",
            generation,
            SessionPermissions {
                write: false,
                feed: true,
                blob: false,
                video: false,
                follow: false,
                block: false,
                mute: true,
                list: true,
                preferences: true,
            },
        );
        let app = public.router_with_flow(SlowMutesTestFlow, storage);
        let logs = FeedTestLogs::default();
        let paths = [
            "/app".to_owned(),
            "/app/feed?uri=at%3A%2F%2Fdid%3Aplc%3Aslow-mutes-viewer%2Fapp.bsky.feed.generator%2Ffeed".to_owned(),
            "/notifications".to_owned(),
            format!("/profile?did={other}"),
            format!("/thread?uri={}", form_encoded(&post.uri)),
        ];
        for path in paths {
            let subscriber = tracing_subscriber::fmt()
                .without_time()
                .with_ansi(false)
                .with_max_level(tracing::Level::INFO)
                .with_writer(logs.clone())
                .finish();
            let response = app
                .clone()
                .oneshot(
                    axum::http::Request::builder()
                        .uri(path)
                        .header("cookie", format!("{SESSION_COOKIE}={token}"))
                        .body(Body::empty())
                        .unwrap(),
                )
                .with_subscriber(subscriber)
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
        }

        let output = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
        let lines = output
            .lines()
            .filter(|line| line.contains("web page phases"))
            .collect::<Vec<_>>();
        assert_eq!(
            lines.len(),
            5,
            "one phase line for every page kind: {output}"
        );
        let metric = |line: &str, name: &str| -> i64 {
            line.split_whitespace()
                .find_map(|field| {
                    field
                        .strip_prefix(&format!("{name}="))
                        .and_then(|value| value.parse().ok())
                })
                .unwrap_or_else(|| panic!("missing {name} in {line}"))
        };
        for line in &lines {
            assert!(metric(line, "phase_gap_ms").abs() <= 5, "{line}");
            assert!(
                metric(line, "total_ms").abs_diff(metric(line, "phase_sum_ms")) <= 5,
                "{line}"
            );
        }
        let app_line = lines
            .iter()
            .find(|line| line.contains("page=\"app\""))
            .unwrap();
        assert!(metric(app_line, "mutes_ms") >= 30, "{app_line}");
        assert!(
            metric(app_line, "render_ms") < metric(app_line, "mutes_ms"),
            "{app_line}"
        );
        let feed_line = lines
            .iter()
            .find(|line| line.contains("page=\"feed\""))
            .unwrap();
        assert!(metric(feed_line, "mutes_ms") >= 30, "{feed_line}");
        let profile_line = lines
            .iter()
            .find(|line| line.contains("page=\"profile\""))
            .unwrap();
        assert!(
            metric(profile_line, "profile_action_mutes_ms") >= 30,
            "{profile_line}"
        );
        assert!(
            metric(profile_line, "profile_list_choices_ms") >= 30,
            "{profile_line}"
        );
    }

    #[derive(Clone)]
    struct TimingConsoleTestFlow {
        client: SafeHttpClient,
        base: String,
    }

    impl OAuthFlow for TimingConsoleTestFlow {
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

        fn muted_actor_dids<'a>(
            &'a self,
            _: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<HashSet<String>, String>> + Send + 'a>> {
            let client = self.client.clone();
            let base = self.base.clone();
            Box::pin(async move {
                client
                    .get(
                        &url::Url::parse(&format!(
                            "{base}/xrpc/com.atproto.repo.getRecord?token=SECRET"
                        ))
                        .unwrap(),
                        HeaderMap::new(),
                        4096,
                    )
                    .await?;
                client
                    .get(
                        &url::Url::parse(&format!("{base}/xrpc/app.bsky.actor.getProfile"))
                            .unwrap(),
                        HeaderMap::new(),
                        4096,
                    )
                    .await?;
                Ok(HashSet::new())
            })
        }
    }

    fn read_only_session_permissions() -> SessionPermissions {
        SessionPermissions {
            write: false,
            feed: true,
            blob: false,
            video: false,
            follow: false,
            block: false,
            mute: true,
            list: true,
            preferences: true,
        }
    }

    async fn collect_body(response: axum::response::Response) -> Vec<u8> {
        http_body_util::BodyExt::collect(response.into_body())
            .await
            .unwrap()
            .to_bytes()
            .to_vec()
    }

    #[tokio::test]
    async fn the_timing_console_shows_a_page_record_with_phases_and_call_details_to_its_viewer() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let upstream = Router::new().route(
            "/{*path}",
            get(|| async {
                tokio::time::sleep(Duration::from_millis(30)).await;
                Json(json!({"ok": true}))
            }),
        );
        let server = tokio::spawn(async move {
            axum::serve(listener, upstream).await.unwrap();
        });
        let storage = Arc::new(MemoryStorage::default());
        let public = OAuthPublic::new_loopback("127.0.0.1:3000".parse().unwrap()).unwrap();
        let viewer = "did:plc:timing-console-viewer";
        let generation = storage
            .rotate_web_session(viewer, "timing-console-session")
            .unwrap();
        let token = public.issue_session_cookie(
            viewer,
            "timing-console-session",
            generation,
            read_only_session_permissions(),
        );
        let app = public.router_with_flow(
            TimingConsoleTestFlow {
                client: SafeHttpClient::new().with_http_test_hosts(["127.0.0.1".to_owned()]),
                base: format!("http://{address}"),
            },
            storage,
        );

        let logs = FeedTestLogs::default();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_max_level(tracing::Level::INFO)
            .with_writer(logs.clone())
            .finish();
        let response = app
            .clone()
            .oneshot(
                axum::http::Request::get("/app")
                    .header("cookie", format!("{SESSION_COOKIE}={token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .with_subscriber(subscriber)
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let header_id = response.headers()["x-timing-id"]
            .to_str()
            .unwrap()
            .to_owned();
        assert_eq!(header_id.len(), 16, "16 unguessable hex chars: {header_id}");
        let html = String::from_utf8(collect_body(response).await).unwrap();
        assert!(
            html.contains(&format!("<body data-timing-id=\"{header_id}\">")),
            "the page body must carry the same id as the response header"
        );

        let phase_line = String::from_utf8(logs.0.lock().unwrap().clone())
            .unwrap()
            .lines()
            .find(|line| line.contains("web page phases"))
            .expect("the page still logs its phases")
            .to_owned();
        for field in [
            "page=",
            "session_token_refresh_ms=",
            "saved_feeds_ms=",
            "preferences_ms=",
            "tabs_ms=",
            "timeline_fetch_index_ms=",
            "timeline_fetch_live_ms=",
            "feed_timeline_ms=",
            "mutes_ms=",
            "list_blocks_ms=",
            "list_blocked_posts=",
            "parent_hydration_ms=",
            "content_hydration_ms=",
            "profile_hydration_ms=",
            "quote_hydration_ms=",
            "account_labels_ms=",
            "profile_counts_ms=",
            "relationship_ms=",
            "relationship_requests=",
            "relationship_rows_written=",
            "relationship_answer=",
            "profile_action_panel_ms=",
            "profile_action_mutes_ms=",
            "profile_list_choices_ms=",
            "render_ms=",
            "ancestors_from_index=",
            "ancestors_live=",
            "ancestors_timed_out=",
            "outbound_call_count=",
            "outbound_call_ms=",
            "outbound_calls=",
            "dpop_nonce_retries=",
            "phase_sum_ms=",
            "phase_gap_ms=",
            "total_ms=",
        ] {
            assert!(
                phase_line.contains(field),
                "{field} must stay in {phase_line}"
            );
        }

        let response = app
            .oneshot(
                axum::http::Request::get("/timing/recent")
                    .header("cookie", format!("{SESSION_COOKIE}={token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(response.headers()[header::CACHE_CONTROL], "no-store");
        let body = collect_body(response).await;
        let body_text = String::from_utf8(body.clone()).unwrap();
        assert!(
            !body_text.contains("SECRET"),
            "query strings never leak: {body_text}"
        );
        let payload: Value = serde_json::from_slice(&body).unwrap();
        let records = payload["records"].as_array().expect("a records array");
        assert_eq!(records.len(), 1, "one record: {body_text}");
        let record = &records[0];
        assert_eq!(record["id"], json!(header_id));
        assert_eq!(record["viewer"], json!(viewer));
        assert_eq!(record["method"], json!("GET"));
        assert_eq!(record["path"], json!("/app"));
        assert_eq!(record["status"], json!(200));
        assert_eq!(record["page"], json!("app"));
        let mutes_ms = record["phases"]["mutes_ms"]
            .as_u64()
            .expect("mutes_ms phase");
        assert!(
            mutes_ms >= 20,
            "the two fake calls ran inside mutes_ms: {record}"
        );
        let calls = record["calls"].as_array().expect("call details");
        assert_eq!(calls.len(), 2, "{record}");
        assert_eq!(calls[0]["kind"], json!("pds_get_record"));
        assert_eq!(calls[0]["method"], json!("GET"));
        assert_eq!(calls[0]["host"], json!("127.0.0.1"));
        assert_eq!(calls[0]["path"], json!("/xrpc/com.atproto.repo.getRecord"));
        assert_eq!(calls[0]["status"], json!(200));
        assert!(calls[0]["error"].is_null());
        assert!(
            calls[0]["duration_ms"].as_u64().unwrap_or_default() >= 15,
            "{record}"
        );
        assert!(
            calls[0]["start_ms"].as_u64().unwrap_or_default() < mutes_ms,
            "{record}"
        );
        assert_eq!(calls[1]["kind"], json!("public_appview"));
        assert_eq!(calls[1]["path"], json!("/xrpc/app.bsky.actor.getProfile"));
        assert!(
            calls[1]["start_ms"].as_u64().unwrap_or_default()
                >= calls[0]["duration_ms"]
                    .as_u64()
                    .unwrap_or_default()
                    .saturating_sub(10),
            "the second call started after the first had run: {record}"
        );
        assert_eq!(record["calls_overflow"], json!(0));
        server.abort();
    }

    #[tokio::test]
    async fn the_timing_console_history_is_per_viewer_keeps_30_newest_and_counts_evictions() {
        let storage = Arc::new(MemoryStorage::default());
        let public = OAuthPublic::new_loopback("127.0.0.1:3000".parse().unwrap()).unwrap();
        let viewer_a = "did:plc:timing-console-a";
        let viewer_b = "did:plc:timing-console-b";
        let token_a = public.issue_session_cookie(
            viewer_a,
            "timing-console-a-session",
            storage
                .rotate_web_session(viewer_a, "timing-console-a-session")
                .unwrap(),
            read_only_session_permissions(),
        );
        let token_b = public.issue_session_cookie(
            viewer_b,
            "timing-console-b-session",
            storage
                .rotate_web_session(viewer_b, "timing-console-b-session")
                .unwrap(),
            read_only_session_permissions(),
        );
        let console = Arc::new(TimingConsole::with_report_interval(Duration::ZERO));
        let app =
            public.router_with_flow_and_timing_console(DisabledOAuthFlow, storage, console.clone());
        let get = |uri: &'static str, cookie: Option<String>| {
            let app = app.clone();
            async move {
                let mut builder = axum::http::Request::get(uri);
                if let Some(cookie) = cookie {
                    builder = builder.header("cookie", cookie);
                }
                app.oneshot(builder.body(Body::empty()).unwrap())
                    .await
                    .unwrap()
            }
        };

        assert_eq!(
            get("/timing/recent", None).await.status(),
            StatusCode::UNAUTHORIZED,
            "a stranger may not read any history"
        );

        let logs = FeedTestLogs::default();
        let metrics_subscriber = || {
            tracing_subscriber::fmt()
                .without_time()
                .with_ansi(false)
                .with_max_level(tracing::Level::INFO)
                .with_writer(logs.clone())
                .finish()
        };
        let mut ids_a = Vec::new();
        for _ in 0..2 {
            let response = get("/login", Some(format!("{SESSION_COOKIE}={token_a}")))
                .with_subscriber(metrics_subscriber())
                .await;
            ids_a.push(
                response.headers()["x-timing-id"]
                    .to_str()
                    .unwrap()
                    .to_owned(),
            );
        }
        let response = get("/login", Some(format!("{SESSION_COOKIE}={token_b}"))).await;
        let id_b = response.headers()["x-timing-id"]
            .to_str()
            .unwrap()
            .to_owned();
        assert!(get("/login", None).await.status().is_success());

        let response = get(
            "/timing/recent",
            Some(format!("{SESSION_COOKIE}={token_a}")),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let payload: Value = serde_json::from_slice(&collect_body(response).await).unwrap();
        let records = payload["records"].as_array().unwrap();
        assert_eq!(records.len(), 2, "viewer A sees exactly their own records");
        assert!(
            records
                .iter()
                .all(|record| record["viewer"] == json!(viewer_a))
        );
        assert_eq!(records[0]["id"], json!(ids_a[1]), "newest first");
        assert_eq!(records[1]["id"], json!(ids_a[0]));
        assert!(
            !records.iter().any(|record| record["id"] == json!(id_b)),
            "B stays hidden"
        );

        let response = get(
            "/timing/recent",
            Some(format!("{SESSION_COOKIE}={token_b}")),
        )
        .await;
        let payload: Value = serde_json::from_slice(&collect_body(response).await).unwrap();
        let records = payload["records"].as_array().unwrap();
        assert_eq!(records.len(), 1);
        assert_eq!(
            records[0]["id"],
            json!(id_b),
            "B sees only their own record"
        );

        for _ in 0..35 {
            let response = get("/login", Some(format!("{SESSION_COOKIE}={token_a}"))).await;
            ids_a.push(
                response.headers()["x-timing-id"]
                    .to_str()
                    .unwrap()
                    .to_owned(),
            );
        }
        let response = get(
            "/timing/recent",
            Some(format!("{SESSION_COOKIE}={token_a}")),
        )
        .await;
        let payload: Value = serde_json::from_slice(&collect_body(response).await).unwrap();
        let records = payload["records"].as_array().unwrap();
        assert_eq!(records.len(), 30, "at most 30 records are returned");
        assert_eq!(records[0]["id"], json!(ids_a[36]));
        assert_eq!(
            records[29]["id"],
            json!(ids_a[7]),
            "the 30 newest, oldest last"
        );

        for _ in 0..200 {
            get("/login", None).await;
        }
        let response = get(
            "/timing/recent",
            Some(format!("{SESSION_COOKIE}={token_a}")),
        )
        .await;
        let payload: Value = serde_json::from_slice(&collect_body(response).await).unwrap();
        assert!(
            payload["records"].as_array().unwrap().is_empty(),
            "A's records are evicted once 200 newer ones exist, and anonymous records are never returned"
        );
        assert_eq!(
            console.held_count(),
            200,
            "the ring keeps the newest 200 records"
        );
        assert_eq!(
            console.evicted_count(),
            39,
            "2+1+1+2+35+200 stored minus 200 held"
        );

        let metrics = String::from_utf8(logs.0.lock().unwrap().clone())
            .unwrap()
            .lines()
            .find(|line| line.contains("timing console metrics"))
            .expect("a timing console metrics line is logged")
            .to_owned();
        assert!(metrics.contains("timing_records_stored="), "{metrics}");
        assert!(metrics.contains("timing_records_evicted="), "{metrics}");
    }

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
            "did:plc:case31author",
            &json!({
                "$type":"app.bsky.embed.video#view",
                "playlist":"https://video.bsky.app/watch/did%3Aplc%3Acase31author/bafk-video/playlist.m3u8",
                "thumbnail":"https://video.bsky.app/watch/did%3Aplc%3Acase31author/bafk-video/thumbnail.jpg",
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
    fn case33_pending_quote_renders_a_deferred_fragment_placeholder() {
        let storage = MemoryStorage::default();
        let uri = "at://did:plc:quoted-author/app.bsky.feed.post/quoted";
        let embed = quote_embed(uri);
        let mut index = QuoteRenderIndex::default();
        index.insert(uri.to_owned(), QuoteRenderState::Pending);

        let rendered = render_web_quote(&storage, &embed, &index, 0);

        assert!(
            rendered.contains("data-quote-fragment=\"/quote-fragment?uri="),
            "{rendered}"
        );
        assert!(rendered.contains("Loading quoted post"), "{rendered}");
        assert!(!rendered.contains(">Quoted post</a>"), "{rendered}");
    }

    #[test]
    fn case33_nested_quote_link_keeps_its_label_and_adds_the_known_author_handle() {
        let storage = MemoryStorage::default();
        let did = "did:plc:nested-author";
        storage
            .set_verified_handle(did, "nested.author.test")
            .unwrap();
        let embed = quote_embed(&format!("at://{did}/app.bsky.feed.post/nested"));

        let rendered = render_web_quote(&storage, &embed, &QuoteRenderIndex::new(), 1);

        assert!(rendered.contains("Quoted post"), "{rendered}");
        assert!(rendered.contains("@nested.author.test"), "{rendered}");
    }

    #[tokio::test]
    async fn case33_timeline_group_ancestor_quote_uses_the_page_quote_hydration_batch() {
        let storage = Arc::new(MemoryStorage::default());
        let viewer = "did:plc:case33-viewer";
        let author = "did:plc:case33-thread-author";
        let root = test_web_post(author, "root", 1, "Thread group ancestor", None);
        let quote = test_web_post(
            "did:plc:case33-quoted-author",
            "quoted",
            1,
            "Hydrated ancestor quote body",
            None,
        );
        let mut root_with_quote = root.clone();
        root_with_quote.record["embed"] = quote_embed(&quote.uri);
        let first = test_web_post(author, "reply-1", 2, "Thread reply one", Some(&root.uri));
        let newest = test_web_post(author, "reply-2", 3, "Thread reply two", Some(&first.uri));
        for record in [&first, &newest] {
            storage.put_record(record).unwrap();
        }
        storage.set_follow(viewer, author, true).unwrap();
        let flow = QuoteHydrationTestFlow::new([root_with_quote, quote.clone()]);

        let page =
            request_test_timeline_at_with_flow(storage, viewer, "/app?limit=2", flow.clone()).await;

        assert!(page.contains("class=\"post-item thread-group\""), "{page}");
        assert!(page.contains("Thread group ancestor"), "{page}");
        assert!(page.contains("Hydrated ancestor quote body"), "{page}");
        let mut fetched = flow.fetched.lock().unwrap().clone();
        fetched.sort();
        let mut expected = vec![root.uri.clone(), quote.uri.clone()];
        expected.sort();
        assert_eq!(
            fetched, expected,
            "one parent fetch and one batched quote fetch"
        );
        let mut batches = flow.batches.lock().unwrap().clone();
        for batch in &mut batches {
            batch.sort();
        }
        assert_eq!(batches, [[root.uri], [quote.uri]]);
    }

    #[tokio::test]
    async fn page_parent_and_quote_uris_share_the_first_batch_round() {
        let storage = MemoryStorage::default();
        let parent = test_web_post("did:plc:parent-author", "parent", 1, "Parent", None);
        let quoted = test_web_post("did:plc:quote-author", "quoted", 1, "Quote", None);
        let mut reply = test_web_post(
            "did:plc:reply-author",
            "reply",
            2,
            "Reply",
            Some(&parent.uri),
        );
        reply.record["embed"] = quote_embed(&quoted.uri);
        let flow = QuoteHydrationTestFlow::new([parent.clone(), quoted.clone()]);
        let page_records = [&reply];

        let hydrated = hydrate_page_parents(
            &flow,
            &storage,
            &page_records,
            tokio::time::Instant::now() + Duration::from_secs(1),
        )
        .await
        .unwrap();

        assert!(hydrated.get(&parent.uri).is_some_and(Option::is_some));
        assert!(hydrated.get(&quoted.uri).is_some_and(Option::is_some));
        let mut batches = flow.batches.lock().unwrap().clone();
        for batch in &mut batches {
            batch.sort();
        }
        let mut expected = vec![parent.uri, quoted.uri];
        expected.sort();
        assert_eq!(batches, [expected]);
    }

    #[tokio::test]
    async fn case33_quote_fragment_endpoint_returns_the_fetched_quoted_post_card() {
        let storage = Arc::new(MemoryStorage::default());
        let viewer = "did:plc:case33-fragment-viewer";
        let quoted = test_web_post(
            "did:plc:case33-fragment-author",
            "fragment",
            1,
            "Fetched quote fragment body",
            None,
        );
        let flow = QuoteHydrationTestFlow::new([quoted.clone()]);
        let public = OAuthPublic::new_loopback("127.0.0.1:3000".parse().unwrap()).unwrap();
        let session_id = "case33-quote-fragment-session";
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
                follow: false,
                block: false,
                mute: false,
                list: false,
                preferences: false,
            },
        );
        let app = public.router_with_flow(flow, storage);
        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .uri(format!("/quote-fragment?uri={}", form_encoded(&quoted.uri)))
                    .header("cookie", format!("{SESSION_COOKIE}={token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let fragment = String::from_utf8(body.to_vec()).unwrap();
        assert!(
            fragment.contains("class=\"quoted-post-card\""),
            "{fragment}"
        );
        assert!(
            fragment.contains("Fetched quote fragment body"),
            "{fragment}"
        );
    }

    #[tokio::test]
    async fn unresolved_parent_author_is_marked_for_the_background_profile_fill() {
        let storage = Arc::new(MemoryStorage::default());
        let viewer = "did:plc:profile-fill-viewer";
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

        let page = request_test_timeline(storage.clone(), viewer).await;

        assert!(page.contains("class=\"reply-parent-card\""), "{page}");
        assert!(
            page.contains(&format!("data-profile-did=\"{parent_did}\"")),
            "a reply-parent author without a stored profile must be marked for the background fill: {page}"
        );
        assert!(
            !page.contains(&format!("data-profile-did=\"{reply_did}\"")),
            "a post author whose profile is stored must not be marked: {page}"
        );

        let thread = request_test_timeline_at_with_flow(
            storage,
            viewer,
            &format!("/app/thread?uri={}", form_encoded(&reply.uri)),
            DisabledOAuthFlow,
        )
        .await;
        assert!(
            thread.contains(&format!("data-profile-did=\"{parent_did}\"")),
            "the thread page must mark the same unresolved ancestor author: {thread}"
        );
    }

    #[derive(Clone)]
    struct ProfileFragmentTestFlow {
        profiles: Arc<HashMap<String, crate::feeds::PublicProfileView>>,
        actor_batches: Arc<Mutex<Vec<Vec<String>>>>,
    }

    impl ProfileFragmentTestFlow {
        fn new(profiles: impl IntoIterator<Item = crate::feeds::PublicProfileView>) -> Self {
            Self {
                profiles: Arc::new(
                    profiles
                        .into_iter()
                        .map(|profile| (profile.did.clone(), profile))
                        .collect(),
                ),
                actor_batches: Arc::new(Mutex::new(Vec::new())),
            }
        }
    }

    impl OAuthFlow for ProfileFragmentTestFlow {
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

        fn page_account_data_for_page<'a>(
            &'a self,
            _: &'a str,
            actor_dids: Vec<String>,
        ) -> Pin<Box<dyn Future<Output = Result<PageAccountData, String>> + Send + 'a>> {
            let batches = self.actor_batches.clone();
            let profiles = self.profiles.clone();
            Box::pin(async move {
                let data = PageAccountData {
                    labels: Vec::new(),
                    profiles: actor_dids
                        .iter()
                        .filter_map(|did| profiles.get(did))
                        .cloned()
                        .collect(),
                };
                batches.lock().unwrap().push(actor_dids);
                Ok(data)
            })
        }
    }

    fn public_profile_fixture(
        did: &str,
        handle: &str,
        display_name: &str,
    ) -> crate::feeds::PublicProfileView {
        crate::feeds::PublicProfileView {
            did: did.to_owned(),
            handle: Some(handle.to_owned()),
            display_name: Some(display_name.to_owned()),
            avatar: Some(format!(
                "https://cdn.bsky.app/img/avatar/plain/{did}/bafk-avatar@jpeg"
            )),
            labels: Vec::new(),
        }
    }

    async fn request_test_profile_fragment(
        storage: Arc<MemoryStorage>,
        viewer: &str,
        query: &str,
        flow: ProfileFragmentTestFlow,
    ) -> (StatusCode, Value) {
        let public = OAuthPublic::new_loopback("127.0.0.1:3000".parse().unwrap()).unwrap();
        let session_id = "profile-fragment-session";
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
                follow: false,
                block: false,
                mute: false,
                list: false,
                preferences: false,
            },
        );
        let app = public.router_with_flow(flow, storage);
        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .uri(format!("/profile-fragment?{query}"))
                    .header("cookie", format!("{SESSION_COOKIE}={token}"))
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        let status = response.status();
        let body = response.into_body().collect().await.unwrap().to_bytes();
        let parsed = serde_json::from_slice(&body).unwrap_or(Value::Null);
        (status, parsed)
    }

    #[tokio::test]
    async fn profile_fragment_fills_missing_authors_in_one_batch_and_stores_them() {
        let storage = Arc::new(MemoryStorage::default());
        let viewer = "did:plc:profile-fragment-viewer";
        let known = "did:plc:fillauthorone";
        let unknown = "did:plc:fillghostauthor";
        let flow = ProfileFragmentTestFlow::new([public_profile_fixture(
            known,
            "fill.example",
            "Fill Example Author",
        )]);

        let query = format!("did={}&did={}", form_encoded(known), form_encoded(unknown));
        let (status, body) =
            request_test_profile_fragment(storage.clone(), viewer, &query, flow.clone()).await;

        assert_eq!(status, StatusCode::OK);
        let profiles = body
            .get("profiles")
            .and_then(Value::as_object)
            .unwrap_or_else(|| panic!("the response must carry a profiles object: {body}"));
        let filled = profiles
            .get(known)
            .unwrap_or_else(|| panic!("the known author must be returned: {body}"));
        assert_eq!(filled["displayName"], json!("Fill Example Author"));
        assert_eq!(filled["handle"], json!("fill.example"));
        assert!(
            filled["avatar"]
                .as_str()
                .is_some_and(|avatar| avatar.starts_with("https://cdn.bsky.app/img/")),
            "{filled}"
        );
        assert!(
            !profiles.contains_key(unknown),
            "an unknown DID must be skipped without failing the others: {body}"
        );
        let stored = storage
            .get_record(known, "app.bsky.actor.profile", "self")
            .unwrap()
            .unwrap_or_else(|| panic!("the fetched profile must be stored for the next page load"));
        assert_eq!(stored.record["displayName"], json!("Fill Example Author"));
        assert_eq!(stored.record["publicHandle"], json!("fill.example"));
        let batches = flow.actor_batches.lock().unwrap().clone();
        assert_eq!(
            batches,
            vec![vec![known.to_owned(), unknown.to_owned()]],
            "one public batch per fragment request"
        );
    }

    #[tokio::test]
    async fn profile_fragment_caps_each_request_at_the_public_batch_size() {
        let storage = Arc::new(MemoryStorage::default());
        let viewer = "did:plc:profile-fragment-viewer";
        let dids = (0..30)
            .map(|index| format!("did:plc:fillbatch{index}"))
            .collect::<Vec<_>>();
        let flow = ProfileFragmentTestFlow::new(
            dids.iter()
                .map(|did| public_profile_fixture(did, "batch.example", "Batch Author")),
        );
        let query = dids
            .iter()
            .map(|did| format!("did={}", form_encoded(did)))
            .collect::<Vec<_>>()
            .join("&");

        let (status, body) =
            request_test_profile_fragment(storage, viewer, &query, flow.clone()).await;

        assert_eq!(status, StatusCode::OK);
        let profiles = body
            .get("profiles")
            .and_then(Value::as_object)
            .unwrap_or_else(|| panic!("the response must carry a profiles object: {body}"));
        assert_eq!(
            profiles.len(),
            25,
            "one public getProfiles batch per request"
        );
        let batches = flow.actor_batches.lock().unwrap().clone();
        assert_eq!(batches, vec![dids[..25].to_vec()]);
    }

    #[test]
    fn video_oauth_scopes_are_explicit_and_old_scopes_do_not_enable_video() {
        for scope in [
            "blob:video/*",
            "rpc:app.bsky.video.getUploadLimits?aud=*",
            "rpc:com.atproto.repo.uploadBlob?aud=*",
        ] {
            assert!(
                REQUESTED_SCOPE
                    .split_whitespace()
                    .any(|value| value == scope)
            );
        }
        let granted_scope = "atproto repo:app.bsky.feed.post?action=create repo:app.bsky.feed.like?action=create&action=delete repo:app.bsky.feed.repost?action=create&action=delete blob:image/* rpc:app.bsky.actor.getPreferences?aud=* rpc:app.bsky.feed.getFeedSkeleton?aud=* blob:video/*";
        assert!(!has_all_video_scopes(Some(granted_scope)));
        let updated_scope = format!("{granted_scope} {}", VIDEO_SCOPES.join(" "));
        assert!(has_all_video_scopes(Some(&updated_scope)));
    }

    #[test]
    fn every_requested_rpc_scope_has_a_spec_valid_audience() {
        for scope in FEED_SCOPES
            .iter()
            .chain(VIDEO_SCOPES.iter())
            .chain(FOLLOW_SCOPES.iter())
            .chain(BLOCK_SCOPES.iter())
            .chain(LIST_SCOPES.iter())
            .chain(MUTE_SCOPES.iter())
        {
            if !scope.starts_with("rpc:") {
                continue;
            }
            let (_, audience) = scope
                .split_once("?aud=")
                .unwrap_or_else(|| panic!("RPC scope has no aud parameter: {scope}"));
            assert!(
                audience == "*" || audience.contains('#') || audience.contains("%23"),
                "RPC scope audience must be * or a DID service reference: {scope}"
            );
        }
    }

    #[test]
    fn profile_action_oauth_scopes_are_requested() {
        for scope in [
            "repo:app.bsky.graph.follow?action=create&action=delete",
            "repo:app.bsky.graph.block?action=create&action=delete",
            "repo:app.bsky.graph.listitem?action=create&action=delete",
            "rpc:app.bsky.graph.muteActor?aud=*",
            "rpc:app.bsky.graph.unmuteActor?aud=*",
            "rpc:app.bsky.graph.getMutes?aud=*",
        ] {
            assert!(
                REQUESTED_SCOPE
                    .split_whitespace()
                    .any(|requested| requested == scope),
                "missing profile action OAuth scope: {scope}"
            );
        }
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
        assert!(session.3.blob, "legacy blob permission remains available");
        assert!(
            !session.3.video,
            "legacy sessions must sign in again for video"
        );
        assert!(
            !session.3.follow,
            "legacy sessions must sign in again to follow"
        );
        assert!(
            !session.3.block,
            "legacy sessions must sign in again to block"
        );
        assert!(
            !session.3.mute,
            "legacy sessions must sign in again to mute"
        );
        assert!(
            !session.3.list,
            "legacy sessions must sign in again to manage lists"
        );
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

    const FEED_VIEWER_DID: &str = "did:plc:case23viewer";
    const FEED_CREATOR_DID: &str = "did:plc:case23creator";
    const FEED_SERVICE_DID: &str = "did:plc:case23service";
    const FEED_URI: &str =
        "at://did:plc:case23creator/app.bsky.feed.generator/case23-authenticated";
    const FAILURE_FEED_URI: &str =
        "at://did:plc:case23creator/app.bsky.feed.generator/case23-auth-failure";
    const PLACEHOLDER_URI: &str = "at://did:plc:case23author/app.bsky.feed.post/placeholder";
    const REAL_POST_URIS: [&str; 3] = [
        "at://did:plc:case23author/app.bsky.feed.post/real-one",
        "at://did:plc:case23author/app.bsky.feed.post/real-two",
        "at://did:plc:case23author/app.bsky.feed.post/real-three",
    ];
    const SERVICE_JWT: &str = "case23-service-jwt";

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
        assert!(WEB_STYLESHEET_URL.contains("v=m40"));
        assert!(WEB_SCRIPT_URL.contains("v=js24"));
    }

    #[test]
    fn profile_block_lists_link_icon_has_a_size() {
        // Without one the inline SVG takes the 300x150 default and the button is huge (owner, 2026-10-30).
        let css = include_str!("../static/app.css");
        assert!(
            css.contains(".profile-block-lists-link .icon {\n  width: 1.1rem;\n  height: 1.1rem;")
        );
    }

    #[test]
    fn mobile_bottom_bar_uses_compact_safe_area_aware_geometry() {
        let css = include_str!("../static/app.css");

        assert!(css.contains(".mobile-profile-avatar {\n    width: 1.4rem;\n    height: 1.4rem;"));
        assert!(css.contains(".mobile-nav .icon {\n    width: 1.2rem;\n    height: 1.2rem;"));
        assert!(css.contains(
            "height: 44px;\n    min-height: 2rem;\n    min-width: 44px;\n    padding: calc((44px - 1.2rem) / 2) 0.2rem;\n    margin: calc(0.6rem - 19px) -0.2rem;"
        ));
        assert!(css.contains(
            "padding-top: 0.25rem;\n    padding-bottom: calc(0.1rem + max(4px, calc(env(safe-area-inset-bottom) * 0.25)));"
        ));
        assert!(css.contains(
            "--mobile-nav-height: calc(1.2rem + 6px + 0.25rem + 0.1rem + 1px + max(4px, calc(env(safe-area-inset-bottom) * 0.25)));"
        ));
        assert!(css.contains("bottom: var(--mobile-compose-clearance);"));
        assert!(css.contains("padding-bottom: max(4.25rem, var(--mobile-compose-clearance));"));
        assert!(WEB_STYLESHEET_URL.contains("v=m40"));
    }

    #[test]
    fn ios_safari_browser_mode_pads_the_bar_around_safaris_own_toolbar_strip() {
        let css = include_str!("../static/app.css");

        assert!(
            css.contains("@supports (-webkit-touch-callout: none) {\n  @media (display-mode: browser) {\n    @media (max-width: 799px) {\n      body:has(.mobile-nav) {\n        --mobile-nav-height: calc(1px + 14px + 1.2rem);\n      }\n\n      body:has(.mobile-nav) .mobile-nav {\n        padding-top: 14px;\n        padding-bottom: 0;\n      }\n\n      body:has(.mobile-nav) .mobile-nav a,\n      body:has(.mobile-nav) .mobile-nav button {\n        margin: calc(0.6rem - 22px) -0.2rem;\n      }\n    }\n  }\n}"),
            "iOS Safari in the browser must drop the 3 px above and below the icon, keep padding-top 14 px and padding-bottom 0, and set --mobile-nav-height to the squeezed 34.2 px bar"
        );
        assert!(
            css.contains("min-height: 0;\n    padding-top: 0.25rem;\n    padding-bottom: calc(0.1rem + max(4px, calc(env(safe-area-inset-bottom) * 0.25)));"),
            "the default body:has(.mobile-nav) .mobile-nav rule must stay unchanged for every other mode"
        );
        assert!(
            css.contains("--mobile-nav-height: calc(1.2rem + 6px + 0.25rem + 0.1rem + 1px + max(4px, calc(env(safe-area-inset-bottom) * 0.25)));"),
            "the default --mobile-nav-height must stay unchanged for every other mode"
        );
        let default_rule = css
            .find("body:has(.mobile-nav) .mobile-nav {\n    min-height: 0;")
            .expect("the default bar rule must exist");
        let ios_rule = css
            .find("body:has(.mobile-nav) .mobile-nav {\n        padding-top: 14px;")
            .expect("the iOS-Safari-browser bar rule must exist");
        assert!(
            ios_rule > default_rule,
            "the iOS-Safari-browser bar rule must come after the default rule so it wins the cascade"
        );
    }

    #[test]
    fn ios_safari_browser_mode_squeezes_the_bar_to_34_2_px_and_keeps_the_icons_centred() {
        let css = include_str!("../static/app.css");

        assert!(
            css.contains(
                "body:has(.mobile-nav) .mobile-nav a,\n      body:has(.mobile-nav) .mobile-nav button {\n        margin: calc(0.6rem - 22px) -0.2rem;\n      }"
            ),
            "inside the iOS-Safari block each bar item must occupy exactly 1.2rem of bar height, its 44 px hit box and equal padding intact so the icon stays centred"
        );
        assert!(
            css.contains("--mobile-nav-height: calc(1px + 14px + 1.2rem);"),
            "--mobile-nav-height must equal the squeezed real bar height (1 px border + 14 px + 1.2rem = 34.2 px)"
        );
        assert!(
            css.contains(
                "padding: calc((44px - 1.2rem) / 2) 0.2rem;\n    margin: calc(0.6rem - 19px) -0.2rem;"
            ),
            "the all-phones item rule must keep the 1.2rem + 6 px trim so Firefox, Android, desktop and standalone mode do not change"
        );
        assert!(
            css.contains(
                "--mobile-nav-height: calc(1.2rem + 6px + 0.25rem + 0.1rem + 1px + max(4px, calc(env(safe-area-inset-bottom) * 0.25)));"
            ),
            "the all-phones --mobile-nav-height line must stay unchanged"
        );
    }

    fn css_rule_bodies(css: &str, selector: &str) -> Vec<String> {
        let mut bodies = Vec::new();
        let mut rest = css;
        while let Some(at) = rest.find(selector) {
            let after = &rest[at + selector.len()..];
            if let Some(open) = after.find('{')
                && after[..open].trim().is_empty()
            {
                let inside = &after[open + 1..];
                let close = inside.find('}').unwrap_or(inside.len());
                bodies.push(inside[..close].to_owned());
            }
            rest = after;
        }
        bodies
    }

    fn css_declaration(block: &str, property: &str) -> Option<String> {
        let at = block.find(&format!("{property}:"))?;
        let value = &block[at + property.len() + 1..];
        let end = value.find(';').unwrap_or(value.len());
        Some(value[..end].trim().to_owned())
    }

    #[test]
    fn composer_avatar_keeps_one_size_before_and_after_app_js_collapses_it() {
        let css = include_str!("../static/app.css");
        let profile = json!({
            "avatar": "https://cdn.bsky.app/img/avatar/plain/did:plc:av48qviewer/bafk-avatar@jpeg"
        });
        let markup = composer_avatar_markup(&profile);

        assert!(
            markup.contains("class=\"composer-avatar\"")
                && markup.contains("width=\"44\" height=\"44\""),
            "the server-rendered composer avatar must carry its 44 px size: {markup}"
        );
        assert!(
            css.contains(".composer-avatar {\n  display: block;\n  width: 44px;\n  height: 44px;"),
            "the .composer-avatar rule must keep the server-rendered 44 px size"
        );
        assert!(
            css.contains(
                ".composer-input-row {\n  display: grid;\n  grid-template-columns: 44px minmax(0, 1fr);"
            ),
            "the default input row must already use the size the collapsed state keeps"
        );
        for block in css_rule_bodies(css, ".composer.is-enhanced.is-collapsed .composer-avatar") {
            assert!(
                !block.contains("40px"),
                "adding is-enhanced is-collapsed must not resize the composer avatar: {block}"
            );
        }
        for block in css_rule_bodies(
            css,
            ".composer.is-enhanced.is-collapsed .composer-input-row",
        ) {
            if let Some(columns) = css_declaration(&block, "grid-template-columns") {
                assert!(
                    columns.starts_with("44px"),
                    "adding is-enhanced is-collapsed must not narrow the avatar column: {columns}"
                );
            }
        }
    }

    #[test]
    fn empty_compact_composers_paint_collapsed_before_app_js_runs() {
        let css = include_str!("../static/app.css");
        let nojs = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/static/nojs.css"))
            .expect("static/nojs.css must exist for visitors without JavaScript");
        let gate = ".composer[data-compact-composer]:not(.is-expanded):not(:has(.reply-context, .quoted-post-preview)):not(:has([data-compose-input]:not(:placeholder-shown)))";

        let default_textarea =
            css_rule_bodies(css, &format!("{gate} .composer-input-row textarea"))
                .into_iter()
                .find(|block| css_declaration(block, "min-height").as_deref() == Some("44px"));
        let default_textarea = default_textarea.unwrap_or_else(|| {
            panic!("an empty composer must already render the 44 px collapsed textarea by default")
        });
        let collapsed_textarea = css_rule_bodies(
            css,
            ".composer.is-enhanced.is-collapsed .composer-input-row textarea",
        )
        .pop()
        .expect("the collapsed rule must stay for when app.js adds the classes");
        for property in ["min-height", "padding-block"] {
            assert_eq!(
                css_declaration(&default_textarea, property).as_deref(),
                css_declaration(&collapsed_textarea, property).as_deref(),
                "adding is-enhanced is-collapsed must not change the {property} of the textarea"
            );
        }
        for child in [".composer-toolbar", ".composer-fields"] {
            assert!(
                css_rule_bodies(css, &format!("{gate} {child}"))
                    .iter()
                    .any(|block| css_declaration(block, "display").as_deref() == Some("none")),
                "{child} must be hidden on first paint of an empty composer"
            );
        }

        assert!(
            css_rule_bodies(&nojs, &format!("{gate} .composer-input-row textarea"))
                .iter()
                .any(|block| {
                    css_declaration(block, "min-height").as_deref() == Some("3rem")
                        && css_declaration(block, "padding-block").as_deref() == Some("0.55rem")
                }),
            "nojs.css must hand no-JS visitors the tall textarea back"
        );
        assert!(
            css_rule_bodies(&nojs, &format!("{gate} .composer-toolbar"))
                .iter()
                .any(|block| css_declaration(block, "display").as_deref() == Some("flex"))
        );
        assert!(
            css_rule_bodies(&nojs, &format!("{gate} .composer-fields"))
                .iter()
                .any(|block| css_declaration(block, "display").as_deref() == Some("grid"))
        );

        let head = page_shell("Write a post", "", true);
        assert!(
            head.contains(
                "<noscript><link rel=\"stylesheet\" href=\"/static/nojs.css?v=m40\"></noscript>"
            ),
            "the head must restore the full form for no-JS visitors: {head}"
        );
    }

    #[test]
    fn phone_frame_orders_the_early_mobile_navigation_back_to_the_bottom() {
        let css = include_str!("../static/app.css");

        assert!(
            css_rule_bodies(css, "body:has(.mobile-nav) .mobile-nav")
                .iter()
                .any(|block| { block.contains("position: static") && block.contains("order: 1") }),
            "the flex-column phone body must order the early bottom bar after the layout"
        );
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
        let html = render_web_notification_group(
            &storage,
            group,
            &QuoteRenderIndex::default(),
            &HashMap::new(),
        );
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
    async fn timeline_group_includes_the_fetched_root_before_page_replies() {
        let storage = Arc::new(MemoryStorage::default());
        let viewer = "did:plc:reply-viewer";
        let root = test_web_post("did:plc:root-author", "root", 1, "Fetched chain root", None);
        let first_reply = test_web_post(
            "did:plc:first-reply-author",
            "first-reply",
            2,
            "Page reply first",
            Some(&root.uri),
        );
        let newest_reply = test_web_post(
            "did:plc:newest-reply-author",
            "newest-reply",
            3,
            "Page reply newest",
            Some(&first_reply.uri),
        );
        for record in [&root, &first_reply, &newest_reply] {
            storage.put_record(record).unwrap();
            storage.set_follow(viewer, &record.did, true).unwrap();
        }

        let page = request_test_timeline_at(storage, viewer, "/app?limit=2").await;

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
        let root_position = page.find("Fetched chain root").unwrap();
        let first_position = page.find("Page reply first").unwrap();
        let newest_position = page.find("Page reply newest").unwrap();
        assert!(root_position < first_position && first_position < newest_position);
        assert!(page.contains(
            "data-post-kind=\"post\" data-post-uri=\"at://did:plc:root-author/app.bsky.feed.post/root\""
        ));
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
        assert_eq!(page.matches("class=\"post-item thread-group\"").count(), 1);
        assert!(
            page.contains(
                "class=\"reply-connector reply-connector-previous thread-group-connector\""
            )
        );
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

        assert_eq!(page.matches("class=\"post-item thread-group\"").count(), 25);
        assert!(!page.contains("class=\"reply-parent-card\""));
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

    fn quote_embed(uri: &str) -> Value {
        json!({
            "$type":"app.bsky.embed.record",
            "record":{
                "$type":"app.bsky.embed.record#view",
                "record":{"$type":"app.bsky.feed.defs#postView","uri":uri,"cid":"quote-cid"}
            }
        })
    }

    #[derive(Clone)]
    struct QuoteHydrationTestFlow {
        records: Arc<HashMap<String, IndexedRecord>>,
        fetched: Arc<Mutex<Vec<String>>>,
        batches: Arc<Mutex<Vec<Vec<String>>>>,
        requested: Arc<Mutex<HashSet<String>>>,
    }

    impl QuoteHydrationTestFlow {
        fn new(records: impl IntoIterator<Item = IndexedRecord>) -> Self {
            Self {
                records: Arc::new(
                    records
                        .into_iter()
                        .map(|record| (record.uri.clone(), record))
                        .collect(),
                ),
                fetched: Arc::new(Mutex::new(Vec::new())),
                batches: Arc::new(Mutex::new(Vec::new())),
                requested: Arc::new(Mutex::new(HashSet::new())),
            }
        }
    }

    impl OAuthFlow for QuoteHydrationTestFlow {
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

        fn fetch_post<'a>(
            &'a self,
            uri: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<IndexedRecord, String>> + Send + 'a>> {
            let uri = uri.to_owned();
            self.fetched.lock().unwrap().push(uri.clone());
            let record = self
                .records
                .get(&uri)
                .cloned()
                .ok_or_else(|| "fixture post was not found".to_owned());
            Box::pin(async move { record })
        }

        fn fetch_public_posts<'a>(
            &'a self,
            uris: &'a [String],
        ) -> Pin<
            Box<dyn Future<Output = HashMap<String, crate::feeds::FetchedPublicPost>> + Send + 'a>,
        > {
            let uris = uris.to_vec();
            let records = self.records.clone();
            let fetched = self.fetched.clone();
            let batches = self.batches.clone();
            let requested = self.requested.clone();
            Box::pin(async move {
                let mut fresh = Vec::new();
                {
                    let mut requested = requested.lock().unwrap();
                    for uri in &uris {
                        if requested.insert(uri.clone()) {
                            fresh.push(uri.clone());
                        }
                    }
                }
                if !fresh.is_empty() {
                    batches.lock().unwrap().push(fresh.clone());
                    fetched.lock().unwrap().extend(fresh);
                }
                uris.into_iter()
                    .filter_map(|uri| {
                        records.get(&uri).cloned().map(|record| {
                            (
                                uri,
                                crate::feeds::FetchedPublicPost {
                                    record,
                                    author: None,
                                },
                            )
                        })
                    })
                    .collect()
            })
        }

        fn supports_public_post_batching(&self) -> bool {
            true
        }
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

    async fn request_test_timeline_at(
        storage: Arc<MemoryStorage>,
        viewer: &str,
        uri: &str,
    ) -> String {
        request_test_timeline_at_with_flow(storage, viewer, uri, DisabledOAuthFlow).await
    }

    async fn request_test_timeline_with_flow<F: OAuthFlow>(
        storage: Arc<MemoryStorage>,
        viewer: &str,
        flow: F,
    ) -> String {
        request_test_timeline_at_with_flow(storage, viewer, "/app", flow).await
    }

    async fn request_test_timeline_at_with_flow<F: OAuthFlow>(
        storage: Arc<MemoryStorage>,
        viewer: &str,
        uri: &str,
        flow: F,
    ) -> String {
        let public = OAuthPublic::new_loopback("127.0.0.1:3000".parse().unwrap()).unwrap();
        let session_id = "case28-timeline-session";
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
                follow: false,
                block: false,
                mute: false,
                list: false,
                preferences: false,
            },
        );
        let app = public.router_with_flow(flow, storage);
        let response = app
            .oneshot(
                axum::http::Request::builder()
                    .uri(uri)
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
        auth_requests: Vec<(String, String)>,
        skeleton_authorizations: Vec<Option<String>>,
        video_requests: Vec<(String, String)>,
        pds_url: String,
        fail_auth: bool,
        mutes_calls: usize,
        mutes_delay: Duration,
        preferences_calls: usize,
        preferences_delay: Duration,
        preferences_response: Option<Value>,
        profiles_calls: usize,
        profiles_delay: Duration,
        profiles_response: Option<Value>,
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

    #[test]
    fn writer_metrics_report_carries_the_refresh_failure_count() {
        let logs = FeedTestLogs::default();
        let metrics = OAuthWriterMetrics::default();
        metrics.record_refresh_failure();
        metrics.record_refresh_failure();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_max_level(tracing::Level::INFO)
            .with_writer(logs.clone())
            .finish();
        let _guard = tracing::subscriber::set_default(subscriber);
        metrics.report();
        drop(_guard);
        let text = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
        assert!(
            text.contains("oauth_refresh_failure_count=2"),
            "the minute metrics must report rejected refreshes: {text}"
        );
        assert!(
            text.contains("OAuth SQLite writer minute metrics"),
            "{text}"
        );
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
            pds_url: pds.clone(),
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
        let video_limits_state = state.clone();
        let video_upload_state = state.clone();
        let video_status_state = state.clone();
        let mutes_state = state.clone();
        let preferences_state = state.clone();
        let profiles_state = state.clone();
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
                get(move |uri: Uri| {
                    let state = auth_state.clone();
                    async move {
                        let mut state = state.lock().unwrap();
                        state.auth_calls += 1;
                        let parameters = url::form_urlencoded::parse(
                            uri.query().unwrap_or_default().as_bytes(),
                        )
                        .into_owned()
                        .collect::<HashMap<_, _>>();
                        state.auth_requests.push((
                            parameters.get("aud").cloned().unwrap_or_default(),
                            parameters.get("lxm").cloned().unwrap_or_default(),
                        ));
                        if state.fail_auth {
                            (
                                StatusCode::INTERNAL_SERVER_ERROR,
                                Json(json!({"error":"Unavailable"})),
                            )
                        } else {
                            let token = match parameters.get("lxm").map(String::as_str) {
                                Some("app.bsky.video.getUploadLimits") => "limits-service-jwt",
                                Some("com.atproto.repo.uploadBlob") => "upload-blob-service-jwt",
                                Some("app.bsky.video.getJobStatus") => "job-status-service-jwt",
                                _ => SERVICE_JWT,
                            };
                            (StatusCode::OK, Json(json!({"token":token})))
                        }
                    }
                }),
            )
            .route(
                "/xrpc/app.bsky.video.getUploadLimits",
                get(move |headers: HeaderMap| {
                    let state = video_limits_state.clone();
                    async move {
                        state.lock().unwrap().video_requests.push((
                            "/xrpc/app.bsky.video.getUploadLimits".to_owned(),
                            headers
                                .get(header::AUTHORIZATION)
                                .and_then(|value| value.to_str().ok())
                                .unwrap_or_default()
                                .to_owned(),
                        ));
                        Json(json!({"canUpload":true,"remainingDailyVideos":5,"remainingDailyBytes":1000}))
                    }
                }),
            )
            .route(
                "/xrpc/app.bsky.video.uploadVideo",
                post(move |headers: HeaderMap, _: Bytes| {
                    let state = video_upload_state.clone();
                    async move {
                        state.lock().unwrap().video_requests.push((
                            "/xrpc/app.bsky.video.uploadVideo".to_owned(),
                            headers
                                .get(header::AUTHORIZATION)
                                .and_then(|value| value.to_str().ok())
                                .unwrap_or_default()
                                .to_owned(),
                        ));
                        Json(json!({"jobStatus":{"jobId":"job-1","state":"JOB_STATE_ENCODING"}}))
                    }
                }),
            )
            .route(
                "/xrpc/app.bsky.video.getJobStatus",
                get(move |headers: HeaderMap| {
                    let state = video_status_state.clone();
                    async move {
                        state.lock().unwrap().video_requests.push((
                            "/xrpc/app.bsky.video.getJobStatus".to_owned(),
                            headers
                                .get(header::AUTHORIZATION)
                                .and_then(|value| value.to_str().ok())
                                .unwrap_or_default()
                                .to_owned(),
                        ));
                        Json(json!({
                            "jobStatus":{
                                "jobId":"job-1",
                                "state":"JOB_STATE_COMPLETED",
                                "blob":{"ref":{"$link":"bafk-video"},"mimeType":"video/mp4","size":99}
                            }
                        }))
                    }
                }),
            )
            .route(
                "/xrpc/app.bsky.graph.getMutes",
                get(move || {
                    let state = mutes_state.clone();
                    async move {
                        let delay = {
                            let mut state = state.lock().unwrap();
                            state.mutes_calls += 1;
                            state.mutes_delay
                        };
                        tokio::time::sleep(delay).await;
                        Json(json!({"mutes":[]}))
                    }
                }),
            )
            .route(
                "/xrpc/app.bsky.actor.getPreferences",
                get(move || {
                    let state = preferences_state.clone();
                    async move {
                        let (delay, response) = {
                            let mut state = state.lock().unwrap();
                            state.preferences_calls += 1;
                            (state.preferences_delay, state.preferences_response.clone())
                        };
                        tokio::time::sleep(delay).await;
                        Json(response.unwrap_or_else(|| json!({"preferences":[]})))
                    }
                }),
            )
            .route(
                "/xrpc/app.bsky.actor.getProfiles",
                get(move || {
                    let state = profiles_state.clone();
                    async move {
                        let (delay, response) = {
                            let mut state = state.lock().unwrap();
                            state.profiles_calls += 1;
                            (state.profiles_delay, state.profiles_response.clone())
                        };
                        tokio::time::sleep(delay).await;
                        Json(response.unwrap_or_else(|| json!({"profiles":[]})))
                    }
                }),
            )
            .route(
                "/xrpc/app.bsky.labeler.getServices",
                get(|| async {
                    Json(json!({"views":[{
                        "creator":{"did":"did:plc:stale-cache-labeler","displayName":"Fresh labeler"},
                        "policies":{"labelValueDefinitions":[{
                            "identifier":"spider",
                            "severity":"alert",
                            "blurs":"media",
                            "locales":[{"lang":"en","name":"Fresh spider label"}]
                        }]}
                    }]}))
                }),
            )
            .route(
                "/xrpc/com.atproto.repo.getRecord",
                get(|uri: Uri| async move {
                    let query = uri.query().unwrap_or_default();
                    if query.contains("collection=app.bsky.feed.generator") {
                        Json(json!({
                            "uri": FEED_URI,
                            "cid": "bafyrecase23generator",
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
                        let feed = if authorization.as_deref() == Some("Bearer case23-service-jwt") {
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
        let public_appview_base = format!("{pds}/xrpc/");
        let flow = AtriumOAuthFlow::new_with_public_appview_base(
            &public,
            identity_resolver,
            store,
            &pds,
            SafeHttpClient::new().with_http_test_hosts(["127.0.0.1".to_owned()]),
            &public_appview_base,
        )
        .unwrap()
        // Setup only: this fake server serves every xrpc route these tests use,
        // so the labelled getProfiles must land here too; expectations unchanged.
        .with_labelled_appview_base(public_appview_base.clone());
        (flow, state, server)
    }

    #[tokio::test]
    async fn stale_mutes_return_immediately_and_refresh_once_in_background() {
        let (flow, state, server) = feed_test_flow(false).await;
        let did = FEED_VIEWER_DID.to_owned();
        let stale = HashSet::from(["did:plc:previously-muted".to_owned()]);
        state.lock().unwrap().mutes_delay = Duration::from_millis(180);
        flow.muted_actor_cache.lock().await.insert(
            did.clone(),
            (
                tokio::time::Instant::now() - Duration::from_secs(1),
                stale.clone(),
            ),
        );

        let responses = futures_util::future::join_all((0..4).map(|_| async {
            tokio::time::timeout(Duration::from_millis(60), flow.fetch_muted_actor_dids(&did))
                .await
                .expect("stale mutes should not wait for the upstream")
                .unwrap()
        }))
        .await;
        assert!(responses.iter().all(|actors| actors == &stale));

        tokio::time::sleep(Duration::from_millis(240)).await;
        assert_eq!(state.lock().unwrap().mutes_calls, 1);
        assert!(flow.fetch_muted_actor_dids(&did).await.unwrap().is_empty());
        server.abort();
    }

    #[tokio::test]
    async fn stale_persisted_mutes_return_immediately_and_refresh_once_in_background() {
        let (flow, state, server) = feed_test_flow(false).await;
        let did = FEED_VIEWER_DID.to_owned();
        let stale = vec!["did:plc:persisted-muted".to_owned()];
        let cache_key = muted_actor_cache_key(&did);
        flow.store
            .web_cache_set(&cache_key, &serde_json::to_string(&stale).unwrap())
            .unwrap();
        let stale_at_ms = unix_time_ms() - MUTED_ACTOR_CACHE_TTL.as_millis() as i64 - 1;
        flow.store
            .writer
            .call(move |connection| {
                connection.execute(
                    "UPDATE web_cache SET stored_at_ms=?1 WHERE cache_key=?2",
                    params![stale_at_ms, cache_key],
                )?;
                Ok(())
            })
            .await
            .unwrap();
        state.lock().unwrap().mutes_delay = Duration::from_millis(180);

        let actors =
            tokio::time::timeout(Duration::from_millis(60), flow.fetch_muted_actor_dids(&did))
                .await
                .expect("stale persisted mutes should not wait for the upstream")
                .unwrap();
        assert_eq!(actors, HashSet::from([stale[0].clone()]));

        tokio::time::sleep(Duration::from_millis(240)).await;
        assert_eq!(state.lock().unwrap().mutes_calls, 1);
        assert!(flow.fetch_muted_actor_dids(&did).await.unwrap().is_empty());
        server.abort();
    }

    #[tokio::test]
    async fn stale_saved_feed_preferences_return_immediately_and_refresh_once_in_background() {
        let (flow, state, server) = feed_test_flow(false).await;
        let did = FEED_VIEWER_DID.to_owned();
        let stale = SavedFeedPreference {
            id: "old-feed".to_owned(),
            feed_type: "feed".to_owned(),
            value: "at://did:plc:old/app.bsky.feed.generator/old".to_owned(),
            pinned: true,
        };
        let refreshed = SavedFeedPreference {
            id: "new-feed".to_owned(),
            feed_type: "feed".to_owned(),
            value: "at://did:plc:new/app.bsky.feed.generator/new".to_owned(),
            pinned: false,
        };
        {
            let mut state = state.lock().unwrap();
            state.preferences_delay = Duration::from_millis(180);
            state.preferences_response = Some(json!({"preferences":[{
                "$type":"app.bsky.actor.defs#savedFeedsPrefV2",
                "items":[{
                    "id":refreshed.id,
                    "type":refreshed.feed_type,
                    "value":refreshed.value,
                    "pinned":refreshed.pinned
                }]
            }]}));
        }
        flow.saved_feed_cache.lock().await.insert(
            did.clone(),
            (
                tokio::time::Instant::now() - Duration::from_secs(1),
                vec![stale.clone()],
            ),
        );

        let responses = futures_util::future::join_all((0..4).map(|_| async {
            tokio::time::timeout(Duration::from_millis(60), flow.saved_feed_preferences(&did))
                .await
                .expect("stale saved feeds should not wait for the upstream")
                .unwrap()
        }))
        .await;
        assert!(
            responses
                .iter()
                .all(|preferences| preferences == std::slice::from_ref(&stale))
        );

        tokio::time::sleep(Duration::from_millis(240)).await;
        assert_eq!(state.lock().unwrap().preferences_calls, 1);
        assert_eq!(
            flow.saved_feed_preferences(&did).await.unwrap(),
            [refreshed]
        );
        server.abort();
    }

    #[tokio::test]
    async fn stale_persisted_saved_feed_preferences_return_immediately_and_refresh_once_in_background()
     {
        let (flow, state, server) = feed_test_flow(false).await;
        let did = FEED_VIEWER_DID.to_owned();
        let stale = SavedFeedPreference {
            id: "persisted-old-feed".to_owned(),
            feed_type: "feed".to_owned(),
            value: "at://did:plc:old/app.bsky.feed.generator/old".to_owned(),
            pinned: true,
        };
        let refreshed = SavedFeedPreference {
            id: "persisted-new-feed".to_owned(),
            feed_type: "feed".to_owned(),
            value: "at://did:plc:new/app.bsky.feed.generator/new".to_owned(),
            pinned: false,
        };
        {
            let mut state = state.lock().unwrap();
            state.preferences_delay = Duration::from_millis(180);
            state.preferences_response = Some(json!({"preferences":[{
                "$type":"app.bsky.actor.defs#savedFeedsPrefV2",
                "items":[{
                    "id":refreshed.id,
                    "type":refreshed.feed_type,
                    "value":refreshed.value,
                    "pinned":refreshed.pinned
                }]
            }]}));
        }
        let cache_key = saved_feed_cache_key(&did);
        flow.store
            .web_cache_set(
                &cache_key,
                &serde_json::to_string(std::slice::from_ref(&stale)).unwrap(),
            )
            .unwrap();
        let stale_at_ms = unix_time_ms() - SAVED_FEED_PREFERENCE_CACHE_TTL.as_millis() as i64 - 1;
        flow.store
            .writer
            .call(move |connection| {
                connection.execute(
                    "UPDATE web_cache SET stored_at_ms=?1 WHERE cache_key=?2",
                    params![stale_at_ms, cache_key],
                )?;
                Ok(())
            })
            .await
            .unwrap();

        let preferences =
            tokio::time::timeout(Duration::from_millis(60), flow.saved_feed_preferences(&did))
                .await
                .expect("stale persisted saved feeds should not wait for the upstream")
                .unwrap();
        assert_eq!(preferences, [stale]);

        tokio::time::sleep(Duration::from_millis(240)).await;
        assert_eq!(state.lock().unwrap().preferences_calls, 1);
        assert_eq!(
            flow.saved_feed_preferences(&did).await.unwrap(),
            [refreshed]
        );
        server.abort();
    }

    #[tokio::test]
    async fn stale_account_labels_return_immediately_and_refresh_once_in_background() {
        let (flow, state, server) = feed_test_flow(false).await;
        let viewer_did = FEED_VIEWER_DID.to_owned();
        let actor_did = "did:plc:stale-cache-actor";
        let labeler_did = "did:plc:stale-cache-labeler";
        let stale_label = AccountLabel {
            subject_did: actor_did.to_owned(),
            labeler_did: labeler_did.to_owned(),
            labeler_name: "Old labeler".to_owned(),
            labeler_handle: "old.labeler.test".to_owned(),
            value: "old".to_owned(),
            display_name: "Old account label".to_owned(),
            description: "Old description kept from the stale cache.".to_owned(),
            severity: "none".to_owned(),
            blurs: "none".to_owned(),
        };
        let fresh_label = AccountLabel {
            subject_did: actor_did.to_owned(),
            labeler_did: labeler_did.to_owned(),
            labeler_name: "Fresh labeler".to_owned(),
            labeler_handle: labeler_did.to_owned(),
            value: "spider".to_owned(),
            display_name: "Fresh spider label".to_owned(),
            description: String::new(),
            severity: "alert".to_owned(),
            blurs: "media".to_owned(),
        };
        {
            let mut state = state.lock().unwrap();
            state.profiles_delay = Duration::from_millis(180);
            state.profiles_response = Some(json!({"profiles":[{
                "did":actor_did,
                "handle":"fresh.actor.test",
                "displayName":"Fresh actor",
                "labels":[{"src":labeler_did,"uri":actor_did,"val":"spider"}]
            }]}));
        }
        flow.labeler_preference_cache.lock().await.insert(
            viewer_did.clone(),
            (
                tokio::time::Instant::now() + Duration::from_secs(60),
                vec![labeler_did.to_owned()],
            ),
        );
        flow.account_label_actor_cache.lock().await.insert(
            (viewer_did.clone(), actor_did.to_owned()),
            (
                tokio::time::Instant::now() - Duration::from_secs(1),
                AccountActorData {
                    labels: vec![stale_label.clone()],
                    profile: Some(crate::feeds::PublicProfileView {
                        did: actor_did.to_owned(),
                        handle: Some("stale.actor.test".to_owned()),
                        display_name: Some("Stale actor".to_owned()),
                        avatar: None,
                        labels: Vec::new(),
                    }),
                },
            ),
        );

        let responses = futures_util::future::join_all((0..4).map(|_| async {
            tokio::time::timeout(
                Duration::from_millis(60),
                flow.fetch_page_account_data_for_page(&viewer_did, vec![actor_did.to_owned()]),
            )
            .await
            .expect("stale account labels should not wait for the upstream")
            .unwrap()
        }))
        .await;
        assert!(responses.iter().all(|data| {
            data.labels == [stale_label.clone()]
                && data.profiles[0].display_name.as_deref() == Some("Stale actor")
        }));

        tokio::time::sleep(Duration::from_millis(240)).await;
        assert_eq!(state.lock().unwrap().profiles_calls, 1);
        let refreshed = flow
            .fetch_page_account_data_for_page(&viewer_did, vec![actor_did.to_owned()])
            .await
            .unwrap();
        assert_eq!(refreshed.labels, [fresh_label]);
        assert_eq!(
            refreshed.profiles[0].display_name.as_deref(),
            Some("Fresh actor")
        );
        server.abort();
    }

    #[tokio::test]
    async fn account_labels_arrive_on_the_next_page_after_a_timed_out_page() {
        let (flow, state, server) = feed_test_flow(false).await;
        let viewer_did = FEED_VIEWER_DID.to_owned();
        let actor_did = "did:plc:page-deadline-actor";
        let labeler_did = "did:plc:stale-cache-labeler";
        {
            let mut state = state.lock().unwrap();
            state.profiles_delay = Duration::from_millis(180);
            state.profiles_response = Some(json!({"profiles":[{
                "did":actor_did,
                "handle":"deadline.actor.test",
                "displayName":"Deadline actor",
                "labels":[{"src":labeler_did,"uri":actor_did,"val":"spider"}]
            }]}));
        }
        flow.labeler_preference_cache.lock().await.insert(
            viewer_did.clone(),
            (
                tokio::time::Instant::now() + Duration::from_secs(60),
                vec![labeler_did.to_owned()],
            ),
        );
        let storage = Arc::new(MemoryStorage::default());
        let post = test_web_post(
            actor_did,
            "page-deadline",
            1,
            "Post by the slow author",
            None,
        );
        storage.put_record(&post).unwrap();
        let page_records = vec![&post];

        let logs = FeedTestLogs::default();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(logs.clone())
            .finish();
        let first = hydrate_page_content(
            &flow,
            &viewer_did,
            storage.clone(),
            &page_records,
            &[],
            [actor_did.to_owned()],
            tokio::time::Instant::now() + Duration::from_millis(60),
        )
        .with_subscriber(subscriber)
        .await
        .unwrap();
        assert!(
            first.quote_index.account_labels.is_empty(),
            "the page that ran out of time cannot show labels it never received"
        );
        let output = String::from_utf8(logs.0.lock().unwrap().clone()).unwrap();
        let timeout_lines = output
            .lines()
            .filter(|line| line.contains("account labels timed out"))
            .collect::<Vec<_>>();
        assert_eq!(
            timeout_lines.len(),
            1,
            "the page-deadline timeout of account data must warn: {output}"
        );
        assert!(
            timeout_lines[0].contains("WARN")
                && timeout_lines[0].contains(&viewer_did)
                && timeout_lines[0].contains("actors=1"),
            "the warn must name the viewer DID and the actor count: {}",
            timeout_lines[0]
        );

        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(state.lock().unwrap().profiles_calls, 1);
        let second = hydrate_page_content(
            &flow,
            &viewer_did,
            storage.clone(),
            &page_records,
            &[],
            [actor_did.to_owned()],
            tokio::time::Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
        assert_eq!(
            second.quote_index.account_labels.get(actor_did),
            Some(&vec![AccountLabel {
                subject_did: actor_did.to_owned(),
                labeler_did: labeler_did.to_owned(),
                labeler_name: "Fresh labeler".to_owned(),
                labeler_handle: labeler_did.to_owned(),
                value: "spider".to_owned(),
                display_name: "Fresh spider label".to_owned(),
                description: String::new(),
                severity: "alert".to_owned(),
                blurs: "media".to_owned(),
            }]),
            "a dropped page must not strand the refresh: the next page shows the labels"
        );
        server.abort();
    }

    #[tokio::test]
    async fn labeler_dids_arrive_after_a_caller_times_out_mid_refresh() {
        let (flow, state, server) = feed_test_flow(false).await;
        let viewer_did = FEED_VIEWER_DID.to_owned();
        {
            let mut state = state.lock().unwrap();
            state.preferences_delay = Duration::from_millis(180);
            state.preferences_response = Some(json!({"preferences":[{
                "$type":"app.bsky.actor.defs#labelersPref",
                "labelers":[{"did":"did:plc:patience-labeler"}]
            }]}));
        }

        let first = tokio::time::timeout(
            Duration::from_millis(60),
            flow.labeler_dids_for_viewer(&viewer_did),
        )
        .await;
        assert!(
            first.is_err(),
            "the first caller runs out of time before the slow fake PDS answers"
        );

        tokio::time::sleep(Duration::from_millis(400)).await;
        assert_eq!(state.lock().unwrap().preferences_calls, 1);
        let dids = flow.labeler_dids_for_viewer(&viewer_did).await.unwrap();
        assert_eq!(
            dids,
            vec![
                BSKY_MODERATION_LABELER_DID.to_owned(),
                "did:plc:patience-labeler".to_owned()
            ],
            "a dropped caller must not strand the refresh: later callers get the labeler DIDs"
        );
        server.abort();
    }

    #[tokio::test]
    async fn account_labels_ms_reports_the_awaited_cold_refresh() {
        let (flow, state, server) = feed_test_flow(false).await;
        let viewer_did = FEED_VIEWER_DID.to_owned();
        let actor_did = "did:plc:measured-actor";
        let labeler_did = "did:plc:stale-cache-labeler";
        {
            let mut state = state.lock().unwrap();
            state.profiles_delay = Duration::from_millis(80);
            state.profiles_response = Some(json!({"profiles":[{
                "did":actor_did,
                "handle":"measured.actor.test",
                "displayName":"Measured actor",
                "labels":[{"src":labeler_did,"uri":actor_did,"val":"spider"}]
            }]}));
        }
        flow.labeler_preference_cache.lock().await.insert(
            viewer_did.clone(),
            (
                tokio::time::Instant::now() + Duration::from_secs(60),
                vec![labeler_did.to_owned()],
            ),
        );
        let storage = Arc::new(MemoryStorage::default());
        let post = test_web_post(
            actor_did,
            "measured",
            1,
            "Post by the measured author",
            None,
        );
        storage.put_record(&post).unwrap();
        let page_records = vec![&post];

        let hydration = hydrate_page_content(
            &flow,
            &viewer_did,
            storage.clone(),
            &page_records,
            &[],
            [actor_did.to_owned()],
            tokio::time::Instant::now() + Duration::from_secs(2),
        )
        .await
        .unwrap();
        assert!(!hydration.quote_index.account_labels.is_empty());
        assert!(
            hydration.account_labels_ms >= 80,
            "account_labels_ms must carry the measured cold-refresh wait, got {} ms",
            hydration.account_labels_ms
        );
        server.abort();
    }

    const CORNELL_FIXTURE_DID: &str = "did:plc:oubsyca6hhgqhmbbk27lvs7c";
    const CORNELL_FIXTURE_NAME: &str = "Account Activity Labeler";

    type CornellFixtureDefinition = (String, String, String, String, String);

    fn cornell_fixture_definitions() -> Vec<CornellFixtureDefinition> {
        let raw = fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/tests/fixtures/cornell-labeler-getServices.json"
        ))
        .expect("Cornell labeler getServices fixture");
        let parsed: Value = serde_json::from_str(&raw).unwrap();
        parsed["views"][0]["policies"]["labelValueDefinitions"]
            .as_array()
            .cloned()
            .unwrap_or_default()
            .into_iter()
            .filter_map(|definition| {
                let identifier = definition.get("identifier")?.as_str()?.to_owned();
                let locales = definition
                    .get("locales")
                    .and_then(Value::as_array)
                    .cloned()
                    .unwrap_or_default();
                let locale = locales
                    .iter()
                    .find(|locale| locale.get("lang").and_then(Value::as_str) == Some("en"))
                    .or_else(|| locales.first());
                Some((
                    identifier,
                    locale
                        .and_then(|locale| locale.get("name"))
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                    locale
                        .and_then(|locale| locale.get("description"))
                        .and_then(Value::as_str)
                        .unwrap_or_default()
                        .to_owned(),
                    definition
                        .get("severity")
                        .and_then(Value::as_str)
                        .unwrap_or("none")
                        .to_owned(),
                    definition
                        .get("blurs")
                        .and_then(Value::as_str)
                        .unwrap_or("none")
                        .to_owned(),
                ))
            })
            .collect()
    }

    fn fixture_account_label(
        definitions: &[CornellFixtureDefinition],
        subject_did: &str,
        labeler_did: &str,
        labeler_name: &str,
        labeler_handle: &str,
        identifier: &str,
    ) -> AccountLabel {
        let definition = definitions
            .iter()
            .find(|(value, ..)| value == identifier)
            .unwrap_or_else(|| panic!("missing fixture definition for {identifier}"))
            .clone();
        AccountLabel {
            subject_did: subject_did.to_owned(),
            labeler_did: labeler_did.to_owned(),
            labeler_name: labeler_name.to_owned(),
            labeler_handle: labeler_handle.to_owned(),
            value: identifier.to_owned(),
            display_name: definition.1,
            description: definition.2,
            severity: definition.3,
            blurs: definition.4,
        }
    }

    fn cornell_labels(
        definitions: &[CornellFixtureDefinition],
        subject_did: &str,
        identifiers: &[&str],
    ) -> Vec<AccountLabel> {
        identifiers
            .iter()
            .map(|identifier| {
                fixture_account_label(
                    definitions,
                    subject_did,
                    CORNELL_FIXTURE_DID,
                    CORNELL_FIXTURE_NAME,
                    "stechlab-labels.bsky.social",
                    identifier,
                )
            })
            .collect()
    }

    fn fixture_summary(rendered: &str) -> &str {
        &rendered[rendered.find("<summary").unwrap()..rendered.find("</summary>").unwrap()]
    }

    #[test]
    fn three_cornell_labels_render_one_control_with_the_ordered_icon_summary() {
        let definitions = cornell_fixture_definitions();
        let subject = "did:plc:labelled-author";
        let mut labels_by_actor = HashMap::new();
        labels_by_actor.insert(
            subject.to_owned(),
            cornell_labels(
                &definitions,
                subject,
                &[
                    "made-over-hundred-posts-yesterday",
                    "posted-same-url-high",
                    "no-gap-more-than-one-hours",
                ],
            ),
        );

        let rendered = render_account_label_chips(&labels_by_actor, subject);

        assert_eq!(
            rendered
                .matches("<details class=\"account-labels\">")
                .count(),
            1,
            "one compact control per account: {rendered}"
        );
        assert!(!rendered.contains("account-label-chip"), "{rendered}");
        let summary = fixture_summary(&rendered);
        let posts = summary
            .find("📝")
            .expect("a posting-volume label shows the memo icon");
        let url = summary
            .find("🔗")
            .expect("a same-url label shows the link icon");
        let gap = summary
            .find("⏱️")
            .expect("a no-gap label shows the stopwatch icon");
        assert!(
            posts < url && url < gap,
            "summary icons follow label order: {summary}"
        );
        assert_eq!(
            summary.matches("account-label-emoji").count(),
            3,
            "one small icon per label: {summary}"
        );
        assert_eq!(
            summary.matches("severity-inform").count(),
            3,
            "inform icons carry the neutral ring class: {summary}"
        );
        assert!(
            rendered.contains("aria-label=\"3 account labels\""),
            "the control is named by its label count: {rendered}"
        );
        for name in [
            "Made over 100 posts yesterday",
            "Posted same url 15+ times yesterday",
            "No break greater than 1 hr yesterday",
        ] {
            assert!(rendered.contains(name), "{rendered}");
        }
        assert_eq!(
            rendered
                .matches("<li class=\"account-label-row severity-inform\">")
                .count(),
            3,
            "{rendered}"
        );
        assert_eq!(
            rendered.matches("by Account Activity Labeler</a>").count(),
            3,
            "{rendered}"
        );
        assert_eq!(
            rendered
                .matches("href=\"/profile?did=did%3Aplc%3Aoubsyca6hhgqhmbbk27lvs7c\"")
                .count(),
            3,
            "every row links to the labeler's profile: {rendered}"
        );
        let unlabelled = render_account_label_chips(&HashMap::new(), "did:plc:plain");
        let empty_list = render_account_label_chips(
            &HashMap::from([("did:plc:plain".to_owned(), Vec::new())]),
            "did:plc:plain",
        );
        assert!(
            unlabelled.is_empty() && empty_list.is_empty(),
            "accounts without labels render nothing"
        );
    }

    #[test]
    fn six_labels_show_four_icons_and_the_overflow_count_in_order() {
        let definitions = cornell_fixture_definitions();
        let subject = "did:plc:busy-author";
        let mut labels_by_actor = HashMap::new();
        labels_by_actor.insert(
            subject.to_owned(),
            cornell_labels(
                &definitions,
                subject,
                &[
                    "made-over-hundred-posts-yesterday",
                    "made-over-hundred-replies-yesterday",
                    "no-gap-more-than-two-hours",
                    "posted-same-url-mid",
                    "metadata-monthly-changes-low",
                    "high-follow-churn-one-hundred",
                ],
            ),
        );

        let rendered = render_account_label_chips(&labels_by_actor, subject);
        let summary = fixture_summary(&rendered);

        assert_eq!(
            summary.matches("account-label-emoji").count(),
            4,
            "{summary}"
        );
        for emoji in ["📝", "💬", "⏱️", "🔗"] {
            assert!(summary.contains(emoji), "{summary}");
        }
        assert!(
            !summary.contains("🪪") && !summary.contains("👥"),
            "icons past the fourth collapse into the overflow count: {summary}"
        );
        assert!(
            summary.contains("<span class=\"account-label-more\">+2</span>"),
            "{summary}"
        );
        assert!(
            rendered.contains("aria-label=\"6 account labels\""),
            "{rendered}"
        );
        assert_eq!(
            rendered.matches("<li class=\"account-label-row").count(),
            6,
            "all six labels stay in the expanded list: {rendered}"
        );
        assert_eq!(
            rendered.matches("🪪").count(),
            1,
            "the profile-changes label keeps its icon in the list: {rendered}"
        );
        assert_eq!(
            rendered.matches("👥").count(),
            1,
            "the follow-churn label keeps its icon in the list: {rendered}"
        );
        assert!(
            rendered.contains("Profile changes often: more than 3 days in last 30 days"),
            "{rendered}"
        );
        assert!(rendered.contains("Unfollowed 100+ yesterday"), "{rendered}");
    }

    #[test]
    fn labels_from_two_labelers_expand_into_rows_grouped_by_labeler() {
        let definitions = cornell_fixture_definitions();
        let subject = "did:plc:multi-labelled";
        let mut labels = cornell_labels(
            &definitions,
            subject,
            &[
                "made-over-hundred-posts-yesterday",
                "no-gap-more-than-one-hours",
            ],
        );
        labels.push(fixture_account_label(
            &definitions,
            subject,
            "did:plc:second-labeler",
            "Steady Watcher",
            "steady.watch",
            "high-follow-churn-fifty",
        ));
        let mut labels_by_actor = HashMap::new();
        labels_by_actor.insert(subject.to_owned(), labels);

        let rendered = render_account_label_chips(&labels_by_actor, subject);

        assert_eq!(
            rendered
                .matches("<li class=\"account-label-group\">")
                .count(),
            2,
            "{rendered}"
        );
        assert!(
            rendered.contains("<p class=\"account-label-group-name\">Account Activity Labeler</p>"),
            "{rendered}"
        );
        assert!(
            rendered.contains("<p class=\"account-label-group-name\">Steady Watcher</p>"),
            "{rendered}"
        );
        assert!(
            rendered.find("Account Activity Labeler</p>") < rendered.find("Steady Watcher</p>"),
            "groups keep the order their labelers first appear: {rendered}"
        );
        let groups = rendered
            .split("<li class=\"account-label-group\">")
            .collect::<Vec<_>>();
        assert_eq!(groups.len(), 3, "{rendered}");
        assert_eq!(
            groups[1].matches("by Account Activity Labeler</a>").count(),
            2,
            "{rendered}"
        );
        assert!(!groups[1].contains("Steady Watcher"), "{rendered}");
        assert_eq!(
            groups[2].matches("by Steady Watcher</a>").count(),
            1,
            "{rendered}"
        );
        assert!(
            groups[1].find("Made over 100 posts yesterday") < groups[1].find("No break greater"),
            "rows keep label order inside a group: {rendered}"
        );
        assert!(
            rendered.contains("href=\"/profile?did=did%3Aplc%3Asecond-labeler\""),
            "{rendered}"
        );
        assert!(
            rendered.contains("aria-label=\"3 account labels\""),
            "{rendered}"
        );
    }

    #[test]
    fn alert_severity_rings_its_icon_and_unknown_values_fall_back_to_the_label_tag() {
        let subject = "did:plc:alert-author";
        let mut labels_by_actor = HashMap::new();
        labels_by_actor.insert(
            subject.to_owned(),
            vec![AccountLabel {
                subject_did: subject.to_owned(),
                labeler_did: "did:plc:careful-labeler".to_owned(),
                labeler_name: "Careful Labeler".to_owned(),
                labeler_handle: "careful.example".to_owned(),
                value: "spider".to_owned(),
                display_name: "Spider Warning".to_owned(),
                description: String::new(),
                severity: "alert".to_owned(),
                blurs: "content".to_owned(),
            }],
        );

        let rendered = render_account_label_chips(&labels_by_actor, subject);

        assert!(
            rendered.contains(
                "<span class=\"account-label-emoji severity-alert\" aria-hidden=\"true\">🏷️</span>"
            ),
            "alert labels ring their icon red and unknown values fall back to the tag icon: {rendered}"
        );
        assert!(
            rendered.contains("<li class=\"account-label-row severity-alert\">"),
            "{rendered}"
        );
        assert!(rendered.contains("Spider Warning"), "{rendered}");
        assert!(
            rendered.contains("aria-label=\"1 account label\""),
            "the accessible count stays singular: {rendered}"
        );
    }

    #[test]
    fn post_card_authors_show_the_compact_control_next_to_the_author_name() {
        let storage = MemoryStorage::default();
        let subject = "did:plc:labelled-poster";
        let post = test_web_post(subject, "labelled-post", 1, "Labelled author post", None);
        storage.put_record(&post).unwrap();
        let definitions = cornell_fixture_definitions();
        let mut index = QuoteRenderIndex::default();
        index.account_labels.insert(
            subject.to_owned(),
            cornell_labels(
                &definitions,
                subject,
                &[
                    "made-over-hundred-posts-yesterday",
                    "posted-same-url-high",
                    "no-gap-more-than-one-hours",
                ],
            ),
        );

        let rendered = render_web_post(
            &storage,
            &post,
            "did:plc:viewer",
            false,
            false,
            false,
            &index,
        );

        assert!(
            rendered.contains("<details class=\"account-labels\">"),
            "{rendered}"
        );
        assert!(!rendered.contains("account-label-chip"), "{rendered}");
        let name = rendered.find("class=\"author-name\"").unwrap();
        let control = rendered.find("<details class=\"account-labels\">").unwrap();
        let handle = rendered.find("class=\"author-handle\"").unwrap();
        assert!(
            name < control && control < handle,
            "the control sits between the author name and handle: {rendered}"
        );
    }

    #[test]
    fn quote_cards_show_the_compact_control_for_the_quoted_author() {
        let storage = MemoryStorage::default();
        let quoted_author = "did:plc:quoted-labelled-author";
        let quoted = test_web_post(
            quoted_author,
            "quoted-labels",
            1,
            "Quoted body under test",
            None,
        );
        let embed = quote_embed(&quoted.uri);
        let mut index = QuoteRenderIndex::default();
        index.insert(quoted.uri.clone(), QuoteRenderState::Found(quoted));
        let definitions = cornell_fixture_definitions();
        index.account_labels.insert(
            quoted_author.to_owned(),
            cornell_labels(
                &definitions,
                quoted_author,
                &["made-over-hundred-posts-yesterday"],
            ),
        );

        let rendered = render_web_quote(&storage, &embed, &index, 0);

        assert!(
            rendered.contains("<details class=\"account-labels\">"),
            "{rendered}"
        );
        assert!(
            rendered.contains("aria-label=\"1 account label\""),
            "{rendered}"
        );
        assert!(
            rendered.contains("Made over 100 posts yesterday"),
            "{rendered}"
        );
    }

    #[test]
    fn the_expanded_label_panel_stays_inside_a_phone_screen_and_rings_alerts_red() {
        let css = include_str!("../static/app.css");

        assert!(
            css_rule_bodies(css, ".account-labels .account-label-groups")
                .iter()
                .any(|block| {
                    block.contains("min(19rem")
                        && block.contains("var(--account-label-panel-inset")
                        && !block.contains("100vw")
                }),
            "the expanded panel must be measured against its card, not the viewport"
        );
        assert!(
            css_rule_bodies(css, ".account-label-emoji.severity-alert")
                .iter()
                .any(|block| { block.contains("var(--label-alert-ring)") }),
            "alert icons get the red ring"
        );
    }

    #[test]
    fn the_open_label_panel_is_anchored_to_the_card_not_the_control() {
        let css = include_str!("../static/app.css");

        let control = css_rule_bodies(css, ".account-labels")
            .into_iter()
            .find(|block| block.contains("display: inline-block"))
            .expect(
                ".account-labels must be an inline-block so the panel's static top sits under the summary",
            );
        assert!(
            css_declaration(&control, "position").is_none(),
            "the closed control must not be the panel's containing block: {control}"
        );
        assert_eq!(
            css_declaration(&control, "vertical-align").as_deref(),
            Some("middle")
        );

        let panel = css_rule_bodies(css, ".account-labels .account-label-groups")
            .into_iter()
            .find(|block| block.contains("var(--account-label-panel-inset"))
            .expect("the panel must span the card through the inset custom property");
        assert_eq!(css_declaration(&panel, "top").as_deref(), Some("auto"));
        assert_eq!(
            css_declaration(&panel, "left").as_deref(),
            Some("var(--account-label-panel-inset, 0.9rem)")
        );
        assert!(
            panel.contains(
                "width: min(19rem, calc(100% - var(--account-label-panel-inset, 0.9rem) * 2))"
            ),
            "the panel spans the card's inner width, capped at 19rem only when there is room: {panel}"
        );
        assert!(
            !panel.contains("100vw"),
            "the panel must be measured against the card, not the viewport: {panel}"
        );

        for (selector, inset) in [
            (".post-card", "--account-label-panel-inset: 1.25rem;"),
            (".quoted-post-card", "--account-label-panel-inset: 0.85rem;"),
            (".profile-header", "--account-label-panel-inset: 0.9rem;"),
        ] {
            assert!(
                css_rule_bodies(css, selector)
                    .iter()
                    .any(|block| { block.contains("position: relative") && block.contains(inset) }),
                "{selector} must establish the panel's containing block and carry its inner-width inset"
            );
        }
        assert!(
            css_rule_bodies(css, ".post-card")
                .iter()
                .any(|block| block.contains("--account-label-panel-inset: 0.9rem;")),
            "the phone media query must shrink the post-card inset with its 0.9rem padding"
        );
    }

    #[test]
    fn the_labeler_link_gets_its_own_line_inside_the_copy_column() {
        let css = include_str!("../static/app.css");
        for block in css_rule_bodies(css, ".account-label-by") {
            assert!(
                !block.contains("max-width: 40%") && !block.contains("flex: 0 0 auto"),
                "the labeler link must stop being a squeeze-prone side column: {block}"
            );
        }

        let definitions = cornell_fixture_definitions();
        let subject = "did:plc:row-layout-author";
        let mut labels_by_actor = HashMap::new();
        labels_by_actor.insert(
            subject.to_owned(),
            cornell_labels(
                &definitions,
                subject,
                &["made-over-hundred-posts-yesterday"],
            ),
        );

        let rendered = render_account_label_chips(&labels_by_actor, subject);

        let row = &rendered[rendered.find("<li class=\"account-label-row").unwrap()..];
        let description = row.find("account-label-description").unwrap();
        let by = row.find("account-label-by").unwrap();
        assert!(
            description < by,
            "the link sits under the description: {row}"
        );
        assert!(
            row[by..].contains("</a></span></li>"),
            "the link is the last thing inside .account-label-copy, not a side column: {row}"
        );
    }

    #[tokio::test]
    async fn stale_oauth_metadata_returns_immediately_and_refreshes_in_background() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let calls = Arc::new(AtomicUsize::new(0));
        let server_calls = calls.clone();
        let app = Router::new().route(
            "/.well-known/oauth-authorization-server",
            get(move || {
                let calls = server_calls.clone();
                async move {
                    calls.fetch_add(1, Ordering::Relaxed);
                    tokio::time::sleep(Duration::from_millis(120)).await;
                    Json(json!({"issuer":"new"}))
                }
            }),
        );
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let client = OAuthMetadataHttpClient::new(
            SafeHttpClient::new().with_http_test_hosts(["127.0.0.1".to_owned()]),
        );
        let uri = format!("http://{address}/.well-known/oauth-authorization-server");
        let key = format!("GET {uri}");
        client.cache.lock().await.insert(
            key,
            CachedOAuthMetadataResponse {
                expires: tokio::time::Instant::now() - Duration::from_secs(1),
                status: StatusCode::OK,
                headers: HeaderMap::new(),
                body: br#"{"issuer":"old"}"#.to_vec(),
            },
        );
        let request = http::Request::builder()
            .method("GET")
            .uri(uri)
            .body(Vec::new())
            .unwrap();

        for _ in 0..2 {
            let response =
                tokio::time::timeout(Duration::from_millis(50), client.send_http(request.clone()))
                    .await
                    .expect("stale OAuth metadata waited for the upstream")
                    .unwrap();
            let body: Value = serde_json::from_slice(response.body()).unwrap();
            assert_eq!(body["issuer"], "old");
        }
        tokio::time::sleep(Duration::from_millis(160)).await;
        assert_eq!(calls.load(Ordering::Relaxed), 1);
        let refreshed = client.send_http(request).await.unwrap();
        let body: Value = serde_json::from_slice(refreshed.body()).unwrap();
        assert_eq!(body["issuer"], "new");
        server.abort();
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
            Some("Bearer case23-service-jwt")
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
                .all(|authorization| authorization.as_deref() == Some("Bearer case23-service-jwt"))
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

    #[tokio::test]
    async fn video_upload_uses_pds_upload_blob_service_auth_parameters() {
        let (mut flow, state, server) = feed_test_flow(false).await;
        flow.video_service_url = state.lock().unwrap().pds_url.clone();
        let (mut temporary, mut file) = create_temporary_video_file().unwrap();
        file.write_all(b"video").await.unwrap();
        drop(file);
        let path = temporary.take_path();
        let video = PostVideo {
            path,
            mime_type: "video/mp4".to_owned(),
            alt: String::new(),
            width: 1920,
            height: 1080,
            duration_seconds: 1.0,
            size_bytes: 5,
            filename: "clip.mp4".to_owned(),
        };

        let blob = flow
            .upload_video_blob(FEED_VIEWER_DID, &video)
            .await
            .unwrap();
        assert_eq!(
            blob.pointer("/ref/$link").and_then(Value::as_str),
            Some("bafk-video")
        );
        assert_eq!(
            state.lock().unwrap().auth_requests.as_slice(),
            &[
                (
                    VIDEO_SERVICE_DID.to_owned(),
                    "app.bsky.video.getUploadLimits".to_owned()
                ),
                (
                    "did:web:127.0.0.1".to_owned(),
                    "com.atproto.repo.uploadBlob".to_owned()
                ),
            ]
        );
        assert_eq!(
            state.lock().unwrap().video_requests.as_slice(),
            &[
                (
                    "/xrpc/app.bsky.video.getUploadLimits".to_owned(),
                    "Bearer limits-service-jwt".to_owned()
                ),
                (
                    "/xrpc/app.bsky.video.uploadVideo".to_owned(),
                    "Bearer upload-blob-service-jwt".to_owned()
                ),
                (
                    "/xrpc/app.bsky.video.getJobStatus".to_owned(),
                    "Bearer upload-blob-service-jwt".to_owned()
                ),
            ]
        );
        server.abort();
    }

    // HTTP/2 browsers may send each cookie as its own `cookie` header, so inspect all values.
    #[tokio::test]
    async fn login_page_prefills_the_remembered_handle_safely() {
        let mut headers = HeaderMap::new();
        headers.append(
            header::COOKIE,
            "appview_handle=owner.example.test".parse().unwrap(),
        );
        let Html(body) = login(headers, Query(HashMap::new())).await;
        assert!(body.contains("value=\"owner.example.test\""), "{body}");

        let mut headers = HeaderMap::new();
        headers.append(
            header::COOKIE,
            "appview_handle=x\"onfocus=alert(1)".parse().unwrap(),
        );
        let Html(body) = login(headers, Query(HashMap::new())).await;
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

    const CHEEKY_URI: &str = "https://cheekypint.substack.com/p/why-sam-altman-is-against-a-single";
    const CHEEKY_TEXT: &str = "cheekypint.substack.com/p/why-sam-al...";

    fn link_facet_fixture_record() -> IndexedRecord {
        let fixture: Value =
            serde_json::from_str(include_str!("../tests/fixtures/link-facet-post.json")).unwrap();
        IndexedRecord {
            did: "did:plc:ckaz32jwl6t2cno6fmuw2nhn".to_owned(),
            collection: "app.bsky.feed.post".to_owned(),
            rkey: "3mz27h4raoc2e".to_owned(),
            uri: fixture["uri"].as_str().unwrap().to_owned(),
            cid: fixture["cid"].as_str().map(str::to_owned),
            time_us: 1,
            record: fixture["record"].clone(),
        }
    }

    fn link_facet(byte_start: u64, byte_end: u64, uri: &str) -> Value {
        json!({
            "index": {"byteStart": byte_start, "byteEnd": byte_end},
            "features": [{"$type": "app.bsky.richtext.facet#link", "uri": uri}]
        })
    }

    #[test]
    fn link_facets_in_post_text_become_anchors_with_the_full_target_url_underneath() {
        let record = link_facet_fixture_record();
        let storage = MemoryStorage::default();
        storage.put_record(&record).unwrap();
        let index = QuoteRenderIndex::default();

        let rendered = render_web_post(
            &storage,
            &record,
            "did:plc:viewer",
            false,
            true,
            false,
            &index,
        );

        assert!(
            rendered.contains(&format!(
                "<a class=\"post-text-link\" href=\"{CHEEKY_URI}\" rel=\"nofollow noopener noreferrer\" target=\"_blank\">{CHEEKY_TEXT}</a>"
            )),
            "the facet span (UTF-8 byte offsets past the curly apostrophe) becomes the tappable anchor: {rendered}"
        );
        assert!(
            rendered.contains(&format!(
                "<p class=\"post-link-targets\"><a class=\"post-link-target\" href=\"{CHEEKY_URI}\" rel=\"nofollow noopener noreferrer\" target=\"_blank\">{CHEEKY_URI}</a></p>"
            )),
            "the full target URL sits in small print under the post text: {rendered}"
        );
        assert!(
            !rendered.contains("<a class=\"post-text-link\" href=\"/app/thread"),
            "the whole-text thread link must not nest around an external anchor: {rendered}"
        );
    }

    #[test]
    fn thread_focus_renders_facet_links_and_their_target_lines() {
        let record = link_facet_fixture_record();
        let storage = MemoryStorage::default();
        storage.put_record(&record).unwrap();
        let index = QuoteRenderIndex::default();

        let rendered =
            render_thread_focus(&storage, &record, "did:plc:viewer", false, false, &index);

        assert!(
            rendered.contains(&format!(
                "<a class=\"post-text-link\" href=\"{CHEEKY_URI}\" rel=\"nofollow noopener noreferrer\" target=\"_blank\">{CHEEKY_TEXT}</a>"
            )),
            "the focused post's link facet is tappable: {rendered}"
        );
        assert!(
            rendered.contains(&format!(
                "<p class=\"post-link-targets\"><a class=\"post-link-target\" href=\"{CHEEKY_URI}\""
            )),
            "the focused post shows the full target line: {rendered}"
        );
    }

    #[test]
    fn out_of_range_overlapping_and_mid_char_link_facets_render_as_plain_text() {
        let text = "it’s https://x.example/a/b tail more";
        let good = link_facet(7, 28, "https://x.example/a/b");

        assert_eq!(
            render_post_text(text, &[link_facet(100, 200, "https://x.example/a/b")]),
            html_escape(text),
            "offsets past the end of the text stay plain text"
        );
        assert_eq!(
            render_post_text(text, &[link_facet(4, 28, "https://x.example/a/b")]),
            html_escape(text),
            "an offset inside the 3-byte apostrophe stays plain text"
        );
        let overlapping = render_post_text(
            text,
            &[link_facet(21, 36, "https://y.example/z"), good.clone()],
        );
        assert_eq!(
            overlapping.matches("<a class=\"post-text-link\"").count(),
            1,
            "only the first of two overlapping facets becomes an anchor: {overlapping}"
        );
        assert!(
            overlapping.contains(
                "<a class=\"post-text-link\" href=\"https://x.example/a/b\" rel=\"nofollow noopener noreferrer\" target=\"_blank\">https://x.example/a/b</a>"
            ),
            "the first facet still renders: {overlapping}"
        );
        assert!(
            overlapping.contains("tail more"),
            "the rest of the text is plain: {overlapping}"
        );
    }

    #[test]
    fn javascript_uris_and_non_link_facets_stay_plain_text() {
        let text = "it’s https://x.example/a/b tail more";
        assert_eq!(
            render_post_text(text, &[link_facet(7, 28, "javascript:alert(1)")]),
            html_escape(text),
            "only http/https facet URIs become links"
        );
        let mention = json!({
            "index": {"byteStart": 6, "byteEnd": 13},
            "features": [{"$type": "app.bsky.richtext.facet#mention", "did": "did:plc:friend"}]
        });
        assert_eq!(
            render_post_text("hello @friend tail", &[mention]),
            html_escape("hello @friend tail"),
            "mentions are out of scope and stay plain text"
        );
    }

    #[test]
    fn target_lines_show_the_full_uri_once_and_skip_links_that_already_show_their_url() {
        let dup = "https://x.example/dup";
        let text = format!("one {dup} two {dup}");
        let targets =
            render_post_link_targets(&text, &[link_facet(4, 25, dup), link_facet(30, 51, dup)]);
        assert_eq!(
            targets.matches("<p class=\"post-link-targets\">").count(),
            1,
            "one target line per distinct link, deduplicated: {targets}"
        );
        assert!(
            targets.contains(&format!(
                "<p class=\"post-link-targets\"><a class=\"post-link-target\" href=\"{dup}\" rel=\"nofollow noopener noreferrer\" target=\"_blank\">{dup}</a></p>"
            )),
            "the line carries the full URI: {targets}"
        );

        let shown = "x.example/plain";
        let text = format!("read {shown}");
        assert_eq!(
            render_post_link_targets(&text, &[link_facet(5, 20, "https://x.example/plain")]),
            "",
            "a link whose visible text is already the URL needs no target line"
        );
        assert!(
            render_post_text(&text, &[link_facet(5, 20, "https://x.example/plain")])
                .contains("<a class=\"post-text-link\" href=\"https://x.example/plain\""),
            "the text itself is still tappable"
        );
    }

    #[test]
    fn a_post_without_link_facets_renders_exactly_as_before() {
        assert_eq!(
            render_post_text("just words", &[]),
            html_escape("just words")
        );
        assert_eq!(
            render_post_text("thread post 2/3", &[]),
            format!(
                "{}<span class=\"thread-number-pill\">{}</span>",
                html_escape("thread post "),
                html_escape("2/3")
            ),
            "the thread-number pill stays where it was"
        );
        assert_eq!(
            render_post_text("1/2", &[]),
            "<span class=\"thread-number-pill\">1/2</span>"
        );
        assert_eq!(render_post_link_targets("just words", &[]), "");
    }

    #[test]
    fn post_link_target_lines_are_small_muted_and_wrap_inside_the_card() {
        let css = include_str!("../static/app.css");
        let bodies = css_rule_bodies(css, ".post-link-targets");
        assert!(
            !bodies.is_empty(),
            "app.css must style the small-print target lines"
        );
        let body = bodies.last().unwrap();
        assert_eq!(
            css_declaration(body, "overflow-wrap").as_deref(),
            Some("anywhere"),
            "a long URL must wrap instead of widening the card at 402 px"
        );
        assert_eq!(
            css_declaration(body, "font-size").as_deref(),
            Some("0.75rem")
        );
        assert_eq!(
            css_declaration(body, "color").as_deref(),
            Some("var(--muted)")
        );
        assert!(WEB_STYLESHEET_URL.contains("v=m40"));
        assert!(WEB_NOJS_STYLESHEET_URL.contains("v=m40"));
    }

    #[test]
    fn post_text_links_are_accent_coloured_after_the_whole_text_link_rule() {
        let css = include_str!("../static/app.css");
        let whole_text = css_rule_bodies(css, ".post-text a")
            .first()
            .expect("the .post-text a rule for the whole-text thread link must stay")
            .clone();
        assert_eq!(
            css_declaration(&whole_text, "color").as_deref(),
            Some("inherit"),
            "the whole-text thread link must keep looking like body text"
        );
        assert_eq!(
            css_declaration(&whole_text, "text-decoration").as_deref(),
            Some("none")
        );
        let facet = css_rule_bodies(css, ".post-text a.post-text-link")
            .into_iter()
            .last()
            .expect("link facets need their own rule so they look like links");
        assert_eq!(
            css_declaration(&facet, "color").as_deref(),
            Some("var(--accent)"),
            "a link facet must render in the accent colour, not the body-text colour"
        );
        assert!(
            css.find(".post-text a.post-text-link {") > css.find(".post-text a {"),
            "the accent rule must come after `.post-text a {{` so it wins the cascade"
        );
        assert!(WEB_STYLESHEET_URL.contains("v=m40"));
    }
}

#[cfg(not(unix))]
fn create_key_file(path: &Path) -> Result<fs::File, std::io::Error> {
    fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
}
