use crate::identity::{HttpIdentityBackend, IdentityResolver};
use crate::{
    api::profile_view_from_storage,
    feeds::{
        FeedClient, FeedTimeline, ResolvedFeed, SavedFeedPreference, parse_saved_feed_preferences,
    },
    ingest::{
        IndexedNotification, IndexedRecord, MemoryStorage, NotificationCursor, RecordCursor,
        Storage, StorageError,
    },
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
use atrium_xrpc::{OutputDataOrBytes, XrpcClient, XrpcRequest};
use axum::{
    Json, Router,
    extract::{Form, Query, State},
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::{Html, IntoResponse, Redirect, Response},
    routing::{get, post},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use futures_util::FutureExt;
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
    path::Path,
    pin::Pin,
    sync::{Arc, Mutex},
};
use thiserror::Error;
use time::{OffsetDateTime, format_description::well_known::Rfc3339};

const METADATA_PATH: &str = "/.well-known/oauth-client-metadata.json";
const CALLBACK_PATH: &str = "/oauth/callback";
const SESSION_COOKIE: &str = "appview_session";
const LOGIN_COOKIE: &str = "appview_login";
const SESSION_TTL_SECONDS: u64 = 30 * 24 * 60 * 60;
const LOGIN_COOKIE_TTL_SECONDS: u64 = 10 * 60;
const MAX_RENDERED_REMOTE_CHARS: usize = 4096;
const WEB_STYLESHEET_URL: &str = "/static/app.css?v=av16";
const REQUESTED_SCOPE: &str = "atproto repo:app.bsky.feed.post?action=create repo:app.bsky.feed.like?action=create&action=delete repo:app.bsky.feed.repost?action=create&action=delete rpc:app.bsky.actor.getPreferences?aud=* rpc:app.bsky.feed.getFeedSkeleton?aud=*";
const WRITE_SCOPES: [&str; 3] = [
    "repo:app.bsky.feed.post?action=create",
    "repo:app.bsky.feed.like?action=create&action=delete",
    "repo:app.bsky.feed.repost?action=create&action=delete",
];
const FEED_SCOPES: [&str; 2] = [
    "rpc:app.bsky.actor.getPreferences?aud=*",
    "rpc:app.bsky.feed.getFeedSkeleton?aud=*",
];
type HmacSha256 = Hmac<Sha256>;

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
        Router::new()
            .route(METADATA_PATH, get(metadata::<F, S>))
            .route("/static/app.css", get(app_stylesheet))
            .route("/", get(root))
            .route("/login", get(login))
            .route("/oauth/authorize", post(authorize::<F, S>))
            .route("/oauth/callback", get(callback::<F, S>))
            .route("/logout", post(logout::<F, S>))
            .route("/app", get(app_home::<F, S>))
            .route("/feeds", get(feeds_page::<F, S>))
            .route("/app/feed", get(feed_timeline_page::<F, S>))
            .route("/notifications", get(notifications_page::<F, S>))
            .route("/profile", get(profile_page::<F, S>))
            .route("/thread", get(thread_page::<F, S>))
            .route("/post", post(write_post::<F, S>))
            .route("/like", post(write_like::<F, S>))
            .route("/repost", post(write_repost::<F, S>))
            .with_state(Arc::new(OAuthWebState {
                public: self.clone(),
                flow,
                storage,
            }))
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
        write_access: bool,
        feed_access: bool,
    ) -> String {
        let expires = unix_time().saturating_add(SESSION_TTL_SECONDS);
        let payload =
            format!("{did}\n{expires}\n{session_id}\n{generation}\n{write_access}\n{feed_access}");
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

    fn verify_session_cookie(&self, token: &str) -> Option<(String, String, u64, bool, bool)> {
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
        let (write_access, feed_access) = permissions
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

pub struct AtriumOAuthFlow<B: crate::identity::IdentityBackend = HttpIdentityBackend> {
    client: Arc<AppOAuthClient>,
    identity_resolver: Arc<IdentityResolver<B>>,
    store: SqliteOAuthStore,
    feed_client: Arc<FeedClient<B>>,
    saved_feed_cache: SavedFeedCache,
}

impl<B: crate::identity::IdentityBackend> Clone for AtriumOAuthFlow<B> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            identity_resolver: self.identity_resolver.clone(),
            store: self.store.clone(),
            feed_client: self.feed_client.clone(),
            saved_feed_cache: self.saved_feed_cache.clone(),
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
        connection.execute_batch(
            "PRAGMA journal_mode = WAL;
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
        Ok(Self {
            connection: Arc::new(Mutex::new(connection)),
        })
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
        let feed_client = Arc::new(FeedClient::new(
            Arc::clone(&identity_resolver),
            guarded_client.clone(),
        ));
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
            saved_feed_cache: Arc::new(tokio::sync::Mutex::new(HashMap::new())),
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
            .map_err(|_| "PDS getPreferences request failed".to_owned())?;
        let response = match response {
            OutputDataOrBytes::Data(value) => value,
            OutputDataOrBytes::Bytes(_) => {
                return Err("PDS getPreferences returned a non-JSON response".to_owned());
            }
        };
        let preferences = parse_saved_feed_preferences(&response);
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

    async fn service_auth_token(
        &self,
        viewer_did: &str,
        service_did: &str,
    ) -> Result<String, String> {
        let did = viewer_did
            .parse::<Did>()
            .map_err(|_| "viewer DID is invalid".to_owned())?;
        let session = self
            .client
            .restore(&did)
            .await
            .map_err(|_| "OAuth session could not be restored".to_owned())?;
        let request = XrpcRequest {
            method: http::Method::GET,
            nsid: "com.atproto.server.getServiceAuth".to_owned(),
            parameters: Some(json!({"aud":service_did,"lxm":"app.bsky.feed.getFeedSkeleton"})),
            input: None,
            encoding: None,
        };
        let response: OutputDataOrBytes<Value> = session
            .send_xrpc::<Value, Value, Value, Value>(&request)
            .await
            .map_err(|_| "PDS getServiceAuth request failed".to_owned())?;
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
            let preferences = self.saved_feed_preferences(did).await?;
            let mut feeds = Vec::new();
            for preference in preferences
                .iter()
                .filter(|item| item.feed_type == "feed" && item.pinned)
            {
                if let Ok(feed) = self.feed_client.resolve_feed(&preference.value).await {
                    feeds.push(feed);
                }
            }
            Ok(feeds)
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
            let generator = self.feed_client.resolve_feed(uri).await?;
            let service_jwt = self
                .service_auth_token(did, &generator.service_did)
                .await
                .ok();
            let skeleton = self
                .feed_client
                .get_skeleton(&generator, cursor, service_jwt.as_deref())
                .await?;
            let timeline = self.feed_client.hydrate_skeleton(skeleton, storage).await?;
            Ok(FeedTimeline {
                generator,
                timeline,
            })
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
    let navigation = if authenticated {
        "<nav class=\"site-nav\" aria-label=\"Main navigation\"><a href=\"/app\">Timeline</a><a href=\"/feeds\">Feeds</a><a href=\"/notifications\">Notifications</a><a href=\"/profile\">Profile</a><form action=\"/logout\" method=\"post\"><button type=\"submit\">Sign out</button></form></nav>"
    } else {
        "<nav class=\"site-nav\" aria-label=\"Main navigation\"><a href=\"/login\">Sign in</a></nav>"
    };
    format!(
        "<!doctype html><html lang=\"en\"><head><meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"><title>{}</title><link rel=\"stylesheet\" href=\"{}\"></head><body><a class=\"skip-link\" href=\"#main-content\">Skip to content</a><div class=\"site-shell\"><header class=\"site-header\"><a class=\"brand\" href=\"/app\">AppView</a>{navigation}</header><main id=\"main-content\" class=\"page-content\">{content}</main></div></body></html>",
        html_escape(title),
        WEB_STYLESHEET_URL
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
    let cookie = format!(
        "{SESSION_COOKIE}={}; Path=/; HttpOnly; SameSite=Lax; Max-Age={SESSION_TTL_SECONDS}{}",
        state.public.issue_session_cookie(
            &account.did,
            &session_id,
            generation,
            write_access,
            feed_access,
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
    let Some((did, session_id, _, _, _)) =
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
    let Some(did) = verified_cookie_did(&state.public, &headers, state.storage.as_ref()) else {
        return Redirect::to("/login").into_response();
    };
    let limit = query
        .get("limit")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(50)
        .clamp(1, 100);
    let cursor = query
        .get("cursor")
        .and_then(|cursor| RecordCursor::parse(cursor));
    let timeline_page = match state
        .storage
        .timeline_posts(&did, cursor.as_ref(), limit + 1)
    {
        Ok(records) => records,
        Err(error) => return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    let has_more = timeline_page.len() > limit;
    let timeline = timeline_page.iter().take(limit).collect::<Vec<_>>();
    let write_forms_enabled =
        verified_cookie_write_access(&state.public, &headers, state.storage.as_ref());
    let posts = timeline
        .iter()
        .map(|record| {
            format!(
                "<li class=\"post-item\">{}</li>",
                render_web_post(
                    state.storage.as_ref(),
                    record,
                    &did,
                    write_forms_enabled,
                    true
                )
            )
        })
        .collect::<String>();
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
        "<form class=\"composer\" action=\"/post\" method=\"post\"><label for=\"new-post\">Write a post</label><textarea id=\"new-post\" name=\"text\" rows=\"3\" maxlength=\"300\" required></textarea><button type=\"submit\">Post</button></form>"
    } else {
        ""
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
    Html(page_shell(
        "Timeline",
        &format!(
            "<header class=\"page-heading\"><p class=\"eyebrow\">Your feed</p><h1>Timeline</h1><p class=\"page-subtitle\">Posts from accounts in your local view.</p></header>{read_only_message}{indexing_status}{post_form}<ol class=\"post-list\">{posts}</ol>{older}",
        ),
        true,
    ))
    .into_response()
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
    let feeds = match state.flow.pinned_feeds(&did).await {
        Ok(feeds) => feeds,
        Err(error) => return oauth_error(StatusCode::BAD_GATEWAY, &error),
    };
    let entries = feeds.iter().map(render_feed_card).collect::<String>();
    let entries = if entries.is_empty() {
        "<li class=\"empty-state\">No pinned custom feeds were found in your account preferences.</li>".to_owned()
    } else {
        entries
    };
    Html(page_shell(
        "Feeds",
        &format!(
            "<header class=\"page-heading\"><p class=\"eyebrow\">Your saved algorithms</p><h1>Feeds</h1><p class=\"page-subtitle\">Pinned custom feeds from your account.</p></header><ol class=\"feed-list\">{entries}</ol>"
        ),
        true,
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
    let Some(did) = verified_cookie_did(&state.public, &headers, state.storage.as_ref()) else {
        return Redirect::to("/login").into_response();
    };
    if !verified_cookie_feed_access(&state.public, &headers, state.storage.as_ref()) {
        return feed_reauthorization_page();
    }
    let Some(uri) = query.get("uri").filter(|uri| !uri.is_empty()) else {
        return oauth_error(StatusCode::BAD_REQUEST, "feed uri is required");
    };
    let page = match state
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
    };
    let write_forms_enabled =
        verified_cookie_write_access(&state.public, &headers, state.storage.as_ref());
    let posts = page
        .timeline
        .feed
        .iter()
        .map(|post| {
            let repost = post
                .reposted_by
                .as_deref()
                .map(|reposter| {
                    format!(
                        "<p class=\"feed-repost\">Reposted by @{}</p>",
                        html_escape(reposter)
                    )
                })
                .unwrap_or_default();
            format!(
                "<li class=\"post-item\">{repost}{}</li>",
                render_web_post(
                    state.storage.as_ref(),
                    &post.record,
                    &did,
                    write_forms_enabled,
                    true,
                )
            )
        })
        .collect::<String>();
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
    Html(page_shell(
        &page.generator.display_name,
        &format!(
            "{}{}<ol class=\"post-list\">{posts}</ol>{older}",
            render_feed_heading(&page.generator),
            skipped
        ),
        true,
    ))
    .into_response()
}

async fn notifications_page<F, S>(
    State(state): State<Arc<OAuthWebState<F, S>>>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response
where
    F: OAuthFlow,
    S: Storage,
{
    let Some(did) = verified_cookie_did(&state.public, &headers, state.storage.as_ref()) else {
        return Redirect::to("/login").into_response();
    };
    let limit = query
        .get("limit")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(50)
        .clamp(1, 100);
    let before = query
        .get("cursor")
        .and_then(|cursor| NotificationCursor::parse(cursor));
    let page = match state
        .storage
        .notifications_for(&did, before.as_ref(), limit + 1)
    {
        Ok(page) => page,
        Err(error) => return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    let has_more = page.len() > limit;
    let items = page
        .iter()
        .take(limit)
        .map(|notification| render_web_notification(state.storage.as_ref(), notification))
        .collect::<String>();
    let items = if items.is_empty() {
        "<li class=\"empty-state\">No notifications yet.</li>".to_owned()
    } else {
        items
    };
    let older = if has_more {
        page.get(limit - 1)
            .map(|notification| {
                let cursor = NotificationCursor {
                    time_us: notification.record.time_us,
                    uri: notification.record.uri.clone(),
                    reason: notification.reason.clone(),
                }
                .encode();
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
    Html(page_shell(
        "Notifications",
        &format!(
            "<header class=\"page-heading\"><p class=\"eyebrow\">What’s happening</p><h1>Notifications</h1><p class=\"page-subtitle\">Recent activity around your posts and profile.</p></header><ol class=\"notification-list\">{items}</ol>{older}"
        ),
        true,
    ))
    .into_response()
}

fn render_web_notification<S: Storage>(storage: &S, notification: &IndexedNotification) -> String {
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
    let target_uri = notification
        .reason_subject
        .as_deref()
        .unwrap_or(&record.uri);
    let thread_link = if target_uri.starts_with("at://") {
        let encoded =
            url::form_urlencoded::byte_serialize(target_uri.as_bytes()).collect::<String>();
        format!(
            "<a href=\"/thread?uri={}\">View thread</a>",
            html_escape(&encoded)
        )
    } else {
        String::new()
    };
    format!(
        "<li class=\"notification-item\"><article class=\"notification-card\"><header class=\"notification-heading\">{}<div><a class=\"author-name\" href=\"{}\">@{}</a><span class=\"notification-reason\">{reason}</span></div></header>{text}{thread_link}</article></li>",
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
    S: Storage,
{
    let Some(did) = verified_cookie_did(&state.public, &headers, state.storage.as_ref()) else {
        return Redirect::to("/login").into_response();
    };
    let subject = query.get("did").map(String::as_str).unwrap_or(&did);
    let profile = match profile_view_from_storage(state.storage.as_ref(), subject) {
        Ok(profile) => profile,
        Err(error) => return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    let handle = profile_handle(&profile, subject);
    let display_name = profile
        .get("displayName")
        .and_then(Value::as_str)
        .unwrap_or(&handle);
    let limit = query
        .get("limit")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(50)
        .clamp(1, 100);
    let cursor = query
        .get("cursor")
        .and_then(|cursor| RecordCursor::parse(cursor));
    let records = match state
        .storage
        .author_posts(subject, cursor.as_ref(), limit + 1, true)
    {
        Ok(records) => records,
        Err(error) => return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    let has_more = records.len() > limit;
    let records = records.iter().take(limit).collect::<Vec<_>>();
    let write_forms_enabled =
        verified_cookie_write_access(&state.public, &headers, state.storage.as_ref());
    let posts = records
        .iter()
        .map(|record| {
            format!(
                "<li class=\"post-item\">{}</li>",
                render_web_post(
                    state.storage.as_ref(),
                    record,
                    &did,
                    write_forms_enabled,
                    true
                )
            )
        })
        .collect::<String>();
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
    Html(page_shell(
        display_name,
        &format!(
            "<section class=\"profile-card\">{}<div><h1>{}</h1><p class=\"profile-handle\">@{}</p></div></section><header class=\"section-heading\"><h2>Posts</h2></header><ol class=\"post-list\">{posts}</ol>{older}",
            avatar_markup(&profile, "profile-avatar"),
            html_escape(display_name),
            html_escape(&handle)
        ),
        true,
    ))
    .into_response()
}

async fn thread_page<F, S>(
    State(state): State<Arc<OAuthWebState<F, S>>>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response
where
    F: OAuthFlow,
    S: Storage,
{
    let Some(viewer_did) = verified_cookie_did(&state.public, &headers, state.storage.as_ref())
    else {
        return Redirect::to("/login").into_response();
    };
    let Some(uri) = query.get("uri") else {
        return oauth_error(StatusCode::BAD_REQUEST, "uri is required");
    };
    let record = match state.storage.record_by_uri("app.bsky.feed.post", uri) {
        Ok(record) => record,
        Err(error) => return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    let Some(record) = record else {
        return oauth_error(StatusCode::NOT_FOUND, "post not found");
    };
    let write_forms_enabled =
        verified_cookie_write_access(&state.public, &headers, state.storage.as_ref());
    let parent_height = query
        .get("parentHeight")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(80)
        .min(20);
    let depth = query
        .get("depth")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(3)
        .min(6);
    let ancestors = match thread_ancestors(state.storage.as_ref(), &record, parent_height) {
        Ok(ancestors) => ancestors,
        Err(error) => return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    let ancestors_html = ancestors
        .iter()
        .map(|ancestor| {
            format!(
                "<li class=\"post-item\">{}</li>",
                render_web_post(
                    state.storage.as_ref(),
                    ancestor,
                    &viewer_did,
                    write_forms_enabled,
                    false,
                )
            )
        })
        .collect::<String>();
    let mut reply_budget = 100;
    let replies_html = match render_thread_replies(
        state.storage.as_ref(),
        &record,
        &viewer_did,
        write_forms_enabled,
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
    Html(page_shell(
        "Thread",
        &format!(
            "<header class=\"page-heading\"><p class=\"eyebrow\">Conversation</p><h1>Thread</h1></header>{read_only_message}<section class=\"thread-ancestors\" aria-label=\"Earlier posts in this thread\"><ol class=\"post-list\">{ancestors_html}</ol></section><section class=\"thread-focus\" aria-label=\"Selected post\">{}</section><section class=\"thread-replies\"><h2>Replies</h2>{replies_html}</section>",
            render_web_post(
                state.storage.as_ref(),
                &record,
                &viewer_did,
                write_forms_enabled,
                false
            )
        ),
        true,
    ))
    .into_response()
}

fn thread_ancestors<S: Storage>(
    storage: &S,
    record: &IndexedRecord,
    parent_height: usize,
) -> Result<Vec<IndexedRecord>, StorageError> {
    let mut ancestors = Vec::new();
    let mut visited = HashSet::from([record.uri.clone()]);
    let mut parent_uri = record
        .record
        .pointer("/reply/parent/uri")
        .and_then(Value::as_str)
        .map(str::to_owned);
    for _ in 0..parent_height {
        let Some(uri) = parent_uri else { break };
        if !visited.insert(uri.clone()) {
            break;
        }
        let Some(parent) = storage.record_by_uri("app.bsky.feed.post", &uri)? else {
            break;
        };
        parent_uri = parent
            .record
            .pointer("/reply/parent/uri")
            .and_then(Value::as_str)
            .map(str::to_owned);
        ancestors.push(parent);
    }
    ancestors.reverse();
    Ok(ancestors)
}

fn render_thread_replies<S: Storage>(
    storage: &S,
    record: &IndexedRecord,
    viewer_did: &str,
    write_forms_enabled: bool,
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
    for reply in replies {
        if *budget == 0 {
            break;
        }
        *budget -= 1;
        let nested = render_thread_replies(
            storage,
            &reply,
            viewer_did,
            write_forms_enabled,
            depth - 1,
            budget,
        )?;
        html.push_str(&format!(
            "<li class=\"post-item\">{}{nested}</li>",
            render_web_post(storage, &reply, viewer_did, write_forms_enabled, false)
        ));
    }
    html.push_str("</ol>");
    Ok(html)
}

async fn write_post<F, S>(
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
    let text = form.get("text").map(String::as_str).unwrap_or("").trim();
    if text.is_empty() || text.chars().count() > 300 {
        return oauth_error(
            StatusCode::BAD_REQUEST,
            "post text must contain 1 to 300 characters",
        );
    }
    match state.flow.create_post(&did, text).await {
        Ok(created) => {
            match persist_created_record(state.storage.clone(), &did, "app.bsky.feed.post", created)
                .await
            {
                Ok(()) => Redirect::to("/app").into_response(),
                Err(error) => oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error),
            }
        }
        Err(error) => oauth_error(StatusCode::BAD_GATEWAY, &error),
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
                Ok(()) => Redirect::to("/app").into_response(),
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
                Ok(()) => Redirect::to("/app").into_response(),
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
            Ok(()) => Redirect::to("/app").into_response(),
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
    verified_cookie_session(public, headers, storage).map(|(did, _, _, _, _)| did)
}

fn verified_cookie_write_access<S: Storage>(
    public: &OAuthPublic,
    headers: &HeaderMap,
    storage: &S,
) -> bool {
    verified_cookie_session(public, headers, storage)
        .is_some_and(|(_, _, _, write_access, _)| write_access)
}

fn verified_cookie_feed_access<S: Storage>(
    public: &OAuthPublic,
    headers: &HeaderMap,
    storage: &S,
) -> bool {
    verified_cookie_session(public, headers, storage)
        .is_some_and(|(_, _, _, _, feed_access)| feed_access)
}

fn verified_cookie_session<S: Storage>(
    public: &OAuthPublic,
    headers: &HeaderMap,
    storage: &S,
) -> Option<(String, String, u64, bool, bool)> {
    let (did, session_id, generation, write_access, feed_access) =
        cookie_value(headers).and_then(|value| public.verify_session_cookie(value))?;
    (storage
        .web_session_generation(&did, &session_id)
        .ok()
        .flatten()
        == Some(generation))
    .then_some((did, session_id, generation, write_access, feed_access))
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
    let avatar = feed_avatar_markup(feed);
    format!(
        "<li class=\"feed-item\"><a class=\"feed-card\" href=\"{href}\">{avatar}<h2>{}</h2><p>{}</p><code>{}</code></a></li>",
        html_escape(&feed.display_name),
        html_escape(feed.description.as_deref().unwrap_or("")),
        html_escape(&feed.uri),
    )
}

fn render_feed_heading(feed: &ResolvedFeed) -> String {
    let href = feed_link_href(feed);
    let avatar = feed_avatar_markup(feed);
    format!(
        "<header class=\"page-heading feed-heading\">{avatar}<p class=\"eyebrow\"><a href=\"/feeds\">Feeds</a></p><h1>{}</h1><p class=\"page-subtitle\">{}</p><code>{}</code><a class=\"pagination-link\" href=\"{href}\">Feed address</a></header>",
        html_escape(&feed.display_name),
        html_escape(feed.description.as_deref().unwrap_or("")),
        html_escape(&feed.uri),
    )
}

fn feed_link_href(feed: &ResolvedFeed) -> String {
    format!("/app/feed?uri={}", html_escape(&form_encoded(&feed.uri)))
}

fn form_encoded(value: &str) -> String {
    url::form_urlencoded::byte_serialize(value.as_bytes()).collect()
}

fn feed_avatar_markup(feed: &ResolvedFeed) -> String {
    let Some(avatar) = feed.avatar.as_deref() else {
        return String::new();
    };
    let Ok(url) = url::Url::parse(avatar) else {
        return String::new();
    };
    if url.scheme() != "https" || url.username() != "" || url.password().is_some() {
        return String::new();
    }
    format!(
        "<img class=\"feed-avatar\" src=\"{}\" alt=\"\" loading=\"lazy\">",
        html_escape(avatar)
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

fn avatar_markup(profile: &Value, class_name: &str) -> String {
    if let Some(avatar) = safe_avatar_url(profile) {
        format!(
            "<img class=\"{class_name}\" src=\"{}\" alt=\"\" loading=\"lazy\">",
            html_escape(avatar)
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

fn render_web_post<S: Storage>(
    storage: &S,
    record: &IndexedRecord,
    viewer_did: &str,
    write_forms_enabled: bool,
    link_text_to_thread: bool,
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
        avatar_markup(&profile, "avatar"),
        html_escape(&profile_uri),
        html_escape(display_name),
        html_escape(&profile_uri),
        html_escape(&handle),
    );
    let reply_context = record
        .record
        .pointer("/reply/parent/uri")
        .and_then(Value::as_str)
        .and_then(|parent_uri| {
            storage
                .record_by_uri("app.bsky.feed.post", parent_uri)
                .ok()
                .flatten()
        })
        .map(|parent| {
            let parent_profile = profile_view_from_storage(storage, &parent.did)
                .unwrap_or_else(|_| json!({"did":parent.did,"handle":parent.did}));
            let parent_handle = profile_handle(&parent_profile, &parent.did);
            let thread_uri = format!(
                "/thread?uri={}",
                url::form_urlencoded::byte_serialize(record.uri.as_bytes()).collect::<String>()
            );
            format!(
                "<p class=\"reply-context\"><a href=\"{}\">replying to @{}</a></p>",
                html_escape(&thread_uri),
                html_escape(&parent_handle)
            )
        })
        .unwrap_or_default();
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
    let text = if link_text_to_thread {
        let thread_uri = format!(
            "/thread?uri={}",
            url::form_urlencoded::byte_serialize(record.uri.as_bytes()).collect::<String>()
        );
        format!(
            "<a class=\"post-text-link\" href=\"{}\">{}</a>",
            html_escape(&thread_uri),
            html_escape(text)
        )
    } else {
        html_escape(text)
    };
    let interactions = storage
        .post_interaction_summary(&record.uri, Some(viewer_did))
        .unwrap_or_default();
    let counts = format!(
        "<ul class=\"post-counts\" aria-label=\"Post engagement\"><li>{} {}</li><li>{} {}</li><li>{} {}</li></ul>",
        interactions.like_count,
        if interactions.like_count == 1 {
            "like"
        } else {
            "likes"
        },
        interactions.repost_count,
        if interactions.repost_count == 1 {
            "repost"
        } else {
            "reposts"
        },
        interactions.reply_count,
        if interactions.reply_count == 1 {
            "reply"
        } else {
            "replies"
        },
    );
    let thread_uri = format!(
        "/thread?uri={}",
        url::form_urlencoded::byte_serialize(record.uri.as_bytes()).collect::<String>()
    );
    let reply_action = format!(
        "<a class=\"post-action\" href=\"{}\" aria-label=\"Open replies\"><span>Reply</span><span class=\"action-count\">{}</span></a>",
        html_escape(&thread_uri),
        interactions.reply_count
    );
    let actions = if write_forms_enabled {
        let uri = html_escape(&record.uri);
        let cid = html_escape(record.cid.as_deref().unwrap_or(""));
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
        format!(
            "{reply_action}<form class=\"post-action-form\" action=\"/like\" method=\"post\"><input type=\"hidden\" name=\"uri\" value=\"{uri}\"><input type=\"hidden\" name=\"cid\" value=\"{cid}\">{like_intent}<button class=\"post-action\" type=\"submit\" aria-label=\"{like_aria}\" aria-pressed=\"{}\">{like_label}</button></form><form class=\"post-action-form\" action=\"/repost\" method=\"post\"><input type=\"hidden\" name=\"uri\" value=\"{uri}\"><input type=\"hidden\" name=\"cid\" value=\"{cid}\"><button class=\"post-action\" type=\"submit\" aria-label=\"Repost by @{}\">{repost_label}</button></form>",
            if interactions.viewer_like.is_some() {
                "true"
            } else {
                "false"
            },
            html_escape(&handle)
        )
    } else {
        reply_action
    };
    let embed = record
        .record
        .get("embed")
        .map(|embed| render_web_embed(storage, &record.did, embed))
        .unwrap_or_default();
    format!(
        "<article class=\"post-card\"><header class=\"post-header\">{author}{time}</header>{reply_context}<p class=\"post-text\">{text}</p>{embed}{counts}<div class=\"post-actions\" aria-label=\"Post actions\">{actions}</div></article>"
    )
}

fn render_web_embed<S: Storage>(storage: &S, author_did: &str, embed: &Value) -> String {
    match embed.get("$type").and_then(Value::as_str) {
        Some("app.bsky.embed.images") => render_web_images(author_did, embed),
        Some("app.bsky.embed.external") => render_web_external(embed),
        Some("app.bsky.embed.record") => render_web_quote(storage, embed),
        Some("app.bsky.embed.recordWithMedia") => {
            let media = embed
                .get("media")
                .map(|media| render_web_embed(storage, author_did, media))
                .unwrap_or_default();
            let quote = embed
                .get("record")
                .map(|record| render_web_quote(storage, record))
                .unwrap_or_default();
            format!("<section class=\"record-with-media\">{media}{quote}</section>")
        }
        _ => String::new(),
    }
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

fn render_web_external(embed: &Value) -> String {
    let Some(external) = embed.get("external") else {
        return String::new();
    };
    let uri = external.get("uri").and_then(Value::as_str).unwrap_or("");
    let title = external.get("title").and_then(Value::as_str).unwrap_or(uri);
    let description = external
        .get("description")
        .and_then(Value::as_str)
        .unwrap_or("");
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
    format!(
        "<aside class=\"external-card\"><p>{title}</p><p>{}</p><p>{uri_markup}</p></aside>",
        html_escape(description)
    )
}

fn safe_external_url(uri: &str) -> bool {
    url::Url::parse(uri).is_ok_and(|url| matches!(url.scheme(), "http" | "https"))
}

fn render_web_quote<S: Storage>(storage: &S, embed: &Value) -> String {
    let Some(reference) = embed
        .pointer("/record/record")
        .or_else(|| embed.get("record"))
    else {
        return String::new();
    };
    let uri = reference.get("uri").and_then(Value::as_str).unwrap_or("");
    let quoted = if uri.is_empty() {
        None
    } else {
        storage
            .record_by_uri("app.bsky.feed.post", uri)
            .ok()
            .flatten()
    };
    if let Some(quoted) = quoted {
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
        let profile_uri = format!(
            "/profile?did={}",
            url::form_urlencoded::byte_serialize(quoted.did.as_bytes()).collect::<String>()
        );
        format!(
            "<blockquote class=\"quoted-post\"><p><a href=\"{}\">{} @{}</a></p><p>{}</p></blockquote>",
            html_escape(&profile_uri),
            html_escape(display_name),
            html_escape(&handle),
            html_escape(text)
        )
    } else {
        let text = reference
            .pointer("/value/text")
            .or_else(|| reference.get("text"))
            .and_then(Value::as_str)
            .unwrap_or(uri);
        if text.is_empty() {
            String::new()
        } else {
            format!(
                "<blockquote class=\"quoted-post\"><p>{}</p></blockquote>",
                html_escape(text)
            )
        }
    }
}

fn has_atproto_scope(scope: Option<&str>) -> bool {
    scope.is_some_and(|scope| scope.split_whitespace().any(|value| value == "atproto"))
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

fn requested_scopes() -> Vec<Scope> {
    std::iter::once(Scope::Known(KnownScope::Atproto))
        .chain(
            WRITE_SCOPES
                .iter()
                .map(|scope| Scope::Unknown((*scope).to_owned())),
        )
        .chain(
            FEED_SCOPES
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
