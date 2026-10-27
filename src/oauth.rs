use crate::identity::{HttpIdentityBackend, IdentityResolver};
use crate::ingest::{MemoryStorage, Storage};
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
    AtprotoClientMetadata, AuthMethod, AuthorizeOptions, DefaultHttpClient, GrantType, KnownScope,
    OAuthClient, OAuthClientConfig, OAuthResolverConfig, Scope,
    store::{
        session::{Session, SessionStore},
        state::{InternalStateData, StateStore},
    },
};
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
use std::time::{SystemTime, UNIX_EPOCH};
use std::{
    collections::HashMap,
    fs,
    future::Future,
    io::Write,
    path::Path,
    pin::Pin,
    sync::{Arc, Mutex},
};
use thiserror::Error;

const METADATA_PATH: &str = "/.well-known/oauth-client-metadata.json";
const CALLBACK_PATH: &str = "/oauth/callback";
const SESSION_COOKIE: &str = "appview_session";
const LOGIN_COOKIE: &str = "appview_login";
const SESSION_TTL_SECONDS: u64 = 30 * 24 * 60 * 60;
const LOGIN_COOKIE_TTL_SECONDS: u64 = 10 * 60;
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
}

#[derive(Clone)]
pub struct OAuthPublic {
    public_url: String,
    private_key: Jwk,
    metadata: Value,
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
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async { Err("post action is not available".to_owned()) })
    }
    fn like<'a>(
        &'a self,
        _did: &'a str,
        _uri: &'a str,
        _cid: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async { Err("like action is not available".to_owned()) })
    }
    fn repost<'a>(
        &'a self,
        _did: &'a str,
        _uri: &'a str,
        _cid: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async { Err("repost action is not available".to_owned()) })
    }
    fn timeline<'a>(
        &'a self,
        _did: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Value>, String>> + Send + 'a>> {
        Box::pin(async { Err("timeline is not available".to_owned()) })
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
        let key_path = key_path.as_ref();
        let key = match fs::read(key_path) {
            Ok(contents) => serde_json::from_slice(&contents)?,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                if let Some(parent) = key_path.parent() {
                    fs::create_dir_all(parent)?;
                }
                let key = create_key();
                let contents = serde_json::to_vec(&key)?;
                match create_key_file(key_path) {
                    Ok(mut file) => file.write_all(&contents)?,
                    Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                        return Self::load_or_create(public_url, key_path);
                    }
                    Err(error) => return Err(error.into()),
                }
                key
            }
            Err(error) => return Err(error.into()),
        };
        Self::from_key(public_url, key)
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
        AtriumOAuthFlow::new(self, identity_resolver, store, DEFAULT_PLC_DIRECTORY_URL)
    }

    /// Construct the production flow against an injectable PLC directory.
    /// This allows offline tests to use a local fake PDS and authorization server.
    pub fn atrium_flow_with_plc_directory<B: crate::identity::IdentityBackend + 'static>(
        &self,
        identity_resolver: Arc<IdentityResolver<B>>,
        store: SqliteOAuthStore,
        plc_directory_url: &str,
    ) -> Result<AtriumOAuthFlow<B>, String> {
        AtriumOAuthFlow::new(self, identity_resolver, store, plc_directory_url)
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
            .route("/login", get(login))
            .route("/oauth/authorize", get(authorize::<F, S>))
            .route("/oauth/callback", get(callback::<F, S>))
            .route("/logout", post(logout::<F, S>))
            .route("/app", get(app_home::<F, S>))
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
        if !matches!(
            &private_key.key,
            Key::Ec(Ec {
                crv: EcCurves::P256,
                d: Some(_),
                ..
            })
        ) {
            return Err(OAuthConfigError::InvalidPrivateKey);
        }
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
            "scope": "atproto",
            "token_endpoint_auth_method": "private_key_jwt",
            "token_endpoint_auth_signing_alg": "ES256",
            "dpop_bound_access_tokens": true,
            "jwks": {"keys": [public_key]}
        });
        Ok(Self {
            public_url: public_url.to_owned(),
            private_key,
            metadata,
        })
    }

    fn issue_session_cookie(&self, did: &str, generation: u64) -> String {
        let expires = unix_time().saturating_add(SESSION_TTL_SECONDS);
        let payload = format!("{did}\n{expires}\n{generation}");
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

    fn verify_session_cookie(&self, token: &str) -> Option<(String, u64)> {
        let (payload, signature) = token.split_once('.')?;
        let signature = URL_SAFE_NO_PAD.decode(signature).ok()?;
        let mut mac = HmacSha256::new_from_slice(&self.cookie_key()).ok()?;
        mac.update(b"session:");
        mac.update(payload.as_bytes());
        mac.verify_slice(&signature).ok()?;
        let decoded = URL_SAFE_NO_PAD.decode(payload).ok()?;
        let value = std::str::from_utf8(&decoded).ok()?;
        let (did, session) = value.split_once('\n')?;
        let (expires, generation) = session.split_once('\n')?;
        if expires.parse::<u64>().ok()? <= unix_time()
            || !(did.starts_with("did:plc:") || did.starts_with("did:web:"))
        {
            return None;
        }
        Some((did.to_owned(), generation.parse().ok()?))
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

type OAuthDidResolver = CommonDidResolver<DefaultHttpClient>;
type OAuthHandleResolver = AtprotoHandleResolver<CloudflareDnsTxtResolver, DefaultHttpClient>;
type AppOAuthClient =
    OAuthClient<SqliteOAuthStore, SqliteOAuthStore, OAuthDidResolver, OAuthHandleResolver>;

pub struct AtriumOAuthFlow<B: crate::identity::IdentityBackend = HttpIdentityBackend> {
    client: Arc<AppOAuthClient>,
    identity_resolver: Arc<IdentityResolver<B>>,
    store: SqliteOAuthStore,
}

impl<B: crate::identity::IdentityBackend> Clone for AtriumOAuthFlow<B> {
    fn clone(&self) -> Self {
        Self {
            client: self.client.clone(),
            identity_resolver: self.identity_resolver.clone(),
            store: self.store.clone(),
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
    ) -> Result<Self, String> {
        let http_client = Arc::new(DefaultHttpClient::default());
        let did_resolver = CommonDidResolver::new(CommonDidResolverConfig {
            plc_directory_url: plc_directory_url.to_owned(),
            http_client: http_client.clone(),
        });
        let handle_resolver = AtprotoHandleResolver::new(AtprotoHandleResolverConfig {
            dns_txt_resolver: CloudflareDnsTxtResolver::default(),
            http_client,
        });
        let metadata = AtprotoClientMetadata {
            client_id: public.metadata["client_id"]
                .as_str()
                .ok_or_else(|| "OAuth client metadata has no client_id".to_owned())?
                .to_owned(),
            client_uri: Some(public.public_url.clone()),
            redirect_uris: vec![
                public.metadata["redirect_uris"][0]
                    .as_str()
                    .ok_or_else(|| "OAuth client metadata has no redirect URI".to_owned())?
                    .to_owned(),
            ],
            token_endpoint_auth_method: AuthMethod::PrivateKeyJwt,
            grant_types: vec![GrantType::AuthorizationCode, GrantType::RefreshToken],
            scopes: vec![Scope::Known(KnownScope::Atproto)],
            jwks_uri: None,
            token_endpoint_auth_signing_alg: Some("ES256".to_owned()),
        };
        let client = OAuthClient::new(OAuthClientConfig {
            client_metadata: metadata,
            keys: Some(vec![public.private_key.clone()]),
            state_store: store.clone(),
            session_store: store.clone(),
            resolver: OAuthResolverConfig {
                did_resolver,
                handle_resolver,
                authorization_server_metadata: Default::default(),
                protected_resource_metadata: Default::default(),
            },
        })
        .map_err(|error| error.to_string())?;
        Ok(Self {
            client: Arc::new(client),
            identity_resolver,
            store,
        })
    }

    async fn create_record(
        &self,
        did: &str,
        collection: &str,
        record: Value,
    ) -> Result<(), String> {
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
        agent
            .api
            .com
            .atproto
            .repo
            .create_record(input)
            .await
            .map(|_| ())
            .map_err(|error| format!("PDS createRecord failed: {error:?}"))
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
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
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
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
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

    fn repost<'a>(
        &'a self,
        did: &'a str,
        uri: &'a str,
        cid: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
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
}

#[derive(Clone, Default)]
struct CloudflareDnsTxtResolver {
    client: reqwest::Client,
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
        let response = self
            .client
            .get("https://cloudflare-dns.com/dns-query")
            .query(&[("name", query), ("type", "TXT")])
            .header(reqwest::header::ACCEPT, "application/dns-json")
            .send()
            .await?
            .error_for_status()?
            .json::<DohResponse>()
            .await?;
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
    S: Storage,
{
    Json(state.public.metadata.clone())
}

async fn login() -> Html<&'static str> {
    Html(
        "<!doctype html><html><head><title>Sign in</title></head><body><main><h1>Sign in to your AppView</h1><form action=\"/oauth/authorize\" method=\"get\"><label>AT Protocol handle or DID <input name=\"handle\" autocomplete=\"username\" required></label><button type=\"submit\">Continue</button></form></main></body></html>",
    )
}

async fn authorize<F, S>(
    State(state): State<Arc<OAuthWebState<F, S>>>,
    Query(query): Query<HashMap<String, String>>,
) -> Response
where
    F: OAuthFlow,
    S: Storage,
{
    if query
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
    match state
        .flow
        .authorize(
            query.get("handle").expect("validated above"),
            &browser_binding,
        )
        .await
    {
        Ok(url) if valid_redirect_url(&url) => {
            let mut response = Redirect::to(&url).into_response();
            let cookie = format!(
                "{LOGIN_COOKIE}={}; Path=/; HttpOnly; SameSite=Lax; Max-Age={LOGIN_COOKIE_TTL_SECONDS}{}",
                state.public.issue_login_cookie(&browser_binding),
                if state.public.public_url.starts_with("https://") {
                    "; Secure"
                } else {
                    ""
                },
            );
            response.headers_mut().insert(
                header::SET_COOKIE,
                HeaderValue::from_str(&cookie).expect("login cookie token is header safe"),
            );
            response
        }
        Ok(_) => oauth_error(
            StatusCode::BAD_GATEWAY,
            "invalid authorization endpoint URL",
        ),
        Err(error) => oauth_error(StatusCode::BAD_REQUEST, &error),
    }
}

async fn callback<F, S>(
    State(state): State<Arc<OAuthWebState<F, S>>>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Response
where
    F: OAuthFlow,
    S: Storage,
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
    let callback_result = std::panic::AssertUnwindSafe(state.flow.callback(params))
        .catch_unwind()
        .await;
    let account = match callback_result {
        Ok(Ok(account)) => account,
        Ok(Err(error)) => return oauth_error(StatusCode::BAD_REQUEST, &error),
        Err(_) => {
            return oauth_error_page(
                StatusCode::BAD_GATEWAY,
                "Sign-in could not be completed. Please start again.",
            );
        }
    };
    if !has_atproto_scope(account.granted_scope.as_deref()) {
        return oauth_error(
            StatusCode::BAD_REQUEST,
            "OAuth token response must grant the atproto scope",
        );
    }
    if let Err(error) = state.storage.register_user(&account.did) {
        return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string());
    }
    if let Some(handle) = account.verified_handle.as_deref()
        && let Err(error) = state.storage.set_verified_handle(&account.did, handle)
    {
        return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string());
    }
    let generation = match state.storage.rotate_web_session(&account.did) {
        Ok(generation) => generation,
        Err(error) => return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    let mut response = Redirect::to("/app").into_response();
    let cookie = format!(
        "{SESSION_COOKIE}={}; Path=/; HttpOnly; SameSite=Lax; Max-Age={SESSION_TTL_SECONDS}{}",
        state.public.issue_session_cookie(&account.did, generation),
        if state.public.public_url.starts_with("https://") {
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
    S: Storage,
{
    let Some(did) = verified_cookie_did(&state.public, &headers, state.storage.as_ref()) else {
        return clear_session_cookie(Redirect::to("/login").into_response());
    };
    if let Err(error) = state.storage.rotate_web_session(&did) {
        return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string());
    }
    let remote_result = state.flow.logout(&did).await;
    if let Err(error) = state.storage.unregister_user(&did) {
        return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string());
    }
    let response = clear_session_cookie(Redirect::to("/login").into_response());
    if remote_result.is_err() {
        return response;
    }
    response
}

async fn app_home<F, S>(
    State(state): State<Arc<OAuthWebState<F, S>>>,
    headers: HeaderMap,
) -> Response
where
    F: OAuthFlow,
    S: Storage,
{
    let Some(did) = verified_cookie_did(&state.public, &headers, state.storage.as_ref()) else {
        return Redirect::to("/login").into_response();
    };
    let timeline = match state.flow.timeline(&did).await {
        Ok(timeline) => timeline,
        Err(error) => return oauth_error(StatusCode::BAD_GATEWAY, &error),
    };
    let posts = timeline
        .iter()
        .map(|item| {
            let post = item.get("post").unwrap_or(item);
            let uri = post.get("uri").and_then(Value::as_str).unwrap_or("");
            let cid = post.get("cid").and_then(Value::as_str).unwrap_or("");
            let text = post
                .pointer("/record/text")
                .and_then(Value::as_str)
                .unwrap_or("");
            format!(
                "<li><a href=\"/thread?uri={}\">{}</a><form action=\"/like\" method=\"post\"><input type=\"hidden\" name=\"uri\" value=\"{}\"><input type=\"hidden\" name=\"cid\" value=\"{}\"><button>Like</button></form><form action=\"/repost\" method=\"post\"><input type=\"hidden\" name=\"uri\" value=\"{}\"><input type=\"hidden\" name=\"cid\" value=\"{}\"><button>Repost</button></form></li>",
                html_escape(uri), html_escape(text), html_escape(uri), html_escape(cid),
                html_escape(uri), html_escape(cid)
            )
        })
        .collect::<String>();
    Html(format!(
        "<!doctype html><html><head><title>AppView</title></head><body><main><h1>Timeline for {}</h1><nav><a href=\"/profile?did={}\">Profile</a> <form action=\"/logout\" method=\"post\"><button>Log out</button></form></nav><ol>{posts}</ol><form action=\"/post\" method=\"post\"><textarea name=\"text\" required></textarea><button>Post</button></form></main></body></html>",
        html_escape(&did), html_escape(&did)
    ))
    .into_response()
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
    if subject != did {
        return oauth_error(
            StatusCode::FORBIDDEN,
            "profiles are limited to the logged-in account",
        );
    }
    let handle = state
        .storage
        .verified_handle(&did)
        .ok()
        .flatten()
        .unwrap_or_else(|| "handle.invalid".to_owned());
    let record = state
        .storage
        .get_record(&did, "app.bsky.actor.profile", "self")
        .ok()
        .flatten();
    let display_name = record
        .as_ref()
        .and_then(|record| record.record.get("displayName"))
        .and_then(Value::as_str)
        .unwrap_or(&handle);
    Html(format!(
        "<!doctype html><html><head><title>Profile</title></head><body><main><h1>{}</h1><p>{}</p><a href=\"/app\">Timeline</a></main></body></html>",
        html_escape(display_name), html_escape(&handle)
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
    if verified_cookie_did(&state.public, &headers, state.storage.as_ref()).is_none() {
        return Redirect::to("/login").into_response();
    }
    let Some(uri) = query.get("uri") else {
        return oauth_error(StatusCode::BAD_REQUEST, "uri is required");
    };
    let record = match state.storage.records() {
        Ok(records) => records.into_iter().find(|record| record.uri == *uri),
        Err(error) => return oauth_error(StatusCode::INTERNAL_SERVER_ERROR, &error.to_string()),
    };
    let Some(record) = record else {
        return oauth_error(StatusCode::NOT_FOUND, "post not found");
    };
    let text = record
        .record
        .get("text")
        .and_then(Value::as_str)
        .unwrap_or("");
    Html(format!(
        "<!doctype html><html><head><title>Post</title></head><body><main><p>{}</p><form action=\"/like\" method=\"post\"><input type=\"hidden\" name=\"uri\" value=\"{}\"><input type=\"hidden\" name=\"cid\" value=\"{}\"><button>Like</button></form><form action=\"/repost\" method=\"post\"><input type=\"hidden\" name=\"uri\" value=\"{}\"><input type=\"hidden\" name=\"cid\" value=\"{}\"><button>Repost</button></form><a href=\"/app\">Timeline</a></main></body></html>",
        html_escape(text), html_escape(&record.uri), html_escape(record.cid.as_deref().unwrap_or("")),
        html_escape(&record.uri), html_escape(record.cid.as_deref().unwrap_or(""))
    ))
    .into_response()
}

async fn write_post<F, S>(
    State(state): State<Arc<OAuthWebState<F, S>>>,
    headers: HeaderMap,
    Form(form): Form<HashMap<String, String>>,
) -> Response
where
    F: OAuthFlow,
    S: Storage,
{
    let Some(did) = verified_cookie_did(&state.public, &headers, state.storage.as_ref()) else {
        return Redirect::to("/login").into_response();
    };
    let text = form.get("text").map(String::as_str).unwrap_or("").trim();
    if text.is_empty() || text.chars().count() > 300 {
        return oauth_error(
            StatusCode::BAD_REQUEST,
            "post text must contain 1 to 300 characters",
        );
    }
    match state.flow.create_post(&did, text).await {
        Ok(()) => Redirect::to("/app").into_response(),
        Err(error) => oauth_error(StatusCode::BAD_GATEWAY, &error),
    }
}

async fn write_like<F, S>(
    State(state): State<Arc<OAuthWebState<F, S>>>,
    headers: HeaderMap,
    Form(form): Form<HashMap<String, String>>,
) -> Response
where
    F: OAuthFlow,
    S: Storage,
{
    let Some(did) = verified_cookie_did(&state.public, &headers, state.storage.as_ref()) else {
        return Redirect::to("/login").into_response();
    };
    let (Some(uri), Some(cid)) = (form.get("uri"), form.get("cid")) else {
        return oauth_error(StatusCode::BAD_REQUEST, "uri and cid are required");
    };
    if !uri.starts_with("at://") || cid.is_empty() {
        return oauth_error(StatusCode::BAD_REQUEST, "uri and cid are invalid");
    }
    match state.flow.like(&did, uri, cid).await {
        Ok(()) => Redirect::to("/app").into_response(),
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
    S: Storage,
{
    let Some(did) = verified_cookie_did(&state.public, &headers, state.storage.as_ref()) else {
        return Redirect::to("/login").into_response();
    };
    let (Some(uri), Some(cid)) = (form.get("uri"), form.get("cid")) else {
        return oauth_error(StatusCode::BAD_REQUEST, "uri and cid are required");
    };
    if !uri.starts_with("at://") || cid.is_empty() {
        return oauth_error(StatusCode::BAD_REQUEST, "uri and cid are invalid");
    }
    match state.flow.repost(&did, uri, cid).await {
        Ok(()) => Redirect::to("/app").into_response(),
        Err(error) => oauth_error(StatusCode::BAD_GATEWAY, &error),
    }
}

fn verified_cookie_did<S: Storage>(
    public: &OAuthPublic,
    headers: &HeaderMap,
    storage: &S,
) -> Option<String> {
    let (did, generation) =
        cookie_value(headers).and_then(|value| public.verify_session_cookie(value))?;
    (storage.web_session_generation(&did).ok().flatten() == Some(generation)).then_some(did)
}

fn cookie_value(headers: &HeaderMap) -> Option<&str> {
    cookie_value_named(headers, SESSION_COOKIE)
}

fn cookie_value_named<'a>(headers: &'a HeaderMap, expected_name: &str) -> Option<&'a str> {
    headers
        .get(header::COOKIE)?
        .to_str()
        .ok()?
        .split(';')
        .filter_map(|part| part.trim().split_once('='))
        .find_map(|(name, value)| (name == expected_name).then_some(value))
}

fn clear_login_cookie(public: &OAuthPublic) -> String {
    format!(
        "{LOGIN_COOKIE}=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0{}",
        if public.public_url.starts_with("https://") {
            "; Secure"
        } else {
            ""
        },
    )
}

fn clear_session_cookie(mut response: Response) -> Response {
    response.headers_mut().insert(
        header::SET_COOKIE,
        HeaderValue::from_static("appview_session=; Path=/; HttpOnly; SameSite=Lax; Max-Age=0"),
    );
    response
}

fn oauth_error(status: StatusCode, message: &str) -> Response {
    (
        status,
        Json(json!({"error":"OAuthError","message":message})),
    )
        .into_response()
}

fn oauth_error_page(status: StatusCode, message: &str) -> Response {
    (
        status,
        Html(format!(
            "<!doctype html><html><head><title>Sign-in error</title></head><body><main><h1>Sign-in error</h1><p>{}</p><a href=\"/login\">Try again</a></main></body></html>",
            html_escape(message)
        )),
    )
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
    value
        .replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
        .replace('\'', "&#39;")
}

fn has_atproto_scope(scope: Option<&str>) -> bool {
    scope.is_some_and(|scope| scope.split_whitespace().any(|value| value == "atproto"))
}

fn unix_time() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
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

    #[test]
    fn cookie_hmac_key_is_distinct_from_the_oauth_signing_scalar() {
        let public = OAuthPublic::new("https://appview.example").unwrap();
        let serialized = serde_json::to_value(public.private_key()).unwrap();
        let signing_scalar = URL_SAFE_NO_PAD
            .decode(serialized.get("d").and_then(Value::as_str).unwrap())
            .unwrap();
        assert_ne!(public.cookie_key(), signing_scalar);
    }
}

#[cfg(not(unix))]
fn create_key_file(path: &Path) -> Result<fs::File, std::io::Error> {
    fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
}
