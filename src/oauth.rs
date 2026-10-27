use crate::ingest::{MemoryStorage, Storage};
use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    response::{Html, IntoResponse},
    routing::{get, post},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use jose_jwk::{Ec, EcCurves, Jwk, Key, Parameters};
use p256::{SecretKey, elliptic_curve::sec1::ToEncodedPoint};
use rand::thread_rng;
use serde_json::{Value, json};
use std::{collections::HashMap, fs, future::Future, io::Write, path::Path, pin::Pin, sync::Arc};
use thiserror::Error;

const METADATA_PATH: &str = "/.well-known/oauth-client-metadata.json";
const CALLBACK_PATH: &str = "/oauth/callback";

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
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OAuthAccount {
    pub did: String,
    pub verified_handle: Option<String>,
}

pub trait OAuthFlow: Clone + Send + Sync + 'static {
    fn authorize<'a>(
        &'a self,
        login: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<String, String>> + Send + 'a>>;
    fn callback<'a>(
        &'a self,
        params: OAuthCallback,
    ) -> Pin<Box<dyn Future<Output = Result<OAuthAccount, String>> + Send + 'a>>;
    fn logout<'a>(
        &'a self,
        did: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>>;
}

#[derive(Clone)]
pub struct DisabledOAuthFlow;

impl OAuthFlow for DisabledOAuthFlow {
    fn authorize<'a>(
        &'a self,
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
) -> impl IntoResponse
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
        );
    }
    let _ = &state.flow;
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({"error":"temporarily_unavailable","message":"OAuth flow is not configured"})),
    )
}

async fn callback<F, S>(
    State(state): State<Arc<OAuthWebState<F, S>>>,
    Query(query): Query<HashMap<String, String>>,
) -> impl IntoResponse
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
        );
    }
    let _ = &state.storage;
    (
        StatusCode::SERVICE_UNAVAILABLE,
        Json(json!({"error":"temporarily_unavailable","message":"OAuth flow is not configured"})),
    )
}

async fn logout<F, S>(State(state): State<Arc<OAuthWebState<F, S>>>) -> impl IntoResponse
where
    F: OAuthFlow,
    S: Storage,
{
    let _ = (&state.flow, &state.storage, &state.public);
    StatusCode::SERVICE_UNAVAILABLE
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

#[cfg(not(unix))]
fn create_key_file(path: &Path) -> Result<fs::File, std::io::Error> {
    fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
}
