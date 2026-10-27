use reqwest::{Client, Url};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    future::Future,
    sync::Arc,
    time::{Duration, Instant},
};
use thiserror::Error;
use tokio::sync::RwLock;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct DidDocument {
    pub id: String,
    #[serde(default, rename = "alsoKnownAs")]
    pub also_known_as: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Identity {
    pub did: String,
    /// Only set when the handle's DNS/HTTPS assertion and DID document agree.
    pub handle: Option<String>,
}

#[derive(Debug, Error)]
pub enum ResolutionError {
    #[error("invalid handle: {0}")]
    InvalidHandle(String),
    #[error("unsupported DID: {0}")]
    UnsupportedDid(String),
    #[error("DID document was not found: {0}")]
    DidNotFound(String),
    #[error("handle is not bidirectionally verified: {0}")]
    UnverifiedHandle(String),
    #[error("identity backend failed: {0}")]
    Backend(String),
}

/// Network boundary for identity resolution. Tests provide a local implementation.
pub trait IdentityBackend: Send + Sync {
    fn did_document(
        &self,
        did: &str,
    ) -> impl Future<Output = Result<Option<DidDocument>, ResolutionError>> + Send;
    fn dns_txt_did(
        &self,
        handle: &str,
    ) -> impl Future<Output = Result<Option<String>, ResolutionError>> + Send;
    fn well_known_did(
        &self,
        handle: &str,
    ) -> impl Future<Output = Result<Option<String>, ResolutionError>> + Send;
}

#[derive(Clone)]
struct CachedIdentity {
    expires_at: Instant,
    identity: Identity,
}

pub struct IdentityResolver<B> {
    backend: Arc<B>,
    ttl: Duration,
    cache: RwLock<HashMap<String, CachedIdentity>>,
}

impl<B: IdentityBackend> IdentityResolver<B> {
    pub fn new(backend: B, ttl: Duration) -> Self {
        Self {
            backend: Arc::new(backend),
            ttl,
            cache: RwLock::new(HashMap::new()),
        }
    }

    pub async fn resolve_handle(&self, handle: &str) -> Result<Identity, ResolutionError> {
        let handle = normalize_handle(handle)?;
        let key = format!("handle:{handle}");
        if let Some(identity) = self.cached(&key).await {
            return Ok(identity);
        }

        let did = self.resolve_handle_assertion(&handle).await?;
        let doc = self
            .backend
            .did_document(&did)
            .await?
            .ok_or_else(|| ResolutionError::DidNotFound(did.clone()))?;
        if doc.id != did
            || !doc
                .also_known_as
                .iter()
                .any(|uri| uri == &format!("at://{handle}"))
        {
            return Err(ResolutionError::UnverifiedHandle(handle));
        }

        let identity = Identity {
            did,
            handle: Some(handle),
        };
        self.insert(key, identity.clone()).await;
        Ok(identity)
    }

    pub async fn resolve_did(&self, did: &str) -> Result<Identity, ResolutionError> {
        validate_did(did)?;
        let key = format!("did:{did}");
        if let Some(identity) = self.cached(&key).await {
            return Ok(identity);
        }

        let doc = self
            .backend
            .did_document(did)
            .await?
            .ok_or_else(|| ResolutionError::DidNotFound(did.to_owned()))?;
        if doc.id != did {
            return Err(ResolutionError::Backend(format!(
                "DID document id {} did not match requested {did}",
                doc.id
            )));
        }

        // AT Protocol uses the first syntactically valid handle claim in order.
        let mut claimed_handle = None;
        for alias in &doc.also_known_as {
            if let Some(candidate) = alias.strip_prefix("at://")
                && let Ok(handle) = normalize_handle(candidate)
            {
                claimed_handle = Some(handle);
                break;
            }
        }

        let verified_handle = if let Some(handle) = claimed_handle {
            match self.resolve_handle_assertion(&handle).await {
                Ok(asserted_did) if asserted_did == did => Some(handle),
                _ => None,
            }
        } else {
            None
        };
        let identity = Identity {
            did: did.to_owned(),
            handle: verified_handle,
        };
        self.insert(key, identity.clone()).await;
        Ok(identity)
    }

    async fn resolve_handle_assertion(&self, handle: &str) -> Result<String, ResolutionError> {
        // DNS TXT is preferred when present; HTTPS is the compatibility fallback.
        if let Some(did) = self.backend.dns_txt_did(handle).await?
            && is_supported_did(&did)
        {
            return Ok(did);
        }
        if let Some(did) = self.backend.well_known_did(handle).await?
            && is_supported_did(&did)
        {
            return Ok(did);
        }
        Err(ResolutionError::UnverifiedHandle(handle.to_owned()))
    }

    async fn cached(&self, key: &str) -> Option<Identity> {
        let now = Instant::now();
        self.cache
            .read()
            .await
            .get(key)
            .filter(|entry| entry.expires_at > now)
            .map(|entry| entry.identity.clone())
    }

    async fn insert(&self, key: String, identity: Identity) {
        self.cache.write().await.insert(
            key,
            CachedIdentity {
                expires_at: Instant::now() + self.ttl,
                identity,
            },
        );
    }
}

fn normalize_handle(handle: &str) -> Result<String, ResolutionError> {
    let handle = handle.trim_end_matches('.').to_ascii_lowercase();
    let valid = !handle.is_empty()
        && handle.len() <= 244
        && handle.split('.').all(|label| {
            !label.is_empty()
                && label.len() <= 63
                && label.as_bytes()[0].is_ascii_alphanumeric()
                && label
                    .as_bytes()
                    .last()
                    .is_some_and(u8::is_ascii_alphanumeric)
                && label
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        });
    if valid {
        Ok(handle)
    } else {
        Err(ResolutionError::InvalidHandle(handle))
    }
}

fn validate_did(did: &str) -> Result<(), ResolutionError> {
    if is_supported_did(did) {
        Ok(())
    } else {
        Err(ResolutionError::UnsupportedDid(did.to_owned()))
    }
}

fn is_supported_did(did: &str) -> bool {
    if did.starts_with("did:plc:") {
        let id = did.trim_start_matches("did:plc:");
        !id.is_empty() && id.bytes().all(|byte| byte.is_ascii_alphanumeric())
    } else if did.starts_with("did:web:") {
        let host = did.trim_start_matches("did:web:");
        !host.is_empty()
            && !host.contains('/')
            && !host.contains('?')
            && !host.contains('#')
            && !host.contains('@')
    } else {
        false
    }
}

#[derive(Clone)]
pub struct HttpIdentityBackend {
    client: Client,
    plc_directory: Url,
    dns_over_https: Url,
}

impl HttpIdentityBackend {
    pub fn new(plc_directory: &str, dns_over_https: &str) -> Result<Self, ResolutionError> {
        let client = Client::builder()
            .timeout(Duration::from_secs(10))
            .redirect(reqwest::redirect::Policy::limited(5))
            .build()
            .map_err(backend_error)?;
        let mut plc_directory = Url::parse(plc_directory).map_err(backend_error)?;
        if !plc_directory.path().ends_with('/') {
            plc_directory.set_path(&format!("{}/", plc_directory.path()));
        }
        let dns_over_https = Url::parse(dns_over_https).map_err(backend_error)?;
        Ok(Self {
            client,
            plc_directory,
            dns_over_https,
        })
    }
}

impl Default for HttpIdentityBackend {
    fn default() -> Self {
        Self::new(
            "https://plc.directory/",
            "https://cloudflare-dns.com/dns-query",
        )
        .expect("built-in identity service URLs are valid")
    }
}

#[derive(Deserialize)]
struct DohResponse {
    #[serde(default, rename = "Answer")]
    answers: Vec<DohAnswer>,
}

#[derive(Deserialize)]
struct DohAnswer {
    data: String,
}

impl IdentityBackend for HttpIdentityBackend {
    async fn did_document(&self, did: &str) -> Result<Option<DidDocument>, ResolutionError> {
        let url = if did.starts_with("did:plc:") {
            self.plc_directory.join(did).map_err(backend_error)?
        } else if let Some(host) = did.strip_prefix("did:web:") {
            let host = host.replace("%3A", ":").replace("%3a", ":");
            if host.contains(':') && !host.starts_with("localhost:") {
                return Err(ResolutionError::UnsupportedDid(did.to_owned()));
            }
            let scheme = if host == "localhost" || host.starts_with("localhost:") {
                "http"
            } else {
                "https"
            };
            Url::parse(&format!("{scheme}://{host}/.well-known/did.json")).map_err(backend_error)?
        } else {
            return Err(ResolutionError::UnsupportedDid(did.to_owned()));
        };

        let response = self.client.get(url).send().await.map_err(backend_error)?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let document = response
            .error_for_status()
            .map_err(backend_error)?
            .json::<DidDocument>()
            .await
            .map_err(backend_error)?;
        Ok(Some(document))
    }

    async fn dns_txt_did(&self, handle: &str) -> Result<Option<String>, ResolutionError> {
        let mut url = self.dns_over_https.clone();
        url.query_pairs_mut()
            .append_pair("name", &format!("_atproto.{handle}"))
            .append_pair("type", "TXT");
        let response = self
            .client
            .get(url)
            .header(reqwest::header::ACCEPT, "application/dns-json")
            .send()
            .await
            .map_err(backend_error)?
            .error_for_status()
            .map_err(backend_error)?
            .json::<DohResponse>()
            .await
            .map_err(backend_error)?;

        for answer in response.answers {
            let record = answer.data.trim_matches('"').replace("\\\"", "\"");
            if let Some(did) = record.strip_prefix("did=") {
                return Ok(Some(did.to_owned()));
            }
        }
        Ok(None)
    }

    async fn well_known_did(&self, handle: &str) -> Result<Option<String>, ResolutionError> {
        let scheme = if handle == "localhost" || handle.starts_with("localhost:") {
            "http"
        } else {
            "https"
        };
        let url = Url::parse(&format!("{scheme}://{handle}/.well-known/atproto-did"))
            .map_err(backend_error)?;
        let response = self.client.get(url).send().await.map_err(backend_error)?;
        if response.status() == reqwest::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        let body = response
            .error_for_status()
            .map_err(backend_error)?
            .text()
            .await
            .map_err(backend_error)?;
        let did = body.trim();
        if is_supported_did(did) {
            Ok(Some(did.to_owned()))
        } else {
            Ok(None)
        }
    }
}

fn backend_error(error: impl std::fmt::Display) -> ResolutionError {
    ResolutionError::Backend(error.to_string())
}
