use crate::{
    ingest::{Storage, StorageError},
    net::SafeHttpClient,
};
use http::{HeaderMap, HeaderValue, header};
use rusqlite::{Connection, OpenFlags, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    future::Future,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex as StdMutex,
        atomic::{AtomicU64, Ordering},
        mpsc::{Receiver, SyncSender, TrySendError, sync_channel},
    },
    thread,
    time::{Duration, Instant},
};
use thiserror::Error;
use tokio::sync::{Mutex, RwLock, watch};
use url::Url;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct DidDocument {
    pub id: String,
    #[serde(default, rename = "alsoKnownAs")]
    pub also_known_as: Vec<String>,
    #[serde(default, rename = "verificationMethod")]
    pub verification_method: Vec<VerificationMethod>,
    #[serde(default)]
    pub service: Vec<DidService>,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct VerificationMethod {
    pub id: String,
    #[serde(rename = "type")]
    pub type_name: String,
    pub controller: String,
    #[serde(rename = "publicKeyMultibase")]
    pub public_key_multibase: String,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct DidService {
    pub id: String,
    #[serde(rename = "type")]
    pub type_name: String,
    #[serde(rename = "serviceEndpoint")]
    pub service_endpoint: String,
}

impl DidDocument {
    /// Return the atproto signing key as a did:key identifier when the DID document
    /// has the standard Multikey verification method controlled by this DID.
    pub fn atproto_signing_key(&self) -> Option<String> {
        self.verification_method.iter().find_map(|method| {
            let key_id_matches =
                method.id == "#atproto" || method.id == format!("{}#atproto", self.id);
            (key_id_matches
                && method.type_name == "Multikey"
                && method.controller == self.id
                && method.public_key_multibase.starts_with('z'))
            .then(|| format!("did:key:{}", method.public_key_multibase))
        })
    }

    /// Return the validated atproto PDS service endpoint from this DID document.
    pub fn pds_endpoint(&self) -> Option<String> {
        self.service.iter().find_map(|service| {
            let service_id_matches =
                service.id == "#atproto_pds" || service.id == format!("{}#atproto_pds", self.id);
            if !service_id_matches || service.type_name != "AtprotoPersonalDataServer" {
                return None;
            }
            let url = Url::parse(&service.service_endpoint).ok()?;
            let local = matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "::1"));
            let valid_scheme = url.scheme() == "https" || (local && url.scheme() == "http");
            (valid_scheme
                && url.username().is_empty()
                && url.password().is_none()
                && url.path() == "/"
                && url.query().is_none()
                && url.fragment().is_none())
            .then(|| service.service_endpoint.trim_end_matches('/').to_owned())
        })
    }
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct Identity {
    pub did: String,
    /// Only set when the handle's DNS/HTTPS assertion and DID document agree.
    pub handle: Option<String>,
}

#[derive(Clone, Debug, Deserialize, Serialize, Error)]
#[serde(tag = "kind", content = "details")]
pub enum ResolutionError {
    #[error("invalid handle: {0}")]
    InvalidHandle(String),
    #[error("unsupported DID: {0}")]
    UnsupportedDid(String),
    #[error("DID document was not found: {0}")]
    DidNotFound(String),
    #[error("DID document id {actual} did not match requested DID {requested}")]
    DidDocumentMismatch { actual: String, requested: String },
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

const PERSISTED_IDENTITY_TTL: Duration = Duration::from_secs(24 * 60 * 60);
const PERSISTED_IDENTITY_FAILURE_TTL: Duration = Duration::from_secs(5 * 60);

#[derive(Clone, Debug, Deserialize, Serialize)]
struct PersistedIdentity {
    identity: Option<Identity>,
    document: Option<DidDocument>,
}

const IDENTITY_CACHE_WRITE_QUEUE: usize = 256;

struct MemoryIdentityEntry {
    did: String,
    expires_at: Instant,
    value: Result<PersistedIdentity, ResolutionError>,
}

#[derive(Default)]
struct IdentityCacheMemory {
    entries: HashMap<String, MemoryIdentityEntry>,
    invalidated_dids: HashMap<String, IdentityInvalidation>,
    active_sqlite_reads: usize,
}

#[derive(Clone, Copy)]
struct IdentityInvalidation {
    generation: u64,
    committed: bool,
}

struct ActiveIdentityCacheRead {
    memory: Arc<StdMutex<IdentityCacheMemory>>,
}

impl Drop for ActiveIdentityCacheRead {
    fn drop(&mut self) {
        if let Ok(mut memory) = self.memory.lock() {
            memory.active_sqlite_reads = memory.active_sqlite_reads.saturating_sub(1);
            if memory.active_sqlite_reads == 0 {
                memory
                    .invalidated_dids
                    .retain(|_, invalidation| !invalidation.committed);
            }
        }
    }
}

enum IdentityCacheWrite {
    Set {
        key: String,
        did: String,
        result_json: String,
        expires_at_ms: i64,
    },
    InvalidateDid {
        did: String,
        generation: u64,
    },
}

/// Small SQLite cache shared by production identity resolvers. SQLite writes run on one
/// background thread; callers read the shared in-memory tier first and then use a read-only
/// connection on Tokio's blocking pool.
#[derive(Clone)]
pub struct SqliteIdentityCache {
    path: Arc<PathBuf>,
    writer: SyncSender<IdentityCacheWrite>,
    memory: Arc<StdMutex<IdentityCacheMemory>>,
    next_invalidation: Arc<AtomicU64>,
    dropped_writes: Arc<AtomicU64>,
}

impl SqliteIdentityCache {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, rusqlite::Error> {
        let path = path.as_ref();
        if path != Path::new(":memory:")
            && let Some(parent) = path
                .parent()
                .filter(|parent| !parent.as_os_str().is_empty())
        {
            std::fs::create_dir_all(parent)
                .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
        }
        crate::ingest::ensure_private_database_file(path)
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;
        let connection = Connection::open(path)?;
        connection.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA busy_timeout = 5000;
             CREATE TABLE IF NOT EXISTS identity_resolution_cache (
                 cache_key TEXT PRIMARY KEY,
                 did TEXT NOT NULL,
                 result_json TEXT NOT NULL,
                 expires_at_ms INTEGER NOT NULL
             );
             CREATE INDEX IF NOT EXISTS identity_resolution_cache_did
                 ON identity_resolution_cache(did);",
        )?;

        let (writer, receiver) = sync_channel(IDENTITY_CACHE_WRITE_QUEUE);
        let memory = Arc::new(StdMutex::new(IdentityCacheMemory::default()));
        let writer_memory = memory.clone();
        thread::Builder::new()
            .name("identity-cache-writer".to_owned())
            .spawn(move || identity_cache_writer(connection, receiver, writer_memory))
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))?;

        Ok(Self {
            path: Arc::new(path.to_owned()),
            writer,
            memory,
            next_invalidation: Arc::new(AtomicU64::new(0)),
            dropped_writes: Arc::new(AtomicU64::new(0)),
        })
    }

    async fn get(
        &self,
        key: &str,
    ) -> Result<Option<Result<PersistedIdentity, ResolutionError>>, rusqlite::Error> {
        let use_sqlite = self.path.as_path() != Path::new(":memory:");
        {
            let mut memory = self.memory.lock().map_err(|_| {
                rusqlite::Error::InvalidParameterName(
                    "identity cache memory lock poisoned".to_owned(),
                )
            })?;
            if let Some(entry) = memory.entries.get(key) {
                if entry.expires_at > Instant::now() {
                    return Ok(Some(entry.value.clone()));
                }
                memory.entries.remove(key);
            }
            if use_sqlite {
                memory.active_sqlite_reads += 1;
            }
        }

        if !use_sqlite {
            return Ok(None);
        }
        let _active_read = ActiveIdentityCacheRead {
            memory: self.memory.clone(),
        };
        let path = self.path.clone();
        let read_key = key.to_owned();
        let stored = tokio::task::spawn_blocking(move || read_identity_cache_row(&path, &read_key))
            .await
            .map_err(|error| rusqlite::Error::ToSqlConversionFailure(Box::new(error)))??;
        let Some((did, json, expires_at_ms)) = stored else {
            return Ok(None);
        };
        let now_ms = unix_time_ms();
        if expires_at_ms <= now_ms {
            return Ok(None);
        }

        let value = match serde_json::from_str::<Result<PersistedIdentity, ResolutionError>>(&json)
        {
            Ok(value) => value,
            Err(_) => return Ok(None),
        };
        let remaining = Duration::from_millis(expires_at_ms.saturating_sub(now_ms) as u64);
        let mut memory = self.memory.lock().map_err(|_| {
            rusqlite::Error::InvalidParameterName("identity cache memory lock poisoned".to_owned())
        })?;
        if let Some(invalidation) = memory.invalidated_dids.get(&did).copied() {
            drop(memory);
            self.enqueue(IdentityCacheWrite::InvalidateDid {
                did,
                generation: invalidation.generation,
            });
            return Ok(None);
        }
        memory.entries.insert(
            key.to_owned(),
            MemoryIdentityEntry {
                did,
                expires_at: Instant::now() + remaining,
                value: value.clone(),
            },
        );
        Ok(Some(value))
    }

    fn set(
        &self,
        key: &str,
        did: &str,
        value: Result<PersistedIdentity, ResolutionError>,
        ttl: Duration,
    ) {
        let result_json = match serde_json::to_string(&value) {
            Ok(json) => json,
            Err(error) => {
                tracing::warn!(%error, cache_key = key, "could not encode persistent identity cache entry");
                return;
            }
        };
        let now_ms = unix_time_ms();
        let expires_at_ms = now_ms.saturating_add(ttl.as_millis().min(i64::MAX as u128) as i64);
        let Ok(mut memory) = self.memory.lock() else {
            tracing::warn!(cache_key = key, "identity cache memory lock poisoned");
            return;
        };
        memory.entries.insert(
            key.to_owned(),
            MemoryIdentityEntry {
                did: did.to_owned(),
                expires_at: Instant::now() + ttl,
                value,
            },
        );
        drop(memory);
        self.enqueue(IdentityCacheWrite::Set {
            key: key.to_owned(),
            did: did.to_owned(),
            result_json,
            expires_at_ms,
        });
    }

    fn enqueue(&self, write: IdentityCacheWrite) {
        if let Err(error) = self.writer.try_send(write) {
            let dropped = self.dropped_writes.fetch_add(1, Ordering::Relaxed) + 1;
            match error {
                TrySendError::Full(_) => tracing::debug!(
                    dropped_writes = dropped,
                    "identity cache writer queue is full; dropping update"
                ),
                TrySendError::Disconnected(_) => tracing::debug!(
                    dropped_writes = dropped,
                    "identity cache writer is stopped; dropping update"
                ),
            }
        }
    }

    /// Remove all aliases, documents, and verified identities for a DID from memory immediately,
    /// then queue the SQLite delete without waiting for the writer.
    pub fn invalidate_did(&self, did: &str) -> Result<(), rusqlite::Error> {
        let generation = self.next_invalidation.fetch_add(1, Ordering::Relaxed) + 1;
        let mut memory = self.memory.lock().map_err(|_| {
            rusqlite::Error::InvalidParameterName("identity cache memory lock poisoned".to_owned())
        })?;
        memory.entries.retain(|_, entry| entry.did != did);
        memory.invalidated_dids.insert(
            did.to_owned(),
            IdentityInvalidation {
                generation,
                committed: false,
            },
        );
        drop(memory);
        self.enqueue(IdentityCacheWrite::InvalidateDid {
            did: did.to_owned(),
            generation,
        });
        Ok(())
    }
}

fn read_identity_cache_row(
    path: &Path,
    key: &str,
) -> Result<Option<(String, String, i64)>, rusqlite::Error> {
    let connection = Connection::open_with_flags(
        path,
        OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
    )?;
    connection.busy_timeout(Duration::from_millis(100))?;
    connection.query_row(
        "SELECT did, result_json, expires_at_ms FROM identity_resolution_cache WHERE cache_key=?1",
        [key],
        |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
    )
    .optional()
}

fn identity_cache_writer(
    connection: Connection,
    receiver: Receiver<IdentityCacheWrite>,
    memory: Arc<StdMutex<IdentityCacheMemory>>,
) {
    while let Ok(write) = receiver.recv() {
        match write {
            IdentityCacheWrite::Set {
                key,
                did,
                result_json,
                expires_at_ms,
            } => {
                let result = connection
                    .execute(
                        "DELETE FROM identity_resolution_cache WHERE expires_at_ms <= ?1",
                        [unix_time_ms()],
                    )
                    .and_then(|_| {
                        connection.execute(
                            "INSERT INTO identity_resolution_cache(cache_key,did,result_json,expires_at_ms)
                             VALUES(?1,?2,?3,?4)
                             ON CONFLICT(cache_key) DO UPDATE SET
                                did=excluded.did,result_json=excluded.result_json,expires_at_ms=excluded.expires_at_ms",
                            params![key, did, result_json, expires_at_ms],
                        )
                    });
                if let Err(error) = result {
                    tracing::warn!(%error, cache_key = key, "could not save persistent identity cache");
                }
            }
            IdentityCacheWrite::InvalidateDid { did, generation } => {
                match connection
                    .execute("DELETE FROM identity_resolution_cache WHERE did=?1", [&did])
                {
                    Ok(_) => {
                        if let Ok(mut memory) = memory.lock() {
                            if let Some(invalidation) = memory.invalidated_dids.get_mut(&did)
                                && invalidation.generation == generation
                            {
                                invalidation.committed = true;
                            }
                            if memory.active_sqlite_reads == 0 {
                                memory
                                    .invalidated_dids
                                    .retain(|_, invalidation| !invalidation.committed);
                            }
                        }
                    }
                    Err(error) => {
                        tracing::warn!(%error, %did, "could not invalidate persistent identity cache")
                    }
                }
            }
        }
    }
}

pub struct IdentityResolver<B> {
    backend: Arc<B>,
    ttl: Duration,
    cache: RwLock<HashMap<String, CachedIdentity>>,
    persistent_cache: Option<Arc<SqliteIdentityCache>>,
}

impl<B: IdentityBackend> IdentityResolver<B> {
    pub fn new(backend: B, ttl: Duration) -> Self {
        Self {
            backend: Arc::new(backend),
            ttl,
            cache: RwLock::new(HashMap::new()),
            persistent_cache: None,
        }
    }

    pub fn with_persistent_cache(mut self, cache: Arc<SqliteIdentityCache>) -> Self {
        self.persistent_cache = Some(cache);
        self
    }

    pub async fn resolve_handle(&self, handle: &str) -> Result<Identity, ResolutionError> {
        let handle = normalize_handle(handle)?;
        let key = format!("handle:{handle}");
        if let Some(cached) = self.persistent_result(&key).await {
            return cached?.identity.ok_or_else(|| {
                ResolutionError::Backend("cached handle result had no identity".to_owned())
            });
        }
        if self.persistent_cache.is_none()
            && let Some(identity) = self.cached(&key).await
        {
            return Ok(identity);
        }

        let mut resolved_did = None;
        let result = async {
            let did = self.resolve_handle_assertion(&handle).await?;
            resolved_did = Some(did.clone());
            let doc = self.resolve_did_document(&did).await?;
            if doc.id != did
                || !doc
                    .also_known_as
                    .iter()
                    .any(|uri| uri == &format!("at://{handle}"))
            {
                return Err(ResolutionError::UnverifiedHandle(handle.clone()));
            }
            Ok((
                Identity {
                    did,
                    handle: Some(handle.clone()),
                },
                doc,
            ))
        }
        .await;
        match result {
            Ok((identity, document)) => {
                self.insert(key.clone(), identity.clone()).await;
                self.persist_success(
                    &key,
                    &identity.did,
                    Some(identity.clone()),
                    Some(document.clone()),
                );
                self.persist_success(
                    &format!("did-document:{}", identity.did),
                    &identity.did,
                    None,
                    Some(document),
                );
                self.persist_success(
                    &format!("did-identity:{}", identity.did),
                    &identity.did,
                    Some(identity.clone()),
                    None,
                );
                Ok(identity)
            }
            Err(error) => {
                self.persist_failure(&key, resolved_did.as_deref().unwrap_or(""), error.clone());
                Err(error)
            }
        }
    }

    pub async fn resolve_did_document(&self, did: &str) -> Result<DidDocument, ResolutionError> {
        validate_did(did)?;
        let key = format!("did-document:{did}");
        if let Some(cached) = self.persistent_result(&key).await {
            let document = cached?.document.ok_or_else(|| {
                ResolutionError::Backend("cached DID document result had no document".to_owned())
            })?;
            if document.id != did {
                return Err(ResolutionError::DidDocumentMismatch {
                    actual: document.id,
                    requested: did.to_owned(),
                });
            }
            return Ok(document);
        }

        let result = async {
            let document = self
                .backend
                .did_document(did)
                .await?
                .ok_or_else(|| ResolutionError::DidNotFound(did.to_owned()))?;
            if document.id != did {
                return Err(ResolutionError::DidDocumentMismatch {
                    actual: document.id,
                    requested: did.to_owned(),
                });
            }
            Ok(document)
        }
        .await;
        match result {
            Ok(document) => {
                self.persist_success(&key, did, None, Some(document.clone()));
                Ok(document)
            }
            Err(error) => {
                self.persist_failure(&key, did, error.clone());
                Err(error)
            }
        }
    }

    pub async fn verified_handle_for_document(
        &self,
        document: &DidDocument,
    ) -> Result<Option<String>, ResolutionError> {
        for alias in &document.also_known_as {
            if let Some(candidate) = alias.strip_prefix("at://")
                && let Ok(handle) = normalize_handle(candidate)
            {
                match self.resolve_handle_assertion(&handle).await {
                    Ok(asserted_did) if asserted_did == document.id => {
                        return Ok(Some(handle));
                    }
                    Ok(_) | Err(ResolutionError::UnverifiedHandle(_)) => {}
                    Err(error) => return Err(error),
                }
            }
        }
        Ok(None)
    }

    pub async fn resolve_did(&self, did: &str) -> Result<Identity, ResolutionError> {
        validate_did(did)?;
        let key = format!("did:{did}");
        let persistent_key = format!("did-identity:{did}");
        if let Some(cached) = self.persistent_result(&persistent_key).await {
            return cached?.identity.ok_or_else(|| {
                ResolutionError::Backend("cached DID identity result had no identity".to_owned())
            });
        }
        if self.persistent_cache.is_none()
            && let Some(identity) = self.cached(&key).await
        {
            return Ok(identity);
        }

        let result: Result<Identity, ResolutionError> = async {
            let doc = self.resolve_did_document(did).await?;
            let verified_handle = self.verified_handle_for_document(&doc).await?;
            Ok(Identity {
                did: did.to_owned(),
                handle: verified_handle,
            })
        }
        .await;
        match result {
            Ok(identity) => {
                self.insert(key, identity.clone()).await;
                self.persist_success(&persistent_key, did, Some(identity.clone()), None);
                Ok(identity)
            }
            Err(error) => {
                self.persist_failure(&persistent_key, did, error.clone());
                Err(error)
            }
        }
    }

    async fn persistent_result(
        &self,
        key: &str,
    ) -> Option<Result<PersistedIdentity, ResolutionError>> {
        let cache = self.persistent_cache.as_ref()?;
        match cache.get(key).await {
            Ok(value) => value,
            Err(error) => {
                tracing::warn!(%error, cache_key = key, "could not read persistent identity cache");
                None
            }
        }
    }

    fn persist_success(
        &self,
        key: &str,
        did: &str,
        identity: Option<Identity>,
        document: Option<DidDocument>,
    ) {
        let Some(cache) = &self.persistent_cache else {
            return;
        };
        cache.set(
            key,
            did,
            Ok(PersistedIdentity { identity, document }),
            PERSISTED_IDENTITY_TTL,
        );
    }

    fn persist_failure(&self, key: &str, did: &str, error: ResolutionError) {
        let Some(cache) = &self.persistent_cache else {
            return;
        };
        cache.set(key, did, Err(error), PERSISTED_IDENTITY_FAILURE_TTL);
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
        if self.persistent_cache.is_some() {
            return;
        }
        self.cache.write().await.insert(
            key,
            CachedIdentity {
                expires_at: Instant::now() + self.ttl,
                identity,
            },
        );
    }
}

const HANDLE_REFRESH_INTERVAL: Duration = Duration::from_millis(100);
const HANDLE_REFRESH_BATCH: usize = 128;

#[derive(Debug, Error)]
pub enum HandleRefreshError {
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error(transparent)]
    Resolution(#[from] ResolutionError),
    #[error("database worker task failed: {0}")]
    StorageTask(#[from] tokio::task::JoinError),
}

async fn storage_call<S, T, F>(storage: Arc<S>, operation: F) -> Result<T, HandleRefreshError>
where
    S: Storage + 'static,
    T: Send + 'static,
    F: FnOnce(&S) -> Result<T, StorageError> + Send + 'static,
{
    Ok(tokio::task::spawn_blocking(move || operation(&storage)).await??)
}

/// Periodically re-verifies stored handles for DIDs with indexed or registered data.
/// Stale values remain available until a completed check proves them invalid.
pub struct HandleRefreshWorker<S: Storage, B: IdentityBackend> {
    storage: Arc<S>,
    resolver: IdentityResolver<B>,
    handle_ttl: Duration,
    poll_interval: Duration,
    last_refresh: Mutex<Option<Instant>>,
}

impl<S: Storage + 'static, B: IdentityBackend> HandleRefreshWorker<S, B> {
    pub fn new(
        storage: Arc<S>,
        resolver: IdentityResolver<B>,
        handle_ttl: Duration,
        poll_interval: Duration,
    ) -> Self {
        Self {
            storage,
            resolver,
            handle_ttl,
            poll_interval: poll_interval.max(Duration::from_millis(1)),
            last_refresh: Mutex::new(None),
        }
    }

    /// Queue stale verifications and process up to one bounded batch.
    pub async fn refresh_once(&self) -> Result<usize, HandleRefreshError> {
        for did in self
            .storage
            .stale_handle_dids(self.handle_ttl, HANDLE_REFRESH_BATCH)?
        {
            let queued_did = did.clone();
            storage_call(Arc::clone(&self.storage), move |storage| {
                storage.queue_handle_refresh(&queued_did)
            })
            .await?;
        }

        let pending = self
            .storage
            .pending_handle_refreshes(HANDLE_REFRESH_BATCH)?;
        let mut completed = 0;
        let mut first_error = None;
        for did in pending {
            self.wait_for_refresh_slot().await;
            match self.refresh_one(&did).await {
                Ok(()) => completed += 1,
                Err(error) => {
                    if first_error.is_none() {
                        first_error = Some(error);
                    }
                }
            }
        }
        match first_error {
            Some(error) => Err(error),
            None => Ok(completed),
        }
    }

    pub async fn run(&self, mut shutdown: watch::Receiver<bool>) -> Result<(), HandleRefreshError> {
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            if let Err(error) = self.refresh_once().await {
                tracing::warn!(%error, "verified handle refresh failed; retry scheduled");
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

    async fn wait_for_refresh_slot(&self) {
        let mut last_refresh = self.last_refresh.lock().await;
        if let Some(previous) = *last_refresh {
            let elapsed = previous.elapsed();
            if elapsed < HANDLE_REFRESH_INTERVAL {
                tokio::time::sleep(HANDLE_REFRESH_INTERVAL - elapsed).await;
            }
        }
        *last_refresh = Some(Instant::now());
    }

    async fn refresh_one(&self, did: &str) -> Result<(), HandleRefreshError> {
        let document = match self.resolver.resolve_did_document(did).await {
            Ok(document) => document,
            Err(
                error @ (ResolutionError::DidNotFound(_)
                | ResolutionError::DidDocumentMismatch { .. }
                | ResolutionError::InvalidHandle(_)
                | ResolutionError::UnsupportedDid(_)),
            ) => {
                tracing::debug!(%did, %error, "DID verification failed; clearing its cached handle");
                let storage_did = did.to_owned();
                storage_call(Arc::clone(&self.storage), move |storage| {
                    storage.clear_verified_handle(&storage_did)?;
                    storage.complete_handle_refresh(&storage_did)
                })
                .await?;
                return Ok(());
            }
            Err(error) => return Err(error.into()),
        };
        let verified_handle = self
            .resolver
            .verified_handle_for_document(&document)
            .await?;
        let storage_did = did.to_owned();
        storage_call(Arc::clone(&self.storage), move |storage| {
            match verified_handle {
                Some(handle) => storage.set_verified_handle(&storage_did, &handle)?,
                None => storage.clear_verified_handle(&storage_did)?,
            }
            storage.complete_handle_refresh(&storage_did)
        })
        .await?;
        Ok(())
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

fn unix_time_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
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
    client: SafeHttpClient,
    plc_directory: Url,
    dns_over_https: Url,
}

impl HttpIdentityBackend {
    pub fn new(plc_directory: &str, dns_over_https: &str) -> Result<Self, ResolutionError> {
        Self::with_client(plc_directory, dns_over_https, SafeHttpClient::new())
    }

    pub fn with_client(
        plc_directory: &str,
        dns_over_https: &str,
        client: SafeHttpClient,
    ) -> Result<Self, ResolutionError> {
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
            plc_did_document_url(&self.plc_directory, did)?
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

        let response = self
            .client
            .get(&url, HeaderMap::new(), 1024 * 1024)
            .await
            .map_err(backend_error)?;
        if response.status() == http::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(backend_error(
                "DID document service returned an error status",
            ));
        }
        let document =
            serde_json::from_slice::<DidDocument>(response.body()).map_err(backend_error)?;
        Ok(Some(document))
    }

    async fn dns_txt_did(&self, handle: &str) -> Result<Option<String>, ResolutionError> {
        let mut url = self.dns_over_https.clone();
        url.query_pairs_mut()
            .append_pair("name", &format!("_atproto.{handle}"))
            .append_pair("type", "TXT");
        let mut headers = HeaderMap::new();
        headers.insert(
            header::ACCEPT,
            HeaderValue::from_static("application/dns-json"),
        );
        let response = self
            .client
            .get(&url, headers, 64 * 1024)
            .await
            .map_err(backend_error)?;
        if !response.status().is_success() {
            return Err(backend_error("DNS service returned an error status"));
        }
        let response =
            serde_json::from_slice::<DohResponse>(response.body()).map_err(backend_error)?;

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
        let response = self
            .client
            .get(&url, HeaderMap::new(), 4096)
            .await
            .map_err(backend_error)?;
        if response.status() == http::StatusCode::NOT_FOUND {
            return Ok(None);
        }
        if !response.status().is_success() {
            return Err(backend_error("handle service returned an error status"));
        }
        let body = std::str::from_utf8(response.body()).map_err(backend_error)?;
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

fn plc_did_document_url(plc_directory: &Url, did: &str) -> Result<Url, ResolutionError> {
    let mut url = plc_directory.clone();
    url.path_segments_mut()
        .map_err(|_| backend_error("PLC directory URL cannot contain path segments"))?
        .pop_if_empty()
        .push(did);
    Ok(url)
}

#[cfg(test)]
mod tests {
    use super::{HttpIdentityBackend, plc_did_document_url};

    #[test]
    fn plc_did_is_appended_as_a_path_segment_for_each_base_shape() {
        const DID: &str = "did:plc:2iz7444ugycwhibyy3z5vord";
        let cases = [
            (
                "https://plc.example",
                "https://plc.example/did:plc:2iz7444ugycwhibyy3z5vord",
            ),
            (
                "https://plc.example/",
                "https://plc.example/did:plc:2iz7444ugycwhibyy3z5vord",
            ),
            (
                "https://proxy.example/plc",
                "https://proxy.example/plc/did:plc:2iz7444ugycwhibyy3z5vord",
            ),
            (
                "https://proxy.example/plc/",
                "https://proxy.example/plc/did:plc:2iz7444ugycwhibyy3z5vord",
            ),
        ];

        for (base, expected) in cases {
            let backend = HttpIdentityBackend::new(base, "https://dns.example/query").unwrap();
            let actual = plc_did_document_url(&backend.plc_directory, DID).unwrap();
            assert_eq!(actual.as_str(), expected, "base URL: {base}");
        }
    }
}
