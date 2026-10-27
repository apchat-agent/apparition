use crate::ingest::{IndexedRecord, Storage, StorageError};
use atrium_crypto::{did::parse_did_key, verify::Verifier};
use atrium_repo::{
    Repository,
    blockstore::{AsyncBlockStoreRead, CarStore},
};
use futures_util::TryStreamExt;
use ipld_core::cid::Cid;
use serde::Deserialize;
use serde_json::Value;
use std::{
    collections::HashMap,
    future::Future,
    io::Cursor,
    sync::Arc,
    time::{Duration, Instant},
};
use thiserror::Error;
use tokio::sync::Mutex;

const MAX_REPO_CAR_BYTES: usize = 512 * 1024 * 1024;
const INDEXED_COLLECTIONS: &[&str] = &[
    "app.bsky.feed.post",
    "app.bsky.feed.like",
    "app.bsky.feed.repost",
    "app.bsky.graph.follow",
    "app.bsky.graph.block",
    "app.bsky.actor.profile",
];

#[derive(Debug, Error)]
pub enum BackfillError {
    #[error("invalid PDS endpoint: {0}")]
    InvalidPds(String),
    #[error("repository fetch failed: {0}")]
    Fetch(String),
    #[error("repository CAR verification failed: {0}")]
    Verify(String),
    #[error(transparent)]
    Storage(#[from] StorageError),
}

#[derive(Clone, Debug, PartialEq)]
pub struct RepoRecord {
    pub collection: String,
    pub rkey: String,
    pub cid: String,
    pub record: Value,
}

#[derive(Clone, Debug, PartialEq)]
pub struct VerifiedRepo {
    pub records: Vec<RepoRecord>,
}

pub trait RepoFetcher: Send + Sync {
    fn fetch_repo<'a>(
        &'a self,
        pds: &'a str,
        did: &'a str,
    ) -> impl Future<Output = Result<Vec<u8>, BackfillError>> + Send;
}

pub trait RepoVerifier: Send + Sync {
    fn verify<'a>(
        &'a self,
        car_bytes: &'a [u8],
        expected_did: &'a str,
        signing_did_key: &'a str,
    ) -> impl Future<Output = Result<VerifiedRepo, BackfillError>> + Send;
}

#[derive(Clone)]
pub struct HttpRepoSource {
    client: reqwest::Client,
    max_bytes: usize,
}

impl HttpRepoSource {
    pub fn new(max_bytes: usize) -> Result<Self, BackfillError> {
        let client = reqwest::Client::builder()
            .timeout(Duration::from_secs(120))
            .build()
            .map_err(|error| BackfillError::Fetch(error.to_string()))?;
        Ok(Self { client, max_bytes })
    }
}

impl Default for HttpRepoSource {
    fn default() -> Self {
        Self::new(MAX_REPO_CAR_BYTES).expect("HTTP client configuration is valid")
    }
}

impl RepoFetcher for HttpRepoSource {
    async fn fetch_repo(&self, pds: &str, did: &str) -> Result<Vec<u8>, BackfillError> {
        let mut endpoint = UrlParts::parse_pds(pds)?;
        endpoint.path = "/xrpc/com.atproto.sync.getRepo".to_owned();
        let mut url = reqwest::Url::parse(&endpoint.to_string())
            .map_err(|error| BackfillError::InvalidPds(error.to_string()))?;
        url.query_pairs_mut().append_pair("did", did);
        let mut response = self
            .client
            .get(url)
            .header(reqwest::header::ACCEPT, "application/vnd.ipld.car")
            .send()
            .await
            .map_err(|error| BackfillError::Fetch(error.to_string()))?
            .error_for_status()
            .map_err(|error| BackfillError::Fetch(error.to_string()))?;
        if response
            .content_length()
            .is_some_and(|length| length > self.max_bytes as u64)
        {
            return Err(BackfillError::Fetch(
                "repository CAR exceeds size limit".into(),
            ));
        }
        let mut body = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|error| BackfillError::Fetch(error.to_string()))?
        {
            if body.len() + chunk.len() > self.max_bytes {
                return Err(BackfillError::Fetch(
                    "repository CAR exceeds size limit".into(),
                ));
            }
            body.extend_from_slice(&chunk);
        }
        Ok(body)
    }
}

struct UrlParts {
    origin: String,
    path: String,
}

impl UrlParts {
    fn parse_pds(pds: &str) -> Result<Self, BackfillError> {
        let url = reqwest::Url::parse(pds)
            .map_err(|error| BackfillError::InvalidPds(error.to_string()))?;
        let local = matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "::1"));
        if (url.scheme() != "https" && !(local && url.scheme() == "http"))
            || !url.username().is_empty()
            || url.password().is_some()
            || url.path() != "/"
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(BackfillError::InvalidPds(pds.to_owned()));
        }
        let mut origin = url.origin().ascii_serialization();
        if origin.ends_with('/') {
            origin.pop();
        }
        Ok(Self {
            origin,
            path: String::new(),
        })
    }
}

impl std::fmt::Display for UrlParts {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "{}{}", self.origin, self.path)
    }
}

#[derive(Default)]
pub struct AtriumRepoVerifier;

#[derive(Deserialize)]
struct CommitIdentity {
    did: String,
}

impl RepoVerifier for AtriumRepoVerifier {
    async fn verify(
        &self,
        car_bytes: &[u8],
        expected_did: &str,
        signing_did_key: &str,
    ) -> Result<VerifiedRepo, BackfillError> {
        let mut car = CarStore::open(Cursor::new(car_bytes))
            .await
            .map_err(|error| BackfillError::Verify(error.to_string()))?;
        let root = car
            .roots()
            .next()
            .ok_or_else(|| BackfillError::Verify("CAR has no root commit".into()))?;
        let commit_bytes = car
            .read_block(root)
            .await
            .map_err(|error| BackfillError::Verify(error.to_string()))?;
        let commit_identity: CommitIdentity = serde_ipld_dagcbor::from_slice(&commit_bytes)
            .map_err(|error| BackfillError::Verify(error.to_string()))?;
        if commit_identity.did != expected_did {
            return Err(BackfillError::Verify(format!(
                "CAR repository DID {} did not match requested {expected_did}",
                commit_identity.did
            )));
        }

        let mut repository = Repository::open(&mut car, root)
            .await
            .map_err(|error| BackfillError::Verify(error.to_string()))?;
        let commit = repository.commit();
        let (algorithm, public_key) = parse_did_key(signing_did_key)
            .map_err(|error| BackfillError::Verify(error.to_string()))?;
        Verifier::default()
            .verify(algorithm, &public_key, &commit.bytes(), commit.sig())
            .map_err(|error| BackfillError::Verify(error.to_string()))?;

        let entries = {
            let mut tree = repository.tree();
            tree.entries()
                .try_collect::<Vec<(String, Cid)>>()
                .await
                .map_err(|error| BackfillError::Verify(error.to_string()))?
        };
        let mut records = Vec::new();
        for (path, cid) in entries {
            let Some((collection, rkey)) = path.split_once('/') else {
                return Err(BackfillError::Verify(format!(
                    "invalid repository path {path}"
                )));
            };
            if !INDEXED_COLLECTIONS.contains(&collection) {
                continue;
            }
            let record = repository
                .get_raw::<Value>(&path)
                .await
                .map_err(|error| BackfillError::Verify(error.to_string()))?
                .ok_or_else(|| BackfillError::Verify(format!("record block {cid} is missing")))?;
            records.push(RepoRecord {
                collection: collection.to_owned(),
                rkey: rkey.to_owned(),
                cid: cid.to_string(),
                record,
            });
        }
        Ok(VerifiedRepo { records })
    }
}

pub struct HostRateLimiter {
    interval: Duration,
    next_allowed: Mutex<HashMap<String, Instant>>,
}

impl HostRateLimiter {
    pub fn new(interval: Duration) -> Self {
        Self {
            interval,
            next_allowed: Mutex::new(HashMap::new()),
        }
    }

    pub async fn wait(&self, host: &str) {
        let wait = {
            let now = Instant::now();
            let mut next_allowed = self.next_allowed.lock().await;
            next_allowed.retain(|_, next| *next > now);
            let next = next_allowed.get(host).copied().unwrap_or(now).max(now);
            next_allowed.insert(host.to_owned(), next + self.interval);
            next.saturating_duration_since(now)
        };
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
    }
}

pub struct RepoBackfiller<S, F, V>
where
    S: Storage,
    F: RepoFetcher,
    V: RepoVerifier,
{
    storage: Arc<S>,
    fetcher: F,
    verifier: V,
    limiter: Arc<HostRateLimiter>,
}

impl<S, F, V> RepoBackfiller<S, F, V>
where
    S: Storage,
    F: RepoFetcher,
    V: RepoVerifier,
{
    pub fn new(storage: Arc<S>, fetcher: F, verifier: V, limiter: HostRateLimiter) -> Self {
        Self {
            storage,
            fetcher,
            verifier,
            limiter: Arc::new(limiter),
        }
    }

    pub async fn backfill(
        &self,
        did: &str,
        pds: &str,
        signing_did_key: &str,
    ) -> Result<usize, BackfillError> {
        let endpoint = UrlParts::parse_pds(pds)?;
        let host = reqwest::Url::parse(&endpoint.origin)
            .map_err(|error| BackfillError::InvalidPds(error.to_string()))?
            .host_str()
            .map(str::to_owned)
            .ok_or_else(|| BackfillError::InvalidPds(pds.to_owned()))?;
        self.limiter.wait(&host).await;
        let car = self.fetcher.fetch_repo(pds, did).await?;
        let verified = self.verifier.verify(&car, did, signing_did_key).await?;
        let mut records = Vec::new();
        let mut following = Vec::new();
        for repo_record in verified.records {
            let record_time = repo_record
                .record
                .get("createdAt")
                .and_then(Value::as_str)
                .and_then(parse_rfc3339_micros)
                .unwrap_or_default();
            if repo_record.collection == "app.bsky.graph.follow"
                && let Some(subject) = repo_record.record.get("subject").and_then(Value::as_str)
            {
                following.push(subject.to_owned());
            }
            records.push(IndexedRecord {
                did: did.to_owned(),
                collection: repo_record.collection.clone(),
                rkey: repo_record.rkey.clone(),
                uri: format!("at://{did}/{}/{}", repo_record.collection, repo_record.rkey),
                cid: Some(repo_record.cid),
                time_us: record_time,
                record: repo_record.record,
            });
        }
        let count = records.len();
        self.storage
            .replace_repo_snapshot(did, &records, &following)?;
        tracing::info!(
            did,
            indexed_records = count,
            host,
            "repository backfill completed"
        );
        Ok(count)
    }
}

fn parse_rfc3339_micros(value: &str) -> Option<u64> {
    let parsed =
        time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339).ok()?;
    u64::try_from(parsed.unix_timestamp_nanos() / 1_000).ok()
}

pub struct BackfillWorker<S, F, V, B>
where
    S: Storage,
    F: RepoFetcher,
    V: RepoVerifier,
    B: crate::identity::IdentityBackend,
{
    storage: Arc<S>,
    resolver: crate::identity::IdentityResolver<B>,
    backfiller: RepoBackfiller<S, F, V>,
    poll_interval: Duration,
}

impl<S, F, V, B> BackfillWorker<S, F, V, B>
where
    S: Storage + 'static,
    F: RepoFetcher + 'static,
    V: RepoVerifier + 'static,
    B: crate::identity::IdentityBackend + 'static,
{
    pub fn new(
        storage: Arc<S>,
        resolver: crate::identity::IdentityResolver<B>,
        backfiller: RepoBackfiller<S, F, V>,
        poll_interval: Duration,
    ) -> Self {
        Self {
            storage,
            resolver,
            backfiller,
            poll_interval,
        }
    }

    pub async fn run(
        &self,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Result<(), BackfillError> {
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            for did in self.storage.pending_backfills()? {
                if *shutdown.borrow() {
                    return Ok(());
                }
                let result = self.backfill_one(&did).await;
                if let Err(error) = result {
                    tracing::warn!(%did, %error, "queued repository backfill will retry");
                }
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

    async fn backfill_one(&self, did: &str) -> Result<(), BackfillError> {
        let document = self
            .resolver
            .resolve_did_document(did)
            .await
            .map_err(|error| BackfillError::Verify(error.to_string()))?;
        match self.resolver.verified_handle_for_document(&document).await {
            Some(handle) => self.storage.set_verified_handle(did, &handle)?,
            None => self.storage.clear_verified_handle(did)?,
        }
        let pds = document.pds_endpoint().ok_or_else(|| {
            BackfillError::InvalidPds(format!("no valid PDS endpoint in {did} DID document"))
        })?;
        let signing_key = document.atproto_signing_key().ok_or_else(|| {
            BackfillError::Verify(format!(
                "no valid #atproto signing key in {did} DID document"
            ))
        })?;
        self.backfiller.backfill(did, &pds, &signing_key).await?;
        self.storage.complete_backfill(did)?;
        Ok(())
    }
}
