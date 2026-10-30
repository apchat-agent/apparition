use crate::net::SafeHttpClient;
use http::HeaderMap;
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::time::Instant;

pub const PUBLIC_APPVIEW_BASE: &str = "https://public.api.bsky.app/xrpc/";
/// Base for the one `app.bsky.actor.getProfiles` request that carries
/// `atproto-accept-labelers` (`refresh_account_actor_data` in `oauth.rs`).
/// It deliberately differs from [`PUBLIC_APPVIEW_BASE`]: measured 2026-10-30,
/// public.api.bsky.app IGNORES `atproto-accept-labelers` for third-party
/// labelers (its `atproto-content-labelers` response header always names only
/// `did:plc:ar7c4by46qjdydhdevvrndac;redact`, Bluesky's own moderation), while
/// api.bsky.app honours the header and returns the requested labels without
/// login. Do NOT "simplify" this back onto [`PUBLIC_APPVIEW_BASE`]: the public
/// host silently drops third-party labels, so viewers would see none.
pub const LABELLED_APPVIEW_BASE: &str = "https://api.bsky.app/xrpc/";
const MAX_PROFILE_COUNT_BATCH_SIZE: usize = 25;
const MAX_PROFILE_COUNT_CACHE_SIZE: usize = 4096;
const MAX_PROFILE_COUNT_RESPONSE_BYTES: usize = 1024 * 1024;
const MAX_PROFILE_COUNT_TIMEOUT: Duration = Duration::from_secs(3);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProfileCounts {
    pub followers: u64,
    pub following: u64,
    pub posts: u64,
}

#[derive(Clone)]
pub struct ProfileCountsClient {
    http: SafeHttpClient,
    public_appview_base: String,
    cache_ttl: Duration,
    timeout: Duration,
    enabled: bool,
    cache: Arc<Mutex<HashMap<String, CachedCounts>>>,
}

#[derive(Clone, Copy)]
struct CachedCounts {
    expires_at: Instant,
    counts: ProfileCounts,
}

impl ProfileCountsClient {
    pub fn new(
        http: SafeHttpClient,
        public_appview_base: impl Into<String>,
        cache_ttl: Duration,
    ) -> Self {
        Self {
            http,
            public_appview_base: format!("{}/", public_appview_base.into().trim_end_matches('/')),
            cache_ttl,
            timeout: MAX_PROFILE_COUNT_TIMEOUT,
            enabled: true,
            cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    /// Set a shorter request budget for tests or deployments. Requests always stay under 3 s.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout.min(MAX_PROFILE_COUNT_TIMEOUT);
        self
    }

    /// Preserve local indexed counts for routers that do not configure a public appview client.
    pub fn local_only() -> Self {
        Self {
            http: SafeHttpClient::new(),
            public_appview_base: PUBLIC_APPVIEW_BASE.to_owned(),
            cache_ttl: Duration::ZERO,
            timeout: MAX_PROFILE_COUNT_TIMEOUT,
            enabled: false,
            cache: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn get_counts(&self, did: &str) -> Option<ProfileCounts> {
        self.get_counts_many(&[did.to_owned()]).await.remove(did)
    }

    /// Return successful public counts keyed by DID. Failed, missing, or invalid values are
    /// omitted so callers can retain their local counts.
    pub async fn get_counts_many(&self, dids: &[String]) -> HashMap<String, ProfileCounts> {
        if !self.enabled || dids.is_empty() {
            return HashMap::new();
        }

        let now = Instant::now();
        let mut results = HashMap::new();
        let mut missing = Vec::new();
        let mut seen = HashSet::new();
        if let Ok(mut cache) = self.cache.lock() {
            cache.retain(|_, entry| entry.expires_at > now);
            for did in dids {
                if !seen.insert(did.as_str()) {
                    continue;
                }
                if let Some(entry) = cache.get(did) {
                    results.insert(did.clone(), entry.counts);
                } else {
                    missing.push(did.clone());
                }
            }
        } else {
            return HashMap::new();
        }

        for batch in missing.chunks(MAX_PROFILE_COUNT_BATCH_SIZE) {
            if let Some(fetched) = self.fetch_batch(batch).await {
                for (did, counts) in fetched {
                    self.cache_counts(did.clone(), counts);
                    results.insert(did, counts);
                }
            }
        }
        results
    }

    async fn fetch_batch(&self, dids: &[String]) -> Option<HashMap<String, ProfileCounts>> {
        if dids.is_empty() {
            return Some(HashMap::new());
        }
        let is_single = dids.len() == 1;
        let method = if is_single {
            "app.bsky.actor.getProfile"
        } else {
            "app.bsky.actor.getProfiles"
        };
        let mut url = url::Url::parse(&format!("{}{method}", self.public_appview_base)).ok()?;
        {
            let mut query = url.query_pairs_mut();
            if is_single {
                query.append_pair("actor", &dids[0]);
            } else {
                for did in dids {
                    query.append_pair("actors", did);
                }
            }
        }

        // SafeHttpClient's own request timeout starts after an outbound permit is acquired, so
        // keep an outer deadline around permit acquisition and DNS resolution too.
        let response = tokio::time::timeout(
            self.timeout,
            self.http.get_with_limits(
                &url,
                HeaderMap::new(),
                MAX_PROFILE_COUNT_RESPONSE_BYTES,
                0,
                self.timeout,
            ),
        )
        .await
        .ok()?
        .ok()?
        .0;
        if !response.status().is_success() {
            return None;
        }
        let body: Value = serde_json::from_slice(response.body()).ok()?;
        let profiles = if is_single {
            std::slice::from_ref(&body)
        } else {
            body.get("profiles")?.as_array()?.as_slice()
        };
        let requested = dids.iter().map(String::as_str).collect::<HashSet<_>>();
        let mut counts = HashMap::new();
        for profile in profiles {
            let Some(did) = profile.get("did").and_then(Value::as_str) else {
                continue;
            };
            if !requested.contains(did) {
                continue;
            }
            if let Some(value) = parse_counts(profile) {
                counts.insert(did.to_owned(), value);
            }
        }
        Some(counts)
    }

    fn cache_counts(&self, did: String, counts: ProfileCounts) {
        if self.cache_ttl.is_zero() {
            return;
        }
        let Ok(mut cache) = self.cache.lock() else {
            return;
        };
        let now = Instant::now();
        cache.retain(|_, entry| entry.expires_at > now);
        if cache.len() >= MAX_PROFILE_COUNT_CACHE_SIZE
            && !cache.contains_key(&did)
            && let Some(oldest) = cache
                .iter()
                .min_by_key(|(_, entry)| entry.expires_at)
                .map(|(key, _)| key.clone())
        {
            cache.remove(&oldest);
        }
        cache.insert(
            did,
            CachedCounts {
                expires_at: now + self.cache_ttl,
                counts,
            },
        );
    }
}

/// How long a `web_cache` relationship confirmation stays authoritative before
/// a profile page may serve the stored rows again and refresh in the
/// background.
pub const RELATIONSHIP_CONFIRMATION_TTL: Duration = Duration::from_secs(6 * 60 * 60);
const MAX_RELATIONSHIP_TIMEOUT: Duration = Duration::from_secs(3);
const MAX_RELATIONSHIP_RESPONSE_BYTES: usize = 1024 * 1024;

pub fn relationship_cache_key(viewer: &str, subject: &str) -> String {
    format!("rel:{viewer}:{subject}")
}

pub(crate) fn now_millis() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or_default()
}

/// What one `app.bsky.graph.getRelationships` response says about a
/// `(viewer, subject)` pair, read from `relationships[0]`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct RelationshipState {
    /// `true` when `followedBy` is a non-null object (the subject follows the viewer).
    pub followed_by: bool,
    /// `Some(true)`/`Some(false)` when the response answers whether the viewer
    /// follows the subject; `None` when the `follows` field is absent.
    pub following: Option<bool>,
}

#[derive(Clone)]
pub struct RelationshipClient {
    http: SafeHttpClient,
    public_appview_base: String,
    timeout: Duration,
    enabled: bool,
    in_progress: Arc<Mutex<HashSet<String>>>,
    persistence: Option<crate::oauth::SqliteOAuthStore>,
}

/// Clears the client's in-progress marker for one pair when dropped, so a
/// refresh whose caller or task died mid-flight cannot strand the key.
pub struct RelationshipRefreshGuard {
    in_progress: Arc<Mutex<HashSet<String>>>,
    key: String,
}

impl Drop for RelationshipRefreshGuard {
    fn drop(&mut self) {
        if let Ok(mut pending) = self.in_progress.lock() {
            pending.remove(&self.key);
        }
    }
}

impl RelationshipClient {
    pub fn new(http: SafeHttpClient, public_appview_base: impl Into<String>) -> Self {
        Self {
            http,
            public_appview_base: format!("{}/", public_appview_base.into().trim_end_matches('/')),
            timeout: Duration::from_millis(1500),
            enabled: true,
            in_progress: Arc::new(Mutex::new(HashSet::new())),
            persistence: None,
        }
    }

    /// Set the wait budget for an unconfirmed pair. Requests always stay under 3 s.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout.min(MAX_RELATIONSHIP_TIMEOUT);
        self
    }

    /// Never ask the public appview: profile pages answer from their own rows.
    pub fn local_only() -> Self {
        Self {
            http: SafeHttpClient::new(),
            public_appview_base: PUBLIC_APPVIEW_BASE.to_owned(),
            timeout: Duration::from_millis(1500),
            enabled: false,
            in_progress: Arc::new(Mutex::new(HashSet::new())),
            persistence: None,
        }
    }

    /// Keep confirmations in the OAuth `web_cache` table so they survive restarts.
    pub fn with_store(mut self, store: crate::oauth::SqliteOAuthStore) -> Self {
        self.persistence = Some(store);
        self
    }

    pub fn relationships_available(&self) -> bool {
        self.enabled
    }

    pub fn timeout(&self) -> Duration {
        self.timeout
    }

    pub fn begin_refresh(&self, key: &str) -> bool {
        let Ok(mut pending) = self.in_progress.lock() else {
            return false;
        };
        pending.insert(key.to_owned())
    }

    pub fn refresh_guard(&self, key: &str) -> RelationshipRefreshGuard {
        RelationshipRefreshGuard {
            in_progress: self.in_progress.clone(),
            key: key.to_owned(),
        }
    }

    /// The `web_cache` confirmation timestamp for this pair, if any.
    pub async fn load_confirmation(
        &self,
        viewer: &str,
        subject: &str,
    ) -> Result<Option<i64>, String> {
        let Some(store) = &self.persistence else {
            return Ok(None);
        };
        let key = relationship_cache_key(viewer, subject);
        let entry = match store.web_cache_get(&key).await? {
            Some(entry) => entry,
            None => return Ok(None),
        };
        let parsed: Value = serde_json::from_str(&entry.0).map_err(|error| error.to_string())?;
        if parsed.get("followedBy").and_then(Value::as_bool).is_none() {
            return Ok(None);
        }
        Ok(Some(entry.1))
    }

    pub fn record_confirmation(&self, viewer: &str, subject: &str, followed_by: bool) {
        let Some(store) = &self.persistence else {
            return;
        };
        let key = relationship_cache_key(viewer, subject);
        let value = serde_json::json!({"followedBy": followed_by}).to_string();
        if let Err(error) = store.web_cache_set(&key, &value) {
            tracing::debug!(
                viewer,
                subject,
                error = %error,
                "could not persist the follows-you relationship confirmation"
            );
        }
    }

    pub async fn fetch_relationship(
        &self,
        viewer: &str,
        subject: &str,
    ) -> Result<RelationshipState, String> {
        if !self.enabled {
            return Err("relationship confirmation is not configured".to_owned());
        }
        let mut url = url::Url::parse(&format!(
            "{}app.bsky.graph.getRelationships",
            self.public_appview_base
        ))
        .map_err(|error| error.to_string())?;
        {
            let mut query = url.query_pairs_mut();
            query.append_pair("actor", viewer);
            query.append_pair("others", subject);
        }
        let response = tokio::time::timeout(
            self.timeout,
            self.http.get_with_limits(
                &url,
                HeaderMap::new(),
                MAX_RELATIONSHIP_RESPONSE_BYTES,
                0,
                self.timeout,
            ),
        )
        .await
        .map_err(|_| "getRelationships request timed out".to_owned())?
        .map_err(|error| format!("getRelationships request failed: {error}"))?
        .0;
        let status = response.status();
        if !status.is_success() {
            return Err(format!("app.bsky.graph.getRelationships returned {status}"));
        }
        let body: Value = serde_json::from_slice(response.body())
            .map_err(|error| format!("getRelationships response was not valid JSON: {error}"))?;
        let relationship = body
            .get("relationships")
            .and_then(Value::as_array)
            .and_then(|relationships| relationships.first())
            .ok_or_else(|| "getRelationships response had no relationships entry".to_owned())?;
        Ok(RelationshipState {
            followed_by: relationship
                .get("followedBy")
                .map(|value| !value.is_null())
                .unwrap_or(false),
            following: match relationship.get("follows") {
                None => None,
                Some(Value::Null) => Some(false),
                Some(_) => Some(true),
            },
        })
    }

    /// Write what a confirmed relationship says into the viewer's own rows:
    /// the follower row (actor=subject) always, and the viewer's own row only
    /// when the response contradicts it. Returns the rows written.
    pub fn apply_relationship<S: crate::ingest::Storage>(
        &self,
        storage: &S,
        viewer: &str,
        subject: &str,
        state: RelationshipState,
    ) -> Result<u32, String> {
        let mut rows_written = 0_u32;
        storage
            .set_follow(subject, viewer, state.followed_by)
            .map_err(|error| error.to_string())?;
        rows_written += 1;
        if let Some(following) = state.following {
            let current = storage
                .is_following(viewer, subject)
                .map_err(|error| error.to_string())?;
            if current != following {
                storage
                    .set_follow(viewer, subject, following)
                    .map_err(|error| error.to_string())?;
                rows_written += 1;
                tracing::debug!(
                    viewer,
                    subject,
                    following,
                    "corrected the viewer's own follow row from the relationship response"
                );
            }
        }
        Ok(rows_written)
    }
}

pub fn apply_profile_counts(profile: &mut Value, counts: ProfileCounts) {
    profile["followersCount"] = Value::from(counts.followers);
    profile["followsCount"] = Value::from(counts.following);
    profile["postsCount"] = Value::from(counts.posts);
}

fn parse_counts(profile: &Value) -> Option<ProfileCounts> {
    Some(ProfileCounts {
        followers: profile.get("followersCount")?.as_u64()?,
        following: profile.get("followsCount")?.as_u64()?,
        posts: profile.get("postsCount")?.as_u64()?,
    })
}
