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
