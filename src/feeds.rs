use crate::{
    identity::{IdentityBackend, IdentityResolver},
    ingest::{IndexedRecord, Storage},
    net::SafeHttpClient,
};
use futures_util::{StreamExt, stream};
use http::HeaderMap;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    sync::{Arc, Mutex, OnceLock},
    time::Duration,
};
use url::Url;

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ResolvedFeed {
    pub uri: String,
    pub display_name: String,
    pub description: Option<String>,
    pub avatar: Option<String>,
    pub service_did: String,
    pub endpoint: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FeedSkeletonEntry {
    pub post_uri: String,
    pub reason: Option<Value>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FeedSkeletonPage {
    pub feed: Vec<FeedSkeletonEntry>,
    pub cursor: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct HydratedFeedPost {
    pub record: IndexedRecord,
    pub reposted_by: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct HydratedFeedPage {
    pub feed: Vec<HydratedFeedPost>,
    pub cursor: Option<String>,
    pub dropped_count: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct PublicProfileView {
    pub did: String,
    pub handle: Option<String>,
    pub display_name: Option<String>,
    pub avatar: Option<String>,
    pub labels: Vec<Value>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct FetchedPublicPost {
    pub record: IndexedRecord,
    pub author: Option<PublicProfileView>,
}

impl PublicProfileView {
    pub fn indexed_record(&self) -> IndexedRecord {
        let mut record = serde_json::Map::new();
        if let Some(handle) = &self.handle {
            record.insert("publicHandle".to_owned(), Value::String(handle.clone()));
        }
        if let Some(display_name) = &self.display_name {
            record.insert(
                "displayName".to_owned(),
                Value::String(display_name.clone()),
            );
        }
        if let Some(avatar) = &self.avatar {
            record.insert("publicAvatar".to_owned(), Value::String(avatar.clone()));
        }
        record.insert("labels".to_owned(), Value::Array(self.labels.clone()));
        IndexedRecord {
            did: self.did.clone(),
            collection: "app.bsky.actor.profile".to_owned(),
            rkey: "self".to_owned(),
            uri: format!("at://{}/app.bsky.actor.profile/self", self.did),
            cid: None,
            time_us: unix_time_us(),
            record: Value::Object(record),
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct FeedTimeline {
    pub generator: ResolvedFeed,
    pub timeline: HydratedFeedPage,
    pub timing: FeedTimelineTiming,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct FeedTimelineTiming {
    pub session_token_refresh_ms: u64,
    pub timeline_fetch_live_ms: u64,
    pub skeleton_hydration_ms: u64,
    pub cache_hit: bool,
    pub skeleton_cache_hit: bool,
}

const LIVE_POST_CACHE_ENTRIES: usize = 20_000;
const LIVE_POST_CACHE_SECS: u64 = 6 * 60 * 60;
const PUBLIC_POST_BATCH_SIZE: usize = 25;
const PUBLIC_POST_BATCH_CONCURRENCY: usize = 4;
const MAX_PUBLIC_POST_RESPONSE_BYTES: usize = 2 * 1024 * 1024;
const FEED_GENERATOR_CACHE_PREFIX: &str = "feed-generator:";

fn unix_time_us() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros()
        .min(u64::MAX as u128) as u64
}

async fn cache_live_post(
    cache: &tokio::sync::Mutex<HashMap<String, (tokio::time::Instant, FetchedPublicPost)>>,
    uri: &str,
    post: FetchedPublicPost,
) {
    let mut cache = cache.lock().await;
    if cache.len() >= LIVE_POST_CACHE_ENTRIES {
        cache.retain(|_, (expires, _)| *expires > tokio::time::Instant::now());
    }
    if cache.len() >= LIVE_POST_CACHE_ENTRIES
        && let Some(oldest) = cache.keys().next().cloned()
    {
        cache.remove(&oldest);
    }
    cache.insert(
        uri.to_owned(),
        (
            tokio::time::Instant::now() + Duration::from_secs(LIVE_POST_CACHE_SECS),
            post,
        ),
    );
}

async fn fetch_public_posts_batch(
    http: &SafeHttpClient,
    public_appview_base: &str,
    uris: &[String],
) -> Result<Vec<FetchedPublicPost>, String> {
    let mut url = Url::parse(&format!("{}app.bsky.feed.getPosts", public_appview_base))
        .map_err(|_| "Bluesky AppView URL is invalid".to_owned())?;
    for uri in uris {
        url.query_pairs_mut().append_pair("uris", uri);
    }
    let response = http
        .get(&url, HeaderMap::new(), MAX_PUBLIC_POST_RESPONSE_BYTES)
        .await?;
    if !response.status().is_success() {
        return Err(format!(
            "Bluesky AppView getPosts failed with status {}",
            response.status()
        ));
    }
    let body: Value = serde_json::from_slice(response.body())
        .map_err(|_| "Bluesky AppView getPosts returned invalid JSON".to_owned())?;
    let posts = body
        .get("posts")
        .and_then(Value::as_array)
        .ok_or_else(|| "Bluesky AppView getPosts response has no posts list".to_owned())?;
    let requested = uris.iter().map(String::as_str).collect::<HashSet<_>>();
    let mut result = Vec::new();
    for view in posts {
        let Some(uri) = view.get("uri").and_then(Value::as_str) else {
            continue;
        };
        if !requested.contains(uri) {
            continue;
        }
        let (did, collection, rkey) = match parse_post_uri(uri) {
            Ok(parts) => parts,
            Err(_) => continue,
        };
        let Some(record) = view
            .get("record")
            .filter(|record| record.is_object())
            .cloned()
        else {
            continue;
        };
        let author = view
            .get("author")
            .and_then(parse_public_profile_view)
            .filter(|author| author.did == did);
        result.push(FetchedPublicPost {
            record: IndexedRecord {
                did,
                collection,
                rkey,
                uri: uri.to_owned(),
                cid: view.get("cid").and_then(Value::as_str).map(str::to_owned),
                time_us: unix_time_us(),
                record,
            },
            author,
        });
    }
    Ok(result)
}

pub(crate) fn parse_public_profile_view(value: &Value) -> Option<PublicProfileView> {
    let did = value.get("did")?.as_str()?.to_owned();
    if !did.starts_with("did:") {
        return None;
    }
    Some(PublicProfileView {
        did,
        handle: value
            .get("handle")
            .and_then(Value::as_str)
            .filter(|handle| !handle.is_empty() && *handle != "handle.invalid")
            .map(str::to_owned),
        display_name: value
            .get("displayName")
            .and_then(Value::as_str)
            .map(str::to_owned),
        avatar: value
            .get("avatar")
            .and_then(Value::as_str)
            .map(str::to_owned),
        labels: value
            .get("labels")
            .and_then(Value::as_array)
            .cloned()
            .unwrap_or_default(),
    })
}

pub(crate) fn persist_public_profiles(
    storage: &dyn Storage,
    profiles: impl IntoIterator<Item = PublicProfileView>,
) -> Result<(), crate::ingest::StorageError> {
    let mut seen = HashSet::new();
    let mut handles = Vec::new();
    for profile in profiles {
        if !seen.insert(profile.did.clone()) {
            continue;
        }
        storage.put_record(&merged_public_profile(storage, &profile)?)?;
        if profile.handle.is_some()
            && storage.verified_handle(&profile.did)?.is_none()
            && !storage.handle_is_invalid(&profile.did)?
        {
            handles.push(profile.did);
        }
    }
    if !handles.is_empty() {
        storage.queue_handle_refresh_ordered(&handles)?;
    }
    Ok(())
}

pub(crate) fn merged_public_profile(
    storage: &dyn Storage,
    profile: &PublicProfileView,
) -> Result<IndexedRecord, crate::ingest::StorageError> {
    let uri = format!("at://{}/app.bsky.actor.profile/self", profile.did);
    let existing = storage.get_record(&profile.did, "app.bsky.actor.profile", "self")?;
    let mut indexed = existing.unwrap_or_else(|| profile.indexed_record());
    indexed.did = profile.did.clone();
    indexed.collection = "app.bsky.actor.profile".to_owned();
    indexed.rkey = "self".to_owned();
    indexed.uri = uri;
    if !indexed.record.is_object() {
        indexed.record = Value::Object(serde_json::Map::new());
    }
    if let Some(handle) = &profile.handle {
        indexed.record["publicHandle"] = Value::String(handle.clone());
    }
    if let Some(display_name) = &profile.display_name {
        indexed.record["displayName"] = Value::String(display_name.clone());
    }
    if let Some(avatar) = &profile.avatar {
        indexed.record["publicAvatar"] = Value::String(avatar.clone());
    }
    indexed.record["labels"] = Value::Array(profile.labels.clone());
    indexed.time_us = unix_time_us();
    Ok(indexed)
}

#[derive(Serialize, Deserialize)]
enum CachedGeneratorResolution {
    Success(ResolvedFeed),
    Failure(String),
}

fn generator_cache_key(uri: &str) -> String {
    format!("{FEED_GENERATOR_CACHE_PREFIX}{uri}")
}

fn unix_time_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis().min(i64::MAX as u128) as i64)
        .unwrap_or_default()
}

fn fallback_feed(uri: &str) -> ResolvedFeed {
    let display_name = parse_feed_uri(uri)
        .ok()
        .map(|(_, _, rkey)| {
            rkey.split(['-', '_', '.'])
                .filter(|part| !part.is_empty())
                .map(|part| {
                    let mut chars = part.chars();
                    chars
                        .next()
                        .map(|first| first.to_uppercase().collect::<String>() + chars.as_str())
                        .unwrap_or_default()
                })
                .collect::<Vec<_>>()
                .join(" ")
        })
        .filter(|name| !name.is_empty())
        .unwrap_or_else(|| "Feed".to_owned());
    ResolvedFeed {
        uri: uri.to_owned(),
        display_name,
        description: None,
        avatar: None,
        service_did: String::new(),
        endpoint: String::new(),
    }
}

pub struct FeedClient<B: IdentityBackend> {
    identity: Arc<IdentityResolver<B>>,
    http: SafeHttpClient,
    public_appview_base: String,
    generator_cache: tokio::sync::Mutex<HashMap<String, (tokio::time::Instant, ResolvedFeed)>>,
    live_post_cache: tokio::sync::Mutex<HashMap<String, (tokio::time::Instant, FetchedPublicPost)>>,
    // Failed resolutions are remembered too: the layout resolves every pinned feed on every page,
    // so an uncached failure would repeat its full network cost on each request.
    generator_failures: tokio::sync::Mutex<HashMap<String, (tokio::time::Instant, String)>>,
    cache_store: Option<crate::oauth::SqliteOAuthStore>,
    background_resolutions: tokio::sync::Mutex<HashSet<String>>,
    resolution_slots: Arc<tokio::sync::Semaphore>,
}

impl<B: IdentityBackend + 'static> FeedClient<B> {
    pub fn new(identity: Arc<IdentityResolver<B>>, http: SafeHttpClient) -> Self {
        Self {
            identity,
            http,
            public_appview_base: crate::profile_counts::PUBLIC_APPVIEW_BASE.to_owned(),
            generator_cache: tokio::sync::Mutex::new(HashMap::new()),
            live_post_cache: tokio::sync::Mutex::new(HashMap::new()),
            generator_failures: tokio::sync::Mutex::new(HashMap::new()),
            cache_store: None,
            background_resolutions: tokio::sync::Mutex::new(HashSet::new()),
            resolution_slots: Arc::new(tokio::sync::Semaphore::new(8)),
        }
    }

    pub fn new_with_store(
        identity: Arc<IdentityResolver<B>>,
        http: SafeHttpClient,
        store: crate::oauth::SqliteOAuthStore,
    ) -> Self {
        let mut client = Self::new(identity, http);
        client.cache_store = Some(store);
        client
    }

    #[cfg(test)]
    fn with_public_appview_base(mut self, base: impl Into<String>) -> Self {
        self.public_appview_base = base.into();
        self
    }

    /// Feed tabs use persisted metadata when available and never wait for resolution I/O.
    pub async fn feed_for_tab(self: &Arc<Self>, uri: &str) -> ResolvedFeed {
        let now_ms = unix_time_ms();
        if let Some(store) = &self.cache_store
            && let Ok(Some((json, stored_at_ms))) = store.web_cache_get(&generator_cache_key(uri))
            && let Ok(cached) = serde_json::from_str::<CachedGeneratorResolution>(&json)
        {
            let age_ms = now_ms.saturating_sub(stored_at_ms).max(0) as u64;
            match cached {
                CachedGeneratorResolution::Success(feed) if age_ms < 24 * 60 * 60 * 1000 => {
                    self.generator_cache.lock().await.insert(
                        uri.to_owned(),
                        (
                            tokio::time::Instant::now() + Duration::from_secs(24 * 60 * 60),
                            feed.clone(),
                        ),
                    );
                    if age_ms > 60 * 60 * 1000 {
                        self.start_background_resolution(uri.to_owned()).await;
                    }
                    return feed;
                }
                CachedGeneratorResolution::Failure(_) if age_ms < 60 * 60 * 1000 => {
                    return fallback_feed(uri);
                }
                _ => {}
            }
        }
        self.start_background_resolution(uri.to_owned()).await;
        fallback_feed(uri)
    }

    async fn start_background_resolution(self: &Arc<Self>, uri: String) {
        let mut pending = self.background_resolutions.lock().await;
        if !pending.insert(uri.clone()) {
            return;
        }
        drop(pending);
        let client = Arc::clone(self);
        tokio::spawn(async move {
            let permit = client.resolution_slots.clone().acquire_owned().await;
            if permit.is_ok() {
                let _ = client.resolve_feed(&uri).await;
            }
            client.background_resolutions.lock().await.remove(&uri);
        });
    }

    pub async fn resolve_feed(&self, uri: &str) -> Result<ResolvedFeed, String> {
        if let Some(store) = &self.cache_store
            && let Ok(Some((json, stored_at_ms))) = store.web_cache_get(&generator_cache_key(uri))
            && let Ok(cached) = serde_json::from_str::<CachedGeneratorResolution>(&json)
        {
            let age_ms = unix_time_ms().saturating_sub(stored_at_ms).max(0) as u64;
            match cached {
                CachedGeneratorResolution::Success(feed) if age_ms < 24 * 60 * 60 * 1000 => {
                    return Ok(feed);
                }
                CachedGeneratorResolution::Failure(error) if age_ms < 60 * 60 * 1000 => {
                    return Err(error);
                }
                _ => {}
            }
        }
        if let Some((expires, error)) = self.generator_failures.lock().await.get(uri).cloned()
            && expires > tokio::time::Instant::now()
        {
            return Err(error);
        }
        let started = std::time::Instant::now();
        let result = self.resolve_feed_uncached(uri).await;
        if let Err(error) = &result {
            tracing::warn!(
                feed = uri,
                %error,
                duration_ms = started.elapsed().as_millis() as u64,
                "feed generator resolution failed"
            );
            let mut failures = self.generator_failures.lock().await;
            failures.retain(|_, (expires, _)| *expires > tokio::time::Instant::now());
            if failures.len() < 1024 {
                failures.insert(
                    uri.to_owned(),
                    (
                        tokio::time::Instant::now() + Duration::from_secs(10 * 60),
                        error.clone(),
                    ),
                );
            }
            self.save_resolution(uri, &CachedGeneratorResolution::Failure(error.clone()));
        }
        result
    }

    async fn resolve_feed_uncached(&self, uri: &str) -> Result<ResolvedFeed, String> {
        if let Some((expires, feed)) = self.generator_cache.lock().await.get(uri).cloned()
            && expires > tokio::time::Instant::now()
        {
            return Ok(feed);
        }
        let (creator_did, collection, rkey) = parse_feed_uri(uri)?;
        let creator_doc = self
            .identity
            .resolve_did_document(&creator_did)
            .await
            .map_err(|error| format!("feed creator DID resolution failed: {error}"))?;
        let pds = creator_doc
            .pds_endpoint()
            .ok_or_else(|| "feed creator DID document has no valid PDS endpoint".to_owned())?;
        let mut record_url = Url::parse(&format!("{pds}/xrpc/com.atproto.repo.getRecord"))
            .map_err(|_| "feed creator PDS URL is invalid".to_owned())?;
        record_url
            .query_pairs_mut()
            .append_pair("repo", &creator_did)
            .append_pair("collection", &collection)
            .append_pair("rkey", &rkey);
        let response = self
            .http
            .get(&record_url, HeaderMap::new(), 1024 * 1024)
            .await?;
        if !response.status().is_success() {
            return Err("feed generator record could not be fetched".to_owned());
        }
        let record: Value = serde_json::from_slice(response.body())
            .map_err(|_| "feed generator record was invalid JSON".to_owned())?;
        let value = record
            .get("value")
            .ok_or_else(|| "feed generator record has no value".to_owned())?;
        let service_did = value
            .get("did")
            .and_then(Value::as_str)
            .filter(|did| !did.is_empty())
            .ok_or_else(|| "feed generator record has no service DID".to_owned())?
            .to_owned();
        let service_doc = self
            .identity
            .resolve_did_document(&service_did)
            .await
            .map_err(|error| format!("feed generator service DID resolution failed: {error}"))?;
        let endpoint = service_doc
            .service
            .iter()
            .find(|service| {
                (service.id == "#bsky_fg" || service.id == format!("{}#bsky_fg", service_doc.id))
                    && service.type_name == "BskyFeedGenerator"
            })
            .map(|service| service.service_endpoint.clone())
            .ok_or_else(|| {
                "feed generator DID document is missing the #bsky_fg service".to_owned()
            })?;
        let endpoint_url = Url::parse(&endpoint)
            .map_err(|_| "feed generator #bsky_fg endpoint URL is invalid".to_owned())?;
        if endpoint_url.query().is_some()
            || endpoint_url.fragment().is_some()
            || !endpoint_url.username().is_empty()
            || endpoint_url.password().is_some()
        {
            return Err("feed generator #bsky_fg endpoint URL has forbidden components".to_owned());
        }
        self.http.resolve_safe(&endpoint_url).await?;

        let feed = ResolvedFeed {
            uri: uri.to_owned(),
            display_name: value
                .get("displayName")
                .and_then(Value::as_str)
                .unwrap_or("Untitled feed")
                .to_owned(),
            description: value
                .get("description")
                .and_then(Value::as_str)
                .map(str::to_owned),
            avatar: value
                .get("avatar")
                .and_then(|avatar| feed_avatar_url(avatar, &creator_did)),
            service_did,
            endpoint: endpoint.trim_end_matches('/').to_owned(),
        };
        let mut cache = self.generator_cache.lock().await;
        cache.retain(|_, (expires, _)| *expires > tokio::time::Instant::now());
        if cache.len() >= 1024
            && let Some(oldest) = cache.keys().next().cloned()
        {
            cache.remove(&oldest);
        }
        cache.insert(
            uri.to_owned(),
            (
                tokio::time::Instant::now() + Duration::from_secs(24 * 60 * 60),
                feed.clone(),
            ),
        );
        self.save_resolution(uri, &CachedGeneratorResolution::Success(feed.clone()));
        Ok(feed)
    }

    fn save_resolution(&self, uri: &str, value: &CachedGeneratorResolution) {
        if let Some(store) = &self.cache_store
            && let Ok(json) = serde_json::to_string(value)
            && let Err(error) = store.web_cache_set(&generator_cache_key(uri), &json)
        {
            tracing::warn!(feed = uri, %error, "could not persist feed generator cache");
        }
    }

    pub async fn get_skeleton(
        &self,
        feed: &ResolvedFeed,
        cursor: Option<&str>,
        service_jwt: Option<&str>,
    ) -> Result<FeedSkeletonPage, String> {
        if service_jwt.is_none() {
            warn_anonymous_feed_once(&feed.uri);
        }
        let mut url = Url::parse(&feed.endpoint)
            .map_err(|_| "feed generator endpoint URL is invalid".to_owned())?;
        let path = format!(
            "{}/xrpc/app.bsky.feed.getFeedSkeleton",
            url.path().trim_end_matches('/')
        );
        url.set_path(&path);
        url.query_pairs_mut()
            .append_pair("feed", &feed.uri)
            .append_pair("limit", "30");
        if let Some(cursor) = cursor {
            url.query_pairs_mut().append_pair("cursor", cursor);
        }
        let mut headers = HeaderMap::new();
        if let Some(jwt) = service_jwt {
            let value = http::HeaderValue::from_str(&format!("Bearer {jwt}"))
                .map_err(|_| "feed service auth token is invalid".to_owned())?;
            headers.insert(http::header::AUTHORIZATION, value);
        }
        let response = tokio::time::timeout(
            Duration::from_secs(10),
            self.http.get(&url, headers, 1024 * 1024),
        )
        .await
        .map_err(|_| "feed skeleton request timed out".to_owned())??;
        if !response.status().is_success() {
            return Err("feed generator returned an error status".to_owned());
        }
        let body: Value = serde_json::from_slice(response.body())
            .map_err(|_| "feed generator returned invalid JSON".to_owned())?;
        let feed_items = body
            .get("feed")
            .and_then(Value::as_array)
            .ok_or_else(|| "feed generator response has no feed list".to_owned())?;
        let items = feed_items
            .iter()
            .filter_map(|item| {
                Some(FeedSkeletonEntry {
                    post_uri: item.get("post")?.as_str()?.to_owned(),
                    reason: item.get("reason").cloned(),
                })
            })
            .collect();
        Ok(FeedSkeletonPage {
            feed: items,
            cursor: body
                .get("cursor")
                .and_then(Value::as_str)
                .map(str::to_owned),
        })
    }

    pub async fn hydrate_skeleton(
        &self,
        skeleton: FeedSkeletonPage,
        storage: &dyn Storage,
    ) -> Result<HydratedFeedPage, String> {
        const PAGE_BUDGET: Duration = Duration::from_secs(8);
        let count = skeleton.feed.len();
        let cursor = skeleton.cursor;
        let deadline = tokio::time::Instant::now() + PAGE_BUDGET;
        let skeleton_items = skeleton.feed;
        let uris = skeleton_items
            .iter()
            .map(|item| item.post_uri.clone())
            .collect::<Vec<_>>();
        let local = storage
            .records_by_uri("app.bsky.feed.post", &uris)
            .unwrap_or_default();
        let missing = uris
            .iter()
            .filter(|uri| !local.contains_key(*uri))
            .cloned()
            .collect::<Vec<_>>();
        let fetched = tokio::time::timeout_at(deadline, self.fetch_posts(&missing))
            .await
            .unwrap_or_default();

        let mut authors = Vec::new();
        let mut feed = Vec::with_capacity(count);
        for item in skeleton_items {
            let record = local
                .get(&item.post_uri)
                .cloned()
                .or_else(|| fetched.get(&item.post_uri).map(|post| post.record.clone()));
            let Some(record) = record else { continue };
            if let Some(author) = fetched
                .get(&item.post_uri)
                .and_then(|post| post.author.as_ref())
            {
                authors.push(author.clone());
            }
            let reposted_by = item
                .reason
                .as_ref()
                .filter(|reason| {
                    reason.get("$type").and_then(Value::as_str)
                        == Some("app.bsky.feed.defs#reasonRepost")
                })
                .and_then(|reason| reason.get("by"))
                .and_then(|by| {
                    by.get("handle")
                        .and_then(Value::as_str)
                        .or_else(|| by.get("did").and_then(Value::as_str))
                })
                .map(str::to_owned);
            feed.push(HydratedFeedPost {
                record,
                reposted_by,
            });
        }
        persist_public_profiles(storage, authors).map_err(|error| error.to_string())?;
        Ok(HydratedFeedPage {
            dropped_count: count.saturating_sub(feed.len()),
            feed,
            cursor,
        })
    }

    /// Fetch unindexed posts through the public AppView in batches of 25. The long-lived cache
    /// stays in front; only missing batch results fall back to their authors' PDSes.
    pub async fn fetch_posts(&self, uris: &[String]) -> HashMap<String, FetchedPublicPost> {
        let now = tokio::time::Instant::now();
        let mut fetched = HashMap::new();
        let mut missing = Vec::new();
        let mut seen = HashSet::new();
        {
            let mut cache = self.live_post_cache.lock().await;
            cache.retain(|_, (expires, _)| *expires > now);
            for uri in uris {
                if !seen.insert(uri.as_str()) {
                    continue;
                }
                if let Some((_, post)) = cache.get(uri) {
                    fetched.insert(uri.clone(), post.clone());
                } else {
                    missing.push(uri.clone());
                }
            }
        }

        let batches = missing
            .chunks(PUBLIC_POST_BATCH_SIZE)
            .map(|batch| batch.to_vec())
            .collect::<Vec<_>>();
        let http = self.http.clone();
        let base = self.public_appview_base.clone();
        let mut batch_results = stream::iter(batches.into_iter().map(|batch| {
            let http = http.clone();
            let base = base.clone();
            async move {
                let result = fetch_public_posts_batch(&http, &base, &batch).await;
                (batch, result)
            }
        }))
        .buffer_unordered(PUBLIC_POST_BATCH_CONCURRENCY);
        let mut unanswered = Vec::new();
        while let Some((batch, result)) = batch_results.next().await {
            match result {
                Ok(posts) => {
                    for post in posts {
                        if batch.iter().any(|requested| requested == &post.record.uri) {
                            cache_live_post(&self.live_post_cache, &post.record.uri, post.clone())
                                .await;
                            fetched.insert(post.record.uri.clone(), post);
                        }
                    }
                    unanswered.extend(batch.into_iter().filter(|uri| !fetched.contains_key(uri)));
                }
                Err(_) => unanswered.extend(batch),
            }
        }

        let client = self;
        let mut fallback_fetches = stream::iter(unanswered.into_iter().map(|uri| async move {
            let post = client.fetch_live_post_data(&uri).await.ok();
            (uri, post)
        }))
        .buffer_unordered(16);
        while let Some((uri, Some(post))) = fallback_fetches.next().await {
            fetched.insert(uri, post);
        }
        fetched
    }

    async fn fetch_live_post_data(&self, uri: &str) -> Result<FetchedPublicPost, String> {
        if let Some((expires, post)) = self.live_post_cache.lock().await.get(uri).cloned()
            && expires > tokio::time::Instant::now()
        {
            return Ok(post);
        }
        let (did, collection, rkey) = parse_post_uri(uri)?;
        let document = self
            .identity
            .resolve_did_document(&did)
            .await
            .map_err(|error| format!("post author DID resolution failed: {error}"))?;
        let pds = document
            .pds_endpoint()
            .ok_or_else(|| "post author DID document has no valid PDS endpoint".to_owned())?;
        let mut url = Url::parse(&format!("{pds}/xrpc/com.atproto.repo.getRecord"))
            .map_err(|_| "post author PDS URL is invalid".to_owned())?;
        url.query_pairs_mut()
            .append_pair("repo", &did)
            .append_pair("collection", &collection)
            .append_pair("rkey", &rkey);
        let response = self.http.get(&url, HeaderMap::new(), 1024 * 1024).await?;
        if !response.status().is_success() {
            return Err("post could not be fetched from its author PDS".to_owned());
        }
        let body: Value = serde_json::from_slice(response.body())
            .map_err(|_| "post PDS response was invalid JSON".to_owned())?;
        if body.get("uri").and_then(Value::as_str) != Some(uri) {
            return Err("post PDS returned a different record URI".to_owned());
        }
        let value = body
            .get("value")
            .filter(|value| value.is_object())
            .cloned()
            .ok_or_else(|| "post PDS response has no record value".to_owned())?;
        let time_us = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros()
            .min(u64::MAX as u128) as u64;
        let post = FetchedPublicPost {
            record: IndexedRecord {
                did,
                collection,
                rkey,
                uri: uri.to_owned(),
                cid: body.get("cid").and_then(Value::as_str).map(str::to_owned),
                time_us,
                record: value,
            },
            author: None,
        };
        cache_live_post(&self.live_post_cache, uri, post.clone()).await;
        Ok(post)
    }

    pub async fn fetch_post(&self, uri: &str) -> Result<IndexedRecord, String> {
        self.fetch_posts(&[uri.to_owned()])
            .await
            .remove(uri)
            .map(|post| post.record)
            .ok_or_else(|| "post could not be fetched from its author PDS".to_owned())
    }
}

fn warn_anonymous_feed_once(feed_uri: &str) {
    static WARNED_FEEDS: OnceLock<Mutex<HashSet<String>>> = OnceLock::new();
    let warned = WARNED_FEEDS.get_or_init(|| Mutex::new(HashSet::new()));
    let Ok(mut warned) = warned.lock() else {
        return;
    };
    if warned.len() >= 512 && !warned.contains(feed_uri) {
        return;
    }
    if warned.insert(feed_uri.to_owned()) {
        tracing::warn!(
            feed_uri,
            "feed service auth unavailable; requesting skeleton anonymously"
        );
    }
}

fn parse_feed_uri(uri: &str) -> Result<(String, String, String), String> {
    let Some(path) = uri.strip_prefix("at://") else {
        return Err("feed URI must use the at:// scheme".to_owned());
    };
    let mut parts = path.split('/');
    let did = parts.next().unwrap_or_default();
    let collection = parts.next().unwrap_or_default();
    let rkey = parts.next().unwrap_or_default();
    if did.is_empty()
        || collection != "app.bsky.feed.generator"
        || rkey.is_empty()
        || parts.next().is_some()
        || rkey.contains(['?', '#', '%'])
    {
        return Err("feed URI must identify one app.bsky.feed.generator record".to_owned());
    }
    Ok((did.to_owned(), collection.to_owned(), rkey.to_owned()))
}

fn parse_post_uri(uri: &str) -> Result<(String, String, String), String> {
    let Some(path) = uri.strip_prefix("at://") else {
        return Err("post URI must use the at:// scheme".to_owned());
    };
    let mut parts = path.split('/');
    let did = parts.next().unwrap_or_default();
    let collection = parts.next().unwrap_or_default();
    let rkey = parts.next().unwrap_or_default();
    if did.is_empty()
        || collection != "app.bsky.feed.post"
        || rkey.is_empty()
        || parts.next().is_some()
        || rkey.contains(['?', '#', '%'])
    {
        return Err("skeleton item must identify one app.bsky.feed.post record".to_owned());
    }
    Ok((did.to_owned(), collection.to_owned(), rkey.to_owned()))
}

pub fn feed_avatar_url(avatar: &Value, creator_did: &str) -> Option<String> {
    if let Some(url) = avatar.as_str() {
        return Some(url.to_owned());
    }
    let cid = avatar
        .pointer("/ref/$link")
        .and_then(Value::as_str)
        .or_else(|| avatar.get("ref").and_then(Value::as_str))?;
    if cid.is_empty()
        || !cid
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-' || byte == b'_')
    {
        return None;
    }
    Some(format!(
        "https://cdn.bsky.app/img/feed_thumbnail/plain/{creator_did}/{cid}@jpeg"
    ))
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SavedFeedPreference {
    pub id: String,
    pub feed_type: String,
    pub value: String,
    pub pinned: bool,
}

pub fn parse_saved_feed_preferences(response: &Value) -> Vec<SavedFeedPreference> {
    let Some(preferences) = response.get("preferences").and_then(Value::as_array) else {
        return Vec::new();
    };

    if let Some(v2) = preferences.iter().find(|preference| {
        preference.get("$type").and_then(Value::as_str)
            == Some("app.bsky.actor.defs#savedFeedsPrefV2")
    }) {
        return v2
            .get("items")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|item| {
                Some(SavedFeedPreference {
                    id: item.get("id")?.as_str()?.to_owned(),
                    feed_type: item.get("type")?.as_str()?.to_owned(),
                    value: item.get("value")?.as_str()?.to_owned(),
                    pinned: item.get("pinned")?.as_bool()?,
                })
            })
            .collect();
    }

    let Some(legacy) = preferences.iter().find(|preference| {
        preference.get("$type").and_then(Value::as_str)
            == Some("app.bsky.actor.defs#savedFeedsPref")
    }) else {
        return Vec::new();
    };
    let pinned = legacy
        .get("pinned")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>();
    let saved = legacy
        .get("saved")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>();
    let mut result = Vec::new();
    for (value, is_pinned) in pinned
        .iter()
        .map(|value| (*value, true))
        .chain(saved.iter().map(|value| (*value, false)))
    {
        if result
            .iter()
            .any(|item: &SavedFeedPreference| item.value == value)
        {
            continue;
        }
        result.push(SavedFeedPreference {
            id: value.to_owned(),
            feed_type: "feed".to_owned(),
            value: value.to_owned(),
            pinned: is_pinned,
        });
    }
    result
}

#[cfg(test)]
mod tests {
    use super::{FeedClient, ResolvedFeed, parse_saved_feed_preferences, unix_time_ms};
    use crate::{
        identity::{DidDocument, DidService, IdentityBackend, IdentityResolver, ResolutionError},
        ingest::IndexedRecord,
        net::{HostResolver, SafeHttpClient},
    };
    use axum::{
        Router,
        extract::State,
        http::{HeaderMap, StatusCode, Uri},
        response::IntoResponse,
        routing::{any, get},
    };
    use serde_json::json;
    use std::{
        collections::HashMap,
        future::Future,
        io::{self, Write},
        net::{IpAddr, Ipv4Addr},
        pin::Pin,
        sync::{Arc, Mutex},
        time::Duration,
    };
    use tokio::net::TcpListener;
    use tracing::instrument::WithSubscriber;
    use tracing_subscriber::fmt::MakeWriter;

    const CREATOR: &str = "did:web:creator.test";

    #[derive(Clone)]
    struct FakeIdentity(HashMap<String, DidDocument>);

    impl IdentityBackend for FakeIdentity {
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

    struct TestResolver;

    #[derive(Clone)]
    struct BatchServerState {
        posts: Arc<HashMap<String, serde_json::Value>>,
        fallback_posts: Arc<HashMap<String, serde_json::Value>>,
        appview_status: StatusCode,
        appview_calls: Arc<Mutex<Vec<usize>>>,
        pds_calls: Arc<Mutex<usize>>,
    }

    async fn batch_get_posts(
        State(state): State<BatchServerState>,
        uri: Uri,
    ) -> axum::response::Response {
        let uris = url::form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
            .filter(|(name, _)| name == "uris")
            .map(|(_, value)| value.into_owned())
            .collect::<Vec<_>>();
        state.appview_calls.lock().unwrap().push(uris.len());
        if !state.appview_status.is_success() {
            return state.appview_status.into_response();
        }
        let posts = uris
            .iter()
            .filter_map(|uri| state.posts.get(uri).cloned())
            .collect::<Vec<_>>();
        axum::Json(json!({"posts":posts})).into_response()
    }

    async fn batch_get_record(
        State(state): State<BatchServerState>,
        uri: Uri,
    ) -> axum::response::Response {
        *state.pds_calls.lock().unwrap() += 1;
        let query = url::form_urlencoded::parse(uri.query().unwrap_or_default().as_bytes())
            .collect::<HashMap<_, _>>();
        let record_uri = format!(
            "at://{}/{}/{}",
            query.get("repo").map_or("", |value| value.as_ref()),
            query.get("collection").map_or("", |value| value.as_ref()),
            query.get("rkey").map_or("", |value| value.as_ref())
        );
        match state.fallback_posts.get(&record_uri) {
            Some(post) => axum::Json(json!({
                "uri":record_uri,
                "cid":post.get("cid").and_then(serde_json::Value::as_str),
                "value":post.get("record").cloned().unwrap_or_default()
            }))
            .into_response(),
            None => StatusCode::NOT_FOUND.into_response(),
        }
    }

    async fn batch_server(
        posts: HashMap<String, serde_json::Value>,
        fallback_posts: HashMap<String, serde_json::Value>,
        appview_status: StatusCode,
    ) -> (String, BatchServerState, tokio::task::JoinHandle<()>) {
        let state = BatchServerState {
            posts: Arc::new(posts),
            fallback_posts: Arc::new(fallback_posts),
            appview_status,
            appview_calls: Arc::new(Mutex::new(Vec::new())),
            pds_calls: Arc::new(Mutex::new(0)),
        };
        let app = Router::new()
            .route("/xrpc/app.bsky.feed.getPosts", get(batch_get_posts))
            .route("/xrpc/com.atproto.repo.getRecord", get(batch_get_record))
            .with_state(state.clone());
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (
            format!("http://localhost:{}/xrpc/", address.port()),
            state,
            task,
        )
    }

    fn public_post(uri: &str) -> serde_json::Value {
        let mut parts = uri.strip_prefix("at://").unwrap().split('/');
        let did = parts.next().unwrap();
        json!({
            "$type":"app.bsky.feed.defs#postView",
            "uri":uri,
            "cid":"batch-cid",
            "author":{"did":did,"handle":"batch.test","displayName":"Batch author","avatar":"https://cdn.bsky.app/img/avatar/plain/did:plc:batch/avatar@jpeg","labels":[]},
            "record":{"$type":"app.bsky.feed.post","text":"batch post","createdAt":"2026-10-30T00:00:00Z"}
        })
    }

    fn fake_batch_identity(
        dids: impl IntoIterator<Item = String>,
        pds: &str,
    ) -> Arc<IdentityResolver<FakeIdentity>> {
        let documents = dids
            .into_iter()
            .map(|did| (did.clone(), document(&did, pds, vec![])))
            .collect();
        Arc::new(IdentityResolver::new(
            FakeIdentity(documents),
            Duration::from_secs(30),
        ))
    }

    fn test_client_with_appview(
        identity: Arc<IdentityResolver<FakeIdentity>>,
        base: String,
    ) -> FeedClient<FakeIdentity> {
        FeedClient::new(
            identity,
            SafeHttpClient::with_resolver(TestResolver)
                .with_http_test_hosts(["localhost".to_owned()]),
        )
        .with_public_appview_base(base)
    }

    #[derive(Clone, Default)]
    struct LogBuffer(Arc<Mutex<Vec<u8>>>);

    struct LogWriter(LogBuffer);

    impl Write for LogWriter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }

    impl<'a> MakeWriter<'a> for LogBuffer {
        type Writer = LogWriter;
        fn make_writer(&'a self) -> Self::Writer {
            LogWriter(self.clone())
        }
    }

    impl HostResolver for TestResolver {
        fn resolve<'a>(
            &'a self,
            _: &'a str,
            _: u16,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<IpAddr>, String>> + Send + 'a>> {
            Box::pin(async { Ok(vec![IpAddr::V4(Ipv4Addr::LOCALHOST)]) })
        }
    }

    fn document(did: &str, pds: &str, services: Vec<DidService>) -> DidDocument {
        let mut service = services;
        if !pds.is_empty() {
            service.insert(
                0,
                DidService {
                    id: "#atproto_pds".into(),
                    type_name: "AtprotoPersonalDataServer".into(),
                    service_endpoint: pds.into(),
                },
            );
        }
        DidDocument {
            id: did.to_owned(),
            also_known_as: vec![],
            verification_method: vec![],
            service,
        }
    }

    async fn fake_pds() -> (String, tokio::task::JoinHandle<()>) {
        fake_pds_with_generator_delay(Duration::ZERO).await
    }

    async fn fake_pds_with_generator_delay(
        generator_delay: Duration,
    ) -> (String, tokio::task::JoinHandle<()>) {
        let app = Router::new().fallback(any(move |uri: Uri| async move {
            if uri.path() == "/xrpc/com.atproto.repo.getRecord" {
                let query = uri.query().unwrap_or_default();
                if query.contains("collection=app.bsky.feed.generator") {
                    if query.contains("rkey=slow-") {
                        tokio::time::sleep(generator_delay).await;
                    }
                    let service_did = if query.contains("rkey=web") { "did:web:generator.test" } else { "did:plc:generator" };
                    axum::Json(json!({ "uri": format!("at://{CREATOR}/app.bsky.feed.generator/{}", if query.contains("rkey=web") { "web" } else { "plc" }), "cid":"bafyreitest", "value":{"did":service_did,"displayName":"Example","description":"A feed","avatar":"https://img.test/avatar"} })).into_response()
                } else if query.contains("rkey=live") {
                    axum::Json(json!({ "uri":"at://did:plc:author/app.bsky.feed.post/live", "cid":"bafyreilive", "value":{"$type":"app.bsky.feed.post","text":"live hydration","createdAt":"2026-10-28T12:00:00Z"} })).into_response()
                } else {
                    axum::http::StatusCode::NOT_FOUND.into_response()
                }
            } else { axum::http::StatusCode::NOT_FOUND.into_response() }
        }));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (format!("http://localhost:{}", address.port()), task)
    }

    fn resolver(pds: String, endpoint: String) -> Arc<IdentityResolver<FakeIdentity>> {
        let docs = HashMap::from([
            (CREATOR.to_owned(), document(CREATOR, &pds, vec![])),
            (
                "did:web:generator.test".to_owned(),
                document(
                    "did:web:generator.test",
                    "",
                    if endpoint.is_empty() {
                        vec![]
                    } else {
                        vec![DidService {
                            id: "#bsky_fg".into(),
                            type_name: "BskyFeedGenerator".into(),
                            service_endpoint: endpoint.clone(),
                        }]
                    },
                ),
            ),
            (
                "did:plc:generator".to_owned(),
                document(
                    "did:plc:generator",
                    "",
                    if endpoint.is_empty() {
                        vec![]
                    } else {
                        vec![DidService {
                            id: "#bsky_fg".into(),
                            type_name: "BskyFeedGenerator".into(),
                            service_endpoint: endpoint,
                        }]
                    },
                ),
            ),
        ]);
        Arc::new(IdentityResolver::new(
            FakeIdentity(docs),
            Duration::from_secs(30),
        ))
    }

    fn client(identity: Arc<IdentityResolver<FakeIdentity>>) -> FeedClient<FakeIdentity> {
        FeedClient::new(
            identity,
            SafeHttpClient::with_resolver(TestResolver)
                .with_http_test_hosts(["localhost".to_owned()]),
        )
    }

    #[tokio::test]
    async fn generator_records_resolve_did_web_and_did_plc_services() {
        let (pds, server) = fake_pds().await;
        let port = pds.rsplit(':').next().unwrap().to_owned();
        let identity = resolver(pds, format!("http://localhost:{port}/feed"));
        let client = client(identity);
        for rkey in ["web", "plc"] {
            let uri = format!("at://{CREATOR}/app.bsky.feed.generator/{rkey}");
            let feed = client.resolve_feed(&uri).await.unwrap();
            assert_eq!(feed.display_name, "Example");
            assert!(feed.endpoint.ends_with("/feed"));
            assert_eq!(
                feed.service_did,
                if rkey == "web" {
                    "did:web:generator.test"
                } else {
                    "did:plc:generator"
                }
            );
        }
        server.abort();
    }

    #[tokio::test]
    async fn generator_resolution_refuses_missing_service_and_ssrf_endpoint() {
        let (pds, server) = fake_pds().await;
        let endpoint = "https://blocked.test/feed";
        let identity = resolver(pds.clone(), endpoint.to_owned());
        let blocked = client(identity);
        let error = blocked
            .resolve_feed(&format!("at://{CREATOR}/app.bsky.feed.generator/web"))
            .await
            .unwrap_err();
        assert!(error.contains("forbidden address"), "{error}");

        let no_service = resolver(pds, "".to_owned());
        // An empty service endpoint is invalid and must produce a clear resolution error.
        let error = client(no_service)
            .resolve_feed(&format!("at://{CREATOR}/app.bsky.feed.generator/web"))
            .await
            .unwrap_err();
        assert!(error.contains("bsky_fg"), "{error}");
        server.abort();
    }

    #[tokio::test]
    async fn feed_tabs_fall_back_during_slow_resolution_and_survive_client_restart() {
        use futures_util::future::join_all;
        let (pds, server) = fake_pds_with_generator_delay(Duration::from_secs(5)).await;
        let port = pds.rsplit(':').next().unwrap().to_owned();
        let identity = resolver(pds, format!("http://localhost:{port}/feed"));
        let db = std::env::temp_dir().join(format!(
            "appview-feed-cache-{}-{}.sqlite",
            std::process::id(),
            unix_time_ms()
        ));
        let store = crate::oauth::SqliteOAuthStore::open(&db).unwrap();
        let client = Arc::new(FeedClient::new_with_store(
            identity.clone(),
            SafeHttpClient::with_resolver(TestResolver)
                .with_http_test_hosts(["localhost".to_owned()]),
            store.clone(),
        ));
        let uris = (0..38)
            .map(|index| {
                format!(
                    "at://{CREATOR}/app.bsky.feed.generator/{}",
                    if index < 5 {
                        format!("slow-{index}")
                    } else {
                        format!("cached-{index}")
                    }
                )
            })
            .collect::<Vec<_>>();
        for uri in uris.iter().skip(5) {
            client.resolve_feed(uri).await.unwrap();
        }
        let started = tokio::time::Instant::now();
        let tabs = tokio::time::timeout(
            Duration::from_millis(200),
            join_all(uris.iter().map(|uri| client.feed_for_tab(uri))),
        )
        .await
        .expect("tabs should not wait for five slow generator resolutions");
        assert_eq!(tabs.len(), 38);
        for (index, feed) in tabs.iter().enumerate() {
            assert_eq!(
                feed.display_name,
                if index < 5 {
                    format!("Slow {index}")
                } else {
                    "Example".to_owned()
                }
            );
        }
        assert!(started.elapsed() < Duration::from_millis(200));

        tokio::time::sleep(Duration::from_millis(5_200)).await;
        let restarted = Arc::new(FeedClient::new_with_store(
            identity,
            SafeHttpClient::with_resolver(TestResolver)
                .with_http_test_hosts(["localhost".to_owned()]),
            store,
        ));
        for uri in &uris {
            let feed = restarted.feed_for_tab(uri).await;
            assert_eq!(feed.display_name, "Example");
        }
        server.abort();
        let _ = std::fs::remove_file(&db);
        let _ = std::fs::remove_file(db.with_extension("sqlite-wal"));
        let _ = std::fs::remove_file(db.with_extension("sqlite-shm"));
    }

    #[tokio::test]
    async fn feed_hydration_keeps_skeleton_order_across_index_and_live_posts() {
        use crate::ingest::{MemoryStorage, Storage};
        let (pds, server) = fake_pds().await;
        let mut docs = HashMap::new();
        docs.insert(CREATOR.to_owned(), document(CREATOR, &pds, vec![]));
        docs.insert(
            "did:plc:author".to_owned(),
            document("did:plc:author", &pds, vec![]),
        );
        docs.insert(
            "did:plc:reposter".to_owned(),
            document("did:plc:reposter", &pds, vec![]),
        );
        let identity = Arc::new(IdentityResolver::new(
            FakeIdentity(docs),
            Duration::from_secs(30),
        ));
        let client = client(identity);
        let storage = MemoryStorage::default();
        let indexed_first = indexed_post("did:plc:indexed", "first", "first indexed post");
        let indexed_last = indexed_post("did:plc:indexed", "last", "last indexed post");
        storage.put_record(&indexed_first).unwrap();
        storage.put_record(&indexed_last).unwrap();
        let missing = "at://did:plc:author/app.bsky.feed.post/missing";
        let skeleton = super::FeedSkeletonPage {
            feed: vec![
                super::FeedSkeletonEntry {
                    post_uri: indexed_first.uri.clone(),
                    reason: None,
                },
                super::FeedSkeletonEntry {
                    post_uri: "at://did:plc:author/app.bsky.feed.post/live".into(),
                    reason: Some(
                        json!({ "$type":"app.bsky.feed.defs#reasonRepost", "by":{"did":"did:plc:reposter"} }),
                    ),
                },
                super::FeedSkeletonEntry {
                    post_uri: missing.into(),
                    reason: None,
                },
                super::FeedSkeletonEntry {
                    post_uri: indexed_last.uri.clone(),
                    reason: None,
                },
            ],
            cursor: Some("next-cursor".into()),
        };

        let page = client.hydrate_skeleton(skeleton, &storage).await.unwrap();
        let uris = page
            .feed
            .iter()
            .map(|item| item.record.uri.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            uris,
            [
                indexed_first.uri.as_str(),
                "at://did:plc:author/app.bsky.feed.post/live",
                indexed_last.uri.as_str()
            ]
        );
        assert_eq!(page.feed[1].record.record["text"], "live hydration");
        assert_eq!(
            page.feed[1].reposted_by.as_deref(),
            Some("did:plc:reposter")
        );
        assert_eq!(page.dropped_count, 1);
        assert_eq!(page.cursor.as_deref(), Some("next-cursor"));
        server.abort();
    }

    #[tokio::test]
    async fn feed_hydration_batches_public_posts_in_groups_of_twenty_five() {
        use crate::ingest::{MemoryStorage, Storage};

        let uris = (0..51)
            .map(|index| format!("at://did:plc:batch{index}/app.bsky.feed.post/p{index}"))
            .collect::<Vec<_>>();
        let posts = uris
            .iter()
            .map(|uri| (uri.clone(), public_post(uri)))
            .collect::<HashMap<_, _>>();
        let (pds, state, server) = batch_server(posts, HashMap::new(), StatusCode::OK).await;
        let pds_endpoint = pds.trim_end_matches("/xrpc/").to_owned();
        let identity = fake_batch_identity(
            uris.iter()
                .map(|uri| uri.split('/').nth(2).unwrap().to_owned()),
            &pds_endpoint,
        );
        let client = test_client_with_appview(identity, pds);
        let skeleton = super::FeedSkeletonPage {
            feed: uris
                .into_iter()
                .map(|post_uri| super::FeedSkeletonEntry {
                    post_uri,
                    reason: None,
                })
                .collect(),
            cursor: None,
        };

        let storage = MemoryStorage::default();
        let page = client.hydrate_skeleton(skeleton, &storage).await.unwrap();

        assert_eq!(page.feed.len(), 51);
        let mut call_sizes = state.appview_calls.lock().unwrap().clone();
        call_sizes.sort_unstable();
        assert_eq!(call_sizes, [1, 25, 25]);
        assert_eq!(*state.pds_calls.lock().unwrap(), 0);
        let author_profile = storage
            .get_record("did:plc:batch0", "app.bsky.actor.profile", "self")
            .unwrap()
            .unwrap();
        assert_eq!(author_profile.record["publicHandle"], "batch.test");
        assert_eq!(author_profile.record["displayName"], "Batch author");
        server.abort();
    }

    #[tokio::test]
    async fn feed_hydration_falls_back_to_author_pds_when_public_appview_is_down() {
        use crate::ingest::MemoryStorage;

        let uri = "at://did:plc:fallback/app.bsky.feed.post/p1";
        let fallback_posts = HashMap::from([(uri.to_owned(), public_post(uri))]);
        let (pds, state, server) = batch_server(
            HashMap::new(),
            fallback_posts,
            StatusCode::SERVICE_UNAVAILABLE,
        )
        .await;
        let pds_endpoint = pds.trim_end_matches("/xrpc/").to_owned();
        let identity = fake_batch_identity(["did:plc:fallback".to_owned()], &pds_endpoint);
        let client = test_client_with_appview(identity, pds);
        let page = client
            .hydrate_skeleton(
                super::FeedSkeletonPage {
                    feed: vec![super::FeedSkeletonEntry {
                        post_uri: uri.to_owned(),
                        reason: None,
                    }],
                    cursor: None,
                },
                &MemoryStorage::default(),
            )
            .await
            .unwrap();

        assert_eq!(page.feed.len(), 1);
        assert_eq!(page.feed[0].record.uri, uri);
        assert_eq!(*state.appview_calls.lock().unwrap(), [1]);
        assert_eq!(*state.pds_calls.lock().unwrap(), 1);
        server.abort();
    }

    fn indexed_post(did: &str, rkey: &str, text: &str) -> IndexedRecord {
        IndexedRecord {
            did: did.into(),
            collection: "app.bsky.feed.post".into(),
            rkey: rkey.into(),
            uri: format!("at://{did}/app.bsky.feed.post/{rkey}"),
            cid: None,
            time_us: 1,
            record: json!({"$type":"app.bsky.feed.post","text":text,"createdAt":"2026-10-28T12:00:00Z"}),
        }
    }

    #[tokio::test]
    async fn skeleton_request_uses_service_jwt_cursor_and_expected_xrpc_path() {
        let captured = Arc::new(Mutex::new(None::<(String, String)>));
        let capture = captured.clone();
        let app = Router::new().route("/fg/xrpc/app.bsky.feed.getFeedSkeleton", get(move |uri: Uri, headers: HeaderMap| {
            let capture = capture.clone();
            async move {
                *capture.lock().unwrap() = Some((headers.get("authorization").and_then(|value| value.to_str().ok()).unwrap_or("").to_owned(), uri.to_string()));
                axum::Json(json!({"feed":[{"post":"at://did:plc:author/app.bsky.feed.post/a"}],"cursor":"next"}))
            }
        }));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let feed = ResolvedFeed {
            uri: "at://did:plc:creator/app.bsky.feed.generator/jwt-log-test".into(),
            display_name: "Example".into(),
            description: None,
            avatar: None,
            service_did: "did:plc:service".into(),
            endpoint: format!("http://localhost:{}/fg", address.port()),
        };
        let client = client(resolver("".to_owned(), "".to_owned()));
        let logs = LogBuffer::default();
        let subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(logs.clone())
            .finish();
        let page = client
            .get_skeleton(&feed, Some("older + cursor"), Some("jwt-secret-test"))
            .with_subscriber(subscriber)
            .await
            .unwrap();
        assert_eq!(page.cursor.as_deref(), Some("next"));
        assert_eq!(page.feed.len(), 1);
        let first_capture = captured.lock().unwrap().clone().unwrap();
        assert_eq!(first_capture.0, "Bearer jwt-secret-test");
        assert!(
            first_capture
                .1
                .starts_with("/fg/xrpc/app.bsky.feed.getFeedSkeleton?")
        );
        assert!(
            first_capture
                .1
                .contains("feed=at%3A%2F%2Fdid%3Aplc%3Acreator")
        );
        assert!(first_capture.1.contains("cursor=older+%2B+cursor"));
        assert!(first_capture.1.contains("limit=30"));
        let fallback_subscriber = tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_writer(logs.clone())
            .finish();
        client
            .get_skeleton(&feed, None, None)
            .with_subscriber(fallback_subscriber)
            .await
            .unwrap();
        assert_eq!(captured.lock().unwrap().as_ref().unwrap().0, "");
        assert!(
            !String::from_utf8(logs.0.lock().unwrap().clone())
                .unwrap()
                .contains("jwt-secret-test")
        );
        server.abort();
    }

    #[test]
    fn saved_feed_preferences_keep_v2_items_and_prefer_v2_over_legacy() {
        let response = json!({
            "preferences": [
                {
                    "$type": "app.bsky.actor.defs#savedFeedsPref",
                    "pinned": ["at://did:plc:legacy/app.bsky.feed.generator/old"],
                    "saved": ["at://did:plc:legacy/app.bsky.feed.generator/old"]
                },
                {
                    "$type": "app.bsky.actor.defs#savedFeedsPrefV2",
                    "items": [
                        {"id":"1", "type":"feed", "value":"at://did:plc:one/app.bsky.feed.generator/a", "pinned":false},
                        {"id":"2", "type":"timeline", "value":"following", "pinned":true},
                        {"id":"3", "type":"feed", "value":"at://did:plc:two/app.bsky.feed.generator/b", "pinned":true},
                        {"id":"4", "type":"list", "value":"at://did:plc:list/app.bsky.graph.list/x", "pinned":true},
                        {"id":"5", "type":"futureType", "value":"opaque", "pinned":false}
                    ]
                }
            ]
        });

        let parsed = parse_saved_feed_preferences(&response);
        assert_eq!(parsed.len(), 5);
        assert_eq!(parsed[1].feed_type, "timeline");
        assert_eq!(parsed[4].feed_type, "futureType");
        let pinned_feeds = parsed
            .iter()
            .filter(|item| item.feed_type == "feed" && item.pinned)
            .map(|item| item.value.as_str())
            .collect::<Vec<_>>();
        assert_eq!(pinned_feeds, ["at://did:plc:two/app.bsky.feed.generator/b"]);
        assert!(!parsed.iter().any(|item| item.value.ends_with("/old")));
    }

    #[test]
    fn legacy_saved_feed_preferences_are_used_when_v2_is_absent() {
        let response = json!({
            "preferences": [{
                "$type": "app.bsky.actor.defs#savedFeedsPref",
                "pinned": ["at://did:plc:one/app.bsky.feed.generator/pinned"],
                "saved": [
                    "at://did:plc:one/app.bsky.feed.generator/pinned",
                    "at://did:plc:two/app.bsky.feed.generator/other"
                ]
            }]
        });

        let parsed = parse_saved_feed_preferences(&response);
        assert_eq!(parsed.len(), 2);
        assert!(parsed[0].pinned);
        assert!(!parsed[1].pinned);
    }

    #[test]
    fn feed_generator_blob_avatar_uses_the_feed_thumbnail_url() {
        let avatar = json!({
            "ref": {"$link":"bafkreib7t6y5x4w3v2u1"},
            "mimeType":"image/jpeg",
            "size":1234
        });
        assert_eq!(
            super::feed_avatar_url(&avatar, "did:plc:creator"),
            Some("https://cdn.bsky.app/img/feed_thumbnail/plain/did:plc:creator/bafkreib7t6y5x4w3v2u1@jpeg".into())
        );
    }
}
