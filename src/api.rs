use crate::{
    http_security,
    identity::IdentityBackend,
    ingest::{IndexedRecord, NotificationCursor, RecordCursor, Storage, StorageError},
    jwt::{ServiceClaims, ServiceJwtVerifier},
    profile_counts::{ProfileCountsClient, apply_profile_counts},
};
use axum::{
    Json, Router,
    extract::{Query, RawQuery, State},
    http::{HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::get,
};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, HashSet},
    sync::Arc,
};
use thiserror::Error;

const ACTOR_PROFILE: &str = "app.bsky.actor.profile";
const POST: &str = "app.bsky.feed.post";
const LIKE: &str = "app.bsky.feed.like";
const REPOST: &str = "app.bsky.feed.repost";
const FOLLOW: &str = "app.bsky.graph.follow";
const MAX_XRPC_PAGE_SIZE: usize = 100;

struct ApiState<S: Storage, B: IdentityBackend> {
    storage: Arc<S>,
    jwt: Arc<ServiceJwtVerifier<B>>,
    appview_did: String,
    profile_counts: ProfileCountsClient,
}

impl<S: Storage, B: IdentityBackend> Clone for ApiState<S, B> {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            jwt: self.jwt.clone(),
            appview_did: self.appview_did.clone(),
            profile_counts: self.profile_counts.clone(),
        }
    }
}

#[derive(Debug, Error)]
#[error("{status}: {message}")]
struct ApiError {
    status: StatusCode,
    error: &'static str,
    message: String,
}

impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            error: "InvalidRequest",
            message: message.into(),
        }
    }
    fn unauthorized(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::UNAUTHORIZED,
            error: "AuthenticationRequired",
            message: message.into(),
        }
    }
    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            error: "NotFound",
            message: message.into(),
        }
    }
    fn internal() -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            error: "InternalServerError",
            message: "storage operation failed".into(),
        }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(json!({"error":self.error,"message":self.message})),
        )
            .into_response()
    }
}

pub fn api_router<S, B>(
    storage: Arc<S>,
    jwt: Arc<ServiceJwtVerifier<B>>,
    appview_did: impl Into<String>,
) -> Router
where
    S: Storage + 'static,
    B: IdentityBackend + 'static,
{
    api_router_with_https_and_profile_counts(
        storage,
        jwt,
        appview_did,
        false,
        ProfileCountsClient::local_only(),
    )
}

pub fn api_router_with_https<S, B>(
    storage: Arc<S>,
    jwt: Arc<ServiceJwtVerifier<B>>,
    appview_did: impl Into<String>,
    public_https: bool,
) -> Router
where
    S: Storage + 'static,
    B: IdentityBackend + 'static,
{
    api_router_with_https_and_profile_counts(
        storage,
        jwt,
        appview_did,
        public_https,
        ProfileCountsClient::local_only(),
    )
}

pub fn api_router_with_profile_counts<S, B>(
    storage: Arc<S>,
    jwt: Arc<ServiceJwtVerifier<B>>,
    appview_did: impl Into<String>,
    profile_counts: ProfileCountsClient,
) -> Router
where
    S: Storage + 'static,
    B: IdentityBackend + 'static,
{
    api_router_with_https_and_profile_counts(storage, jwt, appview_did, false, profile_counts)
}

pub fn api_router_with_https_and_profile_counts<S, B>(
    storage: Arc<S>,
    jwt: Arc<ServiceJwtVerifier<B>>,
    appview_did: impl Into<String>,
    public_https: bool,
    profile_counts: ProfileCountsClient,
) -> Router
where
    S: Storage + 'static,
    B: IdentityBackend + 'static,
{
    let state = ApiState {
        storage,
        jwt,
        appview_did: appview_did.into(),
        profile_counts,
    };
    let router = Router::new()
        .route("/.well-known/did.json", get(did_document::<S, B>))
        .route("/xrpc/app.bsky.actor.getProfile", get(get_profile::<S, B>))
        .route(
            "/xrpc/app.bsky.actor.getProfiles",
            get(get_profiles::<S, B>),
        )
        .route(
            "/xrpc/app.bsky.feed.getAuthorFeed",
            get(get_author_feed::<S, B>),
        )
        .route("/xrpc/app.bsky.feed.getPosts", get(get_posts::<S, B>))
        .route(
            "/xrpc/app.bsky.feed.getPostThread",
            get(get_post_thread::<S, B>),
        )
        .route("/xrpc/app.bsky.feed.getTimeline", get(get_timeline::<S, B>))
        .route("/xrpc/app.bsky.feed.getLikes", get(get_likes::<S, B>))
        .route(
            "/xrpc/app.bsky.feed.getRepostedBy",
            get(get_reposted_by::<S, B>),
        )
        .route("/xrpc/app.bsky.graph.getFollows", get(get_follows::<S, B>))
        .route(
            "/xrpc/app.bsky.graph.getFollowers",
            get(get_followers::<S, B>),
        )
        .route(
            "/xrpc/app.bsky.notification.listNotifications",
            get(list_notifications::<S, B>),
        )
        .route(
            "/xrpc/app.bsky.notification.getUnreadCount",
            get(get_unread_count::<S, B>),
        )
        .with_state(state);
    http_security::protect(router, public_https)
}

async fn did_document<S: Storage, B: IdentityBackend>(
    State(state): State<ApiState<S, B>>,
) -> Json<Value> {
    let host = state
        .appview_did
        .strip_prefix("did:web:")
        .unwrap_or("localhost")
        .replace("%3A", ":");
    let service_endpoint = format!("https://{host}");
    Json(json!({
        "@context":"https://www.w3.org/ns/did/v1",
        "id":state.appview_did,
        "service":[{"id":format!("{}#bsky_appview", state.appview_did),"type":"BskyAppView","serviceEndpoint":service_endpoint}]
    }))
}

async fn get_profile<S: Storage, B: IdentityBackend>(
    State(state): State<ApiState<S, B>>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, ApiError> {
    let _ = authorize(&state, &headers, "app.bsky.actor.getProfile", false).await?;
    let actor = required(&query, "actor")?;
    let did = actor_to_did(&state, actor)?;
    let mut profile = profile_view(&state, &did)?;
    if let Some(counts) = state.profile_counts.get_counts(&did).await {
        apply_profile_counts(&mut profile, counts);
    }
    Ok(Json(profile))
}

async fn get_profiles<S: Storage, B: IdentityBackend>(
    State(state): State<ApiState<S, B>>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Result<Json<Value>, ApiError> {
    let _ = authorize(&state, &headers, "app.bsky.actor.getProfiles", false).await?;
    let actors = repeated_query_values(raw_query.as_deref(), "actors");
    if actors.is_empty() {
        return Err(ApiError::bad_request("missing query parameter actors"));
    }
    let mut profile_dids = Vec::new();
    let mut profiles = Vec::new();
    for actor in actors.iter().take(25) {
        if let Ok(did) = actor_to_did(&state, actor) {
            profiles.push(profile_view(&state, &did)?);
            profile_dids.push(did);
        }
    }
    let remote_counts = state.profile_counts.get_counts_many(&profile_dids).await;
    for (did, profile) in profile_dids.iter().zip(&mut profiles) {
        if let Some(counts) = remote_counts.get(did) {
            apply_profile_counts(profile, *counts);
        }
    }
    Ok(Json(json!({"profiles":profiles})))
}

async fn get_author_feed<S: Storage, B: IdentityBackend>(
    State(state): State<ApiState<S, B>>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, ApiError> {
    let claims = authorize(&state, &headers, "app.bsky.feed.getAuthorFeed", false).await?;
    let actor = actor_to_did(&state, required(&query, "actor")?)?;
    let limit = query_limit(&query, 50);
    let cursor = query
        .get("cursor")
        .and_then(|cursor| RecordCursor::parse(cursor));
    let include_replies = query
        .get("filter")
        .is_none_or(|filter| filter != "posts_no_replies");
    let page_records = state
        .storage
        .author_posts(&actor, cursor.as_ref(), limit + 1, include_replies)
        .map_err(|_| ApiError::internal())?;
    let feed = page_records
        .iter()
        .take(limit)
        .map(|record| {
            post_view(
                &state,
                record,
                claims.as_ref().map(|claims| claims.iss.as_str()),
            )
            .map(|post| json!({"post":post}))
        })
        .collect::<Result<Vec<_>, _>>()?;
    let mut result = json!({"feed":feed});
    if page_records.len() > limit
        && let Some(last) = page_records.get(limit - 1)
    {
        result["cursor"] = json!(
            RecordCursor {
                time_us: last.time_us,
                uri: last.uri.clone(),
            }
            .encode()
        );
    }
    Ok(Json(result))
}

async fn get_posts<S: Storage, B: IdentityBackend>(
    State(state): State<ApiState<S, B>>,
    headers: HeaderMap,
    RawQuery(raw_query): RawQuery,
) -> Result<Json<Value>, ApiError> {
    let claims = authorize(&state, &headers, "app.bsky.feed.getPosts", false).await?;
    let uris = repeated_query_values(raw_query.as_deref(), "uris");
    if uris.is_empty() {
        return Err(ApiError::bad_request("missing query parameter uris"));
    }
    let mut records = Vec::new();
    for uri in uris.iter().take(25) {
        if let Some(record) = state
            .storage
            .record_by_uri(POST, uri)
            .map_err(|_| ApiError::internal())?
        {
            records.push(record);
        }
    }
    let posts = records
        .iter()
        .map(|record| {
            post_view(
                &state,
                record,
                claims.as_ref().map(|claims| claims.iss.as_str()),
            )
        })
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Json(json!({"posts":posts})))
}

async fn get_post_thread<S: Storage, B: IdentityBackend>(
    State(state): State<ApiState<S, B>>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, ApiError> {
    let claims = authorize(&state, &headers, "app.bsky.feed.getPostThread", false).await?;
    let uri = required(&query, "uri")?;
    let post = state
        .storage
        .record_by_uri(POST, uri)
        .map_err(|_| ApiError::internal())?
        .ok_or_else(|| ApiError::not_found("post not found"))?;
    let depth = query
        .get("depth")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(6)
        .min(6);
    let parent_height = query
        .get("parentHeight")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(80)
        .min(20);
    let viewer_did = claims.as_ref().map(|claims| claims.iss.as_str());
    let mut budget = 100;
    let thread = thread_view(
        &state,
        &post,
        depth,
        parent_height,
        viewer_did,
        &mut HashSet::new(),
        &mut budget,
    )?;
    Ok(Json(json!({"thread":thread})))
}

fn thread_view<S: Storage, B: IdentityBackend>(
    state: &ApiState<S, B>,
    record: &IndexedRecord,
    depth: usize,
    parent_height: usize,
    viewer_did: Option<&str>,
    visited: &mut HashSet<String>,
    budget: &mut usize,
) -> Result<Value, ApiError> {
    if *budget == 0 {
        return Ok(Value::Null);
    }
    *budget -= 1;
    if !visited.insert(record.uri.clone()) {
        return Ok(json!({
            "$type":"app.bsky.feed.defs#threadViewPost",
            "post":post_view(state, record, viewer_did)?,
            "parent":null,"replies":[],"threadContext":null
        }));
    }
    let parent = if parent_height > 0 {
        if let Some(parent_uri) = record
            .record
            .pointer("/reply/parent/uri")
            .and_then(Value::as_str)
        {
            let parent = state
                .storage
                .record_by_uri(POST, parent_uri)
                .map_err(|_| ApiError::internal())?;
            if let Some(parent) = parent {
                Some(thread_view(
                    state,
                    &parent,
                    0,
                    parent_height - 1,
                    viewer_did,
                    visited,
                    budget,
                )?)
            } else {
                None
            }
        } else {
            None
        }
    } else {
        None
    }
    .unwrap_or(Value::Null);
    let replies = if depth > 0 && *budget > 0 {
        let candidates = state
            .storage
            .replies_to(&record.uri, (*budget).min(MAX_XRPC_PAGE_SIZE))
            .map_err(|_| ApiError::internal())?;
        let mut replies = Vec::new();
        for reply in candidates {
            if *budget == 0 {
                break;
            }
            replies.push(thread_view(
                state,
                &reply,
                depth - 1,
                0,
                viewer_did,
                visited,
                budget,
            )?);
        }
        replies
    } else {
        Vec::new()
    };
    visited.remove(&record.uri);
    Ok(json!({
        "$type":"app.bsky.feed.defs#threadViewPost",
        "post":post_view(state, record, viewer_did)?,
        "parent":parent,
        "replies":replies,
        "threadContext":null
    }))
}

async fn get_timeline<S: Storage, B: IdentityBackend>(
    State(state): State<ApiState<S, B>>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, ApiError> {
    let claims = authorize(&state, &headers, "app.bsky.feed.getTimeline", true)
        .await?
        .ok_or_else(|| ApiError::unauthorized("service JWT required"))?;
    let actor = claims.iss;
    let limit = query_limit(&query, 50);
    let cursor = query
        .get("cursor")
        .and_then(|cursor| RecordCursor::parse(cursor));
    let page_records = state
        .storage
        .timeline_posts(&actor, cursor.as_ref(), limit + 1)
        .map_err(|_| ApiError::internal())?;
    let feed = page_records
        .iter()
        .take(limit)
        .map(|record| post_view(&state, record, Some(&actor)).map(|post| json!({"post":post})))
        .collect::<Result<Vec<_>, _>>()?;
    let mut result = json!({"feed":feed});
    if page_records.len() > limit
        && let Some(last) = page_records.get(limit - 1)
    {
        result["cursor"] = json!(
            RecordCursor {
                time_us: last.time_us,
                uri: last.uri.clone(),
            }
            .encode()
        );
    }
    Ok(Json(result))
}

/// Select posts for one account's local timeline. The web UI and XRPC API use
/// this same query so neither needs to proxy reads through the account's PDS.
pub fn timeline_records<'a>(
    records: &'a [IndexedRecord],
    actor: &str,
    limit: usize,
) -> Vec<&'a IndexedRecord> {
    let followed = follow_subjects(records, actor);
    records
        .iter()
        .filter(|record| {
            record.collection == POST && (record.did == actor || followed.contains(&record.did))
        })
        .take(limit.clamp(1, MAX_XRPC_PAGE_SIZE))
        .collect()
}

async fn get_likes<S: Storage, B: IdentityBackend>(
    State(state): State<ApiState<S, B>>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, ApiError> {
    let _ = authorize(&state, &headers, "app.bsky.feed.getLikes", false).await?;
    let uri = required(&query, "uri")?;
    let likes = state.storage.records_for_subject(LIKE, uri, query_limit(&query, 50))
        .map_err(|_| ApiError::internal())?.iter().map(|record| json!({
            "actor":profile_view(&state, &record.did).unwrap_or_else(|_| json!({"did":record.did,"handle":"handle.invalid"})),
            "createdAt":record.record.get("createdAt").cloned().unwrap_or(Value::Null),
            "indexedAt":record.record.get("createdAt").cloned().unwrap_or(Value::Null)
        })).collect::<Vec<_>>();
    Ok(Json(json!({"uri":uri,"likes":likes,"cursor":null})))
}

async fn get_reposted_by<S: Storage, B: IdentityBackend>(
    State(state): State<ApiState<S, B>>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, ApiError> {
    let _ = authorize(&state, &headers, "app.bsky.feed.getRepostedBy", false).await?;
    let uri = required(&query, "uri")?;
    let reposts = state
        .storage
        .records_for_subject(REPOST, uri, query_limit(&query, 50))
        .map_err(|_| ApiError::internal())?;
    let reposted_by = reposts
        .iter()
        .map(|record| profile_view(&state, &record.did))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Json(
        json!({"uri":uri,"repostedBy":reposted_by,"cursor":null}),
    ))
}

async fn get_follows<S: Storage, B: IdentityBackend>(
    State(state): State<ApiState<S, B>>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, ApiError> {
    let _ = authorize(&state, &headers, "app.bsky.graph.getFollows", false).await?;
    let actor = actor_to_did(&state, required(&query, "actor")?)?;
    let records = state
        .storage
        .follows_for_actor(&actor, query_limit(&query, 50))
        .map_err(|_| ApiError::internal())?;
    let follows = records
        .iter()
        .filter_map(|record| record.record.get("subject").and_then(Value::as_str))
        .take(query_limit(&query, 50))
        .map(|did| profile_view(&state, did))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Json(
        json!({"subject":profile_view(&state, &actor)?,"follows":follows,"cursor":null}),
    ))
}

async fn get_followers<S: Storage, B: IdentityBackend>(
    State(state): State<ApiState<S, B>>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, ApiError> {
    let _ = authorize(&state, &headers, "app.bsky.graph.getFollowers", false).await?;
    let actor = actor_to_did(&state, required(&query, "actor")?)?;
    let records = state
        .storage
        .followers_for_did(&actor, query_limit(&query, 50))
        .map_err(|_| ApiError::internal())?;
    let followers = records
        .iter()
        .map(|record| profile_view(&state, &record.did))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Json(
        json!({"subject":profile_view(&state, &actor)?,"followers":followers,"cursor":null}),
    ))
}

async fn list_notifications<S: Storage, B: IdentityBackend>(
    State(state): State<ApiState<S, B>>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, ApiError> {
    let claims = authorize(
        &state,
        &headers,
        "app.bsky.notification.listNotifications",
        true,
    )
    .await?
    .ok_or_else(|| ApiError::unauthorized("service JWT required"))?;
    let actor = claims.iss;
    let limit = query_limit(&query, 50);
    let before = query
        .get("cursor")
        .and_then(|cursor| NotificationCursor::parse(cursor));
    let page = state
        .storage
        .notifications_for(&actor, before.as_ref(), limit + 1)
        .map_err(|_| ApiError::internal())?;
    let has_more = page.len() > limit;
    let notifications = page.iter().take(limit).map(|notice| {
        let record = &notice.record;
        json!({
        "$type":"app.bsky.notification.listNotifications#notification",
        "uri":record.uri,"cid":record.cid,"author":profile_view(&state, &record.did).unwrap_or_else(|_| json!({"did":record.did,"handle":"handle.invalid"})),
        "reason":notice.reason,"reasonSubject":notice.reason_subject,"record":record.record,"isRead":false,
        "indexedAt":record.record.get("createdAt").cloned().unwrap_or_else(|| json!(timestamp_us_to_rfc3339(record.time_us)))
    })}).collect::<Vec<_>>();
    let mut result = json!({"notifications":notifications,"seenAt":now_rfc3339()});
    if has_more && let Some(last) = page.get(limit - 1) {
        result["cursor"] = json!(
            NotificationCursor {
                time_us: last.record.time_us,
                uri: last.record.uri.clone(),
                reason: last.reason.clone(),
            }
            .encode()
        );
    }
    Ok(Json(result))
}

async fn get_unread_count<S: Storage, B: IdentityBackend>(
    State(state): State<ApiState<S, B>>,
    headers: HeaderMap,
) -> Result<Json<Value>, ApiError> {
    let claims = authorize(
        &state,
        &headers,
        "app.bsky.notification.getUnreadCount",
        true,
    )
    .await?
    .ok_or_else(|| ApiError::unauthorized("service JWT required"))?;
    let actor = claims.iss;
    let count = state
        .storage
        .unread_notification_count(&actor)
        .map_err(|_| ApiError::internal())?;
    Ok(Json(json!({"count":count})))
}

async fn authorize<S: Storage, B: IdentityBackend>(
    state: &ApiState<S, B>,
    headers: &HeaderMap,
    lexicon: &str,
    required: bool,
) -> Result<Option<ServiceClaims>, ApiError> {
    let Some(value) = headers.get(axum::http::header::AUTHORIZATION) else {
        return if required {
            Err(ApiError::unauthorized("service JWT required"))
        } else {
            Ok(None)
        };
    };
    let value = value
        .to_str()
        .map_err(|_| ApiError::unauthorized("invalid authorization header"))?;
    let token = value
        .strip_prefix("Bearer ")
        .ok_or_else(|| ApiError::unauthorized("Bearer service JWT required"))?;
    state
        .jwt
        .verify(token, lexicon)
        .await
        .map(Some)
        .map_err(|_| ApiError::unauthorized("invalid service JWT"))
}

fn required<'a>(query: &'a HashMap<String, String>, key: &str) -> Result<&'a str, ApiError> {
    query
        .get(key)
        .map(String::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| ApiError::bad_request(format!("missing query parameter {key}")))
}

fn repeated_query_values(query: Option<&str>, key: &str) -> Vec<String> {
    query
        .map(|query| url::form_urlencoded::parse(query.as_bytes()))
        .into_iter()
        .flatten()
        .filter(|(name, _)| name == key)
        .map(|(_, value)| value.into_owned())
        .collect()
}

fn query_limit(query: &HashMap<String, String>, default: usize) -> usize {
    query
        .get("limit")
        .and_then(|value| value.parse::<usize>().ok())
        .unwrap_or(default)
        .clamp(1, MAX_XRPC_PAGE_SIZE)
}

fn actor_to_did<S: Storage, B: IdentityBackend>(
    state: &ApiState<S, B>,
    actor: &str,
) -> Result<String, ApiError> {
    if actor.starts_with("did:") {
        return Ok(actor.to_owned());
    }
    state
        .storage
        .did_by_verified_handle(actor)
        .map_err(|_| ApiError::internal())?
        .ok_or_else(|| ApiError::not_found("actor not found"))
}

fn profile_view<S: Storage, B: IdentityBackend>(
    state: &ApiState<S, B>,
    did: &str,
) -> Result<Value, ApiError> {
    profile_view_from_storage(state.storage.as_ref(), did).map_err(|_| ApiError::internal())
}

pub(crate) fn profile_view_from_storage<S: Storage>(
    storage: &S,
    did: &str,
) -> Result<Value, StorageError> {
    let mut view = profile_card_view_from_storage(storage, did)?;
    let (follows, followers, posts) = storage.profile_counts(did)?;
    view["followersCount"] = json!(followers);
    view["followsCount"] = json!(follows);
    view["postsCount"] = json!(posts);
    Ok(view)
}

/// The identity a post card shows: the same handle rules, display fields,
/// avatars and labels as the full profile view, minus the follower/follows/
/// post counts. Cards never display those numbers, and fetching them means a
/// three-subquery COUNT per lookup, so rendering uses this view.
pub(crate) fn profile_card_view_from_storage<S: Storage>(
    storage: &S,
    did: &str,
) -> Result<Value, StorageError> {
    let profile = storage.get_record(did, ACTOR_PROFILE, "self")?;
    let record = profile.as_ref().map(|profile| &profile.record);
    let public_handle = record
        .and_then(|record| record.get("publicHandle"))
        .and_then(Value::as_str)
        .filter(|handle| !handle.trim().is_empty() && *handle != "handle.invalid");
    let handle = if let Some(handle) = storage.verified_handle(did)? {
        handle
    } else if storage.handle_is_invalid(did)? {
        "handle.invalid".to_owned()
    } else {
        if storage.is_tracked_did(did)? || storage.is_displayed_did(did)? {
            storage.queue_handle_refresh(did)?;
        }
        public_handle.unwrap_or(did).to_owned()
    };
    let mut view = json!({
        "did":did,
        "handle":handle,
        "labels":[],
        "verification":{"verifications":[],"verifiedStatus":"none","trustedVerifierStatus":"none"}
    });
    if let Some(record) = record {
        if let Some(labels) = record.get("labels").filter(|labels| labels.is_array()) {
            view["labels"] = labels.clone();
        }
        if let Some(avatar) = record
            .get("publicAvatar")
            .and_then(Value::as_str)
            .and_then(|avatar| url::Url::parse(avatar).ok())
            .filter(|avatar| {
                avatar.scheme() == "https"
                    && avatar.host_str() == Some("cdn.bsky.app")
                    && avatar.username().is_empty()
                    && avatar.password().is_none()
            })
        {
            view["avatar"] = json!(avatar.as_str());
        }
        for field in [
            "displayName",
            "description",
            "createdAt",
            "indexedAt",
            "associated",
            "status",
        ] {
            if let Some(value) = record.get(field) {
                view[field] = value.clone();
            }
        }
        if let Some(avatar) = record
            .get("avatar")
            .and_then(|blob| blob_url(did, blob, "avatar"))
        {
            view["avatar"] = json!(avatar);
        }
        if let Some(banner) = record
            .get("banner")
            .and_then(|blob| blob_url(did, blob, "banner"))
        {
            view["banner"] = json!(banner);
        }
    }
    Ok(view)
}

fn blob_url(did: &str, blob: &Value, kind: &str) -> Option<String> {
    let cid = blob.pointer("/ref/$link").and_then(Value::as_str)?;
    Some(format!("https://cdn.bsky.app/img/{kind}/plain/{did}/{cid}"))
}

fn post_view<S: Storage, B: IdentityBackend>(
    state: &ApiState<S, B>,
    record: &IndexedRecord,
    viewer_did: Option<&str>,
) -> Result<Value, ApiError> {
    let interactions = state
        .storage
        .post_interaction_summary(&record.uri, viewer_did)
        .map_err(|_| ApiError::internal())?;
    let mut post = json!({
        "uri":record.uri,"cid":record.cid,"author":profile_view(state, &record.did).unwrap_or_else(|_| json!({"did":record.did,"handle":"handle.invalid"})),
        "record":record.record,
        "replyCount":interactions.reply_count,"repostCount":interactions.repost_count,"likeCount":interactions.like_count,"quoteCount":interactions.quote_count,"bookmarkCount":0,"labels":[]
    });
    if viewer_did.is_some() {
        let mut viewer = json!({});
        if let Some(like) = interactions.viewer_like {
            viewer["like"] = json!(like);
        }
        if let Some(repost) = interactions.viewer_repost {
            viewer["repost"] = json!(repost);
        }
        post["viewer"] = viewer;
    }
    if let Some(created_at) = record.record.get("createdAt") {
        post["indexedAt"] = created_at.clone();
    }
    if let Some(embed) = record.record.get("embed") {
        post["embed"] = embed.clone();
    }
    Ok(post)
}

fn follow_subjects(records: &[IndexedRecord], actor: &str) -> HashSet<String> {
    records
        .iter()
        .filter(|record| record.collection == FOLLOW && record.did == actor)
        .filter_map(|record| {
            record
                .record
                .get("subject")
                .and_then(Value::as_str)
                .map(str::to_owned)
        })
        .collect()
}

fn now_rfc3339() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_owned())
}

fn timestamp_us_to_rfc3339(time_us: u64) -> String {
    let nanos = i128::from(time_us).saturating_mul(1_000);
    time::OffsetDateTime::from_unix_timestamp_nanos(nanos)
        .ok()
        .and_then(|date| {
            date.format(&time::format_description::well_known::Rfc3339)
                .ok()
        })
        .unwrap_or_else(now_rfc3339)
}

#[cfg(test)]
mod card_view_tests {
    use super::*;
    use crate::ingest::{IndexedRecord, MemoryStorage};

    fn store_profile(storage: &MemoryStorage, did: &str, record: Value) {
        storage
            .put_record(&IndexedRecord {
                did: did.into(),
                collection: ACTOR_PROFILE.into(),
                rkey: "self".into(),
                uri: format!("at://{did}/{ACTOR_PROFILE}/self"),
                cid: None,
                time_us: 1,
                record,
            })
            .unwrap();
    }

    #[test]
    fn a_card_keeps_the_profile_labels_so_moderation_still_applies() {
        let storage = MemoryStorage::default();
        let labels = json!([{"src":"did:plc:labeler","uri":"at://did:plc:labelled/app.bsky.actor.profile/self","val":"porn","cts":"2026-11-01T00:00:00Z"}]);
        store_profile(
            &storage,
            "did:plc:labelled",
            json!({"publicHandle":"labelled.test","labels":labels}),
        );
        let card = profile_card_view_from_storage(&storage, "did:plc:labelled").unwrap();
        assert_eq!(
            card["labels"], labels,
            "a card must carry the profile labels"
        );
        let full = profile_view_from_storage(&storage, "did:plc:labelled").unwrap();
        assert_eq!(full["labels"], labels, "the full view must carry them too");
    }

    #[test]
    fn a_card_shows_a_public_avatar_only_from_the_bluesky_cdn() {
        let storage = MemoryStorage::default();
        let good = "https://cdn.bsky.app/img/avatar/plain/did:plc:good/bafkreigood@jpeg";
        store_profile(&storage, "did:plc:good", json!({"publicAvatar": good}));
        for (did, avatar) in [
            ("did:plc:other-host", "https://tracker.example/pixel.jpg"),
            (
                "did:plc:http",
                "http://cdn.bsky.app/img/avatar/plain/x/y@jpeg",
            ),
            (
                "did:plc:userinfo",
                "https://user:pw@cdn.bsky.app/img/avatar/plain/x/y@jpeg",
            ),
            (
                "did:plc:lookalike",
                "https://cdn.bsky.app.tracker.example/a.jpg",
            ),
        ] {
            store_profile(&storage, did, json!({"publicAvatar": avatar}));
            let card = profile_card_view_from_storage(&storage, did).unwrap();
            assert!(
                card.get("avatar").is_none(),
                "{avatar} must not become a card avatar: {card}"
            );
        }
        let card = profile_card_view_from_storage(&storage, "did:plc:good").unwrap();
        assert_eq!(card["avatar"], json!(good));
    }
}
