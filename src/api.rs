use crate::{
    identity::IdentityBackend,
    ingest::{IndexedRecord, Storage, StorageError},
    jwt::{ServiceClaims, ServiceJwtVerifier},
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

struct ApiState<S: Storage, B: IdentityBackend> {
    storage: Arc<S>,
    jwt: Arc<ServiceJwtVerifier<B>>,
    appview_did: String,
}

impl<S: Storage, B: IdentityBackend> Clone for ApiState<S, B> {
    fn clone(&self) -> Self {
        Self {
            storage: self.storage.clone(),
            jwt: self.jwt.clone(),
            appview_did: self.appview_did.clone(),
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
    let state = ApiState {
        storage,
        jwt,
        appview_did: appview_did.into(),
    };
    Router::new()
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
        .with_state(state)
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
    Ok(Json(profile_view(&state, &did)?))
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
    let mut profiles = Vec::new();
    for actor in actors.iter().take(25) {
        if let Ok(did) = actor_to_did(&state, actor) {
            profiles.push(profile_view(&state, &did)?);
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
    let before = query
        .get("cursor")
        .and_then(|cursor| cursor.parse::<u64>().ok());
    let records = state.storage.records().map_err(|_| ApiError::internal())?;
    let page_records = records
        .iter()
        .filter(|record| record.did == actor && record.collection == POST)
        .filter(|record| before.is_none_or(|cursor| record.time_us < cursor))
        .take(limit)
        .collect::<Vec<_>>();
    let next_cursor = if page_records.len() == limit {
        page_records.last().map(|record| record.time_us.to_string())
    } else {
        None
    };
    let feed = page_records
        .iter()
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
    if let Some(next_cursor) = next_cursor {
        result["cursor"] = json!(next_cursor);
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
    let records = state.storage.records().map_err(|_| ApiError::internal())?;
    let posts = uris
        .iter()
        .take(25)
        .filter_map(|uri| {
            records
                .iter()
                .find(|record| record.collection == POST && record.uri == uri.as_str())
        })
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
    let records = state.storage.records().map_err(|_| ApiError::internal())?;
    let post = records
        .iter()
        .find(|record| record.collection == POST && record.uri == uri)
        .ok_or_else(|| ApiError::not_found("post not found"))?;
    let parent_record = post
        .record
        .pointer("/reply/parent/uri")
        .and_then(Value::as_str)
        .and_then(|parent_uri| {
            records
                .iter()
                .find(|record| record.collection == POST && record.uri == parent_uri)
        });
    let parent = if let Some(parent) = parent_record {
        json!({
            "$type":"app.bsky.feed.defs#threadViewPost",
            "post":post_view(&state, parent, claims.as_ref().map(|claims| claims.iss.as_str()))?,
            "parent":null,
            "replies":[],
            "threadContext":null
        })
    } else {
        Value::Null
    };
    let replies = records.iter().filter(|record| record.collection == POST
        && record.record.pointer("/reply/parent/uri").and_then(Value::as_str) == Some(uri))
        .map(|record| post_view(&state, record, claims.as_ref().map(|claims| claims.iss.as_str())).map(|post| json!({"$type":"app.bsky.feed.defs#threadViewPost","post":post,"parent":null,"replies":[],"threadContext":null})))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Json(
        json!({"thread":{"$type":"app.bsky.feed.defs#threadViewPost","post":post_view(&state, post, claims.as_ref().map(|claims| claims.iss.as_str()))?,"parent":parent,"replies":replies,"threadContext":null}}),
    ))
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
    let records = state.storage.records().map_err(|_| ApiError::internal())?;
    let feed = timeline_records(&records, &actor, query_limit(&query, 50))
        .into_iter()
        .map(|record| post_view(&state, record, Some(&actor)).map(|post| json!({"post":post})))
        .collect::<Result<Vec<_>, _>>()?;
    Ok(Json(json!({"feed":feed})))
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
        .take(limit.clamp(1, 100))
        .collect()
}

async fn get_likes<S: Storage, B: IdentityBackend>(
    State(state): State<ApiState<S, B>>,
    headers: HeaderMap,
    Query(query): Query<HashMap<String, String>>,
) -> Result<Json<Value>, ApiError> {
    let _ = authorize(&state, &headers, "app.bsky.feed.getLikes", false).await?;
    let uri = required(&query, "uri")?;
    let records = state.storage.records().map_err(|_| ApiError::internal())?;
    let likes = records.iter().filter(|record| record.collection == LIKE && subject_uri(record) == Some(uri))
        .take(query_limit(&query, 50)).map(|record| json!({
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
    let records = state.storage.records().map_err(|_| ApiError::internal())?;
    let reposted_by = records
        .iter()
        .filter(|record| record.collection == REPOST && subject_uri(record) == Some(uri))
        .take(query_limit(&query, 50))
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
    let records = state.storage.records().map_err(|_| ApiError::internal())?;
    let follows = records
        .iter()
        .filter(|record| record.collection == FOLLOW && record.did == actor)
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
    let records = state.storage.records().map_err(|_| ApiError::internal())?;
    let followers = records
        .iter()
        .filter(|record| {
            record.collection == FOLLOW
                && record.record.get("subject").and_then(Value::as_str) == Some(actor.as_str())
        })
        .take(query_limit(&query, 50))
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
    let records = state.storage.records().map_err(|_| ApiError::internal())?;
    let mut notifications = notification_records(&records, &actor).into_iter().map(|(record, reason, subject)| json!({
        "$type":"app.bsky.notification.listNotifications#notification",
        "uri":record.uri,"cid":record.cid,"author":profile_view(&state, &record.did).unwrap_or_else(|_| json!({"did":record.did,"handle":"handle.invalid"})),
        "reason":reason,"reasonSubject":subject,"record":record.record,"isRead":false,
        "indexedAt":record.record.get("createdAt").cloned().unwrap_or(Value::Null)
    })).collect::<Vec<_>>();
    notifications.truncate(query_limit(&query, 50));
    Ok(Json(
        json!({"notifications":notifications,"seenAt":now_rfc3339()}),
    ))
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
    let records = state.storage.records().map_err(|_| ApiError::internal())?;
    Ok(Json(
        json!({"count":notification_records(&records, &actor).len()}),
    ))
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
        .map_err(|error| ApiError::unauthorized(error.to_string()))
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
        .clamp(1, 100)
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
    let profile = storage.get_record(did, ACTOR_PROFILE, "self")?;
    let record = profile.as_ref().map(|profile| &profile.record);
    let all_records = storage.records()?;
    let follows = all_records
        .iter()
        .filter(|entry| entry.collection == FOLLOW && entry.did == did)
        .count();
    let followers = all_records
        .iter()
        .filter(|entry| {
            entry.collection == FOLLOW
                && entry.record.get("subject").and_then(Value::as_str) == Some(did)
        })
        .count();
    let posts = all_records
        .iter()
        .filter(|entry| entry.collection == POST && entry.did == did)
        .count();
    let handle = storage
        .verified_handle(did)?
        .unwrap_or_else(|| "handle.invalid".to_owned());
    let mut view = json!({
        "did":did,
        "handle":handle,
        "followersCount":followers,
        "followsCount":follows,
        "postsCount":posts,
        "labels":[],
        "verification":{"verifications":[],"verifiedStatus":"none","trustedVerifierStatus":"none"}
    });
    if let Some(record) = record {
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
        "replyCount":interactions.reply_count,"repostCount":interactions.repost_count,"likeCount":interactions.like_count,"quoteCount":0,"bookmarkCount":0,"labels":[]
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

fn subject_uri(record: &IndexedRecord) -> Option<&str> {
    record
        .record
        .pointer("/subject/uri")
        .and_then(Value::as_str)
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

fn notification_records<'a>(
    records: &'a [IndexedRecord],
    actor: &str,
) -> Vec<(&'a IndexedRecord, &'static str, Option<String>)> {
    let mut result = records
        .iter()
        .filter_map(|record| {
            if record.collection == FOLLOW
                && record.record.get("subject").and_then(Value::as_str) == Some(actor)
            {
                return Some((record, "follow", None));
            }
            if record.collection == LIKE
                && subject_uri(record).is_some_and(|uri| uri_did(uri) == Some(actor))
            {
                return Some((record, "like", subject_uri(record).map(str::to_owned)));
            }
            if record.collection == REPOST
                && subject_uri(record).is_some_and(|uri| uri_did(uri) == Some(actor))
            {
                return Some((record, "repost", subject_uri(record).map(str::to_owned)));
            }
            None
        })
        .collect::<Vec<_>>();
    result.sort_by_key(|(record, _, _)| std::cmp::Reverse(record.time_us));
    result
}

fn uri_did(uri: &str) -> Option<&str> {
    let rest = uri.strip_prefix("at://")?;
    let (did, _) = rest.split_once('/')?;
    did.starts_with("did:").then_some(did)
}

fn now_rfc3339() -> String {
    time::OffsetDateTime::now_utc()
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap_or_else(|_| "1970-01-01T00:00:00Z".to_owned())
}
