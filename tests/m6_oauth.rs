use appview_rs::{
    config::Config,
    feeds::{FeedTimeline, FeedTimelineTiming, HydratedFeedPage, ResolvedFeed},
    ingest::{IndexedRecord, Ingestor, MemoryStorage, ScopeMode, SqliteStorage, Storage},
    oauth::{
        AccountLabel, FetchedAuthorProfile, OAuthAccount, OAuthCallback, OAuthFlow, OAuthPublic,
        PdsCreatedRecord, PinnedFeedTabs, PostImage, PostStrongRef, ProfileListChoice,
        ProfileRefreshWorker, SqliteOAuthStore, WebShutdown,
    },
};
use appview_rs::{router, router_with_oauth_flow};
use atrium_api::types::string::Did;
use atrium_common::store::Store;
use atrium_oauth::store::{session::Session, state::InternalStateData};
use axum::{Router, body::Body, http::Request, http::StatusCode};
use http_body_util::BodyExt;
use rusqlite::Connection;
use serde_json::Value;
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tower::ServiceExt;
use tracing::{Event, Subscriber, field::Visit};
use tracing_subscriber::{Layer, layer::Context, prelude::*};

const EXPECTED_OAUTH_SCOPE: &str = "atproto repo:app.bsky.feed.post?action=create repo:app.bsky.feed.like?action=create&action=delete repo:app.bsky.feed.repost?action=create&action=delete blob:image/* rpc:app.bsky.actor.getPreferences?aud=* rpc:app.bsky.feed.getFeedSkeleton?aud=* blob:video/* rpc:app.bsky.video.getUploadLimits?aud=* rpc:com.atproto.repo.uploadBlob?aud=* repo:app.bsky.graph.follow?action=create&action=delete repo:app.bsky.graph.block?action=create&action=delete repo:app.bsky.graph.listitem?action=create&action=delete rpc:app.bsky.graph.muteActor?aud=* rpc:app.bsky.graph.unmuteActor?aud=* rpc:app.bsky.graph.getMutes?aud=*";

#[derive(Clone)]
struct ProfileHydrationTiming(Arc<Mutex<Option<u64>>>);

struct ProfileHydrationVisitor<'a>(&'a mut Option<u64>);

impl Visit for ProfileHydrationVisitor<'_> {
    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        if field.name() == "profile_hydration_ms" {
            *self.0 = Some(value);
        }
    }

    fn record_debug(&mut self, _: &tracing::field::Field, _: &dyn std::fmt::Debug) {}
}

impl<S: Subscriber> Layer<S> for ProfileHydrationTiming {
    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
        event.record(&mut ProfileHydrationVisitor(&mut self.0.lock().unwrap()));
    }
}

#[derive(Clone)]
struct FakeOAuthFlow {
    actions: Arc<Mutex<Vec<String>>>,
    granted_scope: Option<String>,
    browser_binding: Arc<Mutex<Option<String>>>,
    callback_failure: Option<String>,
    action_failure: Option<String>,
    pinned_feeds: Vec<ResolvedFeed>,
    pinned_feed_delay_ms: u64,
    feed_timeline_delay_ms: u64,
    author_profiles: Arc<Mutex<std::collections::HashMap<String, FetchedAuthorProfile>>>,
    profile_requests: Arc<Mutex<Vec<String>>>,
    profile_delays_ms: Arc<Mutex<std::collections::HashMap<String, u64>>>,
    live_posts: Arc<Mutex<std::collections::HashMap<String, IndexedRecord>>>,
    post_requests: Arc<Mutex<Vec<String>>>,
    post_delays_ms: Arc<Mutex<std::collections::HashMap<String, u64>>>,
    profile_lists: Arc<Mutex<Vec<ProfileListChoice>>>,
    muted_actors: Arc<Mutex<std::collections::HashSet<String>>>,
    label_requests: Arc<Mutex<Vec<Vec<String>>>>,
    account_labels: Arc<Mutex<Vec<AccountLabel>>>,
}

impl Default for FakeOAuthFlow {
    fn default() -> Self {
        Self {
            actions: Arc::new(Mutex::new(Vec::new())),
            granted_scope: Some(EXPECTED_OAUTH_SCOPE.to_owned()),
            browser_binding: Arc::new(Mutex::new(None)),
            callback_failure: None,
            action_failure: None,
            pinned_feeds: Vec::new(),
            pinned_feed_delay_ms: 0,
            feed_timeline_delay_ms: 0,
            author_profiles: Arc::new(Mutex::new(std::collections::HashMap::new())),
            profile_requests: Arc::new(Mutex::new(Vec::new())),
            profile_delays_ms: Arc::new(Mutex::new(std::collections::HashMap::new())),
            live_posts: Arc::new(Mutex::new(std::collections::HashMap::new())),
            post_requests: Arc::new(Mutex::new(Vec::new())),
            post_delays_ms: Arc::new(Mutex::new(std::collections::HashMap::new())),
            profile_lists: Arc::new(Mutex::new(Vec::new())),
            muted_actors: Arc::new(Mutex::new(std::collections::HashSet::new())),
            label_requests: Arc::new(Mutex::new(Vec::new())),
            account_labels: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl FakeOAuthFlow {
    fn with_granted_scope(scope: Option<&str>) -> Self {
        Self {
            granted_scope: scope.map(str::to_owned),
            ..Self::default()
        }
    }
}

fn fake_pds_create_record(
    did: &str,
    collection: &str,
    rkey: &str,
    cid: &str,
    record: Value,
) -> PdsCreatedRecord {
    PdsCreatedRecord {
        uri: format!("at://{did}/{collection}/{rkey}"),
        cid: cid.to_owned(),
        record,
        time_us: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64,
    }
}

fn fake_feed_post(did: &str, rkey: &str, time_us: u64, text: &str) -> IndexedRecord {
    IndexedRecord {
        did: did.to_owned(),
        collection: "app.bsky.feed.post".to_owned(),
        rkey: rkey.to_owned(),
        uri: format!("at://{did}/app.bsky.feed.post/{rkey}"),
        cid: Some(format!("{rkey}-cid")),
        time_us,
        record: serde_json::json!({
            "$type":"app.bsky.feed.post",
            "text":text,
            "createdAt":"2026-10-28T00:00:00Z"
        }),
    }
}

impl OAuthFlow for FakeOAuthFlow {
    fn authorize<'a>(
        &'a self,
        login: &'a str,
        browser_binding: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<String, String>> + Send + 'a>> {
        Box::pin(async move {
            *self.browser_binding.lock().unwrap() = Some(browser_binding.to_owned());
            Ok(format!("https://auth.example/authorize?login={login}"))
        })
    }

    fn callback<'a>(
        &'a self,
        params: OAuthCallback,
    ) -> Pin<Box<dyn Future<Output = Result<OAuthAccount, String>> + Send + 'a>> {
        Box::pin(async move {
            if let Some(error) = &self.callback_failure {
                return Err(error.clone());
            }
            let expected_binding = self.browser_binding.lock().unwrap().clone();
            if params.browser_binding != expected_binding {
                return Err("invalid browser binding".to_owned());
            }
            if params.code != "valid-code" || params.state != "valid-state" {
                return Err("invalid OAuth state or code".to_owned());
            }
            Ok(OAuthAccount {
                did: "did:plc:alice".to_owned(),
                verified_handle: Some("alice.test".to_owned()),
                granted_scope: self.granted_scope.clone(),
            })
        })
    }

    fn logout<'a>(
        &'a self,
        _: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }

    fn create_post<'a>(
        &'a self,
        did: &'a str,
        text: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<PdsCreatedRecord, String>> + Send + 'a>> {
        Box::pin(async move {
            if let Some(error) = &self.action_failure {
                return Err(error.clone());
            }
            self.actions
                .lock()
                .unwrap()
                .push(format!("post:{did}:{text}"));
            Ok(fake_pds_create_record(
                did,
                "app.bsky.feed.post",
                "pds-post-r1",
                "pds-post-cid",
                serde_json::json!({"$type":"app.bsky.feed.post","text":text,"createdAt":"2026-10-28T00:00:00Z"}),
            ))
        })
    }

    fn create_post_with_images<'a>(
        &'a self,
        did: &'a str,
        text: &'a str,
        images: Vec<PostImage>,
    ) -> Pin<Box<dyn Future<Output = Result<PdsCreatedRecord, String>> + Send + 'a>> {
        Box::pin(async move {
            self.actions
                .lock()
                .unwrap()
                .push(format!("image-post:{did}:{text}:{}", images.len()));
            let images = images
                .into_iter()
                .map(|image| {
                    serde_json::json!({
                        "image":{"$type":"blob","ref":{"$link":"bafk-test-image"},"mimeType":image.mime_type,"size":image.bytes.len()},
                        "alt":image.alt,
                        "aspectRatio":{"width":image.width,"height":image.height}
                    })
                })
                .collect::<Vec<_>>();
            Ok(fake_pds_create_record(
                did,
                "app.bsky.feed.post",
                "pds-image-r1",
                "pds-image-cid",
                serde_json::json!({
                    "$type":"app.bsky.feed.post",
                    "text":text,
                    "createdAt":"2026-10-28T00:00:00Z",
                    "embed":{"$type":"app.bsky.embed.images","images":images}
                }),
            ))
        })
    }

    fn create_reply<'a>(
        &'a self,
        did: &'a str,
        text: &'a str,
        root: PostStrongRef,
        parent: PostStrongRef,
    ) -> Pin<Box<dyn Future<Output = Result<PdsCreatedRecord, String>> + Send + 'a>> {
        Box::pin(async move {
            self.actions.lock().unwrap().push(format!(
                "reply:{did}:{text}:{}:{}:{}:{}",
                root.uri, root.cid, parent.uri, parent.cid
            ));
            Ok(fake_pds_create_record(
                did,
                "app.bsky.feed.post",
                "pds-reply-r1",
                "pds-reply-cid",
                serde_json::json!({
                    "$type":"app.bsky.feed.post",
                    "text":text,
                    "createdAt":"2026-10-28T00:00:00Z",
                    "reply":{"root":root,"parent":parent}
                }),
            ))
        })
    }

    fn create_quote<'a>(
        &'a self,
        did: &'a str,
        text: &'a str,
        quoted: PostStrongRef,
    ) -> Pin<Box<dyn Future<Output = Result<PdsCreatedRecord, String>> + Send + 'a>> {
        Box::pin(async move {
            self.actions
                .lock()
                .unwrap()
                .push(format!("quote:{did}:{text}:{}:{}", quoted.uri, quoted.cid));
            Ok(fake_pds_create_record(
                did,
                "app.bsky.feed.post",
                "pds-quote-r1",
                "pds-quote-cid",
                serde_json::json!({
                    "$type":"app.bsky.feed.post",
                    "text":text,
                    "createdAt":"2026-10-28T00:00:00Z",
                    "embed":{"$type":"app.bsky.embed.record","record":quoted}
                }),
            ))
        })
    }

    fn fetch_post<'a>(
        &'a self,
        uri: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<IndexedRecord, String>> + Send + 'a>> {
        Box::pin(async move {
            self.post_requests.lock().unwrap().push(uri.to_owned());
            let delay_ms = self
                .post_delays_ms
                .lock()
                .unwrap()
                .get(uri)
                .copied()
                .unwrap_or_default();
            if delay_ms > 0 {
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
            self.live_posts
                .lock()
                .unwrap()
                .get(uri)
                .cloned()
                .ok_or_else(|| "fake PDS has no post for URI".to_owned())
        })
    }

    fn fetch_author_profile<'a>(
        &'a self,
        did: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<FetchedAuthorProfile, String>> + Send + 'a>> {
        Box::pin(async move {
            self.profile_requests.lock().unwrap().push(did.to_owned());
            let delay_ms = self
                .profile_delays_ms
                .lock()
                .unwrap()
                .get(did)
                .copied()
                .unwrap_or_default();
            if delay_ms > 0 {
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
            self.author_profiles
                .lock()
                .unwrap()
                .get(did)
                .cloned()
                .ok_or_else(|| "fake PDS has no profile for DID".to_owned())
        })
    }

    fn resolve_author_handle<'a>(
        &'a self,
        did: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Option<String>, String>> + Send + 'a>> {
        Box::pin(async move {
            Ok(self
                .author_profiles
                .lock()
                .unwrap()
                .get(did)
                .and_then(|profile| profile.verified_handle.clone()))
        })
    }

    fn pinned_feeds<'a>(
        &'a self,
        _: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ResolvedFeed>, String>> + Send + 'a>> {
        let feeds = self.pinned_feeds.clone();
        let delay_ms = self.pinned_feed_delay_ms;
        Box::pin(async move {
            if delay_ms > 0 {
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
            Ok(feeds)
        })
    }

    fn pinned_feed_tabs<'a>(
        &'a self,
        _: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<PinnedFeedTabs, String>> + Send + 'a>> {
        let feeds = self.pinned_feeds.clone();
        Box::pin(async move {
            Ok(PinnedFeedTabs {
                feeds,
                ..PinnedFeedTabs::default()
            })
        })
    }

    fn feed_timeline<'a>(
        &'a self,
        _: &'a str,
        uri: &'a str,
        _: Option<&'a str>,
        _: &'a dyn Storage,
    ) -> Pin<Box<dyn Future<Output = Result<FeedTimeline, String>> + Send + 'a>> {
        let uri = uri.to_owned();
        let delay_ms = self.feed_timeline_delay_ms;
        Box::pin(async move {
            if delay_ms > 0 {
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
            Ok(FeedTimeline {
                generator: ResolvedFeed {
                    uri,
                    display_name: "Slow feed".into(),
                    description: None,
                    avatar: None,
                    service_did: "did:plc:service".into(),
                    endpoint: "https://feed.example".into(),
                },
                timeline: HydratedFeedPage {
                    feed: Vec::new(),
                    cursor: None,
                    dropped_count: 0,
                },
                timing: FeedTimelineTiming::default(),
            })
        })
    }

    fn like<'a>(
        &'a self,
        did: &'a str,
        uri: &'a str,
        cid: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<PdsCreatedRecord, String>> + Send + 'a>> {
        Box::pin(async move {
            self.actions
                .lock()
                .unwrap()
                .push(format!("like:{did}:{uri}:{cid}"));
            Ok(fake_pds_create_record(
                did,
                "app.bsky.feed.like",
                "pds-like-r1",
                "pds-like-cid",
                serde_json::json!({
                    "$type":"app.bsky.feed.like",
                    "subject":{"uri":uri,"cid":cid},
                    "createdAt":"2026-10-28T00:00:00Z"
                }),
            ))
        })
    }

    fn delete_like<'a>(
        &'a self,
        did: &'a str,
        like_uri: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move {
            self.actions
                .lock()
                .unwrap()
                .push(format!("unlike:{did}:{like_uri}"));
            Ok(())
        })
    }

    fn repost<'a>(
        &'a self,
        did: &'a str,
        uri: &'a str,
        cid: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<PdsCreatedRecord, String>> + Send + 'a>> {
        Box::pin(async move {
            self.actions
                .lock()
                .unwrap()
                .push(format!("repost:{did}:{uri}:{cid}"));
            Ok(fake_pds_create_record(
                did,
                "app.bsky.feed.repost",
                "pds-repost-r1",
                "pds-repost-cid",
                serde_json::json!({
                    "$type":"app.bsky.feed.repost",
                    "subject":{"uri":uri,"cid":cid},
                    "createdAt":"2026-10-28T00:00:00Z"
                }),
            ))
        })
    }

    fn create_profile_graph_record<'a>(
        &'a self,
        did: &'a str,
        collection: &'a str,
        record: Value,
    ) -> Pin<Box<dyn Future<Output = Result<PdsCreatedRecord, String>> + Send + 'a>> {
        Box::pin(async move {
            let rkey = {
                let mut actions = self.actions.lock().unwrap();
                let rkey = format!("profile-action-{}", actions.len());
                actions.push(format!("create:{collection}:{did}:{record}"));
                rkey
            };
            let created = fake_pds_create_record(
                did,
                collection,
                &rkey,
                &format!("{rkey}-cid"),
                record.clone(),
            );
            if collection == "app.bsky.graph.listitem"
                && let (Some(list_uri), Some(subject)) = (
                    record.get("list").and_then(Value::as_str),
                    record.get("subject").and_then(Value::as_str),
                )
            {
                let mut lists = self.profile_lists.lock().unwrap();
                if let Some(list) = lists
                    .iter_mut()
                    .find(|list| list.uri == list_uri && list.item_uri.is_none())
                {
                    list.item_uri = Some(created.uri.clone());
                }
                let _ = subject;
            }
            Ok(created)
        })
    }

    fn delete_profile_graph_record<'a>(
        &'a self,
        did: &'a str,
        collection: &'a str,
        uri: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move {
            self.actions
                .lock()
                .unwrap()
                .push(format!("delete:{collection}:{did}:{uri}"));
            if collection == "app.bsky.graph.listitem" {
                for list in self.profile_lists.lock().unwrap().iter_mut() {
                    if list.item_uri.as_deref() == Some(uri) {
                        list.item_uri = None;
                    }
                }
            }
            Ok(())
        })
    }

    fn set_actor_muted<'a>(
        &'a self,
        did: &'a str,
        subject: &'a str,
        muted: bool,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move {
            self.actions.lock().unwrap().push(format!(
                "{}:{did}:{subject}",
                if muted { "mute" } else { "unmute" }
            ));
            if muted {
                self.muted_actors.lock().unwrap().insert(subject.to_owned());
            } else {
                self.muted_actors.lock().unwrap().remove(subject);
            }
            Ok(())
        })
    }

    fn muted_actor_dids<'a>(
        &'a self,
        _: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<std::collections::HashSet<String>, String>> + Send + 'a>>
    {
        Box::pin(async move { Ok(self.muted_actors.lock().unwrap().clone()) })
    }

    fn profile_list_choices<'a>(
        &'a self,
        _: &'a str,
        _: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<ProfileListChoice>, String>> + Send + 'a>> {
        Box::pin(async move { Ok(self.profile_lists.lock().unwrap().clone()) })
    }

    fn account_labels_for_page<'a>(
        &'a self,
        _: &'a str,
        mut actor_dids: Vec<String>,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<AccountLabel>, String>> + Send + 'a>> {
        Box::pin(async move {
            actor_dids.sort();
            actor_dids.dedup();
            self.label_requests.lock().unwrap().push(actor_dids.clone());
            Ok(self
                .account_labels
                .lock()
                .unwrap()
                .iter()
                .filter(|label| actor_dids.contains(&label.subject_did))
                .cloned()
                .collect())
        })
    }

    fn timeline<'a>(
        &'a self,
        _: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<Value>, String>> + Send + 'a>> {
        Box::pin(async move {
            self.actions.lock().unwrap().push("timeline".to_owned());
            Ok(vec![serde_json::json!({
                "post": {
                    "uri":"at://did:plc:author/app.bsky.feed.post/r1",
                    "cid":"bafyreifake",
                    "record":{"text":"from OAuth timeline"}
                }
            })])
        })
    }
}

async fn prelogin_cookie(app: &Router) -> String {
    let response = start_login(app).await;
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    response.headers()["set-cookie"]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned()
}

async fn start_login(app: &Router) -> axum::response::Response {
    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/oauth/authorize")
                .header("origin", "https://appview.example")
                .header("sec-fetch-site", "same-origin")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from("handle=alice.test"))
                .unwrap(),
        )
        .await
        .unwrap()
}

async fn finish_login(app: &Router) -> String {
    let login_cookie = prelogin_cookie(app).await;
    let callback = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/oauth/callback?code=valid-code&state=valid-state&scope=atproto")
                .header("cookie", login_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(callback.status(), StatusCode::SEE_OTHER);
    callback
        .headers()
        .get_all("set-cookie")
        .iter()
        .find_map(|header| {
            let value = header.to_str().ok()?;
            value
                .starts_with("appview_session=")
                .then(|| value.split(';').next().unwrap().to_owned())
        })
        .unwrap()
}

fn multipart_post_body(
    boundary: &str,
    text: &str,
    parent_uri: Option<&str>,
    quote_uri: Option<&str>,
    images: &[(&str, &str, &[u8], u32, u32)],
    alts: &[&str],
) -> Vec<u8> {
    let mut body = Vec::new();
    append_multipart_text_field(&mut body, boundary, "text", text);
    if let Some(parent_uri) = parent_uri {
        append_multipart_text_field(&mut body, boundary, "parent_uri", parent_uri);
    }
    if let Some(quote_uri) = quote_uri {
        append_multipart_text_field(&mut body, boundary, "quote_uri", quote_uri);
    }
    for (mime, filename, bytes, width, height) in images {
        body.extend_from_slice(
            format!(
                "--{boundary}\r\nContent-Disposition: form-data; name=\"images\"; filename=\"{filename}\"\r\nContent-Type: {mime}\r\n\r\n"
            )
            .as_bytes(),
        );
        body.extend_from_slice(bytes);
        body.extend_from_slice(b"\r\n");
        append_multipart_text_field(&mut body, boundary, "widths", &width.to_string());
        append_multipart_text_field(&mut body, boundary, "heights", &height.to_string());
    }
    for alt in alts {
        append_multipart_text_field(&mut body, boundary, "alts", alt);
    }
    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
    body
}

fn multipart_video_body(boundary: &str, text: &str, duration: &str) -> Vec<u8> {
    let mut body = Vec::new();
    append_multipart_text_field(&mut body, boundary, "text", text);
    append_multipart_text_field(&mut body, boundary, "video_duration", duration);
    append_multipart_text_field(&mut body, boundary, "video_width", "1920");
    append_multipart_text_field(&mut body, boundary, "video_height", "1080");
    append_multipart_text_field(&mut body, boundary, "video_alt", "A short sample clip");
    body.extend_from_slice(
        format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"video\"; filename=\"sample.mp4\"\r\nContent-Type: video/mp4\r\n\r\n"
        )
        .as_bytes(),
    );
    body.extend_from_slice(b"\0\0\0\x18ftypisom\0\0\0\0");
    body.extend_from_slice(format!("\r\n--{boundary}--\r\n").as_bytes());
    body
}

fn append_multipart_text_field(body: &mut Vec<u8>, boundary: &str, name: &str, value: &str) {
    body.extend_from_slice(
        format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n"
        )
        .as_bytes(),
    );
}

fn multipart_text_only_body_with_empty_file(boundary: &str, text: &str) -> Vec<u8> {
    let mut body = Vec::new();
    append_multipart_text_field(&mut body, boundary, "text", text);
    body.extend_from_slice(
        format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"images\"; filename=\"\"\r\nContent-Type: application/octet-stream\r\n\r\n\r\n--{boundary}\r\nContent-Disposition: form-data; name=\"video\"; filename=\"\"\r\nContent-Type: application/octet-stream\r\n\r\n\r\n--{boundary}--\r\n"
        )
        .as_bytes(),
    );
    body
}

fn multipart_native_media_body(
    boundary: &str,
    text: &str,
    file: Option<(&str, &str, &[u8])>,
) -> Vec<u8> {
    let mut body = Vec::new();
    append_multipart_text_field(&mut body, boundary, "text", text);
    if let Some((mime, filename, bytes)) = file {
        body.extend_from_slice(
            format!(
                "--{boundary}\r\nContent-Disposition: form-data; name=\"media\"; filename=\"{filename}\"\r\nContent-Type: {mime}\r\n\r\n"
            )
            .as_bytes(),
        );
        body.extend_from_slice(bytes);
        body.extend_from_slice(b"\r\n");
    } else {
        body.extend_from_slice(
            format!(
                "--{boundary}\r\nContent-Disposition: form-data; name=\"media\"; filename=\"\"\r\nContent-Type: application/octet-stream\r\n\r\n\r\n"
            )
            .as_bytes(),
        );
    }
    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
    body
}

async fn get_html(app: &Router, uri: &str, cookie: &str) -> String {
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("cookie", cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK, "{uri}");
    String::from_utf8(
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap()
}

#[derive(Clone)]
struct PanicCallbackFlow;

impl OAuthFlow for PanicCallbackFlow {
    fn authorize<'a>(
        &'a self,
        _: &'a str,
        _: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<String, String>> + Send + 'a>> {
        Box::pin(async { Ok("https://auth.example/authorize".to_owned()) })
    }

    fn callback<'a>(
        &'a self,
        _: OAuthCallback,
    ) -> Pin<Box<dyn Future<Output = Result<OAuthAccount, String>> + Send + 'a>> {
        Box::pin(async { panic!("simulated upstream OAuth panic") })
    }

    fn logout<'a>(
        &'a self,
        _: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }
}

#[derive(Clone)]
struct PanicAuthorizeFlow;

impl OAuthFlow for PanicAuthorizeFlow {
    fn authorize<'a>(
        &'a self,
        _: &'a str,
        _: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<String, String>> + Send + 'a>> {
        Box::pin(async { panic!("simulated upstream authorize panic") })
    }

    fn callback<'a>(
        &'a self,
        _: OAuthCallback,
    ) -> Pin<Box<dyn Future<Output = Result<OAuthAccount, String>> + Send + 'a>> {
        Box::pin(async { Err("unused callback".to_owned()) })
    }

    fn logout<'a>(
        &'a self,
        _: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }
}

#[derive(Clone)]
struct AcceptingOAuthFlow;

impl OAuthFlow for AcceptingOAuthFlow {
    fn authorize<'a>(
        &'a self,
        _: &'a str,
        _: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<String, String>> + Send + 'a>> {
        Box::pin(async { Ok("https://auth.example/authorize".to_owned()) })
    }

    fn callback<'a>(
        &'a self,
        _: OAuthCallback,
    ) -> Pin<Box<dyn Future<Output = Result<OAuthAccount, String>> + Send + 'a>> {
        Box::pin(async {
            Ok(OAuthAccount {
                did: "did:plc:alice".to_owned(),
                verified_handle: Some("alice.test".to_owned()),
                granted_scope: Some("atproto".to_owned()),
            })
        })
    }

    fn logout<'a>(
        &'a self,
        _: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }
}

#[test]
fn public_oauth_origin_is_configurable() {
    let config =
        Config::from_toml_and_env("", [("APPVIEW_PUBLIC_URL", "https://social.example")]).unwrap();
    assert!(format!("{config:?}").contains("public_url: \"https://social.example\""));
}

#[tokio::test]
async fn confidential_client_metadata_is_published() {
    let response = router()
        .oneshot(
            Request::builder()
                .uri("/.well-known/oauth-client-metadata.json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let metadata: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        metadata["client_id"],
        "https://appview.example/.well-known/oauth-client-metadata.json"
    );
    assert_eq!(metadata["token_endpoint_auth_method"], "private_key_jwt");
    assert_eq!(metadata["token_endpoint_auth_signing_alg"], "ES256");
    assert!(
        metadata["redirect_uris"][0]
            .as_str()
            .unwrap()
            .ends_with("/oauth/callback")
    );
    assert!(
        metadata["grant_types"]
            .as_array()
            .unwrap()
            .iter()
            .any(|grant| grant == "authorization_code")
    );
    assert!(
        metadata["grant_types"]
            .as_array()
            .unwrap()
            .iter()
            .any(|grant| grant == "refresh_token")
    );
    assert_eq!(metadata["scope"], EXPECTED_OAUTH_SCOPE);
    assert!(!metadata["jwks"]["keys"].as_array().unwrap().is_empty());
}

#[test]
fn loopback_oauth_metadata_uses_the_public_localhost_client() {
    let listen = "127.0.0.1:3000".parse().unwrap();
    let public = OAuthPublic::new_loopback(listen).unwrap();
    let metadata = public.metadata();
    let client_id = reqwest::Url::parse(metadata["client_id"].as_str().unwrap()).unwrap();

    assert_eq!(client_id.scheme(), "http");
    assert_eq!(client_id.host_str(), Some("localhost"));
    assert_eq!(client_id.port(), None);
    let parameters = client_id.query_pairs().collect::<Vec<_>>();
    assert!(parameters.contains(&(
        "redirect_uri".into(),
        "http://127.0.0.1:3000/oauth/callback".into()
    )));
    assert!(parameters.contains(&("scope".into(), EXPECTED_OAUTH_SCOPE.into())));
    assert_eq!(metadata["token_endpoint_auth_method"], "none");
    assert_eq!(metadata["dpop_bound_access_tokens"], true);
    assert!(metadata.get("jwks").is_none());
    assert!(metadata.get("jwks_uri").is_none());
    assert_eq!(
        metadata["redirect_uris"][0],
        "http://127.0.0.1:3000/oauth/callback"
    );
}

#[tokio::test]
async fn loopback_login_cookies_use_http_and_preserve_origin_checks() {
    let public = OAuthPublic::new_loopback("127.0.0.1:3000".parse().unwrap()).unwrap();
    let app = public.router_with_flow(FakeOAuthFlow::default(), Arc::new(MemoryStorage::default()));

    let rejected = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/oauth/authorize")
                .header("origin", "http://attacker.example")
                .header("sec-fetch-site", "cross-site")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from("handle=alice.test"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(rejected.status(), StatusCode::FORBIDDEN);

    let authorization = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/oauth/authorize")
                .header("origin", "http://127.0.0.1:3000")
                .header("sec-fetch-site", "same-origin")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from("handle=alice.test"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(authorization.status(), StatusCode::SEE_OTHER);
    let login_cookie = authorization.headers()["set-cookie"].to_str().unwrap();
    assert!(login_cookie.contains("HttpOnly"));
    assert!(login_cookie.contains("SameSite=Lax"));
    assert!(!login_cookie.contains("Secure"));

    let callback = app
        .oneshot(
            Request::builder()
                .uri("/oauth/callback?code=valid-code&state=valid-state&scope=atproto")
                .header("cookie", login_cookie.split(';').next().unwrap())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(callback.status(), StatusCode::SEE_OTHER);
    for cookie in callback.headers().get_all("set-cookie") {
        let cookie = cookie.to_str().unwrap();
        assert!(cookie.contains("HttpOnly"));
        assert!(cookie.contains("SameSite=Lax"));
        assert!(!cookie.contains("Secure"));
    }
}

#[tokio::test]
async fn login_page_accepts_an_atproto_handle_or_did() {
    let response = router()
        .oneshot(
            Request::builder()
                .uri("/login")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let html = String::from_utf8(body.to_vec()).unwrap();
    assert!(html.contains("name=\"handle\""));
    assert!(html.contains("/oauth/authorize"));
    assert!(html.contains("method=\"post\""));
}

#[tokio::test]
async fn web_pages_share_the_mobile_stylesheet_and_viewport_without_inline_code() {
    let storage = Arc::new(MemoryStorage::default());
    let thread_uri = "at://did:plc:alice/app.bsky.feed.post/root";
    storage
        .put_record(&IndexedRecord {
            did: "did:plc:alice".into(),
            collection: "app.bsky.feed.post".into(),
            rkey: "root".into(),
            uri: thread_uri.into(),
            cid: Some("root-cid".into()),
            time_us: 1,
            record: serde_json::json!({"text":"A local post"}),
        })
        .unwrap();
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(FakeOAuthFlow::default(), storage);
    let cookie = finish_login(&app).await;
    let paths = [
        "/login",
        "/app",
        "/profile?did=did%3Aplc%3Aalice",
        "/thread?uri=at%3A%2F%2Fdid%3Aplc%3Aalice%2Fapp.bsky.feed.post%2Froot",
        "/app/thread?uri=at%3A%2F%2Fdid%3Aplc%3Aalice%2Fapp.bsky.feed.post%2Froot",
        "/notifications",
    ];

    for path in paths {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(path)
                    .header("cookie", &cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{path}");
        let html = String::from_utf8(
            response
                .into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes()
                .to_vec(),
        )
        .unwrap();
        assert!(
            html.contains(
                "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1, viewport-fit=cover\">"
            ),
            "{path}"
        );
        assert!(html.contains("href=\"/static/app.css?v=m21\""), "{path}");
        assert!(!html.contains("style="), "{path}");
        // Both playback and interaction scripts are local, deferred, and never inline.
        assert_eq!(html.matches("<script").count(), 2, "{path}");
        assert!(html.contains("<script src=\"/static/vendor/hls.min.js?v=1.7.3\" defer></script>"));
        assert!(
            html.contains("<script src=\"/static/app.js?v=js16\" defer></script>"),
            "{path}"
        );
    }

    let stylesheet = app
        .oneshot(
            Request::builder()
                .uri("/static/app.css?v=m21")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(stylesheet.status(), StatusCode::OK);
    assert!(
        stylesheet.headers()["content-type"]
            .to_str()
            .unwrap()
            .starts_with("text/css")
    );
    assert!(
        stylesheet.headers()["cache-control"]
            .to_str()
            .unwrap()
            .contains("max-age=")
    );
    let css = String::from_utf8(
        stylesheet
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert!(css.contains("grid-template-columns: 190px minmax(0, 600px)"));
    assert!(css.contains("@media (max-width: 799px)"));
    assert!(css.contains("prefers-color-scheme: dark"));
    assert!(css.contains(".post-author {\n  display: flex;"));
    assert!(css.contains(".action-confirm input {\n  width: 1rem;"));
}

#[tokio::test]
async fn bsky_style_shell_renders_navigation_pinned_feed_tabs_and_post_actions() {
    let storage = Arc::new(MemoryStorage::default());
    storage
        .put_record(&IndexedRecord {
            did: "did:plc:alice".into(),
            collection: "app.bsky.feed.post".into(),
            rkey: "post".into(),
            uri: "at://did:plc:alice/app.bsky.feed.post/post".into(),
            cid: Some("post-cid".into()),
            time_us: 1,
            record: serde_json::json!({"text":"A rendered post"}),
        })
        .unwrap();
    let flow = FakeOAuthFlow {
        pinned_feeds: vec![ResolvedFeed {
            uri: "at://did:plc:creator/app.bsky.feed.generator/focus".into(),
            display_name: "Focus <Feed>".into(),
            description: None,
            avatar: None,
            service_did: "did:plc:service".into(),
            endpoint: "https://feed.example".into(),
        }],
        ..FakeOAuthFlow::default()
    };
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(flow, storage);
    let cookie = finish_login(&app).await;
    let response = app
        .oneshot(
            Request::builder()
                .uri("/app")
                .header("cookie", cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let html = String::from_utf8(
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert!(html.contains("aria-label=\"Main navigation\""), "{html}");
    assert!(html.contains("href=\"/app\">"), "{html}");
    assert!(html.contains("<span>Home</span>"), "{html}");
    assert!(html.contains("href=\"/feeds\">"), "{html}");
    assert!(html.contains("<span>Feeds</span>"), "{html}");
    assert!(html.contains("href=\"/notifications\">"), "{html}");
    assert!(html.contains("<span>Notifications</span>"), "{html}");
    assert!(html.contains("New post"), "{html}");
    assert!(html.contains("aria-label=\"Feed tabs\""), "{html}");
    assert!(html.contains(">Following</a>"), "{html}");
    assert!(html.contains("Focus &lt;Feed&gt;"), "{html}");
    assert!(html.contains("placeholder=\"Search\""), "{html}");
    assert!(html.contains("aria-label=\"Mobile navigation\""), "{html}");
    assert!(html.contains("action=\"/like\""), "{html}");
    assert!(html.contains("action=\"/repost\""), "{html}");
    assert!(
        html.contains("href=\"/app/thread?uri=at%3A%2F%2Fdid%3Aplc%3Aalice"),
        "{html}"
    );
    assert_eq!(html.matches("<symbol id=\"i-like\"").count(), 1, "{html}");
    assert!(html.contains("<use href=\"#i-like\""), "{html}");
    assert!(!html.contains("<svg class=\"icon\" viewBox="), "{html}");
}

#[tokio::test]
async fn quote_composer_quotes_existing_quote_posts_and_notifies_once() {
    let storage = Arc::new(MemoryStorage::default());
    let quote_uri = "at://did:plc:author/app.bsky.feed.post/original";
    storage
        .set_follow("did:plc:alice", "did:plc:author", true)
        .unwrap();
    storage
        .put_record(&IndexedRecord {
            did: "did:plc:author".into(),
            collection: "app.bsky.feed.post".into(),
            rkey: "original".into(),
            uri: quote_uri.into(),
            cid: Some("original-cid".into()),
            time_us: 1,
            record: serde_json::json!({
                "$type":"app.bsky.feed.post",
                "text":"quoted <script>text</script>",
                "createdAt":"2026-10-28T00:00:00Z",
                "embed":{
                    "$type":"app.bsky.embed.record",
                    "record":{"uri":"at://did:plc:root/app.bsky.feed.post/already-quoted","cid":"nested-cid"}
                }
            }),
        })
        .unwrap();
    let flow = FakeOAuthFlow::default();
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(flow.clone(), storage.clone());
    let cookie = finish_login(&app).await;

    let timeline = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/app")
                .header("cookie", &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let timeline_html = String::from_utf8(
        timeline
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert!(timeline_html.contains("Quote post"), "{timeline_html}");
    assert!(
        timeline_html.contains("enctype=\"multipart/form-data\""),
        "{timeline_html}"
    );
    assert!(
        timeline_html.contains(
            "name=\"media\" accept=\"image/*,video/mp4,video/webm,video/quicktime\" multiple"
        ),
        "{timeline_html}"
    );
    assert!(
        timeline_html.contains("Up to 4 pictures, or one video"),
        "{timeline_html}"
    );

    let encoded_uri =
        url::form_urlencoded::byte_serialize(quote_uri.as_bytes()).collect::<String>();
    let compose = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/compose?quote_uri={encoded_uri}"))
                .header("cookie", &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(compose.status(), StatusCode::OK);
    let compose_html = String::from_utf8(
        compose
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert!(compose_html.contains("action=\"/quote\""), "{compose_html}");
    assert!(
        compose_html.contains(
            "name=\"media\" accept=\"image/*,video/mp4,video/webm,video/quicktime\" multiple"
        ),
        "{compose_html}"
    );
    assert!(
        compose_html.contains("quoted &lt;script&gt;text&lt;/script&gt;"),
        "{compose_html}"
    );
    assert!(
        !compose_html.contains("<script>text</script>"),
        "{compose_html}"
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/quote")
                .header("origin", "https://appview.example")
                .header("sec-fetch-site", "same-origin")
                .header("cookie", &cookie)
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(format!(
                    "quote_uri={encoded_uri}&text=adding+context"
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    let records = storage.records().unwrap();
    let quote = records
        .iter()
        .find(|record| record.rkey == "pds-quote-r1")
        .expect("quote createRecord response should be indexed");
    assert_eq!(
        quote.record["embed"]["record"],
        serde_json::json!({"uri":quote_uri,"cid":"original-cid"})
    );
    assert_eq!(storage.notification_count("did:plc:author").unwrap(), 1);

    Ingestor::new(storage.clone(), ScopeMode::Full)
        .process_json(
            &serde_json::json!({
                "did":"did:plc:alice",
                "time_us":quote.time_us,
                "kind":"commit",
                "commit":{
                    "operation":"create",
                    "collection":"app.bsky.feed.post",
                    "rkey":"pds-quote-r1",
                    "cid":quote.cid,
                    "record":quote.record
                }
            })
            .to_string(),
        )
        .unwrap();
    assert_eq!(storage.notification_count("did:plc:author").unwrap(), 1);
    assert_eq!(
        *flow.actions.lock().unwrap(),
        [
            "quote:did:plc:alice:adding context:at://did:plc:author/app.bsky.feed.post/original:original-cid"
        ]
    );
}

#[tokio::test]
async fn sign_in_error_page_uses_the_shared_mobile_document_shell() {
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(PanicCallbackFlow, Arc::new(MemoryStorage::default()));
    let login_cookie = prelogin_cookie(&app).await;
    let response = app
        .oneshot(
            Request::builder()
                .uri("/oauth/callback?code=valid-code&state=valid-state&scope=atproto")
                .header("cookie", login_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let html = String::from_utf8(
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert!(
        html.contains("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1, viewport-fit=cover\">")
    );
    assert!(html.contains("Login failed, try again"));
    assert!(html.contains("href=\"/login\""));
    assert!(html.contains("href=\"/static/app.css?v=m21\""));
    assert!(!html.contains("style="));
    // The shared shell loads both local deferred scripts, never inline code.
    assert_eq!(html.matches("<script").count(), 2);
    assert!(html.contains("<script src=\"/static/vendor/hls.min.js?v=1.7.3\" defer></script>"));
    assert!(html.contains("<script src=\"/static/app.js?v=js16\" defer></script>"));
}

#[tokio::test]
async fn web_posts_show_avatar_relative_time_and_accessible_actions() {
    let storage = Arc::new(MemoryStorage::default());
    storage
        .put_record(&IndexedRecord {
            did: "did:plc:alice".into(),
            collection: "app.bsky.actor.profile".into(),
            rkey: "self".into(),
            uri: "at://did:plc:alice/app.bsky.actor.profile/self".into(),
            cid: Some("profile-cid".into()),
            time_us: 2,
            record: serde_json::json!({
                "displayName":"Alice",
                "avatar":{"ref":{"$link":"bafk-alice-avatar"}}
            }),
        })
        .unwrap();
    storage
        .put_record(&IndexedRecord {
            did: "did:plc:alice".into(),
            collection: "app.bsky.feed.post".into(),
            rkey: "root".into(),
            uri: "at://did:plc:alice/app.bsky.feed.post/root".into(),
            cid: Some("root-cid".into()),
            time_us: 1,
            record: serde_json::json!({
                "text":"A local post",
                "createdAt":"2020-01-01T00:00:00Z"
            }),
        })
        .unwrap();
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(FakeOAuthFlow::default(), storage);
    let cookie = finish_login(&app).await;
    let response = app
        .oneshot(
            Request::builder()
                .uri("/app")
                .header("cookie", cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let html = String::from_utf8(
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert!(html.contains(
        "https://cdn.bsky.app/img/avatar_thumbnail/plain/did:plc:alice/bafk-alice-avatar@jpeg"
    ));
    assert!(html.contains("Alice"));
    assert!(html.contains("@alice.test"));
    assert!(html.contains("datetime=\"2020-01-01T00:00:00Z\""));
    assert!(html.contains("ago</span>"));
    assert!(html.contains("aria-label=\"Open replies\""));
    assert!(html.contains("aria-label=\"Like post by @alice.test\""));
    assert!(html.contains("aria-label=\"Repost by @alice.test\""));
}

#[tokio::test]
async fn hls_playback_library_is_served_from_the_same_origin() {
    let response = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router()
        .oneshot(
            Request::builder()
                .uri("/static/vendor/hls.min.js?v=1.7.3")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers()["content-type"],
        "text/javascript; charset=utf-8"
    );
    assert!(
        response.headers()["cache-control"]
            .to_str()
            .unwrap()
            .contains("immutable")
    );
    assert!(
        !response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .is_empty()
    );
}

#[tokio::test]
async fn post_avatars_use_thumbnail_dimensions_and_lazy_load_after_six() {
    let storage = Arc::new(MemoryStorage::default());
    let did = "did:plc:alice";
    storage
        .put_record(&IndexedRecord {
            did: did.into(),
            collection: "app.bsky.actor.profile".into(),
            rkey: "self".into(),
            uri: format!("at://{did}/app.bsky.actor.profile/self"),
            cid: Some("profile-cid".into()),
            time_us: 1,
            record: serde_json::json!({
                "displayName":"Alice",
                "avatar":{"$type":"blob","ref":{"$link":"avatar-cid"},"mimeType":"image/jpeg"}
            }),
        })
        .unwrap();
    for index in 0..7 {
        storage
            .put_record(&fake_feed_post(
                did,
                &format!("post-{index}"),
                100 - index,
                "Post",
            ))
            .unwrap();
    }
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(FakeOAuthFlow::default(), storage);
    let cookie = finish_login(&app).await;
    let html = get_html(&app, "/app", &cookie).await;
    assert!(
        html.contains(&format!("img/avatar_thumbnail/plain/{did}/avatar-cid@jpeg")),
        "{html}"
    );
    assert_eq!(html.matches("loading=\"eager\"").count(), 6, "{html}");
    assert_eq!(html.matches("loading=\"lazy\"").count(), 1, "{html}");
    assert_eq!(html.matches("decoding=\"async\"").count(), 7, "{html}");
    assert!(
        html.contains("width=\"44\" height=\"44\" loading=\"eager\" decoding=\"async\""),
        "{html}"
    );
}

#[tokio::test]
async fn fifty_post_app_page_stays_under_the_compressed_page_weight_budget() {
    let storage = Arc::new(MemoryStorage::default());
    let did = "did:plc:alice";
    storage
        .put_record(&IndexedRecord {
            did: did.into(),
            collection: "app.bsky.actor.profile".into(),
            rkey: "self".into(),
            uri: format!("at://{did}/app.bsky.actor.profile/self"),
            cid: Some("profile-cid".into()),
            time_us: 1,
            record: serde_json::json!({
                "displayName":"Alice",
                "avatar":{"$type":"blob","ref":{"$link":"avatar-cid"},"mimeType":"image/jpeg"}
            }),
        })
        .unwrap();
    let body = "A representative post with a few sentences about what happened today. \
        Here are a few details, a thought, and a link to share with friends.";
    for index in 0..50 {
        storage
            .put_record(&fake_feed_post(
                did,
                &format!("post-{index}"),
                1_000 - index,
                &format!("{body} ({index})"),
            ))
            .unwrap();
    }
    let app = router_with_oauth_flow(
        OAuthPublic::new("https://appview.example").unwrap(),
        FakeOAuthFlow::default(),
        storage,
    );
    let cookie = finish_login(&app).await;
    let response = app
        .oneshot(
            Request::builder()
                .uri("/app")
                .header("cookie", cookie)
                .header("accept-encoding", "gzip")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-encoding"], "gzip");
    let compressed = response.into_body().collect().await.unwrap().to_bytes();
    eprintln!("case21 /app 50-post gzip body: {} bytes", compressed.len());
    assert!(
        compressed.len() < 60 * 1024,
        "compressed /app body was {} bytes (budget 61440 bytes)",
        compressed.len()
    );
}

#[tokio::test]
async fn authorize_rejects_cross_origin_and_cross_site_requests() {
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(AcceptingOAuthFlow, Arc::new(MemoryStorage::default()));
    for headers in [
        vec![
            ("origin", "https://attacker.example"),
            ("sec-fetch-site", "cross-site"),
        ],
        vec![
            ("origin", "https://other.appview.example"),
            ("sec-fetch-site", "same-site"),
        ],
        vec![("sec-fetch-site", "same-origin")],
    ] {
        let mut request = Request::builder()
            .method("POST")
            .uri("/oauth/authorize")
            .header("content-type", "application/x-www-form-urlencoded");
        for (name, value) in headers {
            request = request.header(name, value);
        }
        let response = app
            .clone()
            .oneshot(request.body(Body::from("handle=alice.test")).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert!(!response.headers().contains_key("set-cookie"));
    }
}

#[tokio::test]
async fn authorization_and_callback_reject_missing_protocol_parameters() {
    for path in ["/oauth/authorize", "/oauth/callback"] {
        let request = if path == "/oauth/authorize" {
            Request::builder()
                .method("POST")
                .uri(path)
                .header("origin", "https://appview.example")
                .header("sec-fetch-site", "same-origin")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from("handle="))
                .unwrap()
        } else {
            Request::builder().uri(path).body(Body::empty()).unwrap()
        };
        let response = router().oneshot(request).await.unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{path}");
    }
}

#[tokio::test]
async fn authorize_normalizes_the_typed_handle_but_not_a_did() {
    // Normalize typed handles before checking them against the DID document because
    // AT Protocol handles are case-insensitive.
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(FakeOAuthFlow::default(), Arc::new(MemoryStorage::default()));
    for (typed, expected) in [
        ("%40Alice.Test+", "login=alice.test"),
        ("+did%3Aplc%3AAbC+", "login=did:plc:AbC"),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/oauth/authorize")
                    .header("origin", "https://appview.example")
                    .header("sec-fetch-site", "same-origin")
                    .header("content-type", "application/x-www-form-urlencoded")
                    .body(Body::from(format!("handle={typed}")))
                    .unwrap(),
            )
            .await
            .unwrap();
        let location = response.headers()["location"].to_str().unwrap().to_owned();
        assert!(location.ends_with(expected), "{typed}: {location}");
    }
}

#[tokio::test]
async fn callback_rejects_valid_state_without_the_matching_browser_cookie() {
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(FakeOAuthFlow::default(), Arc::new(MemoryStorage::default()));
    let response = app
        .oneshot(
            Request::builder()
                .uri("/oauth/callback?code=valid-code&state=valid-state&iss=https%3A%2F%2Fauth.example")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn web_callback_requires_a_login_cookie_independently_of_the_flow() {
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(AcceptingOAuthFlow, Arc::new(MemoryStorage::default()));
    let response = app
        .oneshot(
            Request::builder()
                .uri("/oauth/callback?code=valid-code&state=valid-state")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn web_callback_rejects_a_forged_login_cookie_independently_of_the_flow() {
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(AcceptingOAuthFlow, Arc::new(MemoryStorage::default()));
    let login_cookie = prelogin_cookie(&app).await;
    let payload = login_cookie
        .strip_prefix("appview_login=")
        .unwrap()
        .split_once('.')
        .unwrap()
        .0;
    let forged_cookie = format!("appview_login={payload}.AAAA");
    let response = app
        .oneshot(
            Request::builder()
                .uri("/oauth/callback?code=valid-code&state=valid-state")
                .header("cookie", forged_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn callback_turns_an_upstream_panic_into_a_login_retry_page() {
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(PanicCallbackFlow, Arc::new(MemoryStorage::default()));
    let login_cookie = prelogin_cookie(&app).await;
    let response = app
        .oneshot(
            Request::builder()
                .uri("/oauth/callback?code=valid-code&state=valid-state")
                .header("cookie", login_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response.headers()["content-type"]
            .to_str()
            .unwrap()
            .contains("text/html")
    );
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let html = String::from_utf8(body.to_vec()).unwrap();
    assert!(html.contains("Login failed, try again"), "{html}");
    assert!(html.contains("href=\"/login\""), "{html}");
}

#[tokio::test]
async fn authorize_turns_an_upstream_panic_into_an_error_page() {
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(PanicAuthorizeFlow, Arc::new(MemoryStorage::default()));
    let response = start_login(&app).await;
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    assert!(
        response.headers()["content-type"]
            .to_str()
            .unwrap()
            .contains("text/html")
    );
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert!(String::from_utf8_lossy(&body).contains("Sign-in could not be completed"));
}

#[tokio::test]
async fn oauth_and_write_errors_do_not_echo_tokens_or_internal_paths() {
    let sentinel = "access_token=secret /srv/appview/oauth.sqlite";
    let callback_app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(
            FakeOAuthFlow {
                callback_failure: Some(sentinel.to_owned()),
                ..FakeOAuthFlow::default()
            },
            Arc::new(MemoryStorage::default()),
        );
    let login_cookie = prelogin_cookie(&callback_app).await;
    let callback = callback_app
        .oneshot(
            Request::builder()
                .uri("/oauth/callback?code=valid-code&state=valid-state")
                .header("cookie", login_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let callback_body = callback.into_body().collect().await.unwrap().to_bytes();
    assert!(!String::from_utf8_lossy(&callback_body).contains(sentinel));

    let write_app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(
            FakeOAuthFlow {
                action_failure: Some(sentinel.to_owned()),
                ..FakeOAuthFlow::default()
            },
            Arc::new(MemoryStorage::default()),
        );
    let session_cookie = finish_login(&write_app).await;
    let write_response = write_app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/post")
                .header("origin", "https://appview.example")
                .header("sec-fetch-site", "same-origin")
                .header("cookie", session_cookie)
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from("text=hello"))
                .unwrap(),
        )
        .await
        .unwrap();
    let write_body = write_response
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    assert!(!String::from_utf8_lossy(&write_body).contains(sentinel));
}

#[cfg(unix)]
#[test]
fn sqlite_databases_are_created_with_owner_only_permissions() {
    use std::os::unix::fs::PermissionsExt;

    let file = std::env::temp_dir().join(format!(
        "appview-private-db-{}-{}.sqlite",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let storage = SqliteStorage::open(&file).unwrap();
    assert_eq!(
        std::fs::metadata(&file).unwrap().permissions().mode() & 0o777,
        0o600
    );
    drop(storage);
    std::fs::remove_file(&file).unwrap();

    let store = SqliteOAuthStore::open(&file).unwrap();
    assert_eq!(
        std::fs::metadata(&file).unwrap().permissions().mode() & 0o777,
        0o600
    );
    drop(store);
    std::fs::remove_file(&file).unwrap();
}

#[tokio::test]
async fn oauth_callback_registers_account_and_logout_clears_its_session() {
    let public = OAuthPublic::new("https://appview.example").unwrap();
    let storage = Arc::new(MemoryStorage::default());
    let app = public.router_with_flow(FakeOAuthFlow::default(), storage.clone());

    let authorization = start_login(&app).await;
    assert_eq!(authorization.status(), StatusCode::SEE_OTHER);
    assert_eq!(
        authorization.headers()["location"],
        "https://auth.example/authorize?login=alice.test"
    );
    let login_cookie = authorization.headers()["set-cookie"]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();

    let callback = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/oauth/callback?code=valid-code&state=valid-state&scope=atproto")
                .header("cookie", login_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(callback.status(), StatusCode::SEE_OTHER);
    assert_eq!(callback.headers()["location"], "/app");
    let cookie = callback.headers()["set-cookie"]
        .to_str()
        .unwrap()
        .to_owned();
    assert!(cookie.contains("HttpOnly"));
    assert!(storage.is_registered("did:plc:alice").unwrap());
    assert_eq!(storage.pending_backfills().unwrap(), ["did:plc:alice"]);
    assert_eq!(
        storage.verified_handle("did:plc:alice").unwrap().as_deref(),
        Some("alice.test")
    );

    let logout = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/logout")
                .header("origin", "https://appview.example")
                .header("sec-fetch-site", "same-origin")
                .header("cookie", cookie.split(';').next().unwrap())
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(logout.status(), StatusCode::SEE_OTHER);
    let cleared_cookie = logout.headers()["set-cookie"].to_str().unwrap();
    assert!(cleared_cookie.contains("Secure"));
    assert!(cleared_cookie.contains("HttpOnly"));
    assert!(cleared_cookie.contains("SameSite=Lax"));
    assert!(!storage.is_registered("did:plc:alice").unwrap());
}

#[tokio::test(flavor = "current_thread")]
async fn callback_database_writes_do_not_block_the_runtime_timer() {
    let dir = std::env::temp_dir().join(format!(
        "appview-callback-write-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("appview.db");
    let storage = Arc::new(SqliteStorage::open(&path).unwrap());
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(FakeOAuthFlow::default(), storage.clone());
    let login_cookie = prelogin_cookie(&app).await;

    let blocker = Connection::open(&path).unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    let writer_storage = storage.clone();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let writer = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        writer_storage.put_record(&IndexedRecord {
            did: "did:plc:writer".to_owned(),
            collection: "app.bsky.feed.post".to_owned(),
            rkey: "held-write".to_owned(),
            uri: "at://did:plc:writer/app.bsky.feed.post/held-write".to_owned(),
            cid: None,
            time_us: 1,
            record: serde_json::json!({"text":"writer"}),
        })
    });
    started_rx.recv().unwrap();
    std::thread::sleep(Duration::from_millis(50));
    let release = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(700));
        blocker.execute_batch("COMMIT").unwrap();
    });

    let (timer_tx, timer_rx) = tokio::sync::oneshot::channel();
    let timer_started = Instant::now();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let _ = timer_tx.send(timer_started.elapsed());
    });
    let callback = app
        .oneshot(
            Request::builder()
                .uri("/oauth/callback?code=valid-code&state=valid-state&scope=atproto")
                .header("cookie", login_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let timer_delay = timer_rx.await.unwrap();
    writer.join().unwrap().unwrap();
    release.join().unwrap();
    assert_eq!(callback.status(), StatusCode::SEE_OTHER);
    assert!(
        timer_delay < Duration::from_millis(200),
        "runtime timer fired after {timer_delay:?} while sign-in wrote session state"
    );
    drop(storage);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test(flavor = "current_thread")]
async fn logout_database_writes_do_not_block_the_runtime_timer() {
    let dir = std::env::temp_dir().join(format!(
        "appview-logout-write-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("appview.db");
    let storage = Arc::new(SqliteStorage::open(&path).unwrap());
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(FakeOAuthFlow::default(), storage.clone());
    let session_cookie = finish_login(&app).await;

    let blocker = Connection::open(&path).unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    let writer_storage = storage.clone();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let writer = std::thread::spawn(move || {
        started_tx.send(()).unwrap();
        writer_storage.put_record(&IndexedRecord {
            did: "did:plc:writer".to_owned(),
            collection: "app.bsky.feed.post".to_owned(),
            rkey: "held-write".to_owned(),
            uri: "at://did:plc:writer/app.bsky.feed.post/held-write".to_owned(),
            cid: None,
            time_us: 1,
            record: serde_json::json!({"text":"writer"}),
        })
    });
    started_rx.recv().unwrap();
    std::thread::sleep(Duration::from_millis(50));
    let release = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_millis(700));
        blocker.execute_batch("COMMIT").unwrap();
    });

    let (timer_tx, timer_rx) = tokio::sync::oneshot::channel();
    let timer_started = Instant::now();
    tokio::spawn(async move {
        tokio::time::sleep(Duration::from_millis(50)).await;
        let _ = timer_tx.send(timer_started.elapsed());
    });
    let logout = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/logout")
                .header("origin", "https://appview.example")
                .header("sec-fetch-site", "same-origin")
                .header("cookie", session_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let timer_delay = timer_rx.await.unwrap();
    writer.join().unwrap().unwrap();
    release.join().unwrap();
    assert_eq!(logout.status(), StatusCode::SEE_OTHER);
    assert!(
        timer_delay < Duration::from_millis(200),
        "runtime timer fired after {timer_delay:?} while logout wrote session state"
    );
    drop(storage);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn oauth_callback_accepts_scope_from_the_token_response_without_query_scope() {
    let storage = Arc::new(MemoryStorage::default());
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(FakeOAuthFlow::default(), storage.clone());
    let login_cookie = prelogin_cookie(&app).await;
    let response = app
        .oneshot(
            Request::builder()
                .uri("/oauth/callback?code=valid-code&state=valid-state&iss=https%3A%2F%2Fauth.example")
                .header("cookie", login_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    assert!(storage.is_registered("did:plc:alice").unwrap());
}

#[tokio::test]
async fn oauth_authorization_state_and_tokens_survive_store_reopen() {
    let file = std::env::temp_dir().join(format!(
        "appview-oauth-{}-{}.sqlite",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let public = OAuthPublic::new("https://appview.example").unwrap();
    let state = InternalStateData {
        iss: "https://issuer.example".to_owned(),
        dpop_key: public.private_key().key.clone(),
        verifier: "pkce-verifier".to_owned(),
        app_state: Some("expected-identity".to_owned()),
    };
    let did = "did:plc:alice".parse::<Did>().unwrap();
    let session: Session = serde_json::from_value(serde_json::json!({
        "dpop_key": public.private_key(),
        "token_set": {
            "iss":"https://issuer.example",
            "sub":did,
            "aud":"https://pds.example",
            "scope":"atproto",
            "refresh_token":"refresh-secret",
            "access_token":"access-secret",
            "token_type":"DPoP",
            "expires_at":null
        }
    }))
    .unwrap();

    let store = SqliteOAuthStore::open(&file).unwrap();
    store
        .set("state-1".to_owned(), state.clone())
        .await
        .unwrap();
    store.set(did.clone(), session.clone()).await.unwrap();
    drop(store);

    let reopened = SqliteOAuthStore::open(&file).unwrap();
    assert_eq!(
        reopened.get(&"state-1".to_owned()).await.unwrap(),
        Some(state)
    );
    assert_eq!(reopened.get(&did).await.unwrap(), Some(session));
    let _ = std::fs::remove_file(&file);
}

#[tokio::test]
async fn root_redirects_to_the_app_instead_of_an_empty_404() {
    // A bare 404 with no content type made a phone browser save "/" as a file.
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router();
    let response = app
        .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    assert_eq!(response.headers()["location"], "/app");
}

#[tokio::test]
async fn web_views_and_write_actions_require_a_login_session() {
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router();
    for path in [
        "/profile?did=did%3Aplc%3Aalice",
        "/thread?uri=at%3A%2F%2Fdid%3Aplc%3Aalice%2Fapp.bsky.feed.post%2Fr1",
    ] {
        let response = app
            .clone()
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SEE_OTHER, "{path}");
        assert_eq!(response.headers()["location"], "/login");
    }
    for path in ["/post", "/reply", "/like", "/repost"] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(path)
                    .header("origin", "https://appview.example")
                    .header("sec-fetch-site", "same-origin")
                    .header("content-type", "application/x-www-form-urlencoded")
                    .body(Body::from("text=hello&uri=at%3A%2F%2Fdid%3Aplc%3Aalice%2Fapp.bsky.feed.post%2Fr1&cid=bafyreifake"))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SEE_OTHER, "{path}");
        assert_eq!(response.headers()["location"], "/login");
    }
}

#[tokio::test]
async fn app_timeline_does_not_wait_for_slow_pinned_feed_resolution() {
    let storage = Arc::new(MemoryStorage::default());
    let feeds = (0..38)
        .map(|index| {
            let rkey = format!("feed-{index}");
            ResolvedFeed {
                uri: format!("at://did:plc:creator/app.bsky.feed.generator/{rkey}"),
                display_name: if index < 5 {
                    format!("Feed {index}")
                } else {
                    format!("Cached Feed {index}")
                },
                description: None,
                avatar: None,
                service_did: "did:plc:service".into(),
                endpoint: "https://feed.example".into(),
            }
        })
        .collect();
    let flow = FakeOAuthFlow {
        pinned_feeds: feeds,
        pinned_feed_delay_ms: 5_000,
        ..FakeOAuthFlow::default()
    };
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(flow, storage);
    let cookie = finish_login(&app).await;
    let started = Instant::now();
    let response = app
        .oneshot(
            Request::builder()
                .uri("/app")
                .header("cookie", cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let elapsed = started.elapsed();
    let html = String::from_utf8(
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert!(elapsed < Duration::from_millis(200), "took {elapsed:?}");
    for index in 0..38 {
        let label = if index < 5 {
            format!("Feed {index}")
        } else {
            format!("Cached Feed {index}")
        };
        assert!(html.contains(&label), "missing tab {label}");
    }
}

#[tokio::test]
async fn pinned_feed_list_and_feed_timeline_routes_are_available_to_signed_in_users() {
    let public = OAuthPublic::new("https://appview.example").unwrap();
    let app = public.router_with_flow(FakeOAuthFlow::default(), Arc::new(MemoryStorage::default()));
    let cookie = finish_login(&app).await;
    for uri in [
        "/feeds",
        "/app/feed?uri=at%3A%2F%2Fdid%3Aplc%3Acreator%2Fapp.bsky.feed.generator%2Ffeed",
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .header("cookie", &cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{uri}");
        if uri == "/feeds" {
            let body = response.into_body().collect().await.unwrap().to_bytes();
            let html = String::from_utf8(body.to_vec()).unwrap();
            assert!(html.contains("href=\"/feeds\">"));
            assert!(html.contains("<span>Feeds</span>"));
        }
    }
}

#[tokio::test]
async fn old_scope_session_gets_a_friendly_feed_reauthorization_page() {
    let old_scope = "atproto repo:app.bsky.feed.post?action=create repo:app.bsky.feed.like?action=create&action=delete repo:app.bsky.feed.repost?action=create&action=delete";
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(
            FakeOAuthFlow::with_granted_scope(Some(old_scope)),
            Arc::new(MemoryStorage::default()),
        );
    let cookie = finish_login(&app).await;
    let response = app
        .oneshot(
            Request::builder()
                .uri("/feeds")
                .header("cookie", cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let html = String::from_utf8(
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert!(
        html.to_ascii_lowercase()
            .contains("sign in again to allow feeds")
    );
    assert!(html.contains("href=\"/login\""));
    assert!(!html.contains("\"error\":\"OAuthError\""));
}

#[tokio::test]
async fn old_scope_session_keeps_text_and_picture_composer_and_requires_video_reauthorization() {
    let old_scope = "atproto repo:app.bsky.feed.post?action=create repo:app.bsky.feed.like?action=create&action=delete repo:app.bsky.feed.repost?action=create&action=delete blob:image/*";
    let flow = FakeOAuthFlow::with_granted_scope(Some(old_scope));
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(flow.clone(), Arc::new(MemoryStorage::default()));
    let cookie = finish_login(&app).await;
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/app")
                .header("cookie", &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let html = String::from_utf8(
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert!(html.contains("action=\"/post\""), "{html}");
    assert!(!html.contains("Log in again to enable pictures"), "{html}");
    assert!(
        !html.contains("sign in again to post videos"),
        "video scope feedback appears when selected"
    );
    assert!(html.contains("data-media-input"), "{html}");
    assert!(html.contains("data-image-access=\"true\""), "{html}");
    assert!(html.contains("data-video-access=\"false\""), "{html}");
    assert!(
        html.contains("accept=\"image/*,video/mp4,video/webm,video/quicktime\""),
        "{html}"
    );
    assert!(!html.contains("data-video-input"), "{html}");
    assert_eq!(html.matches("type=\"file\"").count(), 1, "{html}");

    let text_post = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/post")
                .header("origin", "https://appview.example")
                .header("sec-fetch-site", "same-origin")
                .header("cookie", &cookie)
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from("text=still+works"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(text_post.status(), StatusCode::SEE_OTHER);

    let video_boundary = "case31-video";
    let video_post = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/video")
                .header("origin", "https://appview.example")
                .header("sec-fetch-site", "same-origin")
                .header("cookie", &cookie)
                .header(
                    "content-type",
                    format!("multipart/form-data; boundary={video_boundary}"),
                )
                .body(Body::from(multipart_video_body(
                    video_boundary,
                    "video caption",
                    "5",
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(video_post.status(), StatusCode::FORBIDDEN);
    let video_message: Value =
        serde_json::from_slice(&video_post.into_body().collect().await.unwrap().to_bytes())
            .unwrap();
    assert_eq!(video_message["message"], "sign in again to post videos");

    let boundary = "case26-picture";
    let body = multipart_post_body(
        boundary,
        "caption",
        None,
        None,
        &[("image/jpeg", "a.jpg", &[0xff, 0xd8, 0xff], 1, 1)],
        &[""],
    );
    let image_post = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/post")
                .header("origin", "https://appview.example")
                .header("sec-fetch-site", "same-origin")
                .header("cookie", cookie)
                .header(
                    "content-type",
                    format!("multipart/form-data; boundary={boundary}"),
                )
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(image_post.status(), StatusCode::SEE_OTHER);
    assert_eq!(
        *flow.actions.lock().unwrap(),
        [
            "post:did:plc:alice:still works",
            "image-post:did:plc:alice:caption:1"
        ],
        "old image permission still permits picture posting alongside text"
    );
}

#[tokio::test]
async fn overlong_video_is_rejected_before_the_oauth_flow_is_called() {
    let flow = FakeOAuthFlow::default();
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(flow.clone(), Arc::new(MemoryStorage::default()));
    let cookie = finish_login(&app).await;
    let boundary = "case31-too-long";
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/video")
                .header("origin", "https://appview.example")
                .header("sec-fetch-site", "same-origin")
                .header("cookie", cookie)
                .header(
                    "content-type",
                    format!("multipart/form-data; boundary={boundary}"),
                )
                .body(Body::from(multipart_video_body(boundary, "caption", "601")))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    let message: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert!(message["message"].as_str().unwrap().contains("10 minutes"));
    assert!(flow.actions.lock().unwrap().is_empty());
}

#[tokio::test]
async fn invalid_images_are_rejected_before_the_oauth_flow_is_called() {
    let flow = FakeOAuthFlow::default();
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(flow.clone(), Arc::new(MemoryStorage::default()));
    let cookie = finish_login(&app).await;
    let cases = [
        ("image/svg+xml", "wrong.svg", b"<svg></svg>".to_vec()),
        ("image/jpeg", "large.jpg", vec![0x55; 1_000_001]),
    ];
    for (mime, filename, bytes) in cases {
        let boundary = "case26-invalid";
        let body = multipart_post_body(
            boundary,
            "caption",
            None,
            None,
            &[(mime, filename, &bytes, 1, 1)],
            &[""],
        );
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/post")
                    .header("origin", "https://appview.example")
                    .header("sec-fetch-site", "same-origin")
                    .header("cookie", &cookie)
                    .header(
                        "content-type",
                        format!("multipart/form-data; boundary={boundary}"),
                    )
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            response.status(),
            StatusCode::BAD_REQUEST,
            "{mime} {filename}"
        );
        let message: Value =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert!(
            message["message"]
                .as_str()
                .is_some_and(|value| !value.is_empty())
        );
    }
    assert!(flow.actions.lock().unwrap().is_empty());
}

#[tokio::test]
async fn multipart_text_only_post_still_works_without_javascript() {
    let flow = FakeOAuthFlow::default();
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(flow.clone(), Arc::new(MemoryStorage::default()));
    let cookie = finish_login(&app).await;
    let boundary = "case26-no-script";
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/post")
                .header("origin", "https://appview.example")
                .header("sec-fetch-site", "same-origin")
                .header("cookie", cookie)
                .header(
                    "content-type",
                    format!("multipart/form-data; boundary={boundary}"),
                )
                .body(Body::from(multipart_text_only_body_with_empty_file(
                    boundary,
                    "text only from the native form",
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    assert_eq!(
        flow.actions.lock().unwrap().as_slice(),
        ["post:did:plc:alice:text only from the native form"]
    );
}

#[tokio::test]
async fn native_media_input_accepts_images_skips_empty_parts_and_refuses_video() {
    let flow = FakeOAuthFlow::default();
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(flow.clone(), Arc::new(MemoryStorage::default()));
    let cookie = finish_login(&app).await;
    let png =
        b"\x89PNG\r\n\x1a\n\x00\x00\x00\rIHDR\x00\x00\x00\x01\x00\x00\x00\x01\x08\x06\x00\x00\x00";

    let image_boundary = "case37-native-image";
    let image = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/post")
                .header("origin", "https://appview.example")
                .header("sec-fetch-site", "same-origin")
                .header("cookie", &cookie)
                .header(
                    "content-type",
                    format!("multipart/form-data; boundary={image_boundary}"),
                )
                .body(Body::from(multipart_native_media_body(
                    image_boundary,
                    "native image",
                    Some(("image/png", "pixel.png", png)),
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(image.status(), StatusCode::SEE_OTHER);

    let empty_boundary = "case37-native-empty";
    let empty = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/post")
                .header("origin", "https://appview.example")
                .header("sec-fetch-site", "same-origin")
                .header("cookie", &cookie)
                .header(
                    "content-type",
                    format!("multipart/form-data; boundary={empty_boundary}"),
                )
                .body(Body::from(multipart_native_media_body(
                    empty_boundary,
                    "empty media field",
                    None,
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(empty.status(), StatusCode::SEE_OTHER);

    let video_boundary = "case37-native-video";
    let mp4 = b"\x00\x00\x00\x18ftypisom\x00\x00\x00\x00";
    let video = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/post")
                .header("origin", "https://appview.example")
                .header("sec-fetch-site", "same-origin")
                .header("cookie", &cookie)
                .header(
                    "content-type",
                    format!("multipart/form-data; boundary={video_boundary}"),
                )
                .body(Body::from(multipart_native_media_body(
                    video_boundary,
                    "native video",
                    Some(("video/mp4", "clip.mp4", mp4)),
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(video.status(), StatusCode::BAD_REQUEST);
    let message: Value =
        serde_json::from_slice(&video.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(
        message["message"],
        "Enable JavaScript to validate and upload videos."
    );

    assert_eq!(
        flow.actions.lock().unwrap().as_slice(),
        [
            "image-post:did:plc:alice:native image:1",
            "post:did:plc:alice:empty media field"
        ]
    );
}

#[tokio::test]
async fn logged_in_write_forms_use_the_account_oauth_flow() {
    let public = OAuthPublic::new("https://appview.example").unwrap();
    let flow = FakeOAuthFlow::default();
    let actions = flow.actions.clone();
    let app = public.router_with_flow(flow, Arc::new(MemoryStorage::default()));
    let login_cookie = prelogin_cookie(&app).await;
    let callback = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/oauth/callback?code=valid-code&state=valid-state&scope=atproto")
                .header("cookie", login_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let cookie = callback.headers()["set-cookie"]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();

    // A like or repost returns to the referring page (same origin, under /app) at the post's card.
    for (path, body, referer, location) in [
        ("/post", "text=hello+world", None, "/app"),
        (
            "/like",
            "uri=at%3A%2F%2Fdid%3Aplc%3Aauthor%2Fapp.bsky.feed.post%2Fr1&cid=bafyreifake",
            Some("https://appview.example/app/feed?uri=at%3A%2F%2Ffeed"),
            "/app/feed?uri=at%3A%2F%2Ffeed#post-did-plc-author-app-bsky-feed-post-r1",
        ),
        (
            "/repost",
            "uri=at%3A%2F%2Fdid%3Aplc%3Aauthor%2Fapp.bsky.feed.post%2Fr1&cid=bafyreifake",
            Some("https://elsewhere.example/app/feed"),
            "/app#post-did-plc-author-app-bsky-feed-post-r1",
        ),
    ] {
        let mut request = Request::builder();
        if let Some(referer) = referer {
            request = request.header("referer", referer);
        }
        let response = app
            .clone()
            .oneshot(
                request
                    .method("POST")
                    .uri(path)
                    .header("origin", "https://appview.example")
                    .header("sec-fetch-site", "same-origin")
                    .header("cookie", &cookie)
                    .header("content-type", "application/x-www-form-urlencoded")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SEE_OTHER, "{path}");
        assert_eq!(response.headers()["location"], location, "{path}");
    }
    assert_eq!(
        *actions.lock().unwrap(),
        [
            "post:did:plc:alice:hello world",
            "like:did:plc:alice:at://did:plc:author/app.bsky.feed.post/r1:bafyreifake",
            "repost:did:plc:alice:at://did:plc:author/app.bsky.feed.post/r1:bafyreifake",
        ]
    );

    // The like script (static/app.js) sends x-appview-fetch: 1 and gets the new state as JSON, so a like toggles
    // in place: unlike the post liked above, then like it again.
    for (body, liked, count) in [
        (
            "uri=at%3A%2F%2Fdid%3Aplc%3Aauthor%2Fapp.bsky.feed.post%2Fr1&cid=bafyreifake&intent=delete",
            false,
            0,
        ),
        (
            "uri=at%3A%2F%2Fdid%3Aplc%3Aauthor%2Fapp.bsky.feed.post%2Fr1&cid=bafyreifake",
            true,
            1,
        ),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/like")
                    .header("origin", "https://appview.example")
                    .header("sec-fetch-site", "same-origin")
                    .header("cookie", &cookie)
                    .header("x-appview-fetch", "1")
                    .header("content-type", "application/x-www-form-urlencoded")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{body}");
        let json: serde_json::Value =
            serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert_eq!(
            json,
            serde_json::json!({"liked": liked, "likeCount": count}),
            "{body}"
        );
    }
}

#[tokio::test]
async fn successful_pds_writes_are_indexed_and_jetstream_replays_are_idempotent() {
    let public = OAuthPublic::new("https://appview.example").unwrap();
    let flow = FakeOAuthFlow::default();
    let storage = Arc::new(SqliteStorage::open(":memory:").unwrap());
    let app = public.router_with_flow(flow, storage.clone());
    let login_cookie = prelogin_cookie(&app).await;
    let callback = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/oauth/callback?code=valid-code&state=valid-state&scope=atproto")
                .header("cookie", login_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let cookie = callback.headers()["set-cookie"]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();

    for (path, body) in [
        ("/post", "text=write-through-post"),
        (
            "/like",
            "uri=at%3A%2F%2Fdid%3Aplc%3Aalice%2Fapp.bsky.feed.post%2Fpds-post-r1&cid=pds-post-cid",
        ),
        (
            "/repost",
            "uri=at%3A%2F%2Fdid%3Aplc%3Aalice%2Fapp.bsky.feed.post%2Fpds-post-r1&cid=pds-post-cid",
        ),
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri(path)
                    .header("origin", "https://appview.example")
                    .header("sec-fetch-site", "same-origin")
                    .header("cookie", &cookie)
                    .header("content-type", "application/x-www-form-urlencoded")
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SEE_OTHER, "{path}");
    }

    let records = storage.records().unwrap();
    let own_post = records
        .iter()
        .find(|record| record.collection == "app.bsky.feed.post")
        .expect("createRecord response should be indexed before redirect");
    assert_eq!(own_post.did, "did:plc:alice");
    assert_eq!(
        own_post.uri,
        "at://did:plc:alice/app.bsky.feed.post/pds-post-r1"
    );
    assert_eq!(own_post.cid.as_deref(), Some("pds-post-cid"));
    assert_eq!(own_post.record["text"], "write-through-post");
    let own_like = records
        .iter()
        .find(|record| record.collection == "app.bsky.feed.like")
        .expect("like createRecord response should be indexed");
    let own_repost = records
        .iter()
        .find(|record| record.collection == "app.bsky.feed.repost")
        .expect("repost createRecord response should be indexed");
    assert_eq!(storage.notification_count("did:plc:alice").unwrap(), 2);

    let replay = Ingestor::new(storage.clone(), ScopeMode::Full);
    for (record, rkey) in [
        (own_post, "pds-post-r1"),
        (own_like, "pds-like-r1"),
        (own_repost, "pds-repost-r1"),
    ] {
        replay
            .process_json(
                &serde_json::json!({
                    "did":"did:plc:alice",
                    "time_us":record.time_us,
                    "kind":"commit",
                    "commit":{
                        "operation":"create",
                        "collection":record.collection,
                        "rkey":rkey,
                        "cid":record.cid,
                        "record":record.record
                    }
                })
                .to_string(),
            )
            .unwrap();
    }
    assert_eq!(storage.records().unwrap().len(), 3);
    assert_eq!(storage.notification_count("did:plc:alice").unwrap(), 2);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/like")
                .header("origin", "https://appview.example")
                .header("sec-fetch-site", "same-origin")
                .header("cookie", &cookie)
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "uri=at%3A%2F%2Fdid%3Aplc%3Aalice%2Fapp.bsky.feed.post%2Fpds-post-r1&cid=pds-post-cid&intent=delete",
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    assert!(
        storage
            .record_by_uri("app.bsky.feed.like", &own_like.uri)
            .unwrap()
            .is_none()
    );
    assert_eq!(storage.notification_count("did:plc:alice").unwrap(), 1);
}

#[tokio::test]
async fn web_post_actions_and_logout_reject_same_site_cross_origin_requests() {
    let flow = FakeOAuthFlow::default();
    let actions = flow.actions.clone();
    let storage = Arc::new(MemoryStorage::default());
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(flow, storage.clone());
    let login_cookie = prelogin_cookie(&app).await;
    let callback = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/oauth/callback?code=valid-code&state=valid-state&scope=atproto")
                .header("cookie", login_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let cookie = callback.headers()["set-cookie"]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();

    for (path, body) in [
        ("/post", "text=csrf+attempt"),
        (
            "/like",
            "uri=at%3A%2F%2Fdid%3Aplc%3Aauthor%2Fapp.bsky.feed.post%2Fr1&cid=bafyreifake",
        ),
        (
            "/repost",
            "uri=at%3A%2F%2Fdid%3Aplc%3Aauthor%2Fapp.bsky.feed.post%2Fr1&cid=bafyreifake",
        ),
        ("/logout", ""),
    ] {
        let mut request = Request::builder()
            .method("POST")
            .uri(path)
            .header("origin", "https://sibling.appview.example")
            .header("sec-fetch-site", "same-site")
            .header("cookie", &cookie);
        if path != "/logout" {
            request = request.header("content-type", "application/x-www-form-urlencoded");
        }
        let response = app
            .clone()
            .oneshot(request.body(Body::from(body)).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::FORBIDDEN, "{path}");
    }
    assert!(actions.lock().unwrap().is_empty());
    assert!(storage.is_registered("did:plc:alice").unwrap());
}

#[tokio::test]
async fn repeated_feed_page_request_uses_the_sixty_second_page_cache() {
    let flow = FakeOAuthFlow {
        feed_timeline_delay_ms: 2_000,
        ..FakeOAuthFlow::default()
    };
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(flow, Arc::new(MemoryStorage::default()));
    let cookie = finish_login(&app).await;
    let uri = "/app/feed?uri=at%3A%2F%2Fdid%3Aplc%3Acreator%2Fapp.bsky.feed.generator%2Fslow";
    let first_started = Instant::now();
    let first = get_html(&app, uri, &cookie).await;
    assert!(first.contains("Slow feed"), "{first}");
    assert!(first_started.elapsed() >= Duration::from_secs(2));

    let second_started = Instant::now();
    let second = get_html(&app, uri, &cookie).await;
    assert!(second.contains("Slow feed"), "{second}");
    assert!(
        second_started.elapsed() < Duration::from_millis(100),
        "cached feed page took {:?}",
        second_started.elapsed()
    );
}

#[tokio::test]
async fn session_cookie_rejects_a_tampered_hmac() {
    let public = OAuthPublic::new("https://appview.example").unwrap();
    let app = public.router_with_flow(FakeOAuthFlow::default(), Arc::new(MemoryStorage::default()));
    let login_cookie = prelogin_cookie(&app).await;
    let callback = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/oauth/callback?code=valid-code&state=valid-state&scope=atproto")
                .header("cookie", login_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(callback.status(), StatusCode::SEE_OTHER);
    let cookie = callback.headers()["set-cookie"].to_str().unwrap();
    let session_cookie = cookie
        .split(';')
        .next()
        .unwrap()
        .strip_prefix("appview_session=")
        .unwrap();
    let payload = session_cookie.split_once('.').unwrap().0;
    let forged_cookie = format!("appview_session={payload}.AAAA");
    let response = app
        .oneshot(
            Request::builder()
                .uri("/app")
                .header("cookie", forged_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    assert_eq!(response.headers()["location"], "/login");
}

#[tokio::test]
async fn logging_in_again_invalidates_a_cookie_from_before_logout() {
    let public = OAuthPublic::new("https://appview.example").unwrap();
    let storage = Arc::new(SqliteStorage::open(":memory:").unwrap());
    let app = public.router_with_flow(FakeOAuthFlow::default(), storage);

    let first_login_cookie = prelogin_cookie(&app).await;
    let first_callback = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/oauth/callback?code=valid-code&state=valid-state&scope=atproto")
                .header("cookie", first_login_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let old_session_cookie = first_callback.headers()["set-cookie"]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();

    app.clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/logout")
                .header("origin", "https://appview.example")
                .header("sec-fetch-site", "same-origin")
                .header("cookie", &old_session_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let second_login_cookie = prelogin_cookie(&app).await;
    let second_callback = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/oauth/callback?code=valid-code&state=valid-state&scope=atproto")
                .header("cookie", second_login_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let new_session_cookie = second_callback.headers()["set-cookie"]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();

    let old_session = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/app")
                .header("cookie", old_session_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(old_session.status(), StatusCode::SEE_OTHER);
    assert_eq!(old_session.headers()["location"], "/login");

    let new_session = app
        .oneshot(
            Request::builder()
                .uri("/app")
                .header("cookie", new_session_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(new_session.status(), StatusCode::OK);
}

async fn assert_multiple_device_sessions<S: Storage + 'static>(storage: Arc<S>) {
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(FakeOAuthFlow::default(), storage);
    let first_device_cookie = finish_login(&app).await;
    let second_device_cookie = finish_login(&app).await;

    for cookie in [first_device_cookie, second_device_cookie] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/app")
                    .header("cookie", cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
}

#[tokio::test]
async fn logging_in_on_another_device_keeps_the_first_session_valid() {
    assert_multiple_device_sessions(Arc::new(MemoryStorage::default())).await;
    assert_multiple_device_sessions(Arc::new(SqliteStorage::open(":memory:").unwrap())).await;
}

#[tokio::test]
async fn logged_in_web_views_render_from_the_local_index_without_pds_reads() {
    let storage = Arc::new(MemoryStorage::default());
    storage.register_user("did:plc:alice").unwrap();
    storage
        .set_verified_handle("did:plc:alice", "alice.test")
        .unwrap();
    storage
        .set_follow("did:plc:alice", "did:plc:friend", true)
        .unwrap();
    for record in [
        IndexedRecord {
            did: "did:plc:alice".into(),
            collection: "app.bsky.actor.profile".into(),
            rkey: "self".into(),
            uri: "at://did:plc:alice/app.bsky.actor.profile/self".into(),
            cid: Some("profile-cid".into()),
            time_us: 3,
            record: serde_json::json!({"displayName":"Alice from local index"}),
        },
        IndexedRecord {
            did: "did:plc:alice".into(),
            collection: "app.bsky.feed.post".into(),
            rkey: "local-post".into(),
            uri: "at://did:plc:alice/app.bsky.feed.post/local-post".into(),
            cid: Some("local-cid".into()),
            time_us: 2,
            record: serde_json::json!({"text":"post from local index"}),
        },
        IndexedRecord {
            did: "did:plc:friend".into(),
            collection: "app.bsky.feed.post".into(),
            rkey: "friend-post".into(),
            uri: "at://did:plc:friend/app.bsky.feed.post/friend-post".into(),
            cid: Some("friend-cid".into()),
            time_us: 1,
            record: serde_json::json!({"text":"followed account from local index"}),
        },
        IndexedRecord {
            did: "did:plc:alice".into(),
            collection: "app.bsky.graph.follow".into(),
            rkey: "friend-follow".into(),
            uri: "at://did:plc:alice/app.bsky.graph.follow/friend-follow".into(),
            cid: Some("follow-cid".into()),
            time_us: 0,
            record: serde_json::json!({"subject":"did:plc:friend"}),
        },
    ] {
        storage.put_record(&record).unwrap();
    }
    let flow = FakeOAuthFlow::default();
    let actions = flow.actions.clone();
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(flow, storage);
    let login_cookie = prelogin_cookie(&app).await;
    let callback = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/oauth/callback?code=valid-code&state=valid-state&scope=atproto")
                .header("cookie", login_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let cookie = callback.headers()["set-cookie"]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/app")
                .header("cookie", &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let html = String::from_utf8(body.to_vec()).unwrap();
    assert!(html.contains("post from local index"));
    assert!(html.contains("followed account from local index"));
    assert!(!html.contains("from OAuth timeline"));
    assert!(
        !actions
            .lock()
            .unwrap()
            .iter()
            .any(|action| action == "timeline")
    );

    let profile = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/profile?did=did%3Aplc%3Aalice")
                .header("cookie", &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let profile_html = String::from_utf8(
        profile
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert!(profile_html.contains("Alice from local index"));

    let thread = app
        .oneshot(
            Request::builder()
                .uri("/thread?uri=at%3A%2F%2Fdid%3Aplc%3Aalice%2Fapp.bsky.feed.post%2Flocal-post")
                .header("cookie", &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let thread_html = String::from_utf8(
        thread
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert!(thread_html.contains("post from local index"));
    assert!(
        !actions
            .lock()
            .unwrap()
            .iter()
            .any(|action| action == "timeline")
    );
}

#[tokio::test]
async fn web_posts_show_local_counts_and_liked_state_and_submit_unlike() {
    let storage = Arc::new(MemoryStorage::default());
    let post_uri = "at://did:plc:friend/app.bsky.feed.post/p1";
    for record in [
        IndexedRecord {
            did: "did:plc:friend".into(),
            collection: "app.bsky.feed.post".into(),
            rkey: "p1".into(),
            uri: post_uri.into(),
            cid: Some("cid-p1".into()),
            time_us: 50,
            record: serde_json::json!({"$type":"app.bsky.feed.post","text":"indexed post","createdAt":"2026-10-27T00:00:00Z"}),
        },
        IndexedRecord {
            did: "did:plc:alice".into(),
            collection: "app.bsky.feed.like".into(),
            rkey: "like-p1".into(),
            uri: "at://did:plc:alice/app.bsky.feed.like/like-p1".into(),
            cid: Some("cid-like-p1".into()),
            time_us: 40,
            record: serde_json::json!({"subject":{"uri":post_uri,"cid":"cid-p1"}}),
        },
        IndexedRecord {
            did: "did:plc:alice".into(),
            collection: "app.bsky.feed.repost".into(),
            rkey: "repost-p1".into(),
            uri: "at://did:plc:alice/app.bsky.feed.repost/repost-p1".into(),
            cid: Some("cid-repost-p1".into()),
            time_us: 30,
            record: serde_json::json!({"subject":{"uri":post_uri,"cid":"cid-p1"}}),
        },
        IndexedRecord {
            did: "did:plc:friend".into(),
            collection: "app.bsky.feed.post".into(),
            rkey: "reply-p1".into(),
            uri: "at://did:plc:friend/app.bsky.feed.post/reply-p1".into(),
            cid: Some("cid-reply-p1".into()),
            time_us: 20,
            record: serde_json::json!({"text":"reply","reply":{"parent":{"uri":post_uri},"root":{"uri":post_uri}}}),
        },
        IndexedRecord {
            did: "did:plc:alice".into(),
            collection: "app.bsky.graph.follow".into(),
            rkey: "follow-friend".into(),
            uri: "at://did:plc:alice/app.bsky.graph.follow/follow-friend".into(),
            cid: Some("cid-follow".into()),
            time_us: 10,
            record: serde_json::json!({"subject":"did:plc:friend"}),
        },
    ] {
        storage.put_record(&record).unwrap();
    }
    storage
        .set_follow("did:plc:alice", "did:plc:friend", true)
        .unwrap();
    let flow = FakeOAuthFlow::default();
    let actions = flow.actions.clone();
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(flow, storage);
    let cookie = finish_login(&app).await;
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/app")
                .header("cookie", &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let html = String::from_utf8(
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert_eq!(html.matches("class=\"action-count\">1</span>").count(), 3);
    assert!(html.contains("class=\"post-action is-liked\""), "{html}");
    assert!(html.contains("aria-pressed=\"true\""), "{html}");

    let response = app.oneshot(
        Request::builder()
            .method("POST")
            .uri("/like")
            .header("origin", "https://appview.example")
            .header("sec-fetch-site", "same-origin")
            .header("content-type", "application/x-www-form-urlencoded")
            .header("cookie", &cookie)
            .body(Body::from("uri=at%3A%2F%2Fdid%3Aplc%3Afriend%2Fapp.bsky.feed.post%2Fp1&cid=cid-p1&intent=delete"))
            .unwrap(),
    ).await.unwrap();
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    assert!(
        actions
            .lock()
            .unwrap()
            .iter()
            .any(|action| action.starts_with("unlike:"))
    );
}

#[tokio::test]
async fn timeline_replies_name_the_parent_author_and_link_to_the_thread() {
    let storage = Arc::new(MemoryStorage::default());
    storage
        .set_verified_handle("did:plc:friend", "friend.test")
        .unwrap();
    let parent_uri = "at://did:plc:friend/app.bsky.feed.post/parent";
    for record in [
        IndexedRecord {
            did: "did:plc:friend".into(),
            collection: "app.bsky.feed.post".into(),
            rkey: "parent".into(),
            uri: parent_uri.into(),
            cid: Some("cid-parent".into()),
            time_us: 20,
            record: serde_json::json!({"text":"parent post"}),
        },
        IndexedRecord {
            did: "did:plc:alice".into(),
            collection: "app.bsky.feed.post".into(),
            rkey: "reply".into(),
            uri: "at://did:plc:alice/app.bsky.feed.post/reply".into(),
            cid: Some("cid-reply".into()),
            time_us: 10,
            record: serde_json::json!({"text":"reply post","reply":{"parent":{"uri":parent_uri,"cid":"cid-parent"},"root":{"uri":parent_uri,"cid":"cid-parent"}}}),
        },
    ] {
        storage.put_record(&record).unwrap();
    }
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(FakeOAuthFlow::default(), storage);
    let cookie = finish_login(&app).await;
    let response = app
        .oneshot(
            Request::builder()
                .uri("/app")
                .header("cookie", cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let html = String::from_utf8(
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert!(html.contains("class=\"reply-parent-card\""), "{html}");
    assert!(html.contains("parent post"), "{html}");
    assert!(html.contains("@friend.test"), "{html}");
    assert!(html.contains("class=\"reply-connector\""), "{html}");
    assert!(
        html.contains(
            "href=\"/app/thread?uri=at%3A%2F%2Fdid%3Aplc%3Afriend%2Fapp.bsky.feed.post%2Fparent\""
        ),
        "{html}"
    );
    assert!(
        html.contains(
            "href=\"/app/thread?uri=at%3A%2F%2Fdid%3Aplc%3Aalice%2Fapp.bsky.feed.post%2Freply\""
        ),
        "{html}"
    );
}

#[tokio::test]
async fn timeline_reply_context_uses_verified_parent_handle_when_post_is_missing() {
    let storage = Arc::new(MemoryStorage::default());
    storage
        .set_verified_handle("did:plc:friend", "friend.test")
        .unwrap();
    storage
        .put_record(&IndexedRecord {
            did: "did:plc:alice".into(),
            collection: "app.bsky.feed.post".into(),
            rkey: "reply".into(),
            uri: "at://did:plc:alice/app.bsky.feed.post/reply".into(),
            cid: Some("reply-cid".into()),
            time_us: 10,
            record: serde_json::json!({
                "text":"A reply",
                "reply":{
                    "parent":{"uri":"at://did:plc:friend/app.bsky.feed.post/missing","cid":"missing-cid"},
                    "root":{"uri":"at://did:plc:friend/app.bsky.feed.post/missing","cid":"missing-cid"}
                }
            }),
        })
        .unwrap();
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(FakeOAuthFlow::default(), storage);
    let cookie = finish_login(&app).await;
    let response = app
        .oneshot(
            Request::builder()
                .uri("/app")
                .header("cookie", cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let html = String::from_utf8(
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert!(html.contains("Reply to <a"), "{html}");
    assert!(html.contains("@friend.test"), "{html}");
    assert!(html.contains("class=\"reply-connector\""), "{html}");
}

#[tokio::test]
async fn web_thread_renders_the_parent_chain_and_nested_replies_in_time_order() {
    let storage = Arc::new(MemoryStorage::default());
    storage
        .set_verified_handle("did:plc:friend", "friend.test")
        .unwrap();
    let root_uri = "at://did:plc:friend/app.bsky.feed.post/root";
    let parent_uri = "at://did:plc:friend/app.bsky.feed.post/parent";
    let focus_uri = "at://did:plc:friend/app.bsky.feed.post/focus";
    let early_uri = "at://did:plc:friend/app.bsky.feed.post/early";
    for (rkey, time_us, text, parent, root) in [
        ("root", 10, "root post", None, None),
        ("parent", 20, "parent post", Some(root_uri), Some(root_uri)),
        (
            "focus",
            30,
            "focused post",
            Some(parent_uri),
            Some(root_uri),
        ),
        ("late", 50, "late reply", Some(focus_uri), Some(root_uri)),
        ("early", 40, "early reply", Some(focus_uri), Some(root_uri)),
        (
            "nested",
            60,
            "nested reply",
            Some(early_uri),
            Some(root_uri),
        ),
    ] {
        let reply = parent.map(
            |parent| serde_json::json!({"parent":{"uri":parent},"root":{"uri":root.unwrap()}}),
        );
        let record = if let Some(reply) = reply {
            serde_json::json!({"text":text,"reply":reply})
        } else {
            serde_json::json!({"text":text})
        };
        storage
            .put_record(&IndexedRecord {
                did: "did:plc:friend".into(),
                collection: "app.bsky.feed.post".into(),
                rkey: rkey.into(),
                uri: format!("at://did:plc:friend/app.bsky.feed.post/{rkey}"),
                cid: Some(format!("cid-{rkey}")),
                time_us,
                record,
            })
            .unwrap();
    }
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(FakeOAuthFlow::default(), storage);
    let cookie = finish_login(&app).await;
    let uri = url::form_urlencoded::byte_serialize(focus_uri.as_bytes()).collect::<String>();
    let response = app
        .oneshot(
            Request::builder()
                .uri(format!("/thread?uri={uri}"))
                .header("cookie", cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let html = String::from_utf8(
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    let root = html.find("root post").unwrap();
    let parent = html.find("parent post").unwrap();
    let focus = html.find("focused post").unwrap();
    let early = html.find("early reply").unwrap();
    let nested = html.find("nested reply").unwrap();
    let late = html.find("late reply").unwrap();
    assert!(
        root < parent && parent < focus && focus < early && early < nested && nested < late,
        "{html}"
    );
    assert!(html.contains("action=\"/reply\""), "{html}");
    assert!(html.contains("name=\"parent_uri\""), "{html}");
    assert!(html.contains("maxlength=\"300\""), "{html}");
}

#[tokio::test]
async fn reply_form_checks_origin_and_writes_through_to_the_index() {
    let storage = Arc::new(MemoryStorage::default());
    let parent_uri = "at://did:plc:friend/app.bsky.feed.post/parent";
    storage
        .put_record(&IndexedRecord {
            did: "did:plc:friend".into(),
            collection: "app.bsky.feed.post".into(),
            rkey: "parent".into(),
            uri: parent_uri.into(),
            cid: Some("parent-cid".into()),
            time_us: 1,
            record: serde_json::json!({"text":"Parent post"}),
        })
        .unwrap();
    let flow = FakeOAuthFlow::default();
    let actions = flow.actions.clone();
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(flow, storage.clone());
    let cookie = finish_login(&app).await;
    let encoded_parent =
        url::form_urlencoded::byte_serialize(parent_uri.as_bytes()).collect::<String>();
    let thread_html = get_html(&app, &format!("/thread?uri={encoded_parent}"), &cookie).await;
    assert!(thread_html.contains("action=\"/reply\""), "{thread_html}");
    assert!(
        thread_html.contains(
            "name=\"media\" accept=\"image/*,video/mp4,video/webm,video/quicktime\" multiple"
        ),
        "{thread_html}"
    );
    let body =
        "parent_uri=at%3A%2F%2Fdid%3Aplc%3Afriend%2Fapp.bsky.feed.post%2Fparent&text=Reply+text";

    let rejected = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/reply")
                .header("origin", "https://attacker.example")
                .header("sec-fetch-site", "cross-site")
                .header("content-type", "application/x-www-form-urlencoded")
                .header("cookie", &cookie)
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(rejected.status(), StatusCode::FORBIDDEN);

    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/reply")
                .header("origin", "https://appview.example")
                .header("sec-fetch-site", "same-origin")
                .header("content-type", "application/x-www-form-urlencoded")
                .header("cookie", cookie)
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    assert_eq!(
        actions.lock().unwrap().as_slice(),
        [
            "reply:did:plc:alice:Reply text:at://did:plc:friend/app.bsky.feed.post/parent:parent-cid:at://did:plc:friend/app.bsky.feed.post/parent:parent-cid"
        ]
    );
    let indexed = storage
        .record_by_uri(
            "app.bsky.feed.post",
            "at://did:plc:alice/app.bsky.feed.post/pds-reply-r1",
        )
        .unwrap()
        .unwrap();
    assert_eq!(indexed.record["reply"]["root"]["uri"], parent_uri);
    assert_eq!(indexed.record["reply"]["parent"]["cid"], "parent-cid");
}

#[tokio::test]
async fn notifications_page_is_linked_and_shows_all_local_notification_reasons() {
    let storage = Arc::new(MemoryStorage::default());
    storage
        .set_verified_handle("did:plc:friend", "friend.test")
        .unwrap();
    let own_uri = "at://did:plc:alice/app.bsky.feed.post/own";
    let events = [
        (
            "like",
            "app.bsky.feed.like",
            serde_json::json!({"subject":{"uri":own_uri,"cid":"cid-own"}}),
        ),
        (
            "repost",
            "app.bsky.feed.repost",
            serde_json::json!({"subject":{"uri":own_uri,"cid":"cid-own"}}),
        ),
        (
            "reply",
            "app.bsky.feed.post",
            serde_json::json!({"text":"reply body","reply":{"parent":{"uri":own_uri,"cid":"cid-own"},"root":{"uri":own_uri,"cid":"cid-own"}}}),
        ),
        (
            "mention",
            "app.bsky.feed.post",
            serde_json::json!({"text":"mention body","facets":[{"features":[{"$type":"app.bsky.richtext.facet#mention","did":"did:plc:alice"}]}]}),
        ),
        (
            "quote",
            "app.bsky.feed.post",
            serde_json::json!({"text":"quote body","embed":{"$type":"app.bsky.embed.record","record":{"uri":own_uri,"cid":"cid-own"}}}),
        ),
        (
            "follow",
            "app.bsky.graph.follow",
            serde_json::json!({"subject":"did:plc:alice"}),
        ),
    ];
    storage
        .put_record(&IndexedRecord {
            did: "did:plc:alice".into(),
            collection: "app.bsky.feed.post".into(),
            rkey: "own".into(),
            uri: own_uri.into(),
            cid: Some("cid-own".into()),
            time_us: 10,
            record: serde_json::json!({"text":"my post"}),
        })
        .unwrap();
    for (index, (rkey, collection, record)) in events.into_iter().enumerate() {
        storage
            .put_record(&IndexedRecord {
                did: "did:plc:friend".into(),
                collection: collection.into(),
                rkey: rkey.into(),
                uri: format!("at://did:plc:friend/{collection}/{rkey}"),
                cid: Some(format!("cid-{rkey}")),
                time_us: 100 + index as u64,
                record,
            })
            .unwrap();
    }
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(FakeOAuthFlow::default(), storage);
    let cookie = finish_login(&app).await;
    let home = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/app")
                .header("cookie", &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let home_html = String::from_utf8(
        home.into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert!(
        home_html.contains("href=\"/notifications\">"),
        "{home_html}"
    );
    assert!(
        home_html.contains("<span>Notifications</span>"),
        "{home_html}"
    );

    let page = app
        .oneshot(
            Request::builder()
                .uri("/notifications")
                .header("cookie", cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(page.status(), StatusCode::OK);
    let html = String::from_utf8(
        page.into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    for reason in [
        "liked your post",
        "reposted your post",
        "replied to your post",
        "mentioned you",
        "quoted your post",
        "started following you",
    ] {
        assert!(html.contains(reason), "missing {reason}: {html}");
    }
    assert!(html.find("started following you").unwrap() < html.find("quoted your post").unwrap());
}

#[tokio::test]
async fn notifications_stream_requires_a_session() {
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(FakeOAuthFlow::default(), Arc::new(MemoryStorage::default()));
    let response = app
        .oneshot(
            Request::builder()
                .uri("/notifications/stream")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn opening_notifications_marks_prior_events_seen() {
    let storage = Arc::new(MemoryStorage::default());
    let viewer = "did:plc:alice";
    let now_us = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64;
    storage
        .put_record(&IndexedRecord {
            did: "did:plc:friend".into(),
            collection: "app.bsky.feed.like".into(),
            rkey: "unread-like".into(),
            uri: "at://did:plc:friend/app.bsky.feed.like/unread-like".into(),
            cid: None,
            time_us: now_us,
            record: serde_json::json!({"subject":{"uri":"at://did:plc:alice/app.bsky.feed.post/own"}}),
        })
        .unwrap();
    assert_eq!(storage.unread_notification_count(viewer).unwrap(), 1);
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(FakeOAuthFlow::default(), storage.clone());
    let cookie = finish_login(&app).await;
    let response = app
        .oneshot(
            Request::builder()
                .uri("/notifications")
                .header("cookie", cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(storage.unread_notification_count(viewer).unwrap(), 0);
}

#[tokio::test]
async fn ingest_like_updates_a_connected_notification_stream_within_one_second() {
    let storage = Arc::new(MemoryStorage::default());
    let viewer = "did:plc:alice";
    let now_us = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64;
    storage.mark_notifications_seen(viewer, now_us).unwrap();
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(FakeOAuthFlow::default(), storage.clone());
    let cookie = finish_login(&app).await;
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/notifications/stream")
                .header("cookie", &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert!(
        response.headers()["content-type"]
            .to_str()
            .unwrap()
            .starts_with("text/event-stream")
    );
    let mut body = response.into_body();
    let first = tokio::time::timeout(Duration::from_secs(1), body.frame())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .into_data()
        .unwrap();
    assert!(String::from_utf8_lossy(&first).contains("data: 0"));

    Ingestor::new(storage, ScopeMode::Full)
        .process_json(
            &serde_json::json!({
                "did":"did:plc:friend",
                "time_us":now_us + 1_000_000,
                "kind":"commit",
                "commit":{
                    "operation":"create",
                    "collection":"app.bsky.feed.like",
                    "rkey":"live-like",
                    "cid":"live-like-cid",
                    "record":{"subject":{"uri":"at://did:plc:alice/app.bsky.feed.post/own","cid":"own-cid"}}
                }
            })
            .to_string(),
        )
        .unwrap();
    let update = tokio::time::timeout(Duration::from_secs(1), body.frame())
        .await
        .unwrap()
        .unwrap()
        .unwrap()
        .into_data()
        .unwrap();
    assert!(String::from_utf8_lossy(&update).contains("data: 1"));
}

#[tokio::test]
async fn graceful_shutdown_ends_connected_notification_stream_and_server() {
    let storage = Arc::new(MemoryStorage::default());
    let shutdown = WebShutdown::new();
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow_and_shutdown(FakeOAuthFlow::default(), storage, shutdown.clone());
    let cookie = finish_login(&app).await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server_shutdown = shutdown.clone();
    let server = tokio::spawn(async move {
        let mut receiver = server_shutdown.subscribe();
        axum::serve(listener, app)
            .with_graceful_shutdown(async move {
                if !*receiver.borrow() {
                    let _ = receiver.changed().await;
                }
            })
            .await
    });
    let client = reqwest::Client::new();
    let mut response = tokio::time::timeout(
        Duration::from_secs(1),
        client
            .get(format!("http://{address}/notifications/stream"))
            .header("cookie", cookie)
            .send(),
    )
    .await
    .expect("notification stream should connect within one second")
    .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(shutdown.active_notification_streams(), 1);
    let first = tokio::time::timeout(Duration::from_secs(1), response.chunk())
        .await
        .expect("stream should send its initial count")
        .unwrap()
        .unwrap();
    assert!(String::from_utf8_lossy(&first).contains("data: 0"));

    shutdown.shutdown();
    let end = tokio::time::timeout(Duration::from_secs(1), response.chunk())
        .await
        .expect("shutdown should end the notification stream within one second")
        .unwrap();
    assert!(end.is_none(), "notification stream must finish on shutdown");
    assert_eq!(shutdown.active_notification_streams(), 0);
    tokio::time::timeout(Duration::from_secs(1), server)
        .await
        .expect("server should finish graceful shutdown with the stream")
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn notification_streams_are_same_origin_and_capped_per_viewer() {
    let storage = Arc::new(MemoryStorage::default());
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(FakeOAuthFlow::default(), storage);
    let cookie = finish_login(&app).await;
    let cross_origin = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/notifications/stream")
                .header("cookie", &cookie)
                .header("origin", "https://attacker.example")
                .header("sec-fetch-site", "cross-site")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(cross_origin.status(), StatusCode::FORBIDDEN);

    let mut streams = Vec::new();
    for _ in 0..4 {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/notifications/stream")
                    .header("cookie", &cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        streams.push(response.into_body());
    }
    let capped = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/notifications/stream")
                .header("cookie", &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(capped.status(), StatusCode::TOO_MANY_REQUESTS);

    drop(streams.pop());
    let after_close = app
        .oneshot(
            Request::builder()
                .uri("/notifications/stream")
                .header("cookie", cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(after_close.status(), StatusCode::OK);
}

#[tokio::test]
async fn authenticated_pages_render_unread_count_in_both_navigation_bells() {
    let storage = Arc::new(MemoryStorage::default());
    let now_us = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64;
    for index in 0..10 {
        storage
            .put_record(&IndexedRecord {
                did: format!("did:plc:actor{index}"),
                collection: "app.bsky.feed.like".into(),
                rkey: format!("like{index}"),
                uri: format!("at://did:plc:actor{index}/app.bsky.feed.like/like{index}"),
                cid: None,
                time_us: now_us + index,
                record: serde_json::json!({"subject":{"uri":"at://did:plc:alice/app.bsky.feed.post/own"}}),
            })
            .unwrap();
    }
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(FakeOAuthFlow::default(), storage);
    let cookie = finish_login(&app).await;
    let html = get_html(&app, "/app", &cookie).await;
    assert!(html.contains("class=\"primary-nav\""));
    assert!(html.contains("class=\"mobile-nav\""));
    assert_eq!(html.matches(">9+</span>").count(), 2);
    assert_eq!(html.matches("10 unread notifications").count(), 2);
}

#[test]
fn sqlite_notification_last_seen_persists_and_filters_unread_events() {
    let path = std::path::PathBuf::from(format!(
        "target/case25-notification-seen-{}.sqlite3",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&path);
    let viewer = "did:plc:case25viewer";
    let last_seen = 2_000_000_000_000_000;
    {
        let storage = SqliteStorage::open(&path).unwrap();
        storage.mark_notifications_seen(viewer, last_seen).unwrap();
        storage
            .put_record(&IndexedRecord {
                did: "did:plc:case25actor".into(),
                collection: "app.bsky.feed.like".into(),
                rkey: "old".into(),
                uri: "at://did:plc:case25actor/app.bsky.feed.like/old".into(),
                cid: None,
                time_us: last_seen - 1,
                record: serde_json::json!({"subject":{"uri":"at://did:plc:case25viewer/app.bsky.feed.post/p"}}),
            })
            .unwrap();
        assert_eq!(storage.unread_notification_count(viewer).unwrap(), 0);
        storage
            .put_record(&IndexedRecord {
                did: "did:plc:case25actor".into(),
                collection: "app.bsky.feed.like".into(),
                rkey: "new".into(),
                uri: "at://did:plc:case25actor/app.bsky.feed.like/new".into(),
                cid: None,
                time_us: last_seen + 1,
                record: serde_json::json!({"subject":{"uri":"at://did:plc:case25viewer/app.bsky.feed.post/p"}}),
            })
            .unwrap();
        assert_eq!(storage.unread_notification_count(viewer).unwrap(), 1);
    }
    let reopened = SqliteStorage::open(&path).unwrap();
    assert_eq!(reopened.notification_last_seen(viewer).unwrap(), last_seen);
    assert_eq!(reopened.unread_notification_count(viewer).unwrap(), 1);
    drop(reopened);
    for suffix in ["", "-wal", "-shm"] {
        let mut sidecar = path.as_os_str().to_owned();
        sidecar.push(suffix);
        let _ = std::fs::remove_file(sidecar);
    }
}

#[tokio::test]
async fn timeline_profile_and_notification_pages_offer_older_cursor_links() {
    let storage = Arc::new(MemoryStorage::default());
    let post_a = "at://did:plc:alice/app.bsky.feed.post/a";
    let post_b = "at://did:plc:alice/app.bsky.feed.post/b";
    for (rkey, uri, time_us) in [("a", post_a, 20), ("b", post_b, 10)] {
        storage
            .put_record(&IndexedRecord {
                did: "did:plc:alice".into(),
                collection: "app.bsky.feed.post".into(),
                rkey: rkey.into(),
                uri: uri.into(),
                cid: Some(format!("cid-{rkey}")),
                time_us,
                record: serde_json::json!({"text":format!("post {rkey}")}),
            })
            .unwrap();
        storage
            .put_record(&IndexedRecord {
                did: "did:plc:friend".into(),
                collection: "app.bsky.feed.like".into(),
                rkey: format!("like-{rkey}"),
                uri: format!("at://did:plc:friend/app.bsky.feed.like/like-{rkey}"),
                cid: Some(format!("cid-like-{rkey}")),
                time_us: time_us + 100,
                record: serde_json::json!({"subject":{"uri":uri,"cid":format!("cid-{rkey}")}}),
            })
            .unwrap();
    }
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(FakeOAuthFlow::default(), storage);
    let cookie = finish_login(&app).await;
    for uri in [
        "/app?limit=1",
        "/profile?did=did%3Aplc%3Aalice&limit=1",
        "/notifications?limit=1",
    ] {
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri(uri)
                    .header("cookie", &cookie)
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK, "{uri}");
        let html = String::from_utf8(
            response
                .into_body()
                .collect()
                .await
                .unwrap()
                .to_bytes()
                .to_vec(),
        )
        .unwrap();
        assert!(
            html.contains("rel=\"next\"") && html.contains(">Older</a>"),
            "{uri}: {html}"
        );
    }
}

#[tokio::test]
async fn page_fetches_missing_author_profile_stores_it_and_keeps_avatar_url_safe() {
    let storage = Arc::new(MemoryStorage::default());
    let author = "did:plc:author";
    storage.set_follow("did:plc:alice", author, true).unwrap();
    storage
        .put_record(&IndexedRecord {
            did: author.into(),
            collection: "app.bsky.feed.post".into(),
            rkey: "post".into(),
            uri: format!("at://{author}/app.bsky.feed.post/post"),
            cid: Some("post-cid".into()),
            time_us: 2,
            record: serde_json::json!({
                "$type":"app.bsky.feed.post",
                "text":"visible post",
                "createdAt":"2026-10-28T00:00:00Z"
            }),
        })
        .unwrap();
    let flow = FakeOAuthFlow::default();
    flow.author_profiles.lock().unwrap().insert(
        author.into(),
        FetchedAuthorProfile {
            profile: Some(IndexedRecord {
                did: author.into(),
                collection: "app.bsky.actor.profile".into(),
                rkey: "self".into(),
                uri: format!("at://{author}/app.bsky.actor.profile/self"),
                cid: Some("profile-cid".into()),
                time_us: 1_800_000_000_000_000,
                record: serde_json::json!({
                    "$type":"app.bsky.actor.profile",
                    "displayName":"<Hydrated & Co>",
                    "avatar":{"$type":"blob","ref":{"$link":"avatar-cid"}}
                }),
            }),
            verified_handle: Some("author.test".into()),
        },
    );
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(flow.clone(), storage.clone());
    let cookie = finish_login(&app).await;
    let response = app
        .oneshot(
            Request::builder()
                .uri("/app")
                .header("cookie", cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let html = String::from_utf8(
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert!(html.contains("&lt;Hydrated &amp; Co&gt;"), "{html}");
    assert!(
        html.contains(&format!(
            "src=\"https://cdn.bsky.app/img/avatar_thumbnail/plain/{author}/avatar-cid@jpeg\""
        )),
        "{html}"
    );
    assert_eq!(flow.profile_requests.lock().unwrap().as_slice(), [author]);
    assert_eq!(
        storage
            .get_record(author, "app.bsky.actor.profile", "self")
            .unwrap()
            .unwrap()
            .record["displayName"],
        "<Hydrated & Co>"
    );
}

#[tokio::test]
async fn app_timeline_waits_for_new_author_profiles_before_rendering() {
    let storage = Arc::new(MemoryStorage::default());
    let flow = FakeOAuthFlow::default();
    let authors = (0..10)
        .map(|index| format!("did:plc:slow-author-{index}"))
        .collect::<Vec<_>>();
    for (index, author) in authors.iter().enumerate() {
        storage.set_follow("did:plc:alice", author, true).unwrap();
        storage
            .put_record(&fake_feed_post(
                author,
                &format!("post-{index}"),
                20 - index as u64,
                &format!("visible post {index}"),
            ))
            .unwrap();
        flow.profile_delays_ms
            .lock()
            .unwrap()
            .insert(author.clone(), 60);
        flow.author_profiles.lock().unwrap().insert(
            author.clone(),
            FetchedAuthorProfile {
                profile: Some(IndexedRecord {
                    did: author.clone(),
                    collection: "app.bsky.actor.profile".into(),
                    rkey: "self".into(),
                    uri: format!("at://{author}/app.bsky.actor.profile/self"),
                    cid: Some(format!("profile-cid-{index}")),
                    time_us: 1_800_000_000_000_000,
                    record: serde_json::json!({
                        "$type":"app.bsky.actor.profile",
                        "displayName":format!("Hydrated author {index}")
                    }),
                }),
                verified_handle: Some(format!("author-{index}.test")),
            },
        );
    }
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(flow.clone(), storage.clone());
    let cookie = finish_login(&app).await;
    let timing = Arc::new(Mutex::new(None));
    let subscriber = tracing_subscriber::registry().with(ProfileHydrationTiming(timing.clone()));
    let _guard = tracing::subscriber::set_default(subscriber);
    let started = Instant::now();
    let html = get_html(&app, "/app", &cookie).await;
    let elapsed = started.elapsed();
    assert!(elapsed < Duration::from_millis(500), "took {elapsed:?}");
    for (index, _) in authors.iter().enumerate() {
        assert!(html.contains(&format!("Hydrated author {index}")), "{html}");
        assert!(html.contains(&format!("visible post {index}")), "{html}");
    }
    assert_eq!(flow.profile_requests.lock().unwrap().len(), authors.len());
    let profile_hydration_ms = timing
        .lock()
        .unwrap()
        .expect("page timing should record the profile wait");
    assert!(
        profile_hydration_ms >= 60,
        "wait reported {profile_hydration_ms} ms"
    );
    assert!(
        profile_hydration_ms < 500,
        "wait reported {profile_hydration_ms} ms"
    );
}

#[tokio::test]
async fn app_timeline_skips_fresh_indexed_profiles_without_fetching() {
    let storage = Arc::new(MemoryStorage::default());
    let author = "did:plc:fresh-author";
    storage.set_follow("did:plc:alice", author, true).unwrap();
    storage
        .put_record(&fake_feed_post(author, "post", 2, "fresh indexed post"))
        .unwrap();
    storage
        .put_record(&IndexedRecord {
            did: author.into(),
            collection: "app.bsky.actor.profile".into(),
            rkey: "self".into(),
            uri: format!("at://{author}/app.bsky.actor.profile/self"),
            cid: Some("fresh-profile-cid".into()),
            time_us: 1_800_000_000_000_000,
            record: serde_json::json!({
                "$type":"app.bsky.actor.profile",
                "displayName":"Freshly indexed"
            }),
        })
        .unwrap();
    storage
        .set_verified_handle(author, "fresh-author.test")
        .unwrap();
    let flow = FakeOAuthFlow::default();
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(flow.clone(), storage);
    let cookie = finish_login(&app).await;
    let timing = Arc::new(Mutex::new(None));
    let subscriber = tracing_subscriber::registry().with(ProfileHydrationTiming(timing.clone()));
    let _guard = tracing::subscriber::set_default(subscriber);
    let started = Instant::now();
    let html = get_html(&app, "/app", &cookie).await;

    assert!(html.contains("Freshly indexed"), "{html}");
    assert!(flow.profile_requests.lock().unwrap().is_empty());
    assert!(started.elapsed() < Duration::from_millis(200));
    let profile_hydration_ms = timing
        .lock()
        .unwrap()
        .expect("page timing should record profile hydration");
    assert!(
        profile_hydration_ms < 50,
        "wait reported {profile_hydration_ms} ms"
    );
}

#[tokio::test]
async fn profile_fetch_order_deduplicates_page_authors_and_negative_caches_failures() {
    let storage = Arc::new(MemoryStorage::default());
    let first = "did:plc:first";
    let second = "did:plc:second";
    for author in [first, second] {
        storage.set_follow("did:plc:alice", author, true).unwrap();
    }
    storage
        .put_record(&fake_feed_post(first, "top", 30, "top from first"))
        .unwrap();
    storage
        .put_record(&fake_feed_post(second, "middle", 20, "middle from second"))
        .unwrap();
    storage
        .put_record(&fake_feed_post(first, "lower", 10, "lower from first"))
        .unwrap();
    let flow = FakeOAuthFlow::default();
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(flow.clone(), storage.clone());
    let cookie = finish_login(&app).await;

    let first_html = get_html(&app, "/app", &cookie).await;
    assert!(
        first_html.find("top from first").unwrap() < first_html.find("middle from second").unwrap()
    );
    assert_eq!(
        flow.profile_requests.lock().unwrap().as_slice(),
        [first, second]
    );
    assert_eq!(
        storage.pending_handle_refreshes(10).unwrap(),
        [first.to_owned(), second.to_owned()]
    );

    let second_html = get_html(&app, "/app", &cookie).await;
    assert!(second_html.contains("top from first"));
    assert_eq!(
        flow.profile_requests.lock().unwrap().as_slice(),
        [first, second],
        "the fresh negative cache should prevent another PDS read"
    );
    let status = storage.profile_index_snapshot().unwrap();
    assert!(!status[first].has_profile);
    assert!(!status[second].has_profile);
}

#[tokio::test]
async fn profile_budget_keeps_page_rendering_and_queues_timed_out_authors_in_order() {
    let storage = Arc::new(MemoryStorage::default());
    let authors = [
        "did:plc:slow-first",
        "did:plc:slow-second",
        "did:plc:slow-third",
    ];
    let flow = FakeOAuthFlow::default();
    for (index, author) in authors.iter().enumerate() {
        storage.set_follow("did:plc:alice", author, true).unwrap();
        storage
            .put_record(&fake_feed_post(
                author,
                &format!("slow-{index}"),
                30 - index as u64,
                &format!("visible slow post {index}"),
            ))
            .unwrap();
        flow.profile_delays_ms
            .lock()
            .unwrap()
            .insert((*author).to_owned(), 2_000);
        flow.author_profiles.lock().unwrap().insert(
            (*author).to_owned(),
            FetchedAuthorProfile {
                profile: Some(IndexedRecord {
                    did: (*author).to_owned(),
                    collection: "app.bsky.actor.profile".into(),
                    rkey: "self".into(),
                    uri: format!("at://{author}/app.bsky.actor.profile/self"),
                    cid: Some(format!("profile-cid-{index}")),
                    time_us: 1_800_000_000_000_000,
                    record: serde_json::json!({
                        "$type":"app.bsky.actor.profile",
                        "displayName":format!("Background hydrated {index}")
                    }),
                }),
                verified_handle: Some(format!("slow-author-{index}.test")),
            },
        );
    }
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(flow.clone(), storage.clone());
    let cookie = finish_login(&app).await;

    let started = Instant::now();
    let html = get_html(&app, "/app", &cookie).await;
    let elapsed = started.elapsed();
    assert!(elapsed <= Duration::from_millis(1700), "took {elapsed:?}");
    assert!(html.contains("visible slow post 0"));
    assert!(html.contains("visible slow post 2"));
    for author in authors {
        assert!(
            html.contains(author),
            "fallback author {author} missing from {html}"
        );
        assert!(!html.contains("Background hydrated"), "{html}");
    }
    assert_eq!(flow.profile_requests.lock().unwrap().as_slice(), authors);
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if storage.pending_profile_fetches(10).unwrap().len() == authors.len() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("background profile timeout should queue each author");
    assert_eq!(
        storage.pending_profile_fetches(10).unwrap(),
        authors.map(str::to_owned)
    );
    assert_eq!(
        storage.pending_handle_refreshes(10).unwrap(),
        authors.map(str::to_owned)
    );
    for author in authors {
        flow.profile_delays_ms
            .lock()
            .unwrap()
            .insert(author.to_owned(), 0);
    }
    let refresh = ProfileRefreshWorker::new(flow, storage.clone(), Duration::from_millis(1));
    assert_eq!(refresh.refresh_once().await.unwrap(), authors.len());
    for (index, author) in authors.iter().enumerate() {
        assert!(
            storage
                .get_record(author, "app.bsky.actor.profile", "self")
                .unwrap()
                .is_some_and(|profile| {
                    profile.record["displayName"] == format!("Background hydrated {index}")
                })
        );
    }
}

#[tokio::test]
async fn thread_page_uses_large_focus_post_stats_chain_lines_and_reply_bar() {
    let storage = Arc::new(MemoryStorage::default());
    let root_uri = "at://did:plc:root/app.bsky.feed.post/root";
    let focus_uri = "at://did:plc:author/app.bsky.feed.post/focus";
    for record in [
        IndexedRecord {
            did: "did:plc:root".into(),
            collection: "app.bsky.feed.post".into(),
            rkey: "root".into(),
            uri: root_uri.into(),
            cid: Some("root-cid".into()),
            time_us: 1,
            record: serde_json::json!({"text":"thread root"}),
        },
        IndexedRecord {
            did: "did:plc:author".into(),
            collection: "app.bsky.actor.profile".into(),
            rkey: "self".into(),
            uri: "at://did:plc:author/app.bsky.actor.profile/self".into(),
            cid: Some("author-profile".into()),
            time_us: 2,
            record: serde_json::json!({
                "displayName":"Thread Author",
                "avatar":{"$type":"blob","ref":{"$link":"avatar-cid"}}
            }),
        },
        IndexedRecord {
            did: "did:plc:author".into(),
            collection: "app.bsky.feed.post".into(),
            rkey: "focus".into(),
            uri: focus_uri.into(),
            cid: Some("focus-cid".into()),
            time_us: 3,
            record: serde_json::json!({
                "$type":"app.bsky.feed.post",
                "text":"focused post",
                "createdAt":"2026-10-28T12:34:00Z",
                "reply":{"root":{"uri":root_uri,"cid":"root-cid"},"parent":{"uri":root_uri,"cid":"root-cid"}}
            }),
        },
        IndexedRecord {
            did: "did:plc:author".into(),
            collection: "app.bsky.feed.post".into(),
            rkey: "reply-one".into(),
            uri: "at://did:plc:author/app.bsky.feed.post/reply-one".into(),
            cid: Some("reply-one-cid".into()),
            time_us: 6,
            record: serde_json::json!({
                "text":"first chain reply",
                "reply":{"root":{"uri":root_uri,"cid":"root-cid"},"parent":{"uri":focus_uri,"cid":"focus-cid"}}
            }),
        },
        IndexedRecord {
            did: "did:plc:other-author".into(),
            collection: "app.bsky.feed.post".into(),
            rkey: "reply-three".into(),
            uri: "at://did:plc:other-author/app.bsky.feed.post/reply-three".into(),
            cid: Some("reply-three-cid".into()),
            time_us: 4,
            record: serde_json::json!({
                "text":"different author reply",
                "reply":{"root":{"uri":root_uri,"cid":"root-cid"},"parent":{"uri":focus_uri,"cid":"focus-cid"}}
            }),
        },
        IndexedRecord {
            did: "did:plc:author".into(),
            collection: "app.bsky.feed.post".into(),
            rkey: "reply-two".into(),
            uri: "at://did:plc:author/app.bsky.feed.post/reply-two".into(),
            cid: Some("reply-two-cid".into()),
            time_us: 5,
            record: serde_json::json!({
                "text":"second chain reply",
                "reply":{"root":{"uri":root_uri,"cid":"root-cid"},"parent":{"uri":focus_uri,"cid":"focus-cid"}}
            }),
        },
        IndexedRecord {
            did: "did:plc:alice".into(),
            collection: "app.bsky.feed.like".into(),
            rkey: "like-focus".into(),
            uri: "at://did:plc:alice/app.bsky.feed.like/like-focus".into(),
            cid: Some("like-cid".into()),
            time_us: 6,
            record: serde_json::json!({"subject":{"uri":focus_uri,"cid":"focus-cid"}}),
        },
        IndexedRecord {
            did: "did:plc:reposter1".into(),
            collection: "app.bsky.feed.repost".into(),
            rkey: "repost1".into(),
            uri: "at://did:plc:reposter1/app.bsky.feed.repost/repost1".into(),
            cid: Some("repost1-cid".into()),
            time_us: 7,
            record: serde_json::json!({"subject":{"uri":focus_uri,"cid":"focus-cid"}}),
        },
        IndexedRecord {
            did: "did:plc:reposter2".into(),
            collection: "app.bsky.feed.repost".into(),
            rkey: "repost2".into(),
            uri: "at://did:plc:reposter2/app.bsky.feed.repost/repost2".into(),
            cid: Some("repost2-cid".into()),
            time_us: 8,
            record: serde_json::json!({"subject":{"uri":focus_uri,"cid":"focus-cid"}}),
        },
        IndexedRecord {
            did: "did:plc:quoter".into(),
            collection: "app.bsky.feed.post".into(),
            rkey: "quote".into(),
            uri: "at://did:plc:quoter/app.bsky.feed.post/quote".into(),
            cid: Some("quote-cid".into()),
            time_us: 9,
            record: serde_json::json!({
                "text":"quote",
                "embed":{"$type":"app.bsky.embed.record","record":{"uri":focus_uri,"cid":"focus-cid"}}
            }),
        },
    ] {
        storage.put_record(&record).unwrap();
    }
    storage
        .set_verified_handle("did:plc:author", "author.test")
        .unwrap();
    let flow = FakeOAuthFlow::default();
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(flow.clone(), storage);
    let cookie = finish_login(&app).await;
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/thread?uri=at%3A%2F%2Fdid%3Aplc%3Aauthor%2Fapp.bsky.feed.post%2Ffocus")
                .header("cookie", &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let html = String::from_utf8(
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert!(html.contains("class=\"thread-topbar\""), "{html}");
    assert!(html.contains(">Post</h1>"), "{html}");
    assert_eq!(html.matches("thread-post-focus").count(), 1, "{html}");
    assert!(html.contains("<strong>2</strong> reposts"), "{html}");
    assert!(html.contains("<strong>1</strong> quote"), "{html}");
    assert!(html.contains("<strong>1</strong> like"), "{html}");
    assert_eq!(html.matches("thread-continues").count(), 1, "{html}");
    assert!(html.contains("aria-label=\"Mobile reply bar\""), "{html}");
    assert!(html.contains("Write your reply"), "{html}");
    assert!(
        html.contains("aria-label=\"Mobile search\""),
        "thread mobile navigation should include its search tab"
    );
    assert!(
        html.contains("mobile-profile-avatar"),
        "thread mobile navigation should show the viewer avatar"
    );
    assert!(html.contains("aria-pressed=\"true\""), "{html}");
    assert!(!html.contains("0 quotes"), "{html}");

    let reply = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/reply")
                .header("origin", "https://appview.example")
                .header("sec-fetch-site", "same-origin")
                .header("cookie", cookie)
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(format!(
                    "parent_uri={}&text=reply+to+focus",
                    url::form_urlencoded::byte_serialize(focus_uri.as_bytes()).collect::<String>()
                )))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(reply.status(), StatusCode::SEE_OTHER);
    assert_eq!(
        flow.actions.lock().unwrap().last().unwrap(),
        &format!("reply:did:plc:alice:reply to focus:{root_uri}:root-cid:{focus_uri}:focus-cid")
    );
}

#[tokio::test]
async fn web_posts_show_hydrated_authors_and_times_on_timeline_profile_and_thread() {
    let storage = Arc::new(MemoryStorage::default());
    storage.register_user("did:plc:alice").unwrap();
    storage
        .set_verified_handle("did:plc:alice", "alice.test")
        .unwrap();
    storage
        .set_verified_handle("did:plc:friend", "friend.test")
        .unwrap();
    storage
        .set_follow("did:plc:alice", "did:plc:friend", true)
        .unwrap();
    for record in [
        IndexedRecord {
            did: "did:plc:alice".into(),
            collection: "app.bsky.actor.profile".into(),
            rkey: "self".into(),
            uri: "at://did:plc:alice/app.bsky.actor.profile/self".into(),
            cid: Some("alice-profile-cid".into()),
            time_us: 3,
            record: serde_json::json!({"displayName":"Alice"}),
        },
        IndexedRecord {
            did: "did:plc:friend".into(),
            collection: "app.bsky.actor.profile".into(),
            rkey: "self".into(),
            uri: "at://did:plc:friend/app.bsky.actor.profile/self".into(),
            cid: Some("friend-profile-cid".into()),
            time_us: 2,
            record: serde_json::json!({"displayName":"Friend & Co"}),
        },
        IndexedRecord {
            did: "did:plc:alice".into(),
            collection: "app.bsky.graph.follow".into(),
            rkey: "friend-follow".into(),
            uri: "at://did:plc:alice/app.bsky.graph.follow/friend-follow".into(),
            cid: Some("follow-cid".into()),
            time_us: 1,
            record: serde_json::json!({"subject":"did:plc:friend"}),
        },
        IndexedRecord {
            did: "did:plc:friend".into(),
            collection: "app.bsky.feed.post".into(),
            rkey: "friend-post".into(),
            uri: "at://did:plc:friend/app.bsky.feed.post/friend-post".into(),
            cid: Some("friend-post-cid".into()),
            time_us: 1,
            record: serde_json::json!({"text":"A post by a friend", "createdAt":"2026-10-27T09:15:00Z"}),
        },
    ] {
        storage.put_record(&record).unwrap();
    }
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(FakeOAuthFlow::default(), storage);
    let cookie = finish_login(&app).await;

    let timeline = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/app")
                .header("cookie", &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let timeline_html = String::from_utf8(
        timeline
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert!(timeline_html.contains("Friend &amp; Co"));
    assert!(timeline_html.contains("@friend.test"));
    assert!(timeline_html.contains("href=\"/profile?did=did%3Aplc%3Afriend\""));
    assert!(
        timeline_html
            .contains("<time datetime=\"2026-10-27T09:15:00Z\">2026-10-27T09:15:00Z</time>")
    );

    let profile = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/profile?did=did%3Aplc%3Afriend")
                .header("cookie", &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(profile.status(), StatusCode::OK);
    let profile_html = String::from_utf8(
        profile
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert!(profile_html.contains("A post by a friend"));
    assert!(profile_html.contains("@friend.test"));
    assert!(profile_html.contains("2026-10-27T09:15:00Z"));

    let thread = app
        .oneshot(
            Request::builder()
                .uri("/thread?uri=at%3A%2F%2Fdid%3Aplc%3Afriend%2Fapp.bsky.feed.post%2Ffriend-post")
                .header("cookie", &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let thread_html = String::from_utf8(
        thread
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert!(thread_html.contains("Friend &amp; Co"));
    assert!(thread_html.contains("@friend.test"));
    assert!(thread_html.contains("09:15 · 27 Oct 2026"));
}

#[tokio::test]
async fn timeline_fetches_unindexed_quotes_and_renders_safe_nested_media_cards() {
    let storage = Arc::new(MemoryStorage::default());
    let quoted_uri = "at://did:plc:quoted/app.bsky.feed.post/quote";
    let inner_uri = "at://did:plc:inner/app.bsky.feed.post/inner";
    let mut outer = fake_feed_post("did:plc:alice", "outer-quote", 10, "Outer post");
    outer.record["embed"] = serde_json::json!({
        "$type":"app.bsky.embed.recordWithMedia",
        "media":{"$type":"app.bsky.embed.images","images":[{
            "image":{"ref":{"$link":"image-cid"}},"alt":"a beach"
        }]},
        "record":{"$type":"app.bsky.embed.record","record":{"uri":quoted_uri,"cid":"quote-cid"}}
    });
    storage.put_record(&outer).unwrap();

    let mut quoted = fake_feed_post("did:plc:quoted", "quote", 9, "");
    quoted.record = serde_json::json!({
        "$type":"app.bsky.feed.post",
        "text":"Quoted <script>alert(1)</script> & text",
        "createdAt":"2026-10-28T00:00:00Z",
        "embed":{"$type":"app.bsky.embed.record","record":{"uri":inner_uri,"cid":"inner-cid"}}
    });
    let flow = FakeOAuthFlow {
        live_posts: Arc::new(Mutex::new(
            [(quoted_uri.to_owned(), quoted)].into_iter().collect(),
        )),
        author_profiles: Arc::new(Mutex::new(
            [(
                "did:plc:quoted".to_owned(),
                FetchedAuthorProfile {
                    profile: Some(IndexedRecord {
                        did: "did:plc:quoted".into(),
                        collection: "app.bsky.actor.profile".into(),
                        rkey: "self".into(),
                        uri: "at://did:plc:quoted/app.bsky.actor.profile/self".into(),
                        cid: Some("quoted-profile-cid".into()),
                        time_us: 1,
                        record: serde_json::json!({"displayName":"Quoted <Author>"}),
                    }),
                    verified_handle: Some("quoted.test".into()),
                },
            )]
            .into_iter()
            .collect(),
        )),
        ..FakeOAuthFlow::default()
    };
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(flow.clone(), storage);
    let cookie = finish_login(&app).await;
    let response = app
        .oneshot(
            Request::builder()
                .uri("/app")
                .header("cookie", cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let html = String::from_utf8(
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();

    assert_eq!(*flow.post_requests.lock().unwrap(), vec![quoted_uri]);
    assert!(html.contains("quoted-post-card"), "{html}");
    assert!(html.contains("Quoted &lt;Author&gt;"), "{html}");
    assert!(html.contains("@quoted.test"), "{html}");
    assert!(
        html.contains("Quoted &lt;script&gt;alert(1)&lt;/script&gt; &amp; text"),
        "{html}"
    );
    assert!(
        html.contains("href=\"/app/thread?uri=at%3A%2F%2Fdid%3Aplc%3Aquoted"),
        "{html}"
    );
    assert!(html.contains("Quoted post</a>"), "{html}");
    assert!(html.contains("alt=\"a beach\""), "{html}");
    assert!(html.contains("image-cid"), "{html}");
    assert!(!html.contains(inner_uri), "{html}");
}

#[tokio::test]
async fn quote_hydration_keeps_page_order_deduplicates_and_skips_other_record_fetches() {
    let storage = Arc::new(MemoryStorage::default());
    let feed_uri = "at://did:plc:creator/app.bsky.feed.generator/favorite";
    storage
        .put_record(&IndexedRecord {
            did: "did:plc:creator".into(),
            collection: "app.bsky.feed.generator".into(),
            rkey: "favorite".into(),
            uri: feed_uri.into(),
            cid: Some("feed-cid".into()),
            time_us: 1,
            record: serde_json::json!({"displayName":"A curated feed"}),
        })
        .unwrap();
    let expected_uris = (0..18)
        .map(|index| format!("at://did:plc:quoted{index}/app.bsky.feed.post/post"))
        .collect::<Vec<_>>();
    let mut post_refs = vec![expected_uris[0].clone(), expected_uris[1].clone()];
    post_refs.push(expected_uris[0].clone());
    post_refs.push(feed_uri.to_owned());
    post_refs.extend(expected_uris.iter().skip(2).cloned());
    for (index, quote_uri) in post_refs.iter().enumerate() {
        let mut post = fake_feed_post(
            "did:plc:alice",
            &format!("outer-{index}"),
            (1000 - index) as u64,
            "Outer post",
        );
        post.record["embed"] = serde_json::json!({
            "$type":"app.bsky.embed.record",
            "record":{"uri":quote_uri,"cid":"quoted-cid"}
        });
        storage.put_record(&post).unwrap();
    }
    let live_posts = expected_uris
        .iter()
        .enumerate()
        .map(|(index, uri)| {
            let record = fake_feed_post(&format!("did:plc:quoted{index}"), "post", 1, "Quote");
            (uri.clone(), record)
        })
        .collect::<std::collections::HashMap<_, _>>();
    let flow = FakeOAuthFlow {
        live_posts: Arc::new(Mutex::new(live_posts)),
        post_delays_ms: Arc::new(Mutex::new(
            expected_uris.iter().map(|uri| (uri.clone(), 15)).collect(),
        )),
        ..FakeOAuthFlow::default()
    };
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(flow.clone(), storage);
    let cookie = finish_login(&app).await;
    let response = app
        .oneshot(
            Request::builder()
                .uri("/app")
                .header("cookie", cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let html = String::from_utf8(
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();

    assert_eq!(*flow.post_requests.lock().unwrap(), expected_uris);
    let requested_profiles = flow.profile_requests.lock().unwrap().clone();
    assert_eq!(
        &requested_profiles[..4],
        [
            "did:plc:alice",
            "did:plc:quoted0",
            "did:plc:quoted1",
            "did:plc:quoted2"
        ]
    );
    assert!(html.contains("A curated feed"), "{html}");
    assert_eq!(html.matches("quoted-record-type").count(), 1, "{html}");
}

#[tokio::test]
async fn quote_and_author_hydration_share_the_page_deadline() {
    let storage = Arc::new(MemoryStorage::default());
    let quote_uri = "at://did:plc:slow-author/app.bsky.feed.post/slow";
    let mut outer = fake_feed_post("did:plc:alice", "shared-budget", 10, "Outer post");
    outer.record["embed"] = serde_json::json!({
        "$type":"app.bsky.embed.record",
        "record":{"uri":quote_uri,"cid":"slow-cid"}
    });
    storage.put_record(&outer).unwrap();
    let quoted = fake_feed_post(
        "did:plc:slow-author",
        "slow",
        9,
        "Hydrated within one shared budget",
    );
    let flow = FakeOAuthFlow {
        live_posts: Arc::new(Mutex::new(
            [(quote_uri.to_owned(), quoted)].into_iter().collect(),
        )),
        post_delays_ms: Arc::new(Mutex::new(
            [(quote_uri.to_owned(), 1_200)].into_iter().collect(),
        )),
        author_profiles: Arc::new(Mutex::new(
            [(
                "did:plc:slow-author".to_owned(),
                FetchedAuthorProfile {
                    profile: Some(IndexedRecord {
                        did: "did:plc:slow-author".into(),
                        collection: "app.bsky.actor.profile".into(),
                        rkey: "self".into(),
                        uri: "at://did:plc:slow-author/app.bsky.actor.profile/self".into(),
                        cid: Some("slow-profile-cid".into()),
                        time_us: 1,
                        record: serde_json::json!({"displayName":"Slow & Safe"}),
                    }),
                    verified_handle: Some("slow.test".into()),
                },
            )]
            .into_iter()
            .collect(),
        )),
        profile_delays_ms: Arc::new(Mutex::new(
            [("did:plc:slow-author".to_owned(), 1_200)]
                .into_iter()
                .collect(),
        )),
        ..FakeOAuthFlow::default()
    };
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(flow, storage.clone());
    let cookie = finish_login(&app).await;
    let started = Instant::now();
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/app")
                .header("cookie", &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let elapsed = started.elapsed();
    let html = String::from_utf8(
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();

    assert!(
        elapsed < Duration::from_millis(1_700),
        "page took {elapsed:?}"
    );
    assert!(html.contains("Hydrated within one shared budget"), "{html}");
    tokio::time::timeout(Duration::from_millis(600), async {
        loop {
            if storage
                .get_record("did:plc:slow-author", "app.bsky.actor.profile", "self")
                .unwrap()
                .is_some()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("parallel author hydration should finish within the shared page deadline");
    let hydrated_html = get_html(&app, "/app", &cookie).await;
    assert!(hydrated_html.contains("Slow &amp; Safe"), "{hydrated_html}");
}

#[tokio::test]
async fn quote_failures_distinguish_missing_unavailable_and_budget_placeholder() {
    let storage = Arc::new(MemoryStorage::default());
    let deleted_uri = "at://did:plc:deleted/app.bsky.feed.post/deleted";
    let unavailable_uri = "at://did:plc:blocked/app.bsky.feed.post/blocked";
    let delayed_uri = "at://did:plc:slow/app.bsky.feed.post/slow";
    let mut deleted = fake_feed_post("did:plc:alice", "deleted-quote", 30, "Deleted quote");
    deleted.record["embed"] = serde_json::json!({
        "$type":"app.bsky.embed.record","record":{"uri":deleted_uri,"cid":"gone"}
    });
    let mut unavailable = fake_feed_post("did:plc:alice", "blocked-quote", 20, "Blocked quote");
    unavailable.record["embed"] = serde_json::json!({
        "$type":"app.bsky.embed.record",
        "record":{"$type":"app.bsky.embed.record#viewBlocked","uri":unavailable_uri}
    });
    let mut delayed = fake_feed_post("did:plc:alice", "slow-quote", 10, "Slow quote");
    delayed.record["embed"] = serde_json::json!({
        "$type":"app.bsky.embed.record","record":{"uri":delayed_uri,"cid":"slow"}
    });
    for record in [deleted, unavailable, delayed] {
        storage.put_record(&record).unwrap();
    }
    let flow = FakeOAuthFlow {
        post_delays_ms: Arc::new(Mutex::new(
            [(delayed_uri.to_owned(), 1_700)].into_iter().collect(),
        )),
        ..FakeOAuthFlow::default()
    };
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(flow, storage);
    let cookie = finish_login(&app).await;
    let response = app
        .oneshot(
            Request::builder()
                .uri("/app")
                .header("cookie", cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let html = String::from_utf8(
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();

    assert!(html.contains("Post not found"), "{html}");
    assert!(html.contains("Post unavailable"), "{html}");
    assert!(
        html.contains("data-quote-fragment=\"/quote-fragment?uri="),
        "{html}"
    );
    assert!(html.contains("Loading quoted post"), "{html}");
    assert!(!html.contains(">Quoted post</a>"), "{html}");
    assert!(!html.contains(deleted_uri), "{html}");
    assert!(!html.contains(unavailable_uri), "{html}");
    assert!(!html.contains(delayed_uri), "{html}");
}

#[tokio::test]
async fn post_action_rows_are_ordered_hide_zero_counts_and_style_thread_numbers() {
    let storage = Arc::new(MemoryStorage::default());
    let post_uri = "at://did:plc:friend/app.bsky.feed.post/numbered";
    for record in [
        IndexedRecord {
            did: "did:plc:friend".into(),
            collection: "app.bsky.feed.post".into(),
            rkey: "numbered".into(),
            uri: post_uri.into(),
            cid: Some("numbered-cid".into()),
            time_us: 30,
            record: serde_json::json!({"text":"One useful point 1/2","createdAt":"2026-10-28T00:00:00Z"}),
        },
        IndexedRecord {
            did: "did:plc:alice".into(),
            collection: "app.bsky.feed.post".into(),
            rkey: "zero".into(),
            uri: "at://did:plc:alice/app.bsky.feed.post/zero".into(),
            cid: Some("zero-cid".into()),
            time_us: 20,
            record: serde_json::json!({"text":"A plain post"}),
        },
        IndexedRecord {
            did: "did:plc:alice".into(),
            collection: "app.bsky.feed.like".into(),
            rkey: "liked-numbered".into(),
            uri: "at://did:plc:alice/app.bsky.feed.like/liked-numbered".into(),
            cid: Some("like-cid".into()),
            time_us: 19,
            record: serde_json::json!({"subject":{"uri":post_uri,"cid":"numbered-cid"}}),
        },
        IndexedRecord {
            did: "did:plc:reposter".into(),
            collection: "app.bsky.feed.repost".into(),
            rkey: "numbered-repost".into(),
            uri: "at://did:plc:reposter/app.bsky.feed.repost/numbered-repost".into(),
            cid: Some("repost-cid".into()),
            time_us: 18,
            record: serde_json::json!({"subject":{"uri":post_uri,"cid":"numbered-cid"}}),
        },
        IndexedRecord {
            did: "did:plc:replier".into(),
            collection: "app.bsky.feed.post".into(),
            rkey: "numbered-reply".into(),
            uri: "at://did:plc:replier/app.bsky.feed.post/numbered-reply".into(),
            cid: Some("reply-cid".into()),
            time_us: 17,
            record: serde_json::json!({
                "text":"reply",
                "reply":{"root":{"uri":post_uri,"cid":"numbered-cid"},"parent":{"uri":post_uri,"cid":"numbered-cid"}}
            }),
        },
    ] {
        storage.put_record(&record).unwrap();
    }
    storage
        .set_follow("did:plc:alice", "did:plc:friend", true)
        .unwrap();
    storage
        .set_verified_handle("did:plc:friend", "friend.test")
        .unwrap();
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(FakeOAuthFlow::default(), storage);
    let cookie = finish_login(&app).await;
    let timeline = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/app")
                .header("cookie", &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let timeline_html = String::from_utf8(
        timeline
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    let post = timeline_html
        .split("id=\"post-did-plc-friend-app-bsky-feed-post-numbered\"")
        .nth(1)
        .unwrap();
    let actions = post.split("<div class=\"post-actions\"").nth(1).unwrap();
    let action_order = [
        actions.find("aria-label=\"Open replies\"").unwrap(),
        actions.find("class=\"repost-menu\"").unwrap(),
        actions.find("aria-label=\"Unlike post").unwrap(),
        actions.find("aria-label=\"Share or copy link\"").unwrap(),
        actions.find("aria-label=\"More post actions\"").unwrap(),
    ];
    assert!(action_order.windows(2).all(|pair| pair[0] < pair[1]));
    assert!(
        actions.contains("class=\"post-action is-liked\""),
        "{actions}"
    );
    assert!(
        actions.contains("class=\"action-count\">1</span>"),
        "{actions}"
    );
    assert!(post.contains("One useful point <span class=\"thread-number-pill\">1/2</span>"));
    let zero_post = timeline_html
        .split("id=\"post-did-plc-alice-app-bsky-feed-post-zero\"")
        .nth(1)
        .unwrap();
    let zero_actions = zero_post
        .split("<div class=\"post-actions\"")
        .nth(1)
        .unwrap();
    assert!(
        !zero_actions.contains("class=\"action-count\""),
        "{zero_actions}"
    );

    let thread = app
        .oneshot(
            Request::builder()
                .uri(
                    "/app/thread?uri=at%3A%2F%2Fdid%3Aplc%3Afriend%2Fapp.bsky.feed.post%2Fnumbered",
                )
                .header("cookie", cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let thread_html = String::from_utf8(
        thread
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert!(thread_html.contains("One useful point <span class=\"thread-number-pill\">1/2</span>"));
}

#[tokio::test]
async fn thread_page_fetches_parent_and_root_concurrently() {
    let storage = Arc::new(MemoryStorage::default());
    let root_uri = "at://did:plc:thread/app.bsky.feed.post/root";
    let parent_uri = "at://did:plc:thread/app.bsky.feed.post/parent";
    let middle_uri = "at://did:plc:thread/app.bsky.feed.post/middle";
    let mut middle = fake_feed_post("did:plc:thread", "middle", 2, "Middle");
    middle.record["reply"] = serde_json::json!({
        "root":{"uri":root_uri,"cid":"root-cid"},
        "parent":{"uri":root_uri,"cid":"root-cid"}
    });
    let mut parent = fake_feed_post("did:plc:thread", "parent", 3, "Parent");
    parent.record["reply"] = serde_json::json!({
        "root":{"uri":root_uri,"cid":"root-cid"},
        "parent":{"uri":middle_uri,"cid":"middle-cid"}
    });
    let mut target = fake_feed_post("did:plc:thread", "target", 4, "Target");
    target.record["reply"] = serde_json::json!({
        "root":{"uri":root_uri,"cid":"root-cid"},
        "parent":{"uri":parent_uri,"cid":"parent-cid"}
    });
    storage.put_record(&target).unwrap();
    for did in ["did:plc:thread", "did:plc:alice"] {
        storage
            .set_verified_handle(did, &format!("{}.test", did.replace(':', "-")))
            .unwrap();
    }
    let root = fake_feed_post("did:plc:thread", "root", 1, "Root");
    let flow = FakeOAuthFlow {
        live_posts: Arc::new(Mutex::new(
            [
                (root_uri.to_owned(), root),
                (parent_uri.to_owned(), parent),
                (middle_uri.to_owned(), middle),
            ]
            .into_iter()
            .collect(),
        )),
        post_delays_ms: Arc::new(Mutex::new(
            [root_uri, parent_uri, middle_uri]
                .into_iter()
                .map(|uri| (uri.to_owned(), 200))
                .collect(),
        )),
        ..FakeOAuthFlow::default()
    };
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(flow, storage);
    let cookie = finish_login(&app).await;
    let started = Instant::now();
    let response = app
        .oneshot(
            Request::builder()
                .uri("/app/thread?uri=at%3A%2F%2Fdid%3Aplc%3Athread%2Fapp.bsky.feed.post%2Ftarget")
                .header("cookie", cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let elapsed = started.elapsed();
    assert!(
        elapsed < Duration::from_millis(600),
        "thread rendered in {elapsed:?}"
    );
    let html = String::from_utf8(
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert!(html.contains("Root"), "{html}");
    assert!(html.contains("Middle"), "{html}");
    assert!(html.contains("Parent"), "{html}");
}

#[tokio::test]
async fn web_embeds_render_images_cards_quotes_and_escape_user_content() {
    let storage = Arc::new(MemoryStorage::default());
    storage.register_user("did:plc:alice").unwrap();
    for record in [
        IndexedRecord {
            did: "did:plc:alice".into(),
            collection: "app.bsky.actor.profile".into(),
            rkey: "self".into(),
            uri: "at://did:plc:alice/app.bsky.actor.profile/self".into(),
            cid: Some("alice-profile-cid".into()),
            time_us: 10,
            record: serde_json::json!({"displayName":"Alice"}),
        },
        IndexedRecord {
            did: "did:plc:alice".into(),
            collection: "app.bsky.feed.post".into(),
            rkey: "image-only".into(),
            uri: "at://did:plc:alice/app.bsky.feed.post/image-only".into(),
            cid: Some("image-only-cid".into()),
            time_us: 9,
            record: serde_json::json!({
                "$type":"app.bsky.feed.post",
                "createdAt":"2026-10-27T09:20:00Z",
                "embed":{
                    "$type":"app.bsky.embed.images",
                    "images":[{
                        "alt":"portrait <script>alert(1)</script>",
                        "image":{"$type":"blob","ref":{"$link":"bafk-image-one"}}
                    }]
                }
            }),
        },
        IndexedRecord {
            did: "did:plc:alice".into(),
            collection: "app.bsky.feed.post".into(),
            rkey: "record-with-media".into(),
            uri: "at://did:plc:alice/app.bsky.feed.post/record-with-media".into(),
            cid: Some("record-with-media-cid".into()),
            time_us: 8,
            record: serde_json::json!({
                "$type":"app.bsky.feed.post",
                "createdAt":"2026-10-27T09:19:00Z",
                "text":"Photo with a quote",
                "embed":{
                    "$type":"app.bsky.embed.recordWithMedia",
                    "media":{
                        "$type":"app.bsky.embed.images",
                        "images":[{
                            "alt":"media image",
                            "image":{"$type":"blob","ref":{"$link":"bafk-image-two"}}
                        }]
                    },
                    "record":{"$type":"app.bsky.embed.record","record":{"uri":"at://did:plc:quoted/app.bsky.feed.post/quoted","cid":"quoted-cid"}}
                }
            }),
        },
        IndexedRecord {
            did: "did:plc:alice".into(),
            collection: "app.bsky.feed.post".into(),
            rkey: "external".into(),
            uri: "at://did:plc:alice/app.bsky.feed.post/external".into(),
            cid: Some("external-cid".into()),
            time_us: 7,
            record: serde_json::json!({
                "$type":"app.bsky.feed.post",
                "createdAt":"2026-10-27T09:18:00Z",
                "embed":{
                    "$type":"app.bsky.embed.external",
                    "external":{
                        "uri":"https://example.test/card?x=1&y=2",
                        "title":"Card <Title>",
                        "description":"Card description",
                        "thumb":{"$type":"blob","ref":{"$link":"bafk-external-thumb"}}
                    }
                }
            }),
        },
        IndexedRecord {
            did: "did:plc:alice".into(),
            collection: "app.bsky.feed.post".into(),
            rkey: "quoted".into(),
            uri: "at://did:plc:alice/app.bsky.feed.post/quoted".into(),
            cid: Some("quote-post-cid".into()),
            time_us: 6,
            record: serde_json::json!({
                "$type":"app.bsky.feed.post",
                "createdAt":"2026-10-27T09:17:00Z",
                "embed":{
                    "$type":"app.bsky.embed.record",
                    "record":{"uri":"at://did:plc:quoted/app.bsky.feed.post/quoted","cid":"quoted-cid"}
                }
            }),
        },
        IndexedRecord {
            did: "did:plc:quoted".into(),
            collection: "app.bsky.feed.post".into(),
            rkey: "quoted".into(),
            uri: "at://did:plc:quoted/app.bsky.feed.post/quoted".into(),
            cid: Some("quoted-cid".into()),
            time_us: 5,
            record: serde_json::json!({
                "$type":"app.bsky.feed.post",
                "createdAt":"2026-10-27T09:16:00Z",
                "text":"Quoted <strong>text</strong>"
            }),
        },
    ] {
        storage.put_record(&record).unwrap();
    }
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(FakeOAuthFlow::default(), storage);
    let cookie = finish_login(&app).await;
    let response = app
        .oneshot(
            Request::builder()
                .uri("/app")
                .header("cookie", cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let html = String::from_utf8(
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();

    assert!(
        html.contains("https://cdn.bsky.app/img/feed_fullsize/plain/did:plc:alice/bafk-image-one")
    );
    assert!(
        html.contains("https://cdn.bsky.app/img/feed_fullsize/plain/did:plc:alice/bafk-image-two")
    );
    assert!(html.contains("alt=\"portrait &lt;script&gt;alert(1)&lt;/script&gt;\""));
    assert!(html.contains("Card &lt;Title&gt;"));
    assert!(html.contains("https://example.test/card?x=1&amp;y=2"));
    assert!(html.contains(
        "https://cdn.bsky.app/img/feed_thumbnail/plain/did:plc:alice/bafk-external-thumb"
    ));
    assert!(html.contains("rel=\"nofollow noopener noreferrer ugc\""));
    assert!(html.contains("Quoted &lt;strong&gt;text&lt;/strong&gt;"));
    assert!(!html.contains("<script>alert(1)</script>"));
}

#[tokio::test]
async fn web_views_truncate_oversized_remote_post_text() {
    let huge_text = "<script>hostile & text</script>".repeat(1200);
    let storage = Arc::new(MemoryStorage::default());
    storage.register_user("did:plc:alice").unwrap();
    storage
        .put_record(&IndexedRecord {
            did: "did:plc:alice".into(),
            collection: "app.bsky.feed.post".into(),
            rkey: "huge".into(),
            uri: "at://did:plc:alice/app.bsky.feed.post/huge".into(),
            cid: None,
            time_us: 1,
            record: serde_json::json!({"text":huge_text}),
        })
        .unwrap();
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(FakeOAuthFlow::default(), storage);
    let cookie = finish_login(&app).await;
    let response = app
        .oneshot(
            Request::builder()
                .uri("/app")
                .header("cookie", cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let html = String::from_utf8(
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();

    assert!(!html.contains(&huge_text));
    assert!(
        html.len() < 50_000,
        "oversized remote text inflated the page"
    );
}

#[tokio::test]
async fn callback_keeps_partial_write_grants_read_only_and_blocks_actions() {
    let flow = FakeOAuthFlow::with_granted_scope(Some("atproto"));
    let actions = flow.actions.clone();
    let storage = Arc::new(MemoryStorage::default());
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(flow, storage);
    let login_cookie = prelogin_cookie(&app).await;
    let callback = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/oauth/callback?code=valid-code&state=valid-state&scope=atproto")
                .header("cookie", login_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(callback.status(), StatusCode::SEE_OTHER);
    let cookie = callback.headers()["set-cookie"]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();

    let home = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/app")
                .header("cookie", &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let html = String::from_utf8(
        home.into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert!(html.contains("Read-only"));
    assert!(html.contains("write permissions were not granted"));
    assert!(!html.contains("action=\"/post\""));
    assert!(!html.contains("action=\"/like\""));
    assert!(!html.contains("action=\"/repost\""));

    let write = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/post")
                .header("origin", "https://appview.example")
                .header("sec-fetch-site", "same-origin")
                .header("cookie", &cookie)
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from("text=must+not+write"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(write.status(), StatusCode::FORBIDDEN);
    assert!(actions.lock().unwrap().is_empty());
}

#[tokio::test]
async fn empty_timeline_reports_backfill_progress_for_registered_account() {
    let storage = Arc::new(MemoryStorage::default());
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(FakeOAuthFlow::default(), storage.clone());
    let login_cookie = prelogin_cookie(&app).await;
    let callback = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/oauth/callback?code=valid-code&state=valid-state&scope=atproto")
                .header("cookie", login_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let cookie = callback.headers()["set-cookie"]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    let home = app
        .oneshot(
            Request::builder()
                .uri("/app")
                .header("cookie", cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let html = String::from_utf8(
        home.into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert!(html.contains("Indexing 1 account, 0 done"));
}

#[tokio::test]
async fn empty_timeline_reports_failed_backfill_accounts() {
    let storage = Arc::new(MemoryStorage::default());
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(FakeOAuthFlow::default(), storage.clone());
    let login_cookie = prelogin_cookie(&app).await;
    let callback = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/oauth/callback?code=valid-code&state=valid-state&scope=atproto")
                .header("cookie", login_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let cookie = callback.headers()["set-cookie"]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    storage.record_backfill_failure("did:plc:alice").unwrap();

    let home = app
        .oneshot(
            Request::builder()
                .uri("/app")
                .header("cookie", cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let html = String::from_utf8(
        home.into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert!(html.contains("Indexing 1 account, 0 done, 1 failed"));
}

#[tokio::test]
async fn callback_rejects_sessions_without_the_required_atproto_scope() {
    let storage = Arc::new(MemoryStorage::default());
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(
            FakeOAuthFlow::with_granted_scope(Some("profile")),
            storage.clone(),
        );
    let login_cookie = prelogin_cookie(&app).await;
    let response = app
        .oneshot(
            Request::builder()
                .uri("/oauth/callback?code=valid-code&state=valid-state&scope=profile")
                .header("cookie", login_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(!storage.is_registered("did:plc:alice").unwrap());
}

#[tokio::test]
async fn profile_page_shows_relationship_and_actions_for_other_accounts_only() {
    let storage = Arc::new(MemoryStorage::default());
    storage
        .put_record(&IndexedRecord {
            did: "did:plc:alice".into(),
            collection: "app.bsky.graph.follow".into(),
            rkey: "friend-follow".into(),
            uri: "at://did:plc:alice/app.bsky.graph.follow/friend-follow".into(),
            cid: Some("follow-cid".into()),
            time_us: 2,
            record: serde_json::json!({"subject":"did:plc:friend"}),
        })
        .unwrap();
    storage
        .set_follow("did:plc:alice", "did:plc:friend", true)
        .unwrap();
    storage
        .set_follow("did:plc:friend", "did:plc:alice", true)
        .unwrap();
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(FakeOAuthFlow::default(), storage);
    let cookie = finish_login(&app).await;

    let other = get_html(&app, "/profile?did=did%3Aplc%3Afriend", &cookie).await;
    assert!(other.contains("class=\"profile-actions\""), "{other}");
    assert!(other.contains(">Following</button>"), "{other}");
    assert!(
        other.contains("follows-you-pill\">Follows you</span>"),
        "{other}"
    );

    let own = get_html(&app, "/profile?did=did%3Aplc%3Aalice", &cookie).await;
    assert!(!own.contains("class=\"profile-actions\""), "{own}");
    assert!(!own.contains("name=\"subject\""), "{own}");
}

#[tokio::test]
async fn profile_header_renders_banner_avatar_counts_follows_you_and_linked_description() {
    let storage = Arc::new(MemoryStorage::default());
    let friend = "did:plc:friend";
    storage
        .put_record(&IndexedRecord {
            did: friend.into(),
            collection: "app.bsky.actor.profile".into(),
            rkey: "self".into(),
            uri: format!("at://{friend}/app.bsky.actor.profile/self"),
            cid: Some("friend-profile-cid".into()),
            time_us: 1,
            record: serde_json::json!({
                "displayName":"Riley Morgan",
                "description":"More at https://example.test/about",
                "avatar":{"$type":"blob","ref":{"$link":"avatar-cid"}},
                "banner":{"$type":"blob","ref":{"$link":"banner-cid"}}
            }),
        })
        .unwrap();
    storage.set_follow("did:plc:alice", friend, true).unwrap();
    storage.set_follow(friend, "did:plc:alice", true).unwrap();
    storage
        .set_verified_handle(friend, "riley.example")
        .unwrap();
    storage
        .put_record(&fake_feed_post(friend, "profile-post", 5, "hello"))
        .unwrap();
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(FakeOAuthFlow::default(), storage);
    let cookie = finish_login(&app).await;

    let html = get_html(&app, "/profile?did=did%3Aplc%3Afriend", &cookie).await;
    assert!(html.contains("class=\"profile-banner\""), "{html}");
    assert!(html.contains("banner-cid"), "{html}");
    assert!(html.contains("class=\"profile-avatar\""), "{html}");
    assert!(html.contains("avatar-cid"), "{html}");
    assert!(html.contains("<h1>Riley Morgan</h1>"), "{html}");
    assert!(html.contains("@riley.example"), "{html}");
    assert!(
        html.contains("class=\"follows-you-pill\">Follows you</span>"),
        "{html}"
    );
    assert!(html.contains("Followers"), "{html}");
    assert!(html.contains("Following"), "{html}");
    assert!(html.contains("Posts"), "{html}");
    assert!(html.contains("class=\"profile-description\""), "{html}");
    assert!(
        html.contains("href=\"https://example.test/about\" rel=\"nofollow noopener noreferrer\""),
        "{html}"
    );
}

#[tokio::test]
async fn profile_actions_use_one_follow_button_overflow_menu_and_membership_selector() {
    let storage = Arc::new(MemoryStorage::default());
    storage
        .set_follow("did:plc:alice", "did:plc:friend", true)
        .unwrap();
    let flow = FakeOAuthFlow::default();
    *flow.profile_lists.lock().unwrap() = vec![
        ProfileListChoice {
            uri: "at://did:plc:alice/app.bsky.graph.list/friends".into(),
            item_uri: Some("at://did:plc:alice/app.bsky.graph.listitem/friend-item".into()),
            name: "Friends".into(),
            purpose: "app.bsky.graph.defs#curatelist".into(),
        },
        ProfileListChoice {
            uri: "at://did:plc:alice/app.bsky.graph.list/reading".into(),
            item_uri: None,
            name: "Reading circle".into(),
            purpose: "app.bsky.graph.defs#curatelist".into(),
        },
    ];
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(flow, storage);
    let cookie = finish_login(&app).await;

    let html = get_html(&app, "/profile?did=did%3Aplc%3Afriend", &cookie).await;
    let actions = html.split("class=\"profile-actions\"").nth(1).unwrap_or("");
    assert!(
        actions.contains("class=\"profile-primary-action following\""),
        "{html}"
    );
    assert!(actions.contains(">Following</button>"), "{html}");
    assert!(actions.contains("class=\"profile-overflow\""), "{html}");
    assert!(
        actions.contains("aria-label=\"More profile actions\""),
        "{html}"
    );
    assert!(actions.contains("Add to lists"), "{html}");
    assert!(actions.contains("Mute account"), "{html}");
    assert!(actions.contains("Block account"), "{html}");
    assert!(!actions.contains("Confirm block"), "{html}");
    assert!(!actions.contains(">Unfollow</button>"), "{html}");
    assert!(actions.contains("class=\"profile-list-row\""), "{html}");
    assert!(actions.contains("Friends ✓"), "{html}");
    assert!(actions.contains("Reading circle"), "{html}");
    assert_eq!(
        actions.matches("class=\"profile-list-action\"").count(),
        1,
        "{html}"
    );
    assert!(actions.contains(">Remove</button>"), "{html}");
}

#[tokio::test]
async fn block_confirmation_is_a_named_review_page_with_cancel_and_block_actions() {
    let storage = Arc::new(MemoryStorage::default());
    storage
        .set_verified_handle("did:plc:friend", "friend")
        .unwrap();
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(FakeOAuthFlow::default(), storage);
    let cookie = finish_login(&app).await;

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/profile/block-confirm?did=did%3Aplc%3Afriend")
                .header("cookie", &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let html = String::from_utf8(
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert!(html.contains("Block @friend?"), "{html}");
    assert!(
        html.contains("They won't be able to see your posts"),
        "{html}"
    );
    assert!(html.contains("name=\"action\" value=\"block\""), "{html}");
    assert!(html.contains("name=\"confirm\" value=\"yes\""), "{html}");
    assert!(html.contains(">Cancel</a>"), "{html}");
    assert!(html.contains(">Block</button>"), "{html}");
}

#[tokio::test]
async fn profile_list_toggle_uses_current_membership_to_choose_add_or_remove() {
    let storage = Arc::new(MemoryStorage::default());
    let list_uri = "at://did:plc:alice/app.bsky.graph.list/friends";
    let item_uri = "at://did:plc:alice/app.bsky.graph.listitem/friend-item";
    let flow = FakeOAuthFlow::default();
    *flow.profile_lists.lock().unwrap() = vec![ProfileListChoice {
        uri: list_uri.to_owned(),
        item_uri: Some(item_uri.to_owned()),
        name: "Friends".into(),
        purpose: "app.bsky.graph.defs#curatelist".into(),
    }];
    let actions = flow.actions.clone();
    let profile_lists = flow.profile_lists.clone();
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(flow, storage.clone());
    let cookie = finish_login(&app).await;

    for expected_action in [
        "delete:app.bsky.graph.listitem",
        "create:app.bsky.graph.listitem",
    ] {
        let body = url::form_urlencoded::Serializer::new(String::new())
            .append_pair("subject", "did:plc:friend")
            .append_pair("action", "list_toggle")
            .append_pair("list_uri", list_uri)
            .finish();
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/profile/action")
                    .header("origin", "https://appview.example")
                    .header("content-type", "application/x-www-form-urlencoded")
                    .header("cookie", &cookie)
                    .body(Body::from(body))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::SEE_OTHER);
        assert!(
            actions
                .lock()
                .unwrap()
                .last()
                .is_some_and(|action| action.starts_with(expected_action)),
            "{:?}",
            actions.lock().unwrap()
        );
        if expected_action.starts_with("delete") {
            assert_eq!(profile_lists.lock().unwrap()[0].item_uri.as_deref(), None);
        } else {
            assert!(profile_lists.lock().unwrap()[0].item_uri.is_some());
        }
    }
}

#[tokio::test]
async fn old_scope_profile_shows_per_action_reauthorization_messages() {
    let storage = Arc::new(MemoryStorage::default());
    let old_scope = "atproto repo:app.bsky.feed.post?action=create repo:app.bsky.feed.like?action=create&action=delete repo:app.bsky.feed.repost?action=create&action=delete blob:image/* rpc:app.bsky.actor.getPreferences?aud=* rpc:app.bsky.feed.getFeedSkeleton?aud=* blob:video/* rpc:app.bsky.video.getUploadLimits?aud=* rpc:com.atproto.repo.uploadBlob?aud=*";
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(FakeOAuthFlow::with_granted_scope(Some(old_scope)), storage);
    let cookie = finish_login(&app).await;
    let html = get_html(&app, "/profile?did=did%3Aplc%3Afriend", &cookie).await;

    assert!(html.contains("Sign in again to follow"), "{html}");
    assert!(html.contains("Sign in again to block"), "{html}");
    assert!(html.contains("Sign in again to mute"), "{html}");
    assert!(html.contains("Sign in again to add to lists"), "{html}");
}

#[tokio::test]
async fn profile_labels_are_fetched_once_and_render_as_chips_with_labeler_details() {
    let storage = Arc::new(MemoryStorage::default());
    storage
        .put_record(&fake_feed_post(
            "did:plc:friend",
            "post-one",
            5,
            "labelled account post",
        ))
        .unwrap();
    let flow = FakeOAuthFlow::default();
    flow.account_labels.lock().unwrap().push(AccountLabel {
        subject_did: "did:plc:friend".to_owned(),
        labeler_did: "did:plc:labeler".to_owned(),
        labeler_name: "Careful Labeler".to_owned(),
        value: "spider".to_owned(),
        display_name: "Spider Warning".to_owned(),
        severity: "alert".to_owned(),
        blurs: "media".to_owned(),
    });
    let label_requests = flow.label_requests.clone();
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(flow, storage);
    let cookie = finish_login(&app).await;

    let html = get_html(&app, "/profile?did=did%3Aplc%3Afriend", &cookie).await;
    assert!(html.contains("profile-actions"), "{html}");
    assert!(html.contains("Spider Warning"), "{html}");
    assert!(html.contains("Careful Labeler"), "{html}");
    assert!(html.contains("severity: alert"), "{html}");
    assert!(html.contains("blurs: media"), "{html}");
    assert!(html.contains("account-label-chip severity-alert"), "{html}");
    assert!(html.matches("account-label-chip").count() >= 2, "{html}");
    let requests = label_requests.lock().unwrap();
    assert_eq!(requests.len(), 1, "label lookup should be batched per page");
    assert_eq!(
        requests[0]
            .iter()
            .filter(|did| did.as_str() == "did:plc:friend")
            .count(),
        1,
        "duplicate profile/post authors should only be sent once"
    );
}

#[test]
fn mobile_bottom_bar_cuts_safe_area_padding_and_shrinks_the_icon_row() {
    let css = include_str!("../static/app.css");
    assert!(
        css.contains(
            "padding-bottom: calc(0.1rem + max(4px, calc(env(safe-area-inset-bottom) * 0.25)));"
        ),
        "bottom nav should reserve only a quarter of the iOS inset"
    );
    assert!(
        css.contains("min-height: 2rem;\n    min-width: 44px;"),
        "icon row should retain its tap width while using less vertical slack"
    );
    assert!(css.contains("env(safe-area-inset-bottom) * 0.25"));
}

#[tokio::test]
async fn own_timeline_hides_posts_by_muted_and_blocked_accounts() {
    let storage = Arc::new(MemoryStorage::default());
    for (did, rkey, text) in [
        ("did:plc:muted", "muted-post", "muted content"),
        ("did:plc:blocked", "blocked-post", "blocked content"),
        ("did:plc:visible", "visible-post", "visible content"),
    ] {
        storage
            .put_record(&fake_feed_post(did, rkey, 10, text))
            .unwrap();
        storage.set_follow("did:plc:alice", did, true).unwrap();
    }
    storage
        .put_record(&IndexedRecord {
            did: "did:plc:alice".to_owned(),
            collection: "app.bsky.graph.block".to_owned(),
            rkey: "blocked".to_owned(),
            uri: "at://did:plc:alice/app.bsky.graph.block/blocked".to_owned(),
            cid: Some("block-cid".to_owned()),
            time_us: 20,
            record: serde_json::json!({"subject":"did:plc:blocked"}),
        })
        .unwrap();
    let flow = FakeOAuthFlow::default();
    flow.muted_actors
        .lock()
        .unwrap()
        .insert("did:plc:muted".to_owned());
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(flow, storage);
    let cookie = finish_login(&app).await;

    let html = get_html(&app, "/app", &cookie).await;
    assert!(html.contains("visible content"), "{html}");
    assert!(!html.contains("muted content"), "{html}");
    assert!(!html.contains("blocked content"), "{html}");
}

#[tokio::test]
async fn profile_block_requires_explicit_confirmation() {
    let storage = Arc::new(MemoryStorage::default());
    let flow = FakeOAuthFlow::default();
    let actions = flow.actions.clone();
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(flow, storage.clone());
    let cookie = finish_login(&app).await;
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/profile/action")
                .header("origin", "https://appview.example")
                .header("content-type", "application/x-www-form-urlencoded")
                .header("cookie", cookie)
                .body(Body::from("subject=did%3Aplc%3Afriend&action=block"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(actions.lock().unwrap().is_empty());
    assert!(
        storage
            .records_for_actor_collection("did:plc:alice", "app.bsky.graph.block", 10)
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn profile_follow_form_creates_the_follow_record_in_the_viewers_repo() {
    let storage = Arc::new(MemoryStorage::default());
    let flow = FakeOAuthFlow::default();
    let actions = flow.actions.clone();
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(flow, storage.clone());
    let cookie = finish_login(&app).await;
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/profile/action")
                .header("origin", "https://appview.example")
                .header("content-type", "application/x-www-form-urlencoded")
                .header("cookie", &cookie)
                .body(Body::from("subject=did%3Aplc%3Afriend&action=follow"))
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    assert!(
        storage
            .is_following("did:plc:alice", "did:plc:friend")
            .unwrap()
    );
    let created = storage
        .records_for_actor_collection("did:plc:alice", "app.bsky.graph.follow", 10)
        .unwrap();
    assert_eq!(created.len(), 1);
    assert_eq!(created[0].record["$type"], "app.bsky.graph.follow");
    assert_eq!(created[0].record["subject"], "did:plc:friend");
    assert!(
        actions
            .lock()
            .unwrap()
            .iter()
            .any(|action| action.starts_with("create:app.bsky.graph.follow:"))
    );
}
