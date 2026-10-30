use appview_rs::{
    api::api_router_with_profile_counts,
    identity::{
        DidDocument, IdentityBackend, IdentityResolver, ResolutionError, VerificationMethod,
    },
    ingest::{IndexedRecord, MemoryStorage, Storage},
    jwt::ServiceJwtVerifier,
    net::SafeHttpClient,
    oauth::{OAuthAccount, OAuthCallback, OAuthFlow, OAuthPublic, WebShutdown},
    profile_counts::{PUBLIC_APPVIEW_BASE, ProfileCounts, ProfileCountsClient},
};
use atrium_crypto::keypair::{Did as _, P256Keypair};
use axum::{
    Router,
    body::Body,
    extract::{Query, RawQuery, State},
    http::{Request, Response, StatusCode},
    response::IntoResponse,
    routing::get,
};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::net::TcpListener;
use tower::ServiceExt;

const ALICE: &str = "did:plc:alice";
const FRIEND: &str = "did:plc:friend";
const APPVIEW: &str = "did:web:appview.test";

#[derive(Clone)]
enum FakeReply {
    Counts,
    ServerError,
    Garbage,
    Delay(Duration),
}

#[derive(Clone)]
struct FakeAppview {
    calls: Arc<AtomicUsize>,
    reply: Arc<Mutex<FakeReply>>,
}

async fn fake_get_profile(
    State(fake): State<FakeAppview>,
    Query(query): Query<HashMap<String, String>>,
) -> Response<Body> {
    fake.respond(query.get("actor").cloned().into_iter().collect())
        .await
}

async fn fake_get_profiles(
    State(fake): State<FakeAppview>,
    RawQuery(query): RawQuery,
) -> Response<Body> {
    let actors = url::form_urlencoded::parse(query.as_deref().unwrap_or_default().as_bytes())
        .filter(|(key, _)| key == "actors")
        .map(|(_, value)| value.into_owned())
        .collect();
    fake.respond(actors).await
}

impl FakeAppview {
    async fn respond(&self, actors: Vec<String>) -> Response<Body> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let reply = self.reply.lock().unwrap().clone();
        match reply {
            FakeReply::Counts => {
                let profiles = actors
                    .into_iter()
                    .map(|did| {
                        json!({
                            "did": did,
                            "followersCount": 900,
                            "followsCount": 800,
                            "postsCount": 700
                        })
                    })
                    .collect::<Vec<_>>();
                let body = if profiles.len() == 1 {
                    profiles.into_iter().next().unwrap()
                } else {
                    json!({"profiles": profiles})
                };
                axum::Json(body).into_response()
            }
            FakeReply::ServerError => {
                (StatusCode::INTERNAL_SERVER_ERROR, "unavailable").into_response()
            }
            FakeReply::Garbage => (StatusCode::OK, "not json").into_response(),
            FakeReply::Delay(duration) => {
                tokio::time::sleep(duration).await;
                axum::Json(json!({"did": actors.first(), "followersCount": 9})).into_response()
            }
        }
    }
}

async fn start_fake_appview() -> (String, FakeAppview, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let fake = FakeAppview {
        calls: Arc::new(AtomicUsize::new(0)),
        reply: Arc::new(Mutex::new(FakeReply::Counts)),
    };
    let app = Router::new()
        .route("/xrpc/app.bsky.actor.getProfile", get(fake_get_profile))
        .route("/xrpc/app.bsky.actor.getProfiles", get(fake_get_profiles))
        .with_state(fake.clone());
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{address}/xrpc/"), fake, task)
}

fn counts_client(base: &str, ttl: Duration, timeout: Duration) -> ProfileCountsClient {
    ProfileCountsClient::new(
        SafeHttpClient::new().with_http_test_hosts(["127.0.0.1".to_owned()]),
        base,
        ttl,
    )
    .with_timeout(timeout)
}

#[tokio::test]
async fn profile_count_cache_reuses_remote_counts_within_configured_ttl() {
    let (base, fake, server) = start_fake_appview().await;
    let client = counts_client(&base, Duration::from_secs(600), Duration::from_secs(1));
    let first = client.get_counts(ALICE).await.unwrap();
    let second = client.get_counts(ALICE).await.unwrap();
    assert_eq!(
        first,
        ProfileCounts {
            followers: 900,
            following: 800,
            posts: 700
        }
    );
    assert_eq!(second, first);
    assert_eq!(fake.calls.load(Ordering::SeqCst), 1);
    server.abort();
}

#[tokio::test]
async fn xrpc_profile_responses_use_remote_counts_and_batch_get_profiles() {
    let (base, fake, server) = start_fake_appview().await;
    let storage = storage_with_local_counts();
    let router = api_with_counts(
        storage,
        counts_client(&base, Duration::from_secs(600), Duration::from_secs(1)),
    );
    let friends = request_json(
        &router,
        "/xrpc/app.bsky.actor.getProfiles?actors=did%3Aplc%3Aalice&actors=did%3Aplc%3Afriend",
    )
    .await;
    assert_eq!(friends.0, StatusCode::OK);
    assert_eq!(friends.1["profiles"].as_array().unwrap().len(), 2);
    assert!(
        friends.1["profiles"]
            .as_array()
            .unwrap()
            .iter()
            .all(|p| p["followersCount"] == 900),
        "{friends:?}"
    );
    assert_eq!(
        fake.calls.load(Ordering::SeqCst),
        1,
        "getProfiles should fetch both actors in one appview request"
    );
    let alice = request_json(
        &router,
        "/xrpc/app.bsky.actor.getProfile?actor=did%3Aplc%3Aalice",
    )
    .await;
    assert_eq!(alice.0, StatusCode::OK);
    assert_eq!(alice.1["followersCount"], 900);
    assert_eq!(alice.1["followsCount"], 800);
    assert_eq!(alice.1["postsCount"], 700);
    assert_eq!(
        fake.calls.load(Ordering::SeqCst),
        1,
        "getProfile should use the batch-populated cache"
    );
    server.abort();
}

#[tokio::test]
async fn xrpc_profile_falls_back_to_local_counts_for_errors_garbage_and_timeout() {
    let (base, fake, server) = start_fake_appview().await;
    let storage = storage_with_local_counts();
    let router = api_with_counts(
        storage,
        counts_client(&base, Duration::from_secs(600), Duration::from_millis(40)),
    );
    *fake.reply.lock().unwrap() = FakeReply::ServerError;
    let (status, profile) = request_json(
        &router,
        "/xrpc/app.bsky.actor.getProfile?actor=did%3Aplc%3Aalice",
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(profile["followersCount"], 1);
    assert_eq!(profile["followsCount"], 1);
    assert_eq!(profile["postsCount"], 1);

    *fake.reply.lock().unwrap() = FakeReply::Delay(Duration::from_millis(250));
    fake.calls.store(0, Ordering::SeqCst);
    let outgoing = SafeHttpClient::new().with_http_test_hosts(["127.0.0.1".to_owned()]);
    let url = url::Url::parse(&format!("{}app.bsky.actor.getProfile?actor={ALICE}", base)).unwrap();
    let holders = (0..8)
        .map(|_| {
            let outgoing = outgoing.clone();
            let url = url.clone();
            tokio::spawn(async move {
                outgoing
                    .get_with_limits(&url, Default::default(), 1024, 0, Duration::from_secs(2))
                    .await
            })
        })
        .collect::<Vec<_>>();
    tokio::time::timeout(Duration::from_secs(1), async {
        while fake.calls.load(Ordering::SeqCst) < 8 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("fake requests should hold every interactive outbound permit");
    let started = tokio::time::Instant::now();
    let (status, profile) = request_json(
        &router,
        "/xrpc/app.bsky.actor.getProfile?actor=did%3Aplc%3Aalice",
    )
    .await;
    assert!(
        started.elapsed() < Duration::from_millis(180),
        "cold profile lookup waited for a busy outbound pool"
    );
    assert_eq!(status, StatusCode::OK);
    assert_eq!(profile["followersCount"], 1);
    assert_eq!(profile["followsCount"], 1);
    assert_eq!(profile["postsCount"], 1);
    for holder in holders {
        holder.await.unwrap().unwrap();
    }
    server.abort();
}

#[derive(Clone)]
struct AcceptingFlow;

impl OAuthFlow for AcceptingFlow {
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
                did: ALICE.to_owned(),
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

#[tokio::test]
async fn profile_page_uses_remote_counts_and_stays_available_when_appview_fails() {
    let (base, fake, server) = start_fake_appview().await;
    let storage = storage_with_local_counts();
    let public = OAuthPublic::new("https://appview.example").unwrap();
    let router = public.router_with_flow_and_shutdown_and_profile_counts(
        AcceptingFlow,
        storage,
        WebShutdown::new(),
        counts_client(&base, Duration::from_secs(600), Duration::from_millis(40)),
    );
    let cookie = login(&router).await;

    let html = request_html(&router, "/profile?did=did%3Aplc%3Aalice", &cookie).await;
    assert!(
        html.contains("900"),
        "remote follower count missing: {html}"
    );
    assert!(
        html.contains("800"),
        "remote following count missing: {html}"
    );
    assert!(html.contains("700"), "remote post count missing: {html}");

    for reply in [
        FakeReply::ServerError,
        FakeReply::Garbage,
        FakeReply::Delay(Duration::from_millis(250)),
    ] {
        *fake.reply.lock().unwrap() = reply;
        let html = request_html(&router, "/profile?did=did%3Aplc%3Afriend", &cookie).await;
        assert!(html.contains("Followers"));
        assert!(
            html.contains(">1</strong>"),
            "local fallback count missing: {html}"
        );
    }
    server.abort();
}

async fn login(router: &Router) -> String {
    let start = router
        .clone()
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
        .unwrap();
    let login_cookie = start.headers()["set-cookie"]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    let callback = router
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

async fn request_html(router: &Router, uri: &str, cookie: &str) -> String {
    let response = router
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

async fn request_json(router: &Router, uri: &str) -> (StatusCode, Value) {
    let response = router
        .clone()
        .oneshot(Request::builder().uri(uri).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&body).unwrap())
}

fn storage_with_local_counts() -> Arc<MemoryStorage> {
    let storage = Arc::new(MemoryStorage::default());
    for (actor, subject, rkey) in [
        (ALICE, FRIEND, "alice-follows-friend"),
        (FRIEND, ALICE, "friend-follows-alice"),
    ] {
        storage
            .put_record(&IndexedRecord {
                did: actor.to_owned(),
                collection: "app.bsky.graph.follow".to_owned(),
                rkey: rkey.to_owned(),
                uri: format!("at://{actor}/app.bsky.graph.follow/{rkey}"),
                cid: None,
                time_us: 1,
                record: json!({"subject":subject}),
            })
            .unwrap();
    }
    storage
        .put_record(&IndexedRecord {
            did: ALICE.to_owned(),
            collection: "app.bsky.feed.post".to_owned(),
            rkey: "p1".to_owned(),
            uri: format!("at://{ALICE}/app.bsky.feed.post/p1"),
            cid: None,
            time_us: 1,
            record: json!({"text":"alice"}),
        })
        .unwrap();
    storage
        .put_record(&IndexedRecord {
            did: FRIEND.to_owned(),
            collection: "app.bsky.feed.post".to_owned(),
            rkey: "p1".to_owned(),
            uri: format!("at://{FRIEND}/app.bsky.feed.post/p1"),
            cid: None,
            time_us: 1,
            record: json!({"text":"friend"}),
        })
        .unwrap();
    storage
}

fn api_with_counts(storage: Arc<MemoryStorage>, counts: ProfileCountsClient) -> Router {
    let key = P256Keypair::create(&mut rand::thread_rng());
    let signing_did_key = key.did();
    let resolver = Arc::new(IdentityResolver::new(
        TestIdentity(DidDocument {
            id: ALICE.to_owned(),
            also_known_as: vec![],
            service: vec![],
            verification_method: vec![VerificationMethod {
                id: "#atproto".to_owned(),
                type_name: "Multikey".to_owned(),
                controller: ALICE.to_owned(),
                public_key_multibase: signing_did_key.strip_prefix("did:key:").unwrap().to_owned(),
            }],
        }),
        Duration::from_secs(30),
    ));
    api_router_with_profile_counts(
        storage,
        Arc::new(ServiceJwtVerifier::new(resolver, APPVIEW)),
        APPVIEW,
        counts,
    )
}

struct TestIdentity(DidDocument);
impl IdentityBackend for TestIdentity {
    async fn did_document(&self, did: &str) -> Result<Option<DidDocument>, ResolutionError> {
        Ok((self.0.id == did).then(|| self.0.clone()))
    }
    async fn dns_txt_did(&self, _: &str) -> Result<Option<String>, ResolutionError> {
        Ok(None)
    }
    async fn well_known_did(&self, _: &str) -> Result<Option<String>, ResolutionError> {
        Ok(None)
    }
}

#[test]
fn profile_count_ttl_defaults_to_ten_minutes_and_is_configurable() {
    let default = appview_rs::config::Config::default();
    assert_eq!(default.profile_counts_cache_ttl_secs, 600);
    let configured = appview_rs::config::Config::from_toml_and_env(
        "profile_counts_cache_ttl_secs = 120",
        [("APPVIEW_PROFILE_COUNTS_CACHE_TTL_SECS", "300")],
    )
    .unwrap();
    assert_eq!(configured.profile_counts_cache_ttl_secs, 300);
}

#[tokio::test]
#[ignore = "read-only live check; requires network access to public.api.bsky.app"]
async fn live_public_profile_counts_are_available_from_the_public_appview() {
    let client =
        ProfileCountsClient::new(SafeHttpClient::new(), PUBLIC_APPVIEW_BASE, Duration::ZERO);
    let did = "did:plc:z72i7hdynmk6r22z27h6tvur";
    let counts = client
        .get_counts(did)
        .await
        .expect("the public Bluesky profile should include actor counts");
    println!(
        "{did}: followers={}, following={}, posts={}",
        counts.followers, counts.following, counts.posts
    );
}
