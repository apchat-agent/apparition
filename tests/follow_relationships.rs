use appview_rs::{
    ingest::{MemoryStorage, Storage},
    net::SafeHttpClient,
    oauth::{OAuthAccount, OAuthCallback, OAuthFlow, OAuthPublic, SqliteOAuthStore, WebShutdown},
    profile_counts::{ProfileCountsClient, RelationshipClient, RelationshipState},
};
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
    io::{self, Write},
    path::PathBuf,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::net::TcpListener;
use tower::ServiceExt;
use tracing_subscriber::fmt::MakeWriter;

const VIEWER: &str = "did:plc:viewer";
const SUBJECT: &str = "did:plc:subject";
const FOLLOWER: &str = "did:plc:follower";
const OAUTH_SCOPE: &str = "atproto repo:app.bsky.feed.post?action=create repo:app.bsky.feed.like?action=create&action=delete repo:app.bsky.feed.repost?action=create&action=delete blob:image/* rpc:app.bsky.actor.getPreferences?aud=* rpc:app.bsky.feed.getFeedSkeleton?aud=* blob:video/* rpc:app.bsky.video.getUploadLimits?aud=* rpc:com.atproto.repo.uploadBlob?aud=* repo:app.bsky.graph.follow?action=create&action=delete repo:app.bsky.graph.block?action=create&action=delete repo:app.bsky.graph.listitem?action=create&action=delete rpc:app.bsky.graph.muteActor?aud=* rpc:app.bsky.graph.unmuteActor?aud=* rpc:app.bsky.graph.getMutes?aud=*";

struct TempDirGuard(PathBuf);

impl TempDirGuard {
    fn new(path: impl AsRef<std::path::Path>) -> Self {
        std::fs::create_dir_all(path.as_ref()).unwrap();
        Self(path.as_ref().to_owned())
    }
}

impl Drop for TempDirGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn temp_dir(case: &str) -> TempDirGuard {
    TempDirGuard::new(std::env::temp_dir().join(format!(
        "appview-follows-you-{}-{}-{}",
        case,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )))
}

#[derive(Clone)]
enum GraphReply {
    FollowedBy,
    NotFollowed,
    FollowedAndFollowing,
    Error,
    DelayNotFollowed(Duration),
}

type RelationshipQueries = Arc<Mutex<Vec<(String, Vec<String>)>>>;

#[derive(Clone)]
struct FakeGraph {
    calls: Arc<AtomicUsize>,
    queries: RelationshipQueries,
    reply: Arc<Mutex<GraphReply>>,
}

impl FakeGraph {
    async fn respond(&self, actor: String, others: Vec<String>) -> Response<Body> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.queries
            .lock()
            .unwrap()
            .push((actor.clone(), others.clone()));
        let subject = others.first().cloned().unwrap_or_default();
        let followed_by = json!({"actor": subject, "subject": actor});
        let following = json!({"actor": actor, "subject": subject});
        let reply = self.reply.lock().unwrap().clone();
        let relationship = match reply {
            GraphReply::FollowedBy => json!({"follows": Value::Null, "followedBy": followed_by}),
            GraphReply::NotFollowed => json!({"follows": Value::Null, "followedBy": Value::Null}),
            GraphReply::FollowedAndFollowing => {
                json!({"follows": following, "followedBy": followed_by})
            }
            GraphReply::Error => {
                return (StatusCode::INTERNAL_SERVER_ERROR, "graph unavailable").into_response();
            }
            GraphReply::DelayNotFollowed(duration) => {
                tokio::time::sleep(duration).await;
                json!({"follows": Value::Null, "followedBy": Value::Null})
            }
        };
        axum::Json(json!({"actor": actor, "relationships": [relationship]})).into_response()
    }
}

async fn fake_get_relationships(
    State(fake): State<FakeGraph>,
    Query(query): Query<HashMap<String, String>>,
    RawQuery(raw): RawQuery,
) -> Response<Body> {
    let others = url::form_urlencoded::parse(raw.as_deref().unwrap_or_default().as_bytes())
        .filter(|(key, _)| key == "others")
        .map(|(_, value)| value.into_owned())
        .collect();
    fake.respond(query.get("actor").cloned().unwrap_or_default(), others)
        .await
}

async fn start_fake_graph() -> (String, FakeGraph, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let fake = FakeGraph {
        calls: Arc::new(AtomicUsize::new(0)),
        queries: Arc::new(Mutex::new(Vec::new())),
        reply: Arc::new(Mutex::new(GraphReply::FollowedBy)),
    };
    let app = Router::new()
        .route(
            "/xrpc/app.bsky.graph.getRelationships",
            get(fake_get_relationships),
        )
        .with_state(fake.clone());
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{address}/xrpc/"), fake, task)
}

fn relationship_client(
    base: &str,
    timeout: Duration,
    store: SqliteOAuthStore,
) -> RelationshipClient {
    RelationshipClient::new(
        SafeHttpClient::new().with_http_test_hosts(["127.0.0.1".to_owned()]),
        base,
    )
    .with_timeout(timeout)
    .with_store(store)
}

#[derive(Clone)]
struct AcceptingFlow;

impl OAuthFlow for AcceptingFlow {
    fn authorize<'a>(
        &'a self,
        _: &'a str,
        _: &'a str,
        _: Vec<String>,
    ) -> Pin<Box<dyn Future<Output = Result<String, String>> + Send + 'a>> {
        Box::pin(async { Ok("https://auth.example/authorize".to_owned()) })
    }
    fn callback<'a>(
        &'a self,
        _: OAuthCallback,
    ) -> Pin<Box<dyn Future<Output = Result<OAuthAccount, String>> + Send + 'a>> {
        Box::pin(async {
            Ok(OAuthAccount {
                did: VIEWER.to_owned(),
                verified_handle: Some("viewer.test".to_owned()),
                granted_scope: Some(OAUTH_SCOPE.to_owned()),
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

fn build_router(storage: Arc<MemoryStorage>, relationships: RelationshipClient) -> Router {
    OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow_and_shutdown_and_profile_counts_and_relationships(
            AcceptingFlow,
            storage,
            WebShutdown::new(),
            ProfileCountsClient::local_only(),
            relationships,
        )
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
                .body(Body::from("handle=viewer.test"))
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

async fn request_profile(router: &Router, cookie: &str) -> (StatusCode, String) {
    request_profile_for(router, cookie, SUBJECT).await
}

async fn request_profile_for(router: &Router, cookie: &str, subject: &str) -> (StatusCode, String) {
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/profile?did={}", urlencoding(subject)))
                .header("cookie", cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
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
    (status, html)
}

async fn request_html(router: &Router, cookie: &str) -> String {
    let (status, html) = request_profile(router, cookie).await;
    assert_eq!(status, StatusCode::OK);
    html
}

fn has_pill(html: &str) -> bool {
    html.contains("follows-you-pill")
}

fn urlencoding(value: &str) -> String {
    value.replace(':', "%3A")
}

async fn wait_until(mut check: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while !check() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "background relationship update never landed"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

struct CapturedMakeWriter(Arc<Mutex<Vec<u8>>>);

struct CapturedWriter(Arc<Mutex<Vec<u8>>>);

impl<'a> MakeWriter<'a> for CapturedMakeWriter {
    type Writer = CapturedWriter;

    fn make_writer(&'a self) -> Self::Writer {
        CapturedWriter(self.0.clone())
    }
}

impl Write for CapturedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

// (b) A pair never confirmed before must be checked with
// app.bsky.graph.getRelationships before the page renders: the pill shows on
// the FIRST load and the follower row is kept so later loads answer from rows.
#[tokio::test]
async fn an_unconfirmed_relationship_shows_follows_you_on_the_first_load_and_keeps_the_row() {
    let (base, fake, server) = start_fake_graph().await;
    let storage = Arc::new(MemoryStorage::default());
    let store = SqliteOAuthStore::open(":memory:").unwrap();
    let app = build_router(
        storage.clone(),
        relationship_client(&base, Duration::from_secs(2), store),
    );
    let cookie = login(&app).await;

    let html = request_html(&app, &cookie).await;
    assert!(
        has_pill(&html),
        "a relationship the appview confirms must show Follows you on the first load: {html}"
    );
    assert!(
        storage.is_following(SUBJECT, VIEWER).unwrap(),
        "the confirmed follower must live in the viewer's own follows rows"
    );
    assert_eq!(fake.calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        fake.queries.lock().unwrap().first().cloned(),
        Some((VIEWER.to_owned(), vec![SUBJECT.to_owned()])),
        "the confirmation must ask getRelationships as the viewer about the subject"
    );

    let html = request_html(&app, &cookie).await;
    assert!(has_pill(&html));
    assert_eq!(
        fake.calls.load(Ordering::SeqCst),
        1,
        "a freshly confirmed pair must answer from the rows without a new request"
    );
    server.abort();
}

#[tokio::test]
async fn an_unconfirmed_relationship_without_followed_by_stays_silent_and_records_the_answer() {
    let (base, fake, server) = start_fake_graph().await;
    *fake.reply.lock().unwrap() = GraphReply::NotFollowed;
    let storage = Arc::new(MemoryStorage::default());
    let store = SqliteOAuthStore::open(":memory:").unwrap();
    let app = build_router(
        storage.clone(),
        relationship_client(&base, Duration::from_secs(2), store),
    );
    let cookie = login(&app).await;

    let html = request_html(&app, &cookie).await;
    assert!(
        !has_pill(&html),
        "the appview says the subject does not follow the viewer: {html}"
    );
    assert!(
        !storage.is_following(SUBJECT, VIEWER).unwrap(),
        "a not-following confirmation must leave the follows rows answering the same"
    );
    assert_eq!(fake.calls.load(Ordering::SeqCst), 1);
    let html = request_html(&app, &cookie).await;
    assert!(!has_pill(&html));
    assert_eq!(
        fake.calls.load(Ordering::SeqCst),
        1,
        "a confirmed not-following answer must not be re-asked inside the TTL"
    );
    server.abort();
}

// When the rows disagree with the appview about the viewer's OWN follow, the
// response's `follows` field corrects the row the page reads its button from.
#[tokio::test]
async fn a_relationship_that_says_the_viewer_follows_corrects_the_viewers_own_row() {
    let (base, fake, server) = start_fake_graph().await;
    *fake.reply.lock().unwrap() = GraphReply::FollowedAndFollowing;
    let storage = Arc::new(MemoryStorage::default());
    let store = SqliteOAuthStore::open(":memory:").unwrap();
    let app = build_router(
        storage.clone(),
        relationship_client(&base, Duration::from_secs(2), store),
    );
    let cookie = login(&app).await;
    assert!(!storage.is_following(VIEWER, SUBJECT).unwrap());

    request_html(&app, &cookie).await;
    assert!(
        storage.is_following(VIEWER, SUBJECT).unwrap(),
        "the page showed Follow while the relationship said the viewer follows the subject"
    );
    let html = request_html(&app, &cookie).await;
    assert!(
        html.contains("name=\"action\" value=\"unfollow\""),
        "the corrected row must drive the follow button on the next load: {html}"
    );
    server.abort();
}

// (c) A lookup that fails must never take the page down or touch the rows:
// keep answering from them, warn with the real error, and count the failure.
#[tokio::test(flavor = "current_thread")]
async fn a_failed_relationship_lookup_keeps_the_page_and_rows_and_warns() {
    let (base, fake, server) = start_fake_graph().await;
    *fake.reply.lock().unwrap() = GraphReply::Error;
    let storage = Arc::new(MemoryStorage::default());
    storage.set_follow(SUBJECT, VIEWER, true).unwrap();
    let store = SqliteOAuthStore::open(":memory:").unwrap();
    let app = build_router(
        storage.clone(),
        relationship_client(&base, Duration::from_secs(2), store),
    );
    let cookie = login(&app).await;

    let output = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_max_level(tracing::Level::WARN)
        .with_writer(CapturedMakeWriter(output.clone()))
        .finish();
    let dispatch = tracing::Dispatch::new(subscriber);
    let _guard = tracing::dispatcher::set_default(&dispatch);
    let first = request_html(&app, &cookie).await;
    let second = request_html(&app, &cookie).await;
    let third = request_html(&app, &cookie).await;
    drop(_guard);

    assert!(
        has_pill(&first),
        "the stored row must still answer: {first}"
    );
    assert!(has_pill(&second));
    assert!(has_pill(&third));
    assert!(
        storage.is_following(SUBJECT, VIEWER).unwrap(),
        "a failed confirmation must leave the existing rows untouched"
    );
    assert_eq!(
        fake.calls.load(Ordering::SeqCst),
        3,
        "a failure must never be cached as a confirmation"
    );
    let logs = String::from_utf8(output.lock().unwrap().clone()).unwrap();
    let warnings = logs
        .lines()
        .filter(|line| line.contains("WARN"))
        .filter(|line| line.contains("follows-you"))
        .collect::<Vec<_>>();
    assert_eq!(warnings.len(), 3, "one warning per failure: {logs}");
    assert!(
        logs.contains("app.bsky.graph.getRelationships"),
        "the warning must name the failing endpoint: {logs}"
    );
    assert!(
        logs.contains("500"),
        "the warning must carry the real error: {logs}"
    );
    assert!(
        logs.contains("did:plc:subject"),
        "the warning must name the pair: {logs}"
    );
    server.abort();
}

// (d) A confirmed pair answers from its rows forever: the per-pair confirmed
// time lives in the persistent web_cache, so restarting keeps trusting the
// rows and only asks the appview once the answer is older than six hours.
#[tokio::test]
async fn a_confirmation_survives_a_restart_and_only_an_expired_pair_asks_again() {
    let dir = temp_dir("restart");
    let oauth_path = dir.0.join("oauth.db");
    let (base, fake, server) = start_fake_graph().await;
    let storage = Arc::new(MemoryStorage::default());

    let store = SqliteOAuthStore::open(&oauth_path).unwrap();
    let app = build_router(
        storage.clone(),
        relationship_client(&base, Duration::from_secs(2), store.clone()),
    );
    let cookie = login(&app).await;
    let html = request_html(&app, &cookie).await;
    assert!(has_pill(&html));
    request_html(&app, &cookie).await;
    assert_eq!(fake.calls.load(Ordering::SeqCst), 1);
    drop(app);
    drop(store);

    // Restart: a fresh client and a freshly opened OAuth database.
    let store = SqliteOAuthStore::open(&oauth_path).unwrap();
    let app = build_router(
        storage.clone(),
        relationship_client(&base, Duration::from_secs(2), store.clone()),
    );
    let cookie = login(&app).await;
    let html = request_html(&app, &cookie).await;
    assert!(
        has_pill(&html),
        "the restarted page must still show Follows you from the rows"
    );
    assert_eq!(
        fake.calls.load(Ordering::SeqCst),
        1,
        "a restarted viewer must not be asked to re-confirm a fresh pair"
    );

    // Age the confirmation beyond the six-hour TTL: the page answers from the
    // stored rows at once and refreshes exactly once in the background.
    let connection = rusqlite::Connection::open(&oauth_path).unwrap();
    connection
        .execute_batch("PRAGMA busy_timeout = 5000;")
        .unwrap();
    connection
        .execute(
            "UPDATE web_cache SET stored_at_ms = stored_at_ms - 25_200_000",
            [],
        )
        .unwrap();
    assert_eq!(
        connection
            .query_row("SELECT COUNT(*) FROM web_cache", [], |row| row
                .get::<_, i64>(0))
            .unwrap(),
        1
    );
    drop(connection);
    *fake.reply.lock().unwrap() = GraphReply::NotFollowed;

    let html = request_html(&app, &cookie).await;
    assert!(
        has_pill(&html),
        "a stale confirmation must serve the stored answer immediately: {html}"
    );
    wait_until(|| !storage.is_following(SUBJECT, VIEWER).unwrap()).await;
    let html = request_html(&app, &cookie).await;
    assert!(
        !has_pill(&html),
        "the background refresh must have corrected the row: {html}"
    );
    assert_eq!(
        fake.calls.load(Ordering::SeqCst),
        2,
        "exactly one background refresh per stale pair"
    );
    server.abort();
}

// Two profile loads that both find the confirmation stale must share one
// background refresh: the in-progress pair is deduplicated and the spawned
// refresh owns its bookkeeping, so no second request slips through.
#[tokio::test]
async fn two_stale_loads_share_one_background_refresh() {
    let dir = temp_dir("stale-dedup");
    let oauth_path = dir.0.join("oauth.db");
    let (base, fake, server) = start_fake_graph().await;
    let storage = Arc::new(MemoryStorage::default());
    let store = SqliteOAuthStore::open(&oauth_path).unwrap();
    let app = build_router(
        storage.clone(),
        relationship_client(&base, Duration::from_secs(2), store),
    );
    let cookie = login(&app).await;
    request_html(&app, &cookie).await;
    assert_eq!(fake.calls.load(Ordering::SeqCst), 1);

    let connection = rusqlite::Connection::open(&oauth_path).unwrap();
    connection
        .execute_batch("PRAGMA busy_timeout = 5000;")
        .unwrap();
    // The confirmation reaches web_cache through the store's write-behind
    // queue, so age it only once the queued insert has actually landed.
    wait_until(|| {
        connection
            .execute(
                "UPDATE web_cache SET stored_at_ms = stored_at_ms - 25_200_000",
                [],
            )
            .unwrap()
            == 1
    })
    .await;
    drop(connection);
    // The refresh is slow enough that the second stale load while it runs
    // must find the pair already in progress.
    *fake.reply.lock().unwrap() = GraphReply::DelayNotFollowed(Duration::from_millis(250));

    let (first, second) = tokio::join!(request_html(&app, &cookie), request_html(&app, &cookie));
    assert!(has_pill(&first), "the stale path serves the stored answer");
    assert!(has_pill(&second));

    wait_until(|| !storage.is_following(SUBJECT, VIEWER).unwrap()).await;
    assert_eq!(
        fake.calls.load(Ordering::SeqCst),
        2,
        "both stale loads together may trigger only one refresh"
    );
    server.abort();
}

// The guard audit the brief demands: the follower rows a relationship
// confirmation writes (actor = a subject this appview never indexed, never
// registered) must not enqueue anyone for re-backfill or move tracked-account
// state. Registration itself queues the new user, so everything is compared
// against the post-login baseline.
#[tokio::test]
async fn a_follower_row_for_an_unregistered_actor_touches_no_backfill_state() {
    let storage = Arc::new(MemoryStorage::default());
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(AcceptingFlow, storage.clone());
    let cookie = login(&app).await;
    let pending_before = storage.pending_backfills().unwrap();
    let follow_count_before = storage.tracked_follow_count(VIEWER).unwrap();
    let viewer_tracked_before = storage.is_tracked_did(VIEWER).unwrap();

    RelationshipClient::local_only()
        .apply_relationship(
            storage.as_ref(),
            VIEWER,
            FOLLOWER,
            RelationshipState {
                followed_by: true,
                following: None,
            },
        )
        .unwrap();
    storage
        .set_follow("did:plc:follower-two", VIEWER, true)
        .unwrap();

    assert!(storage.is_following(FOLLOWER, VIEWER).unwrap());
    assert!(
        storage
            .is_following("did:plc:follower-two", VIEWER)
            .unwrap(),
        "the direct row write must land too"
    );
    assert_eq!(
        storage.pending_backfills().unwrap(),
        pending_before,
        "follower rows with unregistered actors must not queue anyone for re-backfill"
    );
    assert_eq!(
        storage.tracked_follow_count(VIEWER).unwrap(),
        follow_count_before,
        "follower rows with unregistered actors must not move tracked follow counts"
    );
    assert_eq!(
        storage.is_tracked_did(VIEWER).unwrap(),
        viewer_tracked_before,
        "follower rows must not change whether the viewer is tracked"
    );
    assert!(
        !storage.is_tracked_did(FOLLOWER).unwrap(),
        "a confirmed follower must not become a tracked account by itself"
    );
    let html = request_profile_for(&app, &cookie, FOLLOWER).await.1;
    assert!(
        has_pill(&html),
        "the stored follower row must show Follows you on the follower's profile without any request: {html}"
    );
}
