use appview_rs::{
    ingest::{IndexedRecord, MemoryStorage, Storage},
    net::SafeHttpClient,
    oauth::{OAuthAccount, OAuthCallback, OAuthFlow, OAuthPublic, WebShutdown},
    profile_counts::{ProfileCountsClient, RelationshipClient},
    search::SearchClient,
};
use axum::{
    Router,
    body::Body,
    extract::{RawQuery, State},
    http::{Request, Response, StatusCode},
    response::IntoResponse,
    routing::get,
};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use tokio::net::TcpListener;
use tower::ServiceExt;
use tracing::{Event, Subscriber, field::Visit};
use tracing_subscriber::{Layer, layer::Context, prelude::*};

const ALICE: &str = "did:plc:alice";
const HIT_AUTHOR: &str = "did:plc:searched";
const HIT_URI: &str = "at://did:plc:searched/app.bsky.feed.post/hit1";
const WRITE_SCOPE: &str = "atproto repo:app.bsky.feed.post?action=create repo:app.bsky.feed.like?action=create&action=delete repo:app.bsky.feed.repost?action=create&action=delete";

#[derive(Clone)]
struct FakeSearch {
    calls: Arc<AtomicUsize>,
    requests: Arc<Mutex<Vec<(String, String)>>>,
    posts_body: Arc<Mutex<Value>>,
    actors_body: Arc<Mutex<Value>>,
    fail: Arc<AtomicBool>,
    cursor_forbidden: Arc<AtomicBool>,
    all_forbidden: Arc<AtomicBool>,
}

impl FakeSearch {
    fn posts_body(&self) -> Value {
        self.posts_body.lock().unwrap().clone()
    }

    fn failure(&self) -> Option<Response<Body>> {
        self.fail.load(Ordering::SeqCst).then(|| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                r#"{"error":"InternalServerError","message":"the upstream exploded"}"#,
            )
                .into_response()
        })
    }
}

async fn fake_search_posts(
    State(fake): State<FakeSearch>,
    RawQuery(query): RawQuery,
) -> Response<Body> {
    fake.record_call("/xrpc/app.bsky.feed.searchPosts", query.clone());
    if let Some(failure) = fake.failure() {
        return failure;
    }
    // Measured 2026-10-31: api.bsky.app answers page 1 but refuses every anonymous request
    // that carries a cursor with 403 "Request forbidden by administrative rules".
    if fake.all_forbidden.load(Ordering::SeqCst)
        || (fake.cursor_forbidden.load(Ordering::SeqCst)
            && query
                .as_deref()
                .is_some_and(|query| query.contains("cursor=")))
    {
        return (
            StatusCode::FORBIDDEN,
            r#"{"error":"AuthenticationRequired","message":"Request forbidden by administrative rules"}"#,
        )
            .into_response();
    }
    axum::Json(fake.posts_body()).into_response()
}

async fn fake_search_actors(
    State(fake): State<FakeSearch>,
    RawQuery(query): RawQuery,
) -> Response<Body> {
    fake.record_call("/xrpc/app.bsky.actor.searchActors", query);
    if let Some(failure) = fake.failure() {
        return failure;
    }
    axum::Json(fake.actors_body.lock().unwrap().clone()).into_response()
}

impl FakeSearch {
    fn record_call(&self, path: &str, query: Option<String>) {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.requests
            .lock()
            .unwrap()
            .push((path.to_owned(), query.unwrap_or_default()));
    }
}

fn search_posts_body(cursor: Option<&str>) -> Value {
    // The real searchPosts answer lists PostViews directly, with no "post" wrapper (unlike
    // getTimeline's FeedViewPost). Checked against api.bsky.app on 2026-10-31.
    json!({
        "posts": [{
            "uri": HIT_URI,
            "cid": "hit1-cid",
            "author": {
                "did": "did:plc:searched",
                "handle": "searched.test",
                "displayName": "Searched",
                "avatar": "https://cdn.bsky.app/img/avatar/plain/did:plc:searched/bafkhit@jpeg"
            },
            "record": {
                "$type": "app.bsky.feed.post",
                "text": "mincy post",
                "createdAt": "2026-10-30T10:00:00.000Z"
            },
            "indexedAt": "2026-10-30T10:00:01.000Z"
        }],
        "cursor": cursor
    })
}

async fn start_fake_search(body: Value) -> (String, FakeSearch, tokio::task::JoinHandle<()>) {
    start_fake_search_with(body, json!({"actors": []})).await
}

async fn start_fake_search_with(
    posts_body: Value,
    actors_body: Value,
) -> (String, FakeSearch, tokio::task::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let fake = FakeSearch {
        calls: Arc::new(AtomicUsize::new(0)),
        requests: Arc::new(Mutex::new(Vec::new())),
        posts_body: Arc::new(Mutex::new(posts_body)),
        actors_body: Arc::new(Mutex::new(actors_body)),
        fail: Arc::new(AtomicBool::new(false)),
        cursor_forbidden: Arc::new(AtomicBool::new(false)),
        all_forbidden: Arc::new(AtomicBool::new(false)),
    };
    let app = Router::new()
        .route("/xrpc/app.bsky.feed.searchPosts", get(fake_search_posts))
        .route("/xrpc/app.bsky.actor.searchActors", get(fake_search_actors))
        .with_state(fake.clone());
    let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{address}/xrpc/"), fake, task)
}

fn search_client(base: &str) -> SearchClient {
    SearchClient::new(
        SafeHttpClient::new().with_http_test_hosts(["127.0.0.1".to_owned()]),
        base,
    )
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
                did: ALICE.to_owned(),
                verified_handle: Some("alice.test".to_owned()),
                granted_scope: Some(WRITE_SCOPE.to_owned()),
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

async fn response_for(router: &Router, uri: &str, cookie: Option<&str>) -> Response<Body> {
    let mut builder = Request::builder().uri(uri);
    if let Some(cookie) = cookie {
        builder = builder.header("cookie", cookie);
    }
    router
        .clone()
        .oneshot(builder.body(Body::empty()).unwrap())
        .await
        .unwrap()
}

async fn get_html(router: &Router, uri: &str, cookie: &str) -> String {
    let response = response_for(router, uri, Some(cookie)).await;
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

fn router_with_search(flow: AcceptingFlow, search: SearchClient) -> Router {
    router_with_any_search(flow, search)
}

fn router_with_any_search<F: OAuthFlow>(flow: F, search: SearchClient) -> Router {
    OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow_and_shutdown_and_profile_counts_and_relationships_and_search(
            flow,
            Arc::new(MemoryStorage::default()),
            WebShutdown::new(),
            ProfileCountsClient::local_only(),
            RelationshipClient::local_only(),
            search,
        )
}

#[tokio::test]
async fn search_redirects_signed_out_visitors_like_the_feeds_page() {
    let (_base, _fake, server) = start_fake_search(search_posts_body(None)).await;
    let router = router_with_search(AcceptingFlow, search_client("http://127.0.0.1:1/xrpc/"));
    let response = response_for(&router, "/search?q=minc", None).await;
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    assert_eq!(response.headers()["location"], "/login");
    server.abort();
}

#[tokio::test]
async fn search_ui_has_styles_and_the_sidebar_search_is_a_working_posts_form() {
    let (base, _fake, server) = start_fake_search(search_posts_body(None)).await;
    let router = router_with_search(AcceptingFlow, search_client(&base));
    let cookie = login(&router).await;
    let html = get_html(&router, "/search", &cookie).await;
    let css = include_str!("../static/app.css");

    for class in [
        ".search-page-form",
        ".search-type-tabs",
        ".search-type-tab",
        ".search-actor-list",
        ".search-actor",
        ".search-actor-link",
        ".search-actor-avatar",
        ".search-actor-description",
    ] {
        assert!(css.contains(class), "missing search style for {class}");
    }
    assert!(
        html.contains(
            "<form class=\"search-form\" action=\"/search\" method=\"get\" role=\"search\">"
        ) && html.contains("name=\"q\" type=\"search\"")
            && html.contains("name=\"type\" value=\"posts\"")
            && !html.contains("id=\"site-search\" type=\"search\" placeholder=\"Search\" disabled"),
        "the desktop sidebar search must submit a posts query: {html}"
    );
    server.abort();
}

#[tokio::test]
async fn recent_profiles_keep_the_latest_ten_search_result_clicks_in_order() {
    let actors = json!({"actors": (0..12).map(|index| json!({
        "did": format!("did:plc:recent{index}"),
        "handle": format!("recent{index}.test"),
        "displayName": format!("Recent {index}"),
        "description": format!("Bio {index}"),
    })).collect::<Vec<_>>()});
    let (base, _fake, server) = start_fake_search_with(search_posts_body(None), actors).await;
    let storage = Arc::new(MemoryStorage::default());
    let router = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow_and_shutdown_and_profile_counts_and_relationships_and_search(
            AcceptingFlow,
            storage,
            WebShutdown::new(),
            ProfileCountsClient::local_only(),
            RelationshipClient::local_only(),
            search_client(&base),
        );
    let cookie = login(&router).await;
    let results = get_html(&router, "/search?q=recent&type=people", &cookie).await;
    assert!(results.contains("&amp;from=search"), "search result links must carry the marker: {results}");

    for index in 0..12 {
        let response = response_for(
            &router,
            &format!("/profile?did=did%3Aplc%3Arecent{index}&from=search"),
            Some(&cookie),
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
    }
    let reopened = response_for(
        &router,
        "/profile?did=did%3Aplc%3Arecent5&from=search",
        Some(&cookie),
    )
    .await;
    assert_eq!(reopened.status(), StatusCode::OK);

    let html = get_html(&router, "/search?type=people", &cookie).await;
    let rows = html
        .split("<ol class=\"search-actor-list\">")
        .nth(1)
        .unwrap_or_default()
        .split("</ol>")
        .next()
        .unwrap_or_default();
    assert!(html.contains("Recent"), "the blank People tab must show recent profiles: {html}");
    assert!(rows.contains("Recent 5"), "reopened profile should be first: {rows}");
    assert!(rows.contains("Bio 5"), "stored public profile bio should be reused: {rows}");
    assert_eq!(rows.matches("search-actor\"").count(), 10, "keep only ten unique profiles: {rows}");
    assert!(!rows.contains("Recent 0") && !rows.contains("Recent 1"), "oldest profiles should be evicted: {rows}");
    server.abort();
}

#[tokio::test]
async fn recent_profile_clear_requires_same_origin_and_redirects_back_to_people_search() {
    let (base, _fake, server) = start_fake_search(search_posts_body(None)).await;
    let router = router_with_search(AcceptingFlow, search_client(&base));
    let cookie = login(&router).await;
    let cross_origin = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/search/recent/clear")
                .header("origin", "https://evil.example")
                .header("cookie", &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(cross_origin.status(), StatusCode::FORBIDDEN);

    let cleared = router
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/search/recent/clear")
                .header("origin", "https://appview.example")
                .header("cookie", &cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(cleared.status(), StatusCode::SEE_OTHER);
    assert_eq!(cleared.headers()["location"], "/search?type=people");
    server.abort();
}

#[tokio::test]
async fn empty_query_search_shows_the_form_and_makes_no_outbound_call() {
    let (base, fake, server) = start_fake_search(search_posts_body(None)).await;
    let router = router_with_search(AcceptingFlow, search_client(&base));
    let cookie = login(&router).await;
    let html = get_html(&router, "/search", &cookie).await;
    assert!(
        html.contains("name=\"q\""),
        "the search form must carry the query box: {html}"
    );
    assert!(
        html.contains("action=\"/search\"") && html.contains("method=\"get\""),
        "the form must submit to /search by GET: {html}"
    );
    assert_eq!(fake.calls.load(Ordering::SeqCst), 0);
    server.abort();
}

#[tokio::test]
async fn posts_search_calls_the_labelled_base_once_and_renders_through_the_feed_renderer() {
    let (base, fake, server) = start_fake_search(search_posts_body(None)).await;
    let router = router_with_search(AcceptingFlow, search_client(&base));
    let cookie = login(&router).await;
    let html = get_html(&router, "/search?q=minc", &cookie).await;
    assert_eq!(fake.calls.load(Ordering::SeqCst), 1);
    let requests = fake.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 1);
    assert_eq!(requests[0].0, "/xrpc/app.bsky.feed.searchPosts");
    let query = url::form_urlencoded::parse(requests[0].1.as_bytes())
        .map(|(key, value)| (key.into_owned(), value.into_owned()))
        .collect::<Vec<_>>();
    assert!(
        query
            .iter()
            .any(|(key, value)| key == "q" && value == "minc"),
        "q=minc missing from {query:?}"
    );
    assert!(
        query
            .iter()
            .any(|(key, value)| key == "limit" && value == "25"),
        "limit=25 missing from {query:?}"
    );
    assert!(html.contains("mincy post"), "post text missing: {html}");
    assert!(
        html.contains(
            "/app/thread?uri=at%3A%2F%2Fdid%3Aplc%3Asearched%2Fapp.bsky.feed.post%2Fhit1"
        ),
        "the shared post renderer's thread link must appear: {html}"
    );
    assert!(
        html.contains(&format!("value=\"{HIT_URI}\"")),
        "the shared post renderer's like form must reference the hit: {html}"
    );
    server.abort();
}

#[tokio::test]
async fn people_search_renders_actor_rows_linking_to_the_profile_page() {
    let actors = json!({"actors": [{
        "did": "did:plc:finder",
        "handle": "finder.test",
        "displayName": "Finder",
        "description": "First line.\nSecond line hides.",
        "avatar": "https://cdn.bsky.app/img/avatar/plain/did:plc:finder/bafkfinder@jpeg"
    }]});
    let (base, fake, server) = start_fake_search_with(search_posts_body(None), actors).await;
    let router = router_with_search(AcceptingFlow, search_client(&base));
    let cookie = login(&router).await;
    let html = get_html(&router, "/search?q=fin&type=people", &cookie).await;
    let requests = fake.requests.lock().unwrap().clone();
    assert_eq!(
        requests.len(),
        1,
        "people search must hit the people endpoint once"
    );
    assert_eq!(requests[0].0, "/xrpc/app.bsky.actor.searchActors");
    assert!(requests[0].1.contains("q=fin"), "{}", requests[0].1);
    assert!(requests[0].1.contains("limit=25"), "{}", requests[0].1);
    assert!(html.contains("Finder"), "display name missing: {html}");
    assert!(html.contains("@finder.test"), "handle missing: {html}");
    assert!(
        html.contains("First line."),
        "description first line missing: {html}"
    );
    assert!(
        !html.contains("Second line"),
        "only the description's first line may show: {html}"
    );
    assert!(
        html.contains("/profile?did=did%3Aplc%3Afinder"),
        "the row must link to the app's profile page: {html}"
    );
    assert!(
        html.contains(
            "https://cdn.bsky.app/img/avatar_thumbnail/plain/did:plc:finder/bafkfinder@jpeg"
        ),
        "the row must show the avatar: {html}"
    );
    server.abort();
}

#[derive(Clone)]
struct MutingFlow {
    muted: std::collections::HashSet<String>,
}

impl OAuthFlow for MutingFlow {
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
                did: ALICE.to_owned(),
                verified_handle: Some("alice.test".to_owned()),
                granted_scope: Some(WRITE_SCOPE.to_owned()),
            })
        })
    }
    fn logout<'a>(
        &'a self,
        _: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }
    fn muted_actor_dids<'a>(
        &'a self,
        _: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<std::collections::HashSet<String>, String>> + Send + 'a>>
    {
        let muted = self.muted.clone();
        Box::pin(async move { Ok(muted) })
    }
}

#[tokio::test]
async fn posts_by_a_muted_author_are_hidden_from_search_results() {
    let (base, _fake, server) = start_fake_search(search_posts_body(None)).await;
    let router = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow_and_shutdown_and_profile_counts_and_relationships_and_search(
            MutingFlow {
                muted: [HIT_AUTHOR.to_owned()].into_iter().collect(),
            },
            Arc::new(MemoryStorage::default()),
            WebShutdown::new(),
            ProfileCountsClient::local_only(),
            RelationshipClient::local_only(),
            search_client(&base),
        );
    let cookie = login(&router).await;
    let html = get_html(&router, "/search?q=minc", &cookie).await;
    assert!(
        !html.contains("mincy post"),
        "a muted author's post must not be shown: {html}"
    );
    assert!(
        html.contains("empty-state"),
        "the page needs its empty message: {html}"
    );
    server.abort();
}

#[tokio::test]
async fn posts_by_a_blocked_author_are_hidden_from_search_results() {
    let (base, _fake, server) = start_fake_search(search_posts_body(None)).await;
    let storage = Arc::new(MemoryStorage::default());
    storage
        .put_record(&IndexedRecord {
            did: ALICE.into(),
            collection: "app.bsky.graph.block".into(),
            rkey: "block-hit".into(),
            uri: "at://did:plc:alice/app.bsky.graph.block/block-hit".into(),
            cid: None,
            time_us: 1,
            record: json!({"subject": HIT_AUTHOR, "createdAt": "2026-10-30T09:00:00Z"}),
        })
        .unwrap();
    let router = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow_and_shutdown_and_profile_counts_and_relationships_and_search(
            AcceptingFlow,
            storage,
            WebShutdown::new(),
            ProfileCountsClient::local_only(),
            RelationshipClient::local_only(),
            search_client(&base),
        );
    let cookie = login(&router).await;
    let html = get_html(&router, "/search?q=minc", &cookie).await;
    assert!(
        !html.contains("mincy post"),
        "a blocked author's post must not be shown: {html}"
    );
    assert!(
        html.contains("empty-state"),
        "the page needs its empty message: {html}"
    );
    server.abort();
}

#[derive(Clone)]
struct WarningCapture(Arc<Mutex<Vec<String>>>);

struct WarningVisitor<'a>(&'a mut String);

impl Visit for WarningVisitor<'_> {
    fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
        use std::fmt::Write as _;
        let _ = write!(self.0, "{}={value:?} ", field.name());
    }

    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        use std::fmt::Write as _;
        let _ = write!(self.0, "{}={value} ", field.name());
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        use std::fmt::Write as _;
        let _ = write!(self.0, "{}={value} ", field.name());
    }
}

impl<S: Subscriber> Layer<S> for WarningCapture {
    fn on_event(&self, event: &Event<'_>, _: Context<'_, S>) {
        if *event.metadata().level() == tracing::Level::WARN {
            let mut line = String::new();
            event.record(&mut WarningVisitor(&mut line));
            self.0.lock().unwrap().push(line);
        }
    }
}

#[tokio::test]
async fn a_search_api_failure_shows_a_readable_page_and_logs_status_and_body_excerpt() {
    let (base, fake, server) = start_fake_search(search_posts_body(None)).await;
    fake.fail.store(true, Ordering::SeqCst);
    let router = router_with_search(AcceptingFlow, search_client(&base));
    let cookie = login(&router).await;
    let warnings = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry().with(WarningCapture(warnings.clone()));
    let _guard = tracing::subscriber::set_default(subscriber);
    let html = get_html(&router, "/search?q=minc", &cookie).await;
    drop(_guard);
    assert!(
        html.contains("Search is unavailable right now (status 500)"),
        "the readable error line must name the status: {html}"
    );
    assert!(
        !html.contains("upstream exploded"),
        "the upstream JSON must never be echoed into the page: {html}"
    );
    let warnings = warnings.lock().unwrap();
    assert!(
        warnings.iter().any(|warning| {
            warning.contains("status=500") && warning.contains("upstream exploded")
        }),
        "the warning must carry the status and the body excerpt: {warnings:?}"
    );
    server.abort();
}

#[tokio::test]
async fn a_response_cursor_becomes_a_more_results_link_and_is_sent_back_on_the_next_page() {
    let (base, fake, server) = start_fake_search(search_posts_body(Some("cursor-page-2"))).await;
    let router = router_with_search(AcceptingFlow, search_client(&base));
    let cookie = login(&router).await;
    let html = get_html(&router, "/search?q=minc", &cookie).await;
    assert!(
        html.contains(
            "<a class=\"pagination-link\" rel=\"next\" href=\"/search?q=minc&amp;type=posts&amp;cursor=cursor-page-2\">More results</a>"
        ),
        "the More results link must carry cursor, q and type: {html}"
    );
    let html = get_html(
        &router,
        "/search?q=minc&type=posts&cursor=cursor-page-2",
        &cookie,
    )
    .await;
    let requests = fake.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 2);
    assert!(
        requests[1].1.contains("cursor=cursor-page-2"),
        "the next page must forward the cursor upstream: {}",
        requests[1].1
    );
    assert!(
        html.contains("mincy post"),
        "the second page renders too: {html}"
    );
    server.abort();
}

// Added by claude-helper at review (2026-10-31): mutants M1, M5, M6 and M8 survived the lane's suite.

#[tokio::test]
async fn people_the_viewer_muted_are_hidden_from_people_search() {
    let actors = json!({"actors": [
        {"did": HIT_AUTHOR, "handle": "muted.test", "displayName": "Muted Person"},
        {"did": "did:plc:finder", "handle": "finder.test", "displayName": "Finder"}
    ]});
    let (base, _fake, server) = start_fake_search_with(search_posts_body(None), actors).await;
    let router = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow_and_shutdown_and_profile_counts_and_relationships_and_search(
            MutingFlow {
                muted: [HIT_AUTHOR.to_owned()].into_iter().collect(),
            },
            Arc::new(MemoryStorage::default()),
            WebShutdown::new(),
            ProfileCountsClient::local_only(),
            RelationshipClient::local_only(),
            search_client(&base),
        );
    let cookie = login(&router).await;
    let html = get_html(&router, "/search?q=per&type=people", &cookie).await;
    assert!(
        !html.contains("Muted Person") && !html.contains("@muted.test"),
        "a muted person must not be listed: {html}"
    );
    assert!(
        html.contains("@finder.test"),
        "the other person must stay: {html}"
    );
    server.abort();
}

#[tokio::test]
async fn people_search_escapes_names_and_descriptions_from_upstream() {
    let actors = json!({"actors": [{
        "did": "did:plc:finder",
        "handle": "finder.test",
        "displayName": "<b>Bold</b> Finder",
        "description": "<img src=x onerror=alert(1)> hi"
    }]});
    let (base, _fake, server) = start_fake_search_with(search_posts_body(None), actors).await;
    let router = router_with_search(AcceptingFlow, search_client(&base));
    let cookie = login(&router).await;
    let html = get_html(&router, "/search?q=fin&type=people", &cookie).await;
    assert!(
        !html.contains("<b>Bold</b>"),
        "a display name must not reach the page as markup: {html}"
    );
    assert!(
        html.contains("&lt;b&gt;Bold&lt;/b&gt; Finder"),
        "the display name must be shown escaped: {html}"
    );
    assert!(
        !html.contains("<img src=x"),
        "a description must not reach the page as markup: {html}"
    );
    assert!(
        html.contains("&lt;img src=x onerror=alert(1)&gt; hi"),
        "the description must be shown escaped: {html}"
    );
    server.abort();
}

// Added by lane av64q: anonymous searchPosts answers 403 to any request that carries a
// cursor, so posts search must run through the viewer's own signed-in session (owner,
// msgd 2026-10-31).

type SignedSearchCall = (String, String, Option<String>);

#[derive(Clone)]
struct SignedSearchFlow {
    body: Arc<Mutex<Value>>,
    calls: Arc<Mutex<Vec<SignedSearchCall>>>,
}

impl SignedSearchFlow {
    fn new(body: Value) -> Self {
        Self {
            body: Arc::new(Mutex::new(body)),
            calls: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl OAuthFlow for SignedSearchFlow {
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
                did: ALICE.to_owned(),
                verified_handle: Some("alice.test".to_owned()),
                granted_scope: Some(WRITE_SCOPE.to_owned()),
            })
        })
    }
    fn logout<'a>(
        &'a self,
        _: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }
    fn signed_search_posts<'a>(
        &'a self,
        did: &'a str,
        query: &'a str,
        cursor: Option<&'a str>,
    ) -> Pin<Box<dyn Future<Output = Result<Option<Value>, String>> + Send + 'a>> {
        let calls = self.calls.clone();
        let body = self.body.lock().unwrap().clone();
        let did = did.to_owned();
        let query = query.to_owned();
        let cursor = cursor.map(str::to_owned);
        Box::pin(async move {
            calls.lock().unwrap().push((did, query, cursor));
            Ok(Some(body))
        })
    }
}

fn signed_search_flow(body: Value) -> SignedSearchFlow {
    SignedSearchFlow::new(body)
}

#[tokio::test]
async fn posts_search_uses_the_signed_search_and_never_calls_the_anonymous_api() {
    let signed_body = json!({
        "posts": [{
            "uri": HIT_URI,
            "cid": "hit1-cid",
            "author": {
                "did": "did:plc:searched",
                "handle": "searched.test",
                "displayName": "Searched"
            },
            "record": {
                "$type": "app.bsky.feed.post",
                "text": "signed mincy post",
                "createdAt": "2026-10-31T12:00:00.000Z"
            },
            "indexedAt": "2026-10-31T12:00:01.000Z"
        }],
        "cursor": "signed-cursor-2"
    });
    let (base, fake, server) = start_fake_search(search_posts_body(None)).await;
    let flow = signed_search_flow(signed_body);
    let router = router_with_any_search(flow.clone(), search_client(&base));
    let cookie = login(&router).await;
    let html = get_html(&router, "/search?q=minc", &cookie).await;
    assert_eq!(
        fake.calls.load(Ordering::SeqCst),
        0,
        "a signed answer must not spend the anonymous call"
    );
    let calls = flow.calls.lock().unwrap().clone();
    assert_eq!(
        calls,
        vec![(ALICE.to_owned(), "minc".to_owned(), None)],
        "the flow must get the viewer DID and the query"
    );
    assert!(
        html.contains("signed mincy post"),
        "the signed search's posts must render: {html}"
    );
    assert!(
        html.contains(
            "<a class=\"pagination-link\" rel=\"next\" href=\"/search?q=minc&amp;type=posts&amp;cursor=signed-cursor-2\">More results</a>"
        ),
        "the signed response's cursor must drive the More results link: {html}"
    );
    server.abort();
}

#[tokio::test]
async fn the_page_cursor_is_forwarded_to_the_signed_search() {
    let signed_body = json!({"posts": [], "cursor": null});
    let (base, fake, server) = start_fake_search(search_posts_body(None)).await;
    let flow = signed_search_flow(signed_body);
    let router = router_with_any_search(flow.clone(), search_client(&base));
    let cookie = login(&router).await;
    let html = get_html(
        &router,
        "/search?q=minc&type=posts&cursor=cursor-page-2",
        &cookie,
    )
    .await;
    assert_eq!(
        fake.calls.load(Ordering::SeqCst),
        0,
        "the anonymous endpoint must stay untouched once the flow can answer"
    );
    let calls = flow.calls.lock().unwrap().clone();
    assert_eq!(
        calls,
        vec![(
            ALICE.to_owned(),
            "minc".to_owned(),
            Some("cursor-page-2".to_owned())
        )],
        "the cursor from /search?q=..&cursor=X must reach the flow: {calls:?}"
    );
    assert!(
        html.contains("empty-state"),
        "the empty signed answer still renders the page: {html}"
    );
    server.abort();
}

#[tokio::test]
async fn a_signed_search_failure_warns_with_the_real_error_and_falls_back_to_anonymous() {
    // Owner's logging rule: the fallback must name the real underlying error, never a bare
    // "signed search failed".
    #[derive(Clone)]
    struct SignedFailingFlow;
    impl OAuthFlow for SignedFailingFlow {
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
                    did: ALICE.to_owned(),
                    verified_handle: Some("alice.test".to_owned()),
                    granted_scope: Some(WRITE_SCOPE.to_owned()),
                })
            })
        }
        fn logout<'a>(
            &'a self,
            _: &'a str,
        ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
            Box::pin(async { Ok(()) })
        }
        fn signed_search_posts<'a>(
            &'a self,
            _: &'a str,
            _: &'a str,
            _: Option<&'a str>,
        ) -> Pin<Box<dyn Future<Output = Result<Option<Value>, String>> + Send + 'a>> {
            Box::pin(async {
                Err(
                    "Bluesky searchPosts request failed: InvalidResponseHttp(403, \"Request forbidden by administrative rules\")"
                        .to_owned(),
                )
            })
        }
    }
    let (base, fake, server) = start_fake_search(search_posts_body(None)).await;
    let router = router_with_any_search(SignedFailingFlow, search_client(&base));
    let cookie = login(&router).await;
    let warnings = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry().with(WarningCapture(warnings.clone()));
    let _guard = tracing::subscriber::set_default(subscriber);
    let html = get_html(&router, "/search?q=minc", &cookie).await;
    drop(_guard);
    assert_eq!(
        fake.calls.load(Ordering::SeqCst),
        1,
        "a failed signed search must spend exactly one anonymous call"
    );
    assert!(
        html.contains("mincy post"),
        "the anonymous posts must still render: {html}"
    );
    let warnings = warnings.lock().unwrap();
    assert!(
        warnings
            .iter()
            .any(|warning| warning.contains("Request forbidden by administrative rules")),
        "the warning must name the real underlying error: {warnings:?}"
    );
    server.abort();
}

#[tokio::test]
async fn a_403_on_an_anonymous_cursor_page_asks_for_a_fresh_sign_in_instead_of_the_error_page() {
    let (base, fake, server) = start_fake_search(search_posts_body(Some("cursor-page-2"))).await;
    fake.cursor_forbidden.store(true, Ordering::SeqCst);
    let router = router_with_search(AcceptingFlow, search_client(&base));
    let cookie = login(&router).await;
    let first = get_html(&router, "/search?q=minc", &cookie).await;
    assert!(
        first.contains("More results"),
        "page 1 still offers the next page: {first}"
    );
    let warnings = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::registry().with(WarningCapture(warnings.clone()));
    let _guard = tracing::subscriber::set_default(subscriber);
    let html = get_html(
        &router,
        "/search?q=minc&type=posts&cursor=cursor-page-2",
        &cookie,
    )
    .await;
    drop(_guard);
    assert!(
        html.contains("More results need a fresh sign-in"),
        "the cursor page must ask for a fresh sign-in: {html}"
    );
    assert!(
        !html.contains("Search is unavailable"),
        "this is not the unavailable page: {html}"
    );
    assert!(
        html.contains("href=\"/login?allow=like&amp;allow=repost\""),
        "the sign-in link must keep the boxes the session already holds: {html}"
    );
    let warnings = warnings.lock().unwrap();
    assert!(
        warnings.iter().any(|warning| {
            warning.contains("status=403")
                && warning.contains("/xrpc/app.bsky.feed.searchPosts")
                && warning.contains("cursor=cursor-page-2")
        }),
        "the warning must carry the status and the cursor URL: {warnings:?}"
    );
    server.abort();
}

#[tokio::test]
async fn a_403_on_the_first_posts_page_is_the_unavailable_page_not_a_sign_in_request() {
    // Only a cursor request is the paging refusal; a 403 on page 1 has another cause, and a
    // fresh sign-in would not fix it, so it gets the ordinary unavailable page with its status.
    let (base, fake, server) = start_fake_search(search_posts_body(None)).await;
    fake.all_forbidden.store(true, Ordering::SeqCst);
    let router = router_with_search(AcceptingFlow, search_client(&base));
    let cookie = login(&router).await;
    let html = get_html(&router, "/search?q=minc&type=posts", &cookie).await;
    assert!(
        html.contains("Search is unavailable right now (status 403)"),
        "a first-page 403 shows the unavailable page with its status: {html}"
    );
    assert!(
        !html.contains("More results need a fresh sign-in"),
        "page 1 is not a paging refusal: {html}"
    );
    server.abort();
}

#[tokio::test]
async fn a_blank_query_shows_the_form_and_makes_no_outbound_call() {
    let (base, fake, server) = start_fake_search(search_posts_body(None)).await;
    let router = router_with_search(AcceptingFlow, search_client(&base));
    let cookie = login(&router).await;
    for uri in ["/search?q=", "/search?q=%20%20", "/search?q=&type=people"] {
        let html = get_html(&router, uri, &cookie).await;
        assert!(html.contains("name=\"q\""), "{uri}: form missing: {html}");
    }
    assert_eq!(fake.calls.load(Ordering::SeqCst), 0);
    server.abort();
}
