use appview_rs::{
    ingest::MemoryStorage,
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
        atomic::{AtomicUsize, Ordering},
    },
};
use tokio::net::TcpListener;
use tower::ServiceExt;

const ALICE: &str = "did:plc:alice";
const HIT_URI: &str = "at://did:plc:searched/app.bsky.feed.post/hit1";
const WRITE_SCOPE: &str = "atproto repo:app.bsky.feed.post?action=create repo:app.bsky.feed.like?action=create&action=delete repo:app.bsky.feed.repost?action=create&action=delete";

#[derive(Clone)]
struct FakeSearch {
    calls: Arc<AtomicUsize>,
    requests: Arc<Mutex<Vec<(String, String)>>>,
    posts_body: Arc<Mutex<Value>>,
    actors_body: Arc<Mutex<Value>>,
}

impl FakeSearch {
    fn posts_body(&self) -> Value {
        self.posts_body.lock().unwrap().clone()
    }
}

async fn fake_search_posts(
    State(fake): State<FakeSearch>,
    RawQuery(query): RawQuery,
) -> Response<Body> {
    fake.record_call("/xrpc/app.bsky.feed.searchPosts", query);
    axum::Json(fake.posts_body()).into_response()
}

async fn fake_search_actors(
    State(fake): State<FakeSearch>,
    RawQuery(query): RawQuery,
) -> Response<Body> {
    fake.record_call("/xrpc/app.bsky.actor.searchActors", query);
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
    json!({
        "posts": [{
            "post": {
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
            }
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
    let (base, fake, server) =
        start_fake_search_with(search_posts_body(None), actors).await;
    let router = router_with_search(AcceptingFlow, search_client(&base));
    let cookie = login(&router).await;
    let html = get_html(&router, "/search?q=fin&type=people", &cookie).await;
    let requests = fake.requests.lock().unwrap().clone();
    assert_eq!(requests.len(), 1, "people search must hit the people endpoint once");
    assert_eq!(requests[0].0, "/xrpc/app.bsky.actor.searchActors");
    assert!(requests[0].1.contains("q=fin"), "{}", requests[0].1);
    assert!(requests[0].1.contains("limit=25"), "{}", requests[0].1);
    assert!(html.contains("Finder"), "display name missing: {html}");
    assert!(html.contains("@finder.test"), "handle missing: {html}");
    assert!(html.contains("First line."), "description first line missing: {html}");
    assert!(
        !html.contains("Second line"),
        "only the description's first line may show: {html}"
    );
    assert!(
        html.contains("/profile?did=did%3Aplc%3Afinder"),
        "the row must link to the app's profile page: {html}"
    );
    assert!(
        html.contains("https://cdn.bsky.app/img/avatar_thumbnail/plain/did:plc:finder/bafkfinder@jpeg"),
        "the row must show the avatar: {html}"
    );
    server.abort();
}
