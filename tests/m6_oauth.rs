use appview_rs::router;
use appview_rs::{
    config::Config,
    feeds::ResolvedFeed,
    ingest::{IndexedRecord, Ingestor, MemoryStorage, ScopeMode, SqliteStorage, Storage},
    oauth::{
        FetchedAuthorProfile, OAuthAccount, OAuthCallback, OAuthFlow, OAuthPublic,
        PdsCreatedRecord, PostStrongRef, SqliteOAuthStore,
    },
};
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

const EXPECTED_OAUTH_SCOPE: &str = "atproto repo:app.bsky.feed.post?action=create repo:app.bsky.feed.like?action=create&action=delete repo:app.bsky.feed.repost?action=create&action=delete rpc:app.bsky.actor.getPreferences?aud=* rpc:app.bsky.feed.getFeedSkeleton?aud=*";

#[derive(Clone)]
struct FakeOAuthFlow {
    actions: Arc<Mutex<Vec<String>>>,
    granted_scope: Option<String>,
    browser_binding: Arc<Mutex<Option<String>>>,
    callback_failure: Option<String>,
    action_failure: Option<String>,
    pinned_feeds: Vec<ResolvedFeed>,
    author_profiles: Arc<Mutex<std::collections::HashMap<String, FetchedAuthorProfile>>>,
    profile_requests: Arc<Mutex<Vec<String>>>,
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
            author_profiles: Arc::new(Mutex::new(std::collections::HashMap::new())),
            profile_requests: Arc::new(Mutex::new(Vec::new())),
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

    fn fetch_author_profile<'a>(
        &'a self,
        did: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<FetchedAuthorProfile, String>> + Send + 'a>> {
        Box::pin(async move {
            self.profile_requests.lock().unwrap().push(did.to_owned());
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
        Box::pin(async move { Ok(feeds) })
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
                "<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">"
            ),
            "{path}"
        );
        assert!(html.contains("href=\"/static/app.css?v=av19\""), "{path}");
        assert!(!html.contains("style="), "{path}");
        assert!(!html.contains("<script"), "{path}");
    }

    let stylesheet = app
        .oneshot(
            Request::builder()
                .uri("/static/app.css?v=av19")
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
}

#[tokio::test]
async fn quote_composer_shows_post_and_quote_write_is_indexed_with_one_notification() {
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
                "createdAt":"2026-10-28T00:00:00Z"
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
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
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
        html.contains("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">")
    );
    assert!(html.contains("href=\"/static/app.css?v=av19\""));
    assert!(!html.contains("style="));
    assert!(!html.contains("<script"));
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
    assert!(html.contains("https://cdn.bsky.app/img/avatar/plain/did:plc:alice/bafk-alice-avatar"));
    assert!(html.contains("Alice"));
    assert!(html.contains("@alice.test"));
    assert!(html.contains("datetime=\"2020-01-01T00:00:00Z\""));
    assert!(html.contains("ago</span>"));
    assert!(html.contains("aria-label=\"Open replies\""));
    assert!(html.contains("aria-label=\"Like post by @alice.test\""));
    assert!(html.contains("aria-label=\"Repost by @alice.test\""));
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
    // A phone keyboard sent "Ayourtch.bsky.social"; atproto handles are case-insensitive but the
    // DID-document check downstream compares them exactly, so sign-in failed (2026-10-28).
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
async fn callback_turns_an_upstream_panic_into_an_error_response() {
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
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
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

    for (path, body) in [
        ("/post", "text=hello+world"),
        (
            "/like",
            "uri=at%3A%2F%2Fdid%3Aplc%3Aauthor%2Fapp.bsky.feed.post%2Fr1&cid=bafyreifake",
        ),
        (
            "/repost",
            "uri=at%3A%2F%2Fdid%3Aplc%3Aauthor%2Fapp.bsky.feed.post%2Fr1&cid=bafyreifake",
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
        assert_eq!(response.headers()["location"], "/app");
    }
    assert_eq!(
        *actions.lock().unwrap(),
        [
            "post:did:plc:alice:hello world",
            "like:did:plc:alice:at://did:plc:author/app.bsky.feed.post/r1:bafyreifake",
            "repost:did:plc:alice:at://did:plc:author/app.bsky.feed.post/r1:bafyreifake",
        ]
    );
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
    assert!(
        html.contains("Liked<span class=\"action-count\">1</span>"),
        "{html}"
    );

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
    assert!(html.contains("replying to @friend.test"), "{html}");
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
    assert!(html.contains("replying to @friend.test"), "{html}");
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
            "src=\"https://cdn.bsky.app/img/avatar/plain/{author}/avatar-cid\""
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
            time_us: 4,
            record: serde_json::json!({
                "text":"first chain reply",
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
                        "description":"Card description"
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
