use appview_rs::router;
use appview_rs::{
    config::Config,
    ingest::{IndexedRecord, MemoryStorage, SqliteStorage, Storage},
    oauth::{OAuthAccount, OAuthCallback, OAuthFlow, OAuthPublic, SqliteOAuthStore},
};
use atrium_api::types::string::Did;
use atrium_common::store::Store;
use atrium_oauth::store::{session::Session, state::InternalStateData};
use axum::{Router, body::Body, http::Request, http::StatusCode};
use http_body_util::BodyExt;
use serde_json::Value;
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};
use tower::ServiceExt;

const EXPECTED_OAUTH_SCOPE: &str = "atproto repo:app.bsky.feed.post?action=create repo:app.bsky.feed.like?action=create&action=delete repo:app.bsky.feed.repost?action=create&action=delete";

#[derive(Clone)]
struct FakeOAuthFlow {
    actions: Arc<Mutex<Vec<String>>>,
    granted_scope: Option<String>,
    browser_binding: Arc<Mutex<Option<String>>>,
}

impl Default for FakeOAuthFlow {
    fn default() -> Self {
        Self {
            actions: Arc::new(Mutex::new(Vec::new())),
            granted_scope: Some(EXPECTED_OAUTH_SCOPE.to_owned()),
            browser_binding: Arc::new(Mutex::new(None)),
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
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move {
            self.actions
                .lock()
                .unwrap()
                .push(format!("post:{did}:{text}"));
            Ok(())
        })
    }

    fn like<'a>(
        &'a self,
        did: &'a str,
        uri: &'a str,
        cid: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move {
            self.actions
                .lock()
                .unwrap()
                .push(format!("like:{did}:{uri}:{cid}"));
            Ok(())
        })
    }

    fn repost<'a>(
        &'a self,
        did: &'a str,
        uri: &'a str,
        cid: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async move {
            self.actions
                .lock()
                .unwrap()
                .push(format!("repost:{did}:{uri}:{cid}"));
            Ok(())
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
    assert!(!storage.is_registered("did:plc:alice").unwrap());
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
    for path in ["/post", "/like", "/repost"] {
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
    assert!(thread_html.contains("2026-10-27T09:15:00Z"));
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
