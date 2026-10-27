use appview_rs::router;
use appview_rs::{
    config::Config,
    ingest::{MemoryStorage, Storage},
    oauth::{OAuthAccount, OAuthCallback, OAuthFlow, OAuthPublic, SqliteOAuthStore},
};
use atrium_api::types::string::Did;
use atrium_common::store::Store;
use atrium_oauth::store::{session::Session, state::InternalStateData};
use axum::{body::Body, http::Request, http::StatusCode};
use http_body_util::BodyExt;
use serde_json::Value;
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
};
use tower::ServiceExt;

#[derive(Clone)]
struct FakeOAuthFlow {
    actions: Arc<Mutex<Vec<String>>>,
    granted_scope: Option<String>,
}

impl Default for FakeOAuthFlow {
    fn default() -> Self {
        Self {
            actions: Arc::new(Mutex::new(Vec::new())),
            granted_scope: Some("atproto".to_owned()),
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
    ) -> Pin<Box<dyn Future<Output = Result<String, String>> + Send + 'a>> {
        Box::pin(async move { Ok(format!("https://auth.example/authorize?login={login}")) })
    }

    fn callback<'a>(
        &'a self,
        params: OAuthCallback,
    ) -> Pin<Box<dyn Future<Output = Result<OAuthAccount, String>> + Send + 'a>> {
        Box::pin(async move {
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
        Box::pin(async {
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
    assert!(
        metadata["scope"]
            .as_str()
            .unwrap()
            .split_whitespace()
            .any(|scope| scope == "atproto")
    );
    assert!(!metadata["jwks"]["keys"].as_array().unwrap().is_empty());
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
}

#[tokio::test]
async fn authorization_and_callback_reject_missing_protocol_parameters() {
    for path in ["/oauth/authorize", "/oauth/callback"] {
        let response = router()
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "{path}");
    }
}

#[tokio::test]
async fn oauth_callback_registers_account_and_logout_clears_its_session() {
    let public = OAuthPublic::new("https://appview.example").unwrap();
    let storage = Arc::new(MemoryStorage::default());
    let app = public.router_with_flow(FakeOAuthFlow::default(), storage.clone());

    let authorization = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/oauth/authorize?handle=alice.test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(authorization.status(), StatusCode::SEE_OTHER);
    assert_eq!(
        authorization.headers()["location"],
        "https://auth.example/authorize?login=alice.test"
    );

    let callback = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/oauth/callback?code=valid-code&state=valid-state&scope=atproto")
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
    assert_eq!(
        storage.verified_handle("did:plc:alice").unwrap().as_deref(),
        Some("alice.test")
    );

    let logout = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/logout")
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
    let response = app
        .oneshot(
            Request::builder()
                .uri("/oauth/callback?code=valid-code&state=valid-state&iss=https%3A%2F%2Fauth.example")
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
    let callback = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/oauth/callback?code=valid-code&state=valid-state&scope=atproto")
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
async fn logged_in_timeline_comes_from_the_oauth_account_pds() {
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(FakeOAuthFlow::default(), Arc::new(MemoryStorage::default()));
    let callback = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/oauth/callback?code=valid-code&state=valid-state&scope=atproto")
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
    let body = response.into_body().collect().await.unwrap().to_bytes();
    let html = String::from_utf8(body.to_vec()).unwrap();
    assert!(html.contains("from OAuth timeline"));
    assert!(html.contains("/like"));
    assert!(html.contains("/repost"));
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
    let response = app
        .oneshot(
            Request::builder()
                .uri("/oauth/callback?code=valid-code&state=valid-state&scope=profile")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    assert!(!storage.is_registered("did:plc:alice").unwrap());
}
