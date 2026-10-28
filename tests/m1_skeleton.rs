use appview_rs::{
    config::Config,
    ingest::MemoryStorage,
    oauth::{DisabledOAuthFlow, OAuthPublic},
    router, router_with_oauth_flow,
};
use axum::{body::Body, extract::ConnectInfo, http::Request};
use http_body_util::BodyExt;
use tower::ServiceExt;

#[test]
fn config_loads_toml_and_environment_overrides() {
    let config = Config::from_toml_and_env(
        r#"
            listen = "127.0.0.1:9000"
            data_dir = "./store"
            log_filter = "debug"
        "#,
        [("APPVIEW_LISTEN", "0.0.0.0:7777")],
    )
    .unwrap();

    assert_eq!(config.listen, "0.0.0.0:7777");
    assert_eq!(config.data_dir, "./store");
    assert_eq!(config.log_filter, "debug");
}

#[tokio::test]
async fn health_endpoint_returns_json_ok() {
    let app = router();
    for path in ["/xrpc/_health", "/healthz"] {
        let response = app
            .clone()
            .oneshot(Request::builder().uri(path).body(Body::empty()).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), 200, "{path}");
        assert_eq!(response.headers()["x-content-type-options"], "nosniff");
        assert_eq!(response.headers()["referrer-policy"], "no-referrer");
        let csp = response.headers()["content-security-policy"]
            .to_str()
            .unwrap();
        assert!(csp.contains("script-src 'none'"));
        assert!(csp.contains("frame-ancestors 'none'"));
        assert!(
            response.headers()["strict-transport-security"]
                .to_str()
                .unwrap()
                .contains("max-age=")
        );
        let body = response.into_body().collect().await.unwrap().to_bytes();
        assert_eq!(&body[..], br#"{"status":"ok"}"#, "{path}");
    }
}

#[tokio::test]
async fn eleventh_login_attempt_from_one_ip_returns_429() {
    let app = router_with_oauth_flow(
        OAuthPublic::new("https://appview.example").unwrap(),
        DisabledOAuthFlow,
        std::sync::Arc::new(MemoryStorage::default()),
    );
    let peer: std::net::SocketAddr = "198.51.100.78:43211".parse().unwrap();
    for attempt in 0..11 {
        let mut request = Request::builder()
            .method("POST")
            .uri("/oauth/authorize")
            .header("origin", "https://appview.example")
            .header("sec-fetch-site", "same-origin")
            .header("content-type", "application/x-www-form-urlencoded")
            .body(Body::from("handle=alice.test"))
            .unwrap();
        request.extensions_mut().insert(ConnectInfo(peer));
        let response = app.clone().oneshot(request).await.unwrap();
        assert_eq!(
            response.status().as_u16(),
            if attempt < 10 { 400 } else { 429 }
        );
    }
}

#[tokio::test]
async fn oversized_login_form_body_returns_413() {
    let app = router_with_oauth_flow(
        OAuthPublic::new("https://appview.example").unwrap(),
        DisabledOAuthFlow,
        std::sync::Arc::new(MemoryStorage::default()),
    );
    let body = format!("handle={}", "a".repeat(40_000));
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/oauth/authorize")
                .header("origin", "https://appview.example")
                .header("sec-fetch-site", "same-origin")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status().as_u16(), 413);
}
