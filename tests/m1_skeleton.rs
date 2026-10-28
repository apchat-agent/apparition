use appview_rs::{config::Config, router};
use axum::{body::Body, http::Request};
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
    let response = router()
        .oneshot(
            Request::builder()
                .uri("/xrpc/_health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
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
    assert_eq!(&body[..], br#"{"status":"ok"}"#);
}
