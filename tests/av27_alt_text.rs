use appview_rs::{
    ingest::MemoryStorage,
    oauth::{DisabledOAuthFlow, OAuthPublic},
    router_with_oauth_flow, router_with_oauth_flow_and_alt_model_dir,
};
use axum::{body::Body, http::Request, http::StatusCode};
use http_body_util::BodyExt;
use std::{
    fs,
    path::PathBuf,
    time::{SystemTime, UNIX_EPOCH},
};
use tower::ServiceExt;

fn temporary_model_directory() -> PathBuf {
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "appview-av27-models-{}-{nonce}",
        std::process::id()
    ))
}

#[tokio::test]
async fn alt_test_requires_a_logged_in_session() {
    let app = router_with_oauth_flow(
        OAuthPublic::new("https://appview.example").unwrap(),
        DisabledOAuthFlow,
        std::sync::Arc::new(MemoryStorage::default()),
    );
    let response = app
        .oneshot(
            Request::builder()
                .uri("/alt-test")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    assert_eq!(response.headers()["location"], "/login");
}

#[tokio::test]
async fn model_route_is_not_found_when_model_directory_is_unconfigured() {
    let response = router_with_oauth_flow(
        OAuthPublic::new("https://appview.example").unwrap(),
        DisabledOAuthFlow,
        std::sync::Arc::new(MemoryStorage::default()),
    )
    .oneshot(
        Request::builder()
            .uri("/static/models/alt-caption-v1/config.json")
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();

    assert_eq!(response.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn model_route_serves_files_with_length_type_and_immutable_cache_headers() {
    let root = temporary_model_directory();
    let model = root.join("alt-caption-v1");
    fs::create_dir_all(&model).unwrap();
    let content = b"{\"model_type\":\"smolvlm\"}";
    fs::write(model.join("config.json"), content).unwrap();
    let app = router_with_oauth_flow_and_alt_model_dir(
        OAuthPublic::new("https://appview.example").unwrap(),
        DisabledOAuthFlow,
        std::sync::Arc::new(MemoryStorage::default()),
        Some(root.clone()),
    );

    let response = app
        .oneshot(
            Request::builder()
                .uri("/static/models/alt-caption-v1/config.json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(response.headers()["content-type"], "application/json");
    assert_eq!(
        response.headers()["content-length"],
        content.len().to_string()
    );
    assert!(
        response.headers()["cache-control"]
            .to_str()
            .unwrap()
            .contains("immutable")
    );
    assert_eq!(
        response.into_body().collect().await.unwrap().to_bytes(),
        &content[..]
    );
    fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn model_route_refuses_encoded_parent_directory_traversal() {
    let root = temporary_model_directory();
    let model = root.join("alt-caption-v1");
    fs::create_dir_all(&model).unwrap();
    fs::write(root.join("secret.json"), b"secret").unwrap();
    let app = router_with_oauth_flow_and_alt_model_dir(
        OAuthPublic::new("https://appview.example").unwrap(),
        DisabledOAuthFlow,
        std::sync::Arc::new(MemoryStorage::default()),
        Some(root.clone()),
    );

    let response = app
        .oneshot(
            Request::builder()
                .uri("/static/models/alt-caption-v1/%2e%2e/secret.json")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_ne!(response.status(), StatusCode::OK);
    fs::remove_dir_all(root).unwrap();
}

#[tokio::test]
async fn content_security_policy_allows_only_same_origin_scripts_and_wasm_compilation() {
    let response = router_with_oauth_flow(
        OAuthPublic::new("https://appview.example").unwrap(),
        DisabledOAuthFlow,
        std::sync::Arc::new(MemoryStorage::default()),
    )
    .oneshot(
        Request::builder()
            .uri("/healthz")
            .body(Body::empty())
            .unwrap(),
    )
    .await
    .unwrap();

    assert_eq!(
        response.headers()["content-security-policy"],
        "default-src 'none'; base-uri 'none'; form-action 'self' https:; img-src 'self' blob: https://cdn.bsky.app; style-src 'self'; script-src 'self' 'wasm-unsafe-eval'; connect-src 'self'; object-src 'none'; frame-ancestors 'none'"
    );
}
