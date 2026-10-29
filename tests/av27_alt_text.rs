use appview_rs::{
    ingest::MemoryStorage,
    oauth::{DisabledOAuthFlow, OAuthPublic},
    router_with_oauth_flow,
};
use axum::{body::Body, http::Request, http::StatusCode};
use tower::ServiceExt;

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
        "default-src 'none'; base-uri 'none'; form-action 'self' https:; img-src 'self' https://cdn.bsky.app; style-src 'self'; script-src 'self' 'wasm-unsafe-eval'; connect-src 'self'; object-src 'none'; frame-ancestors 'none'"
    );
}
