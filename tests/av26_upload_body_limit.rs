use appview_rs::{
    ingest::MemoryStorage,
    oauth::{DisabledOAuthFlow, OAuthPublic},
    router_with_oauth_flow,
};
use axum::{body::Body, http::Request, http::StatusCode};
use std::sync::Arc;
use tower::ServiceExt;

#[tokio::test]
async fn protected_web_router_accepts_image_forms_over_the_default_body_limit() {
    let app = router_with_oauth_flow(
        OAuthPublic::new("https://appview.example").unwrap(),
        DisabledOAuthFlow,
        Arc::new(MemoryStorage::default()),
    );
    let body = vec![b'x'; 4 * 1024 * 1024];
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/post")
                .header("origin", "https://appview.example")
                .header("sec-fetch-site", "same-origin")
                .header("content-type", "multipart/form-data; boundary=unused")
                .header("content-length", body.len().to_string())
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
}
