use appview_rs::{
    ingest::MemoryStorage,
    oauth::{DisabledOAuthFlow, OAuthPublic},
    router_with_oauth_flow,
};
use axum::{body::Body, http::Request, http::StatusCode};
use std::sync::Arc;
use tower::ServiceExt;

#[tokio::test]
async fn picture_route_accepts_a_one_mib_multipart_body() {
    let app = router_with_oauth_flow(
        OAuthPublic::new("https://appview.example").unwrap(),
        DisabledOAuthFlow,
        Arc::new(MemoryStorage::default()),
    );
    let mut body = b"--image-boundary\r\nContent-Disposition: form-data; name=\"text\"\r\n\r\npicture\r\n--image-boundary\r\nContent-Disposition: form-data; name=\"images\"; filename=\"large.jpg\"\r\nContent-Type: image/jpeg\r\n\r\n".to_vec();
    let trailer = b"\r\n--image-boundary--\r\n";
    body.extend(std::iter::repeat(b'x').take(1024 * 1024 - body.len() - trailer.len()));
    body.extend_from_slice(trailer);
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

#[tokio::test]
async fn picture_route_rejects_bodies_over_five_mib() {
    let app = router_with_oauth_flow(
        OAuthPublic::new("https://appview.example").unwrap(),
        DisabledOAuthFlow,
        Arc::new(MemoryStorage::default()),
    );
    let body = vec![b'x'; 5 * 1024 * 1024 + 1];
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
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
}

#[tokio::test]
async fn non_picture_post_route_rejects_a_forty_kib_form() {
    let app = router_with_oauth_flow(
        OAuthPublic::new("https://appview.example").unwrap(),
        DisabledOAuthFlow,
        Arc::new(MemoryStorage::default()),
    );
    let body = format!("uri={}&cid=cid", "x".repeat(40_000));
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/like")
                .header("origin", "https://appview.example")
                .header("sec-fetch-site", "same-origin")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
}
