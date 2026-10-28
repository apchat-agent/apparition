use axum::{
    Router,
    body::Body,
    extract::{ConnectInfo, State},
    http::{Request, Response, StatusCode, header},
    middleware::{Next, from_fn_with_state},
    response::IntoResponse,
};
use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    sync::{Mutex, OnceLock},
    time::{Duration, Instant},
};
use tower_http::limit::RequestBodyLimitLayer;

const MAX_URI_BYTES: usize = 8 * 1024;
const MAX_REQUEST_BODY_BYTES: usize = 32 * 1024;
const WINDOW: Duration = Duration::from_secs(60);
const MAX_BUCKETS: usize = 4096;

type BucketKey = (String, &'static str);

#[derive(Default)]
struct RateState {
    buckets: HashMap<BucketKey, VecDeque<Instant>>,
}

static RATE_STATE: OnceLock<Mutex<RateState>> = OnceLock::new();

pub fn protect(router: Router, public_https: bool) -> Router {
    router
        .layer(RequestBodyLimitLayer::new(MAX_REQUEST_BODY_BYTES))
        .layer(from_fn_with_state(public_https, apply))
}

async fn apply(
    State(public_https): State<bool>,
    request: Request<Body>,
    next: Next,
) -> Response<Body> {
    if request.uri().to_string().len() > MAX_URI_BYTES {
        return with_headers(
            (StatusCode::URI_TOO_LONG, "request target is too long").into_response(),
            public_https,
        );
    }
    if let Some(ConnectInfo(peer)) = request.extensions().get::<ConnectInfo<SocketAddr>>()
        && !request_allowed(
            request.uri().path(),
            request.headers().contains_key(header::AUTHORIZATION),
            peer.ip().to_string(),
        )
    {
        return with_headers(
            (StatusCode::TOO_MANY_REQUESTS, "rate limit exceeded").into_response(),
            public_https,
        );
    }
    with_headers(next.run(request).await, public_https)
}

fn request_allowed(path: &str, has_authorization: bool, ip: String) -> bool {
    if !take_rate_slot(("global".to_owned(), "all"), 1000) {
        return false;
    }
    if path == "/oauth/authorize" {
        return take_rate_slot(("global".to_owned(), "login"), 100)
            && take_rate_slot((ip, "login"), 10);
    }
    if matches!(path, "/post" | "/like" | "/repost" | "/logout") {
        return take_rate_slot((ip, "write"), 30)
            && take_rate_slot(("global".to_owned(), "outbound"), 300);
    }
    if path == "/oauth/callback" || (path.starts_with("/xrpc/") && has_authorization) {
        return take_rate_slot((ip, "outbound"), 30)
            && take_rate_slot(("global".to_owned(), "outbound"), 300);
    }
    if path.starts_with("/xrpc/app.bsky.") {
        return take_rate_slot((ip, "xrpc-read"), 120);
    }
    take_rate_slot((ip, "web-read"), 120)
}

fn take_rate_slot(key: BucketKey, limit: usize) -> bool {
    let state = RATE_STATE.get_or_init(|| Mutex::new(RateState::default()));
    let Ok(mut state) = state.lock() else {
        return false;
    };
    take_rate_slot_with_state(&mut state, key, limit, Instant::now())
}

fn take_rate_slot_with_state(
    state: &mut RateState,
    key: BucketKey,
    limit: usize,
    now: Instant,
) -> bool {
    if !state.buckets.contains_key(&key) && state.buckets.len() >= MAX_BUCKETS {
        state.buckets.retain(|_, bucket| {
            bucket
                .back()
                .is_some_and(|last| now.saturating_duration_since(*last) < WINDOW)
        });
        if state.buckets.len() >= MAX_BUCKETS {
            return false;
        }
    }
    let bucket = state.buckets.entry(key).or_default();
    while bucket
        .front()
        .is_some_and(|at| now.duration_since(*at) >= WINDOW)
    {
        bucket.pop_front();
    }
    if bucket.len() >= limit {
        return false;
    }
    bucket.push_back(now);
    true
}

fn with_headers(mut response: Response<Body>, public_https: bool) -> Response<Body> {
    let headers = response.headers_mut();
    headers.insert(header::X_CONTENT_TYPE_OPTIONS, "nosniff".parse().unwrap());
    headers.insert(header::REFERRER_POLICY, "no-referrer".parse().unwrap());
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        "default-src 'none'; base-uri 'none'; form-action 'self'; img-src 'self' https://cdn.bsky.app; style-src 'self'; script-src 'none'; connect-src 'self'; object-src 'none'; frame-ancestors 'none'"
            .parse()
            .unwrap(),
    );
    headers.insert(header::X_FRAME_OPTIONS, "DENY".parse().unwrap());
    if public_https {
        headers.insert(
            header::STRICT_TRANSPORT_SECURITY,
            "max-age=31536000; includeSubDomains".parse().unwrap(),
        );
    }
    response
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_BUCKETS, MAX_REQUEST_BODY_BYTES, RateState, protect, take_rate_slot_with_state,
    };
    use axum::{
        body::{Body, Bytes},
        extract::ConnectInfo,
        http::Request,
        routing::post,
    };
    use std::net::SocketAddr;
    use std::time::Instant;
    use tower::ServiceExt;

    #[tokio::test]
    async fn eleventh_login_attempt_from_one_ip_is_rate_limited() {
        let router = protect(
            axum::Router::new().route("/oauth/authorize", post(|| async { "ok" })),
            true,
        );
        let peer: SocketAddr = "198.51.100.77:43210".parse().unwrap();
        for attempt in 0..11 {
            let mut request = Request::builder()
                .method("POST")
                .uri("/oauth/authorize")
                .body(Body::empty())
                .unwrap();
            request.extensions_mut().insert(ConnectInfo(peer));
            let response = router.clone().oneshot(request).await.unwrap();
            assert_eq!(
                response.status().as_u16(),
                if attempt < 10 { 200 } else { 429 }
            );
        }
    }

    #[tokio::test]
    async fn oversized_request_body_is_rejected() {
        let router = protect(
            axum::Router::new().route("/post", post(|_: Bytes| async { "accepted" })),
            false,
        );
        let response = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/post")
                    .body(Body::from(vec![b'x'; MAX_REQUEST_BODY_BYTES + 1]))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), 413);
    }

    #[tokio::test]
    async fn oversized_content_length_is_rejected_even_when_the_route_ignores_body() {
        let router = protect(
            axum::Router::new().route("/logout", post(|| async { "accepted" })),
            false,
        );
        let response = router
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/logout")
                    .header("content-length", (MAX_REQUEST_BODY_BYTES + 1).to_string())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), 413);
    }

    #[test]
    fn active_rate_buckets_are_preserved_when_the_bucket_map_is_full() {
        let mut state = RateState::default();
        let now = Instant::now();
        for index in 0..MAX_BUCKETS {
            assert!(take_rate_slot_with_state(
                &mut state,
                (format!("active-{index}"), "read"),
                1,
                now,
            ));
        }
        let before = state
            .buckets
            .keys()
            .cloned()
            .collect::<std::collections::HashSet<_>>();

        assert!(!take_rate_slot_with_state(
            &mut state,
            ("new-peer".to_owned(), "read"),
            1,
            now,
        ));
        assert_eq!(
            state
                .buckets
                .keys()
                .cloned()
                .collect::<std::collections::HashSet<_>>(),
            before
        );
    }
}
