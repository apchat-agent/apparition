use axum::{
    Router,
    body::Body,
    extract::{ConnectInfo, State},
    http::{Request, Response, StatusCode, header},
    middleware::{Next, from_fn, from_fn_with_state},
    response::IntoResponse,
};
use http_body_util::Limited;
use std::{
    collections::{HashMap, VecDeque},
    net::SocketAddr,
    sync::{Mutex, OnceLock},
    time::{Duration, Instant},
};
use tower_http::{compression::CompressionLayer, limit::RequestBodyLimitLayer};

const MAX_URI_BYTES: usize = 8 * 1024;
const MAX_REQUEST_BODY_BYTES: usize = 32 * 1024;
const MAX_PICTURE_REQUEST_BODY_BYTES: usize = 5 * 1024 * 1024;
const MAX_VIDEO_REQUEST_BODY_BYTES: usize = 301 * 1024 * 1024;
const WINDOW: Duration = Duration::from_secs(60);
const MAX_BUCKETS: usize = 4096;

type BucketKey = (String, &'static str);

#[derive(Default)]
struct RateState {
    buckets: HashMap<BucketKey, VecDeque<Instant>>,
}

static RATE_STATE: OnceLock<Mutex<RateState>> = OnceLock::new();

pub fn protect(router: Router, public_https: bool) -> Router {
    protect_with_body_limit(router, public_https, MAX_REQUEST_BODY_BYTES)
}

pub fn protect_with_body_limit(
    router: Router,
    public_https: bool,
    max_request_body_bytes: usize,
) -> Router {
    router
        .layer(RequestBodyLimitLayer::new(max_request_body_bytes))
        .layer(CompressionLayer::new().gzip(true).br(true))
        .layer(from_fn_with_state(public_https, apply))
}

pub fn protect_with_picture_body_limit(router: Router, public_https: bool) -> Router {
    router
        .layer(from_fn(limit_picture_request_body))
        .layer(CompressionLayer::new().gzip(true).br(true))
        .layer(from_fn_with_state(public_https, apply))
}

async fn limit_picture_request_body(request: Request<Body>, next: Next) -> Response<Body> {
    let limit = if request.method() == http::Method::POST && request.uri().path() == "/video" {
        MAX_VIDEO_REQUEST_BODY_BYTES
    } else if request.method() == http::Method::POST
        && matches!(request.uri().path(), "/post" | "/reply" | "/quote")
    {
        MAX_PICTURE_REQUEST_BODY_BYTES
    } else {
        MAX_REQUEST_BODY_BYTES
    };
    if request
        .headers()
        .get(header::CONTENT_LENGTH)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<usize>().ok())
        .is_some_and(|length| length > limit)
    {
        return (StatusCode::PAYLOAD_TOO_LARGE, "request body is too large").into_response();
    }

    let (parts, body) = request.into_parts();
    let body = Body::new(Limited::new(body, limit));
    next.run(Request::from_parts(parts, body)).await
}

async fn apply(
    State(public_https): State<bool>,
    request: Request<Body>,
    next: Next,
) -> Response<Body> {
    let started = Instant::now();
    let method = request.method().to_string();
    let path = request.uri().path().to_owned();
    if request.uri().to_string().len() > MAX_URI_BYTES {
        let response = with_headers(
            (StatusCode::URI_TOO_LONG, "request target is too long").into_response(),
            public_https,
        );
        log_request(&method, &path, started, response.status());
        return response;
    }
    if let Some(ConnectInfo(peer)) = request.extensions().get::<ConnectInfo<SocketAddr>>()
        && !request_allowed(
            &path,
            request.headers().contains_key(header::AUTHORIZATION),
            peer.ip().to_string(),
        )
    {
        let response = with_headers(
            (StatusCode::TOO_MANY_REQUESTS, "rate limit exceeded").into_response(),
            public_https,
        );
        log_request(&method, &path, started, response.status());
        return response;
    }
    let response = with_headers(next.run(request).await, public_https);
    log_request(&method, &path, started, response.status());
    response
}

fn log_request(method: &str, path: &str, started: Instant, status: StatusCode) {
    let elapsed = started.elapsed();
    let duration_ms = elapsed.as_millis().min(u64::MAX as u128) as u64;
    tracing::info!(
        method,
        path,
        status = status.as_u16(),
        duration_ms,
        "HTTP request"
    );
    if elapsed > Duration::from_secs(2) {
        tracing::warn!(
            method,
            path,
            status = status.as_u16(),
            duration_ms,
            "HTTP request exceeded duration threshold"
        );
    }
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
    // Not "no-referrer": under it browsers send `Origin: null` on our own form POSTs, and the
    // same-origin check on /oauth/authorize then refuses every real login. "same-origin" still
    // sends nothing to other sites.
    headers.insert(header::REFERRER_POLICY, "same-origin".parse().unwrap());
    // form-action must allow https: because the sign-in form's answer is a redirect to the
    // user's authorization server (any origin), and Safari/Chrome apply form-action to that
    // redirect: with 'self' alone they stopped on our login page (Firefox doesn't, 2026-10-28).
    headers.insert(
        header::CONTENT_SECURITY_POLICY,
        "default-src 'none'; base-uri 'none'; form-action 'self' https:; img-src 'self' blob: https://cdn.bsky.app https://video.bsky.app https://video.cdn.bsky.app; media-src 'self' blob: https://video.bsky.app https://video.cdn.bsky.app; style-src 'self'; script-src 'self' 'wasm-unsafe-eval'; connect-src 'self' https://video.bsky.app https://video.cdn.bsky.app; object-src 'none'; frame-ancestors 'none'"
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
        Json,
        body::{Body, Bytes},
        extract::ConnectInfo,
        http::Request,
        response::Html,
        routing::{get, post},
    };
    use http_body_util::BodyExt;
    use std::{
        io::Write,
        net::SocketAddr,
        sync::{Arc, Mutex},
        time::{Duration, Instant},
    };
    use tower::ServiceExt;

    #[tokio::test]
    async fn av33_csp_allows_the_video_cdn_redirect_origin_for_playback_and_thumbnails() {
        let router = protect(
            axum::Router::new().route("/", get(|| async { "ok" })),
            false,
        );
        let response = router
            .oneshot(Request::builder().uri("/").body(Body::empty()).unwrap())
            .await
            .unwrap();
        let csp = response
            .headers()
            .get("content-security-policy")
            .unwrap()
            .to_str()
            .unwrap();

        for directive in ["img-src", "media-src", "connect-src"] {
            let policy = csp
                .split(';')
                .map(str::trim)
                .find(|policy| policy.starts_with(directive))
                .unwrap();
            assert!(
                policy.contains("https://video.cdn.bsky.app"),
                "{directive} must permit the video service's redirected CDN host: {policy}"
            );
        }
    }

    struct CapturedMakeWriter(Arc<Mutex<Vec<u8>>>);

    struct CapturedWriter(Arc<Mutex<Vec<u8>>>);

    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedMakeWriter {
        type Writer = CapturedWriter;

        fn make_writer(&'a self) -> Self::Writer {
            CapturedWriter(self.0.clone())
        }
    }

    impl Write for CapturedWriter {
        fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(bytes);
            Ok(bytes.len())
        }

        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn request_timing_logs_safe_path_status_and_slow_warning() {
        let output = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_max_level(tracing::Level::INFO)
            .with_writer(CapturedMakeWriter(output.clone()))
            .finish();
        let dispatch = tracing::Dispatch::new(subscriber);
        let _guard = tracing::dispatcher::set_default(&dispatch);

        let app = protect(
            axum::Router::new().route("/slow", get(|| async { "done" })),
            false,
        );
        let response = app
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri("/slow?secret=query-secret")
                    .header("cookie", "session=cookie-secret")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), 200);

        let logs = String::from_utf8(output.lock().unwrap().clone()).unwrap();
        assert!(logs.contains("HTTP request"), "{logs}");
        assert_eq!(
            logs.lines().filter(|line| line.contains(" INFO ")).count(),
            1,
            "{logs}"
        );
        assert_eq!(
            logs.lines().filter(|line| line.contains(" WARN ")).count(),
            0,
            "{logs}"
        );
        assert!(logs.contains("method=\"GET\""), "{logs}");
        assert!(logs.contains("path=\"/slow\""), "{logs}");
        assert!(logs.contains("status=200"), "{logs}");
        assert!(logs.contains("duration_ms="), "{logs}");
        assert!(!logs.contains("query-secret"), "{logs}");
        assert!(!logs.contains("cookie-secret"), "{logs}");
    }

    #[tokio::test]
    async fn compression_honors_gzip_and_brotli_accept_encoding() {
        let large_text = "mobile app page ".repeat(300);
        let app = protect(
            axum::Router::new()
                .route(
                    "/html",
                    get({
                        let large_text = large_text.clone();
                        move || {
                            let text = large_text.clone();
                            async move { Html(text) }
                        }
                    }),
                )
                .route(
                    "/json",
                    get(move || {
                        let text = large_text.clone();
                        async move { Json(serde_json::json!({"text":text})) }
                    }),
                ),
            false,
        );
        for (path, encoding) in [("/html", "gzip"), ("/json", "br")] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .uri(path)
                        .header("accept-encoding", encoding)
                        .body(Body::empty())
                        .unwrap(),
                )
                .await
                .unwrap();
            assert_eq!(response.headers()["content-encoding"], encoding);
            let body = response.into_body().collect().await.unwrap().to_bytes();
            assert!(
                body.len() < 1_000,
                "{encoding} response was {} bytes",
                body.len()
            );
        }
    }

    #[test]
    fn requests_over_two_seconds_emit_a_warning() {
        let output = Arc::new(Mutex::new(Vec::new()));
        let subscriber = tracing_subscriber::fmt()
            .with_ansi(false)
            .with_max_level(tracing::Level::INFO)
            .with_writer(CapturedMakeWriter(output.clone()))
            .finish();
        let dispatch = tracing::Dispatch::new(subscriber);
        let _guard = tracing::dispatcher::set_default(&dispatch);

        super::log_request(
            "GET",
            "/slow",
            Instant::now() - Duration::from_secs(3),
            axum::http::StatusCode::OK,
        );

        let logs = String::from_utf8(output.lock().unwrap().clone()).unwrap();
        assert!(
            logs.contains("HTTP request exceeded duration threshold"),
            "{logs}"
        );
        assert!(logs.contains("duration_ms=3000"), "{logs}");
    }

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
