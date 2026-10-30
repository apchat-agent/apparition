use std::{
    collections::{HashMap, HashSet},
    future::Future,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    pin::Pin,
    sync::{Arc, Mutex, OnceLock},
    time::{Duration, Instant},
};
use tokio::{net::lookup_host, sync::Semaphore, time::timeout};
use url::Url;

const MAX_REDIRECTS: usize = 5;
const MAX_REQUEST_BYTES: usize = 1024 * 1024;
const MAX_OAUTH_RESPONSE_BYTES: usize = 2 * 1024 * 1024;
const REQUEST_TIMEOUT: Duration = Duration::from_secs(20);
const HTTP_CLIENT_CACHE_CAPACITY: usize = 512;
const HTTP_CLIENT_IDLE_TIMEOUT: Duration = Duration::from_secs(90);

static OUTBOUND_REQUESTS: OnceLock<Arc<Semaphore>> = OnceLock::new();
// Repository backfill downloads are large and keep permits for up to REQUEST_TIMEOUT each.
// A separate pool keeps this background work from occupying interactive request capacity.
static BACKGROUND_REQUESTS: OnceLock<Arc<Semaphore>> = OnceLock::new();

#[derive(Clone, Debug, Default)]
pub(crate) struct OutboundCallSnapshot {
    pub total_calls: usize,
    pub total_ms: u64,
    pub dpop_nonce_retries: usize,
    pub by_kind: HashMap<String, (usize, u64)>,
}

#[derive(Clone, Default)]
pub(crate) struct RequestOutboundAccounting {
    snapshot: Arc<Mutex<OutboundCallSnapshot>>,
}

impl RequestOutboundAccounting {
    pub(crate) fn snapshot(&self) -> OutboundCallSnapshot {
        self.snapshot
            .lock()
            .map(|snapshot| snapshot.clone())
            .unwrap_or_default()
    }
}

tokio::task_local! {
    static REQUEST_OUTBOUND_ACCOUNTING: RequestOutboundAccounting;
}

pub(crate) async fn with_request_outbound_accounting<F: Future>(
    accounting: RequestOutboundAccounting,
    future: F,
) -> F::Output {
    REQUEST_OUTBOUND_ACCOUNTING.scope(accounting, future).await
}

fn request_pool(background: bool) -> Arc<Semaphore> {
    if background {
        BACKGROUND_REQUESTS.get_or_init(|| Arc::new(Semaphore::new(6)))
    } else {
        OUTBOUND_REQUESTS.get_or_init(|| Arc::new(Semaphore::new(8)))
    }
    .clone()
}

pub fn is_forbidden_ip(ip: IpAddr) -> bool {
    match ip {
        IpAddr::V4(ip) => forbidden_v4(ip),
        IpAddr::V6(ip) => {
            if let Some(mapped) = ip.to_ipv4_mapped() {
                return forbidden_v4(mapped);
            }
            let segments = ip.segments();
            let global_unicast = segments[0] & 0xe000 == 0x2000;
            let documentation =
                segments[0] == 0x2001 && ((segments[1] == 0x0db8) || segments[1] & 0xfe00 == 0);
            ip.is_unspecified()
                || ip.is_loopback()
                || ip.is_unique_local()
                || ip.is_unicast_link_local()
                || ip.is_multicast()
                || !global_unicast
                || documentation
                || segments[0] == 0x2002 // 6to4 embeds an unchecked IPv4 destination.
                || (segments[0] == 0x3fff && segments[1] & 0xfff0 == 0)
        }
    }
}

fn forbidden_v4(ip: Ipv4Addr) -> bool {
    let octets = ip.octets();
    let shared_address_space = octets[0] == 100 && (64..=127).contains(&octets[1]);
    let protocol_assignments = octets[0] == 192 && octets[1] == 0 && octets[2] == 0;
    let documentation = (octets[0] == 192 && octets[1] == 0 && octets[2] == 2)
        || (octets[0] == 198 && octets[1] == 51 && octets[2] == 100)
        || (octets[0] == 203 && octets[1] == 0 && octets[2] == 113);
    let benchmark = octets[0] == 198 && (18..=19).contains(&octets[1]);
    let old_relay = octets[0] == 192 && octets[1] == 88 && octets[2] == 99;

    ip.is_unspecified()
        || ip.is_loopback()
        || ip.is_private()
        || ip.is_link_local()
        || ip.is_multicast()
        || ip.is_broadcast()
        || octets[0] == 0
        || shared_address_space
        || protocol_assignments
        || documentation
        || benchmark
        || old_relay
        || octets[0] >= 240
}

pub trait HostResolver: Send + Sync {
    fn resolve<'a>(
        &'a self,
        host: &'a str,
        port: u16,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<IpAddr>, String>> + Send + 'a>>;
}

#[derive(Clone, Debug, Hash, PartialEq, Eq)]
struct ClientKey {
    host: String,
    addresses: Vec<SocketAddr>,
}

struct CachedHttpClient {
    client: reqwest::Client,
    last_used: Instant,
}

#[derive(Default)]
struct HttpClientPool {
    clients: HashMap<ClientKey, CachedHttpClient>,
}

#[derive(Default)]
struct SystemResolver;

impl HostResolver for SystemResolver {
    fn resolve<'a>(
        &'a self,
        host: &'a str,
        port: u16,
    ) -> Pin<Box<dyn Future<Output = Result<Vec<IpAddr>, String>> + Send + 'a>> {
        Box::pin(async move {
            lookup_host((host, port))
                .await
                .map(|addresses| addresses.map(|address| address.ip()).collect())
                .map_err(|_| "DNS lookup failed".to_owned())
        })
    }
}

#[derive(Clone)]
pub struct SafeHttpClient {
    resolver: Arc<dyn HostResolver>,
    http_test_hosts: HashSet<String>,
    allow_http: bool,
    background: bool,
    clients: Arc<Mutex<HttpClientPool>>,
}

impl SafeHttpClient {
    pub fn new() -> Self {
        Self {
            resolver: Arc::new(SystemResolver),
            http_test_hosts: HashSet::new(),
            allow_http: false,
            background: false,
            clients: Arc::new(Mutex::new(HttpClientPool::default())),
        }
    }

    pub fn with_resolver(resolver: impl HostResolver + 'static) -> Self {
        Self {
            resolver: Arc::new(resolver),
            http_test_hosts: HashSet::new(),
            allow_http: false,
            background: false,
            clients: Arc::new(Mutex::new(HttpClientPool::default())),
        }
    }

    /// Use the background request pool (bulk work such as repository backfill).
    pub fn background(mut self) -> Self {
        self.background = true;
        self
    }

    pub fn with_http_test_hosts(mut self, hosts: impl IntoIterator<Item = String>) -> Self {
        self.http_test_hosts = hosts
            .into_iter()
            .map(|host| host.trim_end_matches('.').to_ascii_lowercase())
            .collect();
        self
    }

    /// Permit ordinary HTTP URLs while retaining DNS resolution and private-address checks.
    pub fn allow_http_schemes(mut self) -> Self {
        self.allow_http = true;
        self
    }

    pub async fn resolve_safe(&self, url: &Url) -> Result<Vec<SocketAddr>, String> {
        let host = url.host_str().ok_or_else(|| "URL has no host".to_owned())?;
        let normalized_host = host.trim_end_matches('.').to_ascii_lowercase();
        let test_host = self.http_test_hosts.contains(&normalized_host);
        if !url.username().is_empty() || url.password().is_some() || url.fragment().is_some() {
            return Err("URL contains forbidden components".to_owned());
        }
        if url.scheme() != "https" && !(url.scheme() == "http" && (self.allow_http || test_host)) {
            return Err("outbound URL must use HTTPS".to_owned());
        }
        let port = url
            .port_or_known_default()
            .ok_or_else(|| "URL has no port".to_owned())?;
        let addresses = match host.parse::<IpAddr>() {
            Ok(address) => vec![address],
            Err(_) => self.resolver.resolve(host, port).await?,
        };
        if addresses.is_empty() {
            return Err("host has no addresses".to_owned());
        }
        if !test_host && addresses.iter().copied().any(is_forbidden_ip) {
            return Err("host resolved to a forbidden address".to_owned());
        }
        let mut addresses = addresses
            .into_iter()
            .map(|address| SocketAddr::new(address, port))
            .collect::<Vec<_>>();
        addresses.sort_unstable();
        addresses.dedup();
        Ok(addresses)
    }

    pub async fn send(
        &self,
        request: http::Request<Vec<u8>>,
        max_bytes: usize,
    ) -> Result<http::Response<Vec<u8>>, String> {
        self.send_with_limits(request, max_bytes, MAX_REDIRECTS, REQUEST_TIMEOUT)
            .await
            .map(|(response, _)| response)
    }

    /// Send a GET request with an explicit redirect, response-size, and total-time budget.
    /// The final URL is returned so callers can resolve page-relative metadata safely.
    pub async fn get_with_limits(
        &self,
        url: &Url,
        headers: http::HeaderMap,
        max_bytes: usize,
        max_redirects: usize,
        request_timeout: Duration,
    ) -> Result<(http::Response<Vec<u8>>, Url), String> {
        let request = http::Request::get(url.as_str())
            .body(Vec::new())
            .map_err(|_| "outbound request is invalid".to_owned())?;
        let (mut parts, body) = request.into_parts();
        parts.headers = headers;
        self.send_with_limits(
            http::Request::from_parts(parts, body),
            max_bytes,
            max_redirects,
            request_timeout,
        )
        .await
    }

    async fn send_with_limits(
        &self,
        request: http::Request<Vec<u8>>,
        max_bytes: usize,
        max_redirects: usize,
        request_timeout: Duration,
    ) -> Result<(http::Response<Vec<u8>>, Url), String> {
        if request.body().len() > MAX_REQUEST_BYTES {
            return Err("outbound request body is too large".to_owned());
        }
        let permit = request_pool(self.background)
            .acquire_owned()
            .await
            .map_err(|_| "outbound request capacity is unavailable".to_owned())?;
        let result = timeout(
            request_timeout,
            self.send_inner(request, max_bytes, max_redirects),
        )
        .await
        .map_err(|_| "outbound request timed out".to_owned())?;
        drop(permit);
        result
    }

    pub async fn get(
        &self,
        url: &Url,
        headers: http::HeaderMap,
        max_bytes: usize,
    ) -> Result<http::Response<Vec<u8>>, String> {
        let request = http::Request::get(url.as_str())
            .body(Vec::new())
            .map_err(|_| "outbound request is invalid".to_owned())?;
        let (mut parts, body) = request.into_parts();
        parts.headers = headers;
        self.send(http::Request::from_parts(parts, body), max_bytes)
            .await
    }

    async fn send_inner(
        &self,
        mut request: http::Request<Vec<u8>>,
        max_bytes: usize,
        max_redirects: usize,
    ) -> Result<(http::Response<Vec<u8>>, Url), String> {
        let mut url = Url::parse(&request.uri().to_string())
            .map_err(|_| "outbound request URL is invalid".to_owned())?;
        for redirect_count in 0..=max_redirects {
            let addresses = self.resolve_safe(&url).await?;
            let host = url
                .host_str()
                .ok_or_else(|| "outbound URL has no host".to_owned())?;
            let client = self.client_for(host, &addresses)?;
            let response = client
                .request(request.method().clone(), url.clone())
                .headers(request.headers().clone())
                .body(request.body().clone())
                .send()
                .await
                .map_err(|error| {
                    format!("outbound HTTP request failed: {}", error.without_url())
                })?;

            if is_redirect(response.status())
                && let Some(location) = response.headers().get(http::header::LOCATION)
            {
                if redirect_count == max_redirects {
                    return Err("outbound redirect limit exceeded".to_owned());
                }
                let location = location
                    .to_str()
                    .map_err(|_| "outbound redirect location is invalid".to_owned())?;
                let next_url = url
                    .join(location)
                    .map_err(|_| "outbound redirect location is invalid".to_owned())?;
                if next_url.origin() != url.origin() {
                    if request.method() != http::Method::GET {
                        return Err(
                            "cross-origin redirect for a stateful request refused".to_owned()
                        );
                    }
                    for name in [
                        http::header::AUTHORIZATION,
                        http::header::COOKIE,
                        http::header::PROXY_AUTHORIZATION,
                    ] {
                        request.headers_mut().remove(name);
                    }
                    request.headers_mut().remove("dpop");
                }
                if response.status() == http::StatusCode::SEE_OTHER
                    || (matches!(
                        response.status(),
                        http::StatusCode::MOVED_PERMANENTLY | http::StatusCode::FOUND
                    ) && request.method() == http::Method::POST)
                {
                    *request.method_mut() = http::Method::GET;
                    *request.body_mut() = Vec::new();
                }
                url = next_url;
                continue;
            }

            if response
                .content_length()
                .is_some_and(|length| length > max_bytes as u64)
            {
                return Err("outbound response exceeds size limit".to_owned());
            }
            let status = response.status();
            let version = response.version();
            let headers = response.headers().clone();
            let mut body = Vec::new();
            let mut response = response;
            while let Some(chunk) = response
                .chunk()
                .await
                .map_err(|_| "outbound response read failed".to_owned())?
            {
                if body
                    .len()
                    .checked_add(chunk.len())
                    .is_none_or(|length| length > max_bytes)
                {
                    return Err("outbound response exceeds size limit".to_owned());
                }
                body.extend_from_slice(&chunk);
            }
            let mut builder = http::Response::builder().status(status).version(version);
            for (name, value) in &headers {
                builder = builder.header(name, value);
            }
            let response = builder
                .body(body)
                .map_err(|_| "outbound HTTP response is invalid".to_owned())?;
            return Ok((response, url));
        }
        Err("outbound redirect limit exceeded".to_owned())
    }

    fn client_for(&self, host: &str, addresses: &[SocketAddr]) -> Result<reqwest::Client, String> {
        let key = ClientKey {
            // reqwest applies resolve_to_addrs overrides to the exact hostname spelling;
            // preserve a trailing dot so a different override cannot share this client.
            host: host.to_ascii_lowercase(),
            addresses: addresses.to_vec(),
        };
        let now = Instant::now();
        let mut pool = self
            .clients
            .lock()
            .map_err(|_| "outbound HTTP client cache is unavailable".to_owned())?;
        pool.clients
            .retain(|_, cached| now.duration_since(cached.last_used) < HTTP_CLIENT_IDLE_TIMEOUT);
        if let Some(cached) = pool.clients.get_mut(&key) {
            cached.last_used = now;
            return Ok(cached.client.clone());
        }

        let mut builder = reqwest::Client::builder()
            .connect_timeout(Duration::from_secs(5))
            .timeout(REQUEST_TIMEOUT)
            .pool_idle_timeout(HTTP_CLIENT_IDLE_TIMEOUT)
            .redirect(reqwest::redirect::Policy::none())
            .no_proxy();
        if host.parse::<IpAddr>().is_err() {
            builder = builder.resolve_to_addrs(host, addresses);
        }
        let client = builder
            .build()
            .map_err(|_| "outbound HTTP client could not be initialized".to_owned())?;
        if pool.clients.len() >= HTTP_CLIENT_CACHE_CAPACITY
            && let Some(oldest) = pool
                .clients
                .iter()
                .min_by_key(|(_, cached)| cached.last_used)
                .map(|(key, _)| key.clone())
        {
            pool.clients.remove(&oldest);
        }
        tracing::debug!(
            host = %key.host,
            address_count = key.addresses.len(),
            "created outbound HTTP connection pool"
        );
        pool.clients.insert(
            key,
            CachedHttpClient {
                client: client.clone(),
                last_used: now,
            },
        );
        Ok(client)
    }
}

impl Default for SafeHttpClient {
    fn default() -> Self {
        Self::new()
    }
}

impl atrium_xrpc::HttpClient for SafeHttpClient {
    async fn send_http(
        &self,
        request: http::Request<Vec<u8>>,
    ) -> Result<http::Response<Vec<u8>>, Box<dyn std::error::Error + Send + Sync + 'static>> {
        self.send(request, MAX_OAUTH_RESPONSE_BYTES)
            .await
            .map_err(std::io::Error::other)
            .map_err(Into::into)
    }
}

fn is_redirect(status: http::StatusCode) -> bool {
    matches!(
        status,
        http::StatusCode::MOVED_PERMANENTLY
            | http::StatusCode::FOUND
            | http::StatusCode::SEE_OTHER
            | http::StatusCode::TEMPORARY_REDIRECT
            | http::StatusCode::PERMANENT_REDIRECT
    )
}

#[cfg(test)]
mod tests {
    use super::{HostResolver, SafeHttpClient, is_forbidden_ip, request_pool};
    use axum::{Router, response::Redirect, routing::get};
    use std::{
        collections::VecDeque,
        future::Future,
        net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
        pin::Pin,
        sync::{
            Arc, Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };
    use tokio::{
        io::{AsyncBufReadExt, AsyncWriteExt, BufReader},
        net::TcpListener,
    };
    use url::Url;

    #[tokio::test]
    async fn a_full_background_pool_does_not_block_interactive_requests() {
        let background = request_pool(true);
        let held = background
            .clone()
            .acquire_many_owned(background.available_permits() as u32)
            .await
            .unwrap();
        assert_eq!(background.available_permits(), 0);
        let interactive = tokio::time::timeout(
            std::time::Duration::from_millis(200),
            request_pool(false).acquire_owned(),
        )
        .await;
        assert!(
            interactive.is_ok(),
            "interactive request waited on background permits"
        );
        drop(held);
    }

    #[derive(Clone)]
    struct FakeResolver(Arc<Mutex<VecDeque<Vec<IpAddr>>>>);

    impl FakeResolver {
        fn new(responses: impl IntoIterator<Item = Vec<IpAddr>>) -> Self {
            Self(Arc::new(Mutex::new(responses.into_iter().collect())))
        }
    }

    impl HostResolver for FakeResolver {
        fn resolve<'a>(
            &'a self,
            _: &'a str,
            _: u16,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<IpAddr>, String>> + Send + 'a>> {
            Box::pin(async move {
                self.0
                    .lock()
                    .unwrap()
                    .pop_front()
                    .ok_or_else(|| "no fake answer".to_owned())
            })
        }
    }

    #[test]
    fn rejects_non_public_ipv4_and_ipv6_ranges() {
        let forbidden = [
            "0.1.2.3",
            "10.1.2.3",
            "100.64.0.1",
            "100.127.255.254",
            "127.0.0.1",
            "169.254.169.254",
            "172.16.0.1",
            "172.31.255.254",
            "192.168.1.1",
            "224.0.0.1",
            "255.255.255.255",
            "::",
            "::1",
            "fd00::1",
            "fd00:ec2::254",
            "fe80::1",
            "ff02::1",
            "::ffff:127.0.0.1",
        ];
        for address in forbidden {
            let ip = address.parse::<IpAddr>().unwrap();
            assert!(is_forbidden_ip(ip), "{address} must be blocked");
        }
    }

    #[test]
    fn permits_public_ipv4_and_ipv6_addresses() {
        for ip in [
            IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)),
            IpAddr::V4(Ipv4Addr::new(8, 8, 8, 8)),
            IpAddr::V6("2606:4700:4700::1111".parse::<Ipv6Addr>().unwrap()),
        ] {
            assert!(!is_forbidden_ip(ip), "{ip} should be allowed");
        }
    }

    #[tokio::test]
    async fn resolver_policy_rejects_private_answers_and_private_redirect_targets() {
        for address in ["127.0.0.1", "10.0.0.8", "100.64.0.1", "169.254.169.254"] {
            let client =
                SafeHttpClient::with_resolver(FakeResolver::new([vec![address.parse().unwrap()]]));
            let url = Url::parse("https://remote.example/resource").unwrap();
            assert!(
                client.resolve_safe(&url).await.is_err(),
                "{address} must be blocked"
            );
        }

        let client = SafeHttpClient::with_resolver(FakeResolver::new([]));
        let redirect = Url::parse("https://127.0.0.1/private").unwrap();
        assert!(client.resolve_safe(&redirect).await.is_err());
    }

    #[tokio::test]
    async fn resolver_policy_rejects_mixed_and_rebound_dns_answers() {
        let mixed = SafeHttpClient::with_resolver(FakeResolver::new([vec![
            "1.1.1.1".parse().unwrap(),
            "192.168.1.10".parse().unwrap(),
        ]]));
        let url = Url::parse("https://remote.example/resource").unwrap();
        assert!(mixed.resolve_safe(&url).await.is_err());

        let rebind = SafeHttpClient::with_resolver(FakeResolver::new([
            vec!["1.1.1.1".parse().unwrap()],
            vec!["127.0.0.1".parse().unwrap()],
        ]));
        assert_eq!(
            rebind.resolve_safe(&url).await.unwrap(),
            [SocketAddr::from(([1, 1, 1, 1], 443))]
        );
        assert!(rebind.resolve_safe(&url).await.is_err());
    }

    #[tokio::test]
    async fn resolver_policy_allows_a_public_dns_answer() {
        let client = SafeHttpClient::with_resolver(FakeResolver::new([vec![
            "93.184.216.34".parse().unwrap(),
        ]]));
        let url = Url::parse("https://remote.example/resource").unwrap();
        assert_eq!(
            client.resolve_safe(&url).await.unwrap(),
            [SocketAddr::from(([93, 184, 216, 34], 443))]
        );
    }

    async fn test_server() -> (String, tokio::task::JoinHandle<()>) {
        let app = Router::new()
            .route("/body", get(|| async { "body" }))
            .route("/large", get(|| async { "this response is too large" }))
            .route(
                "/redirect",
                get(|| async { Redirect::temporary("https://127.0.0.1/private") }),
            );
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (address.to_string(), task)
    }

    fn local_test_client() -> SafeHttpClient {
        SafeHttpClient::with_resolver(FakeResolver::new([vec!["127.0.0.1".parse().unwrap()]]))
            .with_http_test_hosts(["safe.example".to_owned()])
    }

    #[tokio::test]
    async fn guarded_http_request_pins_dns_and_caps_response_size() {
        let (address, server) = test_server().await;
        let client = local_test_client();
        let response = client
            .send(
                http::Request::get(format!(
                    "http://safe.example:{}/body",
                    address.rsplit(':').next().unwrap()
                ))
                .body(Vec::new())
                .unwrap(),
                4,
            )
            .await
            .unwrap();
        assert_eq!(response.body(), b"body");

        let too_large = client
            .send(
                http::Request::get(format!(
                    "http://safe.example:{}/large",
                    address.rsplit(':').next().unwrap()
                ))
                .body(Vec::new())
                .unwrap(),
                4,
            )
            .await;
        assert!(too_large.is_err());
        server.abort();
    }

    #[tokio::test]
    async fn outbound_call_accounting_counts_request_calls_but_not_spawned_background_calls() {
        use super::{RequestOutboundAccounting, with_request_outbound_accounting};

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let requests = Arc::new(AtomicUsize::new(0));
        let server_requests = requests.clone();
        let app = Router::new().route(
            "/{*path}",
            get(move || {
                let requests = server_requests.clone();
                async move {
                    requests.fetch_add(1, Ordering::Relaxed);
                    "ok"
                }
            }),
        );
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        let client = SafeHttpClient::with_resolver(FakeResolver::new(
            (0..3).map(|_| vec![IpAddr::V4(Ipv4Addr::LOCALHOST)]),
        ))
        .with_http_test_hosts(["safe.example".to_owned()]);
        let request_url = |path: &str| {
            Url::parse(&format!("http://safe.example:{}/{path}", address.port())).unwrap()
        };
        let accounting = RequestOutboundAccounting::default();
        with_request_outbound_accounting(accounting.clone(), async {
            for path in ["xrpc/com.atproto.repo.getRecord", "xrpc/other"] {
                client
                    .get(&request_url(path), http::HeaderMap::new(), 16)
                    .await
                    .unwrap();
            }

            let background_client = client.clone();
            let background_url = request_url("background");
            tokio::spawn(async move {
                background_client
                    .get(&background_url, http::HeaderMap::new(), 16)
                    .await
                    .unwrap();
            })
            .await
            .unwrap();
        })
        .await;

        let snapshot = accounting.snapshot();
        assert_eq!(requests.load(Ordering::Relaxed), 3);
        assert_eq!(
            snapshot.total_calls, 2,
            "spawned work is outside the request scope"
        );
        assert_eq!(snapshot.by_kind.get("pds_get_record").unwrap().0, 1);
        assert_eq!(snapshot.by_kind.get("other").unwrap().0, 1);
        server.abort();
    }

    #[tokio::test]
    async fn guarded_http_request_rechecks_redirect_destinations() {
        let (address, server) = test_server().await;
        let response = local_test_client()
            .send(
                http::Request::get(format!(
                    "http://safe.example:{}/redirect",
                    address.rsplit(':').next().unwrap()
                ))
                .body(Vec::new())
                .unwrap(),
                128,
            )
            .await;
        assert!(response.is_err(), "redirect to loopback must be refused");
        server.abort();
    }

    #[tokio::test]
    async fn repeated_requests_to_one_host_reuse_one_connection() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let accepts = Arc::new(AtomicUsize::new(0));
        let server_accepts = accepts.clone();
        let server = tokio::spawn(async move {
            loop {
                let Ok((stream, _)) = listener.accept().await else {
                    break;
                };
                server_accepts.fetch_add(1, Ordering::Relaxed);
                tokio::spawn(async move {
                    let (reader, mut writer) = stream.into_split();
                    let mut reader = BufReader::new(reader);
                    loop {
                        let mut line = Vec::new();
                        if reader
                            .read_until(b'\n', &mut line)
                            .await
                            .unwrap_or_default()
                            == 0
                        {
                            break;
                        }
                        loop {
                            line.clear();
                            if reader
                                .read_until(b'\n', &mut line)
                                .await
                                .unwrap_or_default()
                                == 0
                            {
                                return;
                            }
                            if line == b"\r\n" {
                                break;
                            }
                        }
                        if writer
                            .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok")
                            .await
                            .is_err()
                        {
                            break;
                        }
                    }
                });
            }
        });

        let client = SafeHttpClient::with_resolver(FakeResolver::new(
            (0..5).map(|_| vec![IpAddr::V4(Ipv4Addr::LOCALHOST)]),
        ))
        .with_http_test_hosts(["safe.example".to_owned()]);
        for _ in 0..5 {
            let url = Url::parse(&format!("http://safe.example:{}/body", address.port())).unwrap();
            let response = client.get(&url, http::HeaderMap::new(), 16).await.unwrap();
            assert_eq!(response.body(), b"ok");
        }

        assert_eq!(accepts.load(Ordering::Relaxed), 1);
        server.abort();
    }

    #[test]
    fn client_pool_keeps_reqwest_dns_override_names_separate() {
        let client = SafeHttpClient::with_resolver(FakeResolver::new([]));
        let address = [SocketAddr::from(([127, 0, 0, 1], 443))];
        client.client_for("safe.example.", &address).unwrap();
        client.client_for("safe.example", &address).unwrap();

        assert_eq!(client.clients.lock().unwrap().clients.len(), 2);
    }
}
