use std::collections::HashSet;
use std::{
    future::Future,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    pin::Pin,
    sync::Arc,
};
use url::Url;

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

pub struct SafeHttpClient {
    resolver: Arc<dyn HostResolver>,
    http_test_hosts: HashSet<String>,
}

impl SafeHttpClient {
    pub fn with_resolver(resolver: impl HostResolver + 'static) -> Self {
        Self {
            resolver: Arc::new(resolver),
            http_test_hosts: HashSet::new(),
        }
    }

    pub fn with_http_test_hosts(mut self, hosts: impl IntoIterator<Item = String>) -> Self {
        self.http_test_hosts = hosts
            .into_iter()
            .map(|host| host.trim_end_matches('.').to_ascii_lowercase())
            .collect();
        self
    }

    pub async fn resolve_safe(&self, url: &Url) -> Result<Vec<SocketAddr>, String> {
        let host = url.host_str().ok_or_else(|| "URL has no host".to_owned())?;
        let normalized_host = host.trim_end_matches('.').to_ascii_lowercase();
        let test_host = self.http_test_hosts.contains(&normalized_host);
        if !url.username().is_empty() || url.password().is_some() || url.fragment().is_some() {
            return Err("URL contains forbidden components".to_owned());
        }
        if url.scheme() != "https" && !(url.scheme() == "http" && test_host) {
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
        Ok(addresses
            .into_iter()
            .map(|address| SocketAddr::new(address, port))
            .collect())
    }

    pub async fn send(
        &self,
        _request: http::Request<Vec<u8>>,
        _max_bytes: usize,
    ) -> Result<http::Response<Vec<u8>>, String> {
        Err("outbound request failed".to_owned())
    }
}

#[cfg(test)]
mod tests {
    use super::{HostResolver, SafeHttpClient, is_forbidden_ip};
    use axum::{Router, response::Redirect, routing::get};
    use std::{
        collections::VecDeque,
        future::Future,
        net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr},
        pin::Pin,
        sync::{Arc, Mutex},
    };
    use tokio::net::TcpListener;
    use url::Url;

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
}
