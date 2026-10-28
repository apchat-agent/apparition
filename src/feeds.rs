use crate::{identity::{DidDocument, IdentityResolver}, net::SafeHttpClient};
use serde_json::Value;
use std::{collections::HashMap, sync::Arc, time::Duration};

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ResolvedFeed {
    pub uri: String,
    pub display_name: String,
    pub description: Option<String>,
    pub avatar: Option<String>,
    pub service_did: String,
    pub endpoint: String,
}

pub struct FeedClient<B: crate::identity::IdentityBackend> {
    identity: Arc<IdentityResolver<B>>,
    http: SafeHttpClient,
    generator_cache: tokio::sync::Mutex<HashMap<String, (tokio::time::Instant, ResolvedFeed)>>,
}

impl<B: crate::identity::IdentityBackend> FeedClient<B> {
    pub fn new(identity: Arc<IdentityResolver<B>>, http: SafeHttpClient) -> Self {
        Self {
            identity,
            http,
            generator_cache: tokio::sync::Mutex::new(HashMap::new()),
        }
    }

    pub async fn resolve_feed(&self, _uri: &str) -> Result<ResolvedFeed, String> {
        Err("feed generator resolution is not implemented".to_owned())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SavedFeedPreference {
    pub id: String,
    pub feed_type: String,
    pub value: String,
    pub pinned: bool,
}

pub fn parse_saved_feed_preferences(response: &Value) -> Vec<SavedFeedPreference> {
    let Some(preferences) = response.get("preferences").and_then(Value::as_array) else {
        return Vec::new();
    };

    if let Some(v2) = preferences.iter().find(|preference| {
        preference.get("$type").and_then(Value::as_str)
            == Some("app.bsky.actor.defs#savedFeedsPrefV2")
    }) {
        return v2
            .get("items")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|item| {
                Some(SavedFeedPreference {
                    id: item.get("id")?.as_str()?.to_owned(),
                    feed_type: item.get("type")?.as_str()?.to_owned(),
                    value: item.get("value")?.as_str()?.to_owned(),
                    pinned: item.get("pinned")?.as_bool()?,
                })
            })
            .collect();
    }

    let Some(legacy) = preferences.iter().find(|preference| {
        preference.get("$type").and_then(Value::as_str)
            == Some("app.bsky.actor.defs#savedFeedsPref")
    }) else {
        return Vec::new();
    };
    let pinned = legacy
        .get("pinned")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>();
    let saved = legacy
        .get("saved")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .filter_map(Value::as_str)
        .collect::<Vec<_>>();
    let mut result = Vec::new();
    for (value, is_pinned) in pinned
        .iter()
        .map(|value| (*value, true))
        .chain(saved.iter().map(|value| (*value, false)))
    {
        if result.iter().any(|item: &SavedFeedPreference| item.value == value) {
            continue;
        }
        result.push(SavedFeedPreference {
            id: value.to_owned(),
            feed_type: "feed".to_owned(),
            value: value.to_owned(),
            pinned: is_pinned,
        });
    }
    result
}

#[cfg(test)]
mod tests {
    use super::{FeedClient, parse_saved_feed_preferences};
    use crate::{identity::{DidDocument, DidService, IdentityBackend, IdentityResolver, ResolutionError}, net::{HostResolver, SafeHttpClient}};
    use axum::{Router, http::Uri, response::IntoResponse, routing::any};
    use serde_json::json;
    use std::{collections::HashMap, future::Future, net::{IpAddr, Ipv4Addr}, pin::Pin, sync::Arc, time::Duration};
    use tokio::net::TcpListener;

    const CREATOR: &str = "did:web:creator.test";

    #[derive(Clone)]
    struct FakeIdentity(HashMap<String, DidDocument>);

    impl IdentityBackend for FakeIdentity {
        async fn did_document(&self, did: &str) -> Result<Option<DidDocument>, ResolutionError> {
            Ok(self.0.get(did).cloned())
        }
        async fn dns_txt_did(&self, _: &str) -> Result<Option<String>, ResolutionError> { Ok(None) }
        async fn well_known_did(&self, _: &str) -> Result<Option<String>, ResolutionError> { Ok(None) }
    }

    struct TestResolver;

    impl HostResolver for TestResolver {
        fn resolve<'a>(&'a self, _: &'a str, _: u16) -> Pin<Box<dyn Future<Output = Result<Vec<IpAddr>, String>> + Send + 'a>> {
            Box::pin(async { Ok(vec![IpAddr::V4(Ipv4Addr::LOCALHOST)]) })
        }
    }

    fn document(did: &str, pds: &str, services: Vec<DidService>) -> DidDocument {
        let mut service = services;
        if !pds.is_empty() {
            service.insert(0, DidService { id: "#atproto_pds".into(), type_name: "AtprotoPersonalDataServer".into(), service_endpoint: pds.into() });
        }
        DidDocument { id: did.to_owned(), also_known_as: vec![], verification_method: vec![], service }
    }

    async fn fake_pds() -> (String, tokio::task::JoinHandle<()>) {
        let app = Router::new().fallback(any(|uri: Uri| async move {
            if uri.path() == "/xrpc/com.atproto.repo.getRecord" {
                let query = uri.query().unwrap_or_default();
                let service_did = if query.contains("rkey=web") { "did:web:generator.test" } else { "did:plc:generator" };
                axum::Json(json!({ "uri": format!("at://{CREATOR}/app.bsky.feed.generator/{}", if query.contains("rkey=web") { "web" } else { "plc" }), "cid":"bafyreitest", "value":{"did":service_did,"displayName":"Example","description":"A feed","avatar":"https://img.test/avatar"} })).into_response()
            } else { axum::http::StatusCode::NOT_FOUND.into_response() }
        }));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let address = listener.local_addr().unwrap();
        let task = tokio::spawn(async move { axum::serve(listener, app).await.unwrap(); });
        (format!("http://creator.test:{}", address.port()), task)
    }

    fn resolver(pds: String, endpoint: String) -> Arc<IdentityResolver<FakeIdentity>> {
        let docs = HashMap::from([
            (CREATOR.to_owned(), document(CREATOR, &pds, vec![])),
            ("did:web:generator.test".to_owned(), document("did:web:generator.test", "", if endpoint.is_empty() { vec![] } else { vec![DidService { id: "#bsky_fg".into(), type_name: "BskyFeedGenerator".into(), service_endpoint: endpoint.clone() }] })),
            ("did:plc:generator".to_owned(), document("did:plc:generator", "", if endpoint.is_empty() { vec![] } else { vec![DidService { id: "#bsky_fg".into(), type_name: "BskyFeedGenerator".into(), service_endpoint: endpoint }] })),
        ]);
        Arc::new(IdentityResolver::new(FakeIdentity(docs), Duration::from_secs(30)))
    }

    fn client(identity: Arc<IdentityResolver<FakeIdentity>>) -> FeedClient<FakeIdentity> {
        FeedClient::new(identity, SafeHttpClient::with_resolver(TestResolver).with_http_test_hosts(["creator.test".to_owned(), "generator.test".to_owned()]))
    }

    #[tokio::test]
    async fn generator_records_resolve_did_web_and_did_plc_services() {
        let (pds, server) = fake_pds().await;
        let port = pds.rsplit(':').next().unwrap().to_owned();
        let identity = resolver(pds, format!("http://generator.test:{port}/feed"));
        let client = client(identity);
        for rkey in ["web", "plc"] {
            let uri = format!("at://{CREATOR}/app.bsky.feed.generator/{rkey}");
            let feed = client.resolve_feed(&uri).await.unwrap();
            assert_eq!(feed.display_name, "Example");
            assert!(feed.endpoint.ends_with("/feed"));
            assert_eq!(feed.service_did, if rkey == "web" { "did:web:generator.test" } else { "did:plc:generator" });
        }
        server.abort();
    }

    #[tokio::test]
    async fn generator_resolution_refuses_missing_service_and_ssrf_endpoint() {
        let (pds, server) = fake_pds().await;
        let endpoint = "https://blocked.test/feed";
        let identity = resolver(pds.clone(), endpoint.to_owned());
        let blocked = client(identity);
        let error = blocked.resolve_feed(&format!("at://{CREATOR}/app.bsky.feed.generator/web")).await.unwrap_err();
        assert!(error.contains("forbidden address"), "{error}");

        let no_service = resolver(pds, "".to_owned());
        // An empty service endpoint is invalid and must produce a clear resolution error.
        let error = client(no_service).resolve_feed(&format!("at://{CREATOR}/app.bsky.feed.generator/web")).await.unwrap_err();
        assert!(error.contains("bsky_fg"), "{error}");
        server.abort();
    }

    #[test]
    fn saved_feed_preferences_keep_v2_items_and_prefer_v2_over_legacy() {
        let response = json!({
            "preferences": [
                {
                    "$type": "app.bsky.actor.defs#savedFeedsPref",
                    "pinned": ["at://did:plc:legacy/app.bsky.feed.generator/old"],
                    "saved": ["at://did:plc:legacy/app.bsky.feed.generator/old"]
                },
                {
                    "$type": "app.bsky.actor.defs#savedFeedsPrefV2",
                    "items": [
                        {"id":"1", "type":"feed", "value":"at://did:plc:one/app.bsky.feed.generator/a", "pinned":false},
                        {"id":"2", "type":"timeline", "value":"following", "pinned":true},
                        {"id":"3", "type":"feed", "value":"at://did:plc:two/app.bsky.feed.generator/b", "pinned":true},
                        {"id":"4", "type":"list", "value":"at://did:plc:list/app.bsky.graph.list/x", "pinned":true},
                        {"id":"5", "type":"futureType", "value":"opaque", "pinned":false}
                    ]
                }
            ]
        });

        let parsed = parse_saved_feed_preferences(&response);
        assert_eq!(parsed.len(), 5);
        assert_eq!(parsed[1].feed_type, "timeline");
        assert_eq!(parsed[4].feed_type, "futureType");
        let pinned_feeds = parsed
            .iter()
            .filter(|item| item.feed_type == "feed" && item.pinned)
            .map(|item| item.value.as_str())
            .collect::<Vec<_>>();
        assert_eq!(
            pinned_feeds,
            ["at://did:plc:two/app.bsky.feed.generator/b"]
        );
        assert!(!parsed.iter().any(|item| item.value.ends_with("/old")));
    }

    #[test]
    fn legacy_saved_feed_preferences_are_used_when_v2_is_absent() {
        let response = json!({
            "preferences": [{
                "$type": "app.bsky.actor.defs#savedFeedsPref",
                "pinned": ["at://did:plc:one/app.bsky.feed.generator/pinned"],
                "saved": [
                    "at://did:plc:one/app.bsky.feed.generator/pinned",
                    "at://did:plc:two/app.bsky.feed.generator/other"
                ]
            }]
        });

        let parsed = parse_saved_feed_preferences(&response);
        assert_eq!(parsed.len(), 2);
        assert!(parsed[0].pinned);
        assert!(!parsed[1].pinned);
    }
}
