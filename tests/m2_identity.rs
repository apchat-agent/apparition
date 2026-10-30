use appview_rs::identity::{
    DidDocument, HttpIdentityBackend, Identity, IdentityBackend, IdentityResolver, ResolutionError,
    resolve_login_allowlist,
};
use appview_rs::net::SafeHttpClient;
use std::{
    collections::HashMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

#[derive(Clone, Default)]
struct FakeIdentityBackend {
    documents: HashMap<String, DidDocument>,
    dns: HashMap<String, String>,
    web: HashMap<String, String>,
    did_lookups: Arc<AtomicUsize>,
    dns_lookups: Arc<AtomicUsize>,
}

impl FakeIdentityBackend {
    fn with_document(mut self, document: DidDocument) -> Self {
        self.documents.insert(document.id.clone(), document);
        self
    }
}

impl IdentityBackend for FakeIdentityBackend {
    async fn did_document(&self, did: &str) -> Result<Option<DidDocument>, ResolutionError> {
        self.did_lookups.fetch_add(1, Ordering::SeqCst);
        Ok(self.documents.get(did).cloned())
    }

    async fn dns_txt_did(&self, handle: &str) -> Result<Option<String>, ResolutionError> {
        self.dns_lookups.fetch_add(1, Ordering::SeqCst);
        Ok(self.dns.get(handle).cloned())
    }

    async fn well_known_did(&self, handle: &str) -> Result<Option<String>, ResolutionError> {
        Ok(self.web.get(handle).cloned())
    }
}

fn document(did: &str, handles: &[&str]) -> DidDocument {
    DidDocument {
        id: did.to_owned(),
        also_known_as: handles
            .iter()
            .map(|handle| format!("at://{handle}"))
            .collect(),
        verification_method: vec![],
        service: vec![],
    }
}

#[tokio::test]
async fn handle_resolution_checks_dns_and_did_document_in_both_directions() {
    let backend = FakeIdentityBackend {
        dns: HashMap::from([("alice.test".into(), "did:plc:alice".into())]),
        ..Default::default()
    }
    .with_document(document("did:plc:alice", &["alice.test"]));
    let resolver = IdentityResolver::new(backend, Duration::from_secs(30));

    let identity = resolver.resolve_handle("alice.test").await.unwrap();
    assert_eq!(identity.did, "did:plc:alice");
    assert_eq!(identity.handle.as_deref(), Some("alice.test"));

    let identity = resolver.resolve_did("did:plc:alice").await.unwrap();
    assert_eq!(identity.handle.as_deref(), Some("alice.test"));
}

#[tokio::test]
async fn did_web_handle_can_be_verified_with_the_well_known_method() {
    let did = "did:web:alice.test";
    let backend = FakeIdentityBackend {
        web: HashMap::from([("alice.test".into(), did.into())]),
        ..Default::default()
    }
    .with_document(document(did, &["alice.test"]));
    let resolver = IdentityResolver::new(backend, Duration::from_secs(30));

    let identity = resolver.resolve_did(did).await.unwrap();
    assert_eq!(
        identity,
        Identity {
            did: did.into(),
            handle: Some("alice.test".into())
        }
    );
}

#[tokio::test]
async fn a_handle_that_does_not_match_the_did_document_is_rejected() {
    let backend = FakeIdentityBackend {
        dns: HashMap::from([("alice.test".into(), "did:plc:alice".into())]),
        ..Default::default()
    }
    .with_document(document("did:plc:alice", &["other.test"]));
    let resolver = IdentityResolver::new(backend, Duration::from_secs(30));

    assert!(matches!(
        resolver.resolve_handle("alice.test").await,
        Err(ResolutionError::UnverifiedHandle(_))
    ));
}

#[tokio::test]
async fn login_allowlist_handles_resolve_to_verified_dids() {
    let backend = FakeIdentityBackend {
        dns: HashMap::from([("alice.test".into(), "did:plc:alice".into())]),
        ..Default::default()
    }
    .with_document(document("did:plc:alice", &["alice.test"]));
    let resolver = IdentityResolver::new(backend, Duration::from_secs(30));
    let entries = vec![" @ALICE.test ".to_owned(), "did:plc:bob".to_owned()];

    let allowed = resolve_login_allowlist(&entries, &resolver).await.unwrap();

    assert_eq!(
        allowed,
        std::collections::HashSet::from(["did:plc:alice".to_owned(), "did:plc:bob".to_owned()])
    );
}

#[tokio::test]
async fn login_allowlist_rejects_a_handle_without_bidirectional_verification() {
    let backend = FakeIdentityBackend {
        dns: HashMap::from([("alice.test".into(), "did:plc:alice".into())]),
        ..Default::default()
    }
    .with_document(document("did:plc:alice", &["other.test"]));
    let resolver = IdentityResolver::new(backend, Duration::from_secs(30));

    assert!(matches!(
        resolve_login_allowlist(&["alice.test".to_owned()], &resolver).await,
        Err(ResolutionError::UnverifiedHandle(_))
    ));
}

#[tokio::test]
async fn successful_handle_resolution_is_cached_for_the_configured_ttl() {
    let backend = FakeIdentityBackend {
        dns: HashMap::from([("alice.test".into(), "did:plc:alice".into())]),
        ..Default::default()
    }
    .with_document(document("did:plc:alice", &["alice.test"]));
    let dns_lookups = backend.dns_lookups.clone();
    let resolver = IdentityResolver::new(backend, Duration::from_secs(30));

    resolver.resolve_handle("alice.test").await.unwrap();
    resolver.resolve_handle("alice.test").await.unwrap();
    assert_eq!(dns_lookups.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn http_identity_backend_reaches_fake_plc_server_using_did_path() {
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        net::TcpListener,
    };

    const DID: &str = "did:plc:2iz7444ugycwhibyy3z5vord";
    let body = format!(r#"{{"id":"{DID}","alsoKnownAs":[],"verificationMethod":[],"service":[]}}"#);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut request = Vec::new();
        loop {
            let mut chunk = [0; 1024];
            let count = stream.read(&mut chunk).await.unwrap();
            assert_ne!(
                count, 0,
                "client disconnected before sending request headers"
            );
            request.extend_from_slice(&chunk[..count]);
            if request.windows(4).any(|window| window == b"\r\n\r\n") {
                break;
            }
        }

        let response = format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{}",
            body.len(),
            body
        );
        stream.write_all(response.as_bytes()).await.unwrap();
        String::from_utf8(request).unwrap()
    });

    let backend = HttpIdentityBackend::with_client(
        &format!("http://{address}/plc"),
        "https://dns.example/query",
        SafeHttpClient::new().with_http_test_hosts(["127.0.0.1".to_owned()]),
    )
    .unwrap();
    let document = backend.did_document(DID).await.unwrap().unwrap();
    assert_eq!(document.id, DID);

    let request = server.await.unwrap();
    assert!(
        request.starts_with(&format!("GET /plc/{DID} HTTP/1.1\r\n")),
        "unexpected fake PLC request: {request}"
    );
}
