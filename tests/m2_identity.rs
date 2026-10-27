use appview_rs::identity::{
    DidDocument, Identity, IdentityBackend, IdentityResolver, ResolutionError,
};
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
