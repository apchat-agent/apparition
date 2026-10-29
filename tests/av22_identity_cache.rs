use appview_rs::{
    identity::{
        DidDocument, DidService, IdentityBackend, IdentityResolver, ResolutionError,
        SqliteIdentityCache,
    },
    ingest::{Ingestor, MemoryStorage, ScopeMode},
};
use serde_json::json;
use std::{
    collections::HashMap,
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, SystemTime, UNIX_EPOCH},
};

const ALICE: &str = "did:plc:alice";
const BOB: &str = "did:plc:bob";

#[derive(Clone, Default)]
struct FakeIdentityBackend {
    documents: Arc<Mutex<HashMap<String, DidDocument>>>,
    handles: Arc<Mutex<HashMap<String, String>>>,
    document_calls: Arc<AtomicUsize>,
}

impl FakeIdentityBackend {
    fn publish(&self, did: &str, handle: Option<&str>, pds: &str) {
        self.documents.lock().unwrap().insert(
            did.to_owned(),
            DidDocument {
                id: did.to_owned(),
                also_known_as: handle
                    .into_iter()
                    .map(|handle| format!("at://{handle}"))
                    .collect(),
                verification_method: vec![],
                service: vec![DidService {
                    id: "#atproto_pds".to_owned(),
                    type_name: "AtprotoPersonalDataServer".to_owned(),
                    service_endpoint: pds.to_owned(),
                }],
            },
        );
        if let Some(handle) = handle {
            self.handles
                .lock()
                .unwrap()
                .insert(handle.to_owned(), did.to_owned());
        }
    }

    fn document_call_count(&self) -> usize {
        self.document_calls.load(Ordering::Relaxed)
    }
}

impl IdentityBackend for FakeIdentityBackend {
    async fn did_document(&self, did: &str) -> Result<Option<DidDocument>, ResolutionError> {
        self.document_calls.fetch_add(1, Ordering::Relaxed);
        Ok(self.documents.lock().unwrap().get(did).cloned())
    }

    async fn dns_txt_did(&self, handle: &str) -> Result<Option<String>, ResolutionError> {
        Ok(self.handles.lock().unwrap().get(handle).cloned())
    }

    async fn well_known_did(&self, _handle: &str) -> Result<Option<String>, ResolutionError> {
        Ok(None)
    }
}

fn database_path() -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "appview-rs-av22-identity-{}-{unique}.sqlite",
        std::process::id()
    ))
}

fn cleanup_database(path: &PathBuf) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(path.with_extension("sqlite-wal"));
    let _ = std::fs::remove_file(path.with_extension("sqlite-shm"));
}

#[tokio::test]
async fn identity_and_pds_are_warm_after_a_resolver_restart() {
    let path = database_path();
    let cache = Arc::new(SqliteIdentityCache::open(&path).unwrap());
    let backend = FakeIdentityBackend::default();
    backend.publish(ALICE, Some("alice.test"), "https://alice-pds.test");
    {
        let resolver = IdentityResolver::new(backend.clone(), Duration::ZERO)
            .with_persistent_cache(cache.clone());
        let identity = resolver.resolve_did(ALICE).await.unwrap();
        assert_eq!(identity.handle.as_deref(), Some("alice.test"));
        assert_eq!(
            resolver
                .resolve_did_document(ALICE)
                .await
                .unwrap()
                .pds_endpoint()
                .as_deref(),
            Some("https://alice-pds.test")
        );
    }
    drop(cache);

    let restarted_cache = Arc::new(SqliteIdentityCache::open(&path).unwrap());
    let restarted_backend = FakeIdentityBackend::default();
    let restarted = IdentityResolver::new(restarted_backend.clone(), Duration::ZERO)
        .with_persistent_cache(restarted_cache.clone());
    let identity = restarted.resolve_did(ALICE).await.unwrap();
    let document = restarted.resolve_did_document(ALICE).await.unwrap();

    assert_eq!(identity.handle.as_deref(), Some("alice.test"));
    assert_eq!(
        document.pds_endpoint().as_deref(),
        Some("https://alice-pds.test")
    );
    assert_eq!(backend.document_call_count(), 1);
    assert_eq!(restarted_backend.document_call_count(), 0);
    drop(restarted_cache);
    cleanup_database(&path);
}

#[tokio::test]
async fn not_found_identity_is_negative_cached_across_a_resolver_restart() {
    let path = database_path();
    let cache = Arc::new(SqliteIdentityCache::open(&path).unwrap());
    let backend = FakeIdentityBackend::default();
    let resolver =
        IdentityResolver::new(backend.clone(), Duration::ZERO).with_persistent_cache(cache.clone());
    assert!(resolver.resolve_did_document(BOB).await.is_err());
    drop(resolver);
    drop(cache);

    let restarted_backend = FakeIdentityBackend::default();
    let restarted = IdentityResolver::new(restarted_backend.clone(), Duration::ZERO)
        .with_persistent_cache(Arc::new(SqliteIdentityCache::open(&path).unwrap()));
    assert!(restarted.resolve_did_document(BOB).await.is_err());
    assert_eq!(backend.document_call_count(), 1);
    assert_eq!(restarted_backend.document_call_count(), 0);
    cleanup_database(&path);
}

#[tokio::test]
async fn jetstream_identity_and_account_events_invalidate_persisted_did_documents() {
    let path = database_path();
    let cache = Arc::new(SqliteIdentityCache::open(&path).unwrap());
    let old_backend = FakeIdentityBackend::default();
    old_backend.publish(ALICE, Some("alice.test"), "https://old-alice-pds.test");
    old_backend.publish(BOB, Some("bob.test"), "https://old-bob-pds.test");
    let resolver =
        IdentityResolver::new(old_backend, Duration::ZERO).with_persistent_cache(cache.clone());
    resolver.resolve_did_document(ALICE).await.unwrap();
    resolver.resolve_did_document(BOB).await.unwrap();

    let ingestor = Ingestor::new(Arc::new(MemoryStorage::default()), ScopeMode::Full)
        .with_identity_cache(cache.clone());
    ingestor
        .process_json_batch(&[
            json!({"did":ALICE,"time_us":1,"kind":"identity"}).to_string(),
            json!({"did":BOB,"time_us":2,"kind":"account"}).to_string(),
        ])
        .unwrap();

    let new_backend = FakeIdentityBackend::default();
    new_backend.publish(ALICE, Some("alice.test"), "https://new-alice-pds.test");
    new_backend.publish(BOB, Some("bob.test"), "https://new-bob-pds.test");
    let refreshed = IdentityResolver::new(new_backend.clone(), Duration::ZERO)
        .with_persistent_cache(cache.clone());
    assert_eq!(
        refreshed
            .resolve_did_document(ALICE)
            .await
            .unwrap()
            .pds_endpoint()
            .as_deref(),
        Some("https://new-alice-pds.test")
    );
    assert_eq!(
        refreshed
            .resolve_did_document(BOB)
            .await
            .unwrap()
            .pds_endpoint()
            .as_deref(),
        Some("https://new-bob-pds.test")
    );
    assert_eq!(new_backend.document_call_count(), 2);
    drop(cache);
    cleanup_database(&path);
}
