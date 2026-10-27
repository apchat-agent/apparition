use appview_rs::{
    identity::{
        DidDocument, HandleRefreshWorker, IdentityBackend, IdentityResolver, ResolutionError,
    },
    ingest::{Ingestor, MemoryStorage, ScopeMode, SqliteStorage, Storage},
};
use serde_json::json;
use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
    time::Duration,
};

#[derive(Clone, Default)]
struct FakeIdentityBackend {
    documents: Arc<RwLock<HashMap<String, DidDocument>>>,
    dns: Arc<RwLock<HashMap<String, String>>>,
}

impl FakeIdentityBackend {
    fn publish(&self, did: &str, handle: Option<&str>) {
        let aliases = handle
            .into_iter()
            .map(|handle| format!("at://{handle}"))
            .collect();
        self.documents.write().unwrap().insert(
            did.to_owned(),
            DidDocument {
                id: did.to_owned(),
                also_known_as: aliases,
                verification_method: vec![],
                service: vec![],
            },
        );
        self.dns
            .write()
            .unwrap()
            .retain(|_, found_did| found_did != did);
        if let Some(handle) = handle {
            self.dns
                .write()
                .unwrap()
                .insert(handle.to_owned(), did.to_owned());
        }
    }
}

impl IdentityBackend for FakeIdentityBackend {
    async fn did_document(&self, did: &str) -> Result<Option<DidDocument>, ResolutionError> {
        Ok(self.documents.read().unwrap().get(did).cloned())
    }

    async fn dns_txt_did(&self, handle: &str) -> Result<Option<String>, ResolutionError> {
        Ok(self.dns.read().unwrap().get(handle).cloned())
    }

    async fn well_known_did(&self, _handle: &str) -> Result<Option<String>, ResolutionError> {
        Ok(None)
    }
}

#[test]
fn stale_sqlite_handle_remains_available_until_refresh_verifies_a_change() {
    let path =
        std::env::temp_dir().join(format!("appview-rs-av11-stale-{}.db", std::process::id()));
    let _ = std::fs::remove_file(&path);
    let storage = SqliteStorage::open(&path).unwrap();
    storage
        .set_verified_handle("did:plc:alice", "alice.test")
        .unwrap();
    drop(storage);

    let connection = rusqlite::Connection::open(&path).unwrap();
    connection
        .execute(
            "UPDATE verified_handles SET updated_at=0 WHERE did=?1",
            ["did:plc:alice"],
        )
        .unwrap();
    drop(connection);

    let storage = SqliteStorage::open(&path).unwrap();
    assert_eq!(
        storage.verified_handle("did:plc:alice").unwrap().as_deref(),
        Some("alice.test"),
        "an expired verification stays visible while refresh is pending"
    );
    let _ = std::fs::remove_file(&path);
}

#[tokio::test]
async fn stale_handle_refresh_updates_after_bidirectional_verification() {
    let storage = Arc::new(MemoryStorage::default());
    storage.register_user("did:plc:alice").unwrap();
    storage
        .set_verified_handle("did:plc:alice", "old.test")
        .unwrap();
    let backend = FakeIdentityBackend::default();
    backend.publish("did:plc:alice", Some("new.test"));
    let resolver = IdentityResolver::new(backend, Duration::ZERO);
    let worker = HandleRefreshWorker::new(
        storage.clone(),
        resolver,
        Duration::ZERO,
        Duration::from_secs(60),
    );

    assert_eq!(worker.refresh_once().await.unwrap(), 1);
    assert_eq!(
        storage.verified_handle("did:plc:alice").unwrap().as_deref(),
        Some("new.test")
    );
}

#[tokio::test]
async fn failed_bidirectional_verification_clears_the_last_handle() {
    let storage = Arc::new(MemoryStorage::default());
    storage.register_user("did:plc:alice").unwrap();
    storage
        .set_verified_handle("did:plc:alice", "old.test")
        .unwrap();
    let backend = FakeIdentityBackend::default();
    backend.publish("did:plc:alice", None);
    let worker = HandleRefreshWorker::new(
        storage.clone(),
        IdentityResolver::new(backend, Duration::ZERO),
        Duration::ZERO,
        Duration::from_secs(60),
    );

    assert_eq!(worker.refresh_once().await.unwrap(), 1);
    assert_eq!(storage.verified_handle("did:plc:alice").unwrap(), None);
    assert!(storage.handle_is_invalid("did:plc:alice").unwrap());
}

#[tokio::test]
async fn missing_did_document_invalidates_that_handle_and_does_not_block_the_batch() {
    let storage = Arc::new(MemoryStorage::default());
    storage.register_user("did:plc:alice").unwrap();
    storage.register_user("did:plc:bob").unwrap();
    storage
        .set_verified_handle("did:plc:alice", "old-alice.test")
        .unwrap();
    storage
        .set_verified_handle("did:plc:bob", "old-bob.test")
        .unwrap();
    let backend = FakeIdentityBackend::default();
    backend.publish("did:plc:bob", Some("bob.test"));
    let worker = HandleRefreshWorker::new(
        storage.clone(),
        IdentityResolver::new(backend, Duration::ZERO),
        Duration::ZERO,
        Duration::from_secs(60),
    );

    assert_eq!(worker.refresh_once().await.unwrap(), 2);
    assert!(storage.handle_is_invalid("did:plc:alice").unwrap());
    assert_eq!(
        storage.verified_handle("did:plc:bob").unwrap().as_deref(),
        Some("bob.test")
    );
}

#[tokio::test]
async fn tracked_identity_event_queues_handle_reverification() {
    let storage = Arc::new(MemoryStorage::default());
    storage.register_user("did:plc:alice").unwrap();
    storage
        .set_verified_handle("did:plc:alice", "old.test")
        .unwrap();
    let backend = FakeIdentityBackend::default();
    backend.publish("did:plc:alice", Some("new.test"));
    let worker = HandleRefreshWorker::new(
        storage.clone(),
        IdentityResolver::new(backend, Duration::ZERO),
        Duration::from_secs(3600),
        Duration::from_secs(60),
    );
    let ingestor = Ingestor::new(storage.clone(), ScopeMode::Scoped);
    ingestor
        .process_json(
            &json!({
                "did":"did:plc:alice",
                "time_us":123,
                "kind":"identity",
                "identity":{"did":"did:plc:alice","handle":"new.test"}
            })
            .to_string(),
        )
        .unwrap();

    assert_eq!(worker.refresh_once().await.unwrap(), 1);
    assert_eq!(
        storage.verified_handle("did:plc:alice").unwrap().as_deref(),
        Some("new.test")
    );
}
