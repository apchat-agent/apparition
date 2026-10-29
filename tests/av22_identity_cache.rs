use appview_rs::{
    config::{database_paths, identity_cache_path},
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
    let unique = 0;
    std::env::temp_dir().join(format!(
        "appview-rs-av22-identity-{}-{unique}.sqlite",
        std::process::id()
    ))
}

#[test]
fn parallel_identity_cache_tests_get_distinct_database_paths() {
    let paths = (0..32)
        .map(|_| std::thread::spawn(database_path))
        .map(|thread| thread.join().unwrap())
        .collect::<std::collections::HashSet<_>>();
    assert_eq!(
        paths.len(),
        32,
        "parallel tests reused an identity cache path"
    );
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
    assert!(cache.flush(), "identity cache writer did not flush");
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
    assert!(cache.flush(), "identity cache writer did not flush");
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
    let resolver = IdentityResolver::new(old_backend.clone(), Duration::ZERO)
        .with_persistent_cache(cache.clone());
    resolver.resolve_did(ALICE).await.unwrap();
    resolver.resolve_did(BOB).await.unwrap();

    let ingestor = Ingestor::new(Arc::new(MemoryStorage::default()), ScopeMode::Full)
        .with_identity_cache(cache.clone());
    ingestor
        .process_json_batch(&[
            json!({"did":ALICE,"time_us":1,"kind":"identity"}).to_string(),
            json!({"did":BOB,"time_us":2,"kind":"account"}).to_string(),
        ])
        .unwrap();

    old_backend.publish(ALICE, Some("alice.test"), "https://new-alice-pds.test");
    old_backend.publish(BOB, Some("bob.test"), "https://new-bob-pds.test");
    assert_eq!(
        resolver
            .resolve_did_document(ALICE)
            .await
            .unwrap()
            .pds_endpoint()
            .as_deref(),
        Some("https://new-alice-pds.test")
    );
    assert_eq!(
        resolver
            .resolve_did_document(BOB)
            .await
            .unwrap()
            .pds_endpoint()
            .as_deref(),
        Some("https://new-bob-pds.test")
    );
    assert_eq!(old_backend.document_call_count(), 4);
    drop(cache);
    cleanup_database(&path);
}

#[tokio::test(flavor = "current_thread")]
async fn identity_cache_writer_lock_never_blocks_resolution_or_tokio_progress() {
    let path = database_path();
    let cache = Arc::new(SqliteIdentityCache::open(&path).unwrap());
    let backend = FakeIdentityBackend::default();
    let dids = (0..20)
        .map(|index| format!("did:plc:user{index}"))
        .collect::<Vec<_>>();
    for did in &dids {
        backend.publish(did, None, "https://pds.test");
    }
    let resolver = Arc::new(
        IdentityResolver::new(backend, Duration::from_secs(30))
            .with_persistent_cache(cache.clone()),
    );

    let blocker = rusqlite::Connection::open(&path).unwrap();
    blocker
        .execute_batch("PRAGMA journal_mode = WAL; BEGIN IMMEDIATE;")
        .unwrap();
    let release_thread = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(3));
        blocker.execute_batch("ROLLBACK;").unwrap();
    });

    let ticks = Arc::new(AtomicUsize::new(0));
    let ticker_ticks = ticks.clone();
    let ticker = tokio::spawn(async move {
        loop {
            ticker_ticks.fetch_add(1, Ordering::Relaxed);
            tokio::task::yield_now().await;
        }
    });
    tokio::task::yield_now().await;
    let initial_ticks = ticks.load(Ordering::Relaxed);

    let started = std::time::Instant::now();
    let resolved =
        futures_util::future::join_all(dids.iter().map(|did| resolver.resolve_did(did))).await;
    let elapsed = started.elapsed();

    assert!(resolved.iter().all(Result::is_ok));
    assert!(
        elapsed < Duration::from_millis(100),
        "identity resolution waited {elapsed:?} for a SQLite writer"
    );
    assert!(
        ticks.load(Ordering::Relaxed) > initial_ticks,
        "the current-thread Tokio worker stopped making progress during identity resolution"
    );

    ticker.abort();
    release_thread.join().unwrap();
    drop(cache);
    cleanup_database(&path);
}

#[tokio::test(flavor = "current_thread")]
async fn account_event_evicts_identity_cache_immediately_while_sqlite_is_locked() {
    let path = database_path();
    let cache = Arc::new(SqliteIdentityCache::open(&path).unwrap());
    let backend = FakeIdentityBackend::default();
    backend.publish(ALICE, Some("alice.test"), "https://old-alice-pds.test");
    let resolver = IdentityResolver::new(backend.clone(), Duration::from_secs(30))
        .with_persistent_cache(cache.clone());
    assert_eq!(
        resolver
            .resolve_did_document(ALICE)
            .await
            .unwrap()
            .pds_endpoint()
            .as_deref(),
        Some("https://old-alice-pds.test")
    );
    backend.publish(ALICE, Some("alice.test"), "https://new-alice-pds.test");

    let blocker = rusqlite::Connection::open(&path).unwrap();
    blocker
        .execute_batch("PRAGMA journal_mode = WAL; BEGIN IMMEDIATE;")
        .unwrap();
    let release_thread = std::thread::spawn(move || {
        std::thread::sleep(Duration::from_secs(3));
        blocker.execute_batch("ROLLBACK;").unwrap();
    });

    let ingestor = Ingestor::new(Arc::new(MemoryStorage::default()), ScopeMode::Full)
        .with_identity_cache(cache.clone());
    let started = std::time::Instant::now();
    ingestor
        .process_json_batch(&[json!({"did":ALICE,"time_us":1,"kind":"account"}).to_string()])
        .unwrap();
    let invalidation_elapsed = started.elapsed();

    let refreshed = resolver.resolve_did_document(ALICE).await.unwrap();
    assert_eq!(
        refreshed.pds_endpoint().as_deref(),
        Some("https://new-alice-pds.test")
    );
    assert_eq!(backend.document_call_count(), 2);
    release_thread.join().unwrap();
    assert!(
        invalidation_elapsed < Duration::from_millis(100),
        "Jetstream account invalidation waited {invalidation_elapsed:?} for SQLite"
    );

    drop(cache);
    cleanup_database(&path);
}

#[tokio::test(flavor = "current_thread")]
async fn records_database_write_lock_does_not_affect_identity_resolution() {
    let data_dir = std::env::temp_dir().join(format!(
        "appview-rs-av22-db-isolation-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&data_dir).unwrap();
    let (records_path, _) = database_paths(data_dir.to_str().unwrap());
    let cache_path = identity_cache_path(data_dir.to_str().unwrap());
    assert_ne!(records_path, cache_path);
    let cache = Arc::new(SqliteIdentityCache::open(cache_path).unwrap());

    let records_writer = rusqlite::Connection::open(records_path).unwrap();
    records_writer
        .execute_batch("PRAGMA journal_mode = WAL; BEGIN IMMEDIATE;")
        .unwrap();
    let backend = FakeIdentityBackend::default();
    backend.publish(ALICE, Some("alice.test"), "https://alice-pds.test");
    let resolver =
        IdentityResolver::new(backend, Duration::from_secs(30)).with_persistent_cache(cache);

    let started = std::time::Instant::now();
    let identity = resolver.resolve_did(ALICE).await.unwrap();
    let elapsed = started.elapsed();
    assert_eq!(identity.did, ALICE);
    assert!(!records_writer.is_autocommit());
    assert!(
        elapsed < Duration::from_millis(100),
        "identity resolution waited {elapsed:?} for a records database writer"
    );

    records_writer.execute_batch("ROLLBACK;").unwrap();
    drop(resolver);
    let _ = std::fs::remove_dir_all(data_dir);
}
