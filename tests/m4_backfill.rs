use appview_rs::{
    backfill::{AtriumRepoVerifier, HostRateLimiter, HttpRepoSource, RepoBackfiller, RepoVerifier},
    identity::{DidDocument, DidService, VerificationMethod},
    ingest::{MemoryStorage, Storage},
};
use atrium_api::types::string::Did;
use atrium_crypto::keypair::{Did as _, P256Keypair};
use atrium_repo::{
    Repository,
    blockstore::{AsyncBlockStoreRead, AsyncBlockStoreWrite, MemoryBlockStore, SHA2_256},
};
use axum::{
    Router,
    body::Body,
    extract::{Query, State},
    http::StatusCode,
    response::Response,
    routing::get,
};
use serde::Serialize;
use serde_json::json;
use std::{
    collections::{BTreeMap, HashMap},
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::net::TcpListener;

#[derive(Clone)]
struct FakePds {
    car: Arc<Vec<u8>>,
    requests: Arc<AtomicUsize>,
    dids: Arc<Mutex<Vec<String>>>,
}

async fn get_repo(
    State(state): State<FakePds>,
    Query(query): Query<HashMap<String, String>>,
) -> Response<Body> {
    state.requests.fetch_add(1, Ordering::SeqCst);
    if let Some(did) = query.get("did") {
        state.dids.lock().unwrap().push(did.clone());
    }
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/vnd.ipld.car")
        .body(Body::from(state.car.as_ref().clone()))
        .unwrap()
}

#[derive(Serialize)]
struct CarHeader {
    roots: Vec<atrium_repo::Cid>,
    version: u64,
}

fn write_varint(mut value: usize, output: &mut Vec<u8>) {
    while value >= 0x80 {
        output.push((value as u8) | 0x80);
        value >>= 7;
    }
    output.push(value as u8);
}

async fn signed_repo_car(did: &str) -> (Vec<u8>, String) {
    let mut rng = rand::thread_rng();
    let key = P256Keypair::create(&mut rng);
    let signing_did_key = key.did();
    let repo_builder =
        Repository::create(MemoryBlockStore::new(), Did::new(did.to_owned()).unwrap())
            .await
            .unwrap();
    let root_sig = key.sign(&repo_builder.bytes()).unwrap();
    let mut repo = repo_builder.finalize(root_sig).await.unwrap();
    let (commit, _) = repo.add_raw(
        "app.bsky.feed.post/post1",
        json!({"$type":"app.bsky.feed.post", "text":"backfilled", "createdAt":"2026-10-27T00:00:00Z"}),
    ).await.unwrap();
    let sig = key.sign(&commit.bytes()).unwrap();
    commit.finalize(sig).await.unwrap();

    let root = repo.root();
    let cids = repo.export().await.unwrap().collect::<Vec<_>>();
    let mut export_store = MemoryBlockStore::new();
    repo.export_into(&mut export_store).await.unwrap();
    let mut car = Vec::new();
    let header = serde_ipld_dagcbor::to_vec(&CarHeader {
        roots: vec![root],
        version: 1,
    })
    .unwrap();
    write_varint(header.len(), &mut car);
    car.extend(header);
    for cid in cids {
        let block = export_store.read_block(cid).await.unwrap();
        let cid_bytes = cid.to_bytes();
        write_varint(cid_bytes.len() + block.len(), &mut car);
        car.extend(cid_bytes);
        car.extend(block);
    }
    (car, signing_did_key)
}

async fn signed_repo_car_with_media(did: &str) -> (Vec<u8>, String, String) {
    use ipld_core::ipld::Ipld;

    fn map(entries: impl IntoIterator<Item = (String, Ipld)>) -> Ipld {
        Ipld::Map(entries.into_iter().collect::<BTreeMap<_, _>>())
    }

    let mut rng = rand::thread_rng();
    let key = P256Keypair::create(&mut rng);
    let signing_did_key = key.did();
    let mut store = MemoryBlockStore::new();
    let blob_cid = store
        .write_block(0x55, SHA2_256, b"fake image bytes")
        .await
        .unwrap();
    let repo_builder = Repository::create(store, Did::new(did.to_owned()).unwrap())
        .await
        .unwrap();
    let root_sig = key.sign(&repo_builder.bytes()).unwrap();
    let mut repo = repo_builder.finalize(root_sig).await.unwrap();

    let records = [
        (
            "app.bsky.feed.post/post1",
            map([
                ("$type".into(), Ipld::String("app.bsky.feed.post".into())),
                ("text".into(), Ipld::String("photo".into())),
                (
                    "createdAt".into(),
                    Ipld::String("2026-10-27T00:00:00Z".into()),
                ),
                (
                    "embed".into(),
                    map([
                        ("$type".into(), Ipld::String("app.bsky.embed.images".into())),
                        (
                            "images".into(),
                            Ipld::List(vec![map([
                                ("alt".into(), Ipld::String("a photo".into())),
                                (
                                    "image".into(),
                                    map([
                                        ("$type".into(), Ipld::String("blob".into())),
                                        ("ref".into(), Ipld::Link(blob_cid)),
                                        ("mimeType".into(), Ipld::String("image/jpeg".into())),
                                        ("size".into(), Ipld::Integer(16)),
                                    ]),
                                ),
                            ])]),
                        ),
                    ]),
                ),
                ("byteFixture".into(), Ipld::Bytes(vec![0xfb, 0xff])),
            ]),
        ),
        (
            "app.bsky.actor.profile/self",
            map([
                (
                    "$type".into(),
                    Ipld::String("app.bsky.actor.profile".into()),
                ),
                (
                    "avatar".into(),
                    map([
                        ("$type".into(), Ipld::String("blob".into())),
                        ("ref".into(), Ipld::Link(blob_cid)),
                        ("mimeType".into(), Ipld::String("image/jpeg".into())),
                        ("size".into(), Ipld::Integer(16)),
                    ]),
                ),
            ]),
        ),
    ];

    for (path, record) in records {
        let (commit, _) = repo.add_raw(path, record).await.unwrap();
        let signature = key.sign(&commit.bytes()).unwrap();
        commit.finalize(signature).await.unwrap();
    }

    let root = repo.root();
    let cids = repo.export().await.unwrap().collect::<Vec<_>>();
    let mut export_store = MemoryBlockStore::new();
    repo.export_into(&mut export_store).await.unwrap();
    let mut car = Vec::new();
    let header = serde_ipld_dagcbor::to_vec(&CarHeader {
        roots: vec![root],
        version: 1,
    })
    .unwrap();
    write_varint(header.len(), &mut car);
    car.extend(header);
    for cid in cids {
        let block = export_store.read_block(cid).await.unwrap();
        let cid_bytes = cid.to_bytes();
        write_varint(cid_bytes.len() + block.len(), &mut car);
        car.extend(cid_bytes);
        car.extend(block);
    }
    (car, signing_did_key, blob_cid.to_string())
}

async fn signed_repo_car_with_bad_record(did: &str) -> (Vec<u8>, String, String) {
    let mut rng = rand::thread_rng();
    let key = P256Keypair::create(&mut rng);
    let signing_did_key = key.did();
    let repo_builder =
        Repository::create(MemoryBlockStore::new(), Did::new(did.to_owned()).unwrap())
            .await
            .unwrap();
    let root_sig = key.sign(&repo_builder.bytes()).unwrap();
    let mut repo = repo_builder.finalize(root_sig).await.unwrap();

    let (commit, bad_cid) = repo
        .add_raw(
            "app.bsky.feed.post/bad",
            BadLinkRecord {
                type_name: "app.bsky.feed.post",
                text: "contains an invalid CID tag",
                bad_link: InvalidCid,
            },
        )
        .await
        .unwrap();
    let signature = key.sign(&commit.bytes()).unwrap();
    commit.finalize(signature).await.unwrap();
    let (commit, _) = repo
        .add_raw(
            "app.bsky.feed.post/good",
            json!({"text":"keep me", "createdAt":"2026-10-27T00:00:00Z"}),
        )
        .await
        .unwrap();
    let signature = key.sign(&commit.bytes()).unwrap();
    commit.finalize(signature).await.unwrap();

    let root = repo.root();
    let cids = repo.export().await.unwrap().collect::<Vec<_>>();
    let mut export_store = MemoryBlockStore::new();
    repo.export_into(&mut export_store).await.unwrap();
    let mut car = Vec::new();
    let header = serde_ipld_dagcbor::to_vec(&CarHeader {
        roots: vec![root],
        version: 1,
    })
    .unwrap();
    write_varint(header.len(), &mut car);
    car.extend(header);
    for cid in cids {
        let block = export_store.read_block(cid).await.unwrap();
        let cid_bytes = cid.to_bytes();
        write_varint(cid_bytes.len() + block.len(), &mut car);
        car.extend(cid_bytes);
        car.extend(block);
    }
    (car, signing_did_key, bad_cid.to_string())
}

#[derive(Serialize)]
struct BadLinkRecord<'a> {
    #[serde(rename = "$type")]
    type_name: &'a str,
    text: &'a str,
    bad_link: InvalidCid,
}

struct InvalidCid;

impl Serialize for InvalidCid {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_newtype_struct(
            ipld_core::cid::serde::CID_SERDE_PRIVATE_IDENTIFIER,
            &InvalidCidBytes,
        )
    }
}

struct InvalidCidBytes;

impl Serialize for InvalidCidBytes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_bytes(&[0xff])
    }
}

#[tokio::test]
async fn backfill_decodes_cid_links_and_bytes_into_atproto_json() {
    let did = "did:plc:media";
    let (car, signing_did_key, blob_cid) = signed_repo_car_with_media(did).await;
    let verified = AtriumRepoVerifier
        .verify(&car, did, &signing_did_key)
        .await
        .unwrap();

    let post = verified
        .records
        .iter()
        .find(|record| record.collection == "app.bsky.feed.post")
        .unwrap();
    assert_eq!(
        post.record["embed"]["images"][0]["image"]["ref"]["$link"],
        blob_cid
    );
    assert_eq!(post.record["byteFixture"]["$bytes"], "+/8");

    let profile = verified
        .records
        .iter()
        .find(|record| record.collection == "app.bsky.actor.profile")
        .unwrap();
    assert_eq!(profile.record["avatar"]["ref"]["$link"], blob_cid);
}

#[tokio::test]
async fn verifier_skips_an_undecodable_record_and_keeps_the_rest_of_the_repo() {
    let did = "did:plc:onebadrecord";
    let (car, signing_did_key, bad_cid) = signed_repo_car_with_bad_record(did).await;
    let verified = AtriumRepoVerifier
        .verify(&car, did, &signing_did_key)
        .await
        .unwrap();

    assert_eq!(verified.records.len(), 1);
    assert_eq!(verified.records[0].rkey, "good");
    assert_ne!(verified.records[0].cid, bad_cid);
}

#[tokio::test]
async fn get_repo_car_is_verified_and_reconciled_with_a_per_host_rate_limit() {
    let did = "did:plc:alice";
    let (car, signing_did_key) = signed_repo_car(did).await;
    let key_multibase = signing_did_key.strip_prefix("did:key:").unwrap();
    let did_document = DidDocument {
        id: did.to_owned(),
        also_known_as: vec!["at://alice.test".into()],
        verification_method: vec![VerificationMethod {
            id: "#atproto".into(),
            type_name: "Multikey".into(),
            controller: did.to_owned(),
            public_key_multibase: key_multibase.to_owned(),
        }],
        service: vec![DidService {
            id: "#atproto_pds".into(),
            type_name: "AtprotoPersonalDataServer".into(),
            service_endpoint: "http://localhost:3001".into(),
        }],
    };
    assert_eq!(
        did_document.atproto_signing_key().as_deref(),
        Some(signing_did_key.as_str())
    );
    assert_eq!(
        did_document.pds_endpoint().as_deref(),
        Some("http://localhost:3001")
    );

    let state = FakePds {
        car: Arc::new(car),
        requests: Arc::new(AtomicUsize::new(0)),
        dids: Arc::new(Mutex::new(Vec::new())),
    };
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server_state = state.clone();
    tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route("/xrpc/com.atproto.sync.getRepo", get(get_repo))
                .with_state(server_state),
        )
        .await
        .unwrap();
    });
    let pds = format!("http://{address}");
    let storage = Arc::new(MemoryStorage::default());
    storage.register_user(did).unwrap();
    storage
        .put_record(&appview_rs::ingest::IndexedRecord {
            did: did.into(),
            collection: "app.bsky.feed.post".into(),
            rkey: "stale".into(),
            uri: format!("at://{did}/app.bsky.feed.post/stale"),
            cid: None,
            time_us: 1,
            record: json!({"text":"stale"}),
        })
        .unwrap();
    // The same stale record, but inside the default 30-day backfill window:
    // the new snapshot proves it is gone, so it must be deleted as before.
    storage
        .put_record(&appview_rs::ingest::IndexedRecord {
            did: did.into(),
            collection: "app.bsky.feed.post".into(),
            rkey: "stale-in-window".into(),
            uri: format!("at://{did}/app.bsky.feed.post/stale-in-window"),
            cid: None,
            time_us: {
                let now = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_micros() as u64;
                now - 10 * 86_400_000_000
            },
            record: json!({"text":"stale inside the window"}),
        })
        .unwrap();
    let backfiller = RepoBackfiller::new(
        storage.clone(),
        HttpRepoSource::with_http_test_hosts(["127.0.0.1".to_owned()]).unwrap(),
        AtriumRepoVerifier,
        HostRateLimiter::new(Duration::from_millis(20)),
    );

    let started = Instant::now();
    let count = backfiller
        .backfill(did, &pds, &signing_did_key)
        .await
        .unwrap();
    let second_count = backfiller
        .backfill(did, &pds, &signing_did_key)
        .await
        .unwrap();

    assert_eq!(count, 1);
    assert_eq!(second_count, 1);
    assert!(started.elapsed() >= Duration::from_millis(20));
    assert_eq!(state.requests.load(Ordering::SeqCst), 2);
    assert_eq!(state.dids.lock().unwrap().as_slice(), [did, did]);
    assert!(
        storage
            .get_record(did, "app.bsky.feed.post", "post1")
            .unwrap()
            .is_some()
    );
    // The windowed snapshot says nothing about records older than its window
    // start (2026-11-01 lane av75q): the pre-window stale row stands, like a
    // 200-day post stored while someone scrolled the profile.
    assert!(
        storage
            .get_record(did, "app.bsky.feed.post", "stale")
            .unwrap()
            .is_some(),
        "rows older than the backfill window must survive a windowed snapshot"
    );
    assert!(
        storage
            .get_record(did, "app.bsky.feed.post", "stale-in-window")
            .unwrap()
            .is_none(),
        "rows inside the window that the snapshot does not carry are deleted as before"
    );

    let bad_key = P256Keypair::create(&mut rand::thread_rng()).did();
    assert!(backfiller.backfill(did, &pds, &bad_key).await.is_err());
    assert!(
        storage
            .get_record(did, "app.bsky.feed.post", "post1")
            .unwrap()
            .is_some()
    );
}

#[test]
fn entering_scope_queues_registered_and_newly_followed_repositories() {
    use appview_rs::ingest::{Ingestor, ScopeMode};

    let storage = Arc::new(MemoryStorage::default());
    storage.register_user("did:plc:root").unwrap();
    assert_eq!(storage.pending_backfills().unwrap(), ["did:plc:root"]);
    let ingestor = Ingestor::new(storage.clone(), ScopeMode::Scoped);
    let follow = json!({
        "did":"did:plc:root",
        "time_us":9,
        "kind":"commit",
        "commit":{
            "operation":"create",
            "collection":"app.bsky.graph.follow",
            "rkey":"follow1",
            "cid":"follow-cid",
            "record":{"subject":"did:plc:friend"}
        }
    });
    ingestor.process_json(&follow.to_string()).unwrap();
    assert_eq!(
        storage.pending_backfills().unwrap(),
        ["did:plc:friend", "did:plc:root"]
    );
}

#[test]
fn pending_backfill_batches_are_bounded() {
    fn check(storage: &impl Storage) {
        for index in 0..12 {
            storage
                .register_user(&format!("did:plc:user-{index:02}"))
                .unwrap();
        }
        assert!(storage.pending_backfills().unwrap().len() <= 8);
    }

    check(&MemoryStorage::default());
    check(&appview_rs::ingest::SqliteStorage::open(":memory:").unwrap());
}

#[test]
fn backfill_enqueue_requires_a_registered_owner_and_existing_follow() {
    fn check(storage: &impl Storage) {
        storage
            .enqueue_backfill_for("did:plc:anonymous", "did:plc:arbitrary")
            .unwrap();
        assert!(storage.pending_backfills().unwrap().is_empty());

        storage.register_user("did:plc:owner").unwrap();
        storage
            .enqueue_backfill_for("did:plc:owner", "did:plc:not-followed")
            .unwrap();
        assert_eq!(storage.pending_backfills().unwrap(), ["did:plc:owner"]);
    }

    check(&MemoryStorage::default());
    check(&appview_rs::ingest::SqliteStorage::open(":memory:").unwrap());
}

#[test]
fn registered_owner_cannot_queue_repository_outside_follow_scope() {
    fn check(storage: &impl Storage) {
        storage.register_user("did:plc:owner").unwrap();
        storage
            .enqueue_backfill_for("did:plc:owner", "did:plc:not-followed")
            .unwrap();
        assert_eq!(storage.pending_backfills().unwrap(), ["did:plc:owner"]);
    }

    check(&MemoryStorage::default());
    check(&appview_rs::ingest::SqliteStorage::open(":memory:").unwrap());
}

#[test]
fn registered_repo_backfill_queues_followed_accounts_and_tracks_completion() {
    fn check<S: Storage>(storage: &S) {
        storage.register_user("did:plc:root").unwrap();
        storage
            .replace_repo_snapshot(
                "did:plc:root",
                &[],
                &["did:plc:friend".to_owned(), "did:plc:other".to_owned()],
            )
            .unwrap();
        assert_eq!(
            storage.pending_backfills().unwrap(),
            ["did:plc:friend", "did:plc:other", "did:plc:root"]
        );
        storage.complete_backfill("did:plc:root").unwrap();
        storage.complete_backfill("did:plc:friend").unwrap();
        assert_eq!(storage.backfill_progress("did:plc:root").unwrap(), (3, 2));
        assert_eq!(storage.pending_backfills().unwrap(), ["did:plc:other"]);
    }

    check(&MemoryStorage::default());
    check(&appview_rs::ingest::SqliteStorage::open(":memory:").unwrap());
}

#[tokio::test]
async fn backfill_worker_drains_new_scope_entries_from_resolved_did_docs() {
    use appview_rs::{
        backfill::BackfillWorker,
        identity::{IdentityBackend, IdentityResolver, ResolutionError},
    };

    #[derive(Clone)]
    struct FakeIdentity(DidDocument);
    impl IdentityBackend for FakeIdentity {
        async fn did_document(&self, did: &str) -> Result<Option<DidDocument>, ResolutionError> {
            Ok((self.0.id == did).then(|| self.0.clone()))
        }
        async fn dns_txt_did(&self, _: &str) -> Result<Option<String>, ResolutionError> {
            Ok(None)
        }
        async fn well_known_did(&self, _: &str) -> Result<Option<String>, ResolutionError> {
            Ok(None)
        }
    }

    let did = "did:plc:worker";
    let (car, signing_did_key) = signed_repo_car(did).await;
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let state = FakePds {
        car: Arc::new(car),
        requests: Arc::new(AtomicUsize::new(0)),
        dids: Arc::new(Mutex::new(Vec::new())),
    };
    tokio::spawn(async move {
        axum::serve(
            listener,
            Router::new()
                .route("/xrpc/com.atproto.sync.getRepo", get(get_repo))
                .with_state(state),
        )
        .await
        .unwrap();
    });
    let key_multibase = signing_did_key.strip_prefix("did:key:").unwrap();
    let document = DidDocument {
        id: did.into(),
        also_known_as: vec![],
        verification_method: vec![VerificationMethod {
            id: "#atproto".into(),
            type_name: "Multikey".into(),
            controller: did.into(),
            public_key_multibase: key_multibase.into(),
        }],
        service: vec![DidService {
            id: "#atproto_pds".into(),
            type_name: "AtprotoPersonalDataServer".into(),
            service_endpoint: format!("http://{address}"),
        }],
    };
    let resolver = IdentityResolver::new(FakeIdentity(document), Duration::from_secs(30));
    let storage = Arc::new(MemoryStorage::default());
    storage.register_user(did).unwrap();
    let backfiller = RepoBackfiller::new(
        storage.clone(),
        HttpRepoSource::with_http_test_hosts(["127.0.0.1".to_owned()]).unwrap(),
        AtriumRepoVerifier,
        HostRateLimiter::new(Duration::ZERO),
    );
    let worker = BackfillWorker::new(
        storage.clone(),
        resolver,
        backfiller,
        Duration::from_millis(5),
    );
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(async move { worker.run(shutdown_rx).await });

    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if storage
                .get_record(did, "app.bsky.feed.post", "post1")
                .unwrap()
                .is_some()
            {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    let _ = shutdown_tx.send(true);
    task.await.unwrap().unwrap();
    assert!(storage.pending_backfills().unwrap().is_empty());
}

#[tokio::test]
async fn backfill_worker_backs_off_after_a_failed_repository_fetch() {
    use appview_rs::{
        backfill::{BackfillError, BackfillWorker, RepoFetcher},
        identity::{IdentityBackend, IdentityResolver, ResolutionError},
    };

    #[derive(Clone)]
    struct FakeIdentity(DidDocument);
    impl IdentityBackend for FakeIdentity {
        async fn did_document(&self, did: &str) -> Result<Option<DidDocument>, ResolutionError> {
            Ok((self.0.id == did).then(|| self.0.clone()))
        }
        async fn dns_txt_did(&self, _: &str) -> Result<Option<String>, ResolutionError> {
            Ok(None)
        }
        async fn well_known_did(&self, _: &str) -> Result<Option<String>, ResolutionError> {
            Ok(None)
        }
    }

    struct FailingFetcher(Arc<AtomicUsize>);
    impl RepoFetcher for FailingFetcher {
        async fn fetch_repo(&self, _: &str, _: &str) -> Result<Vec<u8>, BackfillError> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Err(BackfillError::Fetch("offline fake failure".into()))
        }
    }

    let did = "did:plc:retry";
    let key = P256Keypair::create(&mut rand::thread_rng()).did();
    let document = DidDocument {
        id: did.into(),
        also_known_as: vec![],
        verification_method: vec![VerificationMethod {
            id: "#atproto".into(),
            type_name: "Multikey".into(),
            controller: did.into(),
            public_key_multibase: key.strip_prefix("did:key:").unwrap().into(),
        }],
        service: vec![DidService {
            id: "#atproto_pds".into(),
            type_name: "AtprotoPersonalDataServer".into(),
            service_endpoint: "http://localhost:3001".into(),
        }],
    };
    let storage = Arc::new(MemoryStorage::default());
    storage.register_user(did).unwrap();
    let fetches = Arc::new(AtomicUsize::new(0));
    let backfiller = RepoBackfiller::new(
        storage.clone(),
        FailingFetcher(fetches.clone()),
        AtriumRepoVerifier,
        HostRateLimiter::new(Duration::ZERO),
    );
    let worker = BackfillWorker::new(
        storage.clone(),
        IdentityResolver::new(FakeIdentity(document), Duration::from_secs(30)),
        backfiller,
        Duration::from_millis(5),
    );
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(async move { worker.run(shutdown_rx).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    let _ = shutdown_tx.send(true);
    task.await.unwrap().unwrap();

    assert_eq!(fetches.load(Ordering::SeqCst), 1);
    assert!(storage.pending_backfills().unwrap().is_empty());
    assert_eq!(storage.backfill_progress(did).unwrap(), (1, 0));
    assert_eq!(storage.backfill_failure_count(did).unwrap(), 1);
}

mod windowed_backfill {
    use appview_rs::{
        backfill::{
            BackfillError, HostRateLimiter, RepoBackfiller, RepoFetcher, RepoRecord, RepoVerifier,
            VerifiedRepo,
        },
        ingest::{IndexedRecord, MemoryStorage, SqliteStorage, Storage},
    };
    use serde_json::json;
    use std::{
        io::Write,
        sync::{Arc, Mutex},
    };

    const DID: &str = "did:plc:windowowner";

    struct ScriptedFetcher;
    impl RepoFetcher for ScriptedFetcher {
        async fn fetch_repo(&self, _: &str, _: &str) -> Result<Vec<u8>, BackfillError> {
            Ok(vec![0u8; 8])
        }
    }

    struct ScriptedVerifier(Vec<RepoRecord>);
    impl RepoVerifier for ScriptedVerifier {
        async fn verify(&self, _: &[u8], _: &str, _: &str) -> Result<VerifiedRepo, BackfillError> {
            Ok(VerifiedRepo {
                records: self.0.clone(),
            })
        }
    }

    fn aged_created_at(days: i64) -> String {
        let moment = time::OffsetDateTime::now_utc() - time::Duration::days(days);
        moment
            .format(&time::format_description::well_known::Rfc3339)
            .unwrap()
    }

    fn aged_record(collection: &str, rkey: &str, days: i64) -> RepoRecord {
        RepoRecord {
            collection: collection.to_owned(),
            rkey: rkey.to_owned(),
            cid: format!("cid-{rkey}"),
            record: json!({
                "$type": collection,
                "createdAt": aged_created_at(days),
                "subject": "did:plc:windowfriend",
            }),
        }
    }

    fn windowed_repo() -> Vec<RepoRecord> {
        let mut records = Vec::new();
        for days in [5, 29, 31, 400] {
            records.push(aged_record("app.bsky.feed.post", &format!("p{days}"), days));
            records.push(aged_record("app.bsky.feed.like", &format!("l{days}"), days));
            records.push(aged_record(
                "app.bsky.feed.repost",
                &format!("r{days}"),
                days,
            ));
        }
        records.push(aged_record("app.bsky.graph.follow", "f400", 400));
        records.push(aged_record("app.bsky.graph.block", "b400", 400));
        records.push(aged_record("app.bsky.actor.profile", "self", 400));
        records
    }

    fn backfiller(
        storage: Arc<MemoryStorage>,
    ) -> RepoBackfiller<MemoryStorage, ScriptedFetcher, ScriptedVerifier> {
        RepoBackfiller::new(
            storage,
            ScriptedFetcher,
            ScriptedVerifier(windowed_repo()),
            HostRateLimiter::new(std::time::Duration::ZERO),
        )
    }

    #[derive(Clone, Default)]
    struct CaptureLogs(Arc<Mutex<Vec<u8>>>);
    impl Write for CaptureLogs {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().unwrap().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CaptureLogs {
        type Writer = CaptureLogs;
        fn make_writer(&'a self) -> Self::Writer {
            self.clone()
        }
    }

    // A thread-local `set_default` is not enough to catch the completed line:
    // `backfill` logs it after awaiting `spawn_blocking`, and the continuation
    // can run on a runtime thread that never held the guard (observed flake
    // 2026-11-01). One process-global subscriber with a shared buffer catches
    // it on any thread; the assertion filters by the test's own DID.
    fn shared_logs() -> &'static CaptureLogs {
        static CAPTURE: std::sync::OnceLock<CaptureLogs> = std::sync::OnceLock::new();
        static INIT: std::sync::Once = std::sync::Once::new();
        let logs = CAPTURE.get_or_init(CaptureLogs::default);
        INIT.call_once(|| {
            let subscriber = tracing_subscriber::fmt()
                .without_time()
                .with_ansi(false)
                .with_max_level(tracing::Level::INFO)
                .with_writer(logs.clone())
                .finish();
            let _ = tracing::subscriber::set_global_default(subscriber);
        });
        logs
    }

    fn capture_len(logs: &CaptureLogs) -> usize {
        logs.0.lock().unwrap().len()
    }

    fn completed_line_after(logs: &CaptureLogs, mark: usize, did: &str) -> Option<String> {
        let bytes = logs.0.lock().unwrap()[mark..].to_vec();
        String::from_utf8_lossy(&bytes)
            .lines()
            .find(|line| {
                line.contains("repository backfill completed")
                    && line.contains(&format!("did=\"{did}\""))
            })
            .map(str::to_owned)
    }

    fn indexed<S: Storage>(storage: &S, did: &str, collection: &str, rkey: &str) -> bool {
        storage.get_record(did, collection, rkey).unwrap().is_some()
    }

    fn now_us() -> u64 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64
    }

    fn seeded(
        did: &str,
        collection: &str,
        rkey: &str,
        age_days: i64,
        extra: serde_json::Value,
    ) -> IndexedRecord {
        let time_us = now_us() - age_days as u64 * 86_400_000_000;
        let mut record = json!({
            "$type": collection,
            "createdAt": aged_created_at(age_days),
        });
        if let (Some(target), Some(source)) = (extra.as_object(), record.as_object_mut()) {
            for (key, value) in target {
                source.insert(key.clone(), value.clone());
            }
        }
        IndexedRecord {
            did: did.to_owned(),
            collection: collection.to_owned(),
            rkey: rkey.to_owned(),
            uri: format!("at://{did}/{collection}/{rkey}"),
            cid: Some(format!("cid-{rkey}")),
            time_us,
            record,
        }
    }

    #[tokio::test]
    async fn default_window_indexes_only_recent_posts_likes_reposts_and_keeps_all_graph_state() {
        // A DID of its own so the shared log capture can single out this
        // test's completed line from the other windowed tests' lines.
        const WINDOW_DID: &str = "did:plc:windowdefault";
        let storage = Arc::new(MemoryStorage::default());
        let backfiller = backfiller(storage.clone());
        let logs = shared_logs();
        let mark = capture_len(logs);
        let count = backfiller
            .backfill(
                WINDOW_DID,
                "https://pds.example",
                "did:key:zDnaem-window-test-key",
            )
            .await
            .unwrap();

        assert_eq!(
            count, 9,
            "the 5- and 29-day windowed records plus all 400-day graph records"
        );
        for days in [5, 29] {
            for (collection, prefix) in [
                ("app.bsky.feed.post", "p"),
                ("app.bsky.feed.like", "l"),
                ("app.bsky.feed.repost", "r"),
            ] {
                assert!(
                    indexed(
                        storage.as_ref(),
                        WINDOW_DID,
                        collection,
                        &format!("{prefix}{days}")
                    ),
                    "a {days}-day-old {collection} must be indexed inside the window"
                );
            }
        }
        for days in [31, 400] {
            for (collection, prefix) in [
                ("app.bsky.feed.post", "p"),
                ("app.bsky.feed.like", "l"),
                ("app.bsky.feed.repost", "r"),
            ] {
                assert!(
                    !indexed(
                        storage.as_ref(),
                        WINDOW_DID,
                        collection,
                        &format!("{prefix}{days}")
                    ),
                    "a {days}-day-old {collection} must not be indexed outside the window"
                );
            }
        }
        assert!(indexed(
            storage.as_ref(),
            WINDOW_DID,
            "app.bsky.graph.follow",
            "f400"
        ));
        assert!(indexed(
            storage.as_ref(),
            WINDOW_DID,
            "app.bsky.graph.block",
            "b400"
        ));
        assert!(indexed(
            storage.as_ref(),
            WINDOW_DID,
            "app.bsky.actor.profile",
            "self"
        ));

        let completed = completed_line_after(logs, mark, WINDOW_DID).unwrap_or_else(|| {
            let all = logs.0.lock().unwrap().clone();
            let dump = String::from_utf8_lossy(&all[mark..]).to_string();
            panic!("completed line missing; capture after mark:\n{dump}\n====")
        });
        assert!(
            completed.contains("skipped_old_records=6"),
            "the completed line must carry the skipped count: {completed}"
        );
    }

    #[tokio::test]
    async fn max_age_zero_indexes_the_whole_repository() {
        let storage = Arc::new(MemoryStorage::default());
        let backfiller = backfiller(storage.clone()).with_max_age_days(0);
        let count = backfiller
            .backfill(DID, "https://pds.example", "did:key:zDnaem-window-test-key")
            .await
            .unwrap();
        assert_eq!(count, 15);
        for days in [5, 29, 31, 400] {
            for (collection, prefix) in [
                ("app.bsky.feed.post", "p"),
                ("app.bsky.feed.like", "l"),
                ("app.bsky.feed.repost", "r"),
            ] {
                assert!(
                    indexed(
                        storage.as_ref(),
                        DID,
                        collection,
                        &format!("{prefix}{days}")
                    ),
                    "with no window a {days}-day-old {collection} must be indexed"
                );
            }
        }
    }

    #[tokio::test]
    async fn records_without_a_parsable_created_at_are_kept() {
        let storage = Arc::new(MemoryStorage::default());
        let mut records = windowed_repo();
        records.push(RepoRecord {
            collection: "app.bsky.feed.post".to_owned(),
            rkey: "nodate".to_owned(),
            cid: "cid-nodate".to_owned(),
            record: json!({"$type":"app.bsky.feed.post","text":"no createdAt"}),
        });
        records.push(RepoRecord {
            collection: "app.bsky.feed.post".to_owned(),
            rkey: "baddate".to_owned(),
            cid: "cid-baddate".to_owned(),
            record: json!({"$type":"app.bsky.feed.post","createdAt":"last tuesday"}),
        });
        let backfiller = RepoBackfiller::new(
            storage.clone(),
            ScriptedFetcher,
            ScriptedVerifier(records),
            HostRateLimiter::new(std::time::Duration::ZERO),
        );
        let count = backfiller
            .backfill(DID, "https://pds.example", "did:key:zDnaem-window-test-key")
            .await
            .unwrap();
        assert_eq!(count, 11, "the two undated posts must be kept, not skipped");
        assert!(indexed(
            storage.as_ref(),
            DID,
            "app.bsky.feed.post",
            "nodate"
        ));
        assert!(indexed(
            storage.as_ref(),
            DID,
            "app.bsky.feed.post",
            "baddate"
        ));
    }

    #[tokio::test]
    async fn windowed_backfill_does_not_delete_history_older_than_the_window() {
        // The bug this reproduces: `replace_repo_snapshot_inner` deleted every
        // row of the did with time_us <= now before writing the snapshot, so a
        // re-backfill with a 30-day window wiped 200-day posts that scrolling
        // (or Jetstream) had already stored.
        let storage = Arc::new(SqliteStorage::open(":memory:").unwrap());
        let viewer = "did:plc:windowviewer";
        let subject = format!("at://{viewer}/app.bsky.feed.post/vp1");
        storage
            .put_record(&seeded(
                DID,
                "app.bsky.feed.post",
                "old-scroll",
                200,
                json!({"text": "stored by scrolling before"}),
            ))
            .unwrap();
        storage
            .put_record(&seeded(
                DID,
                "app.bsky.feed.like",
                "old-like",
                200,
                json!({"subject": {"uri": subject}}),
            ))
            .unwrap();
        storage
            .put_record(&seeded(
                DID,
                "app.bsky.feed.post",
                "recent-gone",
                10,
                json!({"text": "deleted at the source"}),
            ))
            .unwrap();
        storage
            .put_record(&seeded(
                DID,
                "app.bsky.feed.like",
                "recent-like-gone",
                10,
                json!({"subject": {"uri": subject}}),
            ))
            .unwrap();

        let backfiller = RepoBackfiller::new(
            storage.clone(),
            ScriptedFetcher,
            ScriptedVerifier(vec![aged_record("app.bsky.feed.post", "p5", 5)]),
            HostRateLimiter::new(std::time::Duration::ZERO),
        );
        let count = backfiller
            .backfill(DID, "https://pds.example", "did:key:zDnaem-window-test-key")
            .await
            .unwrap();
        assert_eq!(count, 1);

        assert!(
            indexed(storage.as_ref(), DID, "app.bsky.feed.post", "old-scroll"),
            "a 200-day post stored by scrolling must survive a re-backfill whose snapshot stops at the window"
        );
        assert!(
            indexed(storage.as_ref(), DID, "app.bsky.feed.like", "old-like"),
            "an old like must survive for the same reason"
        );
        assert!(
            !indexed(storage.as_ref(), DID, "app.bsky.feed.post", "recent-gone"),
            "a post inside the window that is gone from the new snapshot must still be deleted (as today)"
        );
        assert!(
            !indexed(
                storage.as_ref(),
                DID,
                "app.bsky.feed.like",
                "recent-like-gone"
            ),
            "an in-window like gone from the snapshot must still be deleted, with its notification event"
        );
        assert!(indexed(storage.as_ref(), DID, "app.bsky.feed.post", "p5"));
        assert_eq!(
            storage.notification_count(viewer).unwrap(),
            1,
            "the old like's notification event survives; the deleted in-window like's event does not"
        );
    }

    #[tokio::test]
    async fn backfill_with_the_window_disabled_marks_the_history_complete_for_scrolling() {
        async fn check<S: appview_rs::ingest::Storage + 'static>(
            storage: &Arc<S>,
            max_age_days: u64,
        ) {
            let backfiller = RepoBackfiller::new(
                storage.clone(),
                ScriptedFetcher,
                ScriptedVerifier(vec![aged_record("app.bsky.feed.post", "p5", 400)]),
                HostRateLimiter::new(std::time::Duration::ZERO),
            )
            .with_max_age_days(max_age_days);
            backfiller
                .backfill(DID, "https://pds.example", "did:key:zDnaem-window-test-key")
                .await
                .unwrap();
            let state = storage.older_posts_state(DID).unwrap();
            if max_age_days == 0 {
                assert_eq!(
                    state,
                    Some(appview_rs::ingest::OlderPostsState {
                        cursor: None,
                        exhausted: true,
                    }),
                    "a full-repository backfill is the whole history: scrolling must not walk the PDS"
                );
            } else {
                assert_eq!(
                    state, None,
                    "a windowed backfill stops at the window edge, so it must not claim the history is complete"
                );
            }
        }
        check(&Arc::new(MemoryStorage::default()), 0).await;
        check(
            &Arc::new(appview_rs::ingest::SqliteStorage::open(":memory:").unwrap()),
            0,
        )
        .await;
        check(&Arc::new(MemoryStorage::default()), 30).await;
        check(
            &Arc::new(appview_rs::ingest::SqliteStorage::open(":memory:").unwrap()),
            30,
        )
        .await;
    }
}

#[test]
fn failed_repository_retry_delay_doubles_and_caps_at_one_hour() {
    fn check(storage: &impl Storage) {
        for failures in 1..=20 {
            let expected = (5_u64 * 2_u64.pow(failures - 1)).min(60 * 60);
            assert_eq!(
                storage
                    .record_backfill_failure("did:plc:retry-policy")
                    .unwrap(),
                Duration::from_secs(expected),
                "failure {failures}"
            );
        }
    }

    check(&MemoryStorage::default());
    check(&appview_rs::ingest::SqliteStorage::open(":memory:").unwrap());
}
