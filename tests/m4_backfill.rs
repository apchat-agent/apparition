use appview_rs::{
    backfill::{AtriumRepoVerifier, HostRateLimiter, HttpRepoSource, RepoBackfiller},
    identity::{DidDocument, DidService, VerificationMethod},
    ingest::{MemoryStorage, Storage},
};
use atrium_api::types::string::Did;
use atrium_crypto::keypair::{Did as _, P256Keypair};
use atrium_repo::{
    Repository,
    blockstore::{AsyncBlockStoreRead, AsyncBlockStoreWrite, MemoryBlockStore},
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
    collections::HashMap,
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
    let backfiller = RepoBackfiller::new(
        storage.clone(),
        HttpRepoSource::default(),
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
    assert!(
        storage
            .get_record(did, "app.bsky.feed.post", "stale")
            .unwrap()
            .is_none()
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
