use appview_rs::ingest::{IndexedRecord, SqliteStorage, Storage};
use std::time::{Duration, Instant};

// A big repository snapshot used to hold the one connection lock for its whole
// write (70 s for an 82k-record repo on appview-host), freezing every page and sign-in.
#[test]
fn large_snapshot_lets_other_storage_calls_in_between_batches() {
    let storage = SqliteStorage::open(":memory:").unwrap();
    let did = "did:plc:big";
    let records: Vec<IndexedRecord> = (0..60_000)
        .map(|i| IndexedRecord {
            did: did.to_owned(),
            collection: "app.bsky.feed.post".to_owned(),
            rkey: format!("r{i}"),
            uri: format!("at://{did}/app.bsky.feed.post/r{i}"),
            cid: Some(format!("cid{i}")),
            time_us: i,
            record: serde_json::json!({"text": "x".repeat(200), "createdAt": "2026-01-01T00:00:00Z"}),
        })
        .collect();

    let (total, worst) = std::thread::scope(|scope| {
        let writer = scope.spawn(|| {
            let started = Instant::now();
            storage.replace_repo_snapshot(did, &records, &[]).unwrap();
            started.elapsed()
        });
        let mut worst = Duration::ZERO;
        while !writer.is_finished() {
            let started = Instant::now();
            storage
                .record_by_uri("app.bsky.feed.post", "at://did:plc:other/app.bsky.feed.post/x")
                .unwrap();
            worst = worst.max(started.elapsed());
        }
        (writer.join().unwrap(), worst)
    });

    assert!(total > Duration::from_millis(300), "snapshot too fast to measure: {total:?}");
    assert!(
        worst < total / 4,
        "a reader waited {worst:?} of a {total:?} snapshot write"
    );
    assert_eq!(
        storage
            .record_by_uri("app.bsky.feed.post", "at://did:plc:big/app.bsky.feed.post/r59999")
            .unwrap()
            .map(|r| r.rkey),
        Some("r59999".to_owned())
    );
}
