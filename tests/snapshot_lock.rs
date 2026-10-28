use appview_rs::{
    backfill::{
        BackfillError, HostRateLimiter, RepoBackfiller, RepoFetcher, RepoRecord, RepoVerifier,
        VerifiedRepo,
    },
    ingest::{IndexedRecord, SqliteStorage, Storage},
};
use rusqlite::Connection;
use std::time::{Duration, Instant};

#[test]
fn page_reads_do_not_wait_for_a_writer_holding_the_database() {
    let dir = std::env::temp_dir().join(format!(
        "appview-reader-isolation-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("appview.db");
    let storage = SqliteStorage::open(&path).unwrap();
    let actor = "did:plc:reader";
    let friend = "did:plc:friend";
    storage
        .put_record(&IndexedRecord {
            did: actor.to_owned(),
            collection: "app.bsky.graph.follow".to_owned(),
            rkey: "follow".to_owned(),
            uri: format!("at://{actor}/app.bsky.graph.follow/follow"),
            cid: None,
            time_us: 1,
            record: serde_json::json!({"subject":friend}),
        })
        .unwrap();
    storage
        .put_record(&IndexedRecord {
            did: friend.to_owned(),
            collection: "app.bsky.feed.post".to_owned(),
            rkey: "post".to_owned(),
            uri: format!("at://{friend}/app.bsky.feed.post/post"),
            cid: None,
            time_us: 1,
            record: serde_json::json!({"text":"hello"}),
        })
        .unwrap();
    storage.set_verified_handle(actor, "reader.test").unwrap();
    storage.rotate_web_session(actor, "session").unwrap();

    let blocker = Connection::open(&path).unwrap();
    blocker.execute_batch("BEGIN IMMEDIATE").unwrap();
    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let (elapsed, write_result) = std::thread::scope(|scope| {
        let writer = scope.spawn(|| {
            started_tx.send(()).unwrap();
            storage.put_record(&IndexedRecord {
                did: actor.to_owned(),
                collection: "app.bsky.feed.post".to_owned(),
                rkey: "held-write".to_owned(),
                uri: format!("at://{actor}/app.bsky.feed.post/held-write"),
                cid: None,
                time_us: 2,
                record: serde_json::json!({"text":"writer"}),
            })
        });
        started_rx.recv().unwrap();
        std::thread::sleep(Duration::from_millis(50));
        let release = scope.spawn(move || {
            std::thread::sleep(Duration::from_millis(700));
            blocker.execute_batch("COMMIT").unwrap();
        });

        let started = Instant::now();
        let record = storage
            .record_by_uri(
                "app.bsky.feed.post",
                &format!("at://{friend}/app.bsky.feed.post/post"),
            )
            .unwrap();
        assert_eq!(record.unwrap().rkey, "post");
        assert_eq!(storage.timeline_posts(actor, None, 10).unwrap().len(), 1);
        assert_eq!(
            storage.web_session_generation(actor, "session").unwrap(),
            Some(1)
        );
        let elapsed = started.elapsed();
        let write_result = writer.join().unwrap();
        release.join().unwrap();
        (elapsed, write_result)
    });

    write_result.unwrap();
    assert!(
        elapsed < Duration::from_millis(200),
        "page reads waited {elapsed:?} for a database writer"
    );
    drop(storage);
    let _ = std::fs::remove_dir_all(&dir);
}

#[tokio::test]
async fn large_repo_backfill_is_spread_across_batches_by_a_record_rate_budget() {
    struct EmptyFetcher;
    impl RepoFetcher for EmptyFetcher {
        async fn fetch_repo(&self, _: &str, _: &str) -> Result<Vec<u8>, BackfillError> {
            Ok(Vec::new())
        }
    }

    struct LargeVerifier;
    impl RepoVerifier for LargeVerifier {
        async fn verify(&self, _: &[u8], _: &str, _: &str) -> Result<VerifiedRepo, BackfillError> {
            Ok(VerifiedRepo {
                records: (0..1_000)
                    .map(|index| RepoRecord {
                        collection: "app.bsky.feed.post".to_owned(),
                        rkey: format!("p{index}"),
                        cid: format!("cid{index}"),
                        record: serde_json::json!({"text":"backfill"}),
                    })
                    .collect(),
            })
        }
    }

    let dir = std::env::temp_dir().join(format!(
        "appview-rate-budget-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let storage = std::sync::Arc::new(SqliteStorage::open(dir.join("appview.db")).unwrap());
    let backfiller = RepoBackfiller::new(
        storage.clone(),
        EmptyFetcher,
        LargeVerifier,
        HostRateLimiter::new(Duration::ZERO),
    );

    let started = Instant::now();
    let indexed = backfiller
        .backfill("did:plc:large", "https://pds.example", "did:key:fake")
        .await
        .unwrap();

    assert_eq!(indexed, 1_000);
    assert!(
        started.elapsed() >= Duration::from_millis(1_500),
        "1,000 records were written in {:?}; the 300 records/s budget was not applied between batches",
        started.elapsed()
    );
    drop(storage);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn session_writes_can_run_between_batches_while_a_snapshot_removes_old_records() {
    let dir = std::env::temp_dir().join(format!(
        "appview-snapshot-cleanup-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let storage = SqliteStorage::open(dir.join("appview.db")).unwrap();
    let did = "did:plc:cleanup";
    let existing: Vec<IndexedRecord> = (0..60_000)
        .map(|index| IndexedRecord {
            did: did.to_owned(),
            collection: "app.bsky.feed.post".to_owned(),
            rkey: format!("p{index}"),
            uri: format!("at://{did}/app.bsky.feed.post/p{index}"),
            cid: None,
            time_us: index,
            record: serde_json::json!({"text":"x".repeat(200)}),
        })
        .collect();
    storage.replace_repo_snapshot(did, &existing, &[]).unwrap();

    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let writer = std::thread::scope(|scope| {
        let writer = scope.spawn(|| {
            started_tx.send(()).unwrap();
            storage.replace_repo_snapshot(did, &[], &[]).unwrap();
        });
        started_rx.recv().unwrap();
        std::thread::sleep(Duration::from_millis(20));
        let started = Instant::now();
        storage
            .rotate_web_session("did:plc:login", "session")
            .unwrap();
        let elapsed = started.elapsed();
        writer.join().unwrap();
        elapsed
    });

    assert!(
        writer < Duration::from_millis(200),
        "session write waited {writer:?} behind snapshot cleanup"
    );
    assert!(storage.records().unwrap().is_empty());
    drop(storage);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn session_writes_can_run_between_batches_while_a_snapshot_rebuilds_many_follows() {
    let dir = std::env::temp_dir().join(format!(
        "appview-follow-cleanup-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let storage = SqliteStorage::open(dir.join("appview.db")).unwrap();
    let did = "did:plc:many-follows";
    storage.register_user(did).unwrap();
    let following: Vec<String> = (0..100_000)
        .map(|index| format!("did:plc:subject-{index}"))
        .collect();

    let (started_tx, started_rx) = std::sync::mpsc::channel();
    let elapsed = std::thread::scope(|scope| {
        let writer = scope.spawn(|| {
            started_tx.send(()).unwrap();
            storage.replace_repo_snapshot(did, &[], &following).unwrap();
        });
        started_rx.recv().unwrap();
        std::thread::sleep(Duration::from_millis(20));
        let started = Instant::now();
        storage
            .rotate_web_session("did:plc:login", "session")
            .unwrap();
        storage
            .set_follow(did, "did:plc:subject-99999", false)
            .unwrap();
        let elapsed = started.elapsed();
        writer.join().unwrap();
        elapsed
    });

    assert!(
        elapsed < Duration::from_millis(200),
        "session write waited {elapsed:?} behind follow snapshot work"
    );
    assert!(
        !storage
            .is_followed_by_registered("did:plc:subject-99999")
            .unwrap()
    );
    assert!(
        storage
            .is_followed_by_registered("did:plc:subject-99998")
            .unwrap()
    );
    drop(storage);
    let _ = std::fs::remove_dir_all(&dir);
}

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
                .record_by_uri(
                    "app.bsky.feed.post",
                    "at://did:plc:other/app.bsky.feed.post/x",
                )
                .unwrap();
            worst = worst.max(started.elapsed());
        }
        (writer.join().unwrap(), worst)
    });

    assert!(
        total > Duration::from_millis(300),
        "snapshot too fast to measure: {total:?}"
    );
    assert!(
        worst < total / 4,
        "a reader waited {worst:?} of a {total:?} snapshot write"
    );
    assert_eq!(
        storage
            .record_by_uri(
                "app.bsky.feed.post",
                "at://did:plc:big/app.bsky.feed.post/r59999"
            )
            .unwrap()
            .map(|r| r.rkey),
        Some("r59999".to_owned())
    );
}

// SQLite's automatic checkpoint runs inside the committing write, i.e. under the
// connection lock; on appview-host's disk a 1 GB WAL checkpoint held it for minutes.
#[test]
fn record_writes_never_run_the_wal_checkpoint_under_the_lock() {
    let dir = std::env::temp_dir().join(format!("appview-ckpt-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let path = dir.join("appview.db");
    let storage = SqliteStorage::open(&path).unwrap();
    assert_eq!(storage.wal_autocheckpoint().unwrap(), 0);

    // ...and the side checkpointer still drains the WAL so it doesn't grow forever.
    let record = |i: u64| IndexedRecord {
        did: "did:plc:a".to_owned(),
        collection: "app.bsky.feed.post".to_owned(),
        rkey: format!("r{i}"),
        uri: format!("at://did:plc:a/app.bsky.feed.post/r{i}"),
        cid: None,
        time_us: i,
        record: serde_json::json!({"text": "hello"}),
    };
    // Only a checkpoint copies pages from the WAL into the main file.
    let before = std::fs::metadata(&path).unwrap().len();
    for i in 0..2000 {
        storage.put_record(&record(i)).unwrap();
    }
    let deadline = Instant::now() + Duration::from_secs(15);
    while std::fs::metadata(&path).unwrap().len() <= before {
        assert!(
            Instant::now() < deadline,
            "main database file never grew: WAL not checkpointed"
        );
        std::thread::sleep(Duration::from_millis(200));
    }
    drop(storage);
    let _ = std::fs::remove_dir_all(&dir);
}

// The backfill must not outrun the disk: it waits while the WAL has more than
// `limit` frames not yet checkpointed.
#[tokio::test]
async fn backfill_waits_for_the_checkpoint_backlog_to_drain() {
    use appview_rs::backfill::wait_for_write_backlog;
    use std::sync::atomic::{AtomicU64, Ordering};
    let (_tx, rx) = tokio::sync::watch::channel(false);
    let calls = AtomicU64::new(0);
    let backlog = || match calls.fetch_add(1, Ordering::Relaxed) {
        0 | 1 => 10,
        _ => 0,
    };
    wait_for_write_backlog(backlog, 5, Duration::from_millis(1), rx.clone()).await;
    assert_eq!(calls.load(Ordering::Relaxed), 3);

    let (tx, rx) = tokio::sync::watch::channel(false);
    tx.send(true).unwrap();
    tokio::time::timeout(
        Duration::from_secs(1),
        wait_for_write_backlog(|| 10, 5, Duration::from_millis(1), rx),
    )
    .await
    .expect("shutdown must end the wait");
}

#[test]
fn write_backlog_reads_as_behind_until_a_checkpoint_reports() {
    let dir = std::env::temp_dir().join(format!("appview-backlog-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let storage = SqliteStorage::open(dir.join("appview.db")).unwrap();
    assert_eq!(storage.write_backlog(), u64::MAX);
    let deadline = Instant::now() + Duration::from_secs(10);
    while storage.write_backlog() == u64::MAX {
        assert!(Instant::now() < deadline, "checkpointer never reported");
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(storage.write_backlog() < 100);
    drop(storage);
    let _ = std::fs::remove_dir_all(&dir);
}
