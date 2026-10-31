use appview_rs::{
    backfill::{
        BackfillError, HostRateLimiter, RepoBackfiller, RepoFetcher, RepoRecord, RepoVerifier,
        VerifiedRepo,
    },
    ingest::{IndexedRecord, SqliteStorage, Storage},
};
use rusqlite::Connection;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

struct TempDirGuard(PathBuf);

impl TempDirGuard {
    fn new(path: impl AsRef<Path>) -> Self {
        Self(path.as_ref().to_owned())
    }
}

impl Drop for TempDirGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

#[test]
fn snapshot_test_temp_directory_is_removed_during_unwind() {
    let dir = std::env::temp_dir().join(format!(
        "appview-snapshot-guard-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();

    let result = std::panic::catch_unwind(|| {
        let _guard = TempDirGuard::new(&dir);
        panic!("simulate a failed assertion after creating a large fixture");
    });

    assert!(result.is_err());
    assert!(
        !dir.exists(),
        "snapshot test temp directory leaked on unwind"
    );
}

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
    let _guard = TempDirGuard::new(&dir);
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
}

#[test]
fn page_reads_do_not_wait_for_a_checkpoint_waiting_on_an_open_reader() {
    let dir = std::env::temp_dir().join(format!(
        "appview-checkpoint-read-isolation-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let _guard = TempDirGuard::new(&dir);
    let path = dir.join("appview.db");
    let storage = SqliteStorage::open(&path).unwrap();
    let writer = Connection::open(&path).unwrap();
    writer
        .execute_batch(
            "PRAGMA wal_autocheckpoint=0;
             CREATE TABLE checkpoint_payload(k INTEGER PRIMARY KEY, v BLOB);",
        )
        .unwrap();
    for index in 0..128 {
        writer
            .execute(
                "INSERT INTO checkpoint_payload VALUES (?1, zeroblob(32768))",
                [index],
            )
            .unwrap();
    }
    drop(writer);

    let reader = Connection::open(&path).unwrap();
    reader.execute_batch("BEGIN").unwrap();
    let _: i64 = reader
        .query_row("SELECT count(*) FROM checkpoint_payload", [], |row| {
            row.get(0)
        })
        .unwrap();

    // Keep page reads active across the checkpointer's two-second interval.
    // Its TRUNCATE attempt is held by the read transaction and times out after
    // 50 ms; WAL-mode page reads use their own connections and should continue.
    let deadline = Instant::now() + Duration::from_millis(2_500);
    let mut worst = Duration::ZERO;
    let mut reads = 0_u64;
    while Instant::now() < deadline {
        let started = Instant::now();
        assert!(
            storage
                .get_record("did:plc:read", "app.bsky.feed.post", "missing")
                .unwrap()
                .is_none()
        );
        worst = worst.max(started.elapsed());
        reads += 1;
    }
    reader.execute_batch("ROLLBACK").unwrap();
    drop(reader);
    drop(storage);
    assert!(
        reads > 10,
        "only {reads} page reads ran during the checkpoint interval"
    );
    assert!(
        worst < Duration::from_millis(100),
        "page reads waited {worst:?} behind a checkpoint waiting for a reader"
    );
}

#[test]
fn checkpointer_skips_a_round_while_page_requests_wait_for_connections() {
    let dir = std::env::temp_dir().join(format!(
        "appview-checkpoint-pressure-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    let _guard = TempDirGuard::new(&dir);
    let path = dir.join("appview.db");
    let storage = std::sync::Arc::new(SqliteStorage::open(&path).unwrap());
    let writer = Connection::open(&path).unwrap();
    writer
        .execute_batch(
            "PRAGMA wal_autocheckpoint=0;
             CREATE TABLE checkpoint_payload(k INTEGER PRIMARY KEY, v BLOB);",
        )
        .unwrap();
    for index in 0..128 {
        writer
            .execute(
                "INSERT INTO checkpoint_payload VALUES (?1, zeroblob(32768))",
                [index],
            )
            .unwrap();
    }
    drop(writer);
    let wal_path = PathBuf::from(format!("{}-wal", path.display()));
    let initial_bytes = std::fs::metadata(&wal_path).unwrap().len();
    assert!(initial_bytes > 65_536);

    let held_connections = (0..4)
        .map(|_| storage.read_conn().unwrap())
        .collect::<Vec<_>>();
    let waiting_storage = std::sync::Arc::clone(&storage);
    let (waiting_tx, waiting_rx) = std::sync::mpsc::channel();
    let waiter = std::thread::spawn(move || {
        waiting_tx.send(()).unwrap();
        drop(waiting_storage.read_conn().unwrap());
    });
    waiting_rx.recv().unwrap();
    std::thread::sleep(Duration::from_millis(2_300));
    let pressured_bytes = std::fs::metadata(&wal_path).unwrap().len();
    drop(held_connections);
    waiter.join().unwrap();
    assert!(
        pressured_bytes > 65_536,
        "checkpointer reset a {initial_bytes} byte WAL during page pressure"
    );

    let deadline = Instant::now() + Duration::from_secs(4);
    while std::fs::metadata(&wal_path)
        .map(|metadata| metadata.len())
        .unwrap_or(0)
        > 65_536
    {
        assert!(
            Instant::now() < deadline,
            "checkpointer did not reset WAL after pressure cleared"
        );
        std::thread::sleep(Duration::from_millis(25));
    }
    drop(storage);
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
    let _guard = TempDirGuard::new(&dir);
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
    let _guard = TempDirGuard::new(&dir);
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
    let _guard = TempDirGuard::new(&dir);
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
}

// A big repository snapshot used to hold the one connection lock for its whole
// write, delaying page reads and session updates.
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

// SQLite's automatic checkpoint runs inside the committing write, under the
// connection lock, and a large WAL can delay concurrent requests.
#[test]
fn record_writes_never_run_the_wal_checkpoint_under_the_lock() {
    let dir = std::env::temp_dir().join(format!("appview-ckpt-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let _guard = TempDirGuard::new(&dir);
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
    wait_for_write_backlog(backlog, || false, 5, Duration::from_millis(1), rx.clone()).await;
    assert_eq!(calls.load(Ordering::Relaxed), 3);

    let (tx, rx) = tokio::sync::watch::channel(false);
    tx.send(true).unwrap();
    tokio::time::timeout(
        Duration::from_secs(1),
        wait_for_write_backlog(|| 10, || false, 5, Duration::from_millis(1), rx),
    )
    .await
    .expect("shutdown must end the wait");
}

#[tokio::test]
async fn backfill_waits_for_interactive_sqlite_pressure_to_clear() {
    use appview_rs::backfill::wait_for_write_backlog;
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };

    let pressure = Arc::new(AtomicBool::new(true));
    let task_pressure = Arc::clone(&pressure);
    let (_tx, shutdown) = tokio::sync::watch::channel(false);
    let waiter = tokio::spawn(async move {
        wait_for_write_backlog(
            || 0,
            || task_pressure.load(Ordering::Relaxed),
            5,
            Duration::from_millis(1),
            shutdown,
        )
        .await
    });
    tokio::time::sleep(Duration::from_millis(10)).await;
    pressure.store(false, Ordering::Relaxed);
    let waited = waiter.await.unwrap();
    assert!(
        waited >= Duration::from_millis(10),
        "pressure wait was only {waited:?}"
    );
}

#[test]
fn write_backlog_reads_as_behind_until_a_checkpoint_reports() {
    let dir = std::env::temp_dir().join(format!("appview-backlog-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let _guard = TempDirGuard::new(&dir);
    let storage = SqliteStorage::open(dir.join("appview.db")).unwrap();
    assert_eq!(storage.write_backlog(), u64::MAX);
    let started = Instant::now();
    let deadline = Instant::now() + Duration::from_secs(10);
    while storage.write_backlog() == u64::MAX {
        assert!(Instant::now() < deadline, "checkpointer never reported");
        std::thread::sleep(Duration::from_millis(100));
    }
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "small-WAL startup checkpoint took {:?}",
        started.elapsed()
    );
    assert!(storage.write_backlog() < 100);
    drop(storage);
}
