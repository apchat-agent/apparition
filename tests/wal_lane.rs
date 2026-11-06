use appview_rs::ingest::{IndexedRecord, JetstreamStorageUpdate, SqliteStorage, Storage};
use rusqlite::Connection;
use std::{
    io::{BufRead, BufReader},
    path::PathBuf,
    process::{Command, Stdio},
    sync::{
        Arc, Barrier,
        atomic::{AtomicBool, Ordering},
    },
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

fn temp_db(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "appview-av68c-{label}-{}-{}.sqlite3",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

#[test]
fn passive_catches_up_and_reuses_wal_with_short_readers() {
    let path = temp_db("passive");
    let writer = Connection::open(&path).unwrap();
    writer.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; PRAGMA journal_size_limit=65536; CREATE TABLE t(k INTEGER PRIMARY KEY, v BLOB);").unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let barrier = Arc::new(Barrier::new(2));
    let reader_path = path.clone();
    let reader_stop = Arc::clone(&stop);
    let reader_barrier = Arc::clone(&barrier);
    let reader = thread::spawn(move || {
        let connection = Connection::open(reader_path).unwrap();
        connection
            .execute_batch("PRAGMA busy_timeout=1000;")
            .unwrap();
        reader_barrier.wait();
        while !reader_stop.load(Ordering::Relaxed) {
            connection.execute_batch("BEGIN").unwrap();
            let _: i64 = connection
                .query_row("SELECT count(*) FROM t", [], |row| row.get(0))
                .unwrap();
            connection.execute_batch("COMMIT").unwrap();
        }
    });
    barrier.wait();
    let mut rounds = 0_u64;
    let mut caught_up = 0_u64;
    let mut wal_peak = 0_u64;
    for index in 0..2_000 {
        writer
            .execute("INSERT INTO t VALUES (?1, zeroblob(2048))", [index])
            .unwrap();
        if index % 25 == 0 {
            let checkpoint: (i64, i64, i64) = writer
                .query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                })
                .unwrap();
            rounds += 1;
            caught_up += u64::from(checkpoint.1 == checkpoint.2);
            wal_peak = wal_peak.max(
                std::fs::metadata(format!("{}-wal", path.display()))
                    .unwrap()
                    .len(),
            );
        }
    }
    stop.store(true, Ordering::Relaxed);
    reader.join().unwrap();
    let wal = PathBuf::from(format!("{}-wal", path.display()));
    let before_final = std::fs::metadata(&wal).map(|m| m.len()).unwrap_or(0);
    let latest: (i64, i64, i64) = writer
        .query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .unwrap();
    let after_passive = std::fs::metadata(&wal).map(|m| m.len()).unwrap_or(0);
    writer
        .execute("INSERT INTO t VALUES (2001, zeroblob(2048))", [])
        .unwrap();
    let after_next_write = std::fs::metadata(&wal).map(|m| m.len()).unwrap_or(0);
    let reused: (i64, i64, i64) = writer
        .query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .unwrap();
    eprintln!(
        "A measurement: passive_rounds={rounds} fully_caught_up={caught_up} final={latest:?} wal_peak={wal_peak} before_final={before_final} after_passive={after_passive} after_next_write={after_next_write} reused={reused:?}"
    );
    drop(writer);
    let _ = std::fs::remove_file(&path);
    // Whether one of the intermediate checkpoints wins the short-reader race is
    // scheduler-dependent. The final PASSIVE runs after the reader has joined,
    // so its caught-up result below is the deterministic condition that matters.
    assert_eq!(latest.1, latest.2);
    assert!(
        after_next_write <= 65_536,
        "WAL did not shrink to journal_size_limit after restart: {after_next_write}"
    );
}

#[test]
fn periodic_passive_checkpoints_bound_continuous_writes_without_truncate() {
    const JOURNAL_LIMIT: u64 = 65_536;
    const WRITES_PER_BATCH: usize = 128;
    const BATCHES: usize = 8;

    let path = temp_db("passive-bound");
    let connection = Connection::open(&path).unwrap();
    connection
        .execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;
             PRAGMA journal_size_limit=65536; CREATE TABLE t(k INTEGER PRIMARY KEY, v BLOB);",
        )
        .unwrap();
    let wal_path = PathBuf::from(format!("{}-wal", path.display()));
    let mut peak_bytes = 0_u64;
    let mut resets_after_checkpoint = 0_u64;
    let mut caught_up_rounds = 0_u64;

    for batch in 0..BATCHES {
        for offset in 0..WRITES_PER_BATCH {
            let index = (batch * WRITES_PER_BATCH + offset) as i64;
            connection
                .execute("INSERT INTO t VALUES (?1, zeroblob(2048))", [index])
                .unwrap();
            peak_bytes = peak_bytes.max(std::fs::metadata(&wal_path).unwrap().len());
        }
        let checkpoint: (i64, i64, i64) = connection
            .query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |row| {
                Ok((row.get(0)?, row.get(1)?, row.get(2)?))
            })
            .unwrap();
        assert_eq!(checkpoint.0, 0, "PASSIVE returned busy: {checkpoint:?}");
        assert_eq!(
            checkpoint.1, checkpoint.2,
            "PASSIVE did not catch up: {checkpoint:?}"
        );
        caught_up_rounds += 1;
        let after_passive = std::fs::metadata(&wal_path).unwrap().len();
        assert!(
            after_passive > JOURNAL_LIMIT,
            "fixture did not exceed the journal limit before reset: {after_passive}"
        );

        // A writer beginning after a completed checkpoint resets the WAL and
        // applies journal_size_limit. This is the steady-state truncation path.
        let reset_key = (BATCHES * WRITES_PER_BATCH + batch) as i64;
        connection
            .execute("INSERT INTO t VALUES (?1, zeroblob(2048))", [reset_key])
            .unwrap();
        let after_next_write = std::fs::metadata(&wal_path).unwrap().len();
        peak_bytes = peak_bytes.max(after_next_write);
        assert!(
            after_next_write <= JOURNAL_LIMIT,
            "WAL did not reset to journal_size_limit after PASSIVE: {after_next_write}"
        );
        resets_after_checkpoint += 1;
    }

    let final_checkpoint: (i64, i64, i64) = connection
        .query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .unwrap();
    assert_eq!(
        final_checkpoint.1, final_checkpoint.2,
        "{final_checkpoint:?}"
    );
    connection
        .execute(
            "INSERT INTO t VALUES (?1, zeroblob(2048))",
            [(BATCHES * WRITES_PER_BATCH + BATCHES) as i64],
        )
        .unwrap();
    let final_wal_bytes = std::fs::metadata(&wal_path).unwrap().len();
    eprintln!(
        "PASSIVE-only continuous-write measurement: batches={BATCHES} writes={} caught_up_rounds={caught_up_rounds} resets_after_checkpoint={resets_after_checkpoint} peak_bytes={peak_bytes} final_wal_bytes={final_wal_bytes} journal_limit={JOURNAL_LIMIT}",
        BATCHES * WRITES_PER_BATCH
    );
    drop(connection);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(wal_path);
    assert!(
        peak_bytes < 4 * 1024 * 1024,
        "periodic PASSIVE did not keep WAL growth bounded: peak={peak_bytes}"
    );
    assert_eq!(resets_after_checkpoint, BATCHES as u64);
    assert!(final_wal_bytes <= JOURNAL_LIMIT, "{final_wal_bytes}");
}

#[test]
fn passive_checkpoint_latency_is_measured_for_smaller_write_batches() {
    const TOTAL_WRITES: usize = 256;
    for writes_per_checkpoint in [16_usize, 64] {
        let path = temp_db(&format!("batch-size-{writes_per_checkpoint}"));
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch(
                "PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;
                 PRAGMA journal_size_limit=65536; CREATE TABLE t(k INTEGER PRIMARY KEY, v BLOB);",
            )
            .unwrap();
        let mut total_checkpoint_ns = 0_u128;
        let mut max_checkpoint_ns = 0_u128;
        let mut total_frames = 0_i64;
        let mut rounds = 0_u64;
        for index in 0..TOTAL_WRITES {
            connection
                .execute("INSERT INTO t VALUES (?1, zeroblob(2048))", [index as i64])
                .unwrap();
            if (index + 1) % writes_per_checkpoint == 0 {
                let started = std::time::Instant::now();
                let checkpoint: (i64, i64, i64) = connection
                    .query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |row| {
                        Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                    })
                    .unwrap();
                let elapsed_ns = started.elapsed().as_nanos();
                assert_eq!(checkpoint.0, 0, "{checkpoint:?}");
                assert_eq!(checkpoint.1, checkpoint.2, "{checkpoint:?}");
                total_checkpoint_ns += elapsed_ns;
                max_checkpoint_ns = max_checkpoint_ns.max(elapsed_ns);
                total_frames += checkpoint.1;
                rounds += 1;
            }
        }
        eprintln!(
            "PASSIVE batch-size measurement: writes_per_checkpoint={writes_per_checkpoint} rounds={rounds} avg_ms={:.3} max_ms={:.3} avg_frames={:.1}",
            total_checkpoint_ns as f64 / rounds as f64 / 1_000_000.0,
            max_checkpoint_ns as f64 / 1_000_000.0,
            total_frames as f64 / rounds as f64,
        );
        drop(connection);
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{}-wal", path.display()));
        let _ = std::fs::remove_file(format!("{}-shm", path.display()));
    }
}

#[test]
fn a_long_lived_reader_allows_growth_then_passive_recovers_and_resets_the_wal() {
    const JOURNAL_LIMIT: u64 = 65_536;
    let path = temp_db("reader-pinned");
    let writer = Connection::open(&path).unwrap();
    writer
        .execute_batch(
            "PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0;
             PRAGMA journal_size_limit=65536; CREATE TABLE t(k INTEGER PRIMARY KEY, v BLOB);",
        )
        .unwrap();
    let reader = Connection::open(&path).unwrap();
    reader.execute_batch("BEGIN;").unwrap();
    let _: i64 = reader
        .query_row("SELECT count(*) FROM t", [], |row| row.get(0))
        .unwrap();

    let wal_path = PathBuf::from(format!("{}-wal", path.display()));
    let mut peak_bytes = 0_u64;
    let mut blocked_rounds = 0_u64;
    for index in 0..1_000_i64 {
        writer
            .execute("INSERT INTO t VALUES (?1, zeroblob(2048))", [index])
            .unwrap();
        peak_bytes = peak_bytes.max(std::fs::metadata(&wal_path).unwrap().len());
        if index % 25 == 24 {
            let checkpoint: (i64, i64, i64) = writer
                .query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |row| {
                    Ok((row.get(0)?, row.get(1)?, row.get(2)?))
                })
                .unwrap();
            assert_eq!(checkpoint.0, 0, "PASSIVE returned busy: {checkpoint:?}");
            assert!(
                checkpoint.1 > checkpoint.2,
                "held snapshot did not pin the checkpoint: {checkpoint:?}"
            );
            blocked_rounds += 1;
        }
    }
    let while_reader_open = std::fs::metadata(&wal_path).unwrap().len();
    assert!(while_reader_open > 4 * JOURNAL_LIMIT);
    reader.execute_batch("COMMIT;").unwrap();
    drop(reader);

    let recovered: (i64, i64, i64) = writer
        .query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .unwrap();
    assert_eq!(recovered.0, 0, "PASSIVE returned busy: {recovered:?}");
    assert_eq!(
        recovered.1, recovered.2,
        "reader release did not recover: {recovered:?}"
    );
    let after_recovery_passive = std::fs::metadata(&wal_path).unwrap().len();
    writer
        .execute("INSERT INTO t VALUES (1001, zeroblob(2048))", [])
        .unwrap();
    let after_next_write = std::fs::metadata(&wal_path).unwrap().len();
    eprintln!(
        "long-reader WAL measurement: blocked_rounds={blocked_rounds} peak_bytes={peak_bytes} while_reader_open={while_reader_open} recovered={recovered:?} after_recovery_passive={after_recovery_passive} after_next_write={after_next_write} journal_limit={JOURNAL_LIMIT}"
    );
    drop(writer);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&wal_path);
    assert!(blocked_rounds >= 10);
    assert!(peak_bytes >= while_reader_open);
    assert!(after_recovery_passive > JOURNAL_LIMIT);
    assert!(
        after_next_write <= JOURNAL_LIMIT,
        "WAL did not return to the configured limit after reader release: {after_next_write}"
    );
}

#[test]
fn storage_checkpointer_reuses_a_fully_checkpointed_wal_under_the_cap() {
    let path = temp_db("automatic-reuse");
    let storage = SqliteStorage::open(&path).unwrap();
    let writer = Connection::open(&path).unwrap();
    writer.execute_batch("PRAGMA wal_autocheckpoint=0; CREATE TABLE test_payload(k INTEGER PRIMARY KEY, v BLOB);").unwrap();
    for index in 0..128 {
        writer
            .execute(
                "INSERT INTO test_payload VALUES (?1, zeroblob(32768))",
                [index],
            )
            .unwrap();
    }
    let wal_path = PathBuf::from(format!("{}-wal", path.display()));
    assert!(std::fs::metadata(&wal_path).unwrap().len() > 4 * 65_536);
    drop(writer);

    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while storage.write_backlog() != u64::MAX {
        assert!(
            std::time::Instant::now() < deadline,
            "checkpointer did not start its first round"
        );
        thread::sleep(Duration::from_millis(25));
    }
    while storage.write_backlog() == u64::MAX {
        assert!(
            std::time::Instant::now() < deadline,
            "checkpointer did not report its first round"
        );
        thread::sleep(Duration::from_millis(25));
    }
    assert_eq!(storage.write_backlog(), 0, "PASSIVE should catch up");
    let wal_bytes = std::fs::metadata(&wal_path)
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    eprintln!("GREEN automatic reuse measurement: wal_bytes={wal_bytes}");
    drop(storage);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(wal_path);
    assert!(
        wal_bytes > 4 * 65_536,
        "fully checkpointed WAL below the cap should keep reusable pages: {wal_bytes}"
    );
}

#[test]
fn clean_shutdown_reuses_a_small_wal() {
    let path = temp_db("shutdown-reuse");
    let storage = SqliteStorage::open(&path).unwrap();
    let writer = Connection::open(&path).unwrap();
    writer.execute_batch("PRAGMA wal_autocheckpoint=0; CREATE TABLE test_payload(k INTEGER PRIMARY KEY, v BLOB);").unwrap();
    for index in 0..128 {
        writer
            .execute(
                "INSERT INTO test_payload VALUES (?1, zeroblob(32768))",
                [index],
            )
            .unwrap();
    }
    let wal_path = PathBuf::from(format!("{}-wal", path.display()));
    assert!(std::fs::metadata(&wal_path).unwrap().len() > 4 * 65_536);
    drop(storage);
    let wal_bytes = std::fs::metadata(&wal_path)
        .map(|metadata| metadata.len())
        .unwrap_or(0);
    eprintln!("clean shutdown measurement: wal_bytes={wal_bytes}");
    drop(writer);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(wal_path);
    assert!(
        wal_bytes > 4 * 65_536,
        "clean shutdown should not truncate a reusable WAL below the cap: {wal_bytes}"
    );
}

#[test]
#[ignore = "subprocess helper for crash checkpoint test"]
fn crash_checkpoint_child() {
    let Ok(path) = std::env::var("AV68C_CRASH_DB") else {
        return;
    };
    let connection = Connection::open(&path).unwrap();
    connection.execute_batch("PRAGMA journal_mode=WAL; PRAGMA wal_autocheckpoint=0; CREATE TABLE t(k INTEGER PRIMARY KEY, v BLOB);").unwrap();
    for index in 0..100 {
        connection
            .execute("INSERT INTO t VALUES (?1, zeroblob(2000))", [index])
            .unwrap();
    }
    let checkpoint: (i64, i64, i64) = connection
        .query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .unwrap();
    println!("READY {checkpoint:?}");
    std::io::Write::flush(&mut std::io::stdout()).unwrap();
    loop {
        thread::sleep(Duration::from_secs(1));
    }
}

#[test]
fn first_passive_after_crash_reports_checkpointed_frames() {
    let path = temp_db("crash");
    let mut child = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("crash_checkpoint_child")
        .arg("--nocapture")
        .arg("--ignored")
        .env("AV68C_CRASH_DB", &path)
        .stdout(Stdio::piped())
        .spawn()
        .unwrap();
    let mut output = BufReader::new(child.stdout.take().unwrap());
    let mut line = String::new();
    loop {
        line.clear();
        assert_ne!(
            output.read_line(&mut line).unwrap(),
            0,
            "child exited before checkpoint: {line}"
        );
        if line.contains("READY") {
            break;
        }
    }
    child.kill().unwrap();
    child.wait().unwrap();
    let wal = PathBuf::from(format!("{}-wal", path.display()));
    let before = std::fs::metadata(&wal).map(|m| m.len()).unwrap_or(0);
    let database = std::fs::metadata(&path).unwrap();
    let mtime_before = database.modified().unwrap();
    let database_size_before = database.len();
    let reopened = Connection::open(&path).unwrap();
    reopened
        .execute_batch("PRAGMA wal_autocheckpoint=0;")
        .unwrap();
    let first: (i64, i64, i64) = reopened
        .query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |row| {
            Ok((row.get(0)?, row.get(1)?, row.get(2)?))
        })
        .unwrap();
    let after = std::fs::metadata(&wal).map(|m| m.len()).unwrap_or(0);
    let database_after = std::fs::metadata(&path).unwrap();
    let mtime_after = database_after.modified().unwrap();
    eprintln!(
        "B measurement: child={line:?} first={first:?} wal_before={before} wal_after={after} db_bytes_before={} db_bytes_after={} db_mtime_advanced={}",
        database_size_before,
        database_after.len(),
        mtime_after > mtime_before
    );
    drop(reopened);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(wal);
    assert_eq!(first, (0, 201, 201));
    assert_eq!(before, 828_152);
    assert_eq!(after, before);
    assert!(
        mtime_after > mtime_before,
        "first PASSIVE did not update the main database file"
    );
}

#[test]
fn production_index_bytes_per_event() {
    let path = temp_db("events");
    let storage = SqliteStorage::open(&path).unwrap();
    let probe = Connection::open(&path).unwrap();
    probe
        .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
        .unwrap();
    let events = 1_000_u64;
    for first in (0..events).step_by(100) {
        let updates = (first..first + 100).map(|index| JetstreamStorageUpdate {
            time_us: index,
            upsert: Some(IndexedRecord {
                did: format!("did:plc:actor-{index}"),
                collection: "app.bsky.feed.post".to_owned(),
                rkey: format!("post-{index}"),
                uri: format!("at://did:plc:actor-{index}/app.bsky.feed.post/post-{index}"),
                cid: Some(format!("bafybeievent{index}")),
                time_us: index,
                record: serde_json::json!({"text":"x".repeat(200), "createdAt":"2026-01-01T00:00:00Z"}),
            }),
            ..JetstreamStorageUpdate::default()
        }).collect::<Vec<_>>();
        storage.apply_jetstream_batch(&updates).unwrap();
    }
    let wal_path = PathBuf::from(format!("{}-wal", path.display()));
    let wal_bytes = std::fs::metadata(&wal_path).unwrap().len();
    let page_size: i64 = probe
        .query_row("PRAGMA page_size", [], |row| row.get(0))
        .unwrap();
    let frames = (wal_bytes.saturating_sub(32)) / (page_size as u64 + 24);
    eprintln!(
        "writer event measurement: events={events} page_size={page_size} wal_bytes={wal_bytes} wal_frames={frames} bytes_per_event={:.1} frames_per_event={:.2}",
        wal_bytes as f64 / events as f64,
        frames as f64 / events as f64
    );
    drop(probe);
    drop(storage);
    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(wal_path);
    let _ = std::fs::remove_file(format!("{}-shm", path.display()));
}
