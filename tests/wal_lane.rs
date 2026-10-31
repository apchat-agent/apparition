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
fn passive_probe() {
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
    eprintln!(
        "A measurement: passive_rounds={rounds} fully_caught_up={caught_up} final={latest:?} wal_peak={wal_peak} before_final={before_final} after_passive={after_passive} after_next_write={after_next_write}"
    );
    drop(writer);
    let _ = std::fs::remove_file(&path);
    assert!(
        caught_up > 0,
        "PASSIVE never caught up during short reads and writes"
    );
    assert_eq!(latest.1, latest.2);
    assert!(
        after_next_write <= 65_536,
        "WAL did not shrink to journal_size_limit after restart: {after_next_write}"
    );
}

#[test]
fn crash_child() {
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
fn crash_probe() {
    let path = temp_db("crash");
    let mut child = Command::new(std::env::current_exe().unwrap())
        .arg("--exact")
        .arg("crash_child")
        .arg("--nocapture")
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
fn production_index_bytes_per_event_probe() {
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
