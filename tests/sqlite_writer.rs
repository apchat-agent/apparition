use std::{
    path::PathBuf,
    sync::mpsc,
    thread,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

use appview_rs::ingest::{SqliteStorage, Storage};
use rusqlite::Connection;

fn database_path(label: &str) -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join(format!(
            "sqlite-writer-{label}-{}-{}.sqlite3",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
}

#[test]
fn page_read_and_refresh_enqueue_do_not_wait_for_a_long_ingest_write() {
    let path = database_path("page-latency");
    let storage = SqliteStorage::open(&path).unwrap();
    let lock_path = path.clone();
    let (ready, started) = mpsc::channel();
    let ingest = thread::spawn(move || {
        let connection = Connection::open(lock_path).unwrap();
        connection.execute_batch("BEGIN IMMEDIATE").unwrap();
        ready.send(()).unwrap();
        thread::sleep(Duration::from_millis(350));
        connection.execute_batch("COMMIT").unwrap();
    });
    started.recv().unwrap();

    let request_started = Instant::now();
    assert!(
        storage
            .get_record("did:plc:page", "app.bsky.feed.post", "missing")
            .unwrap()
            .is_none()
    );
    storage
        .queue_handle_refresh_ordered(&["did:plc:page-author".to_owned()])
        .unwrap();
    let elapsed = request_started.elapsed();

    ingest.join().unwrap();
    drop(storage);
    let _ = std::fs::remove_file(&path);
    assert!(
        elapsed < Duration::from_millis(100),
        "page read or refresh enqueue waited {elapsed:?} for the ingest write"
    );
}
