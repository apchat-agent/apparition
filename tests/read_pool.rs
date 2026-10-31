use appview_rs::ingest::SqliteStorage;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

fn database_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(format!(
        "target/read-pool-av65q-{}-{name}.sqlite3",
        std::process::id()
    ))
}

fn remove_database(path: &Path) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}-wal", path.display()));
    let _ = std::fs::remove_file(format!("{}-shm", path.display()));
}

/// One held connection must not stall readers that arrive while three
/// connections sit idle. The round-robin cursor visits the held connection
/// within eight calls; today that visit blocks for as long as the holder is
/// busy, which is the head-of-line blocking that tied /thread pages to the
/// 90-second /app query in production.
#[test]
fn a_read_does_not_queue_behind_a_busy_connection_while_others_are_free() {
    let path = database_path("head-of-line");
    remove_database(&path);
    let storage = Arc::new(SqliteStorage::open(&path).unwrap());

    let (held_tx, held_rx) = std::sync::mpsc::channel();
    let holding_storage = Arc::clone(&storage);
    let holder = std::thread::spawn(move || {
        let guard = holding_storage.read_conn().expect("read connection");
        held_tx.send(()).unwrap();
        std::thread::sleep(Duration::from_millis(600));
        drop(guard);
    });
    held_rx.recv().unwrap();

    let mut slowest = Duration::ZERO;
    for _ in 0..8 {
        let started = Instant::now();
        let connection = storage.read_conn().expect("free read connection");
        let elapsed = started.elapsed();
        drop(connection);
        slowest = slowest.max(elapsed);
    }
    assert!(
        slowest < Duration::from_millis(250),
        "a read waited {slowest:?} for a connection whose pool had three idle ones: \
         read_conn must try the other connections before blocking"
    );

    holder.join().unwrap();
    drop(storage);
    remove_database(&path);
}
