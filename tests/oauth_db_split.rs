use appview_rs::{config::database_paths, oauth::SqliteOAuthStore};
use std::time::{Duration, Instant};

#[test]
fn oauth_store_opens_while_the_record_database_is_write_locked() {
    let dir = std::env::temp_dir().join(format!("appview-oauth-split-{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let (records, oauth) = database_paths(dir.to_str().unwrap());
    assert_ne!(records, oauth);

    // Stand-in for a backfill transaction holding the record database's write lock.
    let backfill = rusqlite::Connection::open(&records).unwrap();
    backfill
        .execute_batch("PRAGMA journal_mode = WAL; CREATE TABLE t (x); BEGIN IMMEDIATE; INSERT INTO t VALUES (1);")
        .unwrap();

    let started = Instant::now();
    let store = SqliteOAuthStore::open(&oauth);
    assert!(store.is_ok(), "{:?}", store.err());
    assert!(
        started.elapsed() < Duration::from_secs(2),
        "sign-in store waited on the record database"
    );

    backfill.execute_batch("ROLLBACK;").unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
