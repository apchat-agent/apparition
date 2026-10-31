use appview_rs::ingest::{SqliteStorage, Storage};
use rusqlite::{Connection, params};
use std::path::{Path, PathBuf};

const BASE: i64 = 1_700_000_000_000_000;
const TARGET: &str = "did:plc:target";

fn follow_uri(did: &str, rkey: &str) -> String {
    format!("at://{did}/app.bsky.graph.follow/{rkey}")
}

fn database_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(format!(
        "target/followers-query-av65q-{}-{name}.sqlite3",
        std::process::id()
    ))
}

fn remove_database(path: &Path) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}-wal", path.display()));
    let _ = std::fs::remove_file(format!("{}-shm", path.display()));
}

/// A real temporary SQLite database: 20 000 follow records between other DIDs,
/// all NEWER than the target's followers, so the old newest-first scan of the
/// whole follow collection never fills its LIMIT; the target itself has four
/// followers, two sharing a `time_us` so the uri DESC tie-break is pinned.
fn seed(path: &Path) {
    remove_database(path);
    drop(SqliteStorage::open(path).unwrap());
    let mut connection = Connection::open(path).unwrap();
    let transaction = connection.transaction().unwrap();
    {
        let mut insert = transaction
            .prepare_cached(
                "INSERT INTO records(did,collection,rkey,uri,cid,time_us,record_json)
                 VALUES(?1,?2,?3,?4,?5,?6,?7)",
            )
            .unwrap();
        for index in 0..20_000 {
            let actor = format!("did:plc:bulk-actor-{}", index % 300);
            let subject = format!("did:plc:bulk-target-{}", index % 50);
            let rkey = format!("f-{index}");
            insert
                .execute(params![
                    actor,
                    "app.bsky.graph.follow",
                    rkey,
                    follow_uri(&actor, &rkey),
                    format!("cid-{index}"),
                    BASE + 1_000 + index as i64,
                    format!(r#"{{"subject":"{subject}"}}"#)
                ])
                .unwrap();
        }
        for (actor, rkey, time_us) in [
            ("did:plc:f-old", "old", BASE + 10),
            ("did:plc:f-b", "two", BASE + 20),
            ("did:plc:f-a", "two", BASE + 20),
            ("did:plc:f-new", "new", BASE + 30),
        ] {
            insert
                .execute(params![
                    actor,
                    "app.bsky.graph.follow",
                    rkey,
                    follow_uri(actor, rkey),
                    format!("cid-{actor}-{rkey}"),
                    time_us,
                    format!(r#"{{"subject":"{TARGET}"}}"#)
                ])
                .unwrap();
        }
    }
    transaction.commit().unwrap();
    drop(connection);
}

#[test]
fn followers_page_keeps_its_rows_and_uri_tie_break() {
    let path = database_path("page");
    seed(&path);
    let storage = SqliteStorage::open(&path).unwrap();

    let page = storage.followers_for_did(TARGET, 50).unwrap();
    assert_eq!(
        page.iter()
            .map(|record| record.uri.as_str())
            .collect::<Vec<_>>(),
        vec![
            follow_uri("did:plc:f-new", "new"),
            follow_uri("did:plc:f-b", "two"),
            follow_uri("did:plc:f-a", "two"),
            follow_uri("did:plc:f-old", "old"),
        ],
        "newest-first with uri DESC on ties, only the target's followers"
    );

    drop(storage);
    remove_database(&path);
}

#[test]
fn followers_query_does_not_walk_the_follow_collection() {
    let path = database_path("work");
    seed(&path);
    let storage = SqliteStorage::open(&path).unwrap();

    let (page, work) = storage.followers_for_did_counted(TARGET, 50).unwrap();
    assert_eq!(page.len(), 4);
    assert_eq!(
        work.fullscan_steps, 0,
        "the followers query must not run a full scan: {work:?}"
    );
    assert!(
        work.vm_steps < 20_000,
        "the followers query costs {work:?} VM steps against a 20 004-row follow set: \
         its cost must be bounded by this DID's followers, not by the collection size"
    );

    drop(storage);
    remove_database(&path);
}
