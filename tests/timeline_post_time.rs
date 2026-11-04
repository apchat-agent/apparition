use appview_rs::ingest::{PostTimeCursor, SqliteStorage, Storage, post_created_at};
use rusqlite::{Connection, params};
use std::collections::HashSet;
use std::path::{Path, PathBuf};

const VIEWER: &str = "did:plc:viewer";
const FOLLOWED_A: &str = "did:plc:followed-a";
const FOLLOWED_B: &str = "did:plc:followed-b";
const FOLLOWED_C: &str = "did:plc:followed-c";

fn post_uri(did: &str, rkey: &str) -> String {
    format!("at://{did}/app.bsky.feed.post/{rkey}")
}

fn database_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(format!(
        "target/timeline-post-time-av80d-{}-{name}.sqlite3",
        std::process::id()
    ))
}

fn remove_database(path: &Path) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}-wal", path.display()));
    let _ = std::fs::remove_file(format!("{}-shm", path.display()));
}

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
        for (rkey, subject) in [
            ("f-a", FOLLOWED_A),
            ("f-b", FOLLOWED_B),
            ("f-c", FOLLOWED_C),
        ] {
            insert
                .execute(params![
                    VIEWER,
                    "app.bsky.graph.follow",
                    rkey,
                    format!("at://{VIEWER}/app.bsky.graph.follow/{rkey}"),
                    format!("cid-{rkey}"),
                    10,
                    format!(r#"{{"subject":"{subject}"}}"#),
                ])
                .unwrap();
        }
        for (did, rkey, time_us, created_at, text) in [
            (
                FOLLOWED_A,
                "old-reply",
                100,
                "2026-11-03T23:44:32Z",
                "old reply",
            ),
            (
                FOLLOWED_B,
                "new-reply",
                90,
                "2026-11-04T06:42:08Z",
                "new reply",
            ),
            (
                FOLLOWED_C,
                "parent-old",
                80,
                "2026-11-03T20:00:00Z",
                "older parent",
            ),
            (
                FOLLOWED_C,
                "parent-new",
                70,
                "2026-11-04T06:42:08Z",
                "newer parent",
            ),
            (
                FOLLOWED_C,
                "c-new",
                60,
                "2026-11-04T06:42:08Z",
                "top-level post",
            ),
        ] {
            insert
                .execute(params![
                    did,
                    "app.bsky.feed.post",
                    rkey,
                    post_uri(did, rkey),
                    format!("cid-{rkey}"),
                    time_us,
                    format!(
                        r#"{{"$type":"app.bsky.feed.post","text":"{}","createdAt":"{}"}}"#,
                        text, created_at
                    ),
                ])
                .unwrap();
        }
    }
    transaction.commit().unwrap();
    drop(connection);
}

#[test]
fn post_time_timeline_pages_without_duplicates_or_gaps() {
    let path = database_path("page");
    seed(&path);
    let storage = SqliteStorage::open(&path).unwrap();

    let first = storage
        .timeline_posts_by_post_time(VIEWER, None, 2)
        .unwrap();
    assert_eq!(
        first
            .iter()
            .map(|record| record.uri.as_str())
            .collect::<Vec<_>>(),
        vec![
            post_uri(FOLLOWED_B, "new-reply"),
            post_uri(FOLLOWED_C, "parent-new"),
        ],
        "the first page must be newest by createdAt, not by arrival time_us"
    );

    let last = first[1].clone();
    let cursor = PostTimeCursor {
        created_at: post_created_at(&last),
        time_us: last.time_us,
        uri: last.uri.clone(),
    };
    let second = storage
        .timeline_posts_by_post_time(VIEWER, Some(&cursor), 10)
        .unwrap();
    assert_eq!(
        second
            .iter()
            .map(|record| record.uri.as_str())
            .collect::<Vec<_>>(),
        vec![
            post_uri(FOLLOWED_C, "c-new"),
            post_uri(FOLLOWED_A, "old-reply"),
            post_uri(FOLLOWED_C, "parent-old"),
        ],
        "the next page must resume after the createdAt/time_us/uri cursor with no gaps"
    );

    let mut all = first
        .iter()
        .map(|record| record.uri.as_str())
        .collect::<Vec<_>>();
    all.extend(second.iter().map(|record| record.uri.as_str()));
    assert_eq!(
        all.len(),
        all.iter().collect::<HashSet<_>>().len(),
        "pages must not repeat a post"
    );

    drop(storage);
    remove_database(&path);
}
