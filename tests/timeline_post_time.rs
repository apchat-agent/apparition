use appview_rs::ingest::{IndexedRecord, SortTimeCursor, SqliteStorage, Storage};
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

fn post_record(did: &str, rkey: &str, time_us: u64, created_at: &str, text: &str) -> IndexedRecord {
    IndexedRecord {
        did: did.into(),
        collection: "app.bsky.feed.post".into(),
        rkey: rkey.into(),
        uri: post_uri(did, rkey),
        cid: format!("cid-{rkey}").into(),
        time_us,
        record: serde_json::json!({
            "$type": "app.bsky.feed.post",
            "text": text,
            "createdAt": created_at,
        }),
    }
}

fn follow_record(did: &str, subject: &str) -> IndexedRecord {
    IndexedRecord {
        did: did.into(),
        collection: "app.bsky.graph.follow".into(),
        rkey: format!("f-{}", subject),
        uri: format!("at://{did}/app.bsky.graph.follow/f-{}", subject),
        cid: format!("cid-f-{}", subject).into(),
        time_us: 10,
        record: serde_json::json!({ "subject": subject }),
    }
}

fn seed(path: &Path) {
    remove_database(path);
    let storage = SqliteStorage::open(path).unwrap();
    for subject in [FOLLOWED_A, FOLLOWED_B, FOLLOWED_C] {
        storage.put_record(&follow_record(VIEWER, subject)).unwrap();
    }
    for record in [
        post_record(
            FOLLOWED_A,
            "old-reply",
            1_793_800_000_000_000,
            "2026-11-03T23:44:32Z",
            "old reply",
        ),
        post_record(
            FOLLOWED_B,
            "new-reply",
            1_793_776_000_000_000,
            "2026-11-04T06:42:08Z",
            "new reply",
        ),
        post_record(
            FOLLOWED_C,
            "parent-old",
            1_793_760_000_000_000,
            "2026-11-03T20:00:00Z",
            "older parent",
        ),
        post_record(
            FOLLOWED_C,
            "parent-new",
            1_793_775_000_000_000,
            "2026-11-04T06:42:08Z",
            "newer parent",
        ),
        post_record(
            FOLLOWED_C,
            "c-new",
            1_793_774_600_000_000,
            "2026-11-04T06:42:08Z",
            "top-level post",
        ),
    ] {
        storage.put_record(&record).unwrap();
    }
    drop(storage);
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
        "the first page must be newest by post sort time, not by arrival time_us"
    );

    let last = first[1].clone();
    let cursor = SortTimeCursor {
        sort_us: storage.sort_us_for_record(&last).unwrap(),
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
        "the next page must resume after the sort-time/time_us/uri cursor with no gaps"
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
