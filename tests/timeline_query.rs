use appview_rs::ingest::{RecordCursor, SqliteStorage, Storage};
use rusqlite::{Connection, params};
use std::path::{Path, PathBuf};

const BASE: i64 = 1_700_000_000_000_000;
const VIEWER: &str = "did:plc:viewer";
const FOLLOWED_A: &str = "did:plc:followed-a";
const FOLLOWED_B: &str = "did:plc:followed-b";

fn post_uri(did: &str, rkey: &str) -> String {
    format!("at://{did}/app.bsky.feed.post/{rkey}")
}

fn database_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(format!(
        "target/timeline-query-av65q-{}-{name}.sqlite3",
        std::process::id()
    ))
}

fn remove_database(path: &Path) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}-wal", path.display()));
    let _ = std::fs::remove_file(format!("{}-shm", path.display()));
}

/// A real temporary SQLite database (not the in-memory store): 20 000 posts by
/// 400 un-followed authors, all NEWER than anything the viewer could see, plus a
/// viewer who follows two authors with a few old posts and has two posts of their
/// own, two of which share a `time_us` so the cursor's tie-break on `uri` is
/// exercised at a page boundary.
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
        for author in 0..400 {
            let did = format!("did:plc:other-{author:03}");
            for index in 0..50 {
                let rkey = format!("p-{author}-{index}");
                insert
                    .execute(params![
                        did,
                        "app.bsky.feed.post",
                        rkey,
                        post_uri(&did, &rkey),
                        format!("cid-other-{author}-{index}"),
                        BASE + 1_000_000 + (author * 50 + index) as i64,
                        r#"{"text":"unfollowed author"}"#
                    ])
                    .unwrap();
            }
        }
        for (rkey, subject, time_us) in [
            ("f-a", FOLLOWED_A, BASE + 10),
            ("f-b", FOLLOWED_B, BASE + 11),
        ] {
            insert
                .execute(params![
                    VIEWER,
                    "app.bsky.graph.follow",
                    rkey,
                    format!("at://{VIEWER}/app.bsky.graph.follow/{rkey}"),
                    format!("cid-{rkey}"),
                    time_us,
                    format!(r#"{{"subject":"{subject}"}}"#)
                ])
                .unwrap();
        }
        for (did, rkey, time_us) in [
            (VIEWER, "v-hi", BASE + 500_000),
            (VIEWER, "z9-vtie", BASE + 400_000),
            (FOLLOWED_A, "bbb", BASE + 400_000),
            (FOLLOWED_A, "aaa", BASE + 400_000),
            (FOLLOWED_A, "a-old", BASE + 100_000),
            (FOLLOWED_B, "b-old", BASE + 50_000),
        ] {
            insert
                .execute(params![
                    did,
                    "app.bsky.feed.post",
                    rkey,
                    post_uri(did, rkey),
                    format!("cid-{did}-{rkey}"),
                    time_us,
                    r#"{"text":"timeline"}"#
                ])
                .unwrap();
        }
    }
    transaction.commit().unwrap();
    drop(connection);
}

fn expected_uri(did: &str, rkey: &str) -> String {
    post_uri(did, rkey)
}

#[test]
fn home_timeline_page_keeps_its_rows_order_and_cursor_tie_break() {
    let path = database_path("page");
    seed(&path);
    let storage = SqliteStorage::open(&path).unwrap();

    let page = storage.timeline_posts(VIEWER, None, 50).unwrap();
    assert_eq!(
        page.iter()
            .map(|record| record.uri.as_str())
            .collect::<Vec<_>>(),
        vec![
            expected_uri(VIEWER, "v-hi"),
            expected_uri(VIEWER, "z9-vtie"),
            expected_uri(FOLLOWED_A, "bbb"),
            expected_uri(FOLLOWED_A, "aaa"),
            expected_uri(FOLLOWED_A, "a-old"),
            expected_uri(FOLLOWED_B, "b-old"),
        ],
        "newest-first, uri DESC on ties, the viewer's own posts included, nothing else"
    );

    let first = storage.timeline_posts(VIEWER, None, 3).unwrap();
    assert_eq!(
        first
            .iter()
            .map(|record| record.uri.as_str())
            .collect::<Vec<_>>(),
        vec![
            expected_uri(VIEWER, "v-hi"),
            expected_uri(VIEWER, "z9-vtie"),
            expected_uri(FOLLOWED_A, "bbb"),
        ]
    );
    let cursor = RecordCursor {
        time_us: first[2].time_us,
        uri: first[2].uri.clone(),
    };
    let second = storage.timeline_posts(VIEWER, Some(&cursor), 50).unwrap();
    assert_eq!(
        second
            .iter()
            .map(|record| record.uri.as_str())
            .collect::<Vec<_>>(),
        vec![
            expected_uri(FOLLOWED_A, "aaa"),
            expected_uri(FOLLOWED_A, "a-old"),
            expected_uri(FOLLOWED_B, "b-old"),
        ],
        "the next page must resume inside the shared time_us via the uri tie-break"
    );

    drop(storage);
    remove_database(&path);
}

#[test]
fn home_timeline_query_does_not_walk_the_post_collection() {
    let path = database_path("work");
    seed(&path);
    let storage = SqliteStorage::open(&path).unwrap();

    let (page, work) = storage.timeline_posts_counted(VIEWER, None, 50).unwrap();
    assert_eq!(page.len(), 6);
    assert_eq!(
        work.fullscan_steps, 0,
        "the timeline query must not run a full scan: {work:?}"
    );
    assert!(
        work.vm_steps < 20_000,
        "the timeline query costs {work:?} VM steps against a 20 006-row post table: \
         its cost must be bounded by the viewer's follows, not by the table size"
    );

    drop(storage);
    remove_database(&path);
}

#[test]
fn viewer_interaction_lookup_uses_the_per_author_index_and_keeps_newest_uri() {
    let path = database_path("viewer-interactions");
    seed(&path);
    let mut connection = Connection::open(&path).unwrap();
    let transaction = connection.transaction().unwrap();
    {
        let mut insert = transaction
            .prepare_cached(
                "INSERT INTO records(did,collection,rkey,uri,cid,time_us,record_json)
                 VALUES(?1,?2,?3,?4,?5,?6,?7)",
            )
            .unwrap();
        for index in 0..2_000 {
            let did = format!("did:plc:like-{index:04}");
            let rkey = format!("l-{index}");
            insert
                .execute(params![
                    did,
                    "app.bsky.feed.like",
                    rkey,
                    format!("at://{did}/app.bsky.feed.like/{rkey}"),
                    format!("cid-like-{index}"),
                    BASE + index,
                    format!(
                        r#"{{"subject":{{"uri":"{}"}}}}"#,
                        expected_uri(VIEWER, "v-hi")
                    ),
                ])
                .unwrap();
        }
        for (collection, prefix) in [
            ("app.bsky.feed.like", "like"),
            ("app.bsky.feed.repost", "repost"),
        ] {
            for (rkey, time_us) in [
                (format!("{prefix}-old"), BASE + 10),
                (format!("{prefix}-new"), BASE + 20),
            ] {
                insert
                    .execute(params![
                        VIEWER,
                        collection,
                        rkey,
                        format!("at://{VIEWER}/{collection}/{rkey}"),
                        format!("cid-{collection}-{time_us}"),
                        time_us,
                        format!(
                            r#"{{"subject":{{"uri":"{}"}}}}"#,
                            expected_uri(VIEWER, "v-hi")
                        ),
                    ])
                    .unwrap();
            }
        }
    }
    transaction.commit().unwrap();
    drop(connection);

    let storage = SqliteStorage::open(&path).unwrap();
    let summary = storage
        .post_interaction_summary(&expected_uri(VIEWER, "v-hi"), Some(VIEWER))
        .unwrap();
    assert_eq!(
        summary.viewer_like,
        Some(format!("at://{VIEWER}/app.bsky.feed.like/like-new"))
    );
    assert_eq!(
        summary.viewer_repost,
        Some(format!("at://{VIEWER}/app.bsky.feed.repost/repost-new"))
    );

    let connection = Connection::open(&path).unwrap();
    for collection in ["app.bsky.feed.like", "app.bsky.feed.repost"] {
        let query = format!(
            "EXPLAIN QUERY PLAN SELECT uri FROM records
             WHERE collection='{collection}' AND did=?1
               AND json_extract(record_json, '$.subject.uri')=?2
             ORDER BY time_us DESC LIMIT 1"
        );
        let details = connection
            .prepare(&query)
            .unwrap()
            .query_map(params![VIEWER, expected_uri(VIEWER, "v-hi")], |row| {
                row.get::<_, String>(3)
            })
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap()
            .join("; ");
        assert!(
            details.contains("records_author_subject_uri"),
            "viewer {collection} query plan must use the per-author index: {details}"
        );
    }

    drop(storage);
    remove_database(&path);
}

#[test]
fn a_viewer_who_follows_themselves_sees_each_own_post_once() {
    // The per-author walk already reads the viewer's own posts; a self-follow must
    // not walk them a second time.
    let path = database_path("self-follow");
    seed(&path);
    let connection = Connection::open(&path).unwrap();
    connection
        .execute(
            "INSERT INTO records(did,collection,rkey,uri,cid,time_us,record_json)
             VALUES(?1,'app.bsky.graph.follow','f-self',?2,'cid-f-self',?3,?4)",
            params![
                VIEWER,
                format!("at://{VIEWER}/app.bsky.graph.follow/f-self"),
                BASE + 12,
                format!(r#"{{"subject":"{VIEWER}"}}"#)
            ],
        )
        .unwrap();
    drop(connection);
    let storage = SqliteStorage::open(&path).unwrap();

    let page = storage.timeline_posts(VIEWER, None, 50).unwrap();
    assert_eq!(
        page.iter()
            .map(|record| record.uri.as_str())
            .collect::<Vec<_>>(),
        vec![
            expected_uri(VIEWER, "v-hi"),
            expected_uri(VIEWER, "z9-vtie"),
            expected_uri(FOLLOWED_A, "bbb"),
            expected_uri(FOLLOWED_A, "aaa"),
            expected_uri(FOLLOWED_A, "a-old"),
            expected_uri(FOLLOWED_B, "b-old"),
        ]
    );

    drop(storage);
    remove_database(&path);
}
