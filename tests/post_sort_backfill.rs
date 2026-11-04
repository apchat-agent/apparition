use appview_rs::ingest::{SqliteStorage, Storage};
use rusqlite::{Connection, params};
use std::path::{Path, PathBuf};

const VIEWER: &str = "did:plc:viewer";
const FOLLOWED_A: &str = "did:plc:followed-a";
const FOLLOWED_B: &str = "did:plc:followed-b";

fn database_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(format!(
        "target/post-sort-backfill-av80d-{}-{name}.sqlite3",
        std::process::id()
    ))
}

fn remove_database(path: &Path) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}-wal", path.display()));
    let _ = std::fs::remove_file(format!("{}-shm", path.display()));
}

fn post_uri(index: usize) -> String {
    format!("at://did:plc:example/app.bsky.feed.post/p-{index}")
}

fn seed_backfill_order(path: &Path, future_created_at: &str) {
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
        for subject in [FOLLOWED_A, FOLLOWED_B] {
            insert
                .execute(params![
                    VIEWER,
                    "app.bsky.graph.follow",
                    format!("f-{}", subject),
                    format!("at://{VIEWER}/app.bsky.graph.follow/f-{}", subject),
                    format!("cid-f-{}", subject),
                    10,
                    format!(r#"{{"subject":"{}"}}"#, subject),
                ])
                .unwrap();
        }
    }

    {
        let mut insert = transaction
            .prepare_cached(
                "INSERT INTO records(did,collection,rkey,uri,cid,time_us,record_json)
                 VALUES(?1,?2,?3,?4,?5,?6,?7)",
            )
            .unwrap();
        insert
            .execute(params![
                FOLLOWED_A,
                "app.bsky.feed.post",
                "future",
                format!("at://{FOLLOWED_A}/app.bsky.feed.post/future"),
                "cid-future",
                1_793_700_000_000_000_i64,
                format!(
                    r#"{{"$type":"app.bsky.feed.post","text":"future-dated","createdAt":"{}"}}"#,
                    future_created_at,
                ),
            ])
            .unwrap();
        insert
            .execute(params![
                FOLLOWED_B,
                "app.bsky.feed.post",
                "newer",
                format!("at://{FOLLOWED_B}/app.bsky.feed.post/newer"),
                "cid-newer",
                1_793_800_000_000_000_i64,
                r#"{"$type":"app.bsky.feed.post","text":"newer","createdAt":"2026-11-04T06:42:08Z"}"#,
            ])
            .unwrap();
    }

    transaction.commit().unwrap();
    drop(connection);
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
        for index in 0..12 {
            let rkey = format!("p-{index}");
            insert
                .execute(params![
                    "did:plc:example",
                    "app.bsky.feed.post",
                    rkey,
                    post_uri(index),
                    format!("cid-{index}"),
                    1_700_000_000_000_000_i64 + index as i64,
                    format!(
                        r#"{{"$type":"app.bsky.feed.post","text":"post-{index}","createdAt":"2026-11-04T06:42:08Z"}}"#,
                    ),
                ])
                .unwrap();
        }
    }

    {
        let mut insert = transaction
            .prepare_cached("INSERT INTO post_sort(uri,did,sort_us,time_us) VALUES(?1,?2,?3,?4)")
            .unwrap();
        for index in 0..4 {
            insert
                .execute(params![
                    post_uri(index),
                    "did:plc:example",
                    1_700_000_000_000_000_i64 + index as i64,
                    1_700_000_000_000_000_i64 + index as i64,
                ])
                .unwrap();
        }
    }

    transaction.commit().unwrap();
    drop(connection);
}

#[test]
fn post_sort_backfill_resumes_after_the_previous_batch_last_rowid() {
    let path = database_path("resume");
    seed(&path);
    let storage = SqliteStorage::open(&path).unwrap();
    let trace = storage.trace_statements();

    trace.clear();
    let _first = storage.backfill_post_sort_batch(2).unwrap();
    let first_statements = trace.statements();
    let first_batch = first_statements
        .iter()
        .filter(|sql| sql.contains("FROM records AS r") && sql.contains("r.rowid"))
        .collect::<Vec<_>>();
    assert!(
        !first_batch.is_empty(),
        "the first batch must execute a records query"
    );
    assert!(
        first_batch.iter().any(|sql| sql.contains("r.rowid >")),
        "the first batch must bound its walk by rowid, not start from zero: {first_batch:?}"
    );

    trace.clear();
    let _second = storage.backfill_post_sort_batch(2).unwrap();
    let second_statements = trace.statements();
    let second_batch = second_statements
        .iter()
        .filter(|sql| sql.contains("FROM records AS r") && sql.contains("r.rowid"))
        .collect::<Vec<_>>();
    assert!(
        !second_batch.is_empty(),
        "the second batch must execute a records query"
    );
    assert!(
        second_batch.iter().any(|sql| sql.contains("r.rowid > 6")),
        "the second batch must resume above the first batch's last rowid 6: {second_batch:?}"
    );

    let connection = Connection::open(&path).unwrap();
    for sql in second_batch {
        let plan = connection
            .prepare(&format!("EXPLAIN QUERY PLAN {sql}"))
            .unwrap()
            .query_map([], |row| row.get::<_, String>(3))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(
            plan.iter()
                .any(|line| line.contains("rowid") || line.contains("INTEGER PRIMARY KEY")),
            "the batch plan must search a rowid range: {plan:?}"
        );
    }
    drop(connection);
    drop(storage);
    remove_database(&path);
}

#[test]
fn post_sort_backfill_remaining_is_computed_once_and_decremented() {
    let path = database_path("remaining");
    seed(&path);
    let storage = SqliteStorage::open(&path).unwrap();
    let trace = storage.trace_statements();

    trace.clear();
    let first = storage.post_sort_backfill_remaining().unwrap();
    let first_statements = trace.statements();
    assert!(
        first_statements
            .iter()
            .any(|sql| sql.contains("COUNT(*) FROM records")),
        "the first remaining query must count post rows: {first_statements:?}"
    );
    assert!(
        first_statements
            .iter()
            .any(|sql| sql.contains("COUNT(*) FROM post_sort")),
        "the first remaining query must count filled rows: {first_statements:?}"
    );

    trace.clear();
    let second = storage.post_sort_backfill_remaining().unwrap();
    let second_statements = trace.statements();
    assert_eq!(
        second, first,
        "the remaining count must be cached after the first call"
    );
    assert!(
        !second_statements.iter().any(|sql| sql.contains("COUNT(*)")),
        "a cached health call must not recount records or post_sort: {second_statements:?}"
    );

    drop(storage);
    remove_database(&path);
}

#[test]
fn backfilled_future_created_at_sorts_at_arrival_below_newer_post() {
    let path = database_path("backfill-future");
    seed_backfill_order(&path, "2099-01-01T00:00:00Z");
    let storage = SqliteStorage::open(&path).unwrap();

    let remaining = storage.post_sort_backfill_remaining().unwrap();
    assert_eq!(
        remaining,
        2,
        "both seeded posts must start without a post_sort row: {remaining:?}"
    );

    let last_rowid = storage.backfill_post_sort_batch(10).unwrap();
    assert!(
        last_rowid != 0,
        "the batch must examine the unfilled posts, not report zero: {last_rowid:?}"
    );

    let page = storage
        .timeline_posts_by_post_time(VIEWER, None, 10)
        .unwrap();
    let uris = page
        .iter()
        .map(|record| record.uri.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        uris,
        vec![
            format!("at://{FOLLOWED_B}/app.bsky.feed.post/newer"),
            format!("at://{FOLLOWED_A}/app.bsky.feed.post/future"),
        ],
        "after backfill, the future-dated post must sort at its arrival time below the newer real post: {uris:?}"
    );

    drop(storage);
    remove_database(&path);
}

#[test]
fn backfilled_unparseable_created_at_falls_back_to_arrival() {
    let path = database_path("backfill-junk");
    seed_backfill_order(&path, "zzz");
    let storage = SqliteStorage::open(&path).unwrap();

    let remaining = storage.post_sort_backfill_remaining().unwrap();
    assert_eq!(
        remaining,
        2,
        "both seeded posts must start without a post_sort row: {remaining:?}"
    );

    let last_rowid = storage.backfill_post_sort_batch(10).unwrap();
    assert!(
        last_rowid != 0,
        "the batch must examine the unfilled posts, not report zero: {last_rowid:?}"
    );

    let page = storage
        .timeline_posts_by_post_time(VIEWER, None, 10)
        .unwrap();
    let uris = page
        .iter()
        .map(|record| record.uri.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        uris,
        vec![
            format!("at://{FOLLOWED_B}/app.bsky.feed.post/newer"),
            format!("at://{FOLLOWED_A}/app.bsky.feed.post/future"),
        ],
        "after backfill, an unparseable createdAt must fall back to arrival and not sort on top: {uris:?}"
    );

    drop(storage);
    remove_database(&path);
}
