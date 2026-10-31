use appview_rs::ingest::{
    IndexedRecord, JetstreamStorageUpdate, SqliteStorage, StatementWork, Storage,
};
use rusqlite::{Connection, params};
use serde_json::json;
use std::path::{Path, PathBuf};

const VIEWER: &str = "did:plc:viewer";
const PAGE_SIZE: usize = 25;

fn database_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(format!(
        "target/post-counts-av67c-{}-{name}.sqlite3",
        std::process::id()
    ))
}

fn remove_database(path: &Path) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}-wal", path.display()));
    let _ = std::fs::remove_file(format!("{}-shm", path.display()));
}

fn post_uri(index: usize) -> String {
    format!("at://did:plc:post-{index:02}/app.bsky.feed.post/post")
}

fn now_us() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64
}

fn record(did: &str, collection: &str, rkey: &str, target: &str) -> IndexedRecord {
    let value = match collection {
        "app.bsky.feed.like" | "app.bsky.feed.repost" => {
            json!({"subject":{"uri":target}})
        }
        "app.bsky.feed.post" if rkey.starts_with("reply") => {
            json!({"reply":{"parent":{"uri":target}}})
        }
        "app.bsky.feed.post" if rkey.starts_with("quote") => {
            json!({"embed":{"record":{"uri":target}}})
        }
        _ => json!({"text":"post"}),
    };
    IndexedRecord {
        did: did.to_owned(),
        collection: collection.to_owned(),
        rkey: rkey.to_owned(),
        uri: format!("at://{did}/{collection}/{rkey}"),
        cid: Some(format!("cid-{did}-{collection}-{rkey}")),
        time_us: now_us(),
        record: value,
    }
}

fn seed_interaction_page(path: &Path) {
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
        for post in 0..PAGE_SIZE {
            let target = post_uri(post);
            insert
                .execute(params![
                    format!("did:plc:post-{post:02}"),
                    "app.bsky.feed.post",
                    "post",
                    target,
                    format!("cid-post-{post}"),
                    1_800_000_000_000_000_i64 + post as i64,
                    "{}",
                ])
                .unwrap();
            for index in 0..100 {
                for collection in ["app.bsky.feed.like", "app.bsky.feed.repost"] {
                    let did = format!("did:plc:{collection}-{post}-{index}");
                    let rkey = format!("i-{index}");
                    insert
                        .execute(params![
                            did,
                            collection,
                            rkey,
                            format!("at://{did}/{collection}/{rkey}"),
                            format!("cid-{collection}-{post}-{index}"),
                            index as i64,
                            json!({"subject":{"uri":target}}).to_string(),
                        ])
                        .unwrap();
                }
                for (rkey, value) in [
                    (
                        format!("reply-{index}"),
                        json!({"reply":{"parent":{"uri":target}}}),
                    ),
                    (
                        format!("quote-{index}"),
                        json!({"embed":{"record":{"uri":target}}}),
                    ),
                ] {
                    let did = format!("did:plc:post-interaction-{post}-{index}-{rkey}");
                    insert
                        .execute(params![
                            did,
                            "app.bsky.feed.post",
                            rkey,
                            format!("at://{did}/app.bsky.feed.post/{rkey}"),
                            format!("cid-{post}-{index}-{rkey}"),
                            index as i64,
                            value.to_string(),
                        ])
                        .unwrap();
                }
            }
            for collection in ["app.bsky.feed.like", "app.bsky.feed.repost"] {
                let rkey = format!("viewer-{post}");
                let did = VIEWER;
                insert
                    .execute(params![
                        did,
                        collection,
                        rkey,
                        format!("at://{did}/{collection}/{rkey}"),
                        format!("cid-{collection}-{post}"),
                        2_000_000_i64 + post as i64,
                        json!({"subject":{"uri":target}}).to_string(),
                    ])
                    .unwrap();
            }
        }
    }
    transaction.commit().unwrap();
}

fn sql_counts(connection: &Connection, uri: &str) -> (i64, i64, i64, i64) {
    let count = |collection: &str, path: &str| {
        connection
            .query_row(
                &format!(
                    "SELECT COUNT(*) FROM records WHERE collection=?1
                     AND json_extract(record_json, '{path}')=?2"
                ),
                params![collection, uri],
                |row| row.get(0),
            )
            .unwrap()
    };
    (
        count("app.bsky.feed.like", "$.subject.uri"),
        count("app.bsky.feed.repost", "$.subject.uri"),
        count("app.bsky.feed.post", "$.reply.parent.uri"),
        count("app.bsky.feed.post", "$.embed.record.uri"),
    )
}

fn cached_counts(connection: &Connection, uri: &str) -> (i64, i64, i64, i64) {
    connection
        .query_row(
            "SELECT like_count,repost_count,reply_count,quote_count
             FROM post_counts WHERE uri=?1",
            [uri],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?)),
        )
        .unwrap()
}

#[test]
fn a_page_of_post_summaries_has_bounded_work_after_the_counts_are_warm() {
    let path = database_path("page-work");
    seed_interaction_page(&path);
    let storage = SqliteStorage::open(&path).unwrap();

    // The first read lazily fills counts for an existing database. Measure the
    // next page render, where each post should use the small cached row.
    for post in 0..PAGE_SIZE {
        let summary = storage
            .post_interaction_summary(&post_uri(post), Some(VIEWER))
            .unwrap();
        assert_eq!(summary.like_count, 101);
        assert_eq!(summary.repost_count, 101);
        assert_eq!(summary.reply_count, 100);
        assert_eq!(summary.quote_count, 100);
    }
    let mut page_work = StatementWork::default();
    for post in 0..PAGE_SIZE {
        let (_, work) = storage
            .post_interaction_summary_counted(&post_uri(post), Some(VIEWER))
            .unwrap();
        page_work.vm_steps += work.vm_steps;
        page_work.fullscan_steps += work.fullscan_steps;
    }
    assert_eq!(page_work.fullscan_steps, 0, "{page_work:?}");
    assert!(
        page_work.vm_steps < PAGE_SIZE as u64 * 500,
        "a 25-post cached summary page must cost one small lookup per post; got {page_work:?}"
    );

    drop(storage);
    remove_database(&path);
}

#[test]
fn cached_counts_track_record_writes_and_lazy_reads_match_the_count_queries() {
    let path = database_path("write-correctness");
    remove_database(&path);
    let storage = SqliteStorage::open(&path).unwrap();
    let a = post_uri(100);
    let b = post_uri(101);
    let c = post_uri(102);
    for (index, uri) in [a.as_str(), b.as_str(), c.as_str()].into_iter().enumerate() {
        let did = format!("did:plc:post-{index}");
        storage
            .put_record(&IndexedRecord {
                did: did.clone(),
                collection: "app.bsky.feed.post".to_owned(),
                rkey: "post".to_owned(),
                uri: uri.to_owned(),
                cid: Some(format!("cid-{index}")),
                time_us: now_us(),
                record: json!({"text":"root"}),
            })
            .unwrap();
    }
    // A's count row exists before the following writes. B and C are first read
    // after all changes, exercising the lazy existing-database path.
    storage.post_interaction_summary(&a, None).unwrap();
    assert_eq!(
        cached_counts(&Connection::open(&path).unwrap(), &a),
        (0, 0, 0, 0)
    );

    let keep = record("did:plc:liker", "app.bsky.feed.like", "keep", &a);
    storage.put_record(&keep).unwrap();
    storage.put_record(&keep).unwrap(); // re-delivery must not double-count
    let mut moved = record("did:plc:liker", "app.bsky.feed.like", "moved", &a);
    storage.put_record(&moved).unwrap();
    moved.record = json!({"subject":{"uri":b}});
    storage.put_record(&moved).unwrap(); // changing the subject moves the count

    let transient = record("did:plc:deleter", "app.bsky.feed.like", "transient", &a);
    storage.put_record(&transient).unwrap();
    storage
        .delete_record(&transient.did, &transient.collection, &transient.rkey)
        .unwrap();

    let jetstream_repost = record("did:plc:jetstream", "app.bsky.feed.repost", "repost", &a);
    let reply = record("did:plc:reply", "app.bsky.feed.post", "reply-a", &a);
    let quote = record("did:plc:quote", "app.bsky.feed.post", "quote-a", &a);
    storage
        .apply_jetstream_batch(&[
            JetstreamStorageUpdate {
                upsert: Some(jetstream_repost.clone()),
                ..JetstreamStorageUpdate::default()
            },
            JetstreamStorageUpdate {
                upsert: Some(reply),
                ..JetstreamStorageUpdate::default()
            },
            JetstreamStorageUpdate {
                upsert: Some(quote),
                ..JetstreamStorageUpdate::default()
            },
        ])
        .unwrap();
    storage
        .apply_jetstream_batch(&[JetstreamStorageUpdate {
            delete: Some((
                jetstream_repost.did.clone(),
                jetstream_repost.collection.clone(),
                jetstream_repost.rkey.clone(),
            )),
            ..JetstreamStorageUpdate::default()
        }])
        .unwrap();

    let old_snapshot = record("did:plc:snapshot", "app.bsky.feed.like", "snap-like", &a);
    storage.put_record(&old_snapshot).unwrap();
    let replacement = vec![
        record("did:plc:snapshot", "app.bsky.feed.like", "snap-like", &b),
        record("did:plc:snapshot", "app.bsky.feed.post", "quote-c", &c),
    ];
    storage
        .replace_repo_snapshot("did:plc:snapshot", &replacement, &[])
        .unwrap();

    let connection = Connection::open(&path).unwrap();
    for uri in [&a, &b, &c] {
        let summary = storage.post_interaction_summary(uri, None).unwrap();
        let expected = sql_counts(&connection, uri);
        let actual = (
            summary.like_count as i64,
            summary.repost_count as i64,
            summary.reply_count as i64,
            summary.quote_count as i64,
        );
        assert_eq!(actual, expected, "summary for {uri}");
        assert_eq!(cached_counts(&connection, uri), expected, "cache for {uri}");
    }
    assert_eq!(cached_counts(&connection, &a), (1, 0, 1, 1));
    assert_eq!(cached_counts(&connection, &b), (2, 0, 0, 0));
    assert_eq!(cached_counts(&connection, &c), (0, 0, 0, 1));

    drop(connection);
    drop(storage);
    remove_database(&path);
}

#[test]
fn moving_an_interaction_into_a_warm_post_updates_both_cached_rows() {
    let path = database_path("move-into-warm");
    remove_database(&path);
    let storage = SqliteStorage::open(&path).unwrap();
    let from = post_uri(200);
    let to = post_uri(201);
    for (index, uri) in [from.as_str(), to.as_str()].into_iter().enumerate() {
        storage
            .put_record(&IndexedRecord {
                did: format!("did:plc:move-post-{index}"),
                collection: "app.bsky.feed.post".to_owned(),
                rkey: "post".to_owned(),
                uri: uri.to_owned(),
                cid: Some(format!("cid-move-{index}")),
                time_us: now_us(),
                record: json!({"text":"root"}),
            })
            .unwrap();
        // Both rows exist before the moves, so only the UPDATE trigger can keep them right.
        storage.post_interaction_summary(uri, None).unwrap();
    }
    let mut like = record("did:plc:mover", "app.bsky.feed.like", "like", &from);
    let mut repost = record("did:plc:mover", "app.bsky.feed.repost", "repost", &from);
    let mut reply = record("did:plc:mover", "app.bsky.feed.post", "reply", &from);
    let mut quote = record("did:plc:mover", "app.bsky.feed.post", "quote", &from);
    for interaction in [&like, &repost, &reply, &quote] {
        storage.put_record(interaction).unwrap();
    }
    like.record = json!({"subject":{"uri":to}});
    repost.record = json!({"subject":{"uri":to}});
    reply.record = json!({"text":"r","reply":{"root":{"uri":to},"parent":{"uri":to}}});
    quote.record =
        json!({"text":"q","embed":{"$type":"app.bsky.embed.record","record":{"uri":to}}});
    for interaction in [&like, &repost, &reply, &quote] {
        storage.put_record(interaction).unwrap();
    }

    let connection = Connection::open(&path).unwrap();
    assert_eq!(cached_counts(&connection, &from), (0, 0, 0, 0));
    assert_eq!(cached_counts(&connection, &to), (1, 1, 1, 1));
    assert_eq!(
        cached_counts(&connection, &to),
        sql_counts(&connection, &to)
    );
    drop(connection);
    drop(storage);
    remove_database(&path);
}
