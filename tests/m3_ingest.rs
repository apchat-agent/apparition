use appview_rs::ingest::{
    IndexedRecord, Ingestor, JetstreamConsumer, MemoryStorage, ScopeMode, SqliteStorage, Storage,
};
use futures_util::{SinkExt, StreamExt};
use rusqlite::Connection;
use serde_json::json;
use std::{
    io::{self, Write},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{net::TcpListener, time::timeout};
use tokio_tungstenite::{accept_async, tungstenite::Message};

#[path = "support/synthetic_db.rs"]
mod synthetic_db;

fn event(
    did: &str,
    time_us: u64,
    operation: &str,
    collection: &str,
    rkey: &str,
    record: Option<serde_json::Value>,
) -> String {
    let mut commit = json!({
        "rev": format!("{time_us}"),
        "operation": operation,
        "collection": collection,
        "rkey": rkey,
        "cid": format!("cid-{time_us}")
    });
    if let Some(record) = record {
        commit["record"] = record;
    }
    json!({"did": did, "time_us": time_us, "kind": "commit", "commit": commit}).to_string()
}

#[test]
fn sqlite_memory_storage_does_not_create_a_disk_file() {
    let path = std::path::Path::new(":memory:");
    assert!(!path.exists());
    let storage = SqliteStorage::open(path).unwrap();
    assert!(!path.exists());
    drop(storage);
}

#[test]
fn sqlite_ingest_follow_scope_queries_do_not_scan_follows() {
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let path = root.join(format!(
        "target/av15b-query-plan-{}.sqlite3",
        std::process::id()
    ));
    let _ = std::fs::remove_file(&path);
    drop(SqliteStorage::open(&path).unwrap());
    let connection = Connection::open(&path).unwrap();
    let queries = [
        "SELECT f.actor FROM follows f JOIN registered_users r ON r.did=f.actor
         WHERE f.subject=?1 AND f.following=1",
        "SELECT EXISTS(SELECT 1 FROM follows f JOIN registered_users r ON r.did=f.actor
                       WHERE f.subject=?1 AND f.following=1)",
    ];

    for query in queries {
        let mut statement = connection
            .prepare(&format!("EXPLAIN QUERY PLAN {query}"))
            .unwrap();
        let plan = statement
            .query_map(["did:plc:benchmark"], |row| row.get::<_, String>(3))
            .unwrap()
            .collect::<Result<Vec<_>, _>>()
            .unwrap();
        assert!(
            !plan.iter().any(|line| line.contains("SCAN f")),
            "follow scope query must not scan the follows table; query={query}; plan={plan:?}"
        );
        assert!(
            plan.iter().any(|line| line.contains("SEARCH f")),
            "follow scope query should search an index; query={query}; plan={plan:?}"
        );
    }
}

#[test]
fn ingest_batches_log_parse_scope_record_apply_and_commit_phases_at_debug() {
    let output = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_max_level(tracing::Level::DEBUG)
        .with_writer(CapturedMakeWriter(output.clone()))
        .finish();
    let dispatch = tracing::Dispatch::new(subscriber);
    let _guard = tracing::dispatcher::set_default(&dispatch);
    let ingestor = Ingestor::new(Arc::new(MemoryStorage::default()), ScopeMode::Full);
    ingestor
        .process_json_batch(&[event(
            "did:plc:phase-test",
            1,
            "create",
            "app.bsky.feed.post",
            "one",
            Some(post("phase timing")),
        )])
        .unwrap();

    let logs = String::from_utf8(output.lock().unwrap().clone()).unwrap();
    assert!(logs.contains("Jetstream batch phase timing"), "{logs}");
    for phase in [
        "parse_ms",
        "scope_lookup_ms",
        "record_lookup_ms",
        "apply_jetstream_batch_ms",
        "commit_ms",
        "total_ms",
    ] {
        assert!(logs.contains(phase), "missing {phase} in {logs}");
    }
}

fn post(text: &str) -> serde_json::Value {
    json!({"$type":"app.bsky.feed.post", "text":text, "createdAt":"2026-10-27T00:00:00Z"})
}

#[test]
#[ignore = "large SQLite ingest benchmark; creates a multi-million-row database under target/"]
fn jetstream_large_database_batch_ingest_benchmark() {
    let root = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let path = root.join("target/av15b-synthetic.sqlite3");
    let generated = synthetic_db::ensure_database(&path).unwrap();
    let (records, follows, notifications) = synthetic_db::row_counts(&path).unwrap();
    println!(
        "synthetic database: generated={generated}, records={records}, follows={follows}, notifications={notifications}"
    );

    let startup_started = std::time::Instant::now();
    let storage = Arc::new(SqliteStorage::open(&path).unwrap());
    println!(
        "SqliteStorage::open_ms={:.3}",
        startup_started.elapsed().as_secs_f64() * 1000.0
    );
    let ingestor = Ingestor::new(storage, ScopeMode::Scoped);
    let events = (0..600)
        .map(|index| {
            if index % 100 == 99 {
                event(
                    "did:plc:bench-untracked-author",
                    index + 1,
                    "create",
                    "app.bsky.feed.like",
                    &format!("bench-like-{index}"),
                    Some(json!({"subject":{"uri":"at://did:plc:synthetic-tracked-000/app.bsky.feed.post/target-0","cid":"cid-target"}})),
                )
            } else {
                event(
                    "did:plc:bench-untracked-author",
                    index + 1,
                    "create",
                    "app.bsky.feed.post",
                    &format!("bench-post-{index}"),
                    Some(post("large database benchmark")),
                )
            }
        })
        .collect::<Vec<_>>();

    let benchmark_started = std::time::Instant::now();
    for batch in events.chunks(200) {
        let batch_started = std::time::Instant::now();
        ingestor.process_json_batch(batch).unwrap();
        println!(
            "batch_events={}, batch_ms={:.3}",
            batch.len(),
            batch_started.elapsed().as_secs_f64() * 1000.0
        );
    }
    let elapsed = benchmark_started.elapsed();
    println!(
        "large_database_ingest: events={}, elapsed_ms={:.3}, events_per_second={:.1}",
        events.len(),
        elapsed.as_secs_f64() * 1000.0,
        events.len() as f64 / elapsed.as_secs_f64()
    );
}

#[test]
fn scoped_ingest_indexes_registered_and_followed_posts_and_registered_post_interactions() {
    let storage = Arc::new(MemoryStorage::default());
    storage.register_user("did:plc:root").unwrap();
    storage
        .set_follow("did:plc:root", "did:plc:friend", true)
        .unwrap();
    let ingestor = Ingestor::new(storage.clone(), ScopeMode::Scoped);

    ingestor
        .process_json(&event(
            "did:plc:root",
            1,
            "create",
            "app.bsky.feed.post",
            "own",
            Some(post("own")),
        ))
        .unwrap();
    ingestor
        .process_json(&event(
            "did:plc:friend",
            2,
            "create",
            "app.bsky.feed.post",
            "friend",
            Some(post("friend")),
        ))
        .unwrap();
    ingestor
        .process_json(&event(
            "did:plc:stranger",
            3,
            "create",
            "app.bsky.feed.post",
            "stranger",
            Some(post("stranger")),
        ))
        .unwrap();
    let like = json!({"$type":"app.bsky.feed.like", "subject":{"uri":"at://did:plc:root/app.bsky.feed.post/own", "cid":"cid-1"}, "createdAt":"2026-10-27T00:00:00Z"});
    ingestor
        .process_json(&event(
            "did:plc:stranger",
            4,
            "create",
            "app.bsky.feed.like",
            "like",
            Some(like),
        ))
        .unwrap();

    assert!(
        storage
            .get_record("did:plc:root", "app.bsky.feed.post", "own")
            .unwrap()
            .is_some()
    );
    assert!(
        storage
            .get_record("did:plc:friend", "app.bsky.feed.post", "friend")
            .unwrap()
            .is_some()
    );
    assert!(
        storage
            .get_record("did:plc:stranger", "app.bsky.feed.post", "stranger")
            .unwrap()
            .is_none()
    );
    assert!(
        storage
            .get_record("did:plc:stranger", "app.bsky.feed.like", "like")
            .unwrap()
            .is_some()
    );
}

#[test]
fn scoped_batch_applies_new_follow_scope_to_later_events_in_the_same_batch() {
    let storage = Arc::new(MemoryStorage::default());
    storage.register_user("did:plc:root").unwrap();
    let ingestor = Ingestor::new(storage.clone(), ScopeMode::Scoped);
    let events = vec![
        event(
            "did:plc:root",
            10,
            "create",
            "app.bsky.graph.follow",
            "follow-friend",
            Some(json!({"$type":"app.bsky.graph.follow","subject":"did:plc:friend"})),
        ),
        event(
            "did:plc:friend",
            11,
            "create",
            "app.bsky.feed.post",
            "friend-post",
            Some(post("posted after follow")),
        ),
    ];

    ingestor.process_json_batch(&events).unwrap();

    assert!(
        storage
            .get_record("did:plc:friend", "app.bsky.feed.post", "friend-post")
            .unwrap()
            .is_some(),
        "events later in the committed batch must see earlier scope changes"
    );
}

#[test]
fn scoped_ingest_indexes_interactions_and_post_references_on_followed_accounts() {
    let storage = Arc::new(MemoryStorage::default());
    storage.register_user("did:plc:root").unwrap();
    storage
        .set_follow("did:plc:root", "did:plc:friend", true)
        .unwrap();
    let ingestor = Ingestor::new(storage.clone(), ScopeMode::Scoped);
    let root_uri = "at://did:plc:root/app.bsky.feed.post/root";
    let friend_uri = "at://did:plc:friend/app.bsky.feed.post/friend";

    for (did, rkey, text) in [
        ("did:plc:root", "root", "root post"),
        ("did:plc:friend", "friend", "followed post"),
    ] {
        ingestor
            .process_json(&event(
                did,
                1,
                "create",
                "app.bsky.feed.post",
                rkey,
                Some(post(text)),
            ))
            .unwrap();
    }

    let like = |subject: &str| json!({"subject":{"uri":subject,"cid":"cid-post"},"createdAt":"2026-10-27T00:00:00Z"});
    let repost = |subject: &str| json!({"subject":{"uri":subject,"cid":"cid-post"},"createdAt":"2026-10-27T00:00:00Z"});
    for (time, collection, rkey, record) in [
        (3, "app.bsky.feed.like", "friend-like", like(friend_uri)),
        (4, "app.bsky.feed.like", "root-like", like(root_uri)),
        (5, "app.bsky.feed.repost", "root-repost", repost(root_uri)),
    ] {
        ingestor
            .process_json(&event(
                "did:plc:stranger",
                time,
                "create",
                collection,
                rkey,
                Some(record),
            ))
            .unwrap();
    }

    let reply = json!({
        "text":"reply",
        "reply":{
            "root":{"uri":root_uri,"cid":"cid-root"},
            "parent":{"uri":friend_uri,"cid":"cid-friend"}
        }
    });
    ingestor
        .process_json(&event(
            "did:plc:stranger",
            6,
            "create",
            "app.bsky.feed.post",
            "reply",
            Some(reply),
        ))
        .unwrap();
    let direct_reply = json!({
        "text":"direct reply",
        "reply":{
            "root":{"uri":root_uri,"cid":"cid-root"},
            "parent":{"uri":root_uri,"cid":"cid-root"}
        }
    });
    ingestor
        .process_json(&event(
            "did:plc:stranger",
            11,
            "create",
            "app.bsky.feed.post",
            "root-direct-reply",
            Some(direct_reply),
        ))
        .unwrap();
    let root_only_reply = json!({
        "text":"nested reply",
        "reply":{
            "root":{"uri":root_uri,"cid":"cid-root"},
            "parent":{"uri":"at://did:plc:other/app.bsky.feed.post/parent","cid":"cid-other"}
        }
    });
    ingestor
        .process_json(&event(
            "did:plc:stranger",
            10,
            "create",
            "app.bsky.feed.post",
            "root-only-reply",
            Some(root_only_reply),
        ))
        .unwrap();
    let quote = json!({
        "text":"quote",
        "embed":{"$type":"app.bsky.embed.record","record":{"uri":friend_uri,"cid":"cid-friend"}}
    });
    ingestor
        .process_json(&event(
            "did:plc:stranger",
            7,
            "create",
            "app.bsky.feed.post",
            "quote",
            Some(quote),
        ))
        .unwrap();
    let mention = json!({
        "text":"hello",
        "facets":[{"features":[{"$type":"app.bsky.richtext.facet#mention","did":"did:plc:friend"}]}]
    });
    ingestor
        .process_json(&event(
            "did:plc:stranger",
            8,
            "create",
            "app.bsky.feed.post",
            "mention",
            Some(mention),
        ))
        .unwrap();
    let unrelated = json!({"text":"unrelated","reply":{"parent":{"uri":"at://did:plc:other/app.bsky.feed.post/p"}}});
    ingestor
        .process_json(&event(
            "did:plc:stranger",
            9,
            "create",
            "app.bsky.feed.post",
            "unrelated",
            Some(unrelated),
        ))
        .unwrap();

    for (collection, rkey) in [
        ("app.bsky.feed.like", "friend-like"),
        ("app.bsky.feed.like", "root-like"),
        ("app.bsky.feed.repost", "root-repost"),
        ("app.bsky.feed.post", "reply"),
        ("app.bsky.feed.post", "root-direct-reply"),
        ("app.bsky.feed.post", "root-only-reply"),
        ("app.bsky.feed.post", "quote"),
        ("app.bsky.feed.post", "mention"),
    ] {
        assert!(
            storage
                .get_record("did:plc:stranger", collection, rkey)
                .unwrap()
                .is_some(),
            "expected in-scope {collection}/{rkey}"
        );
    }
    assert!(
        storage
            .get_record("did:plc:stranger", "app.bsky.feed.post", "unrelated")
            .unwrap()
            .is_none()
    );
    let root_summary = storage.post_interaction_summary(root_uri, None).unwrap();
    assert_eq!(root_summary.like_count, 1);
    assert_eq!(root_summary.repost_count, 1);
    assert_eq!(root_summary.reply_count, 1);

    for (collection, rkey, time) in [
        ("app.bsky.feed.like", "root-like", 10),
        ("app.bsky.feed.repost", "root-repost", 11),
    ] {
        ingestor
            .process_json(&event(
                "did:plc:stranger",
                time,
                "delete",
                collection,
                rkey,
                None,
            ))
            .unwrap();
    }
    let root_summary = storage.post_interaction_summary(root_uri, None).unwrap();
    assert_eq!(root_summary.like_count, 0);
    assert_eq!(root_summary.repost_count, 0);
}

#[test]
fn deletes_remove_records_and_advance_the_jetstream_cursor() {
    let storage = Arc::new(MemoryStorage::default());
    storage.register_user("did:plc:root").unwrap();
    let ingestor = Ingestor::new(storage.clone(), ScopeMode::Scoped);

    ingestor
        .process_json(&event(
            "did:plc:root",
            41,
            "create",
            "app.bsky.feed.post",
            "p1",
            Some(post("hello")),
        ))
        .unwrap();
    ingestor
        .process_json(&event(
            "did:plc:root",
            42,
            "delete",
            "app.bsky.feed.post",
            "p1",
            None,
        ))
        .unwrap();

    assert!(
        storage
            .get_record("did:plc:root", "app.bsky.feed.post", "p1")
            .unwrap()
            .is_none()
    );
    assert_eq!(storage.cursor().unwrap(), Some(42));
}

#[tokio::test]
async fn websocket_consumer_processes_events_and_persists_cursor_to_sqlite() {
    let db_path = std::env::temp_dir().join(format!("appview-rs-m3-{}.db", std::process::id()));
    let _ = std::fs::remove_file(&db_path);
    let storage = Arc::new(SqliteStorage::open(&db_path).unwrap());
    storage.register_user("did:plc:root").unwrap();
    let ingestor = Ingestor::new(storage.clone(), ScopeMode::Scoped);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut websocket = accept_async(stream).await.unwrap();
        websocket
            .send(Message::Text(
                event(
                    "did:plc:root",
                    1234,
                    "create",
                    "app.bsky.feed.post",
                    "p1",
                    Some(post("from jetstream")),
                )
                .into(),
            ))
            .await
            .unwrap();
        websocket.close(None).await.unwrap();
    });

    let consumer = JetstreamConsumer::new(ingestor, Duration::from_millis(1));
    timeout(
        Duration::from_secs(3),
        consumer.consume_connection(&format!("ws://{address}/subscribe")),
    )
    .await
    .unwrap()
    .unwrap();
    server.await.unwrap();
    assert_eq!(storage.cursor().unwrap(), Some(1234));
    assert!(
        storage
            .get_record("did:plc:root", "app.bsky.feed.post", "p1")
            .unwrap()
            .is_some()
    );
    drop(storage);

    let reopened = SqliteStorage::open(&db_path).unwrap();
    assert_eq!(reopened.cursor().unwrap(), Some(1234));
    assert!(
        reopened
            .get_record("did:plc:root", "app.bsky.feed.post", "p1")
            .unwrap()
            .is_some()
    );
    drop(reopened);
    let _ = std::fs::remove_file(db_path);
}

#[tokio::test]
async fn websocket_event_batch_rolls_back_records_and_cursor_on_failed_commit() {
    let db_path = std::env::temp_dir().join(format!(
        "appview-rs-m3-batch-red-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let storage = Arc::new(SqliteStorage::open(&db_path).unwrap());
    Connection::open(&db_path)
        .unwrap()
        .execute_batch(
            "CREATE TRIGGER fail_batch_event BEFORE INSERT ON records
             WHEN NEW.rkey='fail'
             BEGIN SELECT RAISE(ABORT, 'injected batch failure'); END;",
        )
        .unwrap();

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut websocket = accept_async(stream).await.unwrap();
        websocket
            .send(Message::Text(
                event(
                    "did:plc:batch",
                    100,
                    "create",
                    "app.bsky.feed.post",
                    "first",
                    Some(post("first")),
                )
                .into(),
            ))
            .await
            .unwrap();
        websocket
            .send(Message::Text(
                event(
                    "did:plc:batch",
                    200,
                    "create",
                    "app.bsky.feed.post",
                    "fail",
                    Some(post("fail")),
                )
                .into(),
            ))
            .await
            .unwrap();
        websocket.close(None).await.unwrap();
    });

    let consumer = JetstreamConsumer::new(
        Ingestor::new(storage.clone(), ScopeMode::Full),
        Duration::from_millis(1),
    );
    let result = timeout(
        Duration::from_secs(3),
        consumer.consume_connection(&format!("ws://{address}/subscribe")),
    )
    .await
    .expect("consumer should flush the final batch on close");
    assert!(
        result.is_err(),
        "injected storage failure should fail the batch"
    );
    assert_eq!(storage.cursor().unwrap(), None);
    assert!(
        storage
            .get_record("did:plc:batch", "app.bsky.feed.post", "first")
            .unwrap()
            .is_none()
    );

    server.await.unwrap();
    drop(storage);
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", db_path.display()));
    }
}

#[test]
#[ignore = "manual local SQLite throughput benchmark"]
fn jetstream_sqlite_batch_throughput_benchmark() {
    let event_count = 5_000_u64;
    let old_path = std::env::temp_dir().join(format!(
        "appview-rs-av15-before-{}-{}.db",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let batched_path = old_path.with_extension("batched.db");
    let events = (1..=event_count)
        .map(|time_us| {
            event(
                "did:plc:benchmark",
                time_us,
                "create",
                "app.bsky.feed.post",
                &format!("r{time_us}"),
                Some(post("benchmark")),
            )
        })
        .collect::<Vec<_>>();

    let old_storage = Arc::new(SqliteStorage::open(&old_path).unwrap());
    let old_started = std::time::Instant::now();
    for time_us in 1..=event_count {
        let rkey = format!("r{time_us}");
        let record = IndexedRecord {
            did: "did:plc:benchmark".to_owned(),
            collection: "app.bsky.feed.post".to_owned(),
            rkey: rkey.clone(),
            uri: format!("at://did:plc:benchmark/app.bsky.feed.post/{rkey}"),
            cid: Some(format!("cid-{time_us}")),
            time_us,
            record: post("benchmark"),
        };
        old_storage.put_record(&record).unwrap();
        old_storage.set_cursor(time_us).unwrap();
    }
    let old_elapsed = old_started.elapsed();
    drop(old_storage);

    let batched_storage = Arc::new(SqliteStorage::open(&batched_path).unwrap());
    let batched_ingestor = Ingestor::new(batched_storage.clone(), ScopeMode::Full);
    let batched_started = std::time::Instant::now();
    for batch in events.chunks(200) {
        batched_ingestor.process_json_batch(batch).unwrap();
    }
    let batched_elapsed = batched_started.elapsed();
    assert_eq!(
        batched_storage.records().unwrap().len(),
        event_count as usize
    );
    assert_eq!(batched_storage.cursor().unwrap(), Some(event_count));

    let old_rate = event_count as f64 / old_elapsed.as_secs_f64();
    let batched_rate = event_count as f64 / batched_elapsed.as_secs_f64();
    println!(
        "SQLite Jetstream throughput: before={old_rate:.0} events/s ({:?}); batches_of_200={batched_rate:.0} events/s ({:?}); speedup={:.2}x",
        old_elapsed,
        batched_elapsed,
        batched_rate / old_rate,
    );

    drop(batched_ingestor);
    drop(batched_storage);
    for path in [old_path, batched_path] {
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
        }
    }
}

#[tokio::test]
async fn consumer_reconnects_after_a_silent_websocket_and_rotates_urls() {
    let storage = Arc::new(MemoryStorage::default());
    let ingestor = Ingestor::new(storage.clone(), ScopeMode::Full);

    let silent_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let silent_address = silent_listener.local_addr().unwrap();
    let silent_server = tokio::spawn(async move {
        let (stream, _) = silent_listener.accept().await.unwrap();
        let mut websocket = accept_async(stream).await.unwrap();
        loop {
            tokio::select! {
                _ = tokio::time::sleep(Duration::from_millis(5)) => {
                    if websocket.send(Message::Ping(Default::default())).await.is_err() {
                        break;
                    }
                }
                message = websocket.next() => {
                    if message.is_none() || matches!(message, Some(Ok(Message::Close(_)))) {
                        break;
                    }
                }
            }
        }
    });

    let live_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let live_address = live_listener.local_addr().unwrap();
    let live_cursor = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_micros() as u64;
    let live_server = tokio::spawn(async move {
        let (stream, _) = live_listener.accept().await.unwrap();
        let mut websocket = accept_async(stream).await.unwrap();
        websocket
            .send(Message::Text(
                event(
                    "did:plc:root",
                    live_cursor,
                    "create",
                    "app.bsky.feed.post",
                    "recovered",
                    Some(post("after reconnect")),
                )
                .into(),
            ))
            .await
            .unwrap();
        websocket.close(None).await.unwrap();
    });

    let consumer = JetstreamConsumer::new(ingestor, Duration::from_millis(1))
        .with_idle_timeout(Duration::from_millis(25));
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let run_task = tokio::spawn(async move {
        consumer
            .run(
                &[
                    format!("ws://{silent_address}/subscribe"),
                    format!("ws://{live_address}/subscribe"),
                ],
                shutdown_rx,
            )
            .await
    });

    timeout(Duration::from_secs(2), async {
        while storage.cursor().unwrap() != Some(live_cursor) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("consumer should reconnect to the second URL and ingest its event");
    shutdown_tx.send(true).unwrap();
    timeout(Duration::from_secs(2), run_task)
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    silent_server.await.unwrap();
    live_server.await.unwrap();
    assert!(
        storage
            .get_record("did:plc:root", "app.bsky.feed.post", "recovered")
            .unwrap()
            .is_some()
    );
}

#[tokio::test]
async fn event_processing_timeout_releases_connection_and_logs_event_identity() {
    let db_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("target")
        .join(format!("av9-event-timeout-{}.db", std::process::id()));
    let _ = std::fs::remove_file(&db_path);
    let storage = Arc::new(SqliteStorage::open(&db_path).unwrap());
    let writer_connection = Connection::open(&db_path).unwrap();
    writer_connection.execute_batch("BEGIN IMMEDIATE").unwrap();
    let ingestor = Ingestor::new(storage.clone(), ScopeMode::Full);

    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut websocket = accept_async(stream).await.unwrap();
        websocket
            .send(Message::Text(
                event(
                    "did:plc:slow",
                    4321,
                    "create",
                    "app.bsky.feed.post",
                    "blocked-write",
                    Some(post("blocked")),
                )
                .into(),
            ))
            .await
            .unwrap();
        let _ = websocket.next().await;
    });

    let output = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_max_level(tracing::Level::WARN)
        .with_writer(CapturedMakeWriter(output.clone()))
        .finish();
    let dispatch = tracing::Dispatch::new(subscriber);
    let _guard = tracing::dispatcher::set_default(&dispatch);

    let consumer = JetstreamConsumer::new(ingestor, Duration::from_millis(1))
        .with_event_timeout(Duration::from_millis(25));
    let result = timeout(
        Duration::from_secs(1),
        consumer.consume_connection(&format!("ws://{address}/subscribe")),
    )
    .await
    .expect("event processing should be bounded")
    .unwrap_err();
    assert!(result.to_string().contains("event processing timed out"));
    let logs = String::from_utf8(output.lock().unwrap().clone()).unwrap();
    assert!(logs.contains("did:plc:slow"), "{logs}");
    assert!(logs.contains("app.bsky.feed.post"), "{logs}");
    assert!(logs.contains("blocked-write"), "{logs}");

    writer_connection.execute_batch("COMMIT").unwrap();
    server.await.unwrap();
    timeout(Duration::from_secs(1), async {
        while storage.cursor().unwrap() != Some(4321) {
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .expect("timed out blocking worker should finish after SQLite unlocks");
    drop(storage);
    drop(writer_connection);
    for suffix in ["", "-wal", "-shm"] {
        let _ = std::fs::remove_file(format!("{}{suffix}", db_path.display()));
    }
}

#[tokio::test(flavor = "current_thread")]
async fn metrics_are_emitted_during_idle_periods_with_zero_events_and_cursor_lag() {
    let storage = Arc::new(MemoryStorage::default());
    let ingestor = Ingestor::new(storage, ScopeMode::Full);
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut websocket = accept_async(stream).await.unwrap();
        let _ = websocket.next().await;
    });

    let output = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_max_level(tracing::Level::INFO)
        .with_writer(CapturedMakeWriter(output.clone()))
        .finish();
    let dispatch = tracing::Dispatch::new(subscriber);
    let _guard = tracing::dispatcher::set_default(&dispatch);

    let consumer = JetstreamConsumer::new(ingestor, Duration::from_millis(1))
        .with_idle_timeout(Duration::from_millis(200))
        .with_metrics_interval(Duration::from_millis(10));
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let urls = vec![format!("ws://{address}/subscribe")];
    let run = consumer.run(&urls, shutdown_rx);
    tokio::pin!(run);
    tokio::select! {
        result = &mut run => panic!("consumer stopped before shutdown: {result:?}"),
        _ = tokio::time::sleep(Duration::from_millis(35)) => {},
    }
    shutdown_tx.send(true).unwrap();
    run.await.unwrap();
    server.await.unwrap();

    let logs = String::from_utf8(output.lock().unwrap().clone()).unwrap();
    assert!(logs.contains("Jetstream ingestion metrics"), "{logs}");
    assert!(logs.contains("events=0"), "{logs}");
    for field in [
        "batches=0",
        "avg_batch_ms=",
        "max_batch_ms=",
        "slowest_phase=",
    ] {
        assert!(logs.contains(field), "missing {field} in {logs}");
    }
    assert!(logs.contains("cursor_lag_seconds="), "{logs}");
    assert!(logs.contains("Jetstream disconnected"), "{logs}");
}

#[tokio::test]
async fn websocket_connection_lifecycle_is_logged_at_info() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut websocket = accept_async(stream).await.unwrap();
        websocket.close(None).await.unwrap();
    });

    let output = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_max_level(tracing::Level::INFO)
        .with_writer(CapturedMakeWriter(output.clone()))
        .finish();
    let dispatch = tracing::Dispatch::new(subscriber);
    let _guard = tracing::dispatcher::set_default(&dispatch);

    let consumer = JetstreamConsumer::new(
        Ingestor::new(Arc::new(MemoryStorage::default()), ScopeMode::Full),
        Duration::from_millis(1),
    );
    let secret_endpoint = format!("ws://{address}/subscribe?access_token=do-not-log");
    consumer.consume_connection(&secret_endpoint).await.unwrap();
    server.await.unwrap();

    let logs = String::from_utf8(output.lock().unwrap().clone()).unwrap();
    assert!(logs.contains("Jetstream connected"), "{logs}");
    assert!(logs.contains("Jetstream disconnected"), "{logs}");
    assert!(!logs.contains("do-not-log"), "{logs}");
}

struct CapturedMakeWriter(Arc<Mutex<Vec<u8>>>);

struct CapturedWriter(Arc<Mutex<Vec<u8>>>);

impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CapturedMakeWriter {
    type Writer = CapturedWriter;

    fn make_writer(&'a self) -> Self::Writer {
        CapturedWriter(self.0.clone())
    }
}

impl Write for CapturedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

#[test]
fn full_scope_accepts_supported_collections_and_updates_existing_records() {
    let storage = Arc::new(MemoryStorage::default());
    let ingestor = Ingestor::new(storage.clone(), ScopeMode::Full);
    for (index, (collection, did, value)) in [
        ("app.bsky.feed.post", "did:plc:a", post("post")),
        (
            "app.bsky.feed.like",
            "did:plc:b",
            json!({"subject":{"uri":"at://did:plc:a/app.bsky.feed.post/p1"}}),
        ),
        (
            "app.bsky.feed.repost",
            "did:plc:b",
            json!({"subject":{"uri":"at://did:plc:a/app.bsky.feed.post/p1"}}),
        ),
        (
            "app.bsky.graph.follow",
            "did:plc:b",
            json!({"subject":"did:plc:a"}),
        ),
        (
            "app.bsky.graph.block",
            "did:plc:b",
            json!({"subject":"did:plc:a"}),
        ),
        (
            "app.bsky.actor.profile",
            "did:plc:a",
            json!({"displayName":"A"}),
        ),
    ]
    .into_iter()
    .enumerate()
    {
        ingestor
            .process_json(&event(
                did,
                index as u64 + 1,
                "create",
                collection,
                "r1",
                Some(value),
            ))
            .unwrap();
        assert!(
            storage.get_record(did, collection, "r1").unwrap().is_some(),
            "collection {collection}"
        );
    }
}

#[test]
fn scope_mode_defaults_to_scoped_and_can_be_configured() {
    use appview_rs::config::Config;
    assert_eq!(
        Config::from_toml_and_env("", std::iter::empty::<(&str, &str)>())
            .unwrap()
            .scope_mode,
        ScopeMode::Scoped
    );
    assert_eq!(
        Config::from_toml_and_env("scope_mode = 'full'", std::iter::empty::<(&str, &str)>())
            .unwrap()
            .scope_mode,
        ScopeMode::Full
    );
    assert_eq!(
        Config::from_toml_and_env("", [("APPVIEW_SCOPE", "full")])
            .unwrap()
            .scope_mode,
        ScopeMode::Full
    );
}

#[test]
fn jetstream_urls_are_configurable_from_environment() {
    use appview_rs::config::Config;
    let config = Config::from_toml_and_env(
        "",
        [(
            "APPVIEW_JETSTREAM_URLS",
            "ws://one.test/subscribe,ws://two.test/subscribe",
        )],
    )
    .unwrap();
    assert_eq!(
        config.jetstream_urls,
        ["ws://one.test/subscribe", "ws://two.test/subscribe"]
    );
}

#[test]
fn jetstream_timeouts_default_to_sixty_seconds_and_are_configurable() {
    use appview_rs::config::Config;

    let defaults = Config::from_toml_and_env("", std::iter::empty::<(&str, &str)>()).unwrap();
    assert_eq!(defaults.jetstream_idle_timeout_secs, 60);
    assert_eq!(defaults.jetstream_event_timeout_secs, 60);

    let from_toml = Config::from_toml_and_env(
        "jetstream_idle_timeout_secs = 7\njetstream_event_timeout_secs = 9",
        std::iter::empty::<(&str, &str)>(),
    )
    .unwrap();
    assert_eq!(from_toml.jetstream_idle_timeout_secs, 7);
    assert_eq!(from_toml.jetstream_event_timeout_secs, 9);

    let from_env = Config::from_toml_and_env(
        "",
        [
            ("APPVIEW_JETSTREAM_IDLE_TIMEOUT_SECS", "12"),
            ("APPVIEW_JETSTREAM_EVENT_TIMEOUT_SECS", "4"),
        ],
    )
    .unwrap();
    assert_eq!(from_env.jetstream_idle_timeout_secs, 12);
    assert_eq!(from_env.jetstream_event_timeout_secs, 4);
}
