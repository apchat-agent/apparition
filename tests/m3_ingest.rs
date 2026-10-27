use appview_rs::ingest::{
    Ingestor, JetstreamConsumer, MemoryStorage, ScopeMode, SqliteStorage, Storage,
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

fn post(text: &str) -> serde_json::Value {
    json!({"$type":"app.bsky.feed.post", "text":text, "createdAt":"2026-10-27T00:00:00Z"})
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
async fn consumer_reconnects_after_a_silent_websocket_and_rotates_urls() {
    let storage = Arc::new(MemoryStorage::default());
    let ingestor = Ingestor::new(storage.clone(), ScopeMode::Full);

    let silent_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let silent_address = silent_listener.local_addr().unwrap();
    let silent_server = tokio::spawn(async move {
        let (stream, _) = silent_listener.accept().await.unwrap();
        let mut websocket = accept_async(stream).await.unwrap();
        let _ = websocket.next().await;
    });

    let live_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let live_address = live_listener.local_addr().unwrap();
    let live_server = tokio::spawn(async move {
        let (stream, _) = live_listener.accept().await.unwrap();
        let mut websocket = accept_async(stream).await.unwrap();
        websocket
            .send(Message::Text(
                event(
                    "did:plc:root",
                    1234,
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
        while storage.cursor().unwrap() != Some(1234) {
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
    let run_task = tokio::spawn(async move {
        consumer
            .run(&[format!("ws://{address}/subscribe")], shutdown_rx)
            .await
    });
    tokio::time::sleep(Duration::from_millis(35)).await;
    shutdown_tx.send(true).unwrap();
    run_task.await.unwrap().unwrap();
    server.await.unwrap();

    let logs = String::from_utf8(output.lock().unwrap().clone()).unwrap();
    assert!(logs.contains("Jetstream ingestion metrics"), "{logs}");
    assert!(logs.contains("events=0"), "{logs}");
    assert!(logs.contains("cursor_lag_seconds="), "{logs}");
    assert!(logs.contains("Jetstream connected"), "{logs}");
    assert!(logs.contains("Jetstream disconnected"), "{logs}");
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
