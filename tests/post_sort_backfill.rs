use appview_rs::{
    HealthState, health_with_storage_and_status,
    ingest::{
        BatchApplyTiming, IndexedNotification, IndexedRecord, JetstreamStatus,
        JetstreamStorageUpdate, NotificationCursor, NotificationEventRow, NotificationGroupSkip,
        NotificationKeyWindow, NotificationSubscription, PostInteractionSummary, ProfileIndexEntry,
        RecordCursor, SkippedCursorWindow, SortTimeCursor, SqliteStorage, Storage, StorageError,
        post_sort_backfill,
    },
};
use axum::{body::Body, http::Request};
use http_body_util::BodyExt;
use rusqlite::{Connection, params};
use std::{
    collections::HashMap,
    path::{Path, PathBuf},
    sync::{
        Arc,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    time::Duration,
};
use tower::ServiceExt;

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
        remaining, 2,
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
        remaining, 2,
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

struct FlakyBackfillStorage {
    inner: SqliteStorage,
    lock_failures: AtomicUsize,
    failures: AtomicU64,
}

impl FlakyBackfillStorage {
    fn lock_failure(error: &str) -> StorageError {
        StorageError::Sqlite(rusqlite::Error::SqliteFailure(
            rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_BUSY),
            Some(error.to_owned()),
        ))
    }
}

impl Storage for FlakyBackfillStorage {
    fn put_record(&self, record: &IndexedRecord) -> Result<(), StorageError> {
        self.inner.put_record(record)
    }

    fn delete_record(&self, did: &str, collection: &str, rkey: &str) -> Result<(), StorageError> {
        self.inner.delete_record(did, collection, rkey)
    }

    fn apply_jetstream_batch(
        &self,
        updates: &[JetstreamStorageUpdate],
    ) -> Result<BatchApplyTiming, StorageError> {
        self.inner.apply_jetstream_batch(updates)
    }

    fn record_skipped_window(&self, from: u64, to: u64) -> Result<(), StorageError> {
        self.inner.record_skipped_window(from, to)
    }

    fn skipped_windows(&self) -> Result<Vec<SkippedCursorWindow>, StorageError> {
        self.inner.skipped_windows()
    }

    fn replace_repo_snapshot(
        &self,
        did: &str,
        records: &[IndexedRecord],
        following: &[String],
    ) -> Result<(), StorageError> {
        self.inner.replace_repo_snapshot(did, records, following)
    }

    fn get_record(
        &self,
        did: &str,
        collection: &str,
        rkey: &str,
    ) -> Result<Option<IndexedRecord>, StorageError> {
        self.inner.get_record(did, collection, rkey)
    }

    fn record_by_uri(
        &self,
        collection: &str,
        uri: &str,
    ) -> Result<Option<IndexedRecord>, StorageError> {
        self.inner.record_by_uri(collection, uri)
    }

    fn records_by_uri(
        &self,
        collection: &str,
        uris: &[String],
    ) -> Result<HashMap<String, IndexedRecord>, StorageError> {
        self.inner.records_by_uri(collection, uris)
    }

    fn replies_to(
        &self,
        parent_uri: &str,
        limit: usize,
    ) -> Result<Vec<IndexedRecord>, StorageError> {
        self.inner.replies_to(parent_uri, limit)
    }

    fn records_for_subject(
        &self,
        collection: &str,
        subject_uri: &str,
        limit: usize,
    ) -> Result<Vec<IndexedRecord>, StorageError> {
        self.inner
            .records_for_subject(collection, subject_uri, limit)
    }

    fn records_for_actor_collection(
        &self,
        did: &str,
        collection: &str,
        limit: usize,
    ) -> Result<Vec<IndexedRecord>, StorageError> {
        self.inner
            .records_for_actor_collection(did, collection, limit)
    }

    fn follows_for_actor(
        &self,
        did: &str,
        limit: usize,
    ) -> Result<Vec<IndexedRecord>, StorageError> {
        self.inner.follows_for_actor(did, limit)
    }

    fn followers_for_did(
        &self,
        did: &str,
        limit: usize,
    ) -> Result<Vec<IndexedRecord>, StorageError> {
        self.inner.followers_for_did(did, limit)
    }

    fn profile_counts(&self, did: &str) -> Result<(usize, usize, usize), StorageError> {
        self.inner.profile_counts(did)
    }

    fn remember_recent_profile(
        &self,
        viewer_did: &str,
        profile_did: &str,
    ) -> Result<(), StorageError> {
        self.inner.remember_recent_profile(viewer_did, profile_did)
    }

    fn recent_profiles_for_viewer(&self, viewer_did: &str) -> Result<Vec<String>, StorageError> {
        self.inner.recent_profiles_for_viewer(viewer_did)
    }

    fn clear_recent_profiles_for_viewer(&self, viewer_did: &str) -> Result<(), StorageError> {
        self.inner.clear_recent_profiles_for_viewer(viewer_did)
    }

    fn set_last_active_feed_tab(&self, viewer_did: &str, tab: &str) -> Result<(), StorageError> {
        self.inner.set_last_active_feed_tab(viewer_did, tab)
    }

    fn last_active_feed_tab(&self, viewer_did: &str) -> Result<Option<String>, StorageError> {
        self.inner.last_active_feed_tab(viewer_did)
    }

    fn profile_index_snapshot(&self) -> Result<HashMap<String, ProfileIndexEntry>, StorageError> {
        self.inner.profile_index_snapshot()
    }

    fn set_profile_index_entry(
        &self,
        did: &str,
        entry: ProfileIndexEntry,
    ) -> Result<(), StorageError> {
        self.inner.set_profile_index_entry(did, entry)
    }

    fn enqueue_profile_fetches(&self, dids: &[String]) -> Result<(), StorageError> {
        self.inner.enqueue_profile_fetches(dids)
    }

    fn pending_profile_fetches(&self, limit: usize) -> Result<Vec<String>, StorageError> {
        self.inner.pending_profile_fetches(limit)
    }

    fn complete_profile_fetch(&self, did: &str) -> Result<(), StorageError> {
        self.inner.complete_profile_fetch(did)
    }

    fn notifications_for(
        &self,
        did: &str,
        before: Option<&NotificationCursor>,
        limit: usize,
    ) -> Result<Vec<IndexedNotification>, StorageError> {
        self.inner.notifications_for(did, before, limit)
    }

    fn notification_events_window(
        &self,
        did: &str,
        before: Option<&NotificationCursor>,
        skip: &[NotificationGroupSkip],
        limit: usize,
    ) -> Result<Vec<NotificationEventRow>, StorageError> {
        self.inner
            .notification_events_window(did, before, skip, limit)
    }

    fn notification_key_event_count(
        &self,
        did: &str,
        reason: &str,
        reason_subject: Option<&str>,
        from_us: u64,
        to_us: u64,
    ) -> Result<usize, StorageError> {
        self.inner
            .notification_key_event_count(did, reason, reason_subject, from_us, to_us)
    }

    fn notification_key_events(
        &self,
        did: &str,
        reason: &str,
        reason_subject: Option<&str>,
        before: Option<&NotificationCursor>,
        window: &NotificationKeyWindow,
        limit: usize,
    ) -> Result<Vec<NotificationEventRow>, StorageError> {
        self.inner
            .notification_key_events(did, reason, reason_subject, before, window, limit)
    }

    fn notification_count(&self, did: &str) -> Result<usize, StorageError> {
        self.inner.notification_count(did)
    }

    fn notification_last_seen(&self, did: &str) -> Result<u64, StorageError> {
        self.inner.notification_last_seen(did)
    }

    fn mark_notifications_seen(&self, did: &str, time_us: u64) -> Result<(), StorageError> {
        self.inner.mark_notifications_seen(did, time_us)
    }

    fn unread_notification_count(&self, did: &str) -> Result<usize, StorageError> {
        self.inner.unread_notification_count(did)
    }

    fn subscribe_notification_counts(&self, did: &str) -> NotificationSubscription {
        self.inner.subscribe_notification_counts(did)
    }

    fn author_posts(
        &self,
        did: &str,
        cursor: Option<&RecordCursor>,
        limit: usize,
        include_replies: bool,
    ) -> Result<Vec<IndexedRecord>, StorageError> {
        self.inner.author_posts(did, cursor, limit, include_replies)
    }

    fn timeline_posts(
        &self,
        actor: &str,
        cursor: Option<&RecordCursor>,
        limit: usize,
    ) -> Result<Vec<IndexedRecord>, StorageError> {
        self.inner.timeline_posts(actor, cursor, limit)
    }

    fn timeline_posts_by_post_time(
        &self,
        actor: &str,
        cursor: Option<&SortTimeCursor>,
        limit: usize,
    ) -> Result<Vec<IndexedRecord>, StorageError> {
        self.inner.timeline_posts_by_post_time(actor, cursor, limit)
    }

    fn post_sort_backfill_remaining(&self) -> Result<u64, StorageError> {
        self.inner.post_sort_backfill_remaining()
    }

    fn backfill_post_sort_batch(&self, limit: usize) -> Result<u64, StorageError> {
        if self.lock_failures.load(Ordering::SeqCst) > 0 {
            self.lock_failures.fetch_sub(1, Ordering::SeqCst);
            return Err(Self::lock_failure("database is locked"));
        }
        self.inner.backfill_post_sort_batch(limit)
    }

    fn post_sort_backfill_failures(&self) -> u64 {
        self.failures.load(Ordering::Relaxed)
    }

    fn record_post_sort_backfill_failure(&self) {
        self.failures.fetch_add(1, Ordering::Relaxed);
    }

    fn sort_us_for_record(&self, record: &IndexedRecord) -> Result<u64, StorageError> {
        self.inner.sort_us_for_record(record)
    }

    fn post_interaction_summary(
        &self,
        uri: &str,
        viewer_did: Option<&str>,
    ) -> Result<PostInteractionSummary, StorageError> {
        self.inner.post_interaction_summary(uri, viewer_did)
    }

    fn records(&self) -> Result<Vec<IndexedRecord>, StorageError> {
        self.inner.records()
    }

    fn cursor(&self) -> Result<Option<u64>, StorageError> {
        self.inner.cursor()
    }

    fn set_cursor(&self, cursor: u64) -> Result<(), StorageError> {
        self.inner.set_cursor(cursor)
    }

    fn register_user(&self, did: &str) -> Result<(), StorageError> {
        self.inner.register_user(did)
    }

    fn unregister_user(&self, did: &str) -> Result<(), StorageError> {
        self.inner.unregister_user(did)
    }

    fn is_registered(&self, did: &str) -> Result<bool, StorageError> {
        self.inner.is_registered(did)
    }

    fn rotate_web_session(&self, did: &str, session_id: &str) -> Result<u64, StorageError> {
        self.inner.rotate_web_session(did, session_id)
    }

    fn web_session_generation(
        &self,
        did: &str,
        session_id: &str,
    ) -> Result<Option<u64>, StorageError> {
        self.inner.web_session_generation(did, session_id)
    }

    fn delete_web_session(&self, did: &str, session_id: &str) -> Result<(), StorageError> {
        self.inner.delete_web_session(did, session_id)
    }

    fn set_follow(&self, actor: &str, subject: &str, following: bool) -> Result<(), StorageError> {
        self.inner.set_follow(actor, subject, following)
    }

    fn is_following(&self, actor: &str, subject: &str) -> Result<bool, StorageError> {
        self.inner.is_following(actor, subject)
    }

    fn has_follow_record(&self, actor: &str, subject: &str) -> Result<bool, StorageError> {
        self.inner.has_follow_record(actor, subject)
    }

    fn tracked_follow_count(&self, did: &str) -> Result<u32, StorageError> {
        self.inner.tracked_follow_count(did)
    }

    fn is_followed_by_registered(&self, did: &str) -> Result<bool, StorageError> {
        self.inner.is_followed_by_registered(did)
    }

    fn enqueue_backfill(&self, did: &str) -> Result<(), StorageError> {
        self.inner.enqueue_backfill(did)
    }

    fn enqueue_backfill_for(&self, owner_did: &str, did: &str) -> Result<(), StorageError> {
        self.inner.enqueue_backfill_for(owner_did, did)
    }

    fn pending_backfills(&self) -> Result<Vec<String>, StorageError> {
        self.inner.pending_backfills()
    }

    fn complete_backfill(&self, did: &str) -> Result<(), StorageError> {
        self.inner.complete_backfill(did)
    }

    fn record_backfill_failure(&self, did: &str) -> Result<Duration, StorageError> {
        self.inner.record_backfill_failure(did)
    }

    fn backfill_progress(&self, owner_did: &str) -> Result<(usize, usize), StorageError> {
        self.inner.backfill_progress(owner_did)
    }

    fn backfill_failure_count(&self, owner_did: &str) -> Result<usize, StorageError> {
        self.inner.backfill_failure_count(owner_did)
    }

    fn set_verified_handle(&self, did: &str, handle: &str) -> Result<(), StorageError> {
        self.inner.set_verified_handle(did, handle)
    }

    fn clear_verified_handle(&self, did: &str) -> Result<(), StorageError> {
        self.inner.clear_verified_handle(did)
    }

    fn verified_handle(&self, did: &str) -> Result<Option<String>, StorageError> {
        self.inner.verified_handle(did)
    }

    fn handle_is_invalid(&self, did: &str) -> Result<bool, StorageError> {
        self.inner.handle_is_invalid(did)
    }

    fn did_by_verified_handle(&self, handle: &str) -> Result<Option<String>, StorageError> {
        self.inner.did_by_verified_handle(handle)
    }

    fn is_tracked_did(&self, did: &str) -> Result<bool, StorageError> {
        self.inner.is_tracked_did(did)
    }

    fn registered_followers(&self, did: &str) -> Result<Vec<String>, StorageError> {
        self.inner.registered_followers(did)
    }

    fn is_displayed_did(&self, did: &str) -> Result<bool, StorageError> {
        self.inner.is_displayed_did(did)
    }

    fn stale_handle_dids(&self, ttl: Duration, limit: usize) -> Result<Vec<String>, StorageError> {
        self.inner.stale_handle_dids(ttl, limit)
    }

    fn queue_handle_refresh(&self, did: &str) -> Result<(), StorageError> {
        self.inner.queue_handle_refresh(did)
    }

    fn queue_handle_refresh_ordered(&self, dids: &[String]) -> Result<(), StorageError> {
        self.inner.queue_handle_refresh_ordered(dids)
    }

    fn pending_handle_refreshes(&self, limit: usize) -> Result<Vec<String>, StorageError> {
        self.inner.pending_handle_refreshes(limit)
    }

    fn complete_handle_refresh(&self, did: &str) -> Result<(), StorageError> {
        self.inner.complete_handle_refresh(did)
    }

    fn bytes_on_disk(&self) -> u64 {
        self.inner.bytes_on_disk()
    }
}

#[tokio::test]
async fn post_sort_backfill_survives_transient_database_locks() {
    let path = database_path("locked");
    seed(&path);
    let storage = Arc::new(FlakyBackfillStorage {
        inner: SqliteStorage::open(&path).unwrap(),
        lock_failures: AtomicUsize::new(2),
        failures: AtomicU64::new(0),
    });
    let retry_base = Duration::from_millis(1);
    let result = post_sort_backfill(
        storage.as_ref(),
        2,
        Duration::from_millis(1),
        retry_base,
        Duration::from_millis(4),
    )
    .await
    .unwrap();
    let failures = result.failures;
    let batches = result.batches;
    let backoff = result.backoff;
    assert_eq!(
        failures, 2,
        "the two transient lock failures must be counted: {failures:?}"
    );
    assert_eq!(
        batches, 4,
        "after the locks, four batches must fill the eight remaining rows: {batches:?}"
    );
    assert_eq!(
        backoff, retry_base,
        "the backoff must reset after a successful batch: {backoff:?}"
    );
    assert_eq!(
        storage.post_sort_backfill_remaining().unwrap(),
        0,
        "the backfill must finish with no rows remaining"
    );
    let connection = Connection::open(&path).unwrap();
    let filled: i64 = connection
        .query_row("SELECT COUNT(*) FROM post_sort", [], |row| row.get(0))
        .unwrap();
    assert_eq!(
        filled, 12,
        "every seeded post must have a post_sort row: {filled:?}"
    );
    drop(connection);
    drop(storage);
    remove_database(&path);
}

#[tokio::test]
async fn post_sort_backfill_failure_counter_is_visible_in_health() {
    let path = database_path("health-failures");
    seed(&path);
    let storage = Arc::new(FlakyBackfillStorage {
        inner: SqliteStorage::open(&path).unwrap(),
        lock_failures: AtomicUsize::new(2),
        failures: AtomicU64::new(0),
    });
    let result = post_sort_backfill(
        storage.as_ref(),
        2,
        Duration::from_millis(1),
        Duration::from_millis(1),
        Duration::from_millis(4),
    )
    .await
    .unwrap();
    let failures = result.failures;
    assert_eq!(
        failures, 2,
        "the health counter must see the two failures: {failures:?}"
    );
    let status = Arc::new(JetstreamStatus::default());
    status.waiting_to_reconnect();
    status.record_event(1234);
    let state = HealthState {
        storage: storage.clone(),
        status,
    };
    let app = axum::Router::new()
        .route(
            "/healthz",
            axum::routing::get(health_with_storage_and_status),
        )
        .with_state(state);
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/healthz")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), 200);
    let body = response.into_body().collect().await.unwrap().to_bytes();
    assert_eq!(
        &body[..],
        br#"{"status":"ok","jetstream_state":"waiting to reconnect","supervisor_restarts":0,"last_event_us":1234,"post_sort_backfill_remaining":0,"post_sort_backfill_failures":2}"#,
        "health must expose the post-sort backfill failure count"
    );
    drop(storage);
    remove_database(&path);
}
