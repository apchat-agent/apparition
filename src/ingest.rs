use futures_util::{FutureExt, StreamExt};
use rusqlite::{Connection, OptionalExtension, params, params_from_iter};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    cell::{Cell, RefCell},
    cmp::Reverse,
    collections::{HashMap, HashSet, VecDeque},
    fs,
    io::Read,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
        mpsc::{self, Receiver, RecvTimeoutError, SyncSender, TryRecvError},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use thiserror::Error;
use tokio::sync::{broadcast, watch};
use tokio_tungstenite::{connect_async, tungstenite::Message};

const SUPPORTED_COLLECTIONS: &[&str] = &[
    "app.bsky.feed.post",
    "app.bsky.feed.like",
    "app.bsky.feed.repost",
    "app.bsky.graph.follow",
    "app.bsky.graph.block",
    "app.bsky.actor.profile",
];
const BACKFILL_RETRY_BASE: Duration = Duration::from_secs(5);
const BACKFILL_RETRY_MAX: Duration = Duration::from_secs(60 * 60);
const MAX_BACKFILL_QUEUE: usize = 1_000;
const BACKFILL_BATCH_SIZE: usize = 8;
const MAX_PROFILE_FETCH_QUEUE: usize = 10_000;
/// Bound on the deduplicated per-account "recent posts" queue (owner 2026-11-01:
/// an unfollowed profile lookup should load their recent posts in a worker, and
/// cache). Overflow is dropped and counted, never blocked.
pub const MAX_RECENT_POSTS_QUEUE: usize = 500;
const SQLITE_WRITER_QUEUE: usize = 256;
/// Coarse bound on post-count fills tracked as waiting for the writer. The
/// writer's own queue (256 slots, full means dropped) is the tighter bound;
/// this one keeps the dedup set itself small whatever the queue does. The fill
/// is a warm-cache optimization, so dropping one only means the next page
/// recomputes the counts on a read connection.
const MAX_PENDING_POST_COUNTS_FILLS: usize = 2_048;
pub const POST_SORT_REMAINING_UNKNOWN: u64 = u64::MAX;
pub const POST_SORT_BACKFILL_BATCH: usize = 2_000;
const SQLITE_WRITER_BATCH_SIZE: usize = 64;
const SQLITE_WRITER_BATCH_DELAY: Duration = Duration::from_millis(5);
const WAL_JOURNAL_SIZE_LIMIT_BYTES: u64 = 64 * 1024 * 1024;
const WAL_PRESSURE_CHECKPOINT_BYTES: u64 = WAL_JOURNAL_SIZE_LIMIT_BYTES * 3;
const CHECKPOINT_BUSY_TIMEOUT: Duration = Duration::from_millis(50);
const WAL_FORCE_SHRINK_DEFAULT_BYTES: u64 = 4 * 1024 * 1024 * 1024;
const WAL_FORCE_SHRINK_DEFAULT_INTERVAL: Duration = Duration::from_secs(60 * 60);
const WAL_FORCE_SHRINK_DEFAULT_TIMEOUT: Duration = Duration::from_secs(10);
const SQLITE_WRITER_METRICS_INTERVAL: Duration = Duration::from_secs(60);
const SQLITE_SLOW_WRITE_THRESHOLD: Duration = Duration::from_secs(1);
const CHECKPOINT_NONE: usize = 0;
const CHECKPOINT_PASSIVE: usize = 1;
const CHECKPOINT_TRUNCATE: usize = 2;
/// Hide a post while its own creation timestamp is more than 30 days ahead; the grace window
/// lets legitimately scheduled posts become visible shortly before their publication date.
pub const FUTURE_POST_HIDE_WINDOW_US: u64 = 30 * 24 * 60 * 60 * 1_000_000;
/// At most 500 candidates per list request keeps hidden future rows from turning one page into an unbounded scan.
pub const FILTERED_POST_PAGE_SCAN_LIMIT: usize = 500;
static FUTURE_POSTS_HIDDEN: AtomicU64 = AtomicU64::new(0);
static NEXT_MEMORY_DATABASE_ID: AtomicU64 = AtomicU64::new(1);

thread_local! {
    static SQLITE_OPERATION_KIND: Cell<Option<&'static str>> = const { Cell::new(None) };
}

/// Labels the SQLite writer operation submitted by the current blocking task.
/// The label crosses the actor queue with the operation so slow transactions
/// and SQLITE_BUSY logs retain the originating kind.
pub(crate) struct SqliteOperationKindGuard(Option<&'static str>);

impl SqliteOperationKindGuard {
    pub(crate) fn enter(kind: &'static str) -> Self {
        Self(SQLITE_OPERATION_KIND.with(|current| current.replace(Some(kind))))
    }
}

impl Drop for SqliteOperationKindGuard {
    fn drop(&mut self) {
        SQLITE_OPERATION_KIND.with(|current| current.set(self.0));
    }
}

fn current_sqlite_operation_kind(default: &'static str) -> &'static str {
    SQLITE_OPERATION_KIND.with(|current| current.get().unwrap_or(default))
}

/// Current wall time for page-level visibility decisions, in Unix microseconds.
pub fn visibility_now_us() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros()
        .min(u64::MAX as u128) as u64
}

/// Whether this post's own `createdAt` is more than the visibility grace window ahead of `now`.
/// Missing or malformed timestamps remain visible, and the supplied clock makes this policy
/// deterministic in tests.
pub fn post_is_too_far_in_future(record: &IndexedRecord, now_us: u64) -> bool {
    if record.collection != "app.bsky.feed.post" {
        return false;
    }
    let Some(created_at) = record.record.get("createdAt").and_then(Value::as_str) else {
        return false;
    };
    let Some(created_us) = crate::backfill::parse_rfc3339_micros(created_at) else {
        return false;
    };
    let hidden = created_us > now_us.saturating_add(FUTURE_POST_HIDE_WINDOW_US);
    if hidden {
        FUTURE_POSTS_HIDDEN.fetch_add(1, Ordering::Relaxed);
    }
    hidden
}

/// Read enough cursor-ordered candidates to return a visible page and one-item lookahead. The
/// fetch closure remains page-sized, while advancing its cursor across every examined row keeps
/// future-dated rows from repeating or displacing ordinary posts on later pages. A capped short
/// page carries the last examined cursor so the next request can continue the scan.
pub fn filtered_post_page<C, F, G>(
    cursor: Option<C>,
    limit: usize,
    now_us: u64,
    mut fetch: F,
    mut cursor_after: G,
) -> Result<(Vec<IndexedRecord>, bool, Option<C>), StorageError>
where
    C: Clone,
    F: FnMut(Option<&C>, usize) -> Result<Vec<IndexedRecord>, StorageError>,
    G: FnMut(&IndexedRecord) -> C,
{
    let limit = limit.clamp(1, 100);
    let mut scan_cursor = cursor;
    let mut visible = Vec::with_capacity(limit + 1);
    let mut rows_examined = 0usize;
    let mut scan_capped = false;
    loop {
        if rows_examined >= FILTERED_POST_PAGE_SCAN_LIMIT {
            scan_capped = true;
            break;
        }
        let batch_limit = (limit + 1)
            .min(101)
            .min(FILTERED_POST_PAGE_SCAN_LIMIT - rows_examined);
        let batch = fetch(scan_cursor.as_ref(), batch_limit)?;
        if batch.is_empty() {
            break;
        }
        let batch_len = batch.len();
        for record in batch {
            rows_examined += 1;
            scan_cursor = Some(cursor_after(&record));
            if !post_is_too_far_in_future(&record, now_us) {
                visible.push(record);
                if visible.len() > limit {
                    break;
                }
            }
        }
        if visible.len() > limit || batch_len < batch_limit {
            break;
        }
    }
    let has_more = visible.len() > limit || scan_capped;
    let next_cursor = if visible.len() > limit {
        visible.get(limit - 1).map(&mut cursor_after)
    } else if scan_capped {
        scan_cursor
    } else {
        None
    };
    Ok((visible, has_more, next_cursor))
}

fn take_future_posts_hidden() -> u64 {
    FUTURE_POSTS_HIDDEN.swap(0, Ordering::Relaxed)
}

struct EphemeralDatabase(PathBuf);

impl Drop for EphemeralDatabase {
    fn drop(&mut self) {
        let _ = fs::remove_file(&self.0);
        let _ = fs::remove_file(format!("{}-wal", self.0.display()));
        let _ = fs::remove_file(format!("{}-shm", self.0.display()));
    }
}
const JETSTREAM_BATCH_SIZE: usize = 200;
const JETSTREAM_BATCH_DELAY: Duration = Duration::from_millis(250);
const JETSTREAM_LOCK_RETRY_FIRST_BACKOFF: Duration = Duration::from_millis(50);
const JETSTREAM_LOCK_RETRY_MAX_BACKOFF: Duration = Duration::from_secs(1);

fn is_jetstream_sqlite_lock(error: &IngestError) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    message.contains("database is locked")
        || message.contains("database is busy")
        || message.contains("database table is locked")
}

fn backfill_retry_delay(failures: u32) -> Duration {
    let exponent = failures.saturating_sub(1).min(63);
    let multiplier = 1_u64.checked_shl(exponent).unwrap_or(u64::MAX);
    Duration::from_secs(
        BACKFILL_RETRY_BASE
            .as_secs()
            .saturating_mul(multiplier)
            .min(BACKFILL_RETRY_MAX.as_secs()),
    )
}

fn epoch_millis() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

fn epoch_micros() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros()
        .min(i64::MAX as u128) as i64
}

fn profile_time_ms(time_us: u64) -> i64 {
    i64::try_from(time_us / 1_000).unwrap_or(i64::MAX)
}

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum ScopeMode {
    #[default]
    Scoped,
    Full,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
pub struct IndexedRecord {
    pub did: String,
    pub collection: String,
    pub rkey: String,
    pub uri: String,
    pub cid: Option<String>,
    pub time_us: u64,
    pub record: Value,
}

#[derive(Clone, Debug, Default)]
pub struct JetstreamStorageUpdate {
    pub time_us: u64,
    pub upsert: Option<IndexedRecord>,
    pub delete: Option<(String, String, String)>,
    pub follows: Vec<FollowUpdate>,
    pub backfill_for: Vec<(String, String)>,
    pub handle_refresh: Option<String>,
}

#[derive(Clone, Debug)]
pub struct FollowUpdate {
    pub actor: String,
    pub subject: String,
    pub following: bool,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
pub struct SkippedCursorWindow {
    pub from: u64,
    pub to: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PostInteractionSummary {
    pub like_count: usize,
    pub repost_count: usize,
    pub reply_count: usize,
    pub quote_count: usize,
    pub viewer_like: Option<String>,
    pub viewer_repost: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ProfileIndexEntry {
    pub has_profile: bool,
    pub fetched_at_ms: i64,
}

/// What one profile visit's enqueue attempt did to the bounded recent-posts queue.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RecentPostsEnqueue {
    /// A job for this account was newly queued.
    Queued,
    /// A job for this account was already queued (the queue deduplicates).
    Duplicate,
    /// The queue was at its bound; the job was dropped and counted.
    Overflowed,
}

/// Per-account recent-posts fetch state: when posts were last stored (0 = never),
/// when a failed fetch may be retried (0 = no backoff), and whether a job is
/// currently queued or running for the account.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RecentPostsFetchStatus {
    pub fetched_at_ms: i64,
    pub next_retry_at_ms: i64,
    pub queued: bool,
}

/// Per-account state of the "older posts" walk through the account's own PDS
/// (profile scrolling, lane av75q 2026-11-01): the next PDS cursor to continue
/// from (`None` until the first older page ran), and whether the account's
/// whole post history is local (`exhausted`: the PDS stopped returning a
/// cursor, or a repository was backfilled with `backfill_max_age_days = 0`).
/// An exhausted account never triggers another older-posts fetch.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OlderPostsState {
    pub cursor: Option<String>,
    pub exhausted: bool,
}

/// Cumulative recent-posts counters a running service exposes: jobs queued,
/// fetches done, fetches failed, and jobs dropped at the queue bound. The
/// `SQLite writer minute metrics` line reports these counts.
#[derive(Default, Debug)]
pub struct RecentPostsCounters {
    pub queued: AtomicU64,
    pub done: AtomicU64,
    pub failed: AtomicU64,
    pub dropped: AtomicU64,
}

impl RecentPostsCounters {
    pub fn snapshot(&self) -> (u64, u64, u64, u64) {
        (
            self.queued.load(Ordering::Relaxed),
            self.done.load(Ordering::Relaxed),
            self.failed.load(Ordering::Relaxed),
            self.dropped.load(Ordering::Relaxed),
        )
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct IndexedNotification {
    pub record: IndexedRecord,
    pub reason: String,
    pub reason_subject: Option<String>,
}

/// One notification event as the grouping pass reads it: the `notification_events`
/// columns only, with no join to `records` and no `record_json`. Lane av72q reads
/// these newest-first in batches to build one page of groups without loading every
/// notification the viewer ever got.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NotificationEventRow {
    pub source_did: String,
    pub source_collection: String,
    pub source_rkey: String,
    pub source_uri: String,
    pub reason: String,
    pub reason_subject: Option<String>,
    pub time_us: u64,
}

impl NotificationEventRow {
    pub fn sorts_before(&self, cursor: &NotificationCursor) -> bool {
        self.time_us < cursor.time_us
            || (self.time_us == cursor.time_us
                && (self.source_uri < cursor.uri
                    || (self.source_uri == cursor.uri && self.reason < cursor.reason)))
    }

    pub fn cursor(&self) -> NotificationCursor {
        NotificationCursor {
            time_us: self.time_us,
            uri: self.source_uri.clone(),
            reason: self.reason.clone(),
        }
    }
}

/// A `reason` + `reason_subject` pair whose events at or after `floor_us` have
/// already been accounted for by an open (or resolved) notification group; the
/// windowed read skips them.
pub type NotificationGroupSkip = (String, Option<String>, u64);

/// Inclusive time bounds for a single key's event lookups.
#[derive(Debug, Clone, Copy)]
pub struct NotificationKeyWindow {
    pub from_us: Option<u64>,
    pub to_us: Option<u64>,
}

/// The record collections a repository backfill windows by age (owner
/// 2026-11-01: keep only recent posts/likes/reposts; older history is fetched
/// per account while someone scrolls their profile). A snapshot replace must
/// never delete rows of these collections that are OLDER than the window it
/// covers, or re-backfilling an account would wipe posts that scrolling or
/// Jetstream stored. Graph and profile records are current state and are not
/// windowed.
pub const WINDOWED_SNAPSHOT_COLLECTIONS: &[&str] = &[
    "app.bsky.feed.post",
    "app.bsky.feed.like",
    "app.bsky.feed.repost",
];

/// Time bounds of one batched snapshot-delete loop: rows up to `cutoff_us`
/// are candidates, and when `window_start_us` is set the windowed collections
/// only lose rows from the window start onward (see
/// [`WINDOWED_SNAPSHOT_COLLECTIONS`]).
#[derive(Clone, Copy)]
struct SnapshotDeleteBounds {
    cutoff_us: i64,
    window_start_us: Option<i64>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RecordCursor {
    pub time_us: u64,
    pub uri: String,
}

impl RecordCursor {
    pub fn parse(value: &str) -> Option<Self> {
        if let Some((time, uri)) = value.split_once(':') {
            Some(Self {
                time_us: time.parse().ok()?,
                uri: uri.to_owned(),
            })
        } else {
            Some(Self {
                time_us: value.parse().ok()?,
                uri: String::new(),
            })
        }
    }

    pub fn encode(&self) -> String {
        format!("{}:{}", self.time_us, self.uri)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SortTimeCursor {
    pub sort_us: u64,
    pub time_us: u64,
    pub uri: String,
}

impl SortTimeCursor {
    pub fn parse(value: &str) -> Option<Self> {
        let parts = value.split('|').collect::<Vec<_>>();
        if parts.len() != 3 {
            return None;
        }
        Some(Self {
            sort_us: parts[0].parse().ok()?,
            time_us: parts[1].parse().ok()?,
            uri: parts[2].to_owned(),
        })
    }

    pub fn encode(&self) -> String {
        format!("{}|{}|{}", self.sort_us, self.time_us, self.uri)
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NotificationCursor {
    pub time_us: u64,
    pub uri: String,
    pub reason: String,
}

impl NotificationCursor {
    pub fn parse(value: &str) -> Option<Self> {
        if let Some((time, rest)) = value.split_once('|') {
            let (uri, reason) = rest.split_once('|')?;
            Some(Self {
                time_us: time.parse().ok()?,
                uri: uri.to_owned(),
                reason: reason.to_owned(),
            })
        } else {
            Some(Self {
                time_us: value.parse().ok()?,
                uri: String::new(),
                reason: String::new(),
            })
        }
    }

    pub fn encode(&self) -> String {
        format!("{}|{}|{}", self.time_us, self.uri, self.reason)
    }
}

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
    #[error("record encoding error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("storage lock was poisoned")]
    Poisoned,
    #[error("storage I/O error: {0}")]
    Io(#[from] std::io::Error),
    #[error("SQLite writer stopped before replying")]
    WriterStopped,
    #[error("blocking storage task failed: {0}")]
    BlockingTask(String),
    #[error("web SQLite read {operation} exceeded its budget after {elapsed:?}")]
    WebReadTimeout {
        operation: &'static str,
        elapsed: Duration,
    },
}

const WEB_READ_PROGRESS_OPS: i32 = 1_000;
static WEB_READ_BUDGET_MS: AtomicU64 = AtomicU64::new(10_000);
static WEB_READ_TIMEOUT_COUNT: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy)]
struct WebReadBudget {
    started: Instant,
    limit: Duration,
}

thread_local! {
    static WEB_READ_BUDGET: Cell<Option<WebReadBudget>> = const { Cell::new(None) };
    static WEB_READ_INTERRUPTED: Cell<bool> = const { Cell::new(false) };
    static WEB_READ_USED_SQLITE: Cell<bool> = const { Cell::new(false) };
}

/// Configures the maximum time a web storage operation may spend in SQLite.
/// SQLite read connections enforce the deadline with their progress handler.
pub fn configure_web_read_timeout(budget: Duration) {
    WEB_READ_BUDGET_MS.store(
        budget.as_millis().min(u128::from(u64::MAX)) as u64,
        Ordering::Relaxed,
    );
}

/// Cumulative number of web SQLite reads interrupted for exceeding their budget.
pub fn web_read_timeout_count() -> u64 {
    WEB_READ_TIMEOUT_COUNT.load(Ordering::Relaxed)
}

struct WebReadBudgetGuard {
    previous_budget: Option<WebReadBudget>,
    previous_interrupted: bool,
    previous_used_sqlite: bool,
}

impl WebReadBudgetGuard {
    fn install(started: Instant, limit: Duration) -> Self {
        Self {
            previous_budget: WEB_READ_BUDGET
                .with(|budget| budget.replace(Some(WebReadBudget { started, limit }))),
            previous_interrupted: WEB_READ_INTERRUPTED.with(|value| value.replace(false)),
            previous_used_sqlite: WEB_READ_USED_SQLITE.with(|value| value.replace(false)),
        }
    }

    fn exceeded(&self, elapsed: Duration, limit: Duration) -> bool {
        WEB_READ_INTERRUPTED.with(Cell::get)
            || (WEB_READ_USED_SQLITE.with(Cell::get) && elapsed >= limit)
    }
}

impl Drop for WebReadBudgetGuard {
    fn drop(&mut self) {
        WEB_READ_BUDGET.with(|budget| budget.set(self.previous_budget));
        WEB_READ_INTERRUPTED.with(|value| value.set(self.previous_interrupted));
        WEB_READ_USED_SQLITE.with(|value| value.set(self.previous_used_sqlite));
    }
}

fn web_read_progress_handler() -> bool {
    let exceeded = WEB_READ_BUDGET.with(|budget| {
        budget
            .get()
            .is_some_and(|budget| budget.started.elapsed() >= budget.limit)
    });
    if exceeded {
        WEB_READ_INTERRUPTED.with(|interrupted| interrupted.set(true));
    }
    exceeded
}

pub(crate) fn install_web_read_progress_handler(connection: &Connection) {
    if WEB_READ_BUDGET.with(|budget| budget.get().is_some()) {
        WEB_READ_USED_SQLITE.with(|used| used.set(true));
    }
    connection.progress_handler(WEB_READ_PROGRESS_OPS, Some(web_read_progress_handler));
}

/// Run a synchronous Storage operation away from Tokio's async worker threads.
/// Web handlers use this for all reads and writes that touch the index database.
pub async fn web_storage_call<S, T, F>(
    storage: Arc<S>,
    operation: &'static str,
    call: F,
) -> Result<T, StorageError>
where
    S: Storage + 'static,
    T: Send + 'static,
    F: FnOnce(&S) -> Result<T, StorageError> + Send + 'static,
{
    web_blocking_call(storage, operation, call).await
}

pub(crate) async fn web_blocking_call<S, T, F>(
    storage: Arc<S>,
    operation: &'static str,
    call: F,
) -> Result<T, StorageError>
where
    S: Send + Sync + 'static,
    T: Send + 'static,
    F: FnOnce(&S) -> Result<T, StorageError> + Send + 'static,
{
    let budget = Duration::from_millis(WEB_READ_BUDGET_MS.load(Ordering::Relaxed));
    web_blocking_call_with_budget(storage, operation, budget, call).await
}

async fn web_blocking_call_with_budget<S, T, F>(
    storage: Arc<S>,
    operation: &'static str,
    budget: Duration,
    call: F,
) -> Result<T, StorageError>
where
    S: Send + Sync + 'static,
    T: Send + 'static,
    F: FnOnce(&S) -> Result<T, StorageError> + Send + 'static,
{
    let trace_sink = STATEMENT_TRACE_SINK.with(|sink| sink.borrow().clone());
    let result = tokio::task::spawn_blocking(move || {
        let _trace_sink = StatementTraceSinkGuard::install(trace_sink);
        let started = Instant::now();
        let budget_guard = WebReadBudgetGuard::install(started, budget);
        let result = call(&storage);
        let elapsed = started.elapsed();
        if budget_guard.exceeded(elapsed, budget) {
            WEB_READ_TIMEOUT_COUNT.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                operation,
                elapsed_ms = elapsed.as_secs_f64() * 1_000.0,
                budget_ms = budget.as_secs_f64() * 1_000.0,
                "web SQLite read interrupted after exceeding its time budget"
            );
            Err(StorageError::WebReadTimeout { operation, elapsed })
        } else {
            result
        }
    })
    .await;
    match result {
        Ok(Ok(value)) => Ok(value),
        Ok(Err(error)) => {
            tracing::warn!(operation, %error, "web storage operation failed");
            Err(error)
        }
        Err(error) => {
            let error = StorageError::BlockingTask(error.to_string());
            tracing::error!(operation, %error, "web storage blocking task failed");
            Err(error)
        }
    }
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
struct TrackedSet {
    registered: HashSet<String>,
    follow_counts: HashMap<String, u32>,
}

impl TrackedSet {
    fn is_tracked(&self, did: &str) -> bool {
        self.registered.contains(did) || self.follow_counts.get(did).is_some_and(|count| *count > 0)
    }

    fn follow_count(&self, did: &str) -> u32 {
        self.follow_counts.get(did).copied().unwrap_or_default()
    }

    fn apply_follow_delta(&mut self, subject: &str, delta: i64) {
        let current = i64::from(self.follow_count(subject));
        let next = current.saturating_add(delta).clamp(0, i64::from(u32::MAX)) as u32;
        if next == 0 {
            self.follow_counts.remove(subject);
        } else {
            self.follow_counts.insert(subject.to_owned(), next);
        }
    }
}

fn notification_edges(
    record: &IndexedRecord,
) -> Vec<(IndexedRecord, String, String, Option<String>)> {
    let mut edges = Vec::new();
    let mut seen = HashSet::new();
    let mut add = |did: &str, reason: &str, subject: Option<&str>| {
        if did.starts_with("did:") && seen.insert((did.to_owned(), reason.to_owned())) {
            edges.push((
                record.clone(),
                did.to_owned(),
                reason.to_owned(),
                subject.map(str::to_owned),
            ));
        }
    };
    match record.collection.as_str() {
        "app.bsky.feed.like" | "app.bsky.feed.repost" => {
            if let Some(uri) = record
                .record
                .pointer("/subject/uri")
                .and_then(Value::as_str)
                && let Some(did) = at_uri_did(uri)
            {
                add(
                    did,
                    if record.collection == "app.bsky.feed.like" {
                        "like"
                    } else {
                        "repost"
                    },
                    Some(uri),
                );
            }
        }
        "app.bsky.graph.follow" => {
            if let Some(did) = record.record.get("subject").and_then(Value::as_str) {
                add(did, "follow", None);
            }
        }
        "app.bsky.feed.post" => {
            if let Some(uri) = record
                .record
                .pointer("/reply/parent/uri")
                .and_then(Value::as_str)
                && let Some(did) = at_uri_did(uri)
            {
                add(did, "reply", Some(uri));
            }
            if let Some(facets) = record.record.get("facets").and_then(Value::as_array) {
                for did in facets
                    .iter()
                    .filter_map(|facet| facet.get("features").and_then(Value::as_array))
                    .flatten()
                    .filter(|feature| {
                        feature.get("$type").and_then(Value::as_str)
                            == Some("app.bsky.richtext.facet#mention")
                    })
                    .filter_map(|feature| feature.get("did").and_then(Value::as_str))
                {
                    add(did, "mention", None);
                }
            }
            for quote_uri in [
                record.record.pointer("/embed/record/uri"),
                record.record.pointer("/embed/record/record/uri"),
                record.record.pointer("/embed/recordWithMedia/record/uri"),
                record
                    .record
                    .pointer("/embed/recordWithMedia/record/record/uri"),
            ]
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            {
                if let Some(did) = at_uri_did(quote_uri) {
                    add(did, "quote", Some(quote_uri));
                }
            }
        }
        _ => {}
    }
    edges
}

fn at_uri_did(uri: &str) -> Option<&str> {
    let rest = uri.strip_prefix("at://")?;
    let (did, _) = rest.split_once('/')?;
    did.starts_with("did:").then_some(did)
}

fn record_matches_cursor(record: &IndexedRecord, cursor: Option<&RecordCursor>) -> bool {
    cursor.is_none_or(|cursor| {
        record.time_us < cursor.time_us
            || (record.time_us == cursor.time_us && record.uri < cursor.uri)
    })
}

pub fn post_created_at(record: &IndexedRecord) -> String {
    record
        .record
        .get("createdAt")
        .and_then(Value::as_str)
        .unwrap_or("")
        .to_string()
}

/// The sort time Bluesky uses for a post: the earlier of the author's
/// `createdAt` and the arrival time, so a future-dated post cannot pin itself
/// above real new posts forever. Unparseable or missing `createdAt` uses the
/// arrival time.
fn sort_us(created_at: Option<&str>, arrival_us: u64) -> u64 {
    let created_us = created_at
        .and_then(crate::backfill::parse_rfc3339_micros)
        .unwrap_or(arrival_us);
    created_us.min(arrival_us)
}

pub fn post_sort_us(record: &IndexedRecord) -> u64 {
    let created_at = record.record.get("createdAt").and_then(Value::as_str);
    sort_us(created_at, record.time_us)
}

fn post_sort_us_from_json(json: &str, time_us: u64) -> u64 {
    let Ok(record) = serde_json::from_str::<Value>(json) else {
        return time_us;
    };
    let created_at = record.get("createdAt").and_then(Value::as_str);
    sort_us(created_at, time_us)
}

fn sort_time_matches_cursor(
    record: &IndexedRecord,
    sort_us: u64,
    cursor: Option<&SortTimeCursor>,
) -> bool {
    cursor.is_none_or(|cursor| {
        sort_us < cursor.sort_us
            || (sort_us == cursor.sort_us
                && (record.time_us < cursor.time_us
                    || (record.time_us == cursor.time_us && record.uri < cursor.uri)))
    })
}

fn notification_event_order(
    left: &NotificationEventRow,
    right: &NotificationEventRow,
) -> std::cmp::Ordering {
    right
        .time_us
        .cmp(&left.time_us)
        .then_with(|| right.source_uri.cmp(&left.source_uri))
        .then_with(|| right.reason.cmp(&left.reason))
}

fn memory_notification_rows<'records>(
    records: &'records HashMap<(String, String, String), IndexedRecord>,
    did: &str,
) -> impl Iterator<Item = NotificationEventRow> + 'records {
    let did = did.to_owned();
    records
        .values()
        .flat_map(notification_edges)
        .filter(move |(_, subject_did, _, _)| *subject_did == did)
        .map(|(record, _, reason, reason_subject)| NotificationEventRow {
            source_did: record.did,
            source_collection: record.collection,
            source_rkey: record.rkey,
            source_uri: record.uri,
            reason,
            reason_subject,
            time_us: record.time_us,
        })
}

fn notification_matches_cursor(
    record: &IndexedRecord,
    reason: &str,
    cursor: Option<&NotificationCursor>,
) -> bool {
    cursor.is_none_or(|cursor| {
        record.time_us < cursor.time_us
            || (record.time_us == cursor.time_us
                && (record.uri < cursor.uri
                    || (record.uri == cursor.uri && reason < cursor.reason.as_str())))
    })
}

pub trait Storage: Send + Sync {
    /// WAL frames written but not yet checkpointed into the database file.
    fn write_backlog(&self) -> u64 {
        0
    }
    fn write_pressure(&self) -> bool {
        false
    }
    fn record_backfill_wait(&self, _waited: Duration) {}
    /// Records a Jetstream batch retry after SQLite returned BUSY/LOCKED.
    fn record_jetstream_lock_retry(&self) {}
    fn put_record(&self, record: &IndexedRecord) -> Result<(), StorageError>;
    /// Queues a cache-warm write without waiting for the writer. Page paths
    /// use it for records they render from their own copy (freshly fetched
    /// author profiles); storage backends that can keep the queued record
    /// readable (read-your-writes) accept the write, everything else falls
    /// back to the acknowledged `put_record`.
    fn queue_record_write(&self, record: &IndexedRecord) -> Result<(), StorageError> {
        self.put_record(record)
    }
    fn apply_jetstream_batch(
        &self,
        updates: &[JetstreamStorageUpdate],
    ) -> Result<BatchApplyTiming, StorageError>;
    fn record_skipped_window(&self, from: u64, to: u64) -> Result<(), StorageError>;
    fn skipped_windows(&self) -> Result<Vec<SkippedCursorWindow>, StorageError>;
    fn replace_repo_snapshot(
        &self,
        did: &str,
        records: &[IndexedRecord],
        following: &[String],
    ) -> Result<(), StorageError>;
    fn replace_repo_snapshot_throttled(
        &self,
        did: &str,
        records: &[IndexedRecord],
        following: &[String],
        records_per_second: u64,
    ) -> Result<(), StorageError> {
        let _operation = SqliteOperationKindGuard::enter("backfill");
        let _ = records_per_second;
        self.replace_repo_snapshot(did, records, following)
    }
    /// Snapshot replace for a windowed backfill: `window_start_us` is the
    /// window start (microseconds since the epoch) the snapshot's coverage
    /// stops at. Rows of [`WINDOWED_SNAPSHOT_COLLECTIONS`] with `time_us`
    /// below it are NOT deleted, because this snapshot says nothing about
    /// them and they may have arrived from an older-on-scroll fetch or the
    /// firehose. `None` keeps the historical all-or-nothing replace.
    /// Backends that do not track a window delegate unchanged.
    fn replace_repo_snapshot_windowed(
        &self,
        did: &str,
        records: &[IndexedRecord],
        following: &[String],
        records_per_second: u64,
        window_start_us: Option<u64>,
    ) -> Result<(), StorageError> {
        let _operation = SqliteOperationKindGuard::enter("backfill");
        let _ = window_start_us;
        self.replace_repo_snapshot_throttled(did, records, following, records_per_second)
    }
    fn delete_record(&self, did: &str, collection: &str, rkey: &str) -> Result<(), StorageError>;
    fn get_record(
        &self,
        did: &str,
        collection: &str,
        rkey: &str,
    ) -> Result<Option<IndexedRecord>, StorageError>;
    fn record_by_uri(
        &self,
        collection: &str,
        uri: &str,
    ) -> Result<Option<IndexedRecord>, StorageError>;
    fn records_by_uri(
        &self,
        collection: &str,
        uris: &[String],
    ) -> Result<HashMap<String, IndexedRecord>, StorageError>;
    fn replies_to(
        &self,
        parent_uri: &str,
        limit: usize,
    ) -> Result<Vec<IndexedRecord>, StorageError>;
    fn records_for_subject(
        &self,
        collection: &str,
        subject_uri: &str,
        limit: usize,
    ) -> Result<Vec<IndexedRecord>, StorageError>;
    fn records_for_actor_collection(
        &self,
        did: &str,
        collection: &str,
        limit: usize,
    ) -> Result<Vec<IndexedRecord>, StorageError>;
    fn follows_for_actor(
        &self,
        did: &str,
        limit: usize,
    ) -> Result<Vec<IndexedRecord>, StorageError>;
    fn followers_for_did(
        &self,
        did: &str,
        limit: usize,
    ) -> Result<Vec<IndexedRecord>, StorageError>;
    fn profile_counts(&self, did: &str) -> Result<(usize, usize, usize), StorageError>;
    fn remember_recent_profile(
        &self,
        viewer_did: &str,
        profile_did: &str,
    ) -> Result<(), StorageError>;
    fn recent_profiles_for_viewer(&self, viewer_did: &str) -> Result<Vec<String>, StorageError>;
    fn clear_recent_profiles_for_viewer(&self, viewer_did: &str) -> Result<(), StorageError>;
    fn set_last_active_feed_tab(&self, viewer_did: &str, tab: &str) -> Result<(), StorageError>;
    fn last_active_feed_tab(&self, viewer_did: &str) -> Result<Option<String>, StorageError>;
    fn profile_index_snapshot(&self) -> Result<HashMap<String, ProfileIndexEntry>, StorageError>;
    fn set_profile_index_entry(
        &self,
        did: &str,
        entry: ProfileIndexEntry,
    ) -> Result<(), StorageError>;
    fn enqueue_profile_fetches(&self, dids: &[String]) -> Result<(), StorageError>;
    fn pending_profile_fetches(&self, limit: usize) -> Result<Vec<String>, StorageError>;
    fn complete_profile_fetch(&self, did: &str) -> Result<(), StorageError>;
    /// The per-account recent-posts fetch state, read without touching the
    /// writer. Backends that do not support the recent-posts worker report `None`.
    fn recent_posts_status(
        &self,
        _did: &str,
    ) -> Result<Option<RecentPostsFetchStatus>, StorageError> {
        Ok(None)
    }
    /// Queue one bounded, deduplicated "recent posts" job for one account. The
    /// queue is bounded by [`MAX_RECENT_POSTS_QUEUE`]; overflow is dropped and
    /// counted, never blocked.
    fn enqueue_recent_posts_fetch(&self, _did: &str) -> Result<RecentPostsEnqueue, StorageError> {
        Ok(RecentPostsEnqueue::Duplicate)
    }
    /// Record that a recent-posts fetch succeeded at `fetched_at_ms` (wall clock)
    /// and clear any failure backoff.
    fn record_recent_posts_fetched(
        &self,
        _did: &str,
        _fetched_at_ms: i64,
    ) -> Result<(), StorageError> {
        Ok(())
    }
    /// Record that a recent-posts fetch failed: keep the previous fetch time and
    /// do not retry before `next_retry_at_ms` (wall clock).
    fn record_recent_posts_failure(
        &self,
        _did: &str,
        _next_retry_at_ms: i64,
    ) -> Result<(), StorageError> {
        Ok(())
    }
    fn pending_recent_posts_fetches(&self, _limit: usize) -> Result<Vec<String>, StorageError> {
        Ok(Vec::new())
    }
    fn complete_recent_posts_fetch(&self, _did: &str) -> Result<(), StorageError> {
        Ok(())
    }
    /// The cumulative queued / done / failed / dropped counts this storage
    /// exposes for the recent-posts worker.
    fn recent_posts_counters(&self) -> (u64, u64, u64, u64) {
        (0, 0, 0, 0)
    }
    /// Where the older-posts walk through one account's PDS stands. `None`
    /// means no older page has been fetched for it yet.
    fn older_posts_state(&self, _did: &str) -> Result<Option<OlderPostsState>, StorageError> {
        Ok(None)
    }
    /// Store the outcome of one older-posts page: the PDS cursor to continue
    /// from (a `None` cursor means the history is exhausted) and whether the
    /// account's history is now complete locally.
    fn record_older_posts_page(
        &self,
        _did: &str,
        _cursor: Option<&str>,
        _exhausted: bool,
    ) -> Result<(), StorageError> {
        Ok(())
    }
    fn notifications_for(
        &self,
        did: &str,
        before: Option<&NotificationCursor>,
        limit: usize,
    ) -> Result<Vec<IndexedNotification>, StorageError>;
    /// Newest-first batch of the viewer's notification events, strictly after
    /// `before` in (time, uri, reason) order, skipping events already claimed by
    /// an open group (`skip`: reason, reason_subject, floor time inclusive).
    /// Never joins `records` and never reads `record_json`.
    fn notification_events_window(
        &self,
        did: &str,
        before: Option<&NotificationCursor>,
        skip: &[NotificationGroupSkip],
        limit: usize,
    ) -> Result<Vec<NotificationEventRow>, StorageError>;
    /// How many of the viewer's events for one (reason, reason_subject) key fall
    /// in the inclusive time range. A group's size is only ever rendered as a
    /// count, so the page COUNTs it instead of reading the rows.
    fn notification_key_event_count(
        &self,
        did: &str,
        reason: &str,
        reason_subject: Option<&str>,
        from_us: u64,
        to_us: u64,
    ) -> Result<usize, StorageError>;
    /// One key's events newest-first, strictly after `before` in (time, uri,
    /// reason) order and inside `window`: the leading-author probe (limit 5)
    /// and the cursor-edge group-chain probe (limit 1).
    fn notification_key_events(
        &self,
        did: &str,
        reason: &str,
        reason_subject: Option<&str>,
        before: Option<&NotificationCursor>,
        window: &NotificationKeyWindow,
        limit: usize,
    ) -> Result<Vec<NotificationEventRow>, StorageError>;
    fn notification_count(&self, did: &str) -> Result<usize, StorageError>;
    fn notification_last_seen(&self, did: &str) -> Result<u64, StorageError>;
    fn mark_notifications_seen(&self, did: &str, time_us: u64) -> Result<(), StorageError>;
    fn unread_notification_count(&self, did: &str) -> Result<usize, StorageError>;
    fn subscribe_notification_counts(&self, did: &str) -> NotificationSubscription;
    fn author_posts(
        &self,
        did: &str,
        cursor: Option<&RecordCursor>,
        limit: usize,
        include_replies: bool,
    ) -> Result<Vec<IndexedRecord>, StorageError>;
    fn timeline_posts(
        &self,
        actor: &str,
        cursor: Option<&RecordCursor>,
        limit: usize,
    ) -> Result<Vec<IndexedRecord>, StorageError>;
    /// One page of the home timeline ordered by the post sort time
    /// (`min(createdAt, arrival)`), not by the Jetstream arrival time in `time_us`.
    fn timeline_posts_by_post_time(
        &self,
        actor: &str,
        cursor: Option<&SortTimeCursor>,
        limit: usize,
    ) -> Result<Vec<IndexedRecord>, StorageError>;
    /// Rows still waiting for the bounded post-sort backfill.
    fn post_sort_backfill_remaining(&self) -> Result<u64, StorageError> {
        Ok(0)
    }
    /// Fills one bounded batch of old post rows into the post-sort side table.
    /// Returns the last `records.rowid` examined (0 when the walk is done), not a count.
    fn backfill_post_sort_batch(&self, _limit: usize) -> Result<u64, StorageError> {
        Ok(0)
    }
    /// Cumulative failed post-sort backfill attempts, exposed in /healthz.
    fn post_sort_backfill_failures(&self) -> u64 {
        0
    }
    /// Counts one failed post-sort backfill attempt. A running service exposes
    /// the total through [`post_sort_backfill_failures`].
    fn record_post_sort_backfill_failure(&self) {}
    /// The sort key the timeline query uses for this record: the stored sort time
    /// when the row has been filled, otherwise its arrival time.
    fn sort_us_for_record(&self, record: &IndexedRecord) -> Result<u64, StorageError> {
        Ok(post_sort_us(record))
    }
    fn post_interaction_summary(
        &self,
        uri: &str,
        viewer_did: Option<&str>,
    ) -> Result<PostInteractionSummary, StorageError>;
    /// Interaction summaries for a whole page of posts. Counts come from the
    /// `post_counts` cache where warm and from one batched read pass otherwise
    /// (never from the writer), and cache fills are queued fire-and-forget, so
    /// a page never waits on pending writes. Counts may be one render stale.
    fn page_post_interaction_summaries(
        &self,
        uris: &[String],
        viewer_did: Option<&str>,
    ) -> Result<HashMap<String, PostInteractionSummary>, StorageError> {
        let mut summaries = HashMap::new();
        for uri in uris {
            summaries.insert(uri.clone(), self.post_interaction_summary(uri, viewer_did)?);
        }
        Ok(summaries)
    }
    fn records(&self) -> Result<Vec<IndexedRecord>, StorageError>;
    fn cursor(&self) -> Result<Option<u64>, StorageError>;
    fn set_cursor(&self, cursor: u64) -> Result<(), StorageError>;
    fn register_user(&self, did: &str) -> Result<(), StorageError>;
    fn unregister_user(&self, did: &str) -> Result<(), StorageError>;
    fn is_registered(&self, did: &str) -> Result<bool, StorageError>;
    fn rotate_web_session(&self, did: &str, session_id: &str) -> Result<u64, StorageError>;
    fn web_session_generation(
        &self,
        did: &str,
        session_id: &str,
    ) -> Result<Option<u64>, StorageError>;
    fn delete_web_session(&self, did: &str, session_id: &str) -> Result<(), StorageError>;
    fn set_follow(&self, actor: &str, subject: &str, following: bool) -> Result<(), StorageError>;
    fn is_following(&self, actor: &str, subject: &str) -> Result<bool, StorageError>;
    /// Whether the actor's own `app.bsky.graph.follow` records in `records`
    /// name this subject, independent of the `follows` projection table.
    fn has_follow_record(&self, actor: &str, subject: &str) -> Result<bool, StorageError>;
    fn tracked_follow_count(&self, did: &str) -> Result<u32, StorageError>;
    fn is_followed_by_registered(&self, did: &str) -> Result<bool, StorageError>;
    fn enqueue_backfill(&self, did: &str) -> Result<(), StorageError>;
    fn enqueue_backfill_for(&self, owner_did: &str, did: &str) -> Result<(), StorageError>;
    fn pending_backfills(&self) -> Result<Vec<String>, StorageError>;
    fn complete_backfill(&self, did: &str) -> Result<(), StorageError>;
    fn record_backfill_failure(&self, did: &str) -> Result<Duration, StorageError>;
    fn backfill_progress(&self, owner_did: &str) -> Result<(usize, usize), StorageError>;
    fn backfill_failure_count(&self, owner_did: &str) -> Result<usize, StorageError>;
    fn set_verified_handle(&self, did: &str, handle: &str) -> Result<(), StorageError>;
    fn clear_verified_handle(&self, did: &str) -> Result<(), StorageError>;
    fn verified_handle(&self, did: &str) -> Result<Option<String>, StorageError>;
    fn handle_is_invalid(&self, did: &str) -> Result<bool, StorageError>;
    fn did_by_verified_handle(&self, handle: &str) -> Result<Option<String>, StorageError>;
    fn is_tracked_did(&self, did: &str) -> Result<bool, StorageError>;
    fn registered_followers(&self, did: &str) -> Result<Vec<String>, StorageError>;
    fn is_displayed_did(&self, did: &str) -> Result<bool, StorageError>;
    fn stale_handle_dids(&self, ttl: Duration, limit: usize) -> Result<Vec<String>, StorageError>;
    fn queue_handle_refresh(&self, did: &str) -> Result<(), StorageError>;
    fn queue_handle_refresh_ordered(&self, dids: &[String]) -> Result<(), StorageError>;
    fn pending_handle_refreshes(&self, limit: usize) -> Result<Vec<String>, StorageError>;
    fn complete_handle_refresh(&self, did: &str) -> Result<(), StorageError>;
    fn bytes_on_disk(&self) -> u64;
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct PostSortBackfillRun {
    pub failures: u64,
    pub batches: u64,
    pub backoff: Duration,
}

/// Drives the post-sort backfill to completion. A transient error (a locked
/// database, a failed read, a failed batch) is logged, counted, slept on with
/// a doubling backoff, and retried; it never ends the walk. The backoff resets
/// after every successful batch. The walk ends only when the remaining count
/// is zero or the batch reports rowid 0.
pub async fn post_sort_backfill<S: Storage>(
    storage: &S,
    batch_size: usize,
    pause: Duration,
    retry_base: Duration,
    retry_max: Duration,
) -> Result<PostSortBackfillRun, StorageError> {
    let mut failures = 0_u64;
    let mut batches = 0_u64;
    let mut backoff = retry_base;
    loop {
        let remaining_result = storage.post_sort_backfill_remaining();
        if let Err(error) = remaining_result {
            failures = failures.saturating_add(1);
            storage.record_post_sort_backfill_failure();
            tracing::warn!(%error, "post-sort backfill could not read its remaining row count");
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(retry_max);
            continue;
        }
        let Ok(remaining) = remaining_result else {
            continue;
        };
        if remaining == 0 {
            break;
        }
        let batch_result = storage.backfill_post_sort_batch(batch_size);
        if let Err(error) = batch_result {
            failures = failures.saturating_add(1);
            storage.record_post_sort_backfill_failure();
            tracing::warn!(%error, "post-sort backfill batch failed");
            tokio::time::sleep(backoff).await;
            backoff = (backoff * 2).min(retry_max);
            continue;
        }
        let Ok(last_rowid) = batch_result else {
            continue;
        };
        if last_rowid == 0 {
            break;
        }
        batches = batches.saturating_add(1);
        backoff = retry_base;
        tokio::time::sleep(pause).await;
    }
    Ok(PostSortBackfillRun {
        failures,
        batches,
        backoff,
    })
}

#[derive(Clone, Default)]
pub struct NotificationHub {
    channels: Arc<Mutex<HashMap<String, broadcast::Sender<usize>>>>,
}

impl NotificationHub {
    const CHANNEL_CAPACITY: usize = 16;

    pub fn subscribe(&self, did: &str) -> NotificationSubscription {
        let receiver = if let Ok(mut channels) = self.channels.lock() {
            channels.retain(|_, sender| sender.receiver_count() > 0);
            channels
                .entry(did.to_owned())
                .or_insert_with(|| broadcast::channel(Self::CHANNEL_CAPACITY).0)
                .subscribe()
        } else {
            let (sender, receiver) = broadcast::channel(1);
            drop(sender);
            receiver
        };
        NotificationSubscription {
            did: did.to_owned(),
            hub: self.clone(),
            receiver,
        }
    }

    pub fn publish(&self, did: &str, unread_count: usize) {
        if let Ok(mut channels) = self.channels.lock()
            && let Some(sender) = channels.get(did)
        {
            let _ = sender.send(unread_count);
            if sender.receiver_count() == 0 {
                channels.remove(did);
            }
        }
    }
}

pub struct NotificationSubscription {
    did: String,
    hub: NotificationHub,
    receiver: broadcast::Receiver<usize>,
}

impl NotificationSubscription {
    pub async fn recv(&mut self) -> Result<usize, broadcast::error::RecvError> {
        self.receiver.recv().await
    }
}

impl Drop for NotificationSubscription {
    fn drop(&mut self) {
        if let Ok(mut channels) = self.hub.channels.lock()
            && channels
                .get(&self.did)
                .is_some_and(|sender| sender.receiver_count() <= 1)
        {
            channels.remove(&self.did);
        }
    }
}

fn publish_notification_counts<S: Storage>(
    storage: &S,
    hub: &NotificationHub,
    dids: impl IntoIterator<Item = String>,
) {
    for did in dids.into_iter().collect::<HashSet<_>>() {
        if let Ok(count) = storage.unread_notification_count(&did) {
            hub.publish(&did, count);
        }
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct BatchApplyTiming {
    pub apply: Duration,
    pub commit: Duration,
}

pub(crate) fn ensure_private_database_file(path: &Path) -> Result<(), std::io::Error> {
    if path == Path::new(":memory:") {
        return Ok(());
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::{OpenOptionsExt, PermissionsExt};

        match std::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(path)
        {
            Ok(file) => drop(file),
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {}
            Err(error) => return Err(error),
        }
        if std::fs::symlink_metadata(path)?.file_type().is_symlink() {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "SQLite database path must not be a symlink",
            ));
        }
        fs::set_permissions(path, fs::Permissions::from_mode(0o600))?;
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(())
}

#[derive(Default)]
pub struct MemoryStorage {
    inner: Mutex<MemoryState>,
    notification_hub: NotificationHub,
    recent_posts_counters: RecentPostsCounters,
    #[cfg(test)]
    record_uri_single_lookups: AtomicUsize,
    #[cfg(test)]
    record_uri_batch_lookups: AtomicUsize,
    #[cfg(test)]
    record_uri_batch_delay_ms: AtomicU64,
    #[cfg(test)]
    put_record_delay_ms: AtomicU64,
    #[cfg(test)]
    timeline_post_time_delay_ms: AtomicU64,
    #[cfg(test)]
    force_home_timeline_timeout: AtomicBool,
    #[cfg(test)]
    notification_window_read_delay_ms: AtomicU64,
    cursor_lock_failures: AtomicUsize,
    #[cfg(test)]
    apply_jetstream_busy_failures: AtomicUsize,
    #[cfg(test)]
    apply_jetstream_nonlock_failures: AtomicUsize,
    #[cfg(test)]
    apply_jetstream_attempts: AtomicUsize,
    #[cfg(test)]
    jetstream_lock_retries: AtomicUsize,
}

impl MemoryStorage {
    #[cfg(test)]
    fn set_apply_jetstream_busy_failures(&self, failures: usize) {
        self.apply_jetstream_busy_failures
            .store(failures, Ordering::SeqCst);
    }

    #[cfg(test)]
    fn set_apply_jetstream_nonlock_failures(&self, failures: usize) {
        self.apply_jetstream_nonlock_failures
            .store(failures, Ordering::SeqCst);
    }

    fn replace_snapshot_state(
        &self,
        did: &str,
        records: &[IndexedRecord],
        following: &[String],
        window_start_us: Option<u64>,
    ) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        let mut affected = inner
            .records
            .values()
            .filter(|record| record.did == did)
            .flat_map(notification_edges)
            .map(|(_, subject_did, _, _)| subject_did)
            .collect::<Vec<_>>();
        affected.extend(
            records
                .iter()
                .flat_map(notification_edges)
                .map(|(_, subject_did, _, _)| subject_did),
        );
        inner.records.retain(|(record_did, collection, _), entry| {
            if record_did != did {
                return true;
            }
            // Same rule as the SQLite snapshot delete: a windowed snapshot
            // proves nothing about windowed-collection rows older than its
            // window start, so those rows survive.
            window_start_us.is_some_and(|window_start| {
                WINDOWED_SNAPSHOT_COLLECTIONS.contains(&collection.as_str())
                    && entry.time_us < window_start
            })
        });
        let mut latest_profile = None;
        let old_follows = inner
            .follows
            .iter()
            .filter(|(actor, _)| actor == did)
            .map(|(_, subject)| subject.clone())
            .collect::<Vec<_>>();
        if inner.tracked.registered.contains(did) {
            for subject in &old_follows {
                inner.tracked.apply_follow_delta(subject, -1);
            }
        }
        inner.follows.retain(|(actor, _)| actor != did);
        inner
            .backfill_owners
            .retain(|(owner, target)| owner != did || target == did);
        for record in records {
            let key = (
                record.did.clone(),
                record.collection.clone(),
                record.rkey.clone(),
            );
            inner.records.insert(key, record.clone());
            if record.collection == "app.bsky.actor.profile" {
                latest_profile = Some(record.time_us);
            }
        }
        inner.profile_index.insert(
            did.to_owned(),
            ProfileIndexEntry {
                has_profile: latest_profile.is_some(),
                fetched_at_ms: latest_profile
                    .map(profile_time_ms)
                    .unwrap_or_else(epoch_millis),
            },
        );
        for subject in following {
            let inserted = inner.follows.insert((did.to_owned(), subject.clone()));
            if inserted && inner.tracked.registered.contains(did) {
                inner.tracked.apply_follow_delta(subject, 1);
            }
            if inner.tracked.registered.contains(did)
                && (inner.backfill_queue.contains(subject)
                    || inner.backfill_queue.len() < MAX_BACKFILL_QUEUE)
            {
                inner
                    .backfill_owners
                    .insert((did.to_owned(), subject.clone()));
                inner.backfill_queue.insert(subject.clone());
                inner.backfill_completed.remove(subject);
            }
        }
        drop(inner);
        publish_notification_counts(self, &self.notification_hub, affected);
        Ok(())
    }

    pub fn set_cursor_lock_failures(&self, failures: usize) {
        self.cursor_lock_failures.store(failures, Ordering::SeqCst);
    }
}

#[cfg(test)]
impl MemoryStorage {
    pub(crate) fn record_uri_lookup_counts(&self) -> (usize, usize) {
        (
            self.record_uri_single_lookups.load(Ordering::Relaxed),
            self.record_uri_batch_lookups.load(Ordering::Relaxed),
        )
    }

    pub(crate) fn set_put_record_delay(&self, delay: Duration) {
        self.put_record_delay_ms.store(
            delay.as_millis().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
    }

    pub(crate) fn set_timeline_post_time_delay(&self, delay: Duration) {
        self.timeline_post_time_delay_ms.store(
            delay.as_millis().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
    }

    #[cfg(test)]
    pub(crate) fn force_home_timeline_timeout_for_test(&self) {
        self.force_home_timeline_timeout
            .store(true, Ordering::Relaxed);
    }

    pub(crate) fn set_record_uri_batch_delay(&self, delay: Duration) {
        self.record_uri_batch_delay_ms.store(
            delay.as_millis().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
    }

    pub(crate) fn set_notification_window_read_delay(&self, delay: Duration) {
        self.notification_window_read_delay_ms.store(
            delay.as_millis().min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
    }
}

#[derive(Default)]
struct MemoryState {
    records: HashMap<(String, String, String), IndexedRecord>,
    cursor: Option<u64>,
    skipped_windows: Vec<SkippedCursorWindow>,
    tracked: TrackedSet,
    web_session_generations: HashMap<(String, String), u64>,
    follows: HashSet<(String, String)>,
    backfill_queue: HashSet<String>,
    backfill_retries: HashMap<String, BackfillRetry>,
    backfill_owners: HashSet<(String, String)>,
    backfill_completed: HashSet<String>,
    verified_handles: HashMap<String, (String, Instant)>,
    invalid_handles: HashMap<String, Instant>,
    handle_refresh_queue: HashSet<String>,
    handle_refresh_order: VecDeque<String>,
    profile_index: HashMap<String, ProfileIndexEntry>,
    recent_profiles: HashMap<String, Vec<String>>,
    last_active_feed_tabs: HashMap<String, String>,
    profile_fetch_queue: VecDeque<String>,
    recent_posts_queue: VecDeque<String>,
    recent_posts_queued: HashSet<String>,
    recent_posts_status: HashMap<String, (i64, i64)>,
    older_posts: HashMap<String, OlderPostsState>,
    notification_last_seen: HashMap<String, u64>,
}

struct BackfillRetry {
    failures: u32,
    retry_at: Instant,
}

impl Storage for MemoryStorage {
    fn record_jetstream_lock_retry(&self) {
        #[cfg(test)]
        self.jetstream_lock_retries.fetch_add(1, Ordering::SeqCst);
    }

    fn put_record(&self, record: &IndexedRecord) -> Result<(), StorageError> {
        #[cfg(test)]
        {
            let delay = self.put_record_delay_ms.load(Ordering::Relaxed);
            if delay > 0 {
                std::thread::sleep(Duration::from_millis(delay));
            }
        }
        let affected = notification_edges(record)
            .into_iter()
            .map(|(_, subject_did, _, _)| subject_did)
            .collect::<Vec<_>>();
        let key = (
            record.did.clone(),
            record.collection.clone(),
            record.rkey.clone(),
        );
        let mut inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        inner.records.insert(key, record.clone());
        if record.collection == "app.bsky.actor.profile" {
            inner.profile_index.insert(
                record.did.clone(),
                ProfileIndexEntry {
                    has_profile: true,
                    fetched_at_ms: profile_time_ms(record.time_us),
                },
            );
        }
        drop(inner);
        publish_notification_counts(self, &self.notification_hub, affected);
        Ok(())
    }

    fn apply_jetstream_batch(
        &self,
        updates: &[JetstreamStorageUpdate],
    ) -> Result<BatchApplyTiming, StorageError> {
        #[cfg(test)]
        {
            self.apply_jetstream_attempts.fetch_add(1, Ordering::SeqCst);
            if self.apply_jetstream_busy_failures.load(Ordering::SeqCst) > 0 {
                self.apply_jetstream_busy_failures
                    .fetch_sub(1, Ordering::SeqCst);
                return Err(StorageError::Sqlite(rusqlite::Error::SqliteFailure(
                    rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_BUSY),
                    Some("database is locked".to_owned()),
                )));
            }
            if self.apply_jetstream_nonlock_failures.load(Ordering::SeqCst) > 0 {
                self.apply_jetstream_nonlock_failures
                    .fetch_sub(1, Ordering::SeqCst);
                return Err(StorageError::Poisoned);
            }
        }
        let started = Instant::now();
        if updates.is_empty() {
            return Ok(BatchApplyTiming::default());
        }
        let mut inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        let mut affected = Vec::new();
        for update in updates {
            if let Some(record) = &update.upsert {
                affected.extend(
                    notification_edges(record)
                        .into_iter()
                        .map(|(_, subject_did, _, _)| subject_did),
                );
                let key = (
                    record.did.clone(),
                    record.collection.clone(),
                    record.rkey.clone(),
                );
                inner.records.insert(key, record.clone());
                if record.collection == "app.bsky.actor.profile" {
                    inner.profile_index.insert(
                        record.did.clone(),
                        ProfileIndexEntry {
                            has_profile: true,
                            fetched_at_ms: profile_time_ms(record.time_us),
                        },
                    );
                }
            }
            if let Some((did, collection, rkey)) = &update.delete {
                if let Some(record) =
                    inner
                        .records
                        .get(&(did.clone(), collection.clone(), rkey.clone()))
                {
                    affected.extend(
                        notification_edges(record)
                            .into_iter()
                            .map(|(_, subject_did, _, _)| subject_did),
                    );
                }
                inner
                    .records
                    .remove(&(did.clone(), collection.clone(), rkey.clone()));
                if collection == "app.bsky.actor.profile" {
                    inner.profile_index.insert(
                        did.clone(),
                        ProfileIndexEntry {
                            has_profile: false,
                            fetched_at_ms: epoch_millis(),
                        },
                    );
                }
            }
            for follow in &update.follows {
                let edge = (follow.actor.clone(), follow.subject.clone());
                let was_following = inner.follows.contains(&edge);
                if follow.following {
                    inner.follows.insert(edge);
                } else {
                    inner.follows.remove(&edge);
                    inner
                        .backfill_owners
                        .remove(&(follow.actor.clone(), follow.subject.clone()));
                }
                if was_following != follow.following
                    && inner.tracked.registered.contains(&follow.actor)
                {
                    inner
                        .tracked
                        .apply_follow_delta(&follow.subject, if follow.following { 1 } else { -1 });
                }
            }
            for (owner_did, did) in &update.backfill_for {
                if !inner.tracked.registered.contains(owner_did)
                    || (did != owner_did
                        && !inner.follows.contains(&(owner_did.clone(), did.clone())))
                    || (!inner.backfill_queue.contains(did)
                        && inner.backfill_queue.len() >= MAX_BACKFILL_QUEUE)
                {
                    continue;
                }
                inner
                    .backfill_owners
                    .insert((owner_did.clone(), did.clone()));
                inner.backfill_queue.insert(did.clone());
                inner.backfill_completed.remove(did);
            }
            if let Some(did) = &update.handle_refresh
                && inner.handle_refresh_queue.insert(did.clone())
            {
                inner.handle_refresh_order.push_back(did.clone());
            }
            inner.cursor = Some(update.time_us);
        }
        drop(inner);
        publish_notification_counts(self, &self.notification_hub, affected);
        Ok(BatchApplyTiming {
            apply: started.elapsed(),
            commit: Duration::ZERO,
        })
    }

    fn record_skipped_window(&self, from: u64, to: u64) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        inner.skipped_windows.push(SkippedCursorWindow { from, to });
        inner.cursor = Some(to);
        Ok(())
    }

    fn skipped_windows(&self) -> Result<Vec<SkippedCursorWindow>, StorageError> {
        Ok(self
            .inner
            .lock()
            .map_err(|_| StorageError::Poisoned)?
            .skipped_windows
            .clone())
    }

    fn replace_repo_snapshot(
        &self,
        did: &str,
        records: &[IndexedRecord],
        following: &[String],
    ) -> Result<(), StorageError> {
        self.replace_snapshot_state(did, records, following, None)
    }

    fn replace_repo_snapshot_windowed(
        &self,
        did: &str,
        records: &[IndexedRecord],
        following: &[String],
        _records_per_second: u64,
        window_start_us: Option<u64>,
    ) -> Result<(), StorageError> {
        self.replace_snapshot_state(did, records, following, window_start_us)
    }

    fn delete_record(&self, did: &str, collection: &str, rkey: &str) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        let removed =
            inner
                .records
                .remove(&(did.to_owned(), collection.to_owned(), rkey.to_owned()));
        if collection == "app.bsky.actor.profile" {
            inner.profile_index.insert(
                did.to_owned(),
                ProfileIndexEntry {
                    has_profile: false,
                    fetched_at_ms: epoch_millis(),
                },
            );
        }
        drop(inner);
        if let Some(record) = removed {
            let affected = notification_edges(&record)
                .into_iter()
                .map(|(_, subject_did, _, _)| subject_did)
                .collect::<Vec<_>>();
            publish_notification_counts(self, &self.notification_hub, affected);
        }
        Ok(())
    }

    fn get_record(
        &self,
        did: &str,
        collection: &str,
        rkey: &str,
    ) -> Result<Option<IndexedRecord>, StorageError> {
        Ok(self
            .inner
            .lock()
            .map_err(|_| StorageError::Poisoned)?
            .records
            .get(&(did.to_owned(), collection.to_owned(), rkey.to_owned()))
            .cloned())
    }

    fn record_by_uri(
        &self,
        collection: &str,
        uri: &str,
    ) -> Result<Option<IndexedRecord>, StorageError> {
        #[cfg(test)]
        self.record_uri_single_lookups
            .fetch_add(1, Ordering::Relaxed);
        Ok(self
            .inner
            .lock()
            .map_err(|_| StorageError::Poisoned)?
            .records
            .values()
            .find(|record| record.collection == collection && record.uri == uri)
            .cloned())
    }

    fn records_by_uri(
        &self,
        collection: &str,
        uris: &[String],
    ) -> Result<HashMap<String, IndexedRecord>, StorageError> {
        #[cfg(test)]
        {
            self.record_uri_batch_lookups
                .fetch_add(1, Ordering::Relaxed);
            let delay = self.record_uri_batch_delay_ms.load(Ordering::Relaxed);
            if delay > 0 {
                std::thread::sleep(Duration::from_millis(delay));
            }
        }
        let wanted = uris.iter().map(String::as_str).collect::<HashSet<_>>();
        Ok(self
            .inner
            .lock()
            .map_err(|_| StorageError::Poisoned)?
            .records
            .values()
            .filter(|record| {
                record.collection == collection && wanted.contains(record.uri.as_str())
            })
            .map(|record| (record.uri.clone(), record.clone()))
            .collect())
    }

    fn replies_to(
        &self,
        parent_uri: &str,
        limit: usize,
    ) -> Result<Vec<IndexedRecord>, StorageError> {
        let mut replies = self
            .inner
            .lock()
            .map_err(|_| StorageError::Poisoned)?
            .records
            .values()
            .filter(|record| {
                record.collection == "app.bsky.feed.post"
                    && record
                        .record
                        .pointer("/reply/parent/uri")
                        .and_then(Value::as_str)
                        == Some(parent_uri)
            })
            .cloned()
            .collect::<Vec<_>>();
        replies.sort_by(|left, right| {
            left.time_us
                .cmp(&right.time_us)
                .then_with(|| left.uri.cmp(&right.uri))
        });
        replies.truncate(limit.clamp(1, 100));
        Ok(replies)
    }

    fn records_for_subject(
        &self,
        collection: &str,
        subject_uri: &str,
        limit: usize,
    ) -> Result<Vec<IndexedRecord>, StorageError> {
        let mut records = self
            .inner
            .lock()
            .map_err(|_| StorageError::Poisoned)?
            .records
            .values()
            .filter(|record| {
                record.collection == collection
                    && record
                        .record
                        .pointer("/subject/uri")
                        .and_then(Value::as_str)
                        == Some(subject_uri)
            })
            .cloned()
            .collect::<Vec<_>>();
        records.sort_by_key(|record| Reverse(record.time_us));
        records.truncate(limit.clamp(1, 101));
        Ok(records)
    }

    fn records_for_actor_collection(
        &self,
        did: &str,
        collection: &str,
        limit: usize,
    ) -> Result<Vec<IndexedRecord>, StorageError> {
        let mut records = self
            .inner
            .lock()
            .map_err(|_| StorageError::Poisoned)?
            .records
            .values()
            .filter(|record| record.collection == collection && record.did == did)
            .cloned()
            .collect::<Vec<_>>();
        records.sort_by_key(|record| Reverse(record.time_us));
        records.truncate(limit.clamp(1, 10_000));
        Ok(records)
    }

    fn follows_for_actor(
        &self,
        did: &str,
        limit: usize,
    ) -> Result<Vec<IndexedRecord>, StorageError> {
        let mut records = self
            .inner
            .lock()
            .map_err(|_| StorageError::Poisoned)?
            .records
            .values()
            .filter(|record| record.collection == "app.bsky.graph.follow" && record.did == did)
            .cloned()
            .collect::<Vec<_>>();
        records.sort_by_key(|record| Reverse(record.time_us));
        records.truncate(limit.clamp(1, 101));
        Ok(records)
    }

    fn followers_for_did(
        &self,
        did: &str,
        limit: usize,
    ) -> Result<Vec<IndexedRecord>, StorageError> {
        let mut records = self
            .inner
            .lock()
            .map_err(|_| StorageError::Poisoned)?
            .records
            .values()
            .filter(|record| {
                record.collection == "app.bsky.graph.follow"
                    && record.record.get("subject").and_then(Value::as_str) == Some(did)
            })
            .cloned()
            .collect::<Vec<_>>();
        records.sort_by_key(|record| Reverse(record.time_us));
        records.truncate(limit.clamp(1, 101));
        Ok(records)
    }

    fn profile_counts(&self, did: &str) -> Result<(usize, usize, usize), StorageError> {
        let inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        let mut follows = 0;
        let mut followers = 0;
        let mut posts = 0;
        for record in inner.records.values() {
            match record.collection.as_str() {
                "app.bsky.graph.follow" => {
                    follows += usize::from(record.did == did);
                    followers += usize::from(
                        record.record.get("subject").and_then(Value::as_str) == Some(did),
                    );
                }
                "app.bsky.feed.post" => posts += usize::from(record.did == did),
                _ => {}
            }
        }
        Ok((follows, followers, posts))
    }

    fn remember_recent_profile(
        &self,
        viewer_did: &str,
        profile_did: &str,
    ) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        let recent = inner
            .recent_profiles
            .entry(viewer_did.to_owned())
            .or_default();
        recent.retain(|did| did != profile_did);
        recent.insert(0, profile_did.to_owned());
        recent.truncate(10);
        Ok(())
    }

    fn recent_profiles_for_viewer(&self, viewer_did: &str) -> Result<Vec<String>, StorageError> {
        Ok(self
            .inner
            .lock()
            .map_err(|_| StorageError::Poisoned)?
            .recent_profiles
            .get(viewer_did)
            .cloned()
            .unwrap_or_default())
    }

    fn clear_recent_profiles_for_viewer(&self, viewer_did: &str) -> Result<(), StorageError> {
        self.inner
            .lock()
            .map_err(|_| StorageError::Poisoned)?
            .recent_profiles
            .remove(viewer_did);
        Ok(())
    }

    fn set_last_active_feed_tab(&self, viewer_did: &str, tab: &str) -> Result<(), StorageError> {
        self.inner
            .lock()
            .map_err(|_| StorageError::Poisoned)?
            .last_active_feed_tabs
            .insert(viewer_did.to_owned(), tab.to_owned());
        Ok(())
    }

    fn last_active_feed_tab(&self, viewer_did: &str) -> Result<Option<String>, StorageError> {
        Ok(self
            .inner
            .lock()
            .map_err(|_| StorageError::Poisoned)?
            .last_active_feed_tabs
            .get(viewer_did)
            .cloned())
    }

    fn profile_index_snapshot(&self) -> Result<HashMap<String, ProfileIndexEntry>, StorageError> {
        Ok(self
            .inner
            .lock()
            .map_err(|_| StorageError::Poisoned)?
            .profile_index
            .clone())
    }

    fn set_profile_index_entry(
        &self,
        did: &str,
        entry: ProfileIndexEntry,
    ) -> Result<(), StorageError> {
        self.inner
            .lock()
            .map_err(|_| StorageError::Poisoned)?
            .profile_index
            .insert(did.to_owned(), entry);
        Ok(())
    }

    fn enqueue_profile_fetches(&self, dids: &[String]) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        for did in dids {
            if !inner.profile_fetch_queue.contains(did) {
                inner.profile_fetch_queue.push_back(did.clone());
            }
        }
        Ok(())
    }

    fn pending_profile_fetches(&self, limit: usize) -> Result<Vec<String>, StorageError> {
        Ok(self
            .inner
            .lock()
            .map_err(|_| StorageError::Poisoned)?
            .profile_fetch_queue
            .iter()
            .take(limit.max(1))
            .cloned()
            .collect())
    }

    fn complete_profile_fetch(&self, did: &str) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        inner.profile_fetch_queue.retain(|queued| queued != did);
        Ok(())
    }

    fn recent_posts_status(
        &self,
        did: &str,
    ) -> Result<Option<RecentPostsFetchStatus>, StorageError> {
        let inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        let status = inner.recent_posts_status.get(did).copied();
        let queued = inner.recent_posts_queued.contains(did);
        if status.is_none() && !queued {
            return Ok(None);
        }
        let (fetched_at_ms, next_retry_at_ms) = status.unwrap_or_default();
        Ok(Some(RecentPostsFetchStatus {
            fetched_at_ms,
            next_retry_at_ms,
            queued,
        }))
    }

    fn enqueue_recent_posts_fetch(&self, did: &str) -> Result<RecentPostsEnqueue, StorageError> {
        let mut inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        if inner.recent_posts_queued.contains(did) {
            return Ok(RecentPostsEnqueue::Duplicate);
        }
        if inner.recent_posts_queue.len() >= MAX_RECENT_POSTS_QUEUE {
            self.recent_posts_counters
                .dropped
                .fetch_add(1, Ordering::Relaxed);
            return Ok(RecentPostsEnqueue::Overflowed);
        }
        inner.recent_posts_queued.insert(did.to_owned());
        inner.recent_posts_queue.push_back(did.to_owned());
        self.recent_posts_counters
            .queued
            .fetch_add(1, Ordering::Relaxed);
        Ok(RecentPostsEnqueue::Queued)
    }

    fn record_recent_posts_fetched(
        &self,
        did: &str,
        fetched_at_ms: i64,
    ) -> Result<(), StorageError> {
        self.inner
            .lock()
            .map_err(|_| StorageError::Poisoned)?
            .recent_posts_status
            .insert(did.to_owned(), (fetched_at_ms, 0));
        self.recent_posts_counters
            .done
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn record_recent_posts_failure(
        &self,
        did: &str,
        next_retry_at_ms: i64,
    ) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        inner
            .recent_posts_status
            .entry(did.to_owned())
            .and_modify(|entry| entry.1 = next_retry_at_ms)
            .or_insert((0, next_retry_at_ms));
        self.recent_posts_counters
            .failed
            .fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn pending_recent_posts_fetches(&self, limit: usize) -> Result<Vec<String>, StorageError> {
        Ok(self
            .inner
            .lock()
            .map_err(|_| StorageError::Poisoned)?
            .recent_posts_queue
            .iter()
            .take(limit.max(1))
            .cloned()
            .collect())
    }

    fn complete_recent_posts_fetch(&self, did: &str) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        inner.recent_posts_queue.retain(|queued| queued != did);
        inner.recent_posts_queued.remove(did);
        Ok(())
    }

    fn recent_posts_counters(&self) -> (u64, u64, u64, u64) {
        self.recent_posts_counters.snapshot()
    }

    fn older_posts_state(&self, did: &str) -> Result<Option<OlderPostsState>, StorageError> {
        Ok(self
            .inner
            .lock()
            .map_err(|_| StorageError::Poisoned)?
            .older_posts
            .get(did)
            .cloned())
    }

    fn record_older_posts_page(
        &self,
        did: &str,
        cursor: Option<&str>,
        exhausted: bool,
    ) -> Result<(), StorageError> {
        self.inner
            .lock()
            .map_err(|_| StorageError::Poisoned)?
            .older_posts
            .insert(
                did.to_owned(),
                OlderPostsState {
                    cursor: cursor.map(str::to_owned),
                    exhausted,
                },
            );
        Ok(())
    }

    fn notifications_for(
        &self,
        did: &str,
        before: Option<&NotificationCursor>,
        limit: usize,
    ) -> Result<Vec<IndexedNotification>, StorageError> {
        let inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        let mut notifications = inner
            .records
            .values()
            .flat_map(notification_edges)
            .filter_map(|(record, subject_did, reason, reason_subject)| {
                (subject_did == did && notification_matches_cursor(&record, &reason, before))
                    .then_some(IndexedNotification {
                        record,
                        reason,
                        reason_subject,
                    })
            })
            .collect::<Vec<_>>();
        notifications.sort_by(|left, right| {
            right
                .record
                .time_us
                .cmp(&left.record.time_us)
                .then_with(|| right.record.uri.cmp(&left.record.uri))
                .then_with(|| right.reason.cmp(&left.reason))
        });
        notifications.truncate(limit.max(1));
        Ok(notifications)
    }

    fn notification_events_window(
        &self,
        did: &str,
        before: Option<&NotificationCursor>,
        skip: &[NotificationGroupSkip],
        limit: usize,
    ) -> Result<Vec<NotificationEventRow>, StorageError> {
        #[cfg(test)]
        {
            let delay = self
                .notification_window_read_delay_ms
                .load(Ordering::Relaxed);
            if delay > 0 {
                std::thread::sleep(Duration::from_millis(delay));
            }
        }
        let inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        let mut rows = memory_notification_rows(&inner.records, did)
            .filter(|row| before.is_none_or(|cursor| row.sorts_before(cursor)))
            .filter(|row| {
                !skip.iter().any(|(reason, subject, floor)| {
                    &row.reason == reason && row.reason_subject == *subject && row.time_us >= *floor
                })
            })
            .collect::<Vec<_>>();
        rows.sort_by(notification_event_order);
        rows.truncate(limit);
        Ok(rows)
    }

    fn notification_key_event_count(
        &self,
        did: &str,
        reason: &str,
        reason_subject: Option<&str>,
        from_us: u64,
        to_us: u64,
    ) -> Result<usize, StorageError> {
        let inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        Ok(memory_notification_rows(&inner.records, did)
            .filter(|row| {
                row.reason == reason
                    && row.reason_subject.as_deref() == reason_subject
                    && row.time_us >= from_us
                    && row.time_us <= to_us
            })
            .count())
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
        let inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        let mut rows = memory_notification_rows(&inner.records, did)
            .filter(|row| {
                row.reason == reason
                    && row.reason_subject.as_deref() == reason_subject
                    && before.is_none_or(|cursor| row.sorts_before(cursor))
                    && window.from_us.is_none_or(|floor| row.time_us >= floor)
                    && window.to_us.is_none_or(|ceiling| row.time_us <= ceiling)
            })
            .collect::<Vec<_>>();
        rows.sort_by(notification_event_order);
        rows.truncate(limit);
        Ok(rows)
    }

    fn notification_count(&self, did: &str) -> Result<usize, StorageError> {
        let inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        Ok(inner
            .records
            .values()
            .flat_map(notification_edges)
            .filter(|(_, subject_did, _, _)| subject_did == did)
            .count())
    }

    fn notification_last_seen(&self, did: &str) -> Result<u64, StorageError> {
        Ok(self
            .inner
            .lock()
            .map_err(|_| StorageError::Poisoned)?
            .notification_last_seen
            .get(did)
            .copied()
            .unwrap_or_default())
    }

    fn mark_notifications_seen(&self, did: &str, time_us: u64) -> Result<(), StorageError> {
        self.inner
            .lock()
            .map_err(|_| StorageError::Poisoned)?
            .notification_last_seen
            .entry(did.to_owned())
            .and_modify(|last_seen| *last_seen = (*last_seen).max(time_us))
            .or_insert(time_us);
        publish_notification_counts(self, &self.notification_hub, [did.to_owned()]);
        Ok(())
    }

    fn unread_notification_count(&self, did: &str) -> Result<usize, StorageError> {
        let inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        let last_seen = inner
            .notification_last_seen
            .get(did)
            .copied()
            .unwrap_or_default();
        Ok(inner
            .records
            .values()
            .flat_map(notification_edges)
            .filter(|(record, subject_did, _, _)| subject_did == did && record.time_us > last_seen)
            .count())
    }

    fn subscribe_notification_counts(&self, did: &str) -> NotificationSubscription {
        self.notification_hub.subscribe(did)
    }

    fn author_posts(
        &self,
        did: &str,
        cursor: Option<&RecordCursor>,
        limit: usize,
        include_replies: bool,
    ) -> Result<Vec<IndexedRecord>, StorageError> {
        let inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        let mut posts = inner
            .records
            .values()
            .filter(|record| {
                record.collection == "app.bsky.feed.post"
                    && record.did == did
                    && (include_replies || record.record.get("reply").is_none())
                    && record_matches_cursor(record, cursor)
            })
            .cloned()
            .collect::<Vec<_>>();
        posts.sort_by(|left, right| {
            right
                .time_us
                .cmp(&left.time_us)
                .then_with(|| right.uri.cmp(&left.uri))
        });
        posts.truncate(limit.clamp(1, 101));
        Ok(posts)
    }

    fn timeline_posts(
        &self,
        actor: &str,
        cursor: Option<&RecordCursor>,
        limit: usize,
    ) -> Result<Vec<IndexedRecord>, StorageError> {
        let inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        let mut followed = inner
            .follows
            .iter()
            .filter(|(follow_actor, _)| follow_actor == actor)
            .map(|(_, subject)| subject.clone())
            .collect::<HashSet<_>>();
        followed.extend(inner.records.values().filter_map(|record| {
            (record.collection == "app.bsky.graph.follow" && record.did == actor)
                .then(|| record.record.get("subject").and_then(Value::as_str))
                .flatten()
                .map(str::to_owned)
        }));
        let mut posts = inner
            .records
            .values()
            .filter(|record| {
                record.collection == "app.bsky.feed.post"
                    && (record.did == actor || followed.contains(&record.did))
                    && record_matches_cursor(record, cursor)
            })
            .cloned()
            .collect::<Vec<_>>();
        posts.sort_by(|left, right| {
            right
                .time_us
                .cmp(&left.time_us)
                .then_with(|| right.uri.cmp(&left.uri))
        });
        posts.truncate(limit.clamp(1, 101));
        Ok(posts)
    }

    fn timeline_posts_by_post_time(
        &self,
        actor: &str,
        cursor: Option<&SortTimeCursor>,
        limit: usize,
    ) -> Result<Vec<IndexedRecord>, StorageError> {
        #[cfg(test)]
        {
            let delay = self.timeline_post_time_delay_ms.load(Ordering::Relaxed);
            if delay > 0 {
                std::thread::sleep(Duration::from_millis(delay));
            }
            if self
                .force_home_timeline_timeout
                .swap(false, Ordering::Relaxed)
            {
                return Err(StorageError::WebReadTimeout {
                    operation: "home_timeline",
                    elapsed: Duration::from_secs(10),
                });
            }
        }
        let inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        let mut followed = inner
            .follows
            .iter()
            .filter(|(follow_actor, _)| follow_actor == actor)
            .map(|(_, subject)| subject.clone())
            .collect::<HashSet<_>>();
        followed.extend(inner.records.values().filter_map(|record| {
            (record.collection == "app.bsky.graph.follow" && record.did == actor)
                .then(|| record.record.get("subject").and_then(Value::as_str))
                .flatten()
                .map(str::to_owned)
        }));
        let mut posts = inner
            .records
            .values()
            .filter(|record| {
                record.collection == "app.bsky.feed.post"
                    && (record.did == actor || followed.contains(&record.did))
                    && sort_time_matches_cursor(record, post_sort_us(record), cursor)
            })
            .cloned()
            .collect::<Vec<_>>();
        posts.sort_by(|left, right| {
            let left_sort = post_sort_us(left);
            let right_sort = post_sort_us(right);
            right_sort
                .cmp(&left_sort)
                .then_with(|| right.time_us.cmp(&left.time_us))
                .then_with(|| right.uri.cmp(&left.uri))
        });
        posts.truncate(limit.clamp(1, 101));
        Ok(posts)
    }

    fn post_interaction_summary(
        &self,
        uri: &str,
        viewer_did: Option<&str>,
    ) -> Result<PostInteractionSummary, StorageError> {
        let inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        let mut summary = PostInteractionSummary::default();
        for record in inner.records.values() {
            let subject_uri = record
                .record
                .pointer("/subject/uri")
                .and_then(Value::as_str);
            if subject_uri == Some(uri) {
                match record.collection.as_str() {
                    "app.bsky.feed.like" => {
                        summary.like_count += 1;
                        if viewer_did == Some(record.did.as_str()) {
                            summary.viewer_like = Some(record.uri.clone());
                        }
                    }
                    "app.bsky.feed.repost" => {
                        summary.repost_count += 1;
                        if viewer_did == Some(record.did.as_str()) {
                            summary.viewer_repost = Some(record.uri.clone());
                        }
                    }
                    _ => {}
                }
            }
            if record.collection == "app.bsky.feed.post"
                && record
                    .record
                    .pointer("/reply/parent/uri")
                    .and_then(Value::as_str)
                    == Some(uri)
            {
                summary.reply_count += 1;
            }
            if record.collection == "app.bsky.feed.post"
                && record
                    .record
                    .pointer("/embed/record/uri")
                    .and_then(Value::as_str)
                    == Some(uri)
            {
                summary.quote_count += 1;
            }
        }
        Ok(summary)
    }

    fn records(&self) -> Result<Vec<IndexedRecord>, StorageError> {
        Ok(self
            .inner
            .lock()
            .map_err(|_| StorageError::Poisoned)?
            .records
            .values()
            .cloned()
            .collect())
    }

    fn cursor(&self) -> Result<Option<u64>, StorageError> {
        if self.cursor_lock_failures.load(Ordering::SeqCst) > 0 {
            self.cursor_lock_failures.fetch_sub(1, Ordering::SeqCst);
            return Err(StorageError::Sqlite(rusqlite::Error::SqliteFailure(
                rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_BUSY),
                Some("database is locked".to_string()),
            )));
        }
        Ok(self
            .inner
            .lock()
            .map_err(|_| StorageError::Poisoned)?
            .cursor)
    }

    fn set_cursor(&self, cursor: u64) -> Result<(), StorageError> {
        self.inner
            .lock()
            .map_err(|_| StorageError::Poisoned)?
            .cursor = Some(cursor);
        Ok(())
    }

    fn register_user(&self, did: &str) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        if inner.tracked.registered.insert(did.to_owned()) {
            let record_subjects = inner
                .records
                .values()
                .filter_map(|record| {
                    (record.collection == "app.bsky.graph.follow" && record.did == did)
                        .then(|| {
                            record
                                .record
                                .get("subject")
                                .and_then(Value::as_str)
                                .map(str::to_owned)
                        })
                        .flatten()
                })
                .collect::<Vec<_>>();
            for subject in &record_subjects {
                inner.follows.insert((did.to_owned(), subject.clone()));
            }
            let subjects = inner
                .follows
                .iter()
                .filter(|(actor, _)| actor == did)
                .map(|(_, subject)| subject.clone())
                .collect::<Vec<_>>();
            for subject in subjects {
                inner.tracked.apply_follow_delta(&subject, 1);
            }
            for subject in &record_subjects {
                if inner.backfill_queue.contains(subject.as_str())
                    || inner.backfill_queue.len() < MAX_BACKFILL_QUEUE
                {
                    inner
                        .backfill_owners
                        .insert((did.to_owned(), subject.clone()));
                    inner.backfill_queue.insert(subject.clone());
                    inner.backfill_completed.remove(subject.as_str());
                }
            }
        }
        if inner.backfill_queue.contains(did) || inner.backfill_queue.len() < MAX_BACKFILL_QUEUE {
            inner.backfill_queue.insert(did.to_owned());
            inner
                .backfill_owners
                .insert((did.to_owned(), did.to_owned()));
            inner.backfill_completed.remove(did);
        }
        Ok(())
    }

    fn is_registered(&self, did: &str) -> Result<bool, StorageError> {
        Ok(self
            .inner
            .lock()
            .map_err(|_| StorageError::Poisoned)?
            .tracked
            .registered
            .contains(did))
    }

    fn rotate_web_session(&self, did: &str, session_id: &str) -> Result<u64, StorageError> {
        let mut inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        let generation = inner
            .web_session_generations
            .entry((did.to_owned(), session_id.to_owned()))
            .or_default();
        *generation = generation.saturating_add(1);
        Ok(*generation)
    }

    fn web_session_generation(
        &self,
        did: &str,
        session_id: &str,
    ) -> Result<Option<u64>, StorageError> {
        Ok(self
            .inner
            .lock()
            .map_err(|_| StorageError::Poisoned)?
            .web_session_generations
            .get(&(did.to_owned(), session_id.to_owned()))
            .copied())
    }

    fn delete_web_session(&self, did: &str, session_id: &str) -> Result<(), StorageError> {
        self.inner
            .lock()
            .map_err(|_| StorageError::Poisoned)?
            .web_session_generations
            .remove(&(did.to_owned(), session_id.to_owned()));
        Ok(())
    }

    fn unregister_user(&self, did: &str) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        if inner.tracked.registered.contains(did) {
            let subjects = inner
                .follows
                .iter()
                .filter(|(actor, _)| actor == did)
                .map(|(_, subject)| subject.clone())
                .collect::<Vec<_>>();
            for subject in subjects {
                inner.tracked.apply_follow_delta(&subject, -1);
            }
            inner.tracked.registered.remove(did);
        }
        inner.backfill_owners.retain(|(owner, _)| owner != did);
        if !inner
            .follows
            .iter()
            .any(|(actor, subject)| subject == did && inner.tracked.registered.contains(actor))
        {
            inner.backfill_queue.remove(did);
        }
        Ok(())
    }

    fn set_follow(&self, actor: &str, subject: &str, following: bool) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        let edge = (actor.to_owned(), subject.to_owned());
        let was_following = inner.follows.contains(&edge);
        if following {
            inner.follows.insert(edge);
        } else {
            inner.follows.remove(&edge);
            inner
                .backfill_owners
                .remove(&(actor.to_owned(), subject.to_owned()));
        }
        if was_following != following && inner.tracked.registered.contains(actor) {
            inner
                .tracked
                .apply_follow_delta(subject, if following { 1 } else { -1 });
        }
        Ok(())
    }

    fn is_following(&self, actor: &str, subject: &str) -> Result<bool, StorageError> {
        Ok(self
            .inner
            .lock()
            .map_err(|_| StorageError::Poisoned)?
            .follows
            .contains(&(actor.to_owned(), subject.to_owned())))
    }

    fn has_follow_record(&self, actor: &str, subject: &str) -> Result<bool, StorageError> {
        Ok(self
            .inner
            .lock()
            .map_err(|_| StorageError::Poisoned)?
            .records
            .values()
            .any(|record| {
                record.collection == "app.bsky.graph.follow"
                    && record.did == actor
                    && record.record.get("subject").and_then(Value::as_str) == Some(subject)
            }))
    }

    fn tracked_follow_count(&self, did: &str) -> Result<u32, StorageError> {
        Ok(self
            .inner
            .lock()
            .map_err(|_| StorageError::Poisoned)?
            .tracked
            .follow_count(did))
    }

    fn is_followed_by_registered(&self, did: &str) -> Result<bool, StorageError> {
        Ok(self.tracked_follow_count(did)? > 0)
    }

    fn enqueue_backfill(&self, did: &str) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        let tracked = inner.tracked.is_tracked(did);
        if tracked
            && (inner.backfill_queue.contains(did)
                || inner.backfill_queue.len() < MAX_BACKFILL_QUEUE)
        {
            inner.backfill_queue.insert(did.to_owned());
            inner.backfill_completed.remove(did);
        }
        Ok(())
    }

    fn enqueue_backfill_for(&self, owner_did: &str, did: &str) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        if !inner.tracked.registered.contains(owner_did)
            || (did != owner_did
                && !inner
                    .follows
                    .contains(&(owner_did.to_owned(), did.to_owned())))
            || (!inner.backfill_queue.contains(did)
                && inner.backfill_queue.len() >= MAX_BACKFILL_QUEUE)
        {
            return Ok(());
        }
        inner
            .backfill_owners
            .insert((owner_did.to_owned(), did.to_owned()));
        inner.backfill_queue.insert(did.to_owned());
        inner.backfill_completed.remove(did);
        Ok(())
    }

    fn pending_backfills(&self) -> Result<Vec<String>, StorageError> {
        let inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        let now = Instant::now();
        let mut pending = inner
            .backfill_queue
            .iter()
            .filter(|did| {
                inner
                    .backfill_retries
                    .get(*did)
                    .is_none_or(|retry| retry.retry_at <= now)
            })
            .cloned()
            .collect::<Vec<_>>();
        pending.sort();
        pending.truncate(BACKFILL_BATCH_SIZE);
        Ok(pending)
    }

    fn complete_backfill(&self, did: &str) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        inner.backfill_queue.remove(did);
        inner.backfill_retries.remove(did);
        inner.backfill_completed.insert(did.to_owned());
        Ok(())
    }

    fn record_backfill_failure(&self, did: &str) -> Result<Duration, StorageError> {
        let mut inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        let retry = inner
            .backfill_retries
            .entry(did.to_owned())
            .or_insert_with(|| BackfillRetry {
                failures: 0,
                retry_at: Instant::now(),
            });
        retry.failures = retry.failures.saturating_add(1);
        let delay = backfill_retry_delay(retry.failures);
        retry.retry_at = Instant::now() + delay;
        Ok(delay)
    }

    fn backfill_progress(&self, owner_did: &str) -> Result<(usize, usize), StorageError> {
        let inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        let accounts = inner
            .backfill_owners
            .iter()
            .filter(|(owner, _)| owner == owner_did)
            .map(|(_, target)| target)
            .collect::<Vec<_>>();
        let done = accounts
            .iter()
            .filter(|target| inner.backfill_completed.contains(target.as_str()))
            .count();
        Ok((accounts.len(), done))
    }

    fn backfill_failure_count(&self, owner_did: &str) -> Result<usize, StorageError> {
        let inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        Ok(inner
            .backfill_owners
            .iter()
            .filter(|(owner, target)| {
                owner == owner_did
                    && inner.backfill_retries.contains_key(target)
                    && !inner.backfill_completed.contains(target)
            })
            .count())
    }

    fn set_verified_handle(&self, did: &str, handle: &str) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        inner
            .verified_handles
            .insert(did.to_owned(), (handle.to_owned(), Instant::now()));
        inner.invalid_handles.remove(did);
        Ok(())
    }

    fn clear_verified_handle(&self, did: &str) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        inner.verified_handles.remove(did);
        inner.invalid_handles.insert(did.to_owned(), Instant::now());
        Ok(())
    }

    fn verified_handle(&self, did: &str) -> Result<Option<String>, StorageError> {
        let inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        Ok(inner
            .verified_handles
            .get(did)
            .map(|(handle, _)| handle.clone()))
    }

    fn handle_is_invalid(&self, did: &str) -> Result<bool, StorageError> {
        Ok(self
            .inner
            .lock()
            .map_err(|_| StorageError::Poisoned)?
            .invalid_handles
            .contains_key(did))
    }

    fn did_by_verified_handle(&self, handle: &str) -> Result<Option<String>, StorageError> {
        let inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        Ok(inner
            .verified_handles
            .iter()
            .find(|(_, (stored, _))| stored.eq_ignore_ascii_case(handle))
            .map(|(did, _)| did.clone()))
    }

    fn is_tracked_did(&self, did: &str) -> Result<bool, StorageError> {
        let inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        Ok(inner.tracked.is_tracked(did))
    }

    fn registered_followers(&self, did: &str) -> Result<Vec<String>, StorageError> {
        let inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        Ok(inner
            .follows
            .iter()
            .filter(|(actor, subject)| subject == did && inner.tracked.registered.contains(actor))
            .map(|(actor, _)| actor.clone())
            .collect())
    }

    fn is_displayed_did(&self, did: &str) -> Result<bool, StorageError> {
        let inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        Ok(inner.records.values().any(|record| record.did == did))
    }

    fn stale_handle_dids(&self, ttl: Duration, limit: usize) -> Result<Vec<String>, StorageError> {
        let inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        let is_tracked = |did: &str| {
            inner.tracked.registered.contains(did)
                || inner.records.values().any(|record| record.did == did)
                || inner.tracked.follow_count(did) > 0
        };
        let mut dids = inner
            .verified_handles
            .iter()
            .filter(|(did, (_, verified_at))| verified_at.elapsed() >= ttl && is_tracked(did))
            .map(|(did, _)| did.clone())
            .collect::<Vec<_>>();
        dids.extend(
            inner
                .invalid_handles
                .iter()
                .filter(|(did, checked_at)| checked_at.elapsed() >= ttl && is_tracked(did))
                .map(|(did, _)| did.clone()),
        );
        dids.sort();
        dids.dedup();
        dids.truncate(limit.max(1));
        Ok(dids)
    }

    fn queue_handle_refresh(&self, did: &str) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        if inner.handle_refresh_queue.insert(did.to_owned()) {
            inner.handle_refresh_order.push_back(did.to_owned());
        }
        Ok(())
    }

    fn queue_handle_refresh_ordered(&self, dids: &[String]) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        let mut seen = HashSet::new();
        let mut ordered = dids
            .iter()
            .filter(|did| seen.insert((*did).clone()))
            .cloned()
            .collect::<Vec<_>>();
        let prioritized = ordered.iter().cloned().collect::<HashSet<_>>();
        let mut remainder = inner
            .handle_refresh_order
            .iter()
            .filter(|did| !prioritized.contains(*did))
            .cloned()
            .collect::<Vec<_>>();
        for did in &ordered {
            inner.handle_refresh_queue.insert(did.clone());
        }
        ordered.append(&mut remainder);
        inner.handle_refresh_order = ordered.into();
        Ok(())
    }

    fn pending_handle_refreshes(&self, limit: usize) -> Result<Vec<String>, StorageError> {
        let inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        Ok(inner
            .handle_refresh_order
            .iter()
            .take(limit.max(1))
            .cloned()
            .collect())
    }

    fn complete_handle_refresh(&self, did: &str) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        inner.handle_refresh_queue.remove(did);
        inner.handle_refresh_order.retain(|queued| queued != did);
        Ok(())
    }

    fn bytes_on_disk(&self) -> u64 {
        0
    }
}

/// Records per write transaction in a repository snapshot.
const SNAPSHOT_BATCH: usize = 500;
const SQLITE_READ_CONNECTIONS: usize = 4;

/// The index/scan work one read query's statements performed, read from
/// rusqlite's per-statement counters (`Statement::get_status`).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct StatementWork {
    pub fullscan_steps: u64,
    pub vm_steps: u64,
}

impl StatementWork {
    /// Adds what the statement did since the last observation: `reset_status`
    /// returns the counter and zeroes it, so a statement run several times in
    /// one call is counted exactly once per run.
    fn observe(&mut self, statement: &rusqlite::Statement<'_>) {
        self.fullscan_steps += statement
            .reset_status(rusqlite::StatementStatus::FullscanStep)
            .max(0) as u64;
        self.vm_steps += statement
            .reset_status(rusqlite::StatementStatus::VmStep)
            .max(0) as u64;
    }
}

type FireCompletion = Box<dyn FnOnce() + Send>;
type FireWrite =
    Box<dyn FnOnce(&Connection) -> Result<Option<FireCompletion>, StorageError> + Send>;
type BlockingWrite = Box<dyn FnOnce(&mut Connection) + Send>;

enum SqliteWriterMessage {
    Fire(&'static str, FireWrite),
    Blocking(&'static str, BlockingWrite),
    Urgent(&'static str, BlockingWrite),
}

#[derive(Default)]
struct StoragePressure {
    waiting_reads: AtomicU64,
    waiting_live_ingest: AtomicU64,
}

impl StoragePressure {
    fn is_active(&self) -> bool {
        self.waiting_reads.load(Ordering::Relaxed) > 0
            || self.waiting_live_ingest.load(Ordering::Relaxed) > 0
    }
}

#[derive(Default)]
struct SqliteWriterMetrics {
    queue_depth: AtomicU64,
    queue_depth_max: AtomicU64,
    dropped: AtomicU64,
    failed_writes: AtomicU64,
    batch_count: AtomicU64,
    transaction_count: AtomicU64,
    transaction_total_ns: AtomicU64,
    transaction_max_ns: AtomicU64,
    sqlite_busy: AtomicU64,
    slow_txn: AtomicU64,
    jetstream_lock_retries: AtomicU64,
    /// The recent-posts worker's cumulative counts, shared with `SqliteStorage`
    /// so the minute line can carry them; swapped to the storage's counters
    /// once at open (the default is a private scratch instance).
    recent_posts: Mutex<Arc<RecentPostsCounters>>,
}

#[derive(Default)]
struct WalMetrics {
    file_bytes: AtomicU64,
    file_bytes_max: AtomicU64,
    wal_generation: AtomicU64,
    frames: AtomicU64,
    checkpointed: AtomicU64,
    reset: AtomicBool,
    resets: AtomicU64,
    backfill_wait_ns: AtomicU64,
    sqlite_busy: AtomicU64,
    slow_txn: AtomicU64,
    ckpt_passive_max_ns: AtomicU64,
    ckpt_truncate_max_ns: AtomicU64,
    wal_force_shrink_attempts: AtomicU64,
    wal_force_shrink_ok: AtomicU64,
    wal_force_shrink_busy: AtomicU64,
    wal_force_shrink_ms_max_ns: AtomicU64,
    wal_force_shrink_bytes_before: AtomicU64,
    wal_force_shrink_bytes_after: AtomicU64,
    active_checkpoint: AtomicUsize,
    passive_checkpoint_generation: AtomicU64,
    truncate_checkpoint_generation: AtomicU64,
}

#[derive(Clone, Copy)]
struct CheckpointObservation {
    active_checkpoint: usize,
    passive_generation: u64,
    truncate_generation: u64,
}

struct CheckpointActivity<'a> {
    metrics: &'a WalMetrics,
}

impl WalMetrics {
    fn checkpoint_observation(&self) -> CheckpointObservation {
        CheckpointObservation {
            active_checkpoint: self.active_checkpoint.load(Ordering::Acquire),
            passive_generation: self.passive_checkpoint_generation.load(Ordering::Acquire),
            truncate_generation: self.truncate_checkpoint_generation.load(Ordering::Acquire),
        }
    }

    fn checkpoint_activity(&self, kind: usize) -> CheckpointActivity<'_> {
        match kind {
            CHECKPOINT_PASSIVE => {
                self.passive_checkpoint_generation
                    .fetch_add(1, Ordering::Release);
            }
            CHECKPOINT_TRUNCATE => {
                self.truncate_checkpoint_generation
                    .fetch_add(1, Ordering::Release);
            }
            _ => unreachable!("unknown checkpoint kind"),
        }
        self.active_checkpoint.store(kind, Ordering::Release);
        CheckpointActivity { metrics: self }
    }

    fn record_checkpoint(
        &self,
        path: &Path,
        file_bytes: u64,
        frames: u64,
        checkpointed: u64,
        reset: bool,
    ) {
        let reset_observed = self.observe_file(path, file_bytes, reset);
        self.frames.store(frames, Ordering::Relaxed);
        self.checkpointed.store(checkpointed, Ordering::Relaxed);
        self.reset.fetch_or(reset_observed, Ordering::Release);
    }

    fn record_skipped_checkpoint(&self, path: &Path, file_bytes: u64) {
        let reset_observed = self.observe_file(path, file_bytes, false);
        self.reset.fetch_or(reset_observed, Ordering::Release);
    }

    fn observe_file(&self, path: &Path, file_bytes: u64, reported_reset: bool) -> bool {
        let previous = self.file_bytes.load(Ordering::Acquire);
        let size_reset = previous > file_bytes;
        let generation_reset = wal_file_generation(path).is_some_and(|generation| {
            let old = self.wal_generation.swap(generation, Ordering::Relaxed);
            old != 0 && old != generation
        });
        let reset_observed = size_reset || generation_reset || reported_reset;
        if reset_observed {
            self.resets.fetch_add(1, Ordering::Release);
        }
        // Publish the smaller size only after the minute counter includes the
        // reset, so a report cannot pair a shrunk WAL with a zero reset count.
        self.file_bytes.store(file_bytes, Ordering::Release);
        self.file_bytes_max.fetch_max(file_bytes, Ordering::Relaxed);
        reset_observed
    }
}

impl Drop for CheckpointActivity<'_> {
    fn drop(&mut self) {
        self.metrics
            .active_checkpoint
            .store(CHECKPOINT_NONE, Ordering::Release);
    }
}

impl SqliteWriterMetrics {
    fn queued(&self) {
        let depth = self.queue_depth.fetch_add(1, Ordering::Relaxed) + 1;
        let mut maximum = self.queue_depth_max.load(Ordering::Relaxed);
        while depth > maximum {
            match self.queue_depth_max.compare_exchange_weak(
                maximum,
                depth,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => maximum = actual,
            }
        }
    }

    fn dequeued(&self) {
        self.queue_depth.fetch_sub(1, Ordering::Relaxed);
    }

    fn record_batch(&self, kind: &'static str, elapsed: Duration) {
        let elapsed_ns = elapsed.as_nanos().min(u128::from(u64::MAX)) as u64;
        self.batch_count.fetch_add(1, Ordering::Relaxed);
        self.transaction_count.fetch_add(1, Ordering::Relaxed);
        self.transaction_total_ns
            .fetch_add(elapsed_ns, Ordering::Relaxed);
        self.transaction_max_ns
            .fetch_max(elapsed_ns, Ordering::Relaxed);
        if elapsed >= SQLITE_SLOW_WRITE_THRESHOLD {
            self.slow_txn.fetch_add(1, Ordering::Relaxed);
            tracing::warn!(
                kind,
                duration_ms = elapsed.as_millis() as u64,
                "slow SQLite writer transaction"
            );
        }
    }

    fn record_busy(&self, kind: &'static str, error: &StorageError, lock_holder: &str) {
        self.sqlite_busy.fetch_add(1, Ordering::Relaxed);
        tracing::warn!(
            kind,
            lock_holder,
            error = %error,
            "SQLite writer received SQLITE_BUSY"
        );
    }

    fn record_failed_write(&self) {
        self.failed_writes.fetch_add(1, Ordering::Relaxed);
    }

    fn report(&self, read_metrics: &ReadPoolMetrics, wal_metrics: &WalMetrics) {
        let outbound_host_calls = crate::net::take_outbound_host_metrics();
        let count = self.transaction_count.swap(0, Ordering::Relaxed);
        let total_ns = self.transaction_total_ns.swap(0, Ordering::Relaxed);
        let max_ns = self.transaction_max_ns.swap(0, Ordering::Relaxed);
        let sqlite_busy = self.sqlite_busy.swap(0, Ordering::Relaxed)
            + wal_metrics.sqlite_busy.swap(0, Ordering::Relaxed);
        let slow_txn = self.slow_txn.swap(0, Ordering::Relaxed)
            + wal_metrics.slow_txn.swap(0, Ordering::Relaxed);
        let (recent_posts_queued, recent_posts_done, recent_posts_failed, recent_posts_dropped) =
            self.recent_posts
                .lock()
                .map(|counters| counters.snapshot())
                .unwrap_or_default();
        tracing::info!(
            writer_queue_depth_max = self
                .queue_depth_max
                .swap(self.queue_depth.load(Ordering::Relaxed), Ordering::Relaxed,),
            batch_count = self.batch_count.swap(0, Ordering::Relaxed),
            transaction_mean_ms = if count == 0 {
                0.0
            } else {
                total_ns as f64 / count as f64 / 1_000_000.0
            },
            transaction_max_ms = max_ns as f64 / 1_000_000.0,
            sqlite_busy,
            slow_txn,
            ckpt_passive_ms_max =
                wal_metrics.ckpt_passive_max_ns.swap(0, Ordering::Relaxed) as f64 / 1_000_000.0,
            ckpt_truncate_ms_max =
                wal_metrics.ckpt_truncate_max_ns.swap(0, Ordering::Relaxed) as f64 / 1_000_000.0,
            wal_force_shrink_attempts =
                wal_metrics.wal_force_shrink_attempts.swap(0, Ordering::Relaxed),
            wal_force_shrink_ok = wal_metrics.wal_force_shrink_ok.swap(0, Ordering::Relaxed),
            wal_force_shrink_busy =
                wal_metrics.wal_force_shrink_busy.swap(0, Ordering::Relaxed),
            wal_force_shrink_ms_max = wal_metrics
                .wal_force_shrink_ms_max_ns
                .swap(0, Ordering::Relaxed) as f64
                / 1_000_000.0,
            wal_bytes_before =
                wal_metrics.wal_force_shrink_bytes_before.load(Ordering::Relaxed),
            wal_bytes_after =
                wal_metrics.wal_force_shrink_bytes_after.load(Ordering::Relaxed),
            jetstream_lock_retries = self.jetstream_lock_retries.swap(0, Ordering::Relaxed),
            dropped_count = self.dropped.swap(0, Ordering::Relaxed),
            failed_write_count = self.failed_writes.swap(0, Ordering::Relaxed),
            read_wait_count = read_metrics.waits.swap(0, Ordering::Relaxed),
            read_wait_max_ms =
                read_metrics.wait_max_ns.swap(0, Ordering::Relaxed) as f64 / 1_000_000.0,
            wal_bytes = wal_metrics.file_bytes.load(Ordering::Acquire),
            wal_file_bytes_max = wal_metrics.file_bytes_max.swap(0, Ordering::Relaxed),
            wal_frames = wal_metrics.frames.load(Ordering::Relaxed),
            ckpt_frames = wal_metrics.checkpointed.load(Ordering::Relaxed),
            wal_reset = wal_metrics.reset.swap(false, Ordering::AcqRel),
            wal_resets = wal_metrics.resets.swap(0, Ordering::AcqRel),
            backfill_wait_ms =
                wal_metrics.backfill_wait_ns.swap(0, Ordering::Relaxed) as f64 / 1_000_000.0,
            recent_posts_queued,
            recent_posts_done,
            recent_posts_failed,
            recent_posts_dropped,
            future_hidden = take_future_posts_hidden(),
            web_read_timeout_count = WEB_READ_TIMEOUT_COUNT.load(Ordering::Relaxed),
            outbound_host_calls = %outbound_host_calls,
            "SQLite writer minute metrics"
        );
    }
}

/// Head-of-line pressure on the read connection pool: how many reads could not
/// take the connection the round-robin picked first (and had to search or
/// block), and the longest such wait. The counters are zeroed by the per-minute
/// `SQLite writer minute metrics` line, which carries both.
#[derive(Default)]
pub struct ReadPoolMetrics {
    waits: AtomicU64,
    wait_max_ns: AtomicU64,
}

impl ReadPoolMetrics {
    fn record_wait(&self, waited: Duration) {
        self.waits.fetch_add(1, Ordering::Relaxed);
        self.wait_max_ns.fetch_max(
            u64::try_from(waited.as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }

    /// Reads that had to wait for a connection since the last metrics line.
    pub fn waits(&self) -> u64 {
        self.waits.load(Ordering::Relaxed)
    }

    /// The longest such wait, in ms, since the last metrics line.
    pub fn wait_max_ms(&self) -> f64 {
        self.wait_max_ns.load(Ordering::Relaxed) as f64 / 1_000_000.0
    }
}

struct SqliteWriter {
    sender: Option<SyncSender<SqliteWriterMessage>>,
    worker: Option<std::thread::JoinHandle<()>>,
    metrics: Arc<SqliteWriterMetrics>,
    pressure: Arc<StoragePressure>,
    wal_metrics: Arc<WalMetrics>,
}

impl SqliteWriter {
    fn new(
        connection: Connection,
        read_metrics: Arc<ReadPoolMetrics>,
        wal_metrics: Arc<WalMetrics>,
        pressure: Arc<StoragePressure>,
    ) -> Result<Self, StorageError> {
        let (sender, receiver) = mpsc::sync_channel(SQLITE_WRITER_QUEUE);
        let metrics = Arc::new(SqliteWriterMetrics::default());
        let thread_metrics = Arc::clone(&metrics);
        let thread_wal_metrics = Arc::clone(&wal_metrics);
        let thread_pressure = Arc::clone(&pressure);
        let worker = std::thread::Builder::new()
            .name("sqlite-writer".to_owned())
            .spawn(move || {
                sqlite_writer_loop(
                    connection,
                    receiver,
                    thread_metrics,
                    read_metrics,
                    thread_wal_metrics,
                    thread_pressure,
                )
            })?;
        Ok(Self {
            sender: Some(sender),
            worker: Some(worker),
            metrics,
            pressure,
            wal_metrics,
        })
    }

    /// Points the minute metrics line at the recent-posts counters shared with
    /// `SqliteStorage`. Called once at open, before any report can run; the
    /// writer thread reads through this same `Arc<SqliteWriterMetrics>`.
    fn share_recent_posts_counters(&self, counters: Arc<RecentPostsCounters>) {
        if let Ok(mut recent) = self.metrics.recent_posts.lock() {
            *recent = counters;
        }
    }

    fn call<R: Send + 'static>(
        &self,
        operation: impl FnOnce(&mut Connection) -> Result<R, StorageError> + Send + 'static,
    ) -> Result<R, StorageError> {
        let kind = current_sqlite_operation_kind("writer_call");
        let wal_metrics = Arc::clone(&self.wal_metrics);
        let (reply, result) = mpsc::sync_channel(1);
        let command = SqliteWriterMessage::Blocking(
            kind,
            Box::new(move |connection| {
                let observed = wal_metrics.checkpoint_observation();
                let _ = reply.send((operation(connection), observed));
            }),
        );
        self.metrics.queued();
        let Some(sender) = self.sender.as_ref() else {
            self.metrics.dequeued();
            return Err(StorageError::WriterStopped);
        };
        if sender.send(command).is_err() {
            self.metrics.dequeued();
            return Err(StorageError::WriterStopped);
        }
        let (result, observed) = result.recv().map_err(|_| StorageError::WriterStopped)?;
        if let Err(error) = &result
            && is_sqlite_busy_error(error)
        {
            self.metrics.record_busy(
                kind,
                error,
                checkpoint_lock_holder_since(&self.wal_metrics, observed),
            );
        }
        result
    }

    fn call_live<R: Send + 'static>(
        &self,
        operation: impl FnOnce(&mut Connection) -> Result<R, StorageError> + Send + 'static,
    ) -> Result<R, StorageError> {
        let kind = current_sqlite_operation_kind("ingest_batch");
        let wal_metrics = Arc::clone(&self.wal_metrics);
        let (reply, result) = mpsc::sync_channel(1);
        let command = SqliteWriterMessage::Urgent(
            kind,
            Box::new(move |connection| {
                let observed = wal_metrics.checkpoint_observation();
                let _ = reply.send((operation(connection), observed));
            }),
        );
        self.metrics.queued();
        self.pressure
            .waiting_live_ingest
            .fetch_add(1, Ordering::Relaxed);
        let Some(sender) = self.sender.as_ref() else {
            self.metrics.dequeued();
            self.pressure
                .waiting_live_ingest
                .fetch_sub(1, Ordering::Relaxed);
            return Err(StorageError::WriterStopped);
        };
        if sender.send(command).is_err() {
            self.metrics.dequeued();
            self.pressure
                .waiting_live_ingest
                .fetch_sub(1, Ordering::Relaxed);
            return Err(StorageError::WriterStopped);
        }
        let (result, observed) = result.recv().map_err(|_| StorageError::WriterStopped)?;
        if let Err(error) = &result
            && is_sqlite_busy_error(error)
        {
            self.metrics.record_busy(
                kind,
                error,
                checkpoint_lock_holder_since(&self.wal_metrics, observed),
            );
        }
        result
    }

    /// Queues a fire-and-forget write. Returns whether it was enqueued; a caller
    /// that tracks the write (e.g. the post-count fill dedup set) must undo its
    /// bookkeeping when this returns `false`.
    fn fire(
        &self,
        operation: impl FnOnce(&Connection) -> Result<Option<FireCompletion>, StorageError>
        + Send
        + 'static,
    ) -> bool {
        let kind = current_sqlite_operation_kind("queued_write");
        let Some(sender) = self.sender.as_ref() else {
            self.metrics.dropped.fetch_add(1, Ordering::Relaxed);
            return false;
        };
        self.metrics.queued();
        match sender.try_send(SqliteWriterMessage::Fire(kind, Box::new(operation))) {
            Ok(()) => true,
            Err(error) => {
                self.metrics.dequeued();
                match error {
                    mpsc::TrySendError::Full(_) => {
                        self.metrics.dropped.fetch_add(1, Ordering::Relaxed);
                        tracing::debug!("SQLite writer queue full; dropped request-path write");
                    }
                    mpsc::TrySendError::Disconnected(_) => {
                        self.metrics.dropped.fetch_add(1, Ordering::Relaxed);
                        tracing::warn!("SQLite writer stopped; dropped request-path write");
                    }
                }
                false
            }
        }
    }

    fn flush(&self) -> Result<(), StorageError> {
        self.call(|_| Ok(()))
    }

    fn shutdown(&mut self) {
        drop(self.sender.take());
        if let Some(worker) = self.worker.take()
            && worker.join().is_err()
        {
            tracing::error!("SQLite writer thread panicked during shutdown");
        }
    }
}

impl Drop for SqliteWriter {
    fn drop(&mut self) {
        self.shutdown();
    }
}

fn is_sqlite_busy_error(error: &StorageError) -> bool {
    let message = error.to_string().to_ascii_lowercase();
    message.contains("database is locked")
        || message.contains("database is busy")
        || message.contains("database table is locked")
}

fn checkpoint_lock_holder_since(
    wal_metrics: &WalMetrics,
    observed: CheckpointObservation,
) -> &'static str {
    match wal_metrics.active_checkpoint.load(Ordering::Acquire) {
        CHECKPOINT_PASSIVE => "checkpoint_PASSIVE",
        CHECKPOINT_TRUNCATE => "checkpoint_TRUNCATE",
        _ if observed.active_checkpoint == CHECKPOINT_PASSIVE => "checkpoint_PASSIVE",
        _ if observed.active_checkpoint == CHECKPOINT_TRUNCATE => "checkpoint_TRUNCATE",
        _ if wal_metrics
            .truncate_checkpoint_generation
            .load(Ordering::Acquire)
            != observed.truncate_generation =>
        {
            "checkpoint_TRUNCATE"
        }
        _ if wal_metrics
            .passive_checkpoint_generation
            .load(Ordering::Acquire)
            != observed.passive_generation =>
        {
            "checkpoint_PASSIVE"
        }
        _ => "unknown_or_external_writer",
    }
}

fn sqlite_writer_loop(
    mut connection: Connection,
    receiver: Receiver<SqliteWriterMessage>,
    metrics: Arc<SqliteWriterMetrics>,
    read_metrics: Arc<ReadPoolMetrics>,
    wal_metrics: Arc<WalMetrics>,
    pressure: Arc<StoragePressure>,
) {
    let mut next_metrics = Instant::now() + SQLITE_WRITER_METRICS_INTERVAL;
    let mut pending = VecDeque::new();
    loop {
        while let Ok(message) = receiver.try_recv() {
            pending.push_back(message);
        }
        let urgent_position = pending
            .iter()
            .position(|message| matches!(message, SqliteWriterMessage::Urgent(_, _)));
        let message = if let Some(position) = urgent_position {
            pending.remove(position)
        } else {
            pending.pop_front()
        };
        let Some(message) = message else {
            match receiver.recv_timeout(next_metrics.saturating_duration_since(Instant::now())) {
                Ok(message) => pending.push_back(message),
                Err(RecvTimeoutError::Timeout) => {
                    metrics.report(&read_metrics, &wal_metrics);
                    next_metrics = Instant::now() + SQLITE_WRITER_METRICS_INTERVAL;
                }
                Err(RecvTimeoutError::Disconnected) => break,
            }
            continue;
        };
        metrics.dequeued();
        match message {
            SqliteWriterMessage::Blocking(kind, operation) => {
                let started = Instant::now();
                operation(&mut connection);
                metrics.record_batch(kind, started.elapsed());
            }
            SqliteWriterMessage::Urgent(kind, operation) => {
                pressure.waiting_live_ingest.fetch_sub(1, Ordering::Relaxed);
                let started = Instant::now();
                operation(&mut connection);
                metrics.record_batch(kind, started.elapsed());
            }
            SqliteWriterMessage::Fire(first_kind, first) => {
                let mut writes = vec![first];
                let mut batch_kind = first_kind;
                let deadline = Instant::now() + SQLITE_WRITER_BATCH_DELAY;
                while writes.len() < SQLITE_WRITER_BATCH_SIZE {
                    match receiver.try_recv() {
                        Ok(SqliteWriterMessage::Fire(kind, write)) => {
                            metrics.dequeued();
                            if batch_kind != kind {
                                batch_kind = "mixed_queued_write_batch";
                            }
                            writes.push(write);
                        }
                        Ok(blocking @ SqliteWriterMessage::Blocking(_, _)) => {
                            pending.push_back(blocking);
                            break;
                        }
                        Ok(SqliteWriterMessage::Urgent(kind, operation)) => {
                            metrics.dequeued();
                            pressure.waiting_live_ingest.fetch_sub(1, Ordering::Relaxed);
                            let started = Instant::now();
                            operation(&mut connection);
                            metrics.record_batch(kind, started.elapsed());
                        }
                        Err(TryRecvError::Empty) if Instant::now() < deadline => {
                            std::thread::sleep(Duration::from_micros(100));
                        }
                        Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
                    }
                }
                let checkpoint_observed = wal_metrics.checkpoint_observation();
                let started = Instant::now();
                let mut completions = Vec::new();
                let mut batch_error = None;
                match connection.transaction() {
                    Ok(transaction) => {
                        for write in writes {
                            if let Err(error) = transaction.execute_batch("SAVEPOINT queued_write")
                            {
                                metrics.record_failed_write();
                                batch_error = Some(StorageError::Sqlite(error));
                                break;
                            }
                            match write(&transaction) {
                                Ok(completion) => {
                                    if let Err(error) =
                                        transaction.execute_batch("RELEASE SAVEPOINT queued_write")
                                    {
                                        metrics.record_failed_write();
                                        batch_error = Some(StorageError::Sqlite(error));
                                        break;
                                    }
                                    if let Some(completion) = completion {
                                        completions.push(completion);
                                    }
                                }
                                Err(error) => {
                                    metrics.record_failed_write();
                                    if is_sqlite_busy_error(&error) {
                                        metrics.record_busy(
                                            batch_kind,
                                            &error,
                                            checkpoint_lock_holder_since(
                                                &wal_metrics,
                                                checkpoint_observed,
                                            ),
                                        );
                                    }
                                    tracing::warn!(%error, "SQLite queued write failed");
                                    if let Err(rollback_error) = transaction.execute_batch(
                                        "ROLLBACK TO SAVEPOINT queued_write;
                                         RELEASE SAVEPOINT queued_write;",
                                    ) {
                                        batch_error = Some(StorageError::Sqlite(rollback_error));
                                        break;
                                    }
                                }
                            }
                        }
                        if batch_error.is_none()
                            && let Err(error) = transaction.commit()
                        {
                            let error = StorageError::Sqlite(error);
                            if is_sqlite_busy_error(&error) {
                                metrics.record_busy(
                                    batch_kind,
                                    &error,
                                    checkpoint_lock_holder_since(&wal_metrics, checkpoint_observed),
                                );
                            }
                            tracing::warn!(%error, "SQLite queued write batch commit failed");
                            batch_error = Some(error);
                        }
                    }
                    Err(error) => {
                        let error = StorageError::Sqlite(error);
                        if is_sqlite_busy_error(&error) {
                            metrics.record_busy(
                                batch_kind,
                                &error,
                                checkpoint_lock_holder_since(&wal_metrics, checkpoint_observed),
                            );
                        }
                        tracing::warn!(%error, "SQLite queued write batch could not start");
                        batch_error = Some(error);
                    }
                }
                if batch_error.is_none() {
                    for completion in completions {
                        completion();
                    }
                } else if let Some(error) = batch_error {
                    tracing::warn!(%error, "SQLite queued write batch rolled back");
                }
                metrics.record_batch(batch_kind, started.elapsed());
            }
        }
        if Instant::now() >= next_metrics {
            metrics.report(&read_metrics, &wal_metrics);
            next_metrics = Instant::now() + SQLITE_WRITER_METRICS_INTERVAL;
        }
    }
}

/// (collection, JSON path, pinned expression index) for each counted relation.
/// Every pinned index leads with (collection, json_extract(...)), so a batched
/// count performs one index seek per requested post and never walks the whole
/// records table: one statement per relation for the WHOLE batch of uncounted
/// posts, not four queries per post.
const POST_COUNT_RELATIONS: [(&str, &str, &str); 4] = [
    (
        "app.bsky.feed.like",
        "$.subject.uri",
        "records_collection_subject_uri",
    ),
    (
        "app.bsky.feed.repost",
        "$.subject.uri",
        "records_collection_subject_uri",
    ),
    (
        "app.bsky.feed.post",
        "$.reply.parent.uri",
        "records_collection_parent_uri",
    ),
    (
        "app.bsky.feed.post",
        "$.embed.record.uri",
        "records_collection_embed_record_uri",
    ),
];

/// Counts the four interactions of every listed post in one pass per relation
/// (four statements for the whole batch), on whatever connection is given — a
/// read-pool connection for page rendering, the writer connection for the fill
/// that re-checks inside its own transaction.
fn compute_post_counts(
    connection: &Connection,
    uris: &[String],
) -> Result<(HashMap<String, [usize; 4]>, StatementWork), StorageError> {
    let mut counts = uris
        .iter()
        .map(|uri| (uri.clone(), [0_usize; 4]))
        .collect::<HashMap<_, _>>();
    if uris.is_empty() {
        return Ok((counts, StatementWork::default()));
    }
    let batch = serde_json::to_string(uris)?;
    let mut work = StatementWork::default();
    for (index, (collection, path, index_name)) in POST_COUNT_RELATIONS.into_iter().enumerate() {
        let mut statement = connection.prepare(&format!(
            "SELECT json_extract(record_json, '{path}'), COUNT(*) FROM records
             INDEXED BY {index_name}
             WHERE collection=?1
               AND json_extract(record_json, '{path}') IN (SELECT value FROM json_each(?2))
             GROUP BY 1"
        ))?;
        let rows = statement
            .query_map(params![collection, batch], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        work.observe(&statement);
        for (subject_uri, count) in rows {
            if let Some(entry) = counts.get_mut(&subject_uri) {
                entry[index] = usize::try_from(count).unwrap_or(0);
            }
        }
    }
    Ok((counts, work))
}

/// The newest like (or repost, per `collection`) the viewer made against each
/// listed post, in one statement for the whole batch. Same per-activity
/// semantics as the per-post lookup: the newest row wins.
fn viewer_interaction_uris(
    connection: &Connection,
    uris: &[String],
    viewer_did: &str,
    collection: &str,
) -> Result<HashMap<String, String>, StorageError> {
    if uris.is_empty() {
        return Ok(HashMap::new());
    }
    let batch = serde_json::to_string(uris)?;
    let mut statement = connection.prepare(
        "SELECT subject_uri, uri FROM (
             SELECT json_extract(record_json, '$.subject.uri') AS subject_uri, uri,
                    ROW_NUMBER() OVER (
                        PARTITION BY json_extract(record_json, '$.subject.uri')
                        ORDER BY time_us DESC, uri DESC
                    ) AS newest
             FROM records INDEXED BY records_author_subject_uri
             WHERE collection=?1 AND did=?2
               AND json_extract(record_json, '$.subject.uri') IN (SELECT value FROM json_each(?3))
         ) WHERE newest=1",
    )?;
    statement
        .query_map(params![collection, viewer_did, batch], |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
        })?
        .collect::<Result<HashMap<_, _>, _>>()
        .map_err(Into::into)
}

/// Records the SQL of every statement that finishes executing on the thread
/// that installed the trace (or on the writer thread, via
/// `trace_writer_statements`). Kept public like `hold_writer` for regression
/// tests that assert what a code path asks the database — for example that a
/// render no longer runs a `profile_counts` statement per card. SQLite's
/// trace callback is a plain `fn` pointer in rusqlite, so the sink travels in
/// a thread-local instead of a captured closure.
#[derive(Clone, Default)]
pub struct StatementTrace {
    statements: Arc<Mutex<Vec<String>>>,
}

impl StatementTrace {
    /// The SQL that only `profile_counts` runs: one SELECT bundling the three
    /// COUNT subqueries. Counting its executions counts the profile-count
    /// queries a code path asks the database for. Ends before the `?1`
    /// placeholder so it matches the expanded (values-inlined) SQL too.
    pub const PROFILE_COUNTS_SQL: &'static str =
        "(SELECT COUNT(*) FROM records WHERE collection='app.bsky.feed.post' AND did=";

    fn push(&self, sql: &str) {
        if let Ok(mut statements) = self.statements.lock() {
            statements.push(sql.to_owned());
        }
    }

    /// Drops everything recorded so far, so the next `count_containing` only
    /// sees statements executed after this call.
    pub fn clear(&self) {
        if let Ok(mut statements) = self.statements.lock() {
            statements.clear();
        }
    }

    /// Executed statements whose SQL contains `needle`.
    pub fn count_containing(&self, needle: &str) -> usize {
        self.statements
            .lock()
            .map(|statements| statements.iter().filter(|sql| sql.contains(needle)).count())
            .unwrap_or_default()
    }

    /// Executed statements whose SQL contains every needle at once (expanded
    /// SQL carries the bound values, so shape plus argument identifies one).
    pub fn count_containing_all(&self, needles: &[&str]) -> usize {
        self.statements
            .lock()
            .map(|statements| {
                statements
                    .iter()
                    .filter(|sql| needles.iter().all(|needle| sql.contains(needle)))
                    .count()
            })
            .unwrap_or_default()
    }

    pub fn statements(&self) -> Vec<String> {
        self.statements
            .lock()
            .map(|s| s.clone())
            .unwrap_or_default()
    }
}

thread_local! {
    static STATEMENT_TRACE_SINK: RefCell<Option<StatementTrace>> = const { RefCell::new(None) };
}

struct StatementTraceSinkGuard(Option<StatementTrace>);

impl StatementTraceSinkGuard {
    fn install(trace: Option<StatementTrace>) -> Self {
        Self(STATEMENT_TRACE_SINK.with(|sink| sink.replace(trace)))
    }
}

impl Drop for StatementTraceSinkGuard {
    fn drop(&mut self) {
        STATEMENT_TRACE_SINK.with(|sink| sink.replace(self.0.take()));
    }
}

fn push_traced_statement(sql: &str) {
    STATEMENT_TRACE_SINK.with(|sink| {
        if let Some(sink) = sink.borrow().as_ref() {
            sink.push(sql);
        }
    });
}

fn route_statement_profile(event: rusqlite::trace::TraceEvent<'_>) {
    // PROFILE fires exactly once per executed statement; STMT can fire again
    // inside triggers, so counting PROFILE keeps one entry per execution.
    // The expanded SQL (bound values inlined) is recorded when available so
    // tests can tell statements apart by their arguments, not just shape.
    if let rusqlite::trace::TraceEvent::Profile(statement, _) = event {
        let sql = statement
            .expanded_sql()
            .unwrap_or_else(|| statement.sql().into_owned());
        push_traced_statement(&sql);
    }
}

pub struct SqliteStorage {
    writer: SqliteWriter,
    read_connections: Vec<Mutex<Connection>>,
    next_read_connection: AtomicU64,
    read_metrics: Arc<ReadPoolMetrics>,
    pressure: Arc<StoragePressure>,
    path: PathBuf,
    checkpointer_stop: Arc<AtomicBool>,
    checkpointer_worker: Option<std::thread::JoinHandle<()>>,
    checkpoint_backlog: Arc<AtomicU64>,
    wal_metrics: Arc<WalMetrics>,
    tracked: Arc<RwLock<TrackedSet>>,
    profile_index: Arc<RwLock<HashMap<String, ProfileIndexEntry>>>,
    recent_posts_status: Arc<RwLock<HashMap<String, (i64, i64)>>>,
    recent_posts_queue: Arc<RwLock<(VecDeque<String>, HashSet<String>)>>,
    recent_posts_counters: Arc<RecentPostsCounters>,
    notification_hub: NotificationHub,
    pending_counts_fills: Arc<RwLock<HashSet<String>>>,
    pending_record_writes: Arc<RwLock<HashMap<PendingRecordKey, IndexedRecord>>>,
    post_sort_remaining: Arc<AtomicU64>,
    post_sort_backfill_rowid: Arc<AtomicU64>,
    post_sort_backfill_failures: Arc<AtomicU64>,
    _ephemeral_database: Option<EphemeralDatabase>,
}

type PendingRecordKey = (String, String, String);

impl Drop for SqliteStorage {
    fn drop(&mut self) {
        self.checkpointer_stop.store(true, Ordering::Relaxed);
        if let Some(worker) = self.checkpointer_worker.take() {
            worker.thread().unpark();
            if worker.join().is_err() {
                tracing::error!("SQLite checkpointer thread panicked during shutdown");
            }
        }
        if self.path != Path::new(":memory:") {
            let database_path = self.path.clone();
            let wal_metrics = Arc::clone(&self.wal_metrics);
            let shutdown_checkpoint = self.writer.call(move |connection| {
                connection.busy_timeout(CHECKPOINT_BUSY_TIMEOUT)?;
                let result = checkpoint_wal(connection, &database_path, &wal_metrics)?;
                wal_metrics.record_checkpoint(
                    &database_path,
                    wal_file_bytes(&database_path),
                    result.frames.max(0) as u64,
                    result.checkpointed.max(0) as u64,
                    result.reset,
                );
                Ok(result)
            });
            if let Err(error) = shutdown_checkpoint {
                tracing::warn!(%error, "WAL checkpoint failed during shutdown");
            }
        }
        self.writer.shutdown();
    }
}

fn repair_startup_follow_edges(connection: &Connection) -> Result<(), StorageError> {
    let mut registered = connection.prepare("SELECT did FROM registered_users ORDER BY did")?;
    let dids = registered
        .query_map([], |row| row.get::<_, String>(0))?
        .collect::<Result<Vec<_>, _>>()?;
    drop(registered);
    for did in dids {
        let follow_records: i64 = connection.query_row(
            "SELECT COUNT(*) FROM records
             WHERE collection='app.bsky.graph.follow' AND did=?1",
            [&did],
            |row| row.get(0),
        )?;
        let following_rows: i64 = connection.query_row(
            "SELECT COUNT(*) FROM follows WHERE actor=?1 AND following=1",
            [&did],
            |row| row.get(0),
        )?;
        tracing::info!(
            did = %did,
            follow_records,
            following_rows,
            "Checked a registered user's follow rows against their follow records",
        );
        if follow_records == following_rows {
            continue;
        }
        let mut rebuilt_rows = 0_u64;
        let transaction = connection.unchecked_transaction()?;
        {
            let mut statement = transaction.prepare(
                "SELECT DISTINCT json_extract(record_json, '$.subject') FROM records
                 WHERE collection='app.bsky.graph.follow' AND did=?1
                   AND json_extract(record_json, '$.subject') IS NOT NULL",
            )?;
            let subjects = statement
                .query_map([&did], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?;
            drop(statement);
            for subject in &subjects {
                rebuilt_rows = rebuilt_rows.saturating_add(transaction.execute(
                    "INSERT OR IGNORE INTO follows(actor,subject,following) VALUES(?1,?2,1)",
                    params![did, subject],
                )? as u64);
                transaction.execute(
                    "INSERT OR IGNORE INTO backfill_queue(did)
                     SELECT ?1 WHERE (SELECT COUNT(*) FROM backfill_queue) < ?2
                        OR EXISTS(SELECT 1 FROM backfill_queue WHERE did=?1)",
                    params![subject, MAX_BACKFILL_QUEUE],
                )?;
                transaction.execute(
                    "INSERT OR IGNORE INTO backfill_scope(owner_did,target_did)
                     SELECT ?1,?2 WHERE EXISTS(SELECT 1 FROM backfill_queue WHERE did=?2)",
                    params![did, subject],
                )?;
                transaction.execute(
                    "INSERT INTO backfill_status(did,done) VALUES(?1,0)
                     ON CONFLICT(did) DO UPDATE SET done=0
                     WHERE EXISTS(SELECT 1 FROM backfill_queue WHERE did=?1)",
                    [subject],
                )?;
            }
        }
        let stale_rows = transaction.execute(
            "DELETE FROM follows WHERE actor=?1 AND following=1
             AND subject NOT IN (
                 SELECT json_extract(record_json, '$.subject') FROM records
                 WHERE collection='app.bsky.graph.follow' AND did=?1
             )",
            [&did],
        )?;
        transaction.commit()?;
        tracing::warn!(
            did = %did,
            follow_records,
            following_rows_before = following_rows,
            follow_rows_inserted = rebuilt_rows,
            follow_rows_deleted = stale_rows as u64,
            "Rebuilt a registered user's follow rows from their follow records after a mismatch",
        );
    }
    Ok(())
}

fn build_tracked_set(connection: &Connection) -> Result<(TrackedSet, u64), StorageError> {
    let mut tracked = TrackedSet::default();
    let mut registered = connection.prepare("SELECT did FROM registered_users")?;
    for did in registered.query_map([], |row| row.get::<_, String>(0))? {
        tracked.registered.insert(did?);
    }
    drop(registered);

    let mut active_follow_edges = 0_u64;
    let mut follows = connection.prepare(
        "SELECT f.subject, COUNT(*) FROM follows f
         JOIN registered_users r ON r.did=f.actor
         WHERE f.following=1 GROUP BY f.subject",
    )?;
    for row in follows.query_map([], |row| {
        Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
    })? {
        let (subject, count) = row?;
        let count = u32::try_from(count).unwrap_or(u32::MAX);
        active_follow_edges = active_follow_edges.saturating_add(u64::from(count));
        if count > 0 {
            tracked.follow_counts.insert(subject, count);
        }
    }
    Ok((tracked, active_follow_edges))
}

fn build_profile_index(
    connection: &Connection,
) -> Result<HashMap<String, ProfileIndexEntry>, StorageError> {
    let mut index = HashMap::new();
    let mut statement = connection
        .prepare("SELECT did, has_profile, fetched_at_ms FROM profile_index_status ORDER BY did")?;
    for row in statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, bool>(1)?,
            row.get::<_, i64>(2)?,
        ))
    })? {
        let (did, has_profile, fetched_at_ms) = row?;
        index.insert(
            did,
            ProfileIndexEntry {
                has_profile,
                fetched_at_ms,
            },
        );
    }
    Ok(index)
}

type RecentPostsState = (
    HashMap<String, (i64, i64)>,
    (VecDeque<String>, HashSet<String>),
);

fn build_recent_posts_state(connection: &Connection) -> Result<RecentPostsState, StorageError> {
    let mut status = HashMap::new();
    let mut statement = connection.prepare(
        "SELECT did, fetched_at_ms, next_retry_at_ms FROM recent_posts_status ORDER BY did",
    )?;
    for row in statement.query_map([], |row| {
        Ok((
            row.get::<_, String>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, i64>(2)?,
        ))
    })? {
        let (did, fetched_at_ms, next_retry_at_ms) = row?;
        status.insert(did, (fetched_at_ms, next_retry_at_ms));
    }
    drop(statement);
    let mut queue = VecDeque::new();
    let mut queued = HashSet::new();
    let mut statement =
        connection.prepare("SELECT did FROM recent_posts_queue ORDER BY seq LIMIT ?1")?;
    for row in statement.query_map([MAX_RECENT_POSTS_QUEUE as i64], |row| {
        row.get::<_, String>(0)
    })? {
        let did = row?;
        queued.insert(did.clone());
        queue.push_back(did);
    }
    Ok((status, (queue, queued)))
}

const CHECKPOINT_INTERVAL: Duration = Duration::from_secs(2);

#[derive(Clone, Copy, Debug)]
struct CheckpointResult {
    busy: i64,
    frames: i64,
    checkpointed: i64,
    reset: bool,
}

fn checkpoint_wal(
    connection: &Connection,
    path: &Path,
    metrics: &WalMetrics,
) -> rusqlite::Result<CheckpointResult> {
    checkpoint_wal_with(connection, path, metrics, run_checkpoint_pragma)
}

#[derive(Clone, Copy, Debug)]
struct WalForceShrinkConfig {
    threshold_bytes: u64,
    interval: Duration,
    busy_timeout: Duration,
}

impl WalForceShrinkConfig {
    fn from_env() -> Self {
        Self {
            threshold_bytes: env_u64_or(
                "APPVIEW_WAL_FORCE_SHRINK_BYTES",
                WAL_FORCE_SHRINK_DEFAULT_BYTES,
            ),
            interval: Duration::from_secs(env_u64_or(
                "APPVIEW_WAL_FORCE_SHRINK_INTERVAL_SECS",
                WAL_FORCE_SHRINK_DEFAULT_INTERVAL.as_secs(),
            )),
            busy_timeout: Duration::from_secs(env_u64_or(
                "APPVIEW_WAL_FORCE_SHRINK_BUSY_TIMEOUT_SECS",
                WAL_FORCE_SHRINK_DEFAULT_TIMEOUT.as_secs(),
            )),
        }
    }
}

fn env_u64_or(name: &'static str, default: u64) -> u64 {
    match std::env::var(name) {
        Ok(value) => match value.parse() {
            Ok(parsed) => parsed,
            Err(error) => {
                tracing::warn!(knob = name, value, %error, default, "invalid SQLite WAL setting; using default");
                default
            }
        },
        Err(std::env::VarError::NotPresent) => default,
        Err(error) => {
            tracing::warn!(knob = name, %error, default, "could not read SQLite WAL setting; using default");
            default
        }
    }
}

/// Runs the ordinary PASSIVE checkpoint first. A caught-up oversized WAL may
/// then be truncated, but failed attempts consume the interval too so a pinned
/// reader cannot turn the two-second checkpointer loop into a retry storm.
fn checkpoint_wal_with_force_shrink(
    connection: &Connection,
    path: &Path,
    metrics: &WalMetrics,
    config: WalForceShrinkConfig,
    last_attempt: &mut Instant,
    now: Instant,
    mut before_truncate: impl FnMut(),
) -> rusqlite::Result<CheckpointResult> {
    let wal_bytes_before_passive = wal_file_bytes(path);
    let result = checkpoint_wal(connection, path, metrics)?;
    if config.threshold_bytes == 0
        || wal_bytes_before_passive <= config.threshold_bytes
        || result.busy != 0
        || result.frames != result.checkpointed
        || now.saturating_duration_since(*last_attempt) <= config.interval
    {
        return Ok(result);
    }

    let bytes_before = wal_file_bytes(path);
    force_shrink_wal(
        connection,
        path,
        metrics,
        config.busy_timeout,
        bytes_before,
        (last_attempt, now),
        &mut before_truncate,
    );
    Ok(result)
}

fn force_shrink_wal(
    connection: &Connection,
    path: &Path,
    metrics: &WalMetrics,
    busy_timeout: Duration,
    bytes_before: u64,
    attempt_clock: (&mut Instant, Instant),
    before_truncate: &mut impl FnMut(),
) {
    metrics
        .wal_force_shrink_attempts
        .fetch_add(1, Ordering::Relaxed);
    metrics
        .wal_force_shrink_bytes_before
        .store(bytes_before, Ordering::Relaxed);
    let (last_attempt, now) = attempt_clock;
    *last_attempt = now;

    if let Err(error) = set_bounded_checkpoint_busy_timeout(connection, busy_timeout) {
        let bytes_after = wal_file_bytes(path);
        metrics
            .wal_force_shrink_bytes_after
            .store(bytes_after, Ordering::Relaxed);
        tracing::info!(
            wal_bytes_before = bytes_before,
            wal_bytes_after = bytes_after,
            duration_ms = 0.0_f64,
            result = "error",
            error = %error,
            "SQLite forced WAL shrink attempt"
        );
        if let Err(restore_error) = connection.busy_timeout(CHECKPOINT_BUSY_TIMEOUT) {
            tracing::error!(%restore_error, "could not restore SQLite checkpoint busy timeout after failed WAL shrink setup");
        }
        return;
    }

    let _checkpoint = metrics.checkpoint_activity(CHECKPOINT_TRUNCATE);
    before_truncate();
    let started = Instant::now();
    let truncate_result = run_checkpoint_pragma(connection, CHECKPOINT_TRUNCATE);
    let elapsed = started.elapsed();
    clear_checkpoint_busy_deadline();
    let restore_result = connection.busy_timeout(CHECKPOINT_BUSY_TIMEOUT);
    let bytes_after = wal_file_bytes(path);
    let elapsed_ns = duration_ns(elapsed);
    metrics
        .ckpt_truncate_max_ns
        .fetch_max(elapsed_ns, Ordering::Relaxed);
    metrics
        .wal_force_shrink_ms_max_ns
        .fetch_max(elapsed_ns, Ordering::Relaxed);
    metrics
        .wal_force_shrink_bytes_after
        .store(bytes_after, Ordering::Relaxed);

    let mut pragma_busy = None;
    let mut pragma_frames = None;
    let mut pragma_checkpointed = None;
    let (result_label, error_message) = match truncate_result {
        Err(error) => {
            if is_rusqlite_busy_error(&error) {
                metrics
                    .wal_force_shrink_busy
                    .fetch_add(1, Ordering::Relaxed);
                metrics.sqlite_busy.fetch_add(1, Ordering::Relaxed);
            }
            ("error", Some(error.to_string()))
        }
        Ok((busy, frames, checkpointed)) if busy != 0 || frames != checkpointed => {
            pragma_busy = Some(busy);
            pragma_frames = Some(frames);
            pragma_checkpointed = Some(checkpointed);
            metrics
                .wal_force_shrink_busy
                .fetch_add(1, Ordering::Relaxed);
            record_checkpoint_busy(metrics, "checkpoint_TRUNCATE", busy, frames, checkpointed);
            ("busy", None)
        }
        Ok((busy, frames, checkpointed)) => {
            pragma_busy = Some(busy);
            pragma_frames = Some(frames);
            pragma_checkpointed = Some(checkpointed);
            metrics.wal_force_shrink_ok.fetch_add(1, Ordering::Relaxed);
            ("ok", None)
        }
    };

    let restore_error = restore_result.err();
    if let Some(error) = &restore_error {
        tracing::error!(%error, "could not restore SQLite checkpoint busy timeout after WAL TRUNCATE");
    }
    let error_message = error_message.or_else(|| restore_error.map(|error| error.to_string()));
    tracing::info!(
        wal_bytes_before = bytes_before,
        wal_bytes_after = bytes_after,
        duration_ms = elapsed.as_secs_f64() * 1_000.0,
        result = result_label,
        sqlite_busy = ?pragma_busy,
        wal_frames = ?pragma_frames,
        ckpt_frames = ?pragma_checkpointed,
        error = error_message.as_deref().unwrap_or("none"),
        "SQLite forced WAL shrink attempt"
    );
}

/// A SQLite busy timeout can restart for separate lock acquisitions during a
/// checkpoint. Use one deadline for the entire TRUNCATE call so those waits do
/// not add up beyond the configured bound.
fn set_bounded_checkpoint_busy_timeout(
    connection: &Connection,
    timeout: Duration,
) -> rusqlite::Result<()> {
    let started = Instant::now();
    CHECKPOINT_BUSY_DEADLINE.with(|deadline| deadline.set(Some((started, timeout))));
    if let Err(error) = connection.busy_handler(Some(bounded_checkpoint_busy_handler)) {
        CHECKPOINT_BUSY_DEADLINE.with(|deadline| deadline.set(None));
        return Err(error);
    }
    Ok(())
}

thread_local! {
    static CHECKPOINT_BUSY_DEADLINE: Cell<Option<(Instant, Duration)>> = const { Cell::new(None) };
}

#[cfg(test)]
static FORCE_SHRINK_TEST_ACTIVE: AtomicBool = AtomicBool::new(false);
#[cfg(test)]
static FORCE_SHRINK_CHECKPOINT_BUSY: AtomicBool = AtomicBool::new(false);
#[cfg(test)]
static FORCE_SHRINK_WRITER_BUSY: AtomicBool = AtomicBool::new(false);

fn bounded_checkpoint_busy_handler(_attempt: i32) -> bool {
    #[cfg(test)]
    if FORCE_SHRINK_TEST_ACTIVE.load(Ordering::SeqCst) {
        FORCE_SHRINK_CHECKPOINT_BUSY.store(true, Ordering::SeqCst);
    }
    CHECKPOINT_BUSY_DEADLINE.with(|deadline| {
        let Some((started, timeout)) = deadline.get() else {
            return false;
        };
        let elapsed = started.elapsed();
        if elapsed >= timeout {
            return false;
        }
        std::thread::sleep(
            timeout
                .saturating_sub(elapsed)
                .min(Duration::from_millis(5)),
        );
        started.elapsed() < timeout
    })
}

fn clear_checkpoint_busy_deadline() {
    CHECKPOINT_BUSY_DEADLINE.with(|deadline| deadline.set(None));
}

fn is_rusqlite_busy_error(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(failure, _)
            if matches!(
                failure.code,
                rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
            )
    )
}

fn checkpoint_wal_with(
    connection: &Connection,
    path: &Path,
    metrics: &WalMetrics,
    mut run_checkpoint: impl FnMut(&Connection, usize) -> rusqlite::Result<(i64, i64, i64)>,
) -> rusqlite::Result<CheckpointResult> {
    // PASSIVE never waits for writers. Avoid TRUNCATE in the steady-state
    // loop: its exclusive checkpoint window can overlap new frames and hold
    // every writer out while it copies pages and fsyncs. journal_size_limit
    // bounds WAL reuse after a completed checkpoint.
    let wal_bytes_before = wal_file_bytes(path);
    let passive_started = Instant::now();
    let passive_result = {
        let _checkpoint = metrics.checkpoint_activity(CHECKPOINT_PASSIVE);
        run_checkpoint(connection, CHECKPOINT_PASSIVE)
    };
    let passive_elapsed = passive_started.elapsed();
    let (busy, frames, checkpointed) = passive_result?;
    metrics
        .ckpt_passive_max_ns
        .fetch_max(duration_ns(passive_elapsed), Ordering::Relaxed);
    if passive_elapsed >= SQLITE_SLOW_WRITE_THRESHOLD {
        metrics.slow_txn.fetch_add(1, Ordering::Relaxed);
        tracing::warn!(
            kind = "checkpoint_PASSIVE",
            duration_ms = passive_elapsed.as_millis() as u64,
            wal_bytes = wal_file_bytes(path),
            "slow SQLite checkpoint"
        );
    }
    if busy != 0 {
        record_checkpoint_busy(metrics, "checkpoint_PASSIVE", busy, frames, checkpointed);
    }
    let result = CheckpointResult {
        busy,
        frames,
        checkpointed,
        reset: wal_file_bytes(path) < wal_bytes_before,
    };
    Ok(result)
}

fn run_checkpoint_pragma(
    connection: &Connection,
    kind: usize,
) -> rusqlite::Result<(i64, i64, i64)> {
    let mode = match kind {
        CHECKPOINT_PASSIVE => "PASSIVE",
        CHECKPOINT_TRUNCATE => "TRUNCATE",
        _ => unreachable!("unknown checkpoint kind"),
    };
    connection.query_row(&format!("PRAGMA wal_checkpoint({mode})"), [], |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, i64>(1)?,
            row.get::<_, i64>(2)?,
        ))
    })
}

fn duration_ns(duration: Duration) -> u64 {
    u64::try_from(duration.as_nanos()).unwrap_or(u64::MAX)
}

fn record_checkpoint_busy(
    metrics: &WalMetrics,
    kind: &'static str,
    busy: i64,
    frames: i64,
    checkpointed: i64,
) {
    metrics.sqlite_busy.fetch_add(1, Ordering::Relaxed);
    tracing::warn!(
        kind,
        busy,
        lock_holder = "active_reader_or_writer",
        wal_frames = frames,
        ckpt_frames = checkpointed,
        "SQLite checkpoint could not advance"
    );
}

fn wal_file_bytes(path: &Path) -> u64 {
    fs::metadata(format!("{}-wal", path.display()))
        .map(|metadata| metadata.len())
        .unwrap_or(0)
}

fn wal_file_generation(path: &Path) -> Option<u64> {
    let mut file = fs::File::open(format!("{}-wal", path.display())).ok()?;
    let mut header = [0_u8; 24];
    file.read_exact(&mut header).ok()?;
    let generation = u64::from_be_bytes(header[16..24].try_into().ok()?);
    Some(generation.max(1))
}

fn should_skip_checkpoint_for_pressure(pressure_active: bool, file_bytes: u64) -> bool {
    pressure_active && file_bytes < WAL_PRESSURE_CHECKPOINT_BYTES
}

fn spawn_checkpointer(
    path: PathBuf,
    stop: Arc<AtomicBool>,
    backlog: Arc<AtomicU64>,
    wal_metrics: Arc<WalMetrics>,
    pressure: Arc<StoragePressure>,
) -> Result<std::thread::JoinHandle<()>, StorageError> {
    let connection = Connection::open(&path)?;
    connection.busy_timeout(CHECKPOINT_BUSY_TIMEOUT)?;
    connection.execute_batch(&format!(
        "PRAGMA journal_size_limit = {WAL_JOURNAL_SIZE_LIMIT_BYTES};"
    ))?;
    let force_shrink = WalForceShrinkConfig::from_env();
    let mut last_force_shrink = Instant::now();
    let worker = std::thread::Builder::new()
        .name("sqlite-checkpoint".to_owned())
        .spawn(move || {
            loop {
                std::thread::park_timeout(CHECKPOINT_INTERVAL);
                if stop.load(Ordering::Relaxed) {
                    break;
                }
                // Unknown counts as behind: a checkpoint of a big WAL runs for minutes,
                // and the last small number would let the backfill pile on meanwhile.
                backlog.store(u64::MAX, Ordering::Relaxed);
                let file_bytes = wal_file_bytes(&path);
                let now = Instant::now();
                let force_shrink_due = force_shrink.threshold_bytes > 0
                    && file_bytes > force_shrink.threshold_bytes
                    && now.saturating_duration_since(last_force_shrink) > force_shrink.interval;
                if should_skip_checkpoint_for_pressure(pressure.is_active(), file_bytes)
                    && !force_shrink_due
                {
                    // PASSIVE has no page-count budget. Skip while interactive work is
                    // queued, then run every round after the WAL reaches 3x the limit.
                    wal_metrics.record_skipped_checkpoint(&path, file_bytes);
                    continue;
                }
                match checkpoint_wal_with_force_shrink(
                    &connection,
                    &path,
                    &wal_metrics,
                    force_shrink,
                    &mut last_force_shrink,
                    now,
                    || {},
                ) {
                    Ok(result) => {
                        let file_bytes = wal_file_bytes(&path);
                        wal_metrics.record_checkpoint(
                            &path,
                            file_bytes,
                            result.frames.max(0) as u64,
                            result.checkpointed.max(0) as u64,
                            result.reset,
                        );
                        backlog.store(
                            result.frames.saturating_sub(result.checkpointed).max(0) as u64,
                            Ordering::Relaxed,
                        );
                        if result.busy != 0 {
                            tracing::debug!(
                                busy = result.busy,
                                wal_frames = result.frames,
                                ckpt_frames = result.checkpointed,
                                "WAL checkpoint could not reset while readers were active"
                            );
                        }
                    }
                    Err(error) => tracing::warn!(%error, "WAL checkpoint failed"),
                }
            }
        })?;
    Ok(worker)
}

impl SqliteStorage {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let path = path.as_ref().to_path_buf();
        let is_memory = path == Path::new(":memory:");
        if !is_memory && let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        let ephemeral_database = if is_memory {
            Some(EphemeralDatabase(std::env::temp_dir().join(format!(
                "appview-rs-memory-{}-{}.sqlite3",
                std::process::id(),
                NEXT_MEMORY_DATABASE_ID.fetch_add(1, Ordering::Relaxed)
            ))))
        } else {
            None
        };
        let database_path = ephemeral_database
            .as_ref()
            .map(|database| database.0.clone())
            .unwrap_or_else(|| path.clone());
        ensure_private_database_file(&database_path)?;
        let connection = Connection::open(&database_path)?;
        connection.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA synchronous = NORMAL;
             CREATE TABLE IF NOT EXISTS records (
                 did TEXT NOT NULL,
                 collection TEXT NOT NULL,
                 rkey TEXT NOT NULL,
                 uri TEXT NOT NULL,
                 cid TEXT,
                 time_us INTEGER NOT NULL,
                 record_json TEXT NOT NULL,
                 PRIMARY KEY (did, collection, rkey)
             );
              CREATE TABLE IF NOT EXISTS post_counts (
                  uri TEXT PRIMARY KEY,
                  like_count INTEGER NOT NULL DEFAULT 0,
                  repost_count INTEGER NOT NULL DEFAULT 0,
                  reply_count INTEGER NOT NULL DEFAULT 0,
                  quote_count INTEGER NOT NULL DEFAULT 0
              );
              CREATE TABLE IF NOT EXISTS post_sort (
                  uri TEXT PRIMARY KEY,
                  did TEXT NOT NULL,
                  sort_us INTEGER NOT NULL,
                  time_us INTEGER NOT NULL
              );
              CREATE INDEX IF NOT EXISTS post_sort_author_order
                  ON post_sort(did, sort_us DESC, time_us DESC, uri DESC);
              CREATE TRIGGER IF NOT EXISTS records_post_sort_delete
              AFTER DELETE ON records
              WHEN OLD.collection='app.bsky.feed.post'
              BEGIN
                  DELETE FROM post_sort WHERE uri=OLD.uri;
              END;
              CREATE TRIGGER IF NOT EXISTS records_post_sort_update
              AFTER UPDATE ON records
              WHEN OLD.collection='app.bsky.feed.post'
              BEGIN
                  DELETE FROM post_sort WHERE uri=OLD.uri;
              END;
              CREATE TRIGGER IF NOT EXISTS records_post_counts_insert
             AFTER INSERT ON records
             WHEN (NEW.collection IN ('app.bsky.feed.like','app.bsky.feed.repost')
                       AND instr(NEW.record_json, char(34) || 'subject' || char(34)) > 0)
                  OR (NEW.collection='app.bsky.feed.post'
                       AND (instr(NEW.record_json, char(34) || 'reply' || char(34)) > 0
                            OR instr(NEW.record_json, char(34) || 'embed' || char(34)) > 0))
             BEGIN
                 UPDATE post_counts SET
                     like_count = like_count + CASE WHEN NEW.collection='app.bsky.feed.like'
                         AND uri=json_extract(NEW.record_json, '$.subject.uri') THEN 1 ELSE 0 END,
                     repost_count = repost_count + CASE WHEN NEW.collection='app.bsky.feed.repost'
                         AND uri=json_extract(NEW.record_json, '$.subject.uri') THEN 1 ELSE 0 END,
                     reply_count = reply_count + CASE WHEN NEW.collection='app.bsky.feed.post'
                         AND uri=json_extract(NEW.record_json, '$.reply.parent.uri') THEN 1 ELSE 0 END,
                     quote_count = quote_count + CASE WHEN NEW.collection='app.bsky.feed.post'
                         AND uri=json_extract(NEW.record_json, '$.embed.record.uri') THEN 1 ELSE 0 END
                 WHERE uri IN (
                     CASE WHEN NEW.collection IN ('app.bsky.feed.like','app.bsky.feed.repost')
                         THEN json_extract(NEW.record_json, '$.subject.uri') END,
                     CASE WHEN NEW.collection='app.bsky.feed.post'
                         THEN json_extract(NEW.record_json, '$.reply.parent.uri') END,
                     CASE WHEN NEW.collection='app.bsky.feed.post'
                         THEN json_extract(NEW.record_json, '$.embed.record.uri') END
                 );
             END;
             CREATE TRIGGER IF NOT EXISTS records_post_counts_delete
             AFTER DELETE ON records
             WHEN (OLD.collection IN ('app.bsky.feed.like','app.bsky.feed.repost')
                       AND instr(OLD.record_json, char(34) || 'subject' || char(34)) > 0)
                  OR (OLD.collection='app.bsky.feed.post'
                       AND (instr(OLD.record_json, char(34) || 'reply' || char(34)) > 0
                            OR instr(OLD.record_json, char(34) || 'embed' || char(34)) > 0))
             BEGIN
                 UPDATE post_counts SET
                     like_count = like_count - CASE WHEN OLD.collection='app.bsky.feed.like'
                         AND uri=json_extract(OLD.record_json, '$.subject.uri') THEN 1 ELSE 0 END,
                     repost_count = repost_count - CASE WHEN OLD.collection='app.bsky.feed.repost'
                         AND uri=json_extract(OLD.record_json, '$.subject.uri') THEN 1 ELSE 0 END,
                     reply_count = reply_count - CASE WHEN OLD.collection='app.bsky.feed.post'
                         AND uri=json_extract(OLD.record_json, '$.reply.parent.uri') THEN 1 ELSE 0 END,
                     quote_count = quote_count - CASE WHEN OLD.collection='app.bsky.feed.post'
                         AND uri=json_extract(OLD.record_json, '$.embed.record.uri') THEN 1 ELSE 0 END
                 WHERE uri IN (
                     CASE WHEN OLD.collection IN ('app.bsky.feed.like','app.bsky.feed.repost')
                         THEN json_extract(OLD.record_json, '$.subject.uri') END,
                     CASE WHEN OLD.collection='app.bsky.feed.post'
                         THEN json_extract(OLD.record_json, '$.reply.parent.uri') END,
                     CASE WHEN OLD.collection='app.bsky.feed.post'
                         THEN json_extract(OLD.record_json, '$.embed.record.uri') END
                 );
             END;
             CREATE TRIGGER IF NOT EXISTS records_post_counts_update
             AFTER UPDATE ON records
             WHEN (NEW.collection IN ('app.bsky.feed.like','app.bsky.feed.repost')
                       AND instr(NEW.record_json, char(34) || 'subject' || char(34)) > 0)
                  OR (NEW.collection='app.bsky.feed.post'
                       AND (instr(NEW.record_json, char(34) || 'reply' || char(34)) > 0
                            OR instr(NEW.record_json, char(34) || 'embed' || char(34)) > 0))
                  OR (OLD.collection IN ('app.bsky.feed.like','app.bsky.feed.repost')
                       AND instr(OLD.record_json, char(34) || 'subject' || char(34)) > 0)
                  OR (OLD.collection='app.bsky.feed.post'
                       AND (instr(OLD.record_json, char(34) || 'reply' || char(34)) > 0
                            OR instr(OLD.record_json, char(34) || 'embed' || char(34)) > 0))
             BEGIN
                 UPDATE post_counts SET
                     like_count = like_count
                         + CASE WHEN NEW.collection='app.bsky.feed.like'
                             AND uri=json_extract(NEW.record_json, '$.subject.uri') THEN 1 ELSE 0 END
                         - CASE WHEN OLD.collection='app.bsky.feed.like'
                             AND uri=json_extract(OLD.record_json, '$.subject.uri') THEN 1 ELSE 0 END,
                     repost_count = repost_count
                         + CASE WHEN NEW.collection='app.bsky.feed.repost'
                             AND uri=json_extract(NEW.record_json, '$.subject.uri') THEN 1 ELSE 0 END
                         - CASE WHEN OLD.collection='app.bsky.feed.repost'
                             AND uri=json_extract(OLD.record_json, '$.subject.uri') THEN 1 ELSE 0 END,
                     reply_count = reply_count
                         + CASE WHEN NEW.collection='app.bsky.feed.post'
                             AND uri=json_extract(NEW.record_json, '$.reply.parent.uri') THEN 1 ELSE 0 END
                         - CASE WHEN OLD.collection='app.bsky.feed.post'
                             AND uri=json_extract(OLD.record_json, '$.reply.parent.uri') THEN 1 ELSE 0 END,
                     quote_count = quote_count
                         + CASE WHEN NEW.collection='app.bsky.feed.post'
                             AND uri=json_extract(NEW.record_json, '$.embed.record.uri') THEN 1 ELSE 0 END
                         - CASE WHEN OLD.collection='app.bsky.feed.post'
                             AND uri=json_extract(OLD.record_json, '$.embed.record.uri') THEN 1 ELSE 0 END
                 WHERE uri IN (
                     CASE WHEN NEW.collection IN ('app.bsky.feed.like','app.bsky.feed.repost')
                         THEN json_extract(NEW.record_json, '$.subject.uri') END,
                     CASE WHEN NEW.collection='app.bsky.feed.post'
                         THEN json_extract(NEW.record_json, '$.reply.parent.uri') END,
                     CASE WHEN NEW.collection='app.bsky.feed.post'
                         THEN json_extract(NEW.record_json, '$.embed.record.uri') END,
                     CASE WHEN OLD.collection IN ('app.bsky.feed.like','app.bsky.feed.repost')
                         THEN json_extract(OLD.record_json, '$.subject.uri') END,
                     CASE WHEN OLD.collection='app.bsky.feed.post'
                         THEN json_extract(OLD.record_json, '$.reply.parent.uri') END,
                     CASE WHEN OLD.collection='app.bsky.feed.post'
                         THEN json_extract(OLD.record_json, '$.embed.record.uri') END
                 );
             END;
             CREATE INDEX IF NOT EXISTS records_collection_time ON records(collection, time_us DESC);
             CREATE INDEX IF NOT EXISTS records_collection_uri ON records(collection, uri);
             CREATE INDEX IF NOT EXISTS records_author_time ON records(collection, did, time_us DESC, uri DESC);
             CREATE INDEX IF NOT EXISTS records_author_reply_time
                 ON records(collection, did, json_type(record_json, '$.reply'), time_us DESC, uri DESC);
             CREATE INDEX IF NOT EXISTS records_follow_actor_subject
                 ON records(collection, did, json_extract(record_json, '$.subject'));
             CREATE INDEX IF NOT EXISTS records_follow_subject_actor
                 ON records(collection, json_extract(record_json, '$.subject'), did, time_us DESC);
             CREATE INDEX IF NOT EXISTS records_follow_subject_time
                 ON records(collection, json_extract(record_json, '$.subject'), time_us DESC, uri DESC)
                 WHERE collection='app.bsky.graph.follow';
             CREATE INDEX IF NOT EXISTS records_collection_subject_uri
                 ON records(collection, json_extract(record_json, '$.subject.uri'), time_us DESC);
             CREATE INDEX IF NOT EXISTS records_collection_parent_uri
                 ON records(collection, json_extract(record_json, '$.reply.parent.uri'), time_us DESC);
             CREATE INDEX IF NOT EXISTS records_collection_embed_record_uri
                 ON records(collection, json_extract(record_json, '$.embed.record.uri'), time_us DESC);
             CREATE INDEX IF NOT EXISTS records_replies_order
                 ON records(collection, json_extract(record_json, '$.reply.parent.uri'), time_us, uri);
             CREATE INDEX IF NOT EXISTS records_author_subject_uri
                 ON records(collection, did, json_extract(record_json, '$.subject.uri'));
             CREATE TABLE IF NOT EXISTS appview_state (key TEXT PRIMARY KEY, value TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS notification_events (
                 source_did TEXT NOT NULL,
                 source_collection TEXT NOT NULL,
                 source_rkey TEXT NOT NULL,
                 source_uri TEXT NOT NULL,
                 reason TEXT NOT NULL,
                 subject_did TEXT NOT NULL,
                 reason_subject TEXT,
                 time_us INTEGER NOT NULL,
                 PRIMARY KEY(source_did, source_collection, source_rkey, reason, subject_did)
             );
             CREATE INDEX IF NOT EXISTS notification_events_subject_time
                 ON notification_events(subject_did, time_us DESC, source_uri DESC, reason);
             CREATE INDEX IF NOT EXISTS notification_events_subject_key_time
                 ON notification_events(subject_did, reason, reason_subject, time_us DESC, source_uri DESC);
             CREATE TABLE IF NOT EXISTS notification_last_seen (
                 viewer_did TEXT PRIMARY KEY,
                 time_us INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS registered_users (did TEXT PRIMARY KEY);
             CREATE TABLE IF NOT EXISTS web_session_generations (
                 did TEXT PRIMARY KEY,
                 generation INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS web_sessions (
                 did TEXT NOT NULL,
                 session_id TEXT NOT NULL,
                 generation INTEGER NOT NULL,
                 PRIMARY KEY (did, session_id)
             );
             CREATE TABLE IF NOT EXISTS follows (
                 actor TEXT NOT NULL,
                 subject TEXT NOT NULL,
                 following INTEGER NOT NULL DEFAULT 1,
                 PRIMARY KEY (actor, subject)
             );
             CREATE TABLE IF NOT EXISTS backfill_queue (
                 did TEXT PRIMARY KEY,
                 enqueued_at INTEGER NOT NULL DEFAULT (unixepoch())
             );
             CREATE TABLE IF NOT EXISTS backfill_status (
                 did TEXT PRIMARY KEY,
                 done INTEGER NOT NULL DEFAULT 0
             );
             CREATE TABLE IF NOT EXISTS backfill_retry (
                 did TEXT PRIMARY KEY,
                 failures INTEGER NOT NULL,
                 retry_at_ms INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS backfill_scope (
                 owner_did TEXT NOT NULL,
                 target_did TEXT NOT NULL,
                 PRIMARY KEY (owner_did, target_did)
             );
             CREATE TABLE IF NOT EXISTS verified_handles (
                 did TEXT PRIMARY KEY,
                 handle TEXT NOT NULL,
                 updated_at INTEGER NOT NULL DEFAULT (unixepoch())
             );
             CREATE TABLE IF NOT EXISTS invalid_handles (
                 did TEXT PRIMARY KEY,
                 checked_at INTEGER NOT NULL DEFAULT (unixepoch())
             );
             CREATE TABLE IF NOT EXISTS handle_refresh_queue (
                 did TEXT PRIMARY KEY,
                 requested_at INTEGER NOT NULL DEFAULT (unixepoch()),
                 priority INTEGER NOT NULL DEFAULT 0
             );
             CREATE TABLE IF NOT EXISTS profile_index_status (
                 did TEXT PRIMARY KEY,
                 has_profile INTEGER NOT NULL,
                 fetched_at_ms INTEGER NOT NULL
             );
             CREATE TABLE IF NOT EXISTS profile_fetch_queue (
                 seq INTEGER PRIMARY KEY AUTOINCREMENT,
                 did TEXT NOT NULL UNIQUE
             );
              CREATE TABLE IF NOT EXISTS recent_profile_clicks (
                  seq INTEGER PRIMARY KEY AUTOINCREMENT,
                  viewer_did TEXT NOT NULL,
                  profile_did TEXT NOT NULL,
                  UNIQUE(viewer_did, profile_did)
              );
              CREATE TABLE IF NOT EXISTS recent_posts_status (
                  did TEXT PRIMARY KEY,
                  fetched_at_ms INTEGER NOT NULL,
                  next_retry_at_ms INTEGER NOT NULL
              );
              CREATE TABLE IF NOT EXISTS recent_posts_queue (
                  seq INTEGER PRIMARY KEY AUTOINCREMENT,
                  did TEXT NOT NULL UNIQUE
              );
              CREATE TABLE IF NOT EXISTS older_posts_state (
                  did TEXT PRIMARY KEY,
                  pds_cursor TEXT,
                  exhausted INTEGER NOT NULL DEFAULT 0,
                  updated_at_ms INTEGER NOT NULL DEFAULT 0
              );",
        )?;
        connection.execute_batch(
            "CREATE INDEX IF NOT EXISTS recent_profile_clicks_viewer_order
             ON recent_profile_clicks(viewer_did, seq DESC);",
        )?;
        let handle_refresh_has_priority: bool = connection.query_row(
            "SELECT EXISTS(SELECT 1 FROM pragma_table_info('handle_refresh_queue') WHERE name='priority')",
            [],
            |row| row.get(0),
        )?;
        if !handle_refresh_has_priority {
            connection.execute(
                "ALTER TABLE handle_refresh_queue ADD COLUMN priority INTEGER NOT NULL DEFAULT 0",
                [],
            )?;
        }
        connection.execute_batch(
            "CREATE INDEX IF NOT EXISTS handle_refresh_priority ON handle_refresh_queue(priority DESC);",
        )?;
        connection.execute(
            "INSERT OR IGNORE INTO profile_index_status(did,has_profile,fetched_at_ms)
             SELECT did,1,MAX(time_us)/1000 FROM records
             WHERE collection='app.bsky.actor.profile' GROUP BY did",
            [],
        )?;
        migrate_follow_state(&connection)?;
        connection.execute_batch(
            "CREATE INDEX IF NOT EXISTS follows_active_subject_actor
             ON follows(subject, actor) WHERE following=1;",
        )?;
        repair_startup_follow_edges(&connection)?;
        connection.execute(
            "DELETE FROM backfill_queue
             WHERE did NOT IN (
                 SELECT did FROM backfill_queue ORDER BY enqueued_at,did LIMIT ?1
             )",
            [MAX_BACKFILL_QUEUE],
        )?;
        connection.execute(
            "DELETE FROM recent_posts_queue
             WHERE seq NOT IN (
                 SELECT seq FROM recent_posts_queue ORDER BY seq LIMIT ?1
             )",
            [MAX_RECENT_POSTS_QUEUE as i64],
        )?;
        migrate_notification_events(&connection)?;
        // SQLite's automatic checkpoint runs inside a commit under the connection
        // lock. A side connection checkpoints instead; PASSIVE does not block writers.
        connection.execute_batch(
            "PRAGMA wal_autocheckpoint = 0; PRAGMA busy_timeout = 5000;
             PRAGMA journal_size_limit = 67108864;",
        )?;
        let tracked_build_started = Instant::now();
        let (tracked_set, tracked_follow_edges) = build_tracked_set(&connection)?;
        let tracked_set_build_ms = tracked_build_started.elapsed().as_secs_f64() * 1000.0;
        tracing::info!(
            registered_users = tracked_set.registered.len(),
            followed_dids = tracked_set.follow_counts.len(),
            active_registered_follow_edges = tracked_follow_edges,
            tracked_set_build_ms,
            "Built in-memory tracked DID set"
        );
        let profile_index_build_started = Instant::now();
        let profile_index = build_profile_index(&connection)?;
        let profile_index_build_ms = profile_index_build_started.elapsed().as_secs_f64() * 1000.0;
        tracing::info!(
            profiles = profile_index.len(),
            profile_index_build_ms,
            "Built in-memory profile index"
        );
        let recent_posts_state_started = Instant::now();
        let (recent_posts_status_rows, recent_posts_queue_rows) =
            build_recent_posts_state(&connection)?;
        let recent_posts_state_build_ms =
            recent_posts_state_started.elapsed().as_secs_f64() * 1000.0;
        tracing::info!(
            accounts = recent_posts_status_rows.len(),
            queued = recent_posts_queue_rows.0.len(),
            recent_posts_state_build_ms,
            "Built in-memory recent-posts state"
        );
        let mut read_connections = Vec::new();
        for _ in 0..SQLITE_READ_CONNECTIONS {
            let reader = Connection::open(&database_path)?;
            reader.execute_batch("PRAGMA busy_timeout = 5000; PRAGMA query_only = ON;")?;
            read_connections.push(Mutex::new(reader));
        }
        let read_metrics = Arc::new(ReadPoolMetrics::default());
        let wal_metrics = Arc::new(WalMetrics::default());
        let pressure = Arc::new(StoragePressure::default());
        let recent_posts_counters = Arc::new(RecentPostsCounters::default());
        let writer = SqliteWriter::new(
            connection,
            Arc::clone(&read_metrics),
            Arc::clone(&wal_metrics),
            Arc::clone(&pressure),
        )?;
        writer.share_recent_posts_counters(Arc::clone(&recent_posts_counters));
        let stop = Arc::new(AtomicBool::new(false));
        let backlog = Arc::new(AtomicU64::new(0));
        let checkpointer_worker = if !is_memory {
            backlog.store(u64::MAX, Ordering::Relaxed);
            Some(spawn_checkpointer(
                database_path.clone(),
                Arc::clone(&stop),
                Arc::clone(&backlog),
                Arc::clone(&wal_metrics),
                Arc::clone(&pressure),
            )?)
        } else {
            None
        };
        Ok(Self {
            writer,
            read_connections,
            next_read_connection: AtomicU64::new(0),
            read_metrics,
            pressure,
            path,
            checkpointer_stop: stop,
            checkpointer_worker,
            checkpoint_backlog: backlog,
            wal_metrics,
            tracked: Arc::new(RwLock::new(tracked_set)),
            profile_index: Arc::new(RwLock::new(profile_index)),
            recent_posts_status: Arc::new(RwLock::new(recent_posts_status_rows)),
            recent_posts_queue: Arc::new(RwLock::new(recent_posts_queue_rows)),
            recent_posts_counters,
            notification_hub: NotificationHub::default(),
            pending_counts_fills: Arc::new(RwLock::new(HashSet::new())),
            pending_record_writes: Arc::new(RwLock::new(HashMap::new())),
            post_sort_remaining: Arc::new(AtomicU64::new(POST_SORT_REMAINING_UNKNOWN)),
            post_sort_backfill_rowid: Arc::new(AtomicU64::new(0)),
            post_sort_backfill_failures: Arc::new(AtomicU64::new(0)),
            _ephemeral_database: ephemeral_database,
        })
    }

    /// The main connection's automatic-checkpoint threshold (0 = off).
    pub fn wal_autocheckpoint(&self) -> Result<i64, StorageError> {
        self.writer.call(|connection| {
            Ok(connection.query_row("PRAGMA wal_autocheckpoint", [], |row| row.get(0))?)
        })
    }

    /// Rebuild the startup scope table from SQLite and compare it with the live table.
    pub fn tracked_set_matches_sqlite(&self) -> Result<bool, StorageError> {
        let connection = self.read_conn()?;
        let (rebuilt, _) = build_tracked_set(&connection)?;
        Ok(*self.tracked.read().map_err(|_| StorageError::Poisoned)? == rebuilt)
    }

    pub fn profile_index_matches_sqlite(&self) -> Result<bool, StorageError> {
        self.writer.flush()?;
        let connection = self.read_conn()?;
        let rebuilt = build_profile_index(&connection)?;
        Ok(*self
            .profile_index
            .read()
            .map_err(|_| StorageError::Poisoned)?
            == rebuilt)
    }

    fn apply_tracked_follow_deltas(
        &self,
        deltas: &HashMap<String, i64>,
    ) -> Result<(), StorageError> {
        if deltas.is_empty() {
            return Ok(());
        }
        let mut tracked = self.tracked.write().map_err(|_| StorageError::Poisoned)?;
        for (subject, delta) in deltas {
            tracked.apply_follow_delta(subject, *delta);
        }
        Ok(())
    }

    fn write_profile_index_status(
        &self,
        did: &str,
        entry: ProfileIndexEntry,
    ) -> Result<(), StorageError> {
        let writer_did = did.to_owned();
        self.writer.call(move |connection| {
            connection.execute(
                "INSERT INTO profile_index_status(did,has_profile,fetched_at_ms)
                 VALUES(?1,?2,?3)
                 ON CONFLICT(did) DO UPDATE SET
                     has_profile=excluded.has_profile,fetched_at_ms=excluded.fetched_at_ms",
                params![writer_did, entry.has_profile, entry.fetched_at_ms],
            )?;
            Ok(())
        })?;
        self.profile_index
            .write()
            .map_err(|_| StorageError::Poisoned)?
            .insert(did.to_owned(), entry);
        Ok(())
    }

    fn refresh_profile_index_for_did(&self, did: &str) -> Result<(), StorageError> {
        let latest_profile_us: Option<i64> = self.read_conn()?.query_row(
            "SELECT MAX(time_us) FROM records
             WHERE did=?1 AND collection='app.bsky.actor.profile'",
            [did],
            |row| row.get(0),
        )?;
        self.write_profile_index_status(
            did,
            ProfileIndexEntry {
                has_profile: latest_profile_us.is_some(),
                fetched_at_ms: latest_profile_us
                    .map(|time_us| time_us / 1_000)
                    .unwrap_or_else(epoch_millis),
            },
        )
    }

    /// Picks a read connection. Public so a test (or an investigation) can hold
    /// one connection's guard and watch what the next reader does; production
    /// code should keep using the storage methods.
    ///
    /// Tries every connection without waiting, starting at the round-robin
    /// index, and blocks on that first choice only when all of them are busy —
    /// one slow query must not stall the readers that land on its connection
    /// while the others sit idle. A read that could not take its first choice
    /// is counted, with the longest wait, on the per-minute metrics line.
    pub fn read_conn(&self) -> Result<std::sync::MutexGuard<'_, Connection>, StorageError> {
        if self.read_connections.is_empty() {
            return Err(StorageError::WriterStopped);
        }
        let count = self.read_connections.len();
        let first = self.next_read_connection.fetch_add(1, Ordering::Relaxed) as usize % count;
        let started = Instant::now();
        for offset in 0..count {
            match self.read_connections[(first + offset) % count].try_lock() {
                Ok(connection) => {
                    install_web_read_progress_handler(&connection);
                    if offset > 0 {
                        self.read_metrics.record_wait(started.elapsed());
                    }
                    return Ok(connection);
                }
                Err(std::sync::TryLockError::Poisoned(_)) => {
                    return Err(StorageError::Poisoned);
                }
                Err(std::sync::TryLockError::WouldBlock) => {}
            }
        }
        self.pressure.waiting_reads.fetch_add(1, Ordering::Relaxed);
        let connection = self.read_connections[first].lock();
        self.pressure.waiting_reads.fetch_sub(1, Ordering::Relaxed);
        let connection = connection.map_err(|_| StorageError::Poisoned)?;
        install_web_read_progress_handler(&connection);
        self.read_metrics.record_wait(started.elapsed());
        Ok(connection)
    }

    /// Holds the writer thread busy for `duration`, starting once the returned
    /// receiver signals. Public so a test (or an investigation) can reproduce
    /// "the writer is seconds behind" and watch which page paths wait on it;
    /// production code must never hold the writer on purpose.
    pub fn hold_writer(&self, duration: Duration) -> std::sync::mpsc::Receiver<()> {
        let (started, gate) = std::sync::mpsc::channel();
        self.writer.fire(move |connection| {
            let awake: i64 = connection.query_row("SELECT 1", [], |row| row.get(0))?;
            let _ = awake;
            let _ = started.send(());
            std::thread::sleep(duration);
            Ok(None)
        });
        gate
    }

    /// Waits until every write already queued on the writer has been applied.
    /// Fire-and-forget request-path writes are only eventually visible; tests
    /// and investigations use this to observe them once the writer catches up.
    pub fn writer_barrier(&self) -> Result<(), StorageError> {
        self.writer.flush()
    }

    /// Records statements executed from now on on this request context (read
    /// pool connections get the tracer installed; the thread-local sink is
    /// propagated to web blocking calls made by this context).
    pub fn trace_statements(&self) -> StatementTrace {
        let trace = StatementTrace::default();
        for connection in &self.read_connections {
            if let Ok(connection) = connection.lock() {
                connection.trace_v2(
                    rusqlite::trace::TraceEventCodes::SQLITE_TRACE_PROFILE,
                    Some(route_statement_profile),
                );
            }
        }
        STATEMENT_TRACE_SINK.with(|sink| *sink.borrow_mut() = Some(trace.clone()));
        trace
    }

    /// Records statements executed by the writer thread. Runs on the writer,
    /// installs its tracer, and binds the sink to the writer thread's local,
    /// so only this storage's writes land in the recorder.
    pub fn trace_writer_statements(&self) -> Result<StatementTrace, StorageError> {
        let trace = StatementTrace::default();
        let sink = trace.clone();
        self.writer.call(move |connection| {
            connection.trace_v2(
                rusqlite::trace::TraceEventCodes::SQLITE_TRACE_PROFILE,
                Some(route_statement_profile),
            );
            STATEMENT_TRACE_SINK.with(|slot| *slot.borrow_mut() = Some(sink));
            Ok(())
        })?;
        Ok(trace)
    }

    /// A post's interaction summary and the SQLite work performed by its
    /// statements. Kept public for bounded-work regression tests and
    /// investigations.
    pub fn post_interaction_summary_counted(
        &self,
        uri: &str,
        viewer_did: Option<&str>,
    ) -> Result<(PostInteractionSummary, StatementWork), StorageError> {
        let mut work = StatementWork::default();
        let summary = self.post_interaction_summary_with_work(uri, viewer_did, Some(&mut work))?;
        Ok((summary, work))
    }

    fn post_interaction_summary_with_work(
        &self,
        uri: &str,
        viewer_did: Option<&str>,
        mut work: Option<&mut StatementWork>,
    ) -> Result<PostInteractionSummary, StorageError> {
        let cached_counts = {
            let conn = self.read_conn()?;
            let mut statement = conn.prepare(
                "SELECT like_count,repost_count,reply_count,quote_count
                 FROM post_counts WHERE uri=?1",
            )?;
            let counts = statement
                .query_row([uri], |row| {
                    Ok([row.get(0)?, row.get(1)?, row.get(2)?, row.get(3)?])
                })
                .optional()?;
            if let Some(work) = work.as_deref_mut() {
                work.observe(&statement);
            }
            counts
        };
        let counts = if let Some(counts) = cached_counts {
            counts
        } else {
            // A post_counts miss must not queue behind pending writes: the same
            // first-time scan used to run inside `writer.call` so an interaction
            // could not land between the COUNTs and the cache insert, but one
            // writer round trip per uncounted post made whole pages wait
            // (live 2026-11-01: render_ms 26510 for ~50 posts). Compute the
            // counts on a read connection instead and hand the cache fill to
            // the writer without waiting. The fill re-checks the row and
            // recomputes inside the writer's own transaction, so an interaction
            // that lands between this read and the fill is counted there (and
            // once the row exists, the post_counts triggers keep it exact).
            // Consequence: the counts shown for one render can be slightly
            // stale — an interaction that lands after this read first appears
            // on the next render.
            let read_uri = uri.to_owned();
            let (computed, read_work) = {
                let conn = self.read_conn()?;
                compute_post_counts(&conn, std::slice::from_ref(&read_uri))?
            };
            if let Some(work) = work.as_deref_mut() {
                work.fullscan_steps += read_work.fullscan_steps;
                work.vm_steps += read_work.vm_steps;
            }
            self.queue_post_counts_fill(&read_uri);
            computed.get(&read_uri).copied().unwrap_or([0_usize; 4])
        };
        let mut summary = PostInteractionSummary {
            like_count: counts[0],
            repost_count: counts[1],
            reply_count: counts[2],
            quote_count: counts[3],
            ..PostInteractionSummary::default()
        };
        if let Some(viewer_did) = viewer_did {
            let conn = self.read_conn()?;
            let mut statement = conn.prepare(
                "SELECT uri FROM records INDEXED BY records_author_subject_uri
                 WHERE collection='app.bsky.feed.like' AND did=?1
                   AND json_extract(record_json, '$.subject.uri')=?2
                 ORDER BY time_us DESC LIMIT 1",
            )?;
            summary.viewer_like = statement
                .query_row(params![viewer_did, uri], |row| row.get(0))
                .optional()?;
            if let Some(work) = work.as_deref_mut() {
                work.observe(&statement);
            }
            let mut statement = conn.prepare(
                "SELECT uri FROM records INDEXED BY records_author_subject_uri
                 WHERE collection='app.bsky.feed.repost' AND did=?1
                   AND json_extract(record_json, '$.subject.uri')=?2
                 ORDER BY time_us DESC LIMIT 1",
            )?;
            summary.viewer_repost = statement
                .query_row(params![viewer_did, uri], |row| row.get(0))
                .optional()?;
            if let Some(work) = work {
                work.observe(&statement);
            }
        }
        Ok(summary)
    }

    /// Queues a `post_counts` fill for one post without waiting for the writer.
    /// A fill already pending for the same uri is not queued twice, and at most
    /// [`MAX_PENDING_POST_COUNTS_FILLS`] are outstanding at once: a dropped fill
    /// only means the next page recomputes the counts on a read connection. The
    /// fill re-checks the row and recomputes inside the writer's own
    /// transaction, so an interaction that landed between the page read and the
    /// fill is counted there and never lost.
    fn queue_post_counts_fill(&self, uri: &str) {
        {
            let Ok(mut pending) = self.pending_counts_fills.write() else {
                tracing::warn!("post counts fill tracker is poisoned; skipping the fill");
                return;
            };
            if pending.contains(uri) {
                return;
            }
            if pending.len() >= MAX_PENDING_POST_COUNTS_FILLS {
                drop(pending);
                tracing::debug!(
                    post_uri = %uri,
                    "post counts fill queue is full; the next page will recompute these counts"
                );
                return;
            }
            pending.insert(uri.to_owned());
        }
        let writer_uri = uri.to_owned();
        let tracked_uri = uri.to_owned();
        let pending = Arc::clone(&self.pending_counts_fills);
        let requeue_reason = Arc::clone(&self.pending_counts_fills);
        let requeue_uri = uri.to_owned();
        let enqueued = self.writer.fire(move |connection| {
            // Un-mark before working: if this write later fails the savepoint
            // rolls back and a later page may queue a fresh fill.
            if let Ok(mut pending) = pending.write() {
                pending.remove(&tracked_uri);
            }
            let exists: bool = connection.query_row(
                "SELECT EXISTS(SELECT 1 FROM post_counts WHERE uri=?1)",
                [&writer_uri],
                |row| row.get(0),
            )?;
            if exists {
                return Ok(None);
            }
            let (computed, _work) =
                compute_post_counts(connection, std::slice::from_ref(&writer_uri))?;
            let counts = computed.get(&writer_uri).copied().unwrap_or([0_usize; 4]);
            connection.execute(
                "INSERT INTO post_counts
                 (uri,like_count,repost_count,reply_count,quote_count)
                 VALUES(?1,?2,?3,?4,?5)
                 ON CONFLICT(uri) DO NOTHING",
                params![
                    writer_uri,
                    counts[0] as i64,
                    counts[1] as i64,
                    counts[2] as i64,
                    counts[3] as i64
                ],
            )?;
            Ok(None)
        });
        if !enqueued && let Ok(mut pending) = requeue_reason.write() {
            pending.remove(&requeue_uri);
        }
    }

    /// Fills waiting for the writer right now; kept for tests and the timing
    /// console to observe the fire-and-forget path.
    pub fn pending_post_counts_fills(&self) -> usize {
        self.pending_counts_fills
            .read()
            .map(|pending| pending.len())
            .unwrap_or_default()
    }

    pub fn pending_queued_record_writes(&self) -> usize {
        self.pending_record_writes
            .read()
            .map(|pending| pending.len())
            .unwrap_or_default()
    }

    /// Head-of-line pressure on the read pool; the running service logs these
    /// on the per-minute `SQLite writer minute metrics` line.
    pub fn read_pool_metrics(&self) -> Arc<ReadPoolMetrics> {
        Arc::clone(&self.read_metrics)
    }

    /// One page of a DID's followers plus the index/scan work the statement
    /// performed, so a test can see whether it touched the whole follow
    /// collection or only this DID's followers.
    pub fn followers_for_did_counted(
        &self,
        did: &str,
        limit: usize,
    ) -> Result<(Vec<IndexedRecord>, StatementWork), StorageError> {
        let mut work = StatementWork::default();
        let records = self.followers_for_did_page(did, limit, Some(&mut work))?;
        Ok((records, work))
    }

    fn followers_for_did_page(
        &self,
        did: &str,
        limit: usize,
        mut work: Option<&mut StatementWork>,
    ) -> Result<Vec<IndexedRecord>, StorageError> {
        let conn = self.read_conn()?;
        // INDEXED BY pins the per-subject time order: without sqlite_stat rows the planner
        // otherwise picks records_collection_time and walks the WHOLE follow collection
        // newest-first, testing json_extract(subject) on every row, so a sparsely-followed
        // DID never fills the LIMIT. The forced index yields the rows for this subject in
        // (time_us DESC, uri DESC) order, so the LIMIT stops the walk at one page.
        let mut statement = conn.prepare(
            "SELECT did, collection, rkey, uri, cid, time_us, record_json FROM records
             INDEXED BY records_follow_subject_time
             WHERE collection='app.bsky.graph.follow' AND json_extract(record_json, '$.subject')=?1
             ORDER BY time_us DESC, uri DESC LIMIT ?2",
        )?;
        let records = statement
            .query_map(params![did, limit.clamp(1, 101) as i64], row_to_record)?
            .collect::<Result<Vec<_>, _>>()?;
        if let Some(counted) = work.as_mut() {
            counted.observe(&statement);
        }
        Ok(records)
    }

    /// One page of the home timeline plus the index/scan work its statements
    /// performed, so a test (or a slow-query investigation) can see whether the
    /// query touched the whole post collection or only the viewer's authors.
    pub fn timeline_posts_counted(
        &self,
        actor: &str,
        cursor: Option<&RecordCursor>,
        limit: usize,
    ) -> Result<(Vec<IndexedRecord>, StatementWork), StorageError> {
        let mut work = StatementWork::default();
        let records = self.timeline_posts_page(actor, cursor, limit, Some(&mut work))?;
        Ok((records, work))
    }

    /// One page of the post-sort timeline plus SQLite statement work, for
    /// verifying that a page read stays bounded as post history grows.
    pub fn timeline_posts_by_post_time_counted(
        &self,
        actor: &str,
        cursor: Option<&SortTimeCursor>,
        limit: usize,
    ) -> Result<(Vec<IndexedRecord>, StatementWork), StorageError> {
        let mut work = StatementWork::default();
        let records =
            self.timeline_posts_by_post_time_page(actor, cursor, limit, Some(&mut work))?;
        Ok((records, work))
    }

    /// One page of the home timeline, assembled from per-author index walks.
    ///
    /// The single statement this replaced planned, without `sqlite_stat` rows, as
    /// `SEARCH records USING INDEX records_collection_time (collection=?)`: it walked EVERY post
    /// newest-first and tested the author, so a viewer whose follows posted little never filled
    /// the LIMIT and the page read the whole post table. One walk per followed author on
    /// `records_author_time`, each capped at one page, bounds the work by the viewer's follows;
    /// merging in Rust keeps the exact rows, order (time_us DESC, uri DESC) and cursor
    /// tie-break of the old query, because every candidate for the global page is within the
    /// first `limit` rows of its own author.
    fn timeline_posts_page(
        &self,
        actor: &str,
        cursor: Option<&RecordCursor>,
        limit: usize,
        mut work: Option<&mut StatementWork>,
    ) -> Result<Vec<IndexedRecord>, StorageError> {
        let limit = limit.clamp(1, 101);
        let conn = self.read_conn()?;
        let mut followed = conn.prepare(
            "SELECT DISTINCT json_extract(record_json, '$.subject') FROM records
             WHERE collection='app.bsky.graph.follow' AND did=?1
               AND json_extract(record_json, '$.subject') IS NOT NULL",
        )?;
        let mut authors = followed
            .query_map([actor], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        if let Some(counted) = work.as_mut() {
            counted.observe(&followed);
        }
        drop(followed);
        authors.sort_unstable();
        authors.dedup();
        authors.retain(|subject| subject != actor);
        let mut statement = conn.prepare(
            "SELECT did, collection, rkey, uri, cid, time_us, record_json FROM records
             WHERE collection='app.bsky.feed.post' AND did=?1
               AND (?2 IS NULL OR time_us < ?2 OR (time_us=?2 AND uri < ?3))
             ORDER BY time_us DESC, uri DESC LIMIT ?4",
        )?;
        let mut page = Vec::new();
        for author in std::iter::once(actor).chain(authors.iter().map(String::as_str)) {
            page.extend(
                statement
                    .query_map(
                        params![
                            author,
                            cursor.map(|cursor| cursor.time_us.min(i64::MAX as u64) as i64),
                            cursor.map(|cursor| cursor.uri.as_str()).unwrap_or(""),
                            limit as i64,
                        ],
                        row_to_record,
                    )?
                    .collect::<Result<Vec<_>, _>>()?,
            );
            if let Some(counted) = work.as_mut() {
                counted.observe(&statement);
            }
        }
        drop(statement);
        drop(conn);
        page.sort_by(|left, right| {
            right
                .time_us
                .cmp(&left.time_us)
                .then_with(|| right.uri.cmp(&left.uri))
        });
        page.truncate(limit);
        Ok(page)
    }

    fn timeline_posts_by_post_time_page(
        &self,
        actor: &str,
        cursor: Option<&SortTimeCursor>,
        limit: usize,
        mut work: Option<&mut StatementWork>,
    ) -> Result<Vec<IndexedRecord>, StorageError> {
        let limit = limit.clamp(1, 101);
        // Check before taking a read-pool connection: the uncached first check
        // may need to count old posts and post_sort rows itself.
        let include_unfilled = self.post_sort_backfill_remaining()? > 0;
        let conn = self.read_conn()?;
        let mut followed = conn.prepare(
            "SELECT DISTINCT json_extract(record_json, '$.subject') FROM records
             WHERE collection='app.bsky.graph.follow' AND did=?1
               AND json_extract(record_json, '$.subject') IS NOT NULL",
        )?;
        let mut authors = followed
            .query_map([actor], |row| row.get::<_, String>(0))?
            .collect::<Result<Vec<_>, _>>()?;
        if let Some(counted) = work.as_mut() {
            counted.observe(&followed);
        }
        drop(followed);
        authors.sort_unstable();
        authors.dedup();
        authors.retain(|subject| subject != actor);

        // Every new post is written to post_sort in the same transaction as
        // records. The fallback is only needed while the startup/backfill walk
        // is filling older posts; once it reaches zero, scanning post history
        // to prove that no rows are missing would turn every page into an
        // author-history walk.
        let mut filled = conn.prepare(
            "SELECT r.did, r.collection, r.rkey, r.uri, r.cid, r.time_us, r.record_json, p.sort_us
             FROM post_sort AS p
             INDEXED BY post_sort_author_order
             JOIN records AS r INDEXED BY records_collection_uri
               ON r.collection='app.bsky.feed.post' AND r.uri = p.uri
             WHERE p.did=?1
               AND (p.sort_us, p.time_us, p.uri) < (?2, ?3, ?4)
             ORDER BY p.sort_us DESC, p.time_us DESC, p.uri DESC
             LIMIT ?5",
        )?;
        let mut unfilled = if include_unfilled {
            Some(conn.prepare(
                "SELECT r.did, r.collection, r.rkey, r.uri, r.cid, r.time_us, r.record_json
             FROM records AS r
             INDEXED BY records_author_time
             WHERE r.collection='app.bsky.feed.post' AND r.did=?1
               AND NOT EXISTS (SELECT 1 FROM post_sort AS p WHERE p.uri = r.uri)
               AND (?2 IS NULL
                    OR r.time_us < ?2
                    OR (r.time_us = ?2
                        AND (r.time_us < ?3 OR (r.time_us = ?3 AND r.uri < ?4))))
             ORDER BY r.time_us DESC, r.uri DESC
             LIMIT ?5",
            )?)
        } else {
            None
        };

        let mut page = Vec::new();
        for author in std::iter::once(actor).chain(authors.iter().map(String::as_str)) {
            let cursor_sort = cursor
                .map(|cursor| cursor.sort_us.min(i64::MAX as u64) as i64)
                .unwrap_or(i64::MAX);
            let cursor_time = cursor
                .map(|cursor| cursor.time_us.min(i64::MAX as u64) as i64)
                .unwrap_or(i64::MAX);
            let cursor_uri = cursor
                .map(|cursor| cursor.uri.as_str())
                .unwrap_or("\u{10ffff}");
            page.extend(
                filled
                    .query_map(
                        params![author, cursor_sort, cursor_time, cursor_uri, limit as i64],
                        |row| {
                            let record = row_to_record(row)?;
                            let sort_us = row.get::<_, i64>(7)?;
                            Ok((sort_us.max(0) as u64, record))
                        },
                    )?
                    .collect::<Result<Vec<_>, _>>()?,
            );
            if let Some(counted) = work.as_mut() {
                counted.observe(&filled);
            }
            if let Some(unfilled) = unfilled.as_mut() {
                page.extend(
                    unfilled
                        .query_map(
                            params![author, cursor_sort, cursor_time, cursor_uri, limit as i64],
                            |row| {
                                let record = row_to_record(row)?;
                                Ok((record.time_us, record))
                            },
                        )?
                        .collect::<Result<Vec<_>, _>>()?,
                );
            }
            if let Some(counted) = work.as_mut()
                && let Some(unfilled) = unfilled.as_ref()
            {
                counted.observe(unfilled);
            }
        }
        drop(filled);
        drop(unfilled);
        drop(conn);
        page.sort_by(|left, right| {
            right
                .0
                .cmp(&left.0)
                .then_with(|| right.1.time_us.cmp(&left.1.time_us))
                .then_with(|| right.1.uri.cmp(&left.1.uri))
        });
        page.truncate(limit);
        Ok(page.iter().map(|entry| entry.1.clone()).collect::<Vec<_>>())
    }

    fn pace_snapshot_writes(
        records_per_second: Option<u64>,
        write_started: Instant,
        records_written: u64,
    ) {
        let Some(rate) = records_per_second else {
            return;
        };
        let target_nanos = u128::from(records_written)
            .saturating_mul(1_000_000_000)
            .checked_div(u128::from(rate.max(1)))
            .unwrap_or(u128::MAX)
            .min(u128::from(u64::MAX)) as u64;
        if let Some(remaining) =
            Duration::from_nanos(target_nanos).checked_sub(write_started.elapsed())
        {
            std::thread::sleep(remaining);
        }
    }

    fn wait_for_backfill_batch(&self) {
        let started = Instant::now();
        while crate::backfill::backfill_wait_required(
            self.checkpoint_backlog.load(Ordering::Relaxed),
            crate::backfill::MAX_WRITE_BACKLOG,
            self.pressure.is_active(),
        ) {
            std::thread::sleep(Duration::from_millis(10));
        }
        let waited = started.elapsed();
        self.wal_metrics.backfill_wait_ns.fetch_add(
            u64::try_from(waited.as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }

    fn delete_snapshot_rows(
        &self,
        did: &str,
        sql: &str,
        bounds: SnapshotDeleteBounds,
        records_per_second: Option<u64>,
        write_started: Instant,
        records_written: &mut u64,
    ) -> Result<(), StorageError> {
        loop {
            self.wait_for_backfill_batch();
            let writer_did = did.to_owned();
            let writer_sql = sql.to_owned();
            let deleted = self.writer.call(move |connection| {
                let transaction = connection.transaction()?;
                let deleted = match bounds.window_start_us {
                    Some(window_start_us) => transaction.execute(
                        &writer_sql,
                        params![
                            writer_did,
                            bounds.cutoff_us,
                            SNAPSHOT_BATCH as i64,
                            window_start_us
                        ],
                    ),
                    None => transaction.execute(
                        &writer_sql,
                        params![writer_did, bounds.cutoff_us, SNAPSHOT_BATCH as i64],
                    ),
                }?;
                transaction.commit()?;
                Ok(deleted)
            })?;
            if deleted == 0 {
                return Ok(());
            }
            *records_written = records_written.saturating_add(deleted as u64);
            Self::pace_snapshot_writes(records_per_second, write_started, *records_written);
            std::thread::yield_now();
        }
    }

    fn delete_snapshot_follows(
        &self,
        did: &str,
        rowid_cutoff: i64,
        records_per_second: Option<u64>,
        write_started: Instant,
        records_written: &mut u64,
    ) -> Result<(), StorageError> {
        loop {
            self.wait_for_backfill_batch();
            let writer_did = did.to_owned();
            let (actor_is_registered, subjects, deleted) = self.writer.call(move |connection| {
                let transaction = connection.transaction()?;
                let actor_is_registered: bool = transaction.query_row(
                    "SELECT EXISTS(SELECT 1 FROM registered_users WHERE did=?1)",
                    [&writer_did],
                    |row| row.get(0),
                )?;
                let subjects = {
                    let mut statement = transaction.prepare(
                        "SELECT subject FROM follows WHERE following=1 AND rowid IN (
                             SELECT rowid FROM follows WHERE actor=?1 AND rowid<=?2
                             ORDER BY rowid LIMIT ?3
                         )",
                    )?;
                    statement
                        .query_map(
                            params![writer_did, rowid_cutoff, SNAPSHOT_BATCH as i64],
                            |row| row.get::<_, String>(0),
                        )?
                        .collect::<Result<Vec<_>, _>>()?
                };
                let deleted = transaction.execute(
                    "DELETE FROM follows WHERE rowid IN (
                         SELECT rowid FROM follows WHERE actor=?1 AND rowid<=?2
                         ORDER BY rowid LIMIT ?3
                     )",
                    params![writer_did, rowid_cutoff, SNAPSHOT_BATCH as i64],
                )?;
                transaction.commit()?;
                Ok((actor_is_registered, subjects, deleted))
            })?;
            if actor_is_registered {
                let mut tracked = self.tracked.write().map_err(|_| StorageError::Poisoned)?;
                for subject in &subjects {
                    tracked.apply_follow_delta(subject, -1);
                }
            }
            if deleted == 0 {
                return Ok(());
            }
            *records_written = records_written.saturating_add(deleted as u64);
            Self::pace_snapshot_writes(records_per_second, write_started, *records_written);
            std::thread::yield_now();
        }
    }

    fn replace_repo_snapshot_inner(
        &self,
        did: &str,
        records: &[IndexedRecord],
        following: &[String],
        records_per_second: Option<u64>,
        window_start_us: Option<i64>,
    ) -> Result<(), StorageError> {
        let record_json = records
            .iter()
            .map(|record| serde_json::to_string(&record.record))
            .collect::<Result<Vec<_>, _>>()?;
        let write_started = Instant::now();
        let cutoff_us = epoch_micros();
        let writer_did = did.to_owned();
        let (follow_rowid_cutoff, scope_rowid_cutoff) = self.writer.call(move |connection| {
            let follow_rowid_cutoff: i64 = connection.query_row(
                "SELECT COALESCE(MAX(rowid),0) FROM follows WHERE actor=?1",
                [&writer_did],
                |row| row.get(0),
            )?;
            let scope_rowid_cutoff: i64 = connection.query_row(
                "SELECT COALESCE(MAX(rowid),0) FROM backfill_scope
                 WHERE owner_did=?1 AND target_did!=?1",
                [&writer_did],
                |row| row.get(0),
            )?;
            Ok((follow_rowid_cutoff, scope_rowid_cutoff))
        })?;
        let mut records_written = 0_u64;
        // A windowed snapshot only proves what happened since its window
        // start, so for the windowed collections it may delete rows from the
        // window start onward only; older rows (from older-on-scroll pages or
        // the firehose) stand. The other collections are current state and
        // are replaced in full as always.
        let windowed = window_start_us.is_some();
        let records_sql = if windowed {
            "DELETE FROM records WHERE rowid IN (
                 SELECT rowid FROM records WHERE did=?1 AND time_us<=?2
                     AND (time_us>=?4 OR collection NOT IN ('app.bsky.feed.post','app.bsky.feed.like','app.bsky.feed.repost'))
                 ORDER BY rowid LIMIT ?3
             )"
        } else {
            "DELETE FROM records WHERE rowid IN (
                 SELECT rowid FROM records WHERE did=?1 AND time_us<=?2 ORDER BY rowid LIMIT ?3
             )"
        };
        self.delete_snapshot_rows(
            did,
            records_sql,
            SnapshotDeleteBounds {
                cutoff_us,
                window_start_us,
            },
            records_per_second,
            write_started,
            &mut records_written,
        )?;
        let events_sql = if windowed {
            "DELETE FROM notification_events WHERE rowid IN (
                 SELECT rowid FROM notification_events WHERE source_did=?1 AND time_us<=?2
                     AND (time_us>=?4 OR source_collection NOT IN ('app.bsky.feed.post','app.bsky.feed.like','app.bsky.feed.repost'))
                 ORDER BY rowid LIMIT ?3
             )"
        } else {
            "DELETE FROM notification_events WHERE rowid IN (
                 SELECT rowid FROM notification_events WHERE source_did=?1 AND time_us<=?2 ORDER BY rowid LIMIT ?3
             )"
        };
        self.delete_snapshot_rows(
            did,
            events_sql,
            SnapshotDeleteBounds {
                cutoff_us,
                window_start_us,
            },
            records_per_second,
            write_started,
            &mut records_written,
        )?;
        self.delete_snapshot_rows(
            did,
            "DELETE FROM backfill_scope WHERE rowid IN (
                 SELECT rowid FROM backfill_scope
                 WHERE owner_did=?1 AND target_did!=?1 AND rowid<=?2
                 ORDER BY rowid LIMIT ?3
             )",
            SnapshotDeleteBounds {
                cutoff_us: scope_rowid_cutoff,
                window_start_us: None,
            },
            records_per_second,
            write_started,
            &mut records_written,
        )?;
        let follow_batches = following.chunks(SNAPSHOT_BATCH).collect::<Vec<_>>();
        for (batch_index, batch) in follow_batches.iter().enumerate() {
            self.wait_for_backfill_batch();
            let writer_did = did.to_owned();
            let writer_batch = batch.iter().map(|did| (*did).clone()).collect::<Vec<_>>();
            let (registered_for_tracker, inserted_subjects, inserted_follow_rows) =
                self.writer.call(move |connection| {
                    let transaction = connection.transaction()?;
                    let registered_for_tracker: bool = transaction.query_row(
                        "SELECT EXISTS(SELECT 1 FROM registered_users WHERE did=?1)",
                        [&writer_did],
                        |row| row.get(0),
                    )?;
                    let mut inserted_follow_rows = 0_u64;
                    let mut inserted_subjects = Vec::new();
                    {
                        // Inserting before the stale-row cleanup is what keeps
                        // a re-snapshot from ever blanking rows its records
                        // still back. A snapshotted subject whose row already
                        // says following=1 is re-inserted with REPLACE so it
                        // gets a fresh rowid (> the captured cutoff) and the
                        // cleanup cannot touch it; a row an outside unfollow
                        // left at following=0 stands (the old INSERT OR IGNORE
                        // never overwrote it either); only absent rows insert.
                        let mut insert_follow = transaction.prepare(
                            "INSERT OR REPLACE INTO follows(actor,subject,following) VALUES(?1,?2,1)",
                        )?;
                        let mut insert_scope = transaction.prepare(
                            "INSERT OR IGNORE INTO backfill_scope(owner_did,target_did) VALUES(?1,?2)",
                        )?;
                        let mut enqueue = transaction.prepare(
                            "INSERT OR IGNORE INTO backfill_queue(did)
                             SELECT ?1 WHERE (SELECT COUNT(*) FROM backfill_queue) < ?2
                                OR EXISTS(SELECT 1 FROM backfill_queue WHERE did=?1)",
                        )?;
                        let mut set_pending = transaction.prepare(
                            "INSERT INTO backfill_status(did,done) VALUES(?1,0)
                             ON CONFLICT(did) DO UPDATE SET done=0",
                        )?;
                        for subject in writer_batch {
                            let previous_following: Option<bool> = transaction.query_row(
                                "SELECT following FROM follows WHERE actor=?1 AND subject=?2",
                                params![writer_did, subject],
                                |row| row.get(0),
                            )
                            .optional()?;
                            match previous_following {
                                // An outside unfollow stands: the row is left
                                // exactly as it is, unanswered by this snapshot.
                                Some(false) => continue,
                                Some(true) => {
                                    insert_follow.execute(params![writer_did, subject])?;
                                }
                                None => {
                                    let inserted =
                                        insert_follow.execute(params![writer_did, subject])?;
                                    inserted_follow_rows =
                                        inserted_follow_rows.saturating_add(inserted as u64);
                                    inserted_subjects.push(subject.clone());
                                }
                            }
                            if registered_for_tracker {
                                enqueue.execute(params![subject, MAX_BACKFILL_QUEUE])?;
                                if transaction.query_row(
                                    "SELECT EXISTS(SELECT 1 FROM backfill_queue WHERE did=?1)",
                                    [&subject],
                                    |row| row.get::<_, bool>(0),
                                )? {
                                    insert_scope.execute(params![writer_did, subject])?;
                                    set_pending.execute([subject])?;
                                }
                            }
                        }
                    }
                    transaction.commit()?;
                    Ok((registered_for_tracker, inserted_subjects, inserted_follow_rows))
                })?;
            if registered_for_tracker {
                let mut tracked = self.tracked.write().map_err(|_| StorageError::Poisoned)?;
                for subject in inserted_subjects {
                    tracked.apply_follow_delta(&subject, 1);
                }
            }
            std::thread::yield_now();
            records_written = records_written.saturating_add(inserted_follow_rows);
            if records_per_second.is_some() && batch_index + 1 < follow_batches.len() {
                Self::pace_snapshot_writes(records_per_second, write_started, records_written);
            }
        }
        // Cleanup runs only after every snapshotted follow row is live: it
        // removes the actor's pre-snapshot rows (rowid <= cutoff) that the
        // REPLACE inserts did not replace, i.e. follows the repo dropped.
        self.delete_snapshot_follows(
            did,
            follow_rowid_cutoff,
            records_per_second,
            write_started,
            &mut records_written,
        )?;

        let total_batches = records.len().div_ceil(SNAPSHOT_BATCH);
        for (batch_index, (batch, batch_json)) in records
            .chunks(SNAPSHOT_BATCH)
            .zip(record_json.chunks(SNAPSHOT_BATCH))
            .enumerate()
        {
            self.wait_for_backfill_batch();
            let writer_batch = batch.to_vec();
            let writer_json = batch_json.to_vec();
            let inserted_records = self.writer.call(move |connection| {
                let transaction = connection.transaction()?;
                let mut inserted_records = 0_u64;
                {
                    let mut insert = transaction.prepare(
                        "INSERT INTO records (did, collection, rkey, uri, cid, time_us, record_json)
                         VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                         ON CONFLICT(did, collection, rkey) DO NOTHING",
                    )?;
                    for (record, json) in writer_batch.iter().zip(&writer_json) {
                        let inserted = insert.execute(params![
                            record.did,
                            record.collection,
                            record.rkey,
                            record.uri,
                            record.cid,
                            record.time_us,
                            json,
                        ])?;
                        if inserted > 0 {
                            inserted_records += inserted as u64;
                            if record.collection == "app.bsky.feed.post" {
                                let sort_us = post_sort_us(record);
                                transaction.execute(
                                    "INSERT INTO post_sort(uri,did,sort_us,time_us)
                                     VALUES(?1,?2,?3,?4)
                                     ON CONFLICT(uri) DO UPDATE SET
                                         did=excluded.did, sort_us=excluded.sort_us, time_us=excluded.time_us",
                                    params![
                                        record.uri,
                                        record.did,
                                        sort_us.min(i64::MAX as u64) as i64,
                                        record.time_us.min(i64::MAX as u64) as i64,
                                    ],
                                )?;
                            }
                            write_notification_events(&transaction, record)?;
                        }
                    }
                }
                transaction.commit()?;
                Ok(inserted_records)
            })?;
            // std's Mutex is not fair: without a yield this thread takes it straight back.
            std::thread::yield_now();
            records_written = records_written.saturating_add(inserted_records);

            if records_per_second.is_some() && batch_index + 1 < total_batches {
                Self::pace_snapshot_writes(records_per_second, write_started, records_written);
            }
        }
        Ok(())
    }
}

impl Storage for SqliteStorage {
    fn record_jetstream_lock_retry(&self) {
        self.writer
            .metrics
            .jetstream_lock_retries
            .fetch_add(1, Ordering::Relaxed);
    }

    fn write_backlog(&self) -> u64 {
        self.checkpoint_backlog.load(Ordering::Relaxed)
    }

    fn write_pressure(&self) -> bool {
        self.pressure.is_active()
    }

    fn record_backfill_wait(&self, waited: Duration) {
        self.wal_metrics.backfill_wait_ns.fetch_add(
            u64::try_from(waited.as_nanos()).unwrap_or(u64::MAX),
            Ordering::Relaxed,
        );
    }

    fn put_record(&self, record: &IndexedRecord) -> Result<(), StorageError> {
        let affected = notification_edges(record)
            .into_iter()
            .map(|(_, subject_did, _, _)| subject_did)
            .collect::<Vec<_>>();
        let record_json = serde_json::to_string(&record.record)?;
        let writer_record = record.clone();
        self.writer.call(move |connection| {
            let transaction = connection.transaction()?;
            transaction.execute(
                "INSERT INTO records (did, collection, rkey, uri, cid, time_us, record_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(did, collection, rkey) DO UPDATE SET
                     uri=excluded.uri, cid=excluded.cid, time_us=excluded.time_us, record_json=excluded.record_json",
                params![writer_record.did, writer_record.collection, writer_record.rkey, writer_record.uri, writer_record.cid, writer_record.time_us, record_json],
            )?;
            if writer_record.collection == "app.bsky.feed.post" {
                let sort_us = post_sort_us(&writer_record);
                transaction.execute(
                    "INSERT INTO post_sort(uri,did,sort_us,time_us)
                     VALUES(?1,?2,?3,?4)
                     ON CONFLICT(uri) DO UPDATE SET
                         did=excluded.did, sort_us=excluded.sort_us, time_us=excluded.time_us",
                    params![
                        writer_record.uri,
                        writer_record.did,
                        sort_us.min(i64::MAX as u64) as i64,
                        writer_record.time_us.min(i64::MAX as u64) as i64,
                    ],
                )?;
            }
            write_notification_events(&transaction, &writer_record)?;
            transaction.commit()?;
            Ok(())
        })?;
        if record.collection == "app.bsky.actor.profile" {
            self.write_profile_index_status(
                &record.did,
                ProfileIndexEntry {
                    has_profile: true,
                    fetched_at_ms: profile_time_ms(record.time_us),
                },
            )?;
        }
        publish_notification_counts(self, &self.notification_hub, affected);
        Ok(())
    }

    /// Queues a profile record without waiting for the writer. The record is
    /// immediately readable through `get_record` from a pending overlay, and
    /// the overlay entry is dropped when the write runs, so the page can
    /// render freshly fetched authors while the writer is seconds behind. Only
    /// profile cache warms take the async path: anything with notification
    /// edges (or any other collection) goes through the acknowledged
    /// `put_record`, and a full writer queue simply drops the warm. A key with
    /// a pending task is never queued twice: the caller updates the pending
    /// content in place and the queued task writes whatever the overlay holds
    /// when it runs, so the overlay size is bounded by the writer queue.
    fn queue_record_write(&self, record: &IndexedRecord) -> Result<(), StorageError> {
        if record.collection != "app.bsky.actor.profile" || !notification_edges(record).is_empty() {
            return self.put_record(record);
        }
        let key = (
            record.did.clone(),
            record.collection.clone(),
            record.rkey.clone(),
        );
        let already_pending = {
            let mut pending = self
                .pending_record_writes
                .write()
                .map_err(|_| StorageError::Poisoned)?;
            let already_pending = pending.contains_key(&key);
            pending.insert(key.clone(), record.clone());
            already_pending
        };
        if already_pending {
            return Ok(());
        }
        let overlay = Arc::clone(&self.pending_record_writes);
        let overlay_key = key.clone();
        let index_did = record.did.clone();
        let index_time_ms = profile_time_ms(record.time_us);
        let enqueued = self.writer.fire(move |connection| {
            let Some(pending_record) = overlay
                .write()
                .ok()
                .and_then(|mut pending| pending.remove(&overlay_key))
            else {
                return Ok(None);
            };
            let record_json = serde_json::to_string(&pending_record.record)?;
            connection.execute(
                "INSERT INTO records (did, collection, rkey, uri, cid, time_us, record_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                 ON CONFLICT(did, collection, rkey) DO UPDATE SET
                     uri=excluded.uri, cid=excluded.cid, time_us=excluded.time_us, record_json=excluded.record_json",
                params![
                    pending_record.did,
                    pending_record.collection,
                    pending_record.rkey,
                    pending_record.uri,
                    pending_record.cid,
                    pending_record.time_us,
                    record_json
                ],
            )?;
            connection.execute(
                "INSERT INTO profile_index_status(did,has_profile,fetched_at_ms)
                 VALUES(?1,?2,?3)
                 ON CONFLICT(did) DO UPDATE SET
                     has_profile=excluded.has_profile,fetched_at_ms=excluded.fetched_at_ms",
                params![index_did, true, index_time_ms],
            )?;
            Ok(None)
        });
        if !enqueued {
            self.pending_record_writes
                .write()
                .map_err(|_| StorageError::Poisoned)?
                .remove(&key);
            tracing::debug!(
                record_uri = %record.uri,
                "queue full; dropped a fire-and-forget profile cache write"
            );
            return Ok(());
        }
        self.profile_index
            .write()
            .map_err(|_| StorageError::Poisoned)?
            .insert(
                record.did.clone(),
                ProfileIndexEntry {
                    has_profile: true,
                    fetched_at_ms: index_time_ms,
                },
            );
        Ok(())
    }

    fn apply_jetstream_batch(
        &self,
        updates: &[JetstreamStorageUpdate],
    ) -> Result<BatchApplyTiming, StorageError> {
        if updates.is_empty() {
            return Ok(BatchApplyTiming::default());
        }
        let writer_updates = updates.to_vec();
        // Jetstream updates share the writer with snapshots. Mark them urgent so
        // the writer can pick live events ahead of queued backfill batches.
        let (timing, tracked_follow_deltas, affected) =
            self.writer.call_live(move |connection| {
            let apply_started = Instant::now();
            let transaction = connection.transaction()?;
            let mut tracked_follow_deltas = HashMap::<String, i64>::new();
            let mut affected = Vec::new();
            for update in &writer_updates {
            if let Some(did) = &update.handle_refresh {
                transaction.execute(
                    "INSERT OR IGNORE INTO handle_refresh_queue(did) VALUES(?1)",
                    [did],
                )?;
            }
            if let Some(record) = &update.upsert {
                affected.extend(
                    notification_edges(record)
                        .into_iter()
                        .map(|(_, subject_did, _, _)| subject_did),
                );
                let record_json = serde_json::to_string(&record.record)?;
                transaction.execute(
                    "INSERT INTO records (did, collection, rkey, uri, cid, time_us, record_json)
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
                     ON CONFLICT(did, collection, rkey) DO UPDATE SET
                         uri=excluded.uri, cid=excluded.cid, time_us=excluded.time_us,
                         record_json=excluded.record_json",
                    params![
                        record.did,
                        record.collection,
                        record.rkey,
                        record.uri,
                        record.cid,
                        record.time_us,
                        record_json,
                    ],
                )?;
                if record.collection == "app.bsky.feed.post" {
                    let sort_us = post_sort_us(record);
                    transaction.execute(
                        "INSERT INTO post_sort(uri,did,sort_us,time_us)
                         VALUES(?1,?2,?3,?4)
                         ON CONFLICT(uri) DO UPDATE SET
                             did=excluded.did, sort_us=excluded.sort_us, time_us=excluded.time_us",
                        params![
                            record.uri,
                            record.did,
                            sort_us.min(i64::MAX as u64) as i64,
                            record.time_us.min(i64::MAX as u64) as i64,
                        ],
                    )?;
                }
                write_notification_events(&transaction, record)?;
            }
            if let Some((did, collection, rkey)) = &update.delete {
                let mut statement = transaction.prepare(
                    "SELECT DISTINCT subject_did FROM notification_events
                     WHERE source_did=?1 AND source_collection=?2 AND source_rkey=?3",
                )?;
                affected.extend(
                    statement
                        .query_map(params![did, collection, rkey], |row| {
                            row.get::<_, String>(0)
                        })?
                        .collect::<Result<Vec<_>, _>>()?,
                );
                transaction.execute(
                    "DELETE FROM records WHERE did=?1 AND collection=?2 AND rkey=?3",
                    params![did, collection, rkey],
                )?;
                transaction.execute(
                    "DELETE FROM notification_events WHERE source_did=?1 AND source_collection=?2 AND source_rkey=?3",
                    params![did, collection, rkey],
                )?;
            }
            for follow in &update.follows {
                let actor_is_registered: bool = transaction.query_row(
                    "SELECT EXISTS(SELECT 1 FROM registered_users WHERE did=?1)",
                    [&follow.actor],
                    |row| row.get(0),
                )?;
                let was_following: Option<bool> = transaction
                    .query_row(
                        "SELECT following FROM follows WHERE actor=?1 AND subject=?2",
                        params![follow.actor, follow.subject],
                        |row| row.get(0),
                    )
                    .optional()?;
                let was_following = was_following.unwrap_or(false);
                transaction.execute(
                    "INSERT OR REPLACE INTO follows(actor,subject,following) VALUES(?1,?2,?3)",
                    params![follow.actor, follow.subject, follow.following],
                )?;
                if actor_is_registered && was_following != follow.following {
                    *tracked_follow_deltas
                        .entry(follow.subject.clone())
                        .or_default() += if follow.following { 1 } else { -1 };
                }
                if !follow.following {
                    transaction.execute(
                        "DELETE FROM backfill_scope WHERE owner_did=?1 AND target_did=?2",
                        params![follow.actor, follow.subject],
                    )?;
                }
            }
            for (owner_did, did) in &update.backfill_for {
                transaction.execute(
                    "INSERT OR IGNORE INTO backfill_queue(did)
                     SELECT ?2
                     WHERE ((SELECT COUNT(*) FROM backfill_queue) < ?3
                            OR EXISTS(SELECT 1 FROM backfill_queue WHERE did=?2))
                       AND EXISTS(SELECT 1 FROM registered_users WHERE did=?1)
                       AND (?1=?2 OR EXISTS(
                           SELECT 1 FROM follows WHERE actor=?1 AND subject=?2 AND following=1
                       ))",
                    params![owner_did, did, MAX_BACKFILL_QUEUE],
                )?;
                transaction.execute(
                    "INSERT OR REPLACE INTO backfill_scope(owner_did,target_did)
                     SELECT ?1,?2 WHERE EXISTS(SELECT 1 FROM backfill_queue WHERE did=?2)
                       AND EXISTS(SELECT 1 FROM registered_users WHERE did=?1)
                       AND (?1=?2 OR EXISTS(
                           SELECT 1 FROM follows WHERE actor=?1 AND subject=?2 AND following=1
                       ))",
                    params![owner_did, did],
                )?;
                transaction.execute(
                    "INSERT INTO backfill_status(did,done) VALUES(?1,0)
                     ON CONFLICT(did) DO UPDATE SET done=0
                     WHERE EXISTS(SELECT 1 FROM backfill_queue WHERE did=?1)",
                    [did],
                )?;
            }
            }
            transaction.execute(
                "INSERT INTO appview_state(key,value) VALUES('jetstream_cursor',?1)
                 ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                [writer_updates.last().expect("nonempty batch").time_us.to_string()],
            )?;
            let apply = apply_started.elapsed();
            let commit_started = Instant::now();
            transaction.commit()?;
            Ok((
                BatchApplyTiming {
                    apply,
                    commit: commit_started.elapsed(),
                },
                tracked_follow_deltas,
                affected,
            ))
        })?;
        for update in updates {
            if let Some(record) = &update.upsert
                && record.collection == "app.bsky.actor.profile"
            {
                self.write_profile_index_status(
                    &record.did,
                    ProfileIndexEntry {
                        has_profile: true,
                        fetched_at_ms: profile_time_ms(record.time_us),
                    },
                )?;
            }
            if let Some((did, collection, _)) = &update.delete
                && collection == "app.bsky.actor.profile"
            {
                self.write_profile_index_status(
                    did,
                    ProfileIndexEntry {
                        has_profile: false,
                        fetched_at_ms: epoch_millis(),
                    },
                )?;
            }
        }
        self.apply_tracked_follow_deltas(&tracked_follow_deltas)?;
        publish_notification_counts(self, &self.notification_hub, affected);
        Ok(timing)
    }

    fn record_skipped_window(&self, from: u64, to: u64) -> Result<(), StorageError> {
        self.writer.call(move |connection| {
            let transaction = connection.transaction()?;
            let stored: Option<String> = transaction
                .query_row(
                    "SELECT value FROM appview_state WHERE key='jetstream_skipped_windows'",
                    [],
                    |row| row.get(0),
                )
                .optional()?;
            let mut windows = stored
                .as_deref()
                .map(serde_json::from_str::<Vec<SkippedCursorWindow>>)
                .transpose()?
                .unwrap_or_default();
            windows.push(SkippedCursorWindow { from, to });
            if windows.len() > 128 {
                windows.drain(..windows.len() - 128);
            }
            transaction.execute(
                "INSERT INTO appview_state(key,value) VALUES('jetstream_skipped_windows',?1)
                 ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                [serde_json::to_string(&windows)?],
            )?;
            transaction.execute(
                "INSERT INTO appview_state(key,value) VALUES('jetstream_cursor',?1)
                 ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                [to.to_string()],
            )?;
            transaction.commit()?;
            Ok(())
        })
    }

    fn skipped_windows(&self) -> Result<Vec<SkippedCursorWindow>, StorageError> {
        let stored: Option<String> = self
            .read_conn()?
            .query_row(
                "SELECT value FROM appview_state WHERE key='jetstream_skipped_windows'",
                [],
                |row| row.get(0),
            )
            .optional()?;
        stored
            .as_deref()
            .map(serde_json::from_str)
            .transpose()
            .map(Option::unwrap_or_default)
            .map_err(Into::into)
    }

    fn replace_repo_snapshot(
        &self,
        did: &str,
        records: &[IndexedRecord],
        following: &[String],
    ) -> Result<(), StorageError> {
        let mut affected = {
            let connection = self.read_conn()?;
            let mut statement = connection.prepare(
                "SELECT DISTINCT subject_did FROM notification_events WHERE source_did=?1",
            )?;
            statement
                .query_map([did], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?
        };
        affected.extend(
            records
                .iter()
                .flat_map(notification_edges)
                .map(|(_, subject_did, _, _)| subject_did),
        );
        self.replace_repo_snapshot_inner(did, records, following, None, None)?;
        self.refresh_profile_index_for_did(did)?;
        publish_notification_counts(self, &self.notification_hub, affected);
        Ok(())
    }

    fn replace_repo_snapshot_windowed(
        &self,
        did: &str,
        records: &[IndexedRecord],
        following: &[String],
        records_per_second: u64,
        window_start_us: Option<u64>,
    ) -> Result<(), StorageError> {
        let mut affected = {
            let connection = self.read_conn()?;
            let mut statement = connection.prepare(
                "SELECT DISTINCT subject_did FROM notification_events WHERE source_did=?1",
            )?;
            statement
                .query_map([did], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?
        };
        affected.extend(
            records
                .iter()
                .flat_map(notification_edges)
                .map(|(_, subject_did, _, _)| subject_did),
        );
        self.replace_repo_snapshot_inner(
            did,
            records,
            following,
            Some(records_per_second),
            window_start_us.map(|us| us.min(i64::MAX as u64) as i64),
        )?;
        self.refresh_profile_index_for_did(did)?;
        publish_notification_counts(self, &self.notification_hub, affected);
        Ok(())
    }

    fn replace_repo_snapshot_throttled(
        &self,
        did: &str,
        records: &[IndexedRecord],
        following: &[String],
        records_per_second: u64,
    ) -> Result<(), StorageError> {
        let _operation = SqliteOperationKindGuard::enter("backfill");
        let mut affected = {
            let connection = self.read_conn()?;
            let mut statement = connection.prepare(
                "SELECT DISTINCT subject_did FROM notification_events WHERE source_did=?1",
            )?;
            statement
                .query_map([did], |row| row.get::<_, String>(0))?
                .collect::<Result<Vec<_>, _>>()?
        };
        affected.extend(
            records
                .iter()
                .flat_map(notification_edges)
                .map(|(_, subject_did, _, _)| subject_did),
        );
        self.replace_repo_snapshot_inner(did, records, following, Some(records_per_second), None)?;
        self.refresh_profile_index_for_did(did)?;
        publish_notification_counts(self, &self.notification_hub, affected);
        Ok(())
    }

    fn delete_record(&self, did: &str, collection: &str, rkey: &str) -> Result<(), StorageError> {
        let affected = {
            let connection = self.read_conn()?;
            let mut statement = connection.prepare(
                "SELECT DISTINCT subject_did FROM notification_events
                 WHERE source_did=?1 AND source_collection=?2 AND source_rkey=?3",
            )?;
            statement
                .query_map(params![did, collection, rkey], |row| {
                    row.get::<_, String>(0)
                })?
                .collect::<Result<Vec<_>, _>>()?
        };
        let writer_did = did.to_owned();
        let writer_collection = collection.to_owned();
        let writer_rkey = rkey.to_owned();
        self.writer.call(move |connection| {
            let transaction = connection.transaction()?;
            transaction.execute(
                "DELETE FROM records WHERE did=?1 AND collection=?2 AND rkey=?3",
                params![writer_did, writer_collection, writer_rkey],
            )?;
            transaction.execute(
                "DELETE FROM notification_events WHERE source_did=?1 AND source_collection=?2 AND source_rkey=?3",
                params![writer_did, writer_collection, writer_rkey],
            )?;
            transaction.commit()?;
            Ok(())
        })?;
        if collection == "app.bsky.actor.profile" {
            self.write_profile_index_status(
                did,
                ProfileIndexEntry {
                    has_profile: false,
                    fetched_at_ms: epoch_millis(),
                },
            )?;
        }
        publish_notification_counts(self, &self.notification_hub, affected);
        Ok(())
    }

    fn get_record(
        &self,
        did: &str,
        collection: &str,
        rkey: &str,
    ) -> Result<Option<IndexedRecord>, StorageError> {
        if let Ok(pending) = self.pending_record_writes.read()
            && let Some(record) =
                pending.get(&(did.to_owned(), collection.to_owned(), rkey.to_owned()))
        {
            return Ok(Some(record.clone()));
        }
        self.read_conn()?
            .query_row(
                "SELECT did, collection, rkey, uri, cid, time_us, record_json
                 FROM records WHERE did=?1 AND collection=?2 AND rkey=?3",
                params![did, collection, rkey],
                row_to_record,
            )
            .optional()
            .map_err(Into::into)
    }

    fn record_by_uri(
        &self,
        collection: &str,
        uri: &str,
    ) -> Result<Option<IndexedRecord>, StorageError> {
        self.read_conn()?
            .query_row(
                "SELECT did, collection, rkey, uri, cid, time_us, record_json FROM records WHERE collection=?1 AND uri=?2",
                params![collection, uri],
                row_to_record,
            )
            .optional()
            .map_err(Into::into)
    }

    fn records_by_uri(
        &self,
        collection: &str,
        uris: &[String],
    ) -> Result<HashMap<String, IndexedRecord>, StorageError> {
        if uris.is_empty() {
            return Ok(HashMap::new());
        }
        let placeholders = std::iter::repeat_n("?", uris.len())
            .collect::<Vec<_>>()
            .join(",");
        let sql = format!(
            "SELECT did, collection, rkey, uri, cid, time_us, record_json FROM records
             WHERE collection=? AND uri IN ({placeholders})"
        );
        let connection = self.read_conn()?;
        let mut statement = connection.prepare(&sql)?;
        let params = std::iter::once(collection).chain(uris.iter().map(String::as_str));
        let records = statement
            .query_map(params_from_iter(params), row_to_record)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(records
            .into_iter()
            .map(|record| (record.uri.clone(), record))
            .collect())
    }

    fn replies_to(
        &self,
        parent_uri: &str,
        limit: usize,
    ) -> Result<Vec<IndexedRecord>, StorageError> {
        let conn = self.read_conn()?;
        let mut statement = conn.prepare(
            "SELECT did, collection, rkey, uri, cid, time_us, record_json FROM records
             WHERE collection='app.bsky.feed.post' AND json_extract(record_json, '$.reply.parent.uri')=?1
             ORDER BY time_us ASC, uri ASC LIMIT ?2",
        )?;
        statement
            .query_map(
                params![parent_uri, limit.clamp(1, 100) as i64],
                row_to_record,
            )?
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    fn records_for_subject(
        &self,
        collection: &str,
        subject_uri: &str,
        limit: usize,
    ) -> Result<Vec<IndexedRecord>, StorageError> {
        let conn = self.read_conn()?;
        let mut statement = conn.prepare(
            "SELECT did, collection, rkey, uri, cid, time_us, record_json FROM records
             WHERE collection=?1 AND json_extract(record_json, '$.subject.uri')=?2
             ORDER BY time_us DESC, uri DESC LIMIT ?3",
        )?;
        statement
            .query_map(
                params![collection, subject_uri, limit.clamp(1, 101) as i64],
                row_to_record,
            )?
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    fn records_for_actor_collection(
        &self,
        did: &str,
        collection: &str,
        limit: usize,
    ) -> Result<Vec<IndexedRecord>, StorageError> {
        let conn = self.read_conn()?;
        let mut statement = conn.prepare(
            "SELECT did, collection, rkey, uri, cid, time_us, record_json FROM records
             WHERE did=?1 AND collection=?2 ORDER BY time_us DESC, uri DESC LIMIT ?3",
        )?;
        statement
            .query_map(
                params![did, collection, limit.clamp(1, 10_000) as i64],
                row_to_record,
            )?
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    fn follows_for_actor(
        &self,
        did: &str,
        limit: usize,
    ) -> Result<Vec<IndexedRecord>, StorageError> {
        let conn = self.read_conn()?;
        let mut statement = conn.prepare(
            "SELECT did, collection, rkey, uri, cid, time_us, record_json FROM records
             WHERE collection='app.bsky.graph.follow' AND did=?1
             ORDER BY time_us DESC, uri DESC LIMIT ?2",
        )?;
        statement
            .query_map(params![did, limit.clamp(1, 101) as i64], row_to_record)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    fn followers_for_did(
        &self,
        did: &str,
        limit: usize,
    ) -> Result<Vec<IndexedRecord>, StorageError> {
        self.followers_for_did_page(did, limit, None)
    }

    fn profile_counts(&self, did: &str) -> Result<(usize, usize, usize), StorageError> {
        let counts: (i64, i64, i64) = self.read_conn()?.query_row(
            "SELECT
                (SELECT COUNT(*) FROM records WHERE collection='app.bsky.graph.follow' AND did=?1),
                (SELECT COUNT(*) FROM records WHERE collection='app.bsky.graph.follow' AND json_extract(record_json, '$.subject')=?1),
                (SELECT COUNT(*) FROM records WHERE collection='app.bsky.feed.post' AND did=?1)",
            [did],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        Ok((counts.0 as usize, counts.1 as usize, counts.2 as usize))
    }

    fn profile_index_snapshot(&self) -> Result<HashMap<String, ProfileIndexEntry>, StorageError> {
        Ok(self
            .profile_index
            .read()
            .map_err(|_| StorageError::Poisoned)?
            .clone())
    }

    fn set_profile_index_entry(
        &self,
        did: &str,
        entry: ProfileIndexEntry,
    ) -> Result<(), StorageError> {
        let writer_did = did.to_owned();
        self.writer.fire(move |connection| {
            connection.execute(
                "INSERT INTO profile_index_status(did,has_profile,fetched_at_ms)
                 VALUES(?1,?2,?3)
                 ON CONFLICT(did) DO UPDATE SET
                     has_profile=excluded.has_profile,fetched_at_ms=excluded.fetched_at_ms",
                params![writer_did, entry.has_profile, entry.fetched_at_ms],
            )?;
            Ok(None)
        });
        self.profile_index
            .write()
            .map_err(|_| StorageError::Poisoned)?
            .insert(did.to_owned(), entry);
        Ok(())
    }

    fn enqueue_profile_fetches(&self, dids: &[String]) -> Result<(), StorageError> {
        if dids.is_empty() {
            return Ok(());
        }
        let writer_dids = dids.to_vec();
        self.writer.fire(move |connection| {
            for did in writer_dids {
                connection.execute(
                    "INSERT OR IGNORE INTO profile_fetch_queue(did)
                     SELECT ?1 WHERE (SELECT COUNT(*) FROM profile_fetch_queue) < ?2
                        OR EXISTS(SELECT 1 FROM profile_fetch_queue WHERE did=?1)",
                    params![did, MAX_PROFILE_FETCH_QUEUE],
                )?;
            }
            Ok(None)
        });
        Ok(())
    }

    fn pending_profile_fetches(&self, limit: usize) -> Result<Vec<String>, StorageError> {
        let connection = self.read_conn()?;
        let mut statement =
            connection.prepare("SELECT did FROM profile_fetch_queue ORDER BY seq LIMIT ?1")?;
        statement
            .query_map([i64::try_from(limit.max(1)).unwrap_or(i64::MAX)], |row| {
                row.get(0)
            })?
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    fn complete_profile_fetch(&self, did: &str) -> Result<(), StorageError> {
        let writer_did = did.to_owned();
        self.writer.call(move |connection| {
            connection.execute("DELETE FROM profile_fetch_queue WHERE did=?1", [writer_did])?;
            Ok(())
        })
    }

    fn recent_posts_status(
        &self,
        did: &str,
    ) -> Result<Option<RecentPostsFetchStatus>, StorageError> {
        let status = self
            .recent_posts_status
            .read()
            .map_err(|_| StorageError::Poisoned)?
            .get(did)
            .copied();
        let queued = self
            .recent_posts_queue
            .read()
            .map_err(|_| StorageError::Poisoned)?
            .1
            .contains(did);
        if status.is_none() && !queued {
            return Ok(None);
        }
        let (fetched_at_ms, next_retry_at_ms) = status.unwrap_or_default();
        Ok(Some(RecentPostsFetchStatus {
            fetched_at_ms,
            next_retry_at_ms,
            queued,
        }))
    }

    fn enqueue_recent_posts_fetch(&self, did: &str) -> Result<RecentPostsEnqueue, StorageError> {
        {
            let mut queue = self
                .recent_posts_queue
                .write()
                .map_err(|_| StorageError::Poisoned)?;
            if queue.1.contains(did) {
                return Ok(RecentPostsEnqueue::Duplicate);
            }
            if queue.0.len() >= MAX_RECENT_POSTS_QUEUE {
                self.recent_posts_counters
                    .dropped
                    .fetch_add(1, Ordering::Relaxed);
                return Ok(RecentPostsEnqueue::Overflowed);
            }
            queue.1.insert(did.to_owned());
            queue.0.push_back(did.to_owned());
        }
        self.recent_posts_counters
            .queued
            .fetch_add(1, Ordering::Relaxed);
        let writer_did = did.to_owned();
        self.writer.fire(move |connection| {
            connection.execute(
                "INSERT OR IGNORE INTO recent_posts_queue(did)
                 SELECT ?1 WHERE (SELECT COUNT(*) FROM recent_posts_queue) < ?2",
                params![writer_did, MAX_RECENT_POSTS_QUEUE as i64],
            )?;
            Ok(None)
        });
        Ok(RecentPostsEnqueue::Queued)
    }

    fn record_recent_posts_fetched(
        &self,
        did: &str,
        fetched_at_ms: i64,
    ) -> Result<(), StorageError> {
        self.recent_posts_status
            .write()
            .map_err(|_| StorageError::Poisoned)?
            .insert(did.to_owned(), (fetched_at_ms, 0));
        self.recent_posts_counters
            .done
            .fetch_add(1, Ordering::Relaxed);
        let writer_did = did.to_owned();
        self.writer.fire(move |connection| {
            connection.execute(
                "INSERT INTO recent_posts_status(did,fetched_at_ms,next_retry_at_ms)
                 VALUES(?1,?2,0)
                 ON CONFLICT(did) DO UPDATE SET
                     fetched_at_ms=excluded.fetched_at_ms,next_retry_at_ms=0",
                params![writer_did, fetched_at_ms],
            )?;
            Ok(None)
        });
        Ok(())
    }

    fn record_recent_posts_failure(
        &self,
        did: &str,
        next_retry_at_ms: i64,
    ) -> Result<(), StorageError> {
        let fetched_at_ms = self
            .recent_posts_status
            .read()
            .map_err(|_| StorageError::Poisoned)?
            .get(did)
            .map(|entry| entry.0)
            .unwrap_or_default();
        self.recent_posts_status
            .write()
            .map_err(|_| StorageError::Poisoned)?
            .insert(did.to_owned(), (fetched_at_ms, next_retry_at_ms));
        self.recent_posts_counters
            .failed
            .fetch_add(1, Ordering::Relaxed);
        let writer_did = did.to_owned();
        self.writer.fire(move |connection| {
            connection.execute(
                "INSERT INTO recent_posts_status(did,fetched_at_ms,next_retry_at_ms)
                 VALUES(?1,?2,?3)
                 ON CONFLICT(did) DO UPDATE SET
                     fetched_at_ms=excluded.fetched_at_ms,next_retry_at_ms=excluded.next_retry_at_ms",
                params![writer_did, fetched_at_ms, next_retry_at_ms],
            )?;
            Ok(None)
        });
        Ok(())
    }

    fn pending_recent_posts_fetches(&self, limit: usize) -> Result<Vec<String>, StorageError> {
        Ok(self
            .recent_posts_queue
            .read()
            .map_err(|_| StorageError::Poisoned)?
            .0
            .iter()
            .take(limit.max(1))
            .cloned()
            .collect())
    }

    fn complete_recent_posts_fetch(&self, did: &str) -> Result<(), StorageError> {
        {
            let mut queue = self
                .recent_posts_queue
                .write()
                .map_err(|_| StorageError::Poisoned)?;
            queue.0.retain(|queued| queued != did);
            queue.1.remove(did);
        }
        let writer_did = did.to_owned();
        self.writer.fire(move |connection| {
            connection.execute("DELETE FROM recent_posts_queue WHERE did=?1", [writer_did])?;
            Ok(None)
        });
        Ok(())
    }

    fn recent_posts_counters(&self) -> (u64, u64, u64, u64) {
        self.recent_posts_counters.snapshot()
    }

    fn older_posts_state(&self, did: &str) -> Result<Option<OlderPostsState>, StorageError> {
        Ok(self
            .read_conn()?
            .query_row(
                "SELECT pds_cursor, exhausted FROM older_posts_state WHERE did=?1",
                [did],
                |row| {
                    Ok(OlderPostsState {
                        cursor: row.get::<_, Option<String>>(0)?,
                        exhausted: row.get::<_, i64>(1)? != 0,
                    })
                },
            )
            .optional()?)
    }

    fn record_older_posts_page(
        &self,
        did: &str,
        cursor: Option<&str>,
        exhausted: bool,
    ) -> Result<(), StorageError> {
        let writer_did = did.to_owned();
        let writer_cursor = cursor.map(str::to_owned);
        self.writer.call(move |connection| {
            connection.execute(
                "INSERT INTO older_posts_state(did,pds_cursor,exhausted,updated_at_ms)
                 VALUES(?1,?2,?3,?4)
                 ON CONFLICT(did) DO UPDATE SET
                     pds_cursor=excluded.pds_cursor,
                     exhausted=excluded.exhausted,
                     updated_at_ms=excluded.updated_at_ms",
                params![
                    writer_did,
                    writer_cursor,
                    i64::from(exhausted),
                    epoch_millis()
                ],
            )?;
            Ok(())
        })?;
        Ok(())
    }

    fn notifications_for(
        &self,
        did: &str,
        before: Option<&NotificationCursor>,
        limit: usize,
    ) -> Result<Vec<IndexedNotification>, StorageError> {
        let conn = self.read_conn()?;
        let mut statement = conn.prepare(
            "SELECT r.did, r.collection, r.rkey, r.uri, r.cid, r.time_us, r.record_json,
                    n.reason, n.reason_subject
             FROM notification_events n
             JOIN records r ON r.did=n.source_did AND r.collection=n.source_collection AND r.rkey=n.source_rkey
             WHERE n.subject_did=?1 AND (
                 ?2 IS NULL OR n.time_us < ?2 OR
                 (n.time_us=?2 AND (n.source_uri < ?3 OR (n.source_uri=?3 AND n.reason < ?4)))
             )
             ORDER BY n.time_us DESC, n.source_uri DESC, n.reason DESC
             LIMIT ?5",
        )?;
        statement
            .query_map(
                params![
                    did,
                    before.map(|cursor| cursor.time_us.min(i64::MAX as u64) as i64),
                    before.map(|cursor| cursor.uri.as_str()).unwrap_or(""),
                    before.map(|cursor| cursor.reason.as_str()).unwrap_or(""),
                    i64::try_from(limit.max(1)).unwrap_or(i64::MAX),
                ],
                |row| {
                    Ok(IndexedNotification {
                        record: row_to_record(row)?,
                        reason: row.get(7)?,
                        reason_subject: row.get(8)?,
                    })
                },
            )?
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    fn notification_events_window(
        &self,
        did: &str,
        before: Option<&NotificationCursor>,
        skip: &[NotificationGroupSkip],
        limit: usize,
    ) -> Result<Vec<NotificationEventRow>, StorageError> {
        let mut sql = String::from(
            "SELECT source_did, source_collection, source_rkey, source_uri, reason, reason_subject, time_us
             FROM notification_events
             WHERE subject_did=?1 AND (
                 ?2 IS NULL OR time_us < ?2 OR
                 (time_us=?2 AND (source_uri < ?3 OR (source_uri=?3 AND reason < ?4)))
             )",
        );
        let mut values = vec![
            rusqlite::types::Value::Text(did.to_owned()),
            before
                .map(|cursor| {
                    rusqlite::types::Value::Integer(cursor.time_us.min(i64::MAX as u64) as i64)
                })
                .unwrap_or(rusqlite::types::Value::Null),
            rusqlite::types::Value::Text(
                before
                    .map(|cursor| cursor.uri.as_str())
                    .unwrap_or("")
                    .to_owned(),
            ),
            rusqlite::types::Value::Text(
                before
                    .map(|cursor| cursor.reason.as_str())
                    .unwrap_or("")
                    .to_owned(),
            ),
        ];
        for (index, (reason, subject, floor)) in skip.iter().enumerate() {
            let reason_slot = 5 + index * 3;
            sql.push_str(&format!(
                " AND NOT (reason=?{reason_slot} AND reason_subject IS ?{} AND time_us >= ?{})",
                reason_slot + 1,
                reason_slot + 2
            ));
            values.push(rusqlite::types::Value::Text(reason.clone()));
            values.push(
                subject
                    .as_ref()
                    .map(|subject| rusqlite::types::Value::Text(subject.clone()))
                    .unwrap_or(rusqlite::types::Value::Null),
            );
            values.push(rusqlite::types::Value::Integer(
                (*floor).min(i64::MAX as u64) as i64,
            ));
        }
        let limit_slot = 5 + skip.len() * 3;
        sql.push_str(&format!(
            " ORDER BY time_us DESC, source_uri DESC, reason LIMIT ?{limit_slot}"
        ));
        values.push(rusqlite::types::Value::Integer(
            i64::try_from(limit).unwrap_or(i64::MAX),
        ));
        let connection = self.read_conn()?;
        let mut statement = connection.prepare(&sql)?;
        statement
            .query_map(rusqlite::params_from_iter(values.iter()), |row| {
                Ok(NotificationEventRow {
                    source_did: row.get(0)?,
                    source_collection: row.get(1)?,
                    source_rkey: row.get(2)?,
                    source_uri: row.get(3)?,
                    reason: row.get(4)?,
                    reason_subject: row.get(5)?,
                    time_us: row.get::<_, i64>(6)?.max(0) as u64,
                })
            })?
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    fn notification_key_event_count(
        &self,
        did: &str,
        reason: &str,
        reason_subject: Option<&str>,
        from_us: u64,
        to_us: u64,
    ) -> Result<usize, StorageError> {
        Ok(self.read_conn()?.query_row(
            "SELECT COUNT(*) FROM notification_events
             WHERE subject_did=?1 AND reason=?2 AND reason_subject IS ?3
               AND time_us BETWEEN ?4 AND ?5",
            params![
                did,
                reason,
                reason_subject,
                from_us.min(i64::MAX as u64) as i64,
                to_us.min(i64::MAX as u64) as i64,
            ],
            |row| row.get::<_, i64>(0),
        )? as usize)
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
        self.read_conn()?.prepare_cached(
            "SELECT source_did, source_collection, source_rkey, source_uri, reason, reason_subject, time_us
             FROM notification_events
             WHERE subject_did=?1 AND reason=?2 AND reason_subject IS ?3 AND (
                 ?4 IS NULL OR time_us < ?5 OR
                 (time_us=?5 AND (source_uri < ?6 OR (source_uri=?6 AND reason < ?7)))
             ) AND (?8 IS NULL OR time_us >= ?8) AND (?10 IS NULL OR time_us <= ?10)
             ORDER BY time_us DESC, source_uri DESC, reason LIMIT ?9",
        )?
        .query_map(
            params![
                did,
                reason,
                reason_subject,
                before.map(|cursor| cursor.time_us.min(i64::MAX as u64) as i64),
                before
                    .map(|cursor| cursor.time_us.min(i64::MAX as u64) as i64)
                    .unwrap_or(0),
                before.map(|cursor| cursor.uri.as_str()).unwrap_or(""),
                before.map(|cursor| cursor.reason.as_str()).unwrap_or(""),
                window
                    .from_us
                    .map(|floor| floor.min(i64::MAX as u64) as i64),
                i64::try_from(limit).unwrap_or(i64::MAX),
                window
                    .to_us
                    .map(|ceiling| ceiling.min(i64::MAX as u64) as i64),
            ],
            |row| {
                Ok(NotificationEventRow {
                    source_did: row.get(0)?,
                    source_collection: row.get(1)?,
                    source_rkey: row.get(2)?,
                    source_uri: row.get(3)?,
                    reason: row.get(4)?,
                    reason_subject: row.get(5)?,
                    time_us: row.get::<_, i64>(6)?.max(0) as u64,
                })
            },
        )?
        .collect::<Result<Vec<_>, _>>()
        .map_err(Into::into)
    }

    fn notification_count(&self, did: &str) -> Result<usize, StorageError> {
        Ok(self.read_conn()?.query_row(
            "SELECT COUNT(*) FROM notification_events WHERE subject_did=?1",
            [did],
            |row| row.get(0),
        )?)
    }

    fn notification_last_seen(&self, did: &str) -> Result<u64, StorageError> {
        let stored: Option<i64> = self
            .read_conn()?
            .query_row(
                "SELECT time_us FROM notification_last_seen WHERE viewer_did=?1",
                [did],
                |row| row.get(0),
            )
            .optional()?;
        Ok(stored.unwrap_or_default().max(0) as u64)
    }

    fn mark_notifications_seen(&self, did: &str, time_us: u64) -> Result<(), StorageError> {
        let writer_did = did.to_owned();
        let hub = self.notification_hub.clone();
        self.writer.fire(move |connection| {
            connection.execute(
                "INSERT INTO notification_last_seen(viewer_did,time_us) VALUES(?1,?2)
                 ON CONFLICT(viewer_did) DO UPDATE SET time_us=MAX(time_us,excluded.time_us)",
                params![writer_did, time_us.min(i64::MAX as u64) as i64],
            )?;
            let unread: i64 = connection.query_row(
                "SELECT COUNT(*) FROM notification_events
                 WHERE subject_did=?1 AND time_us > COALESCE(
                     (SELECT time_us FROM notification_last_seen WHERE viewer_did=?1), 0
                 )",
                [&writer_did],
                |row| row.get(0),
            )?;
            Ok(Some(Box::new(move || {
                hub.publish(&writer_did, unread as usize)
            })))
        });
        Ok(())
    }

    fn unread_notification_count(&self, did: &str) -> Result<usize, StorageError> {
        Ok(self.read_conn()?.query_row(
            "SELECT COUNT(*) FROM notification_events
             WHERE subject_did=?1 AND time_us > COALESCE(
                 (SELECT time_us FROM notification_last_seen WHERE viewer_did=?1), 0
             )",
            [did],
            |row| row.get(0),
        )?)
    }

    fn subscribe_notification_counts(&self, did: &str) -> NotificationSubscription {
        self.notification_hub.subscribe(did)
    }

    fn author_posts(
        &self,
        did: &str,
        cursor: Option<&RecordCursor>,
        limit: usize,
        include_replies: bool,
    ) -> Result<Vec<IndexedRecord>, StorageError> {
        let conn = self.read_conn()?;
        let reply_filter = if include_replies {
            ""
        } else {
            " AND json_type(record_json, '$.reply') IS NULL"
        };
        let sql = format!(
            "SELECT did, collection, rkey, uri, cid, time_us, record_json FROM records
             WHERE collection='app.bsky.feed.post' AND did=?1{reply_filter}
             AND (?2 IS NULL OR time_us < ?2 OR (time_us=?2 AND uri < ?3))
             ORDER BY time_us DESC, uri DESC LIMIT ?4"
        );
        let mut statement = conn.prepare(&sql)?;
        statement
            .query_map(
                params![
                    did,
                    cursor.map(|cursor| cursor.time_us.min(i64::MAX as u64) as i64),
                    cursor.map(|cursor| cursor.uri.as_str()).unwrap_or(""),
                    limit.clamp(1, 101) as i64,
                ],
                row_to_record,
            )?
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    fn timeline_posts(
        &self,
        actor: &str,
        cursor: Option<&RecordCursor>,
        limit: usize,
    ) -> Result<Vec<IndexedRecord>, StorageError> {
        self.timeline_posts_page(actor, cursor, limit, None)
    }

    fn timeline_posts_by_post_time(
        &self,
        actor: &str,
        cursor: Option<&SortTimeCursor>,
        limit: usize,
    ) -> Result<Vec<IndexedRecord>, StorageError> {
        self.timeline_posts_by_post_time_page(actor, cursor, limit, None)
    }

    fn sort_us_for_record(&self, record: &IndexedRecord) -> Result<u64, StorageError> {
        let conn = self.read_conn()?;
        let stored: Option<i64> = conn
            .query_row(
                "SELECT sort_us FROM post_sort WHERE uri=?1",
                [&record.uri],
                |row| row.get(0),
            )
            .optional()?;
        Ok(stored
            .map(|value| value.max(0) as u64)
            .unwrap_or(record.time_us))
    }

    fn post_sort_backfill_remaining(&self) -> Result<u64, StorageError> {
        let remaining = self.post_sort_remaining.load(Ordering::Relaxed);
        if remaining != POST_SORT_REMAINING_UNKNOWN {
            return Ok(remaining);
        }
        let conn = self.read_conn()?;
        let posts: i64 = conn.query_row(
            "SELECT COUNT(*) FROM records INDEXED BY records_collection_time
             WHERE collection='app.bsky.feed.post'",
            [],
            |row| row.get(0),
        )?;
        let filled: i64 = conn.query_row("SELECT COUNT(*) FROM post_sort", [], |row| row.get(0))?;
        let remaining = (posts - filled).max(0) as u64;
        self.post_sort_remaining.store(remaining, Ordering::Relaxed);
        Ok(remaining)
    }

    fn post_sort_backfill_failures(&self) -> u64 {
        self.post_sort_backfill_failures.load(Ordering::Relaxed)
    }

    fn record_post_sort_backfill_failure(&self) {
        self.post_sort_backfill_failures
            .fetch_add(1, Ordering::Relaxed);
    }

    /// Returns the last `records.rowid` examined (0 when the walk is done), not a count.
    fn backfill_post_sort_batch(&self, limit: usize) -> Result<u64, StorageError> {
        let _operation = SqliteOperationKindGuard::enter("post_sort_backfill");
        let limit = limit.clamp(1, 10_000);
        let remaining = self.post_sort_backfill_remaining()?;
        if remaining == 0 {
            return Ok(0);
        }
        let last_rowid = self.post_sort_backfill_rowid.load(Ordering::Relaxed);
        let conn = self.read_conn()?;
        let mut rows = conn.prepare(
            "SELECT r.rowid, r.did, r.uri, r.time_us, r.record_json FROM records AS r NOT INDEXED
             WHERE r.rowid > ?1
               AND r.collection='app.bsky.feed.post'
               AND NOT EXISTS (SELECT 1 FROM post_sort AS p WHERE p.uri = r.uri)
             ORDER BY r.rowid
             LIMIT ?2",
        )?;
        let batch = rows
            .query_map(params![last_rowid as i64, limit as i64], |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, String>(1)?,
                    row.get::<_, String>(2)?,
                    row.get::<_, i64>(3)?,
                    row.get::<_, String>(4)?,
                ))
            })?
            .collect::<Result<Vec<_>, _>>()?;
        drop(rows);
        drop(conn);
        if batch.is_empty() {
            self.post_sort_remaining.store(0, Ordering::Relaxed);
            return Ok(0);
        }
        let last_seen = batch.last().unwrap().0.max(0) as u64;
        let prepared = batch
            .iter()
            .map(|(_, did, uri, time_us, json)| {
                let time_us = *time_us.max(&0) as u64;
                let sort_us = post_sort_us_from_json(json, time_us);
                (
                    did.clone(),
                    uri.clone(),
                    sort_us.min(i64::MAX as u64) as i64,
                    time_us.min(i64::MAX as u64) as i64,
                )
            })
            .collect::<Vec<_>>();
        let _filled = self.writer.call(move |connection| {
            let transaction = connection.transaction()?;
            let mut filled = 0_u64;
            {
                let mut insert = transaction.prepare(
                    "INSERT INTO post_sort(uri,did,sort_us,time_us)
                     VALUES(?1,?2,?3,?4)
                     ON CONFLICT(uri) DO NOTHING",
                )?;
                for (did, uri, sort_us, time_us) in prepared {
                    filled += insert.execute(params![uri, did, sort_us, time_us])? as u64;
                }
            }
            transaction.commit()?;
            Ok(filled)
        })?;
        let resolved = batch.len() as u64;
        let before = self.post_sort_remaining.load(Ordering::Relaxed);
        self.post_sort_remaining
            .store(before.saturating_sub(resolved), Ordering::Relaxed);
        self.post_sort_backfill_rowid
            .store(last_seen, Ordering::Relaxed);
        Ok(last_seen)
    }

    fn post_interaction_summary(
        &self,
        uri: &str,
        viewer_did: Option<&str>,
    ) -> Result<PostInteractionSummary, StorageError> {
        self.post_interaction_summary_with_work(uri, viewer_did, None)
    }

    fn page_post_interaction_summaries(
        &self,
        uris: &[String],
        viewer_did: Option<&str>,
    ) -> Result<HashMap<String, PostInteractionSummary>, StorageError> {
        let mut summaries = uris
            .iter()
            .map(|uri| (uri.clone(), PostInteractionSummary::default()))
            .collect::<HashMap<_, _>>();
        let mut cached_uris = HashSet::new();
        let missing: Vec<String>;
        {
            let conn = self.read_conn()?;
            let batch = serde_json::to_string(uris)?;
            let mut statement = conn.prepare(
                "SELECT uri,like_count,repost_count,reply_count,quote_count FROM post_counts
                 WHERE uri IN (SELECT value FROM json_each(?1))",
            )?;
            let rows = statement
                .query_map([batch], |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        [row.get(1)?, row.get(2)?, row.get(3)?, row.get(4)?],
                    ))
                })?
                .collect::<Result<Vec<_>, _>>()?;
            for (uri, counts) in rows {
                cached_uris.insert(uri.clone());
                if let Some(summary) = summaries.get_mut(&uri) {
                    summary.like_count = counts[0];
                    summary.repost_count = counts[1];
                    summary.reply_count = counts[2];
                    summary.quote_count = counts[3];
                }
            }
            missing = uris
                .iter()
                .filter(|uri| !cached_uris.contains(*uri))
                .cloned()
                .collect::<Vec<_>>();
            if !missing.is_empty() {
                let (computed, _work) = compute_post_counts(&conn, &missing)?;
                for uri in &missing {
                    if let Some(counts) = computed.get(uri)
                        && let Some(summary) = summaries.get_mut(uri)
                    {
                        summary.like_count = counts[0];
                        summary.repost_count = counts[1];
                        summary.reply_count = counts[2];
                        summary.quote_count = counts[3];
                    }
                }
            }
            if let Some(viewer_did) = viewer_did {
                let likes = viewer_interaction_uris(&conn, uris, viewer_did, "app.bsky.feed.like")?;
                let reposts =
                    viewer_interaction_uris(&conn, uris, viewer_did, "app.bsky.feed.repost")?;
                for uri in uris {
                    if let Some(summary) = summaries.get_mut(uri) {
                        summary.viewer_like = likes.get(uri).cloned();
                        summary.viewer_repost = reposts.get(uri).cloned();
                    }
                }
            }
        }
        for uri in &missing {
            self.queue_post_counts_fill(uri);
        }
        Ok(summaries)
    }

    fn records(&self) -> Result<Vec<IndexedRecord>, StorageError> {
        let conn = self.read_conn()?;
        let mut statement = conn.prepare(
            "SELECT did, collection, rkey, uri, cid, time_us, record_json FROM records ORDER BY time_us DESC",
        )?;
        let records = statement
            .query_map([], row_to_record)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(records)
    }

    fn cursor(&self) -> Result<Option<u64>, StorageError> {
        self.read_conn()?
            .query_row(
                "SELECT value FROM appview_state WHERE key='jetstream_cursor'",
                [],
                |row| row.get::<_, String>(0),
            )
            .optional()?
            .map(|value| value.parse().map_err(|_| rusqlite::Error::InvalidQuery))
            .transpose()
            .map_err(Into::into)
    }

    fn set_cursor(&self, cursor: u64) -> Result<(), StorageError> {
        self.writer.call(move |connection| {
            connection.execute(
                "INSERT INTO appview_state(key,value) VALUES('jetstream_cursor',?1)
                 ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                [cursor.to_string()],
            )?;
            Ok(())
        })
    }

    fn register_user(&self, did: &str) -> Result<(), StorageError> {
        let writer_did = did.to_owned();
        let (was_registered, active_subjects) = self.writer.call(move |connection| {
            let transaction = connection.transaction()?;
            let was_registered: bool = transaction.query_row(
                "SELECT EXISTS(SELECT 1 FROM registered_users WHERE did=?1)",
                [&writer_did],
                |row| row.get(0),
            )?;
            if !was_registered {
                // A re-login after the `follows` rows were emptied (logout, an
                // interrupted snapshot) must not wait for a re-backfill: rebuild
                // the rows from the follow records this database already holds,
                // with the same backfill-queue side effects the snapshot insert
                // path has.
                let mut statement = transaction.prepare(
                    "SELECT DISTINCT json_extract(record_json, '$.subject') FROM records
                     WHERE collection='app.bsky.graph.follow' AND did=?1
                       AND json_extract(record_json, '$.subject') IS NOT NULL",
                )?;
                let subjects = statement
                    .query_map([&writer_did], |row| row.get::<_, String>(0))?
                    .collect::<Result<Vec<_>, _>>()?;
                drop(statement);
                for subject in &subjects {
                    let inserted = transaction.execute(
                        "INSERT OR IGNORE INTO follows(actor,subject,following) VALUES(?1,?2,1)",
                        params![writer_did, subject],
                    )?;
                    if inserted > 0 {
                        tracing::debug!(
                            actor = %writer_did,
                            %subject,
                            "Rebuilt a follows row from an existing follow record at registration",
                        );
                    }
                    transaction.execute(
                        "INSERT OR IGNORE INTO backfill_queue(did)
                         SELECT ?1 WHERE (SELECT COUNT(*) FROM backfill_queue) < ?2
                            OR EXISTS(SELECT 1 FROM backfill_queue WHERE did=?1)",
                        params![subject, MAX_BACKFILL_QUEUE],
                    )?;
                    transaction.execute(
                        "INSERT OR IGNORE INTO backfill_scope(owner_did,target_did)
                         SELECT ?1,?2 WHERE EXISTS(SELECT 1 FROM backfill_queue WHERE did=?2)",
                        params![writer_did, subject],
                    )?;
                    transaction.execute(
                        "INSERT INTO backfill_status(did,done) VALUES(?1,0)
                         ON CONFLICT(did) DO UPDATE SET done=0
                         WHERE EXISTS(SELECT 1 FROM backfill_queue WHERE did=?1)",
                        [subject],
                    )?;
                }
            }
            let active_subjects = if was_registered {
                Vec::new()
            } else {
                let mut statement = transaction
                    .prepare("SELECT subject FROM follows WHERE actor=?1 AND following=1")?;
                statement
                    .query_map([&writer_did], |row| row.get::<_, String>(0))?
                    .collect::<Result<Vec<_>, _>>()?
            };
            transaction.execute(
                "INSERT OR IGNORE INTO registered_users(did) VALUES(?1)",
                [&writer_did],
            )?;
            transaction.execute(
                "INSERT OR IGNORE INTO backfill_queue(did)
                 SELECT ?1 WHERE (SELECT COUNT(*) FROM backfill_queue) < ?2
                    OR EXISTS(SELECT 1 FROM backfill_queue WHERE did=?1)",
                params![writer_did, MAX_BACKFILL_QUEUE],
            )?;
            transaction.execute(
                "INSERT INTO backfill_status(did,done) VALUES(?1,0)
                 ON CONFLICT(did) DO UPDATE SET done=0
                 WHERE EXISTS(SELECT 1 FROM backfill_queue WHERE did=?1)",
                [&writer_did],
            )?;
            transaction.execute(
                "INSERT OR IGNORE INTO backfill_scope(owner_did,target_did)
                 SELECT ?1,?1 WHERE EXISTS(SELECT 1 FROM backfill_queue WHERE did=?1)",
                [&writer_did],
            )?;
            transaction.commit()?;
            Ok((was_registered, active_subjects))
        })?;
        if !was_registered {
            let mut tracked = self.tracked.write().map_err(|_| StorageError::Poisoned)?;
            tracked.registered.insert(did.to_owned());
            for subject in active_subjects {
                tracked.apply_follow_delta(&subject, 1);
            }
        }
        Ok(())
    }

    fn unregister_user(&self, did: &str) -> Result<(), StorageError> {
        let writer_did = did.to_owned();
        let (was_registered, active_subjects) = self.writer.call(move |connection| {
            let transaction = connection.transaction()?;
            let was_registered: bool = transaction.query_row(
                "SELECT EXISTS(SELECT 1 FROM registered_users WHERE did=?1)",
                [&writer_did],
                |row| row.get(0),
            )?;
            let active_subjects = if was_registered {
                let mut statement = transaction
                    .prepare("SELECT subject FROM follows WHERE actor=?1 AND following=1")?;
                statement
                    .query_map([&writer_did], |row| row.get::<_, String>(0))?
                    .collect::<Result<Vec<_>, _>>()?
            } else {
                Vec::new()
            };
            transaction.execute("DELETE FROM registered_users WHERE did=?1", [&writer_did])?;
            transaction.execute(
                "DELETE FROM backfill_scope WHERE owner_did=?1",
                [&writer_did],
            )?;
            transaction.execute(
                "DELETE FROM backfill_queue WHERE did=?1 AND NOT EXISTS (
                     SELECT 1 FROM follows f JOIN registered_users r ON f.actor=r.did
                     WHERE f.subject=?1 AND f.following=1
                 )",
                [&writer_did],
            )?;
            transaction.commit()?;
            Ok((was_registered, active_subjects))
        })?;
        if was_registered {
            let mut tracked = self.tracked.write().map_err(|_| StorageError::Poisoned)?;
            tracked.registered.remove(did);
            for subject in active_subjects {
                tracked.apply_follow_delta(&subject, -1);
            }
        }
        Ok(())
    }

    fn is_registered(&self, did: &str) -> Result<bool, StorageError> {
        Ok(self
            .tracked
            .read()
            .map_err(|_| StorageError::Poisoned)?
            .registered
            .contains(did))
    }

    fn set_follow(&self, actor: &str, subject: &str, following: bool) -> Result<(), StorageError> {
        let writer_actor = actor.to_owned();
        let writer_subject = subject.to_owned();
        let (actor_is_registered, was_following) = self.writer.call(move |connection| {
            let transaction = connection.transaction()?;
            let actor_is_registered: bool = transaction.query_row(
                "SELECT EXISTS(SELECT 1 FROM registered_users WHERE did=?1)",
                [&writer_actor],
                |row| row.get(0),
            )?;
            let was_following: Option<bool> = transaction
                .query_row(
                    "SELECT following FROM follows WHERE actor=?1 AND subject=?2",
                    params![writer_actor, writer_subject],
                    |row| row.get(0),
                )
                .optional()?;
            let was_following = was_following.unwrap_or(false);
            transaction.execute(
                "INSERT OR REPLACE INTO follows(actor,subject,following) VALUES(?1,?2,?3)",
                params![writer_actor, writer_subject, following],
            )?;
            if !following {
                transaction.execute(
                    "DELETE FROM backfill_scope WHERE owner_did=?1 AND target_did=?2",
                    params![writer_actor, writer_subject],
                )?;
            }
            transaction.commit()?;
            Ok((actor_is_registered, was_following))
        })?;
        if actor_is_registered && was_following != following {
            self.apply_tracked_follow_deltas(&HashMap::from([(
                subject.to_owned(),
                if following { 1 } else { -1 },
            )]))?;
        }
        Ok(())
    }

    fn is_following(&self, actor: &str, subject: &str) -> Result<bool, StorageError> {
        Ok(self.read_conn()?.query_row(
            "SELECT EXISTS(SELECT 1 FROM follows WHERE actor=?1 AND subject=?2 AND following=1)",
            params![actor, subject],
            |row| row.get(0),
        )?)
    }

    fn has_follow_record(&self, actor: &str, subject: &str) -> Result<bool, StorageError> {
        Ok(self.read_conn()?.query_row(
            "SELECT EXISTS(SELECT 1 FROM records
             WHERE collection='app.bsky.graph.follow' AND did=?1
               AND json_extract(record_json, '$.subject')=?2)",
            params![actor, subject],
            |row| row.get(0),
        )?)
    }

    fn tracked_follow_count(&self, did: &str) -> Result<u32, StorageError> {
        Ok(self
            .tracked
            .read()
            .map_err(|_| StorageError::Poisoned)?
            .follow_count(did))
    }

    fn is_followed_by_registered(&self, did: &str) -> Result<bool, StorageError> {
        Ok(self.tracked_follow_count(did)? > 0)
    }

    fn enqueue_backfill(&self, did: &str) -> Result<(), StorageError> {
        let writer_did = did.to_owned();
        self.writer.call(move |connection| {
            let transaction = connection.transaction()?;
            transaction.execute(
                "INSERT OR IGNORE INTO backfill_queue(did)
                 SELECT ?1
                 WHERE ((SELECT COUNT(*) FROM backfill_queue) < ?2
                        OR EXISTS(SELECT 1 FROM backfill_queue WHERE did=?1))
                   AND (EXISTS(SELECT 1 FROM registered_users WHERE did=?1)
                        OR EXISTS(SELECT 1 FROM follows f JOIN registered_users r ON r.did=f.actor
                                  WHERE f.subject=?1 AND f.following=1))",
                params![writer_did, MAX_BACKFILL_QUEUE],
            )?;
            transaction.execute(
                "INSERT INTO backfill_status(did,done) VALUES(?1,0)
                 ON CONFLICT(did) DO UPDATE SET done=0
                 WHERE EXISTS(SELECT 1 FROM backfill_queue WHERE did=?1)",
                [&writer_did],
            )?;
            transaction.commit()?;
            Ok(())
        })
    }

    fn enqueue_backfill_for(&self, owner_did: &str, did: &str) -> Result<(), StorageError> {
        let writer_owner = owner_did.to_owned();
        let writer_did = did.to_owned();
        self.writer.call(move |connection| {
            let transaction = connection.transaction()?;
            transaction.execute(
                "INSERT OR IGNORE INTO backfill_queue(did)
                 SELECT ?2
                 WHERE ((SELECT COUNT(*) FROM backfill_queue) < ?3
                        OR EXISTS(SELECT 1 FROM backfill_queue WHERE did=?2))
                   AND EXISTS(SELECT 1 FROM registered_users WHERE did=?1)
                   AND (?1=?2 OR EXISTS(SELECT 1 FROM follows WHERE actor=?1 AND subject=?2 AND following=1))",
                params![writer_owner, writer_did, MAX_BACKFILL_QUEUE],
            )?;
            transaction.execute(
                "INSERT OR REPLACE INTO backfill_scope(owner_did,target_did)
                 SELECT ?1,?2 WHERE EXISTS(SELECT 1 FROM backfill_queue WHERE did=?2)
                   AND EXISTS(SELECT 1 FROM registered_users WHERE did=?1)
                   AND (?1=?2 OR EXISTS(SELECT 1 FROM follows WHERE actor=?1 AND subject=?2 AND following=1))",
                params![writer_owner, writer_did],
            )?;
            transaction.execute(
                "INSERT INTO backfill_status(did,done) VALUES(?1,0)
                 ON CONFLICT(did) DO UPDATE SET done=0
                 WHERE EXISTS(SELECT 1 FROM backfill_queue WHERE did=?1)",
                [&writer_did],
            )?;
            transaction.commit()?;
            Ok(())
        })
    }

    fn pending_backfills(&self) -> Result<Vec<String>, StorageError> {
        let conn = self.read_conn()?;
        let mut statement = conn.prepare(
            "SELECT q.did FROM backfill_queue q
             LEFT JOIN backfill_retry r ON r.did=q.did
             WHERE r.did IS NULL OR r.retry_at_ms<=?1
             ORDER BY q.enqueued_at,q.did LIMIT ?2",
        )?;
        Ok(statement
            .query_map(params![epoch_millis(), BACKFILL_BATCH_SIZE], |row| {
                row.get(0)
            })?
            .collect::<Result<Vec<_>, _>>()?)
    }

    fn complete_backfill(&self, did: &str) -> Result<(), StorageError> {
        let writer_did = did.to_owned();
        self.writer.call(move |connection| {
            let transaction = connection.transaction()?;
            transaction.execute("DELETE FROM backfill_queue WHERE did=?1", [&writer_did])?;
            transaction.execute("DELETE FROM backfill_retry WHERE did=?1", [&writer_did])?;
            transaction.execute(
                "INSERT INTO backfill_status(did,done) VALUES(?1,1)
                 ON CONFLICT(did) DO UPDATE SET done=1",
                [&writer_did],
            )?;
            transaction.commit()?;
            Ok(())
        })
    }

    fn record_backfill_failure(&self, did: &str) -> Result<Duration, StorageError> {
        let writer_did = did.to_owned();
        self.writer.call(move |connection| {
            let transaction = connection.transaction()?;
            let failures = transaction
                .query_row(
                    "SELECT failures FROM backfill_retry WHERE did=?1",
                    [&writer_did],
                    |row| row.get::<_, u32>(0),
                )
                .optional()?
                .unwrap_or(0_u32)
                .saturating_add(1);
            let delay = backfill_retry_delay(failures);
            let retry_at_ms = epoch_millis().saturating_add(delay.as_millis() as i64);
            transaction.execute(
                "INSERT INTO backfill_retry(did,failures,retry_at_ms) VALUES(?1,?2,?3)
                 ON CONFLICT(did) DO UPDATE SET failures=excluded.failures,retry_at_ms=excluded.retry_at_ms",
                params![writer_did, failures, retry_at_ms],
            )?;
            transaction.commit()?;
            Ok(delay)
        })
    }

    fn backfill_progress(&self, owner_did: &str) -> Result<(usize, usize), StorageError> {
        let (total, done): (i64, i64) = self.read_conn()?.query_row(
            "SELECT COUNT(*), COALESCE(SUM(CASE WHEN s.done=1 THEN 1 ELSE 0 END),0)
             FROM backfill_scope b LEFT JOIN backfill_status s ON s.did=b.target_did
             WHERE b.owner_did=?1",
            [owner_did],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        Ok((total as usize, done as usize))
    }

    fn backfill_failure_count(&self, owner_did: &str) -> Result<usize, StorageError> {
        let count: i64 = self.read_conn()?.query_row(
            "SELECT COUNT(*) FROM backfill_scope b
             JOIN backfill_retry r ON r.did=b.target_did
             LEFT JOIN backfill_status s ON s.did=b.target_did
             WHERE b.owner_did=?1 AND COALESCE(s.done,0)=0",
            [owner_did],
            |row| row.get(0),
        )?;
        Ok(count as usize)
    }

    fn set_verified_handle(&self, did: &str, handle: &str) -> Result<(), StorageError> {
        let writer_did = did.to_owned();
        let writer_handle = handle.to_owned();
        self.writer.call(move |connection| {
            let transaction = connection.transaction()?;
            transaction.execute(
                "INSERT INTO verified_handles(did,handle,updated_at) VALUES(?1,?2,unixepoch())
                 ON CONFLICT(did) DO UPDATE SET handle=excluded.handle,updated_at=unixepoch()",
                params![writer_did, writer_handle],
            )?;
            transaction.execute("DELETE FROM invalid_handles WHERE did=?1", [&writer_did])?;
            transaction.commit()?;
            Ok(())
        })
    }

    fn clear_verified_handle(&self, did: &str) -> Result<(), StorageError> {
        let writer_did = did.to_owned();
        self.writer.call(move |connection| {
            let transaction = connection.transaction()?;
            transaction.execute("DELETE FROM verified_handles WHERE did=?1", [&writer_did])?;
            transaction.execute(
                "INSERT INTO invalid_handles(did,checked_at) VALUES(?1,unixepoch())
                 ON CONFLICT(did) DO UPDATE SET checked_at=unixepoch()",
                [&writer_did],
            )?;
            transaction.commit()?;
            Ok(())
        })
    }

    fn verified_handle(&self, did: &str) -> Result<Option<String>, StorageError> {
        self.read_conn()?
            .query_row(
                "SELECT handle FROM verified_handles WHERE did=?1",
                [did],
                |row| row.get(0),
            )
            .optional()
            .map_err(Into::into)
    }

    fn handle_is_invalid(&self, did: &str) -> Result<bool, StorageError> {
        Ok(self.read_conn()?.query_row(
            "SELECT EXISTS(SELECT 1 FROM invalid_handles WHERE did=?1)",
            [did],
            |row| row.get(0),
        )?)
    }

    fn rotate_web_session(&self, did: &str, session_id: &str) -> Result<u64, StorageError> {
        let writer_did = did.to_owned();
        let writer_session_id = session_id.to_owned();
        self.writer.call(move |connection| {
            let transaction = connection.transaction()?;
            let current = transaction
                .query_row(
                    "SELECT generation FROM web_sessions WHERE did=?1 AND session_id=?2",
                    params![writer_did, writer_session_id],
                    |row| row.get::<_, i64>(0),
                )
                .optional()?
                .unwrap_or(0);
            let next = current
                .checked_add(1)
                .ok_or(rusqlite::Error::InvalidQuery)?;
            transaction.execute(
                "INSERT INTO web_sessions(did,session_id,generation) VALUES(?1,?2,?3)
                 ON CONFLICT(did,session_id) DO UPDATE SET generation=excluded.generation",
                params![writer_did, writer_session_id, next],
            )?;
            transaction.commit()?;
            u64::try_from(next).map_err(|_| rusqlite::Error::InvalidQuery.into())
        })
    }

    fn web_session_generation(
        &self,
        did: &str,
        session_id: &str,
    ) -> Result<Option<u64>, StorageError> {
        self.read_conn()?
            .query_row(
                "SELECT generation FROM web_sessions WHERE did=?1 AND session_id=?2",
                params![did, session_id],
                |row| row.get::<_, i64>(0),
            )
            .optional()?
            .map(|generation| u64::try_from(generation).map_err(|_| rusqlite::Error::InvalidQuery))
            .transpose()
            .map_err(Into::into)
    }

    fn delete_web_session(&self, did: &str, session_id: &str) -> Result<(), StorageError> {
        let did = did.to_owned();
        let session_id = session_id.to_owned();
        // The caller has already cleared the session cookie, so the page must
        // not wait on the writer for the row to disappear; the delete lands as
        // soon as the writer drains.
        self.writer.fire(move |connection| {
            connection.execute(
                "DELETE FROM web_sessions WHERE did=?1 AND session_id=?2",
                params![did, session_id],
            )?;
            Ok(None)
        });
        Ok(())
    }

    fn did_by_verified_handle(&self, handle: &str) -> Result<Option<String>, StorageError> {
        self.read_conn()?
            .query_row(
                "SELECT did FROM verified_handles WHERE lower(handle)=lower(?1)",
                [handle],
                |row| row.get(0),
            )
            .optional()
            .map_err(Into::into)
    }

    fn is_tracked_did(&self, did: &str) -> Result<bool, StorageError> {
        Ok(self
            .tracked
            .read()
            .map_err(|_| StorageError::Poisoned)?
            .is_tracked(did))
    }

    fn registered_followers(&self, did: &str) -> Result<Vec<String>, StorageError> {
        let connection = self.read_conn()?;
        let mut statement = connection.prepare(
            "SELECT f.actor FROM follows f JOIN registered_users r ON r.did=f.actor
             WHERE f.subject=?1 AND f.following=1",
        )?;
        statement
            .query_map([did], |row| row.get(0))?
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    fn is_displayed_did(&self, did: &str) -> Result<bool, StorageError> {
        Ok(self.read_conn()?.query_row(
            "SELECT EXISTS(SELECT 1 FROM records WHERE did=?1)",
            [did],
            |row| row.get(0),
        )?)
    }

    fn stale_handle_dids(&self, ttl: Duration, limit: usize) -> Result<Vec<String>, StorageError> {
        let ttl_seconds = i64::try_from(ttl.as_secs()).unwrap_or(i64::MAX);
        let connection = self.read_conn()?;
        let mut statement = connection.prepare(
            "SELECT did FROM (
                 SELECT h.did AS did,h.updated_at AS checked_at FROM verified_handles h
                 UNION ALL
                 SELECT i.did AS did,i.checked_at AS checked_at FROM invalid_handles i
             ) h
             WHERE h.checked_at<=unixepoch()-?1
               AND (EXISTS(SELECT 1 FROM registered_users r WHERE r.did=h.did)
                    OR EXISTS(SELECT 1 FROM records x WHERE x.did=h.did)
                    OR EXISTS(SELECT 1 FROM follows f JOIN registered_users r ON r.did=f.actor
                              WHERE f.subject=h.did AND f.following=1))
             GROUP BY h.did ORDER BY MIN(h.checked_at),h.did LIMIT ?2",
        )?;
        let dids = statement
            .query_map(
                params![ttl_seconds, i64::try_from(limit.max(1)).unwrap_or(i64::MAX)],
                |row| row.get(0),
            )?
            .collect::<Result<Vec<String>, _>>()?;
        Ok(dids)
    }

    fn queue_handle_refresh(&self, did: &str) -> Result<(), StorageError> {
        let writer_did = did.to_owned();
        self.writer.fire(move |connection| {
            connection.execute(
                "INSERT OR IGNORE INTO handle_refresh_queue(did,priority)
                 VALUES(?1, COALESCE((SELECT MAX(priority) FROM handle_refresh_queue),0)+1)",
                [&writer_did],
            )?;
            Ok(None)
        });
        Ok(())
    }

    fn queue_handle_refresh_ordered(&self, dids: &[String]) -> Result<(), StorageError> {
        if dids.is_empty() {
            return Ok(());
        }
        let mut seen = HashSet::new();
        let ordered = dids
            .iter()
            .filter(|did| seen.insert((*did).clone()))
            .cloned()
            .collect::<Vec<_>>();
        let did_count = ordered.len() as i64;
        self.writer.fire(move |connection| {
            let max_priority: i64 = connection.query_row(
                "SELECT COALESCE(MAX(priority),0) FROM handle_refresh_queue",
                [],
                |row| row.get(0),
            )?;
            let first_priority = max_priority.saturating_add(did_count);
            let mut statement = connection.prepare(
                "INSERT INTO handle_refresh_queue(did,priority) VALUES(?1,?2)
                 ON CONFLICT(did) DO UPDATE SET priority=MAX(priority,excluded.priority)",
            )?;
            for (index, did) in ordered.into_iter().enumerate() {
                let priority = first_priority.saturating_sub(index as i64);
                statement.execute(params![did, priority])?;
            }
            Ok(None)
        });
        Ok(())
    }

    fn pending_handle_refreshes(&self, limit: usize) -> Result<Vec<String>, StorageError> {
        let limit = limit.max(1);
        self.writer.flush()?;
        let connection = self.read_conn()?;
        let prioritized = {
            let mut statement = connection.prepare(
                "SELECT did FROM handle_refresh_queue ORDER BY priority DESC,rowid LIMIT ?1",
            )?;
            statement
                .query_map([i64::try_from(limit).unwrap_or(i64::MAX)], |row| row.get(0))?
                .collect::<Result<Vec<String>, _>>()?
        };
        Ok(prioritized)
    }

    fn complete_handle_refresh(&self, did: &str) -> Result<(), StorageError> {
        let writer_did = did.to_owned();
        self.writer.call(move |connection| {
            connection.execute(
                "DELETE FROM handle_refresh_queue WHERE did=?1",
                [writer_did],
            )?;
            Ok(())
        })
    }

    fn remember_recent_profile(
        &self,
        viewer_did: &str,
        profile_did: &str,
    ) -> Result<(), StorageError> {
        let viewer_did = viewer_did.to_owned();
        let profile_did = profile_did.to_owned();
        // A profile page GET arrived-from-search remembers the click; the row
        // only feeds the NEXT search page's Recent list, so this render must
        // not queue behind the writer. The queued batch transaction applies
        // the three statements atomically, and a repeated click only reorders
        // the row to the top (bounded by the writer queue + drop counter).
        self.writer.fire(move |connection| {
            connection.execute(
                "DELETE FROM recent_profile_clicks WHERE viewer_did=?1 AND profile_did=?2",
                params![viewer_did, profile_did],
            )?;
            connection.execute(
                "INSERT INTO recent_profile_clicks(viewer_did,profile_did) VALUES(?1,?2)",
                params![viewer_did, profile_did],
            )?;
            connection.execute(
                "DELETE FROM recent_profile_clicks WHERE viewer_did=?1 AND seq NOT IN (
                     SELECT seq FROM recent_profile_clicks WHERE viewer_did=?1
                     ORDER BY seq DESC LIMIT 10
                 )",
                [&viewer_did],
            )?;
            Ok(None)
        });
        Ok(())
    }

    fn recent_profiles_for_viewer(&self, viewer_did: &str) -> Result<Vec<String>, StorageError> {
        let connection = self.read_conn()?;
        let mut statement = connection.prepare(
            "SELECT profile_did FROM recent_profile_clicks
             WHERE viewer_did=?1 ORDER BY seq DESC LIMIT 10",
        )?;
        Ok(statement
            .query_map([viewer_did], |row| row.get(0))?
            .collect::<Result<Vec<_>, _>>()?)
    }

    fn clear_recent_profiles_for_viewer(&self, viewer_did: &str) -> Result<(), StorageError> {
        let viewer_did = viewer_did.to_owned();
        self.writer.call(move |connection| {
            connection.execute(
                "DELETE FROM recent_profile_clicks WHERE viewer_did=?1",
                [viewer_did],
            )?;
            Ok(())
        })
    }

    fn set_last_active_feed_tab(&self, viewer_did: &str, tab: &str) -> Result<(), StorageError> {
        let key = format!("last-active-feed-tab:{viewer_did}");
        let tab = tab.to_owned();
        // A navigation click must not wait on the writer to remember the tab;
        // the state read falls back to the last tab that landed.
        self.writer.fire(move |connection| {
            connection.execute(
                "INSERT INTO appview_state(key,value) VALUES(?1,?2)
                 ON CONFLICT(key) DO UPDATE SET value=excluded.value",
                params![key, tab],
            )?;
            Ok(None)
        });
        Ok(())
    }

    fn last_active_feed_tab(&self, viewer_did: &str) -> Result<Option<String>, StorageError> {
        let key = format!("last-active-feed-tab:{viewer_did}");
        Ok(self
            .read_conn()?
            .query_row(
                "SELECT value FROM appview_state WHERE key=?1",
                [key],
                |row| row.get(0),
            )
            .optional()?)
    }

    fn bytes_on_disk(&self) -> u64 {
        let bytes = fs::metadata(&self.path).map(|meta| meta.len()).unwrap_or(0);
        let wal_path = PathBuf::from(format!("{}-wal", self.path.display()));
        bytes + fs::metadata(wal_path).map(|meta| meta.len()).unwrap_or(0)
    }
}

fn row_to_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<IndexedRecord> {
    let record_json: String = row.get(6)?;
    let record = serde_json::from_str(&record_json).map_err(|error| {
        rusqlite::Error::FromSqlConversionFailure(6, rusqlite::types::Type::Text, Box::new(error))
    })?;
    Ok(IndexedRecord {
        did: row.get(0)?,
        collection: row.get(1)?,
        rkey: row.get(2)?,
        uri: row.get(3)?,
        cid: row.get(4)?,
        time_us: row.get(5)?,
        record,
    })
}

fn write_notification_events(
    connection: &Connection,
    record: &IndexedRecord,
) -> Result<(), StorageError> {
    connection.execute(
        "DELETE FROM notification_events WHERE source_did=?1 AND source_collection=?2 AND source_rkey=?3",
        params![record.did, record.collection, record.rkey],
    )?;
    for (record, subject_did, reason, reason_subject) in notification_edges(record) {
        connection.execute(
            "INSERT OR IGNORE INTO notification_events
             (source_did, source_collection, source_rkey, source_uri, reason, subject_did, reason_subject, time_us)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            params![
                record.did,
                record.collection,
                record.rkey,
                record.uri,
                reason,
                subject_did,
                reason_subject,
                record.time_us.min(i64::MAX as u64) as i64,
            ],
        )?;
    }
    Ok(())
}

fn migrate_notification_events(connection: &Connection) -> Result<(), StorageError> {
    let current: Option<String> = connection
        .query_row(
            "SELECT value FROM appview_state WHERE key='notification_events_v1'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    if current.is_some() {
        return Ok(());
    }
    let records = {
        let mut statement = connection
            .prepare("SELECT did, collection, rkey, uri, cid, time_us, record_json FROM records")?;
        statement
            .query_map([], row_to_record)?
            .collect::<Result<Vec<_>, _>>()?
    };
    for record in &records {
        write_notification_events(connection, record)?;
    }
    connection.execute(
        "INSERT INTO appview_state(key,value) VALUES('notification_events_v1','1')",
        [],
    )?;
    Ok(())
}

fn migrate_follow_state(connection: &Connection) -> Result<(), StorageError> {
    let columns = connection
        .prepare("PRAGMA table_info(follows)")?
        .query_map([], |row| row.get::<_, String>(1))?
        .collect::<Result<Vec<_>, _>>()?;
    if !columns.iter().any(|column| column == "following") {
        connection.execute(
            "ALTER TABLE follows ADD COLUMN following INTEGER NOT NULL DEFAULT 1",
            [],
        )?;
    }
    Ok(())
}

#[derive(Debug, Error)]
pub enum IngestError {
    #[error("invalid Jetstream event: {0}")]
    Json(#[from] serde_json::Error),
    #[error(transparent)]
    Storage(#[from] StorageError),
    #[error("Jetstream websocket failed: {0}")]
    WebSocket(String),
}

#[derive(Deserialize)]
struct JetstreamEvent {
    did: String,
    time_us: u64,
    kind: String,
    commit: Option<JetstreamCommit>,
}

#[derive(Deserialize)]
struct JetstreamCommit {
    operation: String,
    collection: String,
    rkey: String,
    cid: Option<String>,
    record: Option<Value>,
}

#[derive(Default)]
pub struct JetstreamStatus {
    pub state: AtomicU64,
    pub restarts: AtomicU64,
    pub last_event_us: AtomicU64,
}

impl JetstreamStatus {
    pub fn state_name(&self) -> &'static str {
        let state = self.state.load(Ordering::Relaxed);
        if state == 0 {
            return "running";
        }
        if state == 1 {
            return "waiting to reconnect";
        }
        if state == 2 {
            return "restarting";
        }
        "unknown"
    }

    pub fn running(&self) {
        self.state.store(0, Ordering::Relaxed);
    }

    pub fn waiting_to_reconnect(&self) {
        self.state.store(1, Ordering::Relaxed);
    }

    pub fn restarting(&self) {
        self.state.store(2, Ordering::Relaxed);
    }

    pub fn restarted(&self) {
        self.restarts.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_event(&self, time_us: u64) {
        self.last_event_us.store(time_us, Ordering::Relaxed);
    }
}

pub struct Ingestor<S: Storage> {
    storage: Arc<S>,
    identity_cache: Option<Arc<crate::identity::SqliteIdentityCache>>,
    scope: ScopeMode,
    cursor: AtomicU64,
    processed: AtomicU64,
    batch_metrics: Mutex<IngestIntervalMetrics>,
    status: Arc<JetstreamStatus>,
}

#[derive(Default)]
struct IngestBatchPhases {
    parse: Duration,
    scope_lookup: Duration,
    record_lookup: Duration,
    apply: Duration,
    commit: Duration,
    total: Duration,
}

#[derive(Default)]
struct IngestBatchState {
    staged_follows: HashMap<(String, String), bool>,
    staged_follow_deltas: HashMap<String, i64>,
    staged_records: HashMap<(String, String, String), Option<IndexedRecord>>,
    phases: IngestBatchPhases,
}

#[derive(Default)]
struct IngestIntervalMetrics {
    batches: u64,
    total_batch_ms: f64,
    max_batch_ms: f64,
    phase_totals_ms: [f64; 5],
}

impl IngestIntervalMetrics {
    fn add(&mut self, phases: &IngestBatchPhases) {
        let milliseconds = |duration: Duration| duration.as_secs_f64() * 1000.0;
        let batch_ms = milliseconds(phases.total);
        self.batches += 1;
        self.total_batch_ms += batch_ms;
        self.max_batch_ms = self.max_batch_ms.max(batch_ms);
        for (total, phase) in self.phase_totals_ms.iter_mut().zip([
            phases.parse,
            phases.scope_lookup,
            phases.record_lookup,
            phases.apply,
            phases.commit,
        ]) {
            *total += milliseconds(phase);
        }
    }

    fn summary(&self) -> (f64, f64, &'static str, f64) {
        let names = [
            "parse",
            "scope_lookup",
            "record_lookup",
            "apply_jetstream_batch",
            "commit",
        ];
        let slowest = self
            .phase_totals_ms
            .iter()
            .enumerate()
            .max_by(|left, right| left.1.total_cmp(right.1));
        let (slowest_phase, slowest_phase_ms) =
            slowest.map_or(("none", 0.0), |(index, time)| (names[index], *time));
        let average = if self.batches == 0 {
            0.0
        } else {
            self.total_batch_ms / self.batches as f64
        };
        (average, self.max_batch_ms, slowest_phase, slowest_phase_ms)
    }
}

impl<S: Storage> Ingestor<S> {
    pub fn new(storage: Arc<S>, scope: ScopeMode) -> Self {
        let cursor = storage.cursor().ok().flatten().unwrap_or_default();
        Self {
            storage,
            identity_cache: None,
            scope,
            cursor: AtomicU64::new(cursor),
            processed: AtomicU64::new(0),
            batch_metrics: Mutex::new(IngestIntervalMetrics::default()),
            status: Arc::new(JetstreamStatus::default()),
        }
    }

    pub fn with_identity_cache(
        mut self,
        identity_cache: Arc<crate::identity::SqliteIdentityCache>,
    ) -> Self {
        self.identity_cache = Some(identity_cache);
        self
    }

    pub fn with_status(mut self, status: Arc<JetstreamStatus>) -> Self {
        self.status = status;
        self
    }

    pub fn process_json(&self, json: &str) -> Result<(), IngestError> {
        self.process_json_batch(&[json.to_owned()])
    }

    pub fn process_json_batch(&self, json_events: &[String]) -> Result<(), IngestError> {
        if json_events.is_empty() {
            return Ok(());
        }
        let batch_started = Instant::now();
        let mut batch_state = IngestBatchState::default();
        let mut updates = Vec::with_capacity(json_events.len());
        for json in json_events {
            let parse_started = Instant::now();
            let event: JetstreamEvent = serde_json::from_str(json)?;
            batch_state.phases.parse += parse_started.elapsed();
            let scope_started = Instant::now();
            let record_lookup_before = batch_state.phases.record_lookup;
            let mut update = JetstreamStorageUpdate {
                time_us: event.time_us,
                ..JetstreamStorageUpdate::default()
            };
            if matches!(event.kind.as_str(), "identity" | "account") {
                if let Some(cache) = &self.identity_cache
                    && let Err(error) = cache.invalidate_did(&event.did)
                {
                    tracing::warn!(%error, did = %event.did, "could not invalidate identity cache after Jetstream event");
                }
                if self.storage.is_tracked_did(&event.did)?
                    || self.storage.is_displayed_did(&event.did)?
                {
                    update.handle_refresh = Some(event.did.clone());
                }
            }
            if event.kind == "commit"
                && let Some(commit) = event.commit
                && SUPPORTED_COLLECTIONS.contains(&commit.collection.as_str())
            {
                self.process_commit(
                    event.did,
                    event.time_us,
                    commit,
                    &mut update,
                    &mut batch_state,
                )?;
            }
            batch_state.phases.scope_lookup += scope_started.elapsed().saturating_sub(
                batch_state
                    .phases
                    .record_lookup
                    .saturating_sub(record_lookup_before),
            );
            updates.push(update);
        }
        let storage_timing = self.storage.apply_jetstream_batch(&updates)?;
        batch_state.phases.apply = storage_timing.apply;
        batch_state.phases.commit = storage_timing.commit;
        let last_cursor = updates.last().expect("nonempty batch").time_us;
        self.cursor.store(last_cursor, Ordering::Relaxed);
        self.status.record_event(last_cursor);
        self.processed
            .fetch_add(updates.len() as u64, Ordering::Relaxed);
        batch_state.phases.total = batch_started.elapsed();
        tracing::debug!(
            batch_events = json_events.len(),
            parse_ms = batch_state.phases.parse.as_secs_f64() * 1000.0,
            scope_lookup_ms = batch_state.phases.scope_lookup.as_secs_f64() * 1000.0,
            record_lookup_ms = batch_state.phases.record_lookup.as_secs_f64() * 1000.0,
            apply_jetstream_batch_ms = batch_state.phases.apply.as_secs_f64() * 1000.0,
            commit_ms = batch_state.phases.commit.as_secs_f64() * 1000.0,
            total_ms = batch_state.phases.total.as_secs_f64() * 1000.0,
            "Jetstream batch phase timing"
        );
        if let Ok(mut metrics) = self.batch_metrics.lock() {
            metrics.add(&batch_state.phases);
        }
        Ok(())
    }

    fn process_commit(
        &self,
        did: String,
        time_us: u64,
        commit: JetstreamCommit,
        update: &mut JetstreamStorageUpdate,
        batch_state: &mut IngestBatchState,
    ) -> Result<(), IngestError> {
        // Parse and filter target DIDs before querying records or writing anything.
        let referenced_tracked = if self.scope == ScopeMode::Full {
            false
        } else {
            match commit.collection.as_str() {
                "app.bsky.feed.like" | "app.bsky.feed.repost" => commit
                    .record
                    .as_ref()
                    .and_then(|record| record.pointer("/subject/uri"))
                    .and_then(Value::as_str)
                    .and_then(uri_did)
                    .map(|target_did| self.is_tracked_in_batch(target_did, batch_state))
                    .transpose()?
                    .unwrap_or(false),
                "app.bsky.feed.post" => {
                    self.post_references_tracked(commit.record.as_ref(), batch_state)?
                }
                _ => false,
            }
        };
        let actor_tracked = match self.scope {
            ScopeMode::Full => true,
            ScopeMode::Scoped => self.is_tracked_in_batch(&did, batch_state)?,
        };
        // Avoid a database record lookup for unrelated creates and updates. For a
        // delete, inspect the key so previously indexed interactions can be removed.
        let record_key = (did.clone(), commit.collection.clone(), commit.rkey.clone());
        let record_lookup_started = Instant::now();
        let previous =
            if commit.operation == "delete" || commit.collection == "app.bsky.graph.follow" {
                if let Some(previous) = batch_state.staged_records.get(&record_key) {
                    previous.clone()
                } else {
                    self.storage
                        .get_record(&did, &commit.collection, &commit.rkey)?
                }
            } else {
                None
            };
        batch_state.phases.record_lookup += record_lookup_started.elapsed();
        let should_index = actor_tracked || referenced_tracked || previous.is_some();

        if should_index {
            if commit.operation == "delete" {
                update.delete = Some((did.clone(), commit.collection.clone(), commit.rkey.clone()));
                batch_state.staged_records.insert(record_key.clone(), None);
            } else if let Some(record) = commit.record.as_ref() {
                let uri = format!("at://{did}/{}/{}", commit.collection, commit.rkey);
                let indexed = IndexedRecord {
                    did: did.clone(),
                    collection: commit.collection.clone(),
                    rkey: commit.rkey.clone(),
                    uri,
                    cid: commit.cid.clone(),
                    time_us,
                    record: record.clone(),
                };
                update.upsert = Some(indexed.clone());
                batch_state
                    .staged_records
                    .insert(record_key.clone(), Some(indexed));
            }
        }

        if commit.collection == "app.bsky.graph.follow" && self.storage.is_registered(&did)? {
            if let Some(old) = previous.as_ref()
                && let Some(subject) = old.record.get("subject").and_then(Value::as_str)
            {
                update.follows.push(FollowUpdate {
                    actor: did.clone(),
                    subject: subject.to_owned(),
                    following: false,
                });
                self.stage_follow(&did, subject, false, batch_state)?;
            }
            if commit.operation != "delete"
                && let Some(subject) = commit
                    .record
                    .as_ref()
                    .and_then(|record| record.get("subject"))
                && let Some(subject) = subject.as_str()
            {
                update.follows.push(FollowUpdate {
                    actor: did.clone(),
                    subject: subject.to_owned(),
                    following: true,
                });
                self.stage_follow(&did, subject, true, batch_state)?;
                if self.scope == ScopeMode::Scoped {
                    update.backfill_for.push((did, subject.to_owned()));
                }
            }
        }
        Ok(())
    }

    fn is_tracked_in_batch(
        &self,
        did: &str,
        batch_state: &IngestBatchState,
    ) -> Result<bool, IngestError> {
        if self.scope == ScopeMode::Full || self.storage.is_registered(did)? {
            return Ok(true);
        }
        let count = i64::from(self.storage.tracked_follow_count(did)?)
            + batch_state
                .staged_follow_deltas
                .get(did)
                .copied()
                .unwrap_or_default();
        Ok(count > 0)
    }

    fn stage_follow(
        &self,
        actor: &str,
        subject: &str,
        following: bool,
        batch_state: &mut IngestBatchState,
    ) -> Result<(), IngestError> {
        let key = (actor.to_owned(), subject.to_owned());
        let was_following = if let Some(staged) = batch_state.staged_follows.get(&key) {
            *staged
        } else {
            self.storage.is_following(actor, subject)?
        };
        if was_following != following {
            *batch_state
                .staged_follow_deltas
                .entry(subject.to_owned())
                .or_default() += if following { 1 } else { -1 };
        }
        batch_state.staged_follows.insert(key, following);
        Ok(())
    }

    fn post_references_tracked(
        &self,
        record: Option<&Value>,
        batch_state: &IngestBatchState,
    ) -> Result<bool, IngestError> {
        let Some(record) = record else {
            return Ok(false);
        };
        let uri_paths = [
            "/reply/parent/uri",
            "/reply/root/uri",
            "/embed/record/uri",
            "/embed/record/record/uri",
            "/embed/recordWithMedia/record/uri",
            "/embed/recordWithMedia/record/record/uri",
        ];
        for uri in uri_paths
            .iter()
            .filter_map(|path| record.pointer(path).and_then(Value::as_str))
        {
            if let Some(did) = uri_did(uri)
                && self.is_tracked_in_batch(did, batch_state)?
            {
                return Ok(true);
            }
        }
        for did in record
            .get("facets")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(|facet| facet.get("features").and_then(Value::as_array))
            .flatten()
            .filter(|feature| {
                feature.get("$type").and_then(Value::as_str)
                    == Some("app.bsky.richtext.facet#mention")
            })
            .filter_map(|feature| feature.get("did").and_then(Value::as_str))
        {
            if self.is_tracked_in_batch(did, batch_state)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn record_metrics(&self, interval: Duration) {
        let events = self.processed.swap(0, Ordering::Relaxed);
        let interval_metrics = self
            .batch_metrics
            .lock()
            .map(|mut metrics| std::mem::take(&mut *metrics))
            .unwrap_or_default();
        let (avg_batch_ms, max_batch_ms, slowest_phase, slowest_phase_ms) =
            interval_metrics.summary();
        let seconds = interval.as_secs_f64().max(0.001);
        let cursor = self.cursor.load(Ordering::Relaxed);
        let cursor_lag_seconds = (cursor != 0).then(|| {
            let now_us = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_micros()
                .min(u64::MAX as u128) as u64;
            now_us.saturating_sub(cursor) as f64 / 1_000_000.0
        });
        tracing::info!(
            events,
            events_per_second = events as f64 / seconds,
            batches = interval_metrics.batches,
            avg_batch_ms,
            max_batch_ms,
            slowest_phase,
            slowest_phase_ms,
            bytes_on_disk = self.storage.bytes_on_disk(),
            cursor_lag_seconds = ?cursor_lag_seconds,
            consumer_state = self.status.state_name(),
            supervisor_restarts = self.status.restarts.load(Ordering::Relaxed),
            last_event_us = self.status.last_event_us.load(Ordering::Relaxed),
            "Jetstream ingestion metrics"
        );
    }
}

fn uri_did(uri: &str) -> Option<&str> {
    let rest = uri.strip_prefix("at://")?;
    rest.split_once('/')?
        .0
        .strip_prefix("did:")
        .map(|_| rest.split_once('/').unwrap().0)
}

pub struct JetstreamConsumer<S: Storage> {
    ingestor: Arc<Ingestor<S>>,
    reconnect_delay: Duration,
    idle_timeout: Duration,
    event_timeout: Duration,
    metrics_interval: Duration,
    stale_cursor_threshold: Duration,
}

struct JetstreamConnectionLog(String);

impl Drop for JetstreamConnectionLog {
    fn drop(&mut self) {
        tracing::info!(endpoint = %self.0, "Jetstream disconnected");
    }
}

fn safe_jetstream_endpoint(value: &str) -> String {
    let Ok(url) = reqwest::Url::parse(value) else {
        return "invalid endpoint".to_owned();
    };
    let Some(host) = url.host_str() else {
        return "invalid endpoint".to_owned();
    };
    let port = url
        .port()
        .map(|port| format!(":{port}"))
        .unwrap_or_default();
    format!("{}://{host}{port}", url.scheme())
}

fn prepare_jetstream_url<S: Storage>(
    ingestor: &Ingestor<S>,
    stale_threshold: Duration,
    base_url: &str,
) -> Result<String, IngestError> {
    let mut url =
        reqwest::Url::parse(base_url).map_err(|error| IngestError::WebSocket(error.to_string()))?;
    let saved_cursor = ingestor.storage.cursor()?;
    let cursor = if let Some(saved_cursor) = saved_cursor {
        let now_us = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros()
            .min(u64::MAX as u128) as u64;
        let threshold_us = stale_threshold.as_micros().min(u64::MAX as u128) as u64;
        let recent_start = now_us.saturating_sub(30_000_000);
        if now_us.saturating_sub(saved_cursor) > threshold_us && saved_cursor < recent_start {
            ingestor
                .storage
                .record_skipped_window(saved_cursor, recent_start)?;
            ingestor.cursor.store(recent_start, Ordering::Relaxed);
            tracing::warn!(
                from_cursor_us = saved_cursor,
                to_cursor_us = recent_start,
                "Jetstream skipped stale cursor window"
            );
            Some(recent_start)
        } else {
            Some(saved_cursor)
        }
    } else {
        None
    };
    if let Some(cursor) = cursor {
        url.query_pairs_mut()
            .append_pair("cursor", &cursor.to_string());
    }
    for collection in SUPPORTED_COLLECTIONS {
        url.query_pairs_mut()
            .append_pair("wantedCollections", collection);
    }
    Ok(url.to_string())
}

impl<S: Storage + 'static> JetstreamConsumer<S> {
    pub fn new(ingestor: Ingestor<S>, reconnect_delay: Duration) -> Self {
        Self {
            ingestor: Arc::new(ingestor),
            reconnect_delay,
            idle_timeout: Duration::from_secs(60),
            event_timeout: Duration::from_secs(60),
            metrics_interval: Duration::from_secs(10),
            stale_cursor_threshold: Duration::from_secs(10 * 60),
        }
    }

    pub fn with_idle_timeout(mut self, idle_timeout: Duration) -> Self {
        self.idle_timeout = idle_timeout;
        self
    }

    pub fn with_event_timeout(mut self, event_timeout: Duration) -> Self {
        self.event_timeout = event_timeout;
        self
    }

    pub fn with_metrics_interval(mut self, metrics_interval: Duration) -> Self {
        self.metrics_interval = metrics_interval.max(Duration::from_millis(1));
        self
    }

    pub fn with_stale_cursor_threshold(mut self, threshold: Duration) -> Self {
        self.stale_cursor_threshold = threshold;
        self
    }

    pub async fn consume_connection(&self, url: &str) -> Result<(), IngestError> {
        let endpoint = safe_jetstream_endpoint(url);
        let (mut websocket, _) = connect_async(url)
            .await
            .map_err(|_| IngestError::WebSocket("Jetstream connection failed".to_owned()))?;
        tracing::info!(%endpoint, "Jetstream connected");
        let _connection_log = JetstreamConnectionLog(endpoint);
        async {
            let mut idle_deadline = tokio::time::Instant::now() + self.idle_timeout;
            let mut batch_deadline = None;
            let mut pending_events = Vec::with_capacity(JETSTREAM_BATCH_SIZE);
            loop {
                let remaining =
                    idle_deadline.saturating_duration_since(tokio::time::Instant::now());
                let incoming = tokio::select! {
                    _ = async {
                        if let Some(deadline) = batch_deadline {
                            tokio::time::sleep_until(deadline).await;
                        } else {
                            std::future::pending::<()>().await;
                        }
                    }, if !pending_events.is_empty() => {
                        self.process_event_batch(std::mem::take(&mut pending_events)).await?;
                        batch_deadline = None;
                        continue;
                    }
                    incoming = tokio::time::timeout(remaining, websocket.next()) => incoming,
                };
                let message = match incoming {
                    Err(_) => {
                        if !pending_events.is_empty() {
                            self.process_event_batch(std::mem::take(&mut pending_events))
                                .await?;
                        }
                        return Err(IngestError::WebSocket(format!(
                            "no Jetstream event received for {} seconds",
                            self.idle_timeout.as_secs_f64()
                        )));
                    }
                    Ok(None) => {
                        if !pending_events.is_empty() {
                            self.process_event_batch(std::mem::take(&mut pending_events))
                                .await?;
                        }
                        break;
                    }
                    Ok(Some(Err(error))) => {
                        if !pending_events.is_empty() {
                            self.process_event_batch(std::mem::take(&mut pending_events))
                                .await?;
                        }
                        return Err(IngestError::WebSocket(error.to_string()));
                    }
                    Ok(Some(Ok(message))) => message,
                };
                match message {
                    Message::Text(text) => {
                        if pending_events.is_empty() {
                            batch_deadline =
                                Some(tokio::time::Instant::now() + JETSTREAM_BATCH_DELAY);
                        }
                        pending_events.push(text.to_string());
                        idle_deadline = tokio::time::Instant::now() + self.idle_timeout;
                        if pending_events.len() >= JETSTREAM_BATCH_SIZE {
                            self.process_event_batch(std::mem::take(&mut pending_events))
                                .await?;
                            batch_deadline = None;
                        }
                    }
                    Message::Binary(bytes) => {
                        let text = std::str::from_utf8(&bytes)
                            .map_err(|error| IngestError::WebSocket(error.to_string()))?;
                        if pending_events.is_empty() {
                            batch_deadline =
                                Some(tokio::time::Instant::now() + JETSTREAM_BATCH_DELAY);
                        }
                        pending_events.push(text.to_owned());
                        idle_deadline = tokio::time::Instant::now() + self.idle_timeout;
                        if pending_events.len() >= JETSTREAM_BATCH_SIZE {
                            self.process_event_batch(std::mem::take(&mut pending_events))
                                .await?;
                            batch_deadline = None;
                        }
                    }
                    Message::Close(_) => {
                        if !pending_events.is_empty() {
                            self.process_event_batch(std::mem::take(&mut pending_events))
                                .await?;
                        }
                        break;
                    }
                    Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {}
                }
            }
            Ok(())
        }
        .await
    }

    async fn process_event_batch(&self, events: Vec<String>) -> Result<(), IngestError> {
        let event: Value =
            serde_json::from_str(events.last().expect("event batch must not be empty"))?;
        let did = event
            .get("did")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_owned();
        let collection = event
            .pointer("/commit/collection")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_owned();
        let rkey = event
            .pointer("/commit/rkey")
            .and_then(Value::as_str)
            .unwrap_or("unknown")
            .to_owned();
        let mut attempt = 0_u32;
        let mut backoff = JETSTREAM_LOCK_RETRY_FIRST_BACKOFF;
        // Keep lock retries unbounded: this batch is the next cursor position,
        // so dropping it or reconnecting cannot make progress and would only
        // replay the same event. The failed batch remains intact while backing off.
        loop {
            let ingestor = self.ingestor.clone();
            let batch = events.clone();
            let task = tokio::task::spawn_blocking(move || ingestor.process_json_batch(&batch));
            let result = match tokio::time::timeout(self.event_timeout, task).await {
                Err(_) => {
                    tracing::warn!(
                        %did,
                        %collection,
                        %rkey,
                        timeout_seconds = self.event_timeout.as_secs_f64(),
                        "Jetstream event processing timed out"
                    );
                    return Err(IngestError::WebSocket(
                        "event processing timed out".to_owned(),
                    ));
                }
                Ok(Err(error)) => {
                    return Err(IngestError::WebSocket(format!(
                        "Jetstream event processing task failed: {error}"
                    )));
                }
                Ok(Ok(result)) => result,
            };
            match result {
                Ok(()) => return Ok(()),
                Err(error) if is_jetstream_sqlite_lock(&error) => {
                    attempt += 1;
                    self.ingestor.storage.record_jetstream_lock_retry();
                    tracing::warn!(
                        %did,
                        %collection,
                        %rkey,
                        attempt,
                        backoff_ms = backoff.as_millis() as u64,
                        error = %error,
                        "Jetstream batch retry after SQLite lock"
                    );
                    tokio::time::sleep(backoff).await;
                    backoff = (backoff * 2).min(JETSTREAM_LOCK_RETRY_MAX_BACKOFF);
                }
                Err(error) => return Err(error),
            }
        }
    }

    pub fn status(&self) -> Arc<JetstreamStatus> {
        self.ingestor.status.clone()
    }

    pub async fn run(
        &self,
        urls: &[String],
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), IngestError> {
        if urls.is_empty() {
            return Err(IngestError::WebSocket(
                "no Jetstream URLs configured".into(),
            ));
        }
        let mut next_url = 0usize;
        let mut backoff = self.reconnect_delay;
        let metrics_interval = self.metrics_interval.max(Duration::from_millis(1));
        let mut metrics = tokio::time::interval_at(
            tokio::time::Instant::now() + metrics_interval,
            metrics_interval,
        );
        metrics.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            self.ingestor.status.running();
            let base_url = &urls[next_url % urls.len()];
            next_url += 1;
            let ingestor = self.ingestor.clone();
            let stale_threshold = self.stale_cursor_threshold;
            let base_url = base_url.clone();
            let url_result = tokio::task::spawn_blocking(move || {
                prepare_jetstream_url(&ingestor, stale_threshold, &base_url)
            })
            .await
            .map_err(|error| {
                IngestError::WebSocket(format!("Jetstream URL worker failed: {error}"))
            });
            let url = match url_result {
                Ok(Ok(url)) => url,
                Ok(Err(error)) => {
                    tracing::warn!(%error, "Jetstream URL preparation failed");
                    self.ingestor.status.waiting_to_reconnect();
                    tokio::select! {
                        _ = tokio::time::sleep(backoff) => {},
                        _ = metrics.tick() => self.ingestor.record_metrics(metrics_interval),
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow() {
                                return Ok(());
                            }
                        }
                    }
                    backoff = (backoff * 2).min(Duration::from_secs(30));
                    continue;
                }
                Err(error) => {
                    tracing::warn!(%error, "Jetstream URL worker failed");
                    self.ingestor.status.waiting_to_reconnect();
                    tokio::select! {
                        _ = tokio::time::sleep(backoff) => {},
                        _ = metrics.tick() => self.ingestor.record_metrics(metrics_interval),
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow() {
                                return Ok(());
                            }
                        }
                    }
                    backoff = (backoff * 2).min(Duration::from_secs(30));
                    continue;
                }
            };
            let endpoint = safe_jetstream_endpoint(&url);
            let connection_result = {
                let connection = self.consume_connection(&url);
                tokio::pin!(connection);
                loop {
                    tokio::select! {
                        result = &mut connection => break result,
                        _ = metrics.tick() => self.ingestor.record_metrics(metrics_interval),
                        changed = shutdown.changed() => {
                            if changed.is_err() || *shutdown.borrow() {
                                return Ok(());
                            }
                        }
                    }
                }
            };
            match connection_result {
                Ok(()) => backoff = self.reconnect_delay,
                Err(error) => {
                    tracing::warn!(%error, %endpoint, "Jetstream connection ended");
                    self.ingestor.status.waiting_to_reconnect();
                }
            }
            tokio::select! {
                _ = tokio::time::sleep(backoff) => {},
                _ = metrics.tick() => self.ingestor.record_metrics(metrics_interval),
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
            }
            backoff = (backoff * 2).min(Duration::from_secs(30));
        }
    }
}

pub struct JetstreamSupervisor<S: Storage> {
    consumer: Arc<JetstreamConsumer<S>>,
    restart_delay: Duration,
    max_restart_delay: Duration,
}

impl<S: Storage + 'static> JetstreamSupervisor<S> {
    pub fn new(consumer: JetstreamConsumer<S>, restart_delay: Duration) -> Self {
        Self {
            consumer: Arc::new(consumer),
            restart_delay,
            max_restart_delay: Duration::from_secs(30),
        }
    }

    pub fn with_max_restart_delay(mut self, delay: Duration) -> Self {
        self.max_restart_delay = delay.max(self.restart_delay);
        self
    }

    pub fn status(&self) -> Arc<JetstreamStatus> {
        self.consumer.status()
    }

    pub async fn run(
        &self,
        urls: &[String],
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), IngestError> {
        let mut backoff = self.restart_delay;
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            let run_shutdown = shutdown.clone();
            let run_result = std::panic::AssertUnwindSafe(self.consumer.run(urls, run_shutdown))
                .catch_unwind()
                .await;
            match run_result {
                Ok(Ok(())) => return Ok(()),
                Ok(Err(error)) => {
                    tracing::error!(%error, "Jetstream consumer stopped; restarting");
                    self.status().restarting();
                }
                Err(panic) => {
                    let panic_text = format!("{panic:?}");
                    tracing::error!(
                        panic = panic_text,
                        "Jetstream consumer panicked; restarting"
                    );
                    self.status().restarting();
                }
            }
            let slept = tokio::select! {
                _ = tokio::time::sleep(backoff) => true,
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                    false
                }
            };
            if !slept {
                return Ok(());
            }
            self.status().restarted();
            backoff = (backoff * 2).min(self.max_restart_delay);
        }
    }
}

#[cfg(test)]
mod web_read_timeout_tests {
    use super::*;

    #[tokio::test]
    async fn a_slow_web_sqlite_read_is_interrupted_at_its_budget() {
        let storage = Arc::new(SqliteStorage::open(":memory:").unwrap());
        let started = Instant::now();
        let timeouts_before = web_read_timeout_count();
        let result = web_blocking_call_with_budget(
            storage,
            "slow_read_test",
            Duration::from_millis(20),
            |storage| {
                let connection = storage.read_conn()?;
                Ok(connection.query_row(
                    "WITH RECURSIVE sequence(value) AS (
                         VALUES(0) UNION ALL SELECT value + 1 FROM sequence WHERE value < 10000000
                     ) SELECT sum(value) FROM sequence",
                    [],
                    |row| row.get::<_, i64>(0),
                )?)
            },
        )
        .await;

        assert!(
            matches!(
                result,
                Err(StorageError::WebReadTimeout {
                    operation: "slow_read_test",
                    ..
                })
            ),
            "a 10-million-step SQLite read exceeded its 20ms web budget but returned {result:?}"
        );
        assert!(
            started.elapsed() < Duration::from_secs(1),
            "the interrupted web read took {:?}",
            started.elapsed()
        );
        assert!(web_read_timeout_count() > timeouts_before);
    }
}

#[cfg(test)]
mod backfill_resource_tests {
    use super::*;

    #[test]
    fn backfill_queue_has_a_global_storage_cap() {
        let memory = MemoryStorage::default();
        let sqlite = SqliteStorage::open(":memory:").unwrap();
        for index in 0..=1_000 {
            let did = format!("did:plc:account-{index:04}");
            memory.register_user(&did).unwrap();
            sqlite.register_user(&did).unwrap();
        }

        let memory_count = memory.inner.lock().unwrap().backfill_queue.len();
        let sqlite_count: usize = sqlite
            .writer
            .call(|connection| {
                Ok(connection
                    .query_row("SELECT COUNT(*) FROM backfill_queue", [], |row| row.get(0))?)
            })
            .unwrap();
        assert!(
            memory_count <= 1_000,
            "memory queue has {memory_count} items"
        );
        assert!(
            sqlite_count <= 1_000,
            "SQLite queue has {sqlite_count} items"
        );
    }

    #[test]
    fn opening_a_database_trims_an_oversized_legacy_backfill_queue() {
        let path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join(format!(
                "backfill-migration-{}-{}.sqlite",
                std::process::id(),
                SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_nanos()
            ));
        let storage = SqliteStorage::open(&path).unwrap();
        storage
            .writer
            .call(|connection| {
                for index in 0..=1_000 {
                    connection.execute(
                        "INSERT INTO backfill_queue(did) VALUES(?1)",
                        [format!("did:plc:legacy-{index:04}")],
                    )?;
                }
                Ok(())
            })
            .unwrap();
        drop(storage);

        let reopened = SqliteStorage::open(&path).unwrap();
        let queue_count: usize = reopened
            .writer
            .call(|connection| {
                Ok(connection
                    .query_row("SELECT COUNT(*) FROM backfill_queue", [], |row| row.get(0))?)
            })
            .unwrap();
        drop(reopened);
        std::fs::remove_file(&path).unwrap();
        assert!(queue_count <= 1_000, "SQLite queue has {queue_count} items");
    }
}

#[cfg(test)]
mod jetstream_catchup_tests {
    use super::*;

    #[tokio::test]
    async fn a_busy_batch_retries_without_advancing_or_reconnecting_the_consumer() {
        let storage = Arc::new(MemoryStorage::default());
        storage.set_apply_jetstream_busy_failures(2);
        let consumer = JetstreamConsumer::new(
            Ingestor::new(storage.clone(), ScopeMode::Full),
            Duration::from_millis(1),
        );
        let event = r#"{"did":"did:plc:av84-ingest","time_us":1234,"kind":"commit","commit":{"operation":"create","collection":"app.bsky.feed.post","rkey":"post-r1","cid":"cid-r1","record":{"$type":"app.bsky.feed.post","text":"retry me"}}}"#;

        consumer
            .process_event_batch(vec![event.to_owned()])
            .await
            .unwrap();

        assert_eq!(storage.apply_jetstream_attempts.load(Ordering::SeqCst), 3);
        assert_eq!(storage.jetstream_lock_retries.load(Ordering::SeqCst), 2);
        assert_eq!(storage.cursor().unwrap(), Some(1234));
        assert!(
            storage
                .record_by_uri(
                    "app.bsky.feed.post",
                    "at://did:plc:av84-ingest/app.bsky.feed.post/post-r1"
                )
                .unwrap()
                .is_some()
        );
    }

    #[tokio::test]
    async fn a_non_lock_apply_error_returns_without_retrying() {
        let storage = Arc::new(MemoryStorage::default());
        storage.set_apply_jetstream_nonlock_failures(1);
        let consumer = JetstreamConsumer::new(
            Ingestor::new(storage.clone(), ScopeMode::Full),
            Duration::from_millis(1),
        );
        let event = r#"{"did":"did:plc:av85-ingest-error","time_us":1235,"kind":"commit","commit":{"operation":"create","collection":"app.bsky.feed.post","rkey":"post-error-r1","cid":"cid-error-r1","record":{"$type":"app.bsky.feed.post","text":"fail once"}}}"#;

        let result = tokio::time::timeout(
            Duration::from_millis(250),
            consumer.process_event_batch(vec![event.to_owned()]),
        )
        .await
        .expect("a non-lock failure must return instead of retrying indefinitely");
        assert!(matches!(
            result,
            Err(IngestError::Storage(StorageError::Poisoned))
        ));
        assert_eq!(storage.apply_jetstream_attempts.load(Ordering::SeqCst), 1);
        assert_eq!(storage.jetstream_lock_retries.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn stale_saved_cursor_starts_recently_and_records_the_skipped_window() {
        let storage = Arc::new(SqliteStorage::open(":memory:").unwrap());
        let now_us = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as u64;
        let stale_cursor = now_us - 30 * 60 * 1_000_000;
        storage.set_cursor(stale_cursor).unwrap();
        let consumer = JetstreamConsumer::new(
            Ingestor::new(storage.clone(), ScopeMode::Full),
            Duration::from_millis(1),
        );

        let url = prepare_jetstream_url(
            &consumer.ingestor,
            consumer.stale_cursor_threshold,
            "wss://jetstream.example/subscribe",
        )
        .unwrap();
        let url = reqwest::Url::parse(&url).unwrap();
        let reconnect_cursor = url
            .query_pairs()
            .find(|(key, _)| key == "cursor")
            .unwrap()
            .1
            .parse::<u64>()
            .unwrap();
        assert!(reconnect_cursor > stale_cursor, "{reconnect_cursor}");
        assert!(now_us.saturating_sub(reconnect_cursor) <= 31_000_000);

        let skipped = storage.skipped_windows().unwrap();
        assert_eq!(skipped.last().unwrap().from, stale_cursor);
        assert_eq!(skipped.last().unwrap().to, reconnect_cursor);
        let persisted: String = storage
            .writer
            .call(|connection| {
                Ok(connection.query_row(
                    "SELECT value FROM appview_state WHERE key='jetstream_skipped_windows'",
                    [],
                    |row| row.get(0),
                )?)
            })
            .unwrap();
        assert!(persisted.contains(&stale_cursor.to_string()));
        assert!(persisted.contains(&reconnect_cursor.to_string()));
        assert_eq!(storage.cursor().unwrap(), Some(reconnect_cursor));
    }
}

#[cfg(test)]
mod sqlite_writer_tests {
    use super::*;

    fn test_path(label: &str) -> PathBuf {
        let target = Path::new(env!("CARGO_MANIFEST_DIR")).join("target");
        fs::create_dir_all(&target).unwrap();
        target.join(format!(
            "sqlite-writer-{label}-{}-{}.sqlite3",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    fn remove_database(path: &Path) {
        let _ = fs::remove_file(path);
        let _ = fs::remove_file(format!("{}-wal", path.display()));
        let _ = fs::remove_file(format!("{}-shm", path.display()));
    }

    fn seed_force_shrink_wal(path: &Path, rows: usize) -> Connection {
        let writer = Connection::open(path).unwrap();
        writer
            .execute_batch(
                "PRAGMA journal_mode=WAL;
                 PRAGMA wal_autocheckpoint=0;
                 PRAGMA journal_size_limit=65536;
                 CREATE TABLE t(value BLOB);",
            )
            .unwrap();
        for _ in 0..rows {
            writer
                .execute("INSERT INTO t VALUES (zeroblob(32768))", [])
                .unwrap();
        }
        writer
    }

    fn force_shrink_config(
        threshold_bytes: u64,
        interval: Duration,
        busy_timeout: Duration,
    ) -> WalForceShrinkConfig {
        WalForceShrinkConfig {
            threshold_bytes,
            interval,
            busy_timeout,
        }
    }

    fn overdue_force_shrink_clock(interval: Duration) -> (Instant, Instant) {
        let now = Instant::now();
        (now - interval - Duration::from_millis(1), now)
    }

    static CHECKPOINT_BUSY_CALLS: AtomicU64 = AtomicU64::new(0);
    static WRITER_BUSY_WAIT_STARTED: AtomicBool = AtomicBool::new(false);

    fn record_busy_during_forced_shrink(_attempt: i32) -> bool {
        if FORCE_SHRINK_TEST_ACTIVE.load(Ordering::SeqCst) {
            FORCE_SHRINK_WRITER_BUSY.store(true, Ordering::SeqCst);
        }
        false
    }

    fn count_busy_call(_attempt: i32) -> bool {
        CHECKPOINT_BUSY_CALLS.fetch_add(1, Ordering::Relaxed);
        false
    }

    fn hold_writer_busy_for_test(_attempt: i32) -> bool {
        WRITER_BUSY_WAIT_STARTED.store(true, Ordering::SeqCst);
        std::thread::sleep(Duration::from_millis(100));
        false
    }

    #[test]
    fn the_writer_counts_a_busy_call_with_an_external_lock_holder() {
        let path = test_path("busy-metric");
        remove_database(&path);
        let storage = SqliteStorage::open(&path).unwrap();
        storage
            .writer
            .call(|connection| {
                connection.busy_timeout(Duration::from_millis(1))?;
                Ok(())
            })
            .unwrap();
        let holder = Connection::open(&path).unwrap();
        holder
            .execute_batch("PRAGMA busy_timeout=5000; BEGIN IMMEDIATE;")
            .unwrap();

        let record = IndexedRecord {
            did: "did:plc:av84-busy-metric".to_owned(),
            collection: "app.bsky.feed.post".to_owned(),
            rkey: "busy-r1".to_owned(),
            uri: "at://did:plc:av84-busy-metric/app.bsky.feed.post/busy-r1".to_owned(),
            cid: Some("cid-busy".to_owned()),
            time_us: 1,
            record: serde_json::json!({"$type":"app.bsky.feed.post","text":"busy"}),
        };
        let error = storage.put_record(&record).unwrap_err();
        assert!(is_sqlite_busy_error(&error), "unexpected error: {error}");
        assert_eq!(
            storage.writer.metrics.sqlite_busy.load(Ordering::Relaxed),
            1
        );
        assert_eq!(
            checkpoint_lock_holder_since(
                &storage.wal_metrics,
                storage.wal_metrics.checkpoint_observation(),
            ),
            "unknown_or_external_writer"
        );

        holder.execute_batch("COMMIT;").unwrap();
        drop(holder);
        drop(storage);
        remove_database(&path);
    }

    #[test]
    fn production_writer_keeps_the_configured_wal_journal_size_limit() {
        let path = test_path("writer-journal-size-limit");
        remove_database(&path);
        let storage = SqliteStorage::open(&path).unwrap();
        let limit = storage
            .writer
            .call(|connection| {
                Ok(connection
                    .query_row("PRAGMA journal_size_limit", [], |row| row.get::<_, i64>(0))?)
            })
            .unwrap();
        assert_eq!(limit, WAL_JOURNAL_SIZE_LIMIT_BYTES as i64);
        drop(storage);
        remove_database(&path);
    }

    #[test]
    fn forced_shrink_truncates_large_wal_once_and_honors_the_interval() {
        let path = test_path("forced-shrink-interval");
        remove_database(&path);
        let writer = seed_force_shrink_wal(&path, 64);
        let checkpointer = Connection::open(&path).unwrap();
        checkpointer.busy_timeout(CHECKPOINT_BUSY_TIMEOUT).unwrap();
        checkpointer
            .execute_batch("PRAGMA journal_size_limit=65536;")
            .unwrap();
        let metrics = WalMetrics::default();
        let threshold = 32_768;
        let interval = Duration::from_secs(10);
        let config = force_shrink_config(threshold, interval, Duration::from_millis(150));
        let (mut last_attempt, first_now) = overdue_force_shrink_clock(interval);
        let mut bytes_before_truncate = 0;

        let before = wal_file_bytes(&path);
        assert!(before > threshold, "WAL only had {before} bytes");
        checkpoint_wal_with_force_shrink(
            &checkpointer,
            &path,
            &metrics,
            config,
            &mut last_attempt,
            first_now,
            || bytes_before_truncate = wal_file_bytes(&path),
        )
        .unwrap();
        let after = wal_file_bytes(&path);
        assert!(
            after <= 65_536,
            "TRUNCATE left a {after}-byte WAL above journal_size_limit"
        );
        assert_eq!(metrics.wal_force_shrink_attempts.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.wal_force_shrink_ok.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.wal_force_shrink_busy.load(Ordering::Relaxed), 0);
        assert_eq!(
            metrics
                .wal_force_shrink_bytes_before
                .load(Ordering::Relaxed),
            bytes_before_truncate
        );
        assert_eq!(
            metrics.wal_force_shrink_bytes_after.load(Ordering::Relaxed),
            after
        );

        for _ in 0..4 {
            writer
                .execute("INSERT INTO t VALUES (zeroblob(32768))", [])
                .unwrap();
        }
        let second_round_before = wal_file_bytes(&path);
        assert!(
            second_round_before > threshold,
            "second round was not above threshold: {second_round_before}"
        );
        checkpoint_wal_with_force_shrink(
            &checkpointer,
            &path,
            &metrics,
            config,
            &mut last_attempt,
            first_now + Duration::from_secs(1),
            || {},
        )
        .unwrap();
        assert_eq!(metrics.wal_force_shrink_attempts.load(Ordering::Relaxed), 1);

        drop((checkpointer, writer));
        remove_database(&path);
    }

    #[test]
    fn forced_shrink_skips_small_wals_and_zero_threshold_disables_it() {
        let interval = Duration::from_secs(5);
        let (mut last_attempt, now) = overdue_force_shrink_clock(interval);

        let small_path = test_path("forced-shrink-below-threshold");
        remove_database(&small_path);
        let small_writer = seed_force_shrink_wal(&small_path, 1);
        let small_checkpointer = Connection::open(&small_path).unwrap();
        let small_bytes = wal_file_bytes(&small_path);
        assert!(small_bytes > 0 && small_bytes < 1_000_000, "{small_bytes}");
        let small_metrics = WalMetrics::default();
        checkpoint_wal_with_force_shrink(
            &small_checkpointer,
            &small_path,
            &small_metrics,
            force_shrink_config(1_000_000, interval, Duration::from_millis(150)),
            &mut last_attempt,
            now,
            || {},
        )
        .unwrap();
        assert_eq!(
            small_metrics
                .wal_force_shrink_attempts
                .load(Ordering::Relaxed),
            0
        );
        drop((small_checkpointer, small_writer));
        remove_database(&small_path);

        let disabled_path = test_path("forced-shrink-disabled");
        remove_database(&disabled_path);
        let disabled_writer = seed_force_shrink_wal(&disabled_path, 64);
        let disabled_checkpointer = Connection::open(&disabled_path).unwrap();
        assert!(wal_file_bytes(&disabled_path) > 32_768);
        let disabled_metrics = WalMetrics::default();
        let (mut disabled_last_attempt, disabled_now) = overdue_force_shrink_clock(interval);
        checkpoint_wal_with_force_shrink(
            &disabled_checkpointer,
            &disabled_path,
            &disabled_metrics,
            force_shrink_config(0, interval, Duration::from_millis(150)),
            &mut disabled_last_attempt,
            disabled_now,
            || {},
        )
        .unwrap();
        assert_eq!(
            disabled_metrics
                .wal_force_shrink_attempts
                .load(Ordering::Relaxed),
            0
        );
        drop((disabled_checkpointer, disabled_writer));
        remove_database(&disabled_path);
    }

    #[test]
    fn forced_shrink_busy_wait_is_bounded_restores_timeout_and_consumes_interval() {
        let path = test_path("forced-shrink-busy-timeout");
        remove_database(&path);
        let writer = seed_force_shrink_wal(&path, 64);
        let checkpointer = Connection::open(&path).unwrap();
        checkpointer.busy_timeout(CHECKPOINT_BUSY_TIMEOUT).unwrap();
        let metrics = WalMetrics::default();
        let interval = Duration::from_secs(10);
        let timeout = Duration::from_millis(200);
        let config = force_shrink_config(1_024, interval, timeout);
        let (mut last_attempt, first_now) = overdue_force_shrink_clock(interval);
        let reader_path = path.clone();
        let (start_reader, reader_started) = mpsc::channel();
        let (reader_ready, wait_until_ready) = mpsc::channel();
        let (release_reader, wait_for_release) = mpsc::channel();
        let reader = std::thread::spawn(move || {
            let reader = Connection::open(reader_path).unwrap();
            reader_started.recv().unwrap();
            reader.execute_batch("BEGIN;").unwrap();
            let _: i64 = reader
                .query_row("SELECT COUNT(*) FROM t", [], |row| row.get(0))
                .unwrap();
            reader_ready.send(()).unwrap();
            // Keep the real SQLite read snapshot beyond the configured timeout.
            let _ = wait_for_release.recv_timeout(Duration::from_secs(5));
            reader.execute_batch("COMMIT;").unwrap();
        });

        let started = Instant::now();
        checkpoint_wal_with_force_shrink(
            &checkpointer,
            &path,
            &metrics,
            config,
            &mut last_attempt,
            first_now,
            || {
                start_reader.send(()).unwrap();
                wait_until_ready.recv().unwrap();
                writer
                    .execute("INSERT INTO t VALUES (zeroblob(32768))", [])
                    .unwrap();
            },
        )
        .unwrap();
        let helper_elapsed = started.elapsed();
        release_reader.send(()).unwrap();
        reader.join().unwrap();
        let truncate_ms =
            metrics.wal_force_shrink_ms_max_ns.load(Ordering::Relaxed) as f64 / 1_000_000.0;
        assert_eq!(metrics.wal_force_shrink_attempts.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.wal_force_shrink_busy.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.wal_force_shrink_ok.load(Ordering::Relaxed), 0);
        assert!(
            (140.0..700.0).contains(&truncate_ms),
            "TRUNCATE duration {truncate_ms:.1}ms did not reflect its {timeout:?} bound"
        );
        let restored_timeout_ms: i64 = checkpointer
            .query_row("PRAGMA busy_timeout", [], |row| row.get(0))
            .unwrap();
        assert_eq!(
            restored_timeout_ms,
            CHECKPOINT_BUSY_TIMEOUT.as_millis() as i64
        );

        // The failed attempt still starts the interval. Once the reader is
        // gone, a caught-up oversized WAL in the next round must not retry.
        checkpoint_wal_with_force_shrink(
            &checkpointer,
            &path,
            &metrics,
            config,
            &mut last_attempt,
            first_now + Duration::from_secs(1),
            || {},
        )
        .unwrap();
        assert_eq!(metrics.wal_force_shrink_attempts.load(Ordering::Relaxed), 1);

        // Verify the normal busy handler was restored, not just the PRAGMA's
        // displayed value: a new real writer lock should wait about 50 ms.
        let holder = Connection::open(&path).unwrap();
        holder.execute_batch("BEGIN IMMEDIATE;").unwrap();
        let normal_wait_started = Instant::now();
        let error = checkpointer
            .execute("INSERT INTO t VALUES (zeroblob(1))", [])
            .unwrap_err();
        let normal_wait = normal_wait_started.elapsed();
        assert!(is_rusqlite_busy_error(&error), "unexpected error: {error}");
        assert!(
            (Duration::from_millis(30)..Duration::from_secs(1)).contains(&normal_wait),
            "normal timeout after forced shrink was {normal_wait:?}"
        );
        holder.execute_batch("COMMIT;").unwrap();
        eprintln!(
            "forced TRUNCATE with a reader-pinned new frame: truncate_ms={truncate_ms:.1}, helper_ms={:.1}, bounded_timeout_ms={}, restored_busy_ms={:.1}",
            helper_elapsed.as_secs_f64() * 1_000.0,
            timeout.as_millis(),
            normal_wait.as_secs_f64() * 1_000.0
        );

        drop((checkpointer, writer));
        remove_database(&path);
    }

    #[tokio::test]
    async fn jetstream_retries_a_busy_write_overlapping_forced_shrink() {
        let path = test_path("forced-shrink-jetstream-retry");
        remove_database(&path);
        let storage = Arc::new(SqliteStorage::open(&path).unwrap());
        FORCE_SHRINK_TEST_ACTIVE.store(false, Ordering::SeqCst);
        FORCE_SHRINK_CHECKPOINT_BUSY.store(false, Ordering::SeqCst);
        FORCE_SHRINK_WRITER_BUSY.store(false, Ordering::SeqCst);
        storage
            .writer
            .call(|connection| {
                connection.busy_timeout(Duration::from_millis(1))?;
                connection.execute_batch(
                    "PRAGMA wal_autocheckpoint=0;
                     PRAGMA journal_size_limit=65536;
                     CREATE TABLE lane_force_shrink(value BLOB);",
                )?;
                for _ in 0..64 {
                    connection
                        .execute("INSERT INTO lane_force_shrink VALUES (zeroblob(32768))", [])?;
                }
                connection.busy_handler(Some(record_busy_during_forced_shrink))?;
                Ok(())
            })
            .unwrap();
        let checkpointer = Connection::open(&path).unwrap();
        checkpointer.busy_timeout(CHECKPOINT_BUSY_TIMEOUT).unwrap();
        let metrics = Arc::clone(&storage.wal_metrics);
        let interval = Duration::from_secs(10);
        let config = force_shrink_config(1_024, interval, Duration::from_millis(180));
        let (mut last_attempt, first_now) = overdue_force_shrink_clock(interval);
        let blocker_slot = Arc::new(Mutex::new(None::<Connection>));
        let callback_slot = Arc::clone(&blocker_slot);
        let (truncate_ready, ready_for_writer) = mpsc::channel();
        let checkpoint_path = path.clone();
        let checkpoint_metrics = Arc::clone(&metrics);
        let checkpoint = std::thread::spawn(move || {
            let result = checkpoint_wal_with_force_shrink(
                &checkpointer,
                &checkpoint_path,
                &checkpoint_metrics,
                config,
                &mut last_attempt,
                first_now,
                || {
                    FORCE_SHRINK_TEST_ACTIVE.store(true, Ordering::SeqCst);
                    let blocker = Connection::open(&checkpoint_path).unwrap();
                    blocker
                        .execute_batch("PRAGMA busy_timeout=1000; BEGIN IMMEDIATE;")
                        .unwrap();
                    *callback_slot.lock().unwrap() = Some(blocker);
                    truncate_ready.send(()).unwrap();
                },
            );
            FORCE_SHRINK_TEST_ACTIVE.store(false, Ordering::SeqCst);
            result.unwrap();
        });
        ready_for_writer.recv().unwrap();
        let busy_wait_started = Instant::now();
        while !FORCE_SHRINK_CHECKPOINT_BUSY.load(Ordering::SeqCst) {
            assert!(
                busy_wait_started.elapsed() < Duration::from_secs(1),
                "real TRUNCATE did not reach SQLite's busy handler"
            );
            std::thread::yield_now();
        }
        let blocker_to_release = Arc::clone(&blocker_slot);
        let release = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(260));
            blocker_to_release
                .lock()
                .unwrap()
                .take()
                .unwrap()
                .execute_batch("COMMIT;")
                .unwrap();
        });

        let consumer = JetstreamConsumer::new(
            Ingestor::new(storage.clone(), ScopeMode::Full),
            Duration::from_millis(1),
        )
        .with_event_timeout(Duration::from_secs(3));
        let event = r#"{"did":"did:plc:av89-force-shrink","time_us":123456,"kind":"commit","commit":{"operation":"create","collection":"app.bsky.feed.post","rkey":"retry-r1","cid":"cid-retry-r1","record":{"$type":"app.bsky.feed.post","text":"retry after forced WAL shrink"}}}"#;
        let ingest_started = Instant::now();
        consumer
            .process_event_batch(vec![event.to_owned()])
            .await
            .unwrap();
        let ingest_elapsed = ingest_started.elapsed();
        checkpoint.join().unwrap();
        release.join().unwrap();
        assert!(
            storage
                .writer
                .metrics
                .jetstream_lock_retries
                .load(Ordering::Relaxed)
                > 0,
            "the event did not take the SQLITE_BUSY retry path"
        );
        assert_eq!(metrics.wal_force_shrink_attempts.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.wal_force_shrink_busy.load(Ordering::Relaxed), 1);
        assert_eq!(metrics.wal_force_shrink_ok.load(Ordering::Relaxed), 0);
        assert!(
            FORCE_SHRINK_WRITER_BUSY.load(Ordering::SeqCst),
            "writer SQLITE_BUSY did not overlap the real forced TRUNCATE call"
        );
        assert!(
            storage
                .record_by_uri(
                    "app.bsky.feed.post",
                    "at://did:plc:av89-force-shrink/app.bsky.feed.post/retry-r1"
                )
                .unwrap()
                .is_some()
        );
        assert_eq!(storage.cursor().unwrap(), Some(123456));
        eprintln!(
            "forced-shrink concurrent writer retry: retries={} ingest_ms={:.1} truncate_ms={:.1} shrink_busy={}",
            storage
                .writer
                .metrics
                .jetstream_lock_retries
                .load(Ordering::Relaxed),
            ingest_elapsed.as_secs_f64() * 1_000.0,
            metrics.wal_force_shrink_ms_max_ns.load(Ordering::Relaxed) as f64 / 1_000_000.0,
            metrics.wal_force_shrink_busy.load(Ordering::Relaxed)
        );

        drop(storage);
        remove_database(&path);
    }

    #[test]
    fn checkpoint_busy_results_increment_the_running_busy_counter() {
        let connection = Connection::open_in_memory().unwrap();
        let metrics = WalMetrics::default();
        let checkpoint =
            checkpoint_wal_with(&connection, Path::new(":memory:"), &metrics, |_, kind| {
                assert_eq!(kind, CHECKPOINT_PASSIVE);
                Ok((1, 9, 8))
            })
            .unwrap();
        assert_eq!(checkpoint.busy, 1);
        assert_eq!(metrics.sqlite_busy.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn wal_generation_changes_count_resets_even_when_file_size_is_reused() {
        let path = test_path("wal-generation-reset");
        remove_database(&path);
        let wal_path = PathBuf::from(format!("{}-wal", path.display()));
        let metrics = WalMetrics::default();
        let mut header = [0_u8; 24];
        header[16..24].copy_from_slice(&1_u64.to_be_bytes());
        fs::write(&wal_path, header).unwrap();
        metrics.record_checkpoint(&path, 65_536, 10, 10, false);

        header[16..24].copy_from_slice(&2_u64.to_be_bytes());
        fs::write(&wal_path, header).unwrap();
        metrics.record_checkpoint(&path, 65_536, 2, 2, false);

        assert_eq!(metrics.file_bytes_max.load(Ordering::Relaxed), 65_536);
        assert_eq!(metrics.resets.load(Ordering::Relaxed), 1);
        remove_database(&path);
    }

    #[test]
    fn a_checkpoint_ending_before_a_busy_writer_returns_is_still_attributed() {
        let path = test_path("checkpoint-holder-window");
        remove_database(&path);
        let storage = Arc::new(SqliteStorage::open(&path).unwrap());
        WRITER_BUSY_WAIT_STARTED.store(false, Ordering::SeqCst);
        storage
            .writer
            .call(move |connection| {
                connection.busy_handler(Some(hold_writer_busy_for_test))?;
                Ok(())
            })
            .unwrap();
        let holder = Connection::open(&path).unwrap();
        holder
            .execute_batch("PRAGMA busy_timeout=5000; BEGIN IMMEDIATE;")
            .unwrap();

        let observed = storage.wal_metrics.checkpoint_observation();
        let record = IndexedRecord {
            did: "did:plc:av85-checkpoint-window".to_owned(),
            collection: "app.bsky.feed.post".to_owned(),
            rkey: "checkpoint-window-r1".to_owned(),
            uri: "at://did:plc:av85-checkpoint-window/app.bsky.feed.post/checkpoint-window-r1"
                .to_owned(),
            cid: Some("checkpoint-window-cid-r1".to_owned()),
            time_us: 1,
            record: serde_json::json!({"$type":"app.bsky.feed.post","text":"busy"}),
        };
        let writer_storage = Arc::clone(&storage);
        let writer = std::thread::spawn(move || writer_storage.put_record(&record));
        let wait_started = Instant::now();
        while !WRITER_BUSY_WAIT_STARTED.load(Ordering::SeqCst) {
            assert!(
                wait_started.elapsed() < Duration::from_secs(1),
                "the write reached SQLite's busy wait"
            );
            std::thread::sleep(Duration::from_millis(1));
        }

        let checkpoint = storage.wal_metrics.checkpoint_activity(CHECKPOINT_TRUNCATE);
        std::thread::sleep(Duration::from_millis(20));
        drop(checkpoint);
        let error = writer.join().unwrap().unwrap_err();
        assert!(is_sqlite_busy_error(&error), "unexpected error: {error}");
        assert_eq!(
            checkpoint_lock_holder_since(&storage.wal_metrics, observed),
            "checkpoint_TRUNCATE",
            "the checkpoint ended before the writer gave up but overlapped its wait"
        );

        holder.execute_batch("COMMIT;").unwrap();
        drop((holder, storage));
        remove_database(&path);
    }

    static N5C_BUSY_WAIT_STARTED: AtomicBool = AtomicBool::new(false);
    static N5C_BUSY_WAIT_RELEASED: AtomicBool = AtomicBool::new(false);

    fn n5c_hold_writer_busy_for_test(_attempt: i32) -> bool {
        N5C_BUSY_WAIT_STARTED.store(true, Ordering::SeqCst);
        while !N5C_BUSY_WAIT_RELEASED.load(Ordering::SeqCst) {
            std::thread::yield_now();
        }
        false
    }

    #[test]
    fn writer_call_uses_the_checkpoint_holder_observed_before_its_write() {
        struct CaptureWriter(Arc<Mutex<Vec<u8>>>);
        impl std::io::Write for CaptureWriter {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        struct CaptureMakeWriter(Arc<Mutex<Vec<u8>>>);
        impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CaptureMakeWriter {
            type Writer = CaptureWriter;
            fn make_writer(&'a self) -> Self::Writer {
                CaptureWriter(Arc::clone(&self.0))
            }
        }

        let path = test_path("writer-call-checkpoint-holder");
        remove_database(&path);
        let storage = SqliteStorage::open(&path).unwrap();
        N5C_BUSY_WAIT_STARTED.store(false, Ordering::SeqCst);
        N5C_BUSY_WAIT_RELEASED.store(false, Ordering::SeqCst);
        storage
            .writer
            .call(|connection| {
                connection.busy_handler(Some(n5c_hold_writer_busy_for_test))?;
                Ok(())
            })
            .unwrap();
        let holder = Connection::open(&path).unwrap();
        holder
            .execute_batch("PRAGMA busy_timeout=5000; BEGIN IMMEDIATE;")
            .unwrap();

        let output = Arc::new(Mutex::new(Vec::new()));
        let _capture = tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .without_time()
                .with_ansi(false)
                .with_max_level(tracing::Level::INFO)
                .with_writer(CaptureMakeWriter(Arc::clone(&output)))
                .finish(),
        );
        tracing::callsite::rebuild_interest_cache();

        let record = IndexedRecord {
            did: "did:plc:av87-writer-call-holder".to_owned(),
            collection: "app.bsky.feed.post".to_owned(),
            rkey: "writer-call-holder-r1".to_owned(),
            uri: "at://did:plc:av87-writer-call-holder/app.bsky.feed.post/writer-call-holder-r1"
                .to_owned(),
            cid: Some("av87-writer-call-holder-cid".to_owned()),
            time_us: 1,
            record: serde_json::json!({"$type":"app.bsky.feed.post","text":"busy"}),
        };
        let checkpoint = storage.wal_metrics.checkpoint_activity(CHECKPOINT_TRUNCATE);
        let error = std::thread::scope(|scope| {
            scope.spawn(move || {
                let deadline = Instant::now() + Duration::from_secs(1);
                while !N5C_BUSY_WAIT_STARTED.load(Ordering::SeqCst) {
                    if Instant::now() >= deadline {
                        N5C_BUSY_WAIT_RELEASED.store(true, Ordering::SeqCst);
                        panic!("write did not reach SQLite busy handler");
                    }
                    std::thread::yield_now();
                }
                drop(checkpoint);
                N5C_BUSY_WAIT_RELEASED.store(true, Ordering::SeqCst);
            });
            storage.put_record(&record).unwrap_err()
        });
        assert!(is_sqlite_busy_error(&error), "unexpected error: {error}");
        let text = String::from_utf8(output.lock().unwrap().clone()).unwrap();
        assert!(
            text.contains("lock_holder=\"checkpoint_TRUNCATE\""),
            "writer.call did not attribute the holder observed before its write: {text}"
        );

        holder.execute_batch("COMMIT;").unwrap();
        drop((holder, storage));
        remove_database(&path);
    }

    #[test]
    fn a_checkpoint_that_is_behind_never_waits_for_readers() {
        let path = test_path("behind-no-truncate");
        remove_database(&path);
        let writer = Connection::open(&path).unwrap();
        writer
            .execute_batch(
                "PRAGMA journal_mode = WAL; PRAGMA wal_autocheckpoint = 0;
                 CREATE TABLE t(x INTEGER); INSERT INTO t VALUES (1);",
            )
            .unwrap();
        let reader = Connection::open(&path).unwrap();
        reader.execute_batch("BEGIN").unwrap();
        let _: i64 = reader
            .query_row("SELECT count(*) FROM t", [], |row| row.get(0))
            .unwrap();
        for value in 0..50 {
            writer
                .execute("INSERT INTO t VALUES (?1)", [value])
                .unwrap();
        }
        let checkpointer = Connection::open(&path).unwrap();
        checkpointer.busy_handler(Some(count_busy_call)).unwrap();
        CHECKPOINT_BUSY_CALLS.store(0, Ordering::Relaxed);
        let wal_metrics = WalMetrics::default();

        // The open reader pins an old snapshot, so PASSIVE cannot catch up. A
        // TRUNCATE here would copy the whole backlog while holding writers out
        // and then wait for the reader: the stall this checkpointer prevents.
        let behind = checkpoint_wal(&checkpointer, &path, &wal_metrics).unwrap();
        assert!(behind.frames > behind.checkpointed, "{behind:?}");
        assert!(!behind.reset);
        assert_eq!(CHECKPOINT_BUSY_CALLS.load(Ordering::Relaxed), 0);

        reader.execute_batch("COMMIT").unwrap();
        let caught_up = checkpoint_wal(&checkpointer, &path, &wal_metrics).unwrap();
        assert_eq!(caught_up.frames, caught_up.checkpointed, "{caught_up:?}");
        assert!(
            !caught_up.reset,
            "a caught-up WAL under the 64 MiB limit is reused without TRUNCATE: {caught_up:?}"
        );

        drop((reader, checkpointer, writer));
        remove_database(&path);
    }

    #[test]
    fn a_truncate_after_passive_caught_up_blocks_a_writer_past_its_busy_timeout() {
        let path = test_path("truncate-writer-stall");
        let wal_path = PathBuf::from(format!("{}-wal", path.display()));
        let lock_path = test_path("truncate-writer-stall-lock");
        remove_database(&path);
        remove_database(&lock_path);
        fs::File::create(&wal_path)
            .unwrap()
            .set_len(WAL_JOURNAL_SIZE_LIMIT_BYTES + 4096)
            .unwrap();

        // A synthetic WAL path keeps this regression test quick. The injected
        // checkpoint returns a caught-up PASSIVE result, appends frames before
        // the next decision, and models TRUNCATE's writer exclusion with a real
        // SQLite write reservation held longer than the writer's busy timeout.
        let lock_setup = Connection::open(&lock_path).unwrap();
        lock_setup
            .execute_batch(
                "PRAGMA journal_mode=WAL; CREATE TABLE t(value INTEGER); INSERT INTO t VALUES (0);",
            )
            .unwrap();
        drop(lock_setup);

        let connection = Connection::open_in_memory().unwrap();
        let metrics = WalMetrics::default();
        let kinds = Arc::new(Mutex::new(Vec::new()));
        let captured_kinds = Arc::clone(&kinds);
        let callback_wal_path = wal_path.clone();
        let callback_lock_path = lock_path.clone();
        let truncate_writer = Arc::new(Mutex::new(None));
        let captured_writer = Arc::clone(&truncate_writer);
        let busy_timeout = Duration::from_millis(60);
        let checkpoint = checkpoint_wal_with(&connection, &path, &metrics, move |_, kind| {
            captured_kinds.lock().unwrap().push(kind);
            if kind == CHECKPOINT_PASSIVE {
                fs::File::options()
                    .write(true)
                    .open(&callback_wal_path)
                    .unwrap()
                    .set_len(WAL_JOURNAL_SIZE_LIMIT_BYTES + 12_288)
                    .unwrap();
                return Ok((0, 100, 100));
            }

            assert_eq!(kind, CHECKPOINT_TRUNCATE);
            let holder = Connection::open(&callback_lock_path).unwrap();
            holder.execute_batch("BEGIN IMMEDIATE;").unwrap();
            let (started_tx, started_rx) = mpsc::channel();
            let writer_path = callback_lock_path.clone();
            let writer = std::thread::spawn(move || {
                let writer = Connection::open(writer_path).unwrap();
                writer.busy_timeout(busy_timeout).unwrap();
                let started = Instant::now();
                started_tx.send(()).unwrap();
                let error = writer
                    .execute("INSERT INTO t VALUES (1)", [])
                    .unwrap_err()
                    .to_string();
                (started.elapsed(), error)
            });
            started_rx.recv().unwrap();
            std::thread::sleep(Duration::from_millis(220));
            let observation = writer.join().unwrap();
            holder.execute_batch("COMMIT;").unwrap();
            *captured_writer.lock().unwrap() = Some(observation);
            Ok((0, 100, 100))
        })
        .unwrap();

        let blocked = truncate_writer.lock().unwrap().clone();
        assert!(
            blocked.is_none(),
            "writer was blocked by the injected TRUNCATE path: observation={blocked:?}, busy_timeout={busy_timeout:?}, checkpoint={checkpoint:?}, calls={:?}",
            *kinds.lock().unwrap()
        );
        assert_eq!(*kinds.lock().unwrap(), [CHECKPOINT_PASSIVE]);
        assert_eq!(metrics.ckpt_truncate_max_ns.load(Ordering::Relaxed), 0);

        drop(connection);
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(&wal_path);
        remove_database(&lock_path);
    }

    #[test]
    fn a_40_mib_reusable_wal_does_not_take_truncate_lock_during_a_writer() {
        let path = test_path("large-wal-concurrent-writer");
        remove_database(&path);
        let mut writer = Connection::open(&path).unwrap();
        writer
            .execute_batch(
                "PRAGMA journal_mode=WAL;
                 PRAGMA wal_autocheckpoint=0;
                 PRAGMA journal_size_limit=67108864;
                 CREATE TABLE t(k INTEGER PRIMARY KEY, v BLOB);",
            )
            .unwrap();
        {
            let transaction = writer.transaction().unwrap();
            for index in 0..10_000 {
                transaction
                    .execute("INSERT INTO t VALUES (?1, zeroblob(2048))", [index])
                    .unwrap();
            }
            transaction.commit().unwrap();
        }
        let before = wal_file_bytes(&path);
        assert!(before >= 32 * 1024 * 1024, "WAL only had {before} bytes");
        assert!(
            before <= WAL_JOURNAL_SIZE_LIMIT_BYTES,
            "reusable WAL unexpectedly exceeded its configured cap: {before} bytes"
        );

        let competing_writer = Connection::open(&path).unwrap();
        competing_writer
            .execute_batch("PRAGMA busy_timeout=5000; BEGIN IMMEDIATE;")
            .unwrap();
        competing_writer
            .execute("INSERT INTO t VALUES (10001, zeroblob(2048))", [])
            .unwrap();
        let checkpointer = Connection::open(&path).unwrap();
        checkpointer.busy_timeout(CHECKPOINT_BUSY_TIMEOUT).unwrap();
        let metrics = WalMetrics::default();

        let during_started = Instant::now();
        let during_writer = checkpoint_wal(&checkpointer, &path, &metrics).unwrap();
        let during_elapsed = during_started.elapsed();
        assert!(
            !during_writer.reset,
            "a concurrent writer reset the WAL: {during_writer:?}"
        );
        assert_eq!(metrics.ckpt_truncate_max_ns.load(Ordering::Relaxed), 0);
        competing_writer.execute_batch("COMMIT;").unwrap();
        let caught_up_started = Instant::now();
        let caught_up = checkpoint_wal(&checkpointer, &path, &metrics).unwrap();
        let caught_up_elapsed = caught_up_started.elapsed();
        assert_eq!(caught_up.frames, caught_up.checkpointed, "{caught_up:?}");
        assert_eq!(metrics.ckpt_truncate_max_ns.load(Ordering::Relaxed), 0);
        assert!(!caught_up.reset);

        eprintln!(
            "WAL repro: bytes={before} passive_during_writer_ms={} passive_after_ms={} ckpt_truncate_ms_max={}",
            during_elapsed.as_micros() as f64 / 1_000.0,
            caught_up_elapsed.as_micros() as f64 / 1_000.0,
            metrics.ckpt_truncate_max_ns.load(Ordering::Relaxed) as f64 / 1_000_000.0,
        );
        drop((competing_writer, checkpointer, writer));
        remove_database(&path);
    }

    #[test]
    fn the_writer_minute_metrics_line_carries_the_read_pool_wait_counters() {
        struct CaptureWriter(Arc<Mutex<Vec<u8>>>);
        impl std::io::Write for CaptureWriter {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        struct CaptureMakeWriter(Arc<Mutex<Vec<u8>>>);
        impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for CaptureMakeWriter {
            type Writer = CaptureWriter;
            fn make_writer(&'a self) -> Self::Writer {
                CaptureWriter(self.0.clone())
            }
        }

        let output = Arc::new(Mutex::new(Vec::new()));
        let _capture = tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .without_time()
                .with_ansi(false)
                .with_max_level(tracing::Level::INFO)
                .with_writer(CaptureMakeWriter(output.clone()))
                .finish(),
        );
        tracing::callsite::rebuild_interest_cache(); // set_default is thread-local; see tests/search_page.rs
        let read_metrics = ReadPoolMetrics::default();
        read_metrics.record_wait(Duration::from_millis(7));
        read_metrics.record_wait(Duration::from_millis(3));
        let wal_metrics = WalMetrics::default();
        wal_metrics.record_checkpoint(Path::new(":memory:"), 8_192, 3, 2, true);
        wal_metrics.record_checkpoint(Path::new(":memory:"), 8_192, 3, 2, false);
        wal_metrics.sqlite_busy.store(1, Ordering::Relaxed);
        wal_metrics.slow_txn.store(2, Ordering::Relaxed);
        wal_metrics
            .ckpt_passive_max_ns
            .store(10_000_000, Ordering::Relaxed);
        wal_metrics
            .ckpt_truncate_max_ns
            .store(250_000_000, Ordering::Relaxed);
        wal_metrics
            .wal_force_shrink_attempts
            .store(3, Ordering::Relaxed);
        wal_metrics.wal_force_shrink_ok.store(2, Ordering::Relaxed);
        wal_metrics
            .wal_force_shrink_busy
            .store(1, Ordering::Relaxed);
        wal_metrics
            .wal_force_shrink_ms_max_ns
            .store(22_500_000, Ordering::Relaxed);
        wal_metrics
            .wal_force_shrink_bytes_before
            .store(9_000_000, Ordering::Relaxed);
        wal_metrics
            .wal_force_shrink_bytes_after
            .store(65_536, Ordering::Relaxed);
        wal_metrics
            .backfill_wait_ns
            .store(1_500_000, Ordering::Relaxed);
        let metrics = SqliteWriterMetrics::default();
        metrics.sqlite_busy.store(2, Ordering::Relaxed);
        metrics.slow_txn.store(3, Ordering::Relaxed);
        metrics.jetstream_lock_retries.store(7, Ordering::Relaxed);
        metrics.report(&read_metrics, &wal_metrics);
        wal_metrics.record_checkpoint(Path::new(":memory:"), 8_192, 3, 3, false);
        metrics.report(&read_metrics, &wal_metrics);

        let text = String::from_utf8(output.lock().unwrap().clone()).unwrap();
        assert!(text.contains("SQLite writer minute metrics"), "{text}");
        assert!(text.contains("read_wait_count=2"), "{text}");
        assert!(text.contains("read_wait_max_ms=7"), "{text}");
        assert!(text.contains("web_read_timeout_count="), "{text}");
        assert!(text.contains("wal_bytes=8192"), "{text}");
        assert!(text.contains("wal_file_bytes_max=8192"), "{text}");
        assert!(text.contains("wal_frames=3"), "{text}");
        assert!(text.contains("ckpt_frames=2"), "{text}");
        assert!(text.contains("sqlite_busy=3"), "{text}");
        assert!(text.contains("slow_txn=5"), "{text}");
        assert!(text.contains("ckpt_passive_ms_max=10"), "{text}");
        assert!(text.contains("ckpt_truncate_ms_max=250"), "{text}");
        assert!(text.contains("wal_force_shrink_attempts=3"), "{text}");
        assert!(text.contains("wal_force_shrink_ok=2"), "{text}");
        assert!(text.contains("wal_force_shrink_busy=1"), "{text}");
        assert!(text.contains("wal_force_shrink_ms_max=22.5"), "{text}");
        assert!(text.contains("wal_bytes_before=9000000"), "{text}");
        assert!(text.contains("wal_bytes_after=65536"), "{text}");
        assert!(text.contains("jetstream_lock_retries=7"), "{text}");
        let lines = text.lines().collect::<Vec<_>>();
        assert_eq!(lines.len(), 2, "expected one line per report: {text}");
        assert!(lines[0].contains("wal_reset=true"), "{}", lines[0]);
        assert!(lines[0].contains("wal_resets=1"), "{}", lines[0]);
        assert!(lines[1].contains("wal_reset=false"), "{}", lines[1]);
        assert!(lines[1].contains("wal_resets=0"), "{}", lines[1]);
        assert!(text.contains("backfill_wait_ms=1.5"), "{text}");
    }

    #[test]
    fn a_reported_wal_shrink_is_counted_once_with_the_smaller_size() {
        let path = Path::new(":memory:");
        let metrics = WalMetrics::default();
        metrics.record_checkpoint(path, 8_192, 3, 3, false);
        metrics.record_checkpoint(path, 4_096, 1, 1, true);

        assert_eq!(metrics.file_bytes.load(Ordering::Acquire), 4_096);
        assert_eq!(metrics.resets.load(Ordering::Acquire), 1);
        assert!(metrics.reset.load(Ordering::Acquire));
    }

    #[test]
    fn checkpoint_pressure_gate_forces_rounds_at_three_journal_limits() {
        assert!(should_skip_checkpoint_for_pressure(
            true,
            WAL_PRESSURE_CHECKPOINT_BYTES - 1
        ));
        assert!(!should_skip_checkpoint_for_pressure(
            true,
            WAL_PRESSURE_CHECKPOINT_BYTES
        ));
        assert!(!should_skip_checkpoint_for_pressure(false, 0));
    }

    #[test]
    fn page_read_and_refresh_enqueue_finish_while_ingest_holds_the_writer() {
        let path = test_path("writer-latency");
        let storage = Arc::new(SqliteStorage::open(&path).unwrap());
        let writer_storage = Arc::clone(&storage);
        let (started_tx, started_rx) = mpsc::channel();
        let ingest = std::thread::spawn(move || {
            writer_storage
                .writer
                .call(move |connection| {
                    let transaction = connection.transaction()?;
                    transaction.execute(
                        "INSERT INTO records(did,collection,rkey,uri,time_us,record_json)
                         VALUES('did:plc:hold','app.bsky.feed.post','hold','at://did:plc:hold/app.bsky.feed.post/hold',1,'{}')",
                        [],
                    )?;
                    started_tx.send(()).unwrap();
                    std::thread::sleep(Duration::from_millis(250));
                    transaction.commit()?;
                    Ok(())
                })
                .unwrap();
        });
        started_rx.recv().unwrap();

        let request_started = Instant::now();
        assert!(
            storage
                .get_record("did:plc:hold", "app.bsky.feed.post", "hold")
                .unwrap()
                .is_none()
        );
        storage
            .queue_handle_refresh_ordered(&["did:plc:page-author".to_owned()])
            .unwrap();
        let elapsed = request_started.elapsed();

        ingest.join().unwrap();
        storage.writer.flush().unwrap();
        drop(storage);
        remove_database(&path);
        assert!(
            elapsed < Duration::from_millis(100),
            "page request waited {elapsed:?} while the writer held an ingest transaction"
        );
    }

    #[test]
    fn snapshot_waits_between_batches_while_live_ingest_is_queued() {
        let storage = Arc::new(SqliteStorage::open(":memory:").unwrap());
        let did = "did:plc:pressure-snapshot";
        let records = (0..501)
            .map(|index| IndexedRecord {
                did: did.to_owned(),
                collection: "app.bsky.feed.post".to_owned(),
                rkey: format!("r{index}"),
                uri: format!("at://{did}/app.bsky.feed.post/r{index}"),
                cid: None,
                time_us: index,
                record: serde_json::json!({"text":"snapshot"}),
            })
            .collect::<Vec<_>>();
        let snapshot_storage = Arc::clone(&storage);
        let snapshot = std::thread::spawn(move || {
            snapshot_storage
                .replace_repo_snapshot_throttled(did, &records, &[], 500)
                .unwrap();
        });

        let deadline = Instant::now() + Duration::from_secs(3);
        while storage.records().unwrap().len() < SNAPSHOT_BATCH {
            assert!(
                Instant::now() < deadline,
                "first snapshot batch did not complete"
            );
            std::thread::yield_now();
        }
        storage
            .pressure
            .waiting_live_ingest
            .store(1, Ordering::Relaxed);
        std::thread::sleep(Duration::from_millis(50));
        assert_eq!(
            storage.records().unwrap().len(),
            SNAPSHOT_BATCH,
            "snapshot opened its next write transaction while live ingest was waiting"
        );
        storage
            .pressure
            .waiting_live_ingest
            .store(0, Ordering::Relaxed);
        snapshot.join().unwrap();
        assert_eq!(storage.records().unwrap().len(), 501);
        assert!(storage.wal_metrics.backfill_wait_ns.load(Ordering::Relaxed) > 0);
    }

    #[test]
    fn live_ingest_jumps_a_queued_backfill_write() {
        let storage = Arc::new(SqliteStorage::open(":memory:").unwrap());
        storage
            .writer
            .call(|connection| {
                connection.execute_batch(
                    "CREATE TABLE writer_order(seq INTEGER PRIMARY KEY, name TEXT NOT NULL);
                     CREATE TRIGGER log_live_record AFTER INSERT ON records BEGIN
                         INSERT INTO writer_order(name) VALUES('live');
                     END;",
                )?;
                Ok(())
            })
            .unwrap();

        let held_storage = Arc::clone(&storage);
        let (held_tx, held_rx) = mpsc::channel();
        let (release_tx, release_rx) = mpsc::channel();
        let holder = std::thread::spawn(move || {
            held_storage
                .writer
                .call(move |connection| {
                    let transaction = connection
                        .transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;
                    held_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                    transaction.commit()?;
                    Ok(())
                })
                .unwrap();
        });
        held_rx.recv().unwrap();

        let backfill_storage = Arc::clone(&storage);
        let backfill = std::thread::spawn(move || {
            backfill_storage
                .writer
                .call(|connection| {
                    connection.execute("INSERT INTO writer_order(name) VALUES('backfill')", [])?;
                    Ok(())
                })
                .unwrap();
        });
        let deadline = Instant::now() + Duration::from_secs(1);
        while storage.writer.metrics.queue_depth.load(Ordering::Relaxed) < 1 {
            assert!(
                Instant::now() < deadline,
                "backfill did not queue behind the held writer"
            );
            std::thread::yield_now();
        }

        let live_storage = Arc::clone(&storage);
        let live = std::thread::spawn(move || {
            live_storage
                .apply_jetstream_batch(&[JetstreamStorageUpdate {
                    time_us: 1,
                    upsert: Some(IndexedRecord {
                        did: "did:plc:live-priority".to_owned(),
                        collection: "app.bsky.feed.post".to_owned(),
                        rkey: "post".to_owned(),
                        uri: "at://did:plc:live-priority/app.bsky.feed.post/post".to_owned(),
                        cid: None,
                        time_us: 1,
                        record: serde_json::json!({"text":"live"}),
                    }),
                    ..JetstreamStorageUpdate::default()
                }])
                .unwrap();
        });
        let deadline = Instant::now() + Duration::from_secs(1);
        while storage.writer.metrics.queue_depth.load(Ordering::Relaxed) < 2 {
            assert!(
                Instant::now() < deadline,
                "live ingest did not queue behind the held writer"
            );
            std::thread::yield_now();
        }
        release_tx.send(()).unwrap();
        holder.join().unwrap();
        backfill.join().unwrap();
        live.join().unwrap();

        let order = storage
            .writer
            .call(|connection| {
                let mut statement =
                    connection.prepare("SELECT name FROM writer_order ORDER BY seq")?;
                let order = statement
                    .query_map([], |row| row.get::<_, String>(0))?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(order)
            })
            .unwrap();
        assert_eq!(order, ["live", "backfill"]);
    }

    #[test]
    fn fire_and_forget_queue_write_is_visible_after_a_writer_barrier() {
        let storage = SqliteStorage::open(":memory:").unwrap();
        storage
            .queue_handle_refresh_ordered(&[
                "did:plc:first".to_owned(),
                "did:plc:second".to_owned(),
            ])
            .unwrap();
        storage.writer.flush().unwrap();
        assert_eq!(
            storage.pending_handle_refreshes(10).unwrap(),
            ["did:plc:first", "did:plc:second"]
        );
    }

    #[test]
    fn full_writer_queue_drops_page_writes_without_waiting_and_counts_them() {
        let storage = Arc::new(SqliteStorage::open(":memory:").unwrap());
        let writer_storage = Arc::clone(&storage);
        let (started_tx, started_rx) = mpsc::channel();
        let ingest = std::thread::spawn(move || {
            writer_storage
                .writer
                .call(move |connection| {
                    let transaction = connection.transaction()?;
                    transaction.execute(
                        "INSERT INTO appview_state(key,value) VALUES('writer-test','held')",
                        [],
                    )?;
                    started_tx.send(()).unwrap();
                    std::thread::sleep(Duration::from_millis(250));
                    transaction.commit()?;
                    Ok(())
                })
                .unwrap();
        });
        started_rx.recv().unwrap();

        let dropped_before = storage.writer.metrics.dropped.load(Ordering::Relaxed);
        for index in 0..SQLITE_WRITER_QUEUE {
            storage
                .queue_handle_refresh(&format!("did:plc:queued-{index}"))
                .unwrap();
        }
        let request_started = Instant::now();
        storage
            .queue_handle_refresh("did:plc:overflow-request")
            .unwrap();
        let elapsed = request_started.elapsed();
        let dropped_after = storage.writer.metrics.dropped.load(Ordering::Relaxed);

        ingest.join().unwrap();
        storage.writer.flush().unwrap();
        drop(storage);
        assert!(
            elapsed < Duration::from_millis(30),
            "full request queue blocked for {elapsed:?}"
        );
        assert_eq!(dropped_after, dropped_before + 1);
    }

    #[test]
    fn remembering_a_recent_profile_does_not_wait_on_the_writer() {
        // A profile page GET arrived-from-search remembers the click before
        // rendering; the page does not need the row to land. It must not stall
        // behind a held writer (same class as the last-active-tab write).
        let storage = Arc::new(SqliteStorage::open(":memory:").unwrap());
        let writer_storage = Arc::clone(&storage);
        let (started_tx, started_rx) = mpsc::channel();
        let holder = std::thread::spawn(move || {
            writer_storage
                .writer
                .call(move |connection| {
                    started_tx.send(()).unwrap();
                    std::thread::sleep(Duration::from_millis(250));
                    let _ = connection;
                    Ok(())
                })
                .unwrap();
        });
        started_rx.recv().unwrap();
        let request_started = Instant::now();
        storage
            .remember_recent_profile("did:plc:viewer", "did:plc:visited")
            .unwrap();
        let elapsed = request_started.elapsed();
        holder.join().unwrap();
        storage.writer_barrier().unwrap();
        assert!(
            elapsed < Duration::from_millis(50),
            "remembering a recent profile waited {elapsed:?} on the writer"
        );
        assert_eq!(
            storage
                .recent_profiles_for_viewer("did:plc:viewer")
                .unwrap(),
            ["did:plc:visited"],
            "the remembered profile must land once the writer drains"
        );
    }

    #[test]
    fn queued_profile_writes_are_readable_deduped_and_bounded() {
        let storage = Arc::new(SqliteStorage::open(":memory:").unwrap());
        let profile = |did: &str, name: &str, time_us: u64| IndexedRecord {
            did: did.to_owned(),
            collection: "app.bsky.actor.profile".to_owned(),
            rkey: "self".to_owned(),
            uri: format!("at://{did}/app.bsky.actor.profile/self"),
            cid: None,
            time_us,
            record: serde_json::json!({"displayName": name}),
        };
        let stored_name = |storage: &SqliteStorage, did: &str| {
            storage
                .get_record(did, "app.bsky.actor.profile", "self")
                .unwrap()
                .and_then(|record| {
                    record
                        .record
                        .get("displayName")
                        .and_then(Value::as_str)
                        .map(str::to_owned)
                })
        };
        let writer_storage = Arc::clone(&storage);
        let (started_tx, started_rx) = mpsc::channel();
        let holder = std::thread::spawn(move || {
            writer_storage
                .writer
                .call(move |connection| {
                    started_tx.send(()).unwrap();
                    std::thread::sleep(Duration::from_millis(250));
                    let _ = connection;
                    Ok(())
                })
                .unwrap();
        });
        started_rx.recv().unwrap();

        storage
            .queue_record_write(&profile("did:plc:warm", "First", 1))
            .unwrap();
        assert_eq!(
            stored_name(&storage, "did:plc:warm").as_deref(),
            Some("First"),
            "a queued profile must be readable before the writer applies it"
        );
        storage
            .queue_record_write(&profile("did:plc:warm", "Second", 2))
            .unwrap();
        assert_eq!(
            storage.pending_queued_record_writes(),
            1,
            "a second warm for the same record must update the pending content, not queue twice"
        );
        assert_eq!(
            stored_name(&storage, "did:plc:warm").as_deref(),
            Some("Second"),
            "the pending overlay must hold the latest content"
        );

        for index in 0..(2 * SQLITE_WRITER_QUEUE) {
            storage
                .queue_record_write(&profile(&format!("did:plc:many-{index}"), "bounded", 3))
                .unwrap();
        }
        assert!(
            storage.pending_queued_record_writes() <= SQLITE_WRITER_QUEUE,
            "pending profile writes exceeded the writer queue bound"
        );

        holder.join().unwrap();
        storage.writer_barrier().unwrap();
        assert_eq!(storage.pending_queued_record_writes(), 0);
        assert_eq!(
            stored_name(&storage, "did:plc:warm").as_deref(),
            Some("Second"),
            "the queued task must write the latest pending content, not the first"
        );
    }

    #[test]
    fn shutdown_flushes_queued_request_writes() {
        let path = test_path("shutdown-flush");
        let storage = SqliteStorage::open(&path).unwrap();
        storage
            .queue_handle_refresh_ordered(&["did:plc:flush-at-shutdown".to_owned()])
            .unwrap();
        drop(storage);

        let reopened = SqliteStorage::open(&path).unwrap();
        assert_eq!(
            reopened.pending_handle_refreshes(10).unwrap(),
            ["did:plc:flush-at-shutdown"]
        );
        drop(reopened);
        remove_database(&path);
    }

    #[test]
    fn shutdown_waits_for_a_delayed_queued_write_before_database_reopens() {
        let path = test_path("shutdown-waits-for-write");
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch("CREATE TABLE shutdown_write(value TEXT NOT NULL);")
            .unwrap();
        let writer = SqliteWriter::new(
            connection,
            Arc::new(ReadPoolMetrics::default()),
            Arc::new(WalMetrics::default()),
            Arc::new(StoragePressure::default()),
        )
        .unwrap();
        writer.fire(|connection| {
            std::thread::sleep(Duration::from_millis(200));
            connection.execute("INSERT INTO shutdown_write(value) VALUES('flushed')", [])?;
            Ok(None)
        });

        drop(writer);

        let reopened = Connection::open(&path).unwrap();
        let value: String = reopened
            .query_row("SELECT value FROM shutdown_write", [], |row| row.get(0))
            .unwrap();
        assert_eq!(value, "flushed");
        drop(reopened);
        remove_database(&path);
    }

    #[test]
    fn a_failing_fire_write_does_not_rollback_other_writes_in_its_batch() {
        let path = test_path("independent-batch-writes");
        let connection = Connection::open(&path).unwrap();
        connection
            .execute_batch("CREATE TABLE batch_write(value TEXT PRIMARY KEY);")
            .unwrap();
        let writer = SqliteWriter::new(
            connection,
            Arc::new(ReadPoolMetrics::default()),
            Arc::new(WalMetrics::default()),
            Arc::new(StoragePressure::default()),
        )
        .unwrap();
        writer.fire(|connection| {
            connection.execute("INSERT INTO batch_write(value) VALUES('good-before')", [])?;
            Ok(None)
        });
        writer.fire(|connection| {
            connection.execute("INSERT INTO batch_write(value) VALUES('bad')", [])?;
            connection.execute("INSERT INTO batch_write(value) VALUES('bad')", [])?;
            Ok(None)
        });
        writer.fire(|connection| {
            connection.execute("INSERT INTO batch_write(value) VALUES('good-after')", [])?;
            Ok(None)
        });
        writer.flush().unwrap();
        assert_eq!(writer.metrics.failed_writes.load(Ordering::Relaxed), 1);

        let values = writer
            .call(|connection| {
                let mut statement =
                    connection.prepare("SELECT value FROM batch_write ORDER BY value")?;
                let values = statement
                    .query_map([], |row| row.get::<_, String>(0))?
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(values)
            })
            .unwrap();
        assert_eq!(values, ["good-after", "good-before"]);
        drop(writer);
        remove_database(&path);
    }

    #[test]
    fn ordered_handle_refresh_only_touches_the_requested_dids() {
        let storage = SqliteStorage::open(":memory:").unwrap();
        storage
            .writer
            .call(|connection| {
                connection.execute_batch(
                    "CREATE TABLE handle_refresh_touches(kind TEXT NOT NULL);
                     CREATE TRIGGER handle_refresh_insert_touch AFTER INSERT ON handle_refresh_queue
                     BEGIN INSERT INTO handle_refresh_touches(kind) VALUES('insert'); END;
                     CREATE TRIGGER handle_refresh_update_touch AFTER UPDATE ON handle_refresh_queue
                     BEGIN INSERT INTO handle_refresh_touches(kind) VALUES('update'); END;
                     CREATE TRIGGER handle_refresh_delete_touch AFTER DELETE ON handle_refresh_queue
                     BEGIN INSERT INTO handle_refresh_touches(kind) VALUES('delete'); END;",
                )?;
                for index in 0..2_000 {
                    connection.execute(
                        "INSERT INTO handle_refresh_queue(did,priority) VALUES(?1,?2)",
                        params![format!("did:plc:old-{index}"), index],
                    )?;
                }
                connection.execute("DELETE FROM handle_refresh_touches", [])?;
                Ok(())
            })
            .unwrap();

        storage
            .queue_handle_refresh_ordered(&["did:plc:new-page-author".to_owned()])
            .unwrap();
        storage.writer.flush().unwrap();
        let touched: i64 = storage
            .writer
            .call(|connection| {
                Ok(connection.query_row(
                    "SELECT COUNT(*) FROM handle_refresh_touches",
                    [],
                    |row| row.get(0),
                )?)
            })
            .unwrap();
        assert_eq!(touched, 1, "one page DID should touch one queue row");
    }
}

#[cfg(test)]
mod recent_profile_storage_tests {
    use super::*;

    fn expected_recent_profiles() -> Vec<String> {
        std::iter::once("did:plc:profile-5".to_owned())
            .chain(
                (2..=11)
                    .rev()
                    .filter(|index| *index != 5)
                    .map(|index| format!("did:plc:profile-{index}")),
            )
            .collect()
    }

    #[test]
    fn recent_profiles_are_unique_capped_ordered_and_viewer_scoped() {
        let memory = MemoryStorage::default();
        check_recent_profile_storage(&memory, &|_| ());
        let sqlite = SqliteStorage::open(":memory:").unwrap();
        check_recent_profile_storage(&sqlite, &|storage: &SqliteStorage| {
            // The remember write is fire-and-forget; let the writer land it
            // before asserting on the read side.
            storage.writer_barrier().unwrap()
        });
    }

    fn check_recent_profile_storage<S: Storage>(storage: &S, settle: &dyn Fn(&S)) {
        {
            for index in 0..12 {
                storage
                    .remember_recent_profile(
                        "did:plc:viewer-one",
                        &format!("did:plc:profile-{index}"),
                    )
                    .unwrap();
            }
            storage
                .remember_recent_profile("did:plc:viewer-one", "did:plc:profile-5")
                .unwrap();
            storage
                .remember_recent_profile("did:plc:viewer-two", "did:plc:other")
                .unwrap();
            settle(storage);
            assert_eq!(
                storage
                    .recent_profiles_for_viewer("did:plc:viewer-one")
                    .unwrap(),
                expected_recent_profiles()
            );
            assert_eq!(
                storage
                    .recent_profiles_for_viewer("did:plc:viewer-two")
                    .unwrap(),
                ["did:plc:other"]
            );
            storage
                .clear_recent_profiles_for_viewer("did:plc:viewer-one")
                .unwrap();
            assert!(
                storage
                    .recent_profiles_for_viewer("did:plc:viewer-one")
                    .unwrap()
                    .is_empty()
            );
            assert_eq!(
                storage
                    .recent_profiles_for_viewer("did:plc:viewer-two")
                    .unwrap(),
                ["did:plc:other"]
            );
        }
    }

    #[test]
    fn sqlite_recent_profiles_survive_reopening_the_database() {
        let path = std::env::temp_dir().join(format!(
            "appview-recent-profiles-{}-{}.sqlite3",
            std::process::id(),
            epoch_millis()
        ));
        {
            let storage = SqliteStorage::open(&path).unwrap();
            storage
                .remember_recent_profile("did:plc:viewer", "did:plc:profile")
                .unwrap();
        }
        {
            let storage = SqliteStorage::open(&path).unwrap();
            assert_eq!(
                storage
                    .recent_profiles_for_viewer("did:plc:viewer")
                    .unwrap(),
                ["did:plc:profile"]
            );
        }
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(format!("{}-wal", path.display()));
        let _ = fs::remove_file(format!("{}-shm", path.display()));
    }
}

#[cfg(test)]
mod remembered_feed_tab_storage_tests {
    use super::*;

    #[test]
    fn remembered_feed_tab_is_viewer_scoped_and_persistent() {
        let memory = MemoryStorage::default();
        memory
            .set_last_active_feed_tab("did:plc:one", "following")
            .unwrap();
        memory
            .set_last_active_feed_tab("did:plc:two", "at://did:plc:feed/app.bsky.feed.generator/x")
            .unwrap();
        assert_eq!(
            memory.last_active_feed_tab("did:plc:one").unwrap(),
            Some("following".to_owned())
        );
        assert_eq!(
            memory.last_active_feed_tab("did:plc:two").unwrap(),
            Some("at://did:plc:feed/app.bsky.feed.generator/x".to_owned())
        );

        let path = std::env::temp_dir().join(format!(
            "appview-remembered-feed-{}-{}.sqlite3",
            std::process::id(),
            epoch_millis()
        ));
        {
            let sqlite = SqliteStorage::open(&path).unwrap();
            sqlite
                .set_last_active_feed_tab(
                    "did:plc:one",
                    "at://did:plc:feed/app.bsky.feed.generator/x",
                )
                .unwrap();
        }
        {
            let sqlite = SqliteStorage::open(&path).unwrap();
            assert_eq!(
                sqlite.last_active_feed_tab("did:plc:one").unwrap(),
                Some("at://did:plc:feed/app.bsky.feed.generator/x".to_owned())
            );
            sqlite
                .set_last_active_feed_tab("did:plc:one", "following")
                .unwrap();
            // The tab write is fire-and-forget; let the writer catch up first.
            sqlite.writer_barrier().unwrap();
            assert_eq!(
                sqlite.last_active_feed_tab("did:plc:one").unwrap(),
                Some("following".to_owned())
            );
        }
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(format!("{}-wal", path.display()));
        let _ = fs::remove_file(format!("{}-shm", path.display()));
    }
}

#[cfg(test)]
mod recent_posts_storage_tests {
    use super::*;

    fn fill_queue(storage: &dyn Storage, count: usize) {
        for index in 0..count {
            assert_eq!(
                storage
                    .enqueue_recent_posts_fetch(&format!("did:plc:fill{index}"))
                    .unwrap(),
                RecentPostsEnqueue::Queued
            );
        }
    }

    fn check_bounds(storage: &dyn Storage) {
        let first = "did:plc:recent-posts-first";
        assert_eq!(
            storage.enqueue_recent_posts_fetch(first).unwrap(),
            RecentPostsEnqueue::Queued,
            "the first enqueue queues a job"
        );
        assert_eq!(
            storage.enqueue_recent_posts_fetch(first).unwrap(),
            RecentPostsEnqueue::Duplicate,
            "the queue deduplicates per account"
        );
        fill_queue(storage, MAX_RECENT_POSTS_QUEUE - 1);
        let overflow = "did:plc:recent-posts-overflow";
        assert_eq!(
            storage.enqueue_recent_posts_fetch(overflow).unwrap(),
            RecentPostsEnqueue::Overflowed,
            "the queue is bounded at {MAX_RECENT_POSTS_QUEUE} jobs"
        );
        let pending = storage
            .pending_recent_posts_fetches(MAX_RECENT_POSTS_QUEUE + 10)
            .unwrap();
        assert_eq!(
            pending.len(),
            MAX_RECENT_POSTS_QUEUE,
            "the bound holds the queue"
        );
        assert!(
            !pending.iter().any(|did| did == overflow),
            "overflow is dropped, not blocked"
        );
        let (queued, _done, _failed, dropped) = storage.recent_posts_counters();
        assert_eq!(queued, MAX_RECENT_POSTS_QUEUE as u64);
        assert_eq!(dropped, 1, "the dropped overflow is counted");
        storage.complete_recent_posts_fetch(first).unwrap();
        assert_eq!(
            storage.enqueue_recent_posts_fetch(overflow).unwrap(),
            RecentPostsEnqueue::Queued,
            "completing a job frees queue space"
        );
    }

    #[test]
    fn recent_posts_queue_is_deduplicated_bounded_and_counted() {
        check_bounds(&MemoryStorage::default());
    }

    #[test]
    fn recent_posts_queue_bounds_hold_for_sqlite_storage_without_waiting_on_the_writer() {
        let storage = SqliteStorage::open(":memory:").unwrap();
        check_bounds(&storage);
    }

    #[test]
    fn recent_posts_status_records_fetches_and_failure_backoff() {
        let storage = MemoryStorage::default();
        let did = "did:plc:recent-posts-status";
        assert_eq!(storage.recent_posts_status(did).unwrap(), None);
        assert_eq!(
            storage.enqueue_recent_posts_fetch(did).unwrap(),
            RecentPostsEnqueue::Queued
        );
        assert_eq!(
            storage.recent_posts_status(did).unwrap(),
            Some(RecentPostsFetchStatus {
                fetched_at_ms: 0,
                next_retry_at_ms: 0,
                queued: true,
            })
        );
        storage.record_recent_posts_failure(did, 5_000).unwrap();
        assert_eq!(
            storage.recent_posts_status(did).unwrap(),
            Some(RecentPostsFetchStatus {
                fetched_at_ms: 0,
                next_retry_at_ms: 5_000,
                queued: true,
            })
        );
        storage.complete_recent_posts_fetch(did).unwrap();
        storage.record_recent_posts_fetched(did, 9_000).unwrap();
        assert_eq!(
            storage.recent_posts_status(did).unwrap(),
            Some(RecentPostsFetchStatus {
                fetched_at_ms: 9_000,
                next_retry_at_ms: 0,
                queued: false,
            }),
            "a successful fetch clears the backoff"
        );
    }

    #[test]
    fn the_writer_minute_metrics_line_carries_the_recent_posts_counts() {
        struct Capture(Arc<Mutex<Vec<u8>>>);
        struct CaptureWriter(Arc<Mutex<Vec<u8>>>);
        impl std::io::Write for CaptureWriter {
            fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(bytes);
                Ok(bytes.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        impl<'a> tracing_subscriber::fmt::MakeWriter<'a> for Capture {
            type Writer = CaptureWriter;
            fn make_writer(&'a self) -> Self::Writer {
                CaptureWriter(Arc::clone(&self.0))
            }
        }
        let output = Arc::new(Mutex::new(Vec::new()));
        let metrics = SqliteWriterMetrics::default();
        let counters = Arc::new(RecentPostsCounters::default());
        counters.queued.store(7, Ordering::Relaxed);
        counters.done.store(4, Ordering::Relaxed);
        counters.failed.store(2, Ordering::Relaxed);
        counters.dropped.store(1, Ordering::Relaxed);
        *metrics.recent_posts.lock().unwrap() = counters;
        let _guard = tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .without_time()
                .with_ansi(false)
                .with_max_level(tracing::Level::INFO)
                .with_writer(Capture(Arc::clone(&output)))
                .finish(),
        );
        tracing::callsite::rebuild_interest_cache(); // set_default is thread-local; see tests/search_page.rs
        metrics.report(&ReadPoolMetrics::default(), &WalMetrics::default());
        drop(_guard);
        let text = String::from_utf8(output.lock().unwrap().clone()).unwrap();
        assert!(text.contains("SQLite writer minute metrics"), "{text}");
        assert!(text.contains("recent_posts_queued=7"), "{text}");
        assert!(text.contains("recent_posts_done=4"), "{text}");
        assert!(text.contains("recent_posts_failed=2"), "{text}");
        assert!(text.contains("recent_posts_dropped=1"), "{text}");
        assert!(text.contains("outbound_host_calls="), "{text}");
    }

    #[test]
    fn recent_posts_status_and_queue_survive_a_restart() {
        let path = std::env::temp_dir().join(format!(
            "appview-rs-recent-posts-{}-{}.sqlite3",
            std::process::id(),
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let did = "did:plc:recent-posts-restart";
        {
            let storage = SqliteStorage::open(&path).unwrap();
            assert_eq!(
                storage.enqueue_recent_posts_fetch(did).unwrap(),
                RecentPostsEnqueue::Queued
            );
            storage.record_recent_posts_fetched(did, 1_000).unwrap();
            storage.writer_barrier().unwrap();
        }
        let reopened = SqliteStorage::open(&path).unwrap();
        let status = reopened.recent_posts_status(did).unwrap();
        assert_eq!(
            status.map(|status| (status.fetched_at_ms, status.next_retry_at_ms, status.queued)),
            Some((1_000, 0, true)),
            "the queue and the fetch timestamp reload from the database at open"
        );
        assert_eq!(
            reopened.pending_recent_posts_fetches(10).unwrap(),
            vec![did.to_owned()]
        );
        drop(reopened);
        let _ = fs::remove_file(&path);
        let _ = fs::remove_file(format!("{}-wal", path.display()));
        let _ = fs::remove_file(format!("{}-shm", path.display()));
    }
}
