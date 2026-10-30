use futures_util::StreamExt;
use rusqlite::{Connection, OptionalExtension, params, params_from_iter};
use serde::{Deserialize, Serialize};
use serde_json::Value;
#[cfg(test)]
use std::sync::atomic::AtomicUsize;
use std::{
    cmp::Reverse,
    collections::{HashMap, HashSet, VecDeque},
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, RwLock,
        atomic::{AtomicBool, AtomicU64, Ordering},
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
const SQLITE_WRITER_QUEUE: usize = 256;
const SQLITE_WRITER_BATCH_SIZE: usize = 64;
const SQLITE_WRITER_BATCH_DELAY: Duration = Duration::from_millis(5);
const SQLITE_WRITER_METRICS_INTERVAL: Duration = Duration::from_secs(60);
static NEXT_MEMORY_DATABASE_ID: AtomicU64 = AtomicU64::new(1);

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

#[derive(Clone, Debug, PartialEq)]
pub struct IndexedNotification {
    pub record: IndexedRecord,
    pub reason: String,
    pub reason_subject: Option<String>,
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
    fn put_record(&self, record: &IndexedRecord) -> Result<(), StorageError>;
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
        let _ = records_per_second;
        self.replace_repo_snapshot(did, records, following)
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
    fn profile_index_snapshot(&self) -> Result<HashMap<String, ProfileIndexEntry>, StorageError>;
    fn set_profile_index_entry(
        &self,
        did: &str,
        entry: ProfileIndexEntry,
    ) -> Result<(), StorageError>;
    fn enqueue_profile_fetches(&self, dids: &[String]) -> Result<(), StorageError>;
    fn pending_profile_fetches(&self, limit: usize) -> Result<Vec<String>, StorageError>;
    fn complete_profile_fetch(&self, did: &str) -> Result<(), StorageError>;
    fn notifications_for(
        &self,
        did: &str,
        before: Option<&NotificationCursor>,
        limit: usize,
    ) -> Result<Vec<IndexedNotification>, StorageError>;
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
    fn post_interaction_summary(
        &self,
        uri: &str,
        viewer_did: Option<&str>,
    ) -> Result<PostInteractionSummary, StorageError>;
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
    #[cfg(test)]
    record_uri_single_lookups: AtomicUsize,
    #[cfg(test)]
    record_uri_batch_lookups: AtomicUsize,
}

#[cfg(test)]
impl MemoryStorage {
    pub(crate) fn record_uri_lookup_counts(&self) -> (usize, usize) {
        (
            self.record_uri_single_lookups.load(Ordering::Relaxed),
            self.record_uri_batch_lookups.load(Ordering::Relaxed),
        )
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
    profile_fetch_queue: VecDeque<String>,
    notification_last_seen: HashMap<String, u64>,
}

struct BackfillRetry {
    failures: u32,
    retry_at: Instant,
}

impl Storage for MemoryStorage {
    fn put_record(&self, record: &IndexedRecord) -> Result<(), StorageError> {
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
        inner
            .records
            .retain(|(record_did, _, _), _| record_did != did);
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
        self.record_uri_batch_lookups
            .fetch_add(1, Ordering::Relaxed);
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
            let subjects = inner
                .follows
                .iter()
                .filter(|(actor, _)| actor == did)
                .map(|(_, subject)| subject.clone())
                .collect::<Vec<_>>();
            for subject in subjects {
                inner.tracked.apply_follow_delta(&subject, 1);
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
        inner.follows.retain(|(actor, _)| actor != did);
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

type FireCompletion = Box<dyn FnOnce() + Send>;
type FireWrite =
    Box<dyn FnOnce(&Connection) -> Result<Option<FireCompletion>, StorageError> + Send>;
type BlockingWrite = Box<dyn FnOnce(&mut Connection) + Send>;

enum SqliteWriterMessage {
    Fire(FireWrite),
    Blocking(BlockingWrite),
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

    fn record_batch(&self, elapsed: Duration) {
        let elapsed_ns = elapsed.as_nanos().min(u128::from(u64::MAX)) as u64;
        self.batch_count.fetch_add(1, Ordering::Relaxed);
        self.transaction_count.fetch_add(1, Ordering::Relaxed);
        self.transaction_total_ns
            .fetch_add(elapsed_ns, Ordering::Relaxed);
        self.transaction_max_ns
            .fetch_max(elapsed_ns, Ordering::Relaxed);
    }

    fn record_failed_write(&self) {
        self.failed_writes.fetch_add(1, Ordering::Relaxed);
    }

    fn report(&self) {
        let count = self.transaction_count.swap(0, Ordering::Relaxed);
        let total_ns = self.transaction_total_ns.swap(0, Ordering::Relaxed);
        let max_ns = self.transaction_max_ns.swap(0, Ordering::Relaxed);
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
            dropped_count = self.dropped.swap(0, Ordering::Relaxed),
            failed_write_count = self.failed_writes.swap(0, Ordering::Relaxed),
            "SQLite writer minute metrics"
        );
    }
}

struct SqliteWriter {
    sender: Option<SyncSender<SqliteWriterMessage>>,
    worker: Option<std::thread::JoinHandle<()>>,
    metrics: Arc<SqliteWriterMetrics>,
}

impl SqliteWriter {
    fn new(connection: Connection) -> Result<Self, StorageError> {
        let (sender, receiver) = mpsc::sync_channel(SQLITE_WRITER_QUEUE);
        let metrics = Arc::new(SqliteWriterMetrics::default());
        let thread_metrics = Arc::clone(&metrics);
        let worker = std::thread::Builder::new()
            .name("sqlite-writer".to_owned())
            .spawn(move || sqlite_writer_loop(connection, receiver, thread_metrics))?;
        Ok(Self {
            sender: Some(sender),
            worker: Some(worker),
            metrics,
        })
    }

    fn call<R: Send + 'static>(
        &self,
        operation: impl FnOnce(&mut Connection) -> Result<R, StorageError> + Send + 'static,
    ) -> Result<R, StorageError> {
        let (reply, result) = mpsc::sync_channel(1);
        let command = SqliteWriterMessage::Blocking(Box::new(move |connection| {
            let _ = reply.send(operation(connection));
        }));
        self.metrics.queued();
        let Some(sender) = self.sender.as_ref() else {
            self.metrics.dequeued();
            return Err(StorageError::WriterStopped);
        };
        if sender.send(command).is_err() {
            self.metrics.dequeued();
            return Err(StorageError::WriterStopped);
        }
        result.recv().map_err(|_| StorageError::WriterStopped)?
    }

    fn fire(
        &self,
        operation: impl FnOnce(&Connection) -> Result<Option<FireCompletion>, StorageError>
        + Send
        + 'static,
    ) {
        let Some(sender) = self.sender.as_ref() else {
            self.metrics.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        };
        self.metrics.queued();
        if let Err(error) = sender.try_send(SqliteWriterMessage::Fire(Box::new(operation))) {
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

fn sqlite_writer_loop(
    mut connection: Connection,
    receiver: Receiver<SqliteWriterMessage>,
    metrics: Arc<SqliteWriterMetrics>,
) {
    let mut next_metrics = Instant::now() + SQLITE_WRITER_METRICS_INTERVAL;
    let mut pending = None;
    loop {
        let message = if let Some(message) = pending.take() {
            Some(message)
        } else {
            match receiver.recv_timeout(next_metrics.saturating_duration_since(Instant::now())) {
                Ok(message) => Some(message),
                Err(RecvTimeoutError::Timeout) => {
                    metrics.report();
                    next_metrics = Instant::now() + SQLITE_WRITER_METRICS_INTERVAL;
                    continue;
                }
                Err(RecvTimeoutError::Disconnected) => None,
            }
        };
        let Some(message) = message else {
            break;
        };
        metrics.dequeued();
        match message {
            SqliteWriterMessage::Blocking(operation) => {
                let started = Instant::now();
                operation(&mut connection);
                metrics.record_batch(started.elapsed());
            }
            SqliteWriterMessage::Fire(first) => {
                let mut writes = vec![first];
                let deadline = Instant::now() + SQLITE_WRITER_BATCH_DELAY;
                while writes.len() < SQLITE_WRITER_BATCH_SIZE {
                    match receiver.try_recv() {
                        Ok(SqliteWriterMessage::Fire(write)) => {
                            metrics.dequeued();
                            writes.push(write);
                        }
                        Ok(blocking @ SqliteWriterMessage::Blocking(_)) => {
                            pending = Some(blocking);
                            break;
                        }
                        Err(TryRecvError::Empty) if Instant::now() < deadline => {
                            std::thread::sleep(Duration::from_micros(100));
                        }
                        Err(TryRecvError::Empty | TryRecvError::Disconnected) => break,
                    }
                }
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
                            tracing::warn!(%error, "SQLite queued write batch commit failed");
                            batch_error = Some(StorageError::Sqlite(error));
                        }
                    }
                    Err(error) => {
                        tracing::warn!(%error, "SQLite queued write batch could not start");
                        batch_error = Some(StorageError::Sqlite(error));
                    }
                }
                if batch_error.is_none() {
                    for completion in completions {
                        completion();
                    }
                } else if let Some(error) = batch_error {
                    tracing::warn!(%error, "SQLite queued write batch rolled back");
                }
                metrics.record_batch(started.elapsed());
            }
        }
        if Instant::now() >= next_metrics {
            metrics.report();
            next_metrics = Instant::now() + SQLITE_WRITER_METRICS_INTERVAL;
        }
    }
}

pub struct SqliteStorage {
    writer: SqliteWriter,
    read_connections: Vec<Mutex<Connection>>,
    next_read_connection: AtomicU64,
    path: PathBuf,
    checkpointer_stop: Arc<AtomicBool>,
    checkpoint_backlog: Arc<AtomicU64>,
    tracked: Arc<RwLock<TrackedSet>>,
    profile_index: Arc<RwLock<HashMap<String, ProfileIndexEntry>>>,
    notification_hub: NotificationHub,
    _ephemeral_database: Option<EphemeralDatabase>,
}

impl Drop for SqliteStorage {
    fn drop(&mut self) {
        self.checkpointer_stop.store(true, Ordering::Relaxed);
        self.writer.shutdown();
    }
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

const CHECKPOINT_INTERVAL: Duration = Duration::from_secs(2);

fn spawn_checkpointer(
    path: PathBuf,
    stop: Arc<AtomicBool>,
    backlog: Arc<AtomicU64>,
) -> Result<(), StorageError> {
    let connection = Connection::open(&path)?;
    connection.execute_batch("PRAGMA busy_timeout = 5000;")?;
    std::thread::Builder::new()
        .name("sqlite-checkpoint".to_owned())
        .spawn(move || {
            while !stop.load(Ordering::Relaxed) {
                std::thread::sleep(CHECKPOINT_INTERVAL);
                // Unknown counts as behind: a checkpoint of a big WAL runs for minutes,
                // and the last small number would let the backfill pile on meanwhile.
                backlog.store(u64::MAX, Ordering::Relaxed);
                // Row: (busy, frames in the WAL, frames checkpointed).
                match connection.query_row("PRAGMA wal_checkpoint(PASSIVE)", [], |row| {
                    Ok((row.get::<_, i64>(1)?, row.get::<_, i64>(2)?))
                }) {
                    Ok((log, done)) => {
                        backlog.store(log.saturating_sub(done).max(0) as u64, Ordering::Relaxed)
                    }
                    Err(error) => tracing::warn!(%error, "WAL checkpoint failed"),
                }
            }
        })?;
    Ok(())
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
             CREATE INDEX IF NOT EXISTS records_collection_time ON records(collection, time_us DESC);
             CREATE INDEX IF NOT EXISTS records_collection_uri ON records(collection, uri);
             CREATE INDEX IF NOT EXISTS records_author_time ON records(collection, did, time_us DESC, uri DESC);
             CREATE INDEX IF NOT EXISTS records_author_reply_time
                 ON records(collection, did, json_type(record_json, '$.reply'), time_us DESC, uri DESC);
             CREATE INDEX IF NOT EXISTS records_follow_actor_subject
                 ON records(collection, did, json_extract(record_json, '$.subject'));
             CREATE INDEX IF NOT EXISTS records_follow_subject_actor
                 ON records(collection, json_extract(record_json, '$.subject'), did, time_us DESC);
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
             );",
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
        connection.execute(
            "DELETE FROM backfill_queue
             WHERE did NOT IN (
                 SELECT did FROM backfill_queue ORDER BY enqueued_at,did LIMIT ?1
             )",
            [MAX_BACKFILL_QUEUE],
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
        let mut read_connections = Vec::new();
        for _ in 0..SQLITE_READ_CONNECTIONS {
            let reader = Connection::open(&database_path)?;
            reader.execute_batch("PRAGMA busy_timeout = 5000; PRAGMA query_only = ON;")?;
            read_connections.push(Mutex::new(reader));
        }
        let stop = Arc::new(AtomicBool::new(false));
        let backlog = Arc::new(AtomicU64::new(0));
        if !is_memory {
            backlog.store(u64::MAX, Ordering::Relaxed);
            spawn_checkpointer(
                database_path.clone(),
                Arc::clone(&stop),
                Arc::clone(&backlog),
            )?;
        }
        Ok(Self {
            writer: SqliteWriter::new(connection)?,
            read_connections,
            next_read_connection: AtomicU64::new(0),
            path,
            checkpointer_stop: stop,
            checkpoint_backlog: backlog,
            tracked: Arc::new(RwLock::new(tracked_set)),
            profile_index: Arc::new(RwLock::new(profile_index)),
            notification_hub: NotificationHub::default(),
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

    fn read_conn(&self) -> Result<std::sync::MutexGuard<'_, Connection>, StorageError> {
        if self.read_connections.is_empty() {
            return Err(StorageError::WriterStopped);
        }
        let index = self.next_read_connection.fetch_add(1, Ordering::Relaxed) as usize
            % self.read_connections.len();
        self.read_connections[index]
            .lock()
            .map_err(|_| StorageError::Poisoned)
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

    fn delete_snapshot_rows(
        &self,
        did: &str,
        cutoff_us: i64,
        sql: &str,
        records_per_second: Option<u64>,
        write_started: Instant,
        records_written: &mut u64,
    ) -> Result<(), StorageError> {
        loop {
            let writer_did = did.to_owned();
            let writer_sql = sql.to_owned();
            let deleted = self.writer.call(move |connection| {
                let transaction = connection.transaction()?;
                let deleted = transaction.execute(
                    &writer_sql,
                    params![writer_did, cutoff_us, SNAPSHOT_BATCH as i64],
                )?;
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
        self.delete_snapshot_rows(
            did,
            cutoff_us,
            "DELETE FROM records WHERE rowid IN (
                 SELECT rowid FROM records WHERE did=?1 AND time_us<=?2 ORDER BY rowid LIMIT ?3
             )",
            records_per_second,
            write_started,
            &mut records_written,
        )?;
        self.delete_snapshot_rows(
            did,
            cutoff_us,
            "DELETE FROM notification_events WHERE rowid IN (
                 SELECT rowid FROM notification_events WHERE source_did=?1 AND time_us<=?2 ORDER BY rowid LIMIT ?3
             )",
            records_per_second,
            write_started,
            &mut records_written,
        )?;
        self.delete_snapshot_follows(
            did,
            follow_rowid_cutoff,
            records_per_second,
            write_started,
            &mut records_written,
        )?;
        self.delete_snapshot_rows(
            did,
            scope_rowid_cutoff,
            "DELETE FROM backfill_scope WHERE rowid IN (
                 SELECT rowid FROM backfill_scope
                 WHERE owner_did=?1 AND target_did!=?1 AND rowid<=?2
                 ORDER BY rowid LIMIT ?3
             )",
            records_per_second,
            write_started,
            &mut records_written,
        )?;
        let follow_batches = following.chunks(SNAPSHOT_BATCH).collect::<Vec<_>>();
        for (batch_index, batch) in follow_batches.iter().enumerate() {
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
                        let mut insert_follow = transaction.prepare(
                            "INSERT OR IGNORE INTO follows(actor,subject,following) VALUES(?1,?2,1)",
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
                            let inserted = insert_follow.execute(params![writer_did, subject])?;
                            inserted_follow_rows =
                                inserted_follow_rows.saturating_add(inserted as u64);
                            if inserted > 0 {
                                inserted_subjects.push(subject.clone());
                            }
                            let is_following: bool = transaction.query_row(
                                "SELECT following FROM follows WHERE actor=?1 AND subject=?2",
                                params![writer_did, subject],
                                |row| row.get(0),
                            )?;
                            if registered_for_tracker && is_following {
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

        let total_batches = records.len().div_ceil(SNAPSHOT_BATCH);
        for (batch_index, (batch, batch_json)) in records
            .chunks(SNAPSHOT_BATCH)
            .zip(record_json.chunks(SNAPSHOT_BATCH))
            .enumerate()
        {
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
    fn write_backlog(&self) -> u64 {
        self.checkpoint_backlog.load(Ordering::Relaxed)
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

    fn apply_jetstream_batch(
        &self,
        updates: &[JetstreamStorageUpdate],
    ) -> Result<BatchApplyTiming, StorageError> {
        if updates.is_empty() {
            return Ok(BatchApplyTiming::default());
        }
        let writer_updates = updates.to_vec();
        let (timing, tracked_follow_deltas, affected) = self.writer.call(move |connection| {
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
        self.replace_repo_snapshot_inner(did, records, following, None)?;
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
        self.replace_repo_snapshot_inner(did, records, following, Some(records_per_second))?;
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
        let conn = self.read_conn()?;
        let mut statement = conn.prepare(
            "SELECT did, collection, rkey, uri, cid, time_us, record_json FROM records
             WHERE collection='app.bsky.graph.follow' AND json_extract(record_json, '$.subject')=?1
             ORDER BY time_us DESC, uri DESC LIMIT ?2",
        )?;
        statement
            .query_map(params![did, limit.clamp(1, 101) as i64], row_to_record)?
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into)
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
        let conn = self.read_conn()?;
        let mut statement = conn.prepare(
            "SELECT did, collection, rkey, uri, cid, time_us, record_json FROM records
             WHERE collection='app.bsky.feed.post'
               AND (did=?1 OR did IN (
                   SELECT json_extract(record_json, '$.subject') FROM records
                   WHERE collection='app.bsky.graph.follow' AND did=?1
               ))
               AND (?2 IS NULL OR time_us < ?2 OR (time_us=?2 AND uri < ?3))
             ORDER BY time_us DESC, uri DESC LIMIT ?4",
        )?;
        statement
            .query_map(
                params![
                    actor,
                    cursor.map(|cursor| cursor.time_us.min(i64::MAX as u64) as i64),
                    cursor.map(|cursor| cursor.uri.as_str()).unwrap_or(""),
                    limit.clamp(1, 101) as i64,
                ],
                row_to_record,
            )?
            .collect::<Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    fn post_interaction_summary(
        &self,
        uri: &str,
        viewer_did: Option<&str>,
    ) -> Result<PostInteractionSummary, StorageError> {
        let conn = self.read_conn()?;
        let count = |collection: &str, path: &str| -> Result<usize, rusqlite::Error> {
            conn.query_row(
                &format!("SELECT COUNT(*) FROM records WHERE collection=?1 AND json_extract(record_json, '{path}')=?2"),
                params![collection, uri],
                |row| row.get(0),
            )
        };
        let mut summary = PostInteractionSummary {
            like_count: count("app.bsky.feed.like", "$.subject.uri")?,
            repost_count: count("app.bsky.feed.repost", "$.subject.uri")?,
            reply_count: count("app.bsky.feed.post", "$.reply.parent.uri")?,
            quote_count: count("app.bsky.feed.post", "$.embed.record.uri")?,
            ..PostInteractionSummary::default()
        };
        if let Some(viewer_did) = viewer_did {
            summary.viewer_like = conn.query_row(
                "SELECT uri FROM records WHERE collection='app.bsky.feed.like' AND did=?1 AND json_extract(record_json, '$.subject.uri')=?2 ORDER BY time_us DESC LIMIT 1",
                params![viewer_did, uri],
                |row| row.get(0),
            ).optional()?;
            summary.viewer_repost = conn.query_row(
                "SELECT uri FROM records WHERE collection='app.bsky.feed.repost' AND did=?1 AND json_extract(record_json, '$.subject.uri')=?2 ORDER BY time_us DESC LIMIT 1",
                params![viewer_did, uri],
                |row| row.get(0),
            ).optional()?;
        }
        Ok(summary)
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
            transaction.execute("DELETE FROM follows WHERE actor=?1", [&writer_did])?;
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
        self.writer.call(move |connection| {
            connection.execute(
                "DELETE FROM web_sessions WHERE did=?1 AND session_id=?2",
                params![did, session_id],
            )?;
            Ok(())
        })
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

pub struct Ingestor<S: Storage> {
    storage: Arc<S>,
    identity_cache: Option<Arc<crate::identity::SqliteIdentityCache>>,
    scope: ScopeMode,
    cursor: AtomicU64,
    processed: AtomicU64,
    batch_metrics: Mutex<IngestIntervalMetrics>,
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
        }
    }

    pub fn with_identity_cache(
        mut self,
        identity_cache: Arc<crate::identity::SqliteIdentityCache>,
    ) -> Self {
        self.identity_cache = Some(identity_cache);
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
        let ingestor = self.ingestor.clone();
        let task = tokio::task::spawn_blocking(move || ingestor.process_json_batch(&events));
        match tokio::time::timeout(self.event_timeout, task).await {
            Err(_) => {
                tracing::warn!(
                    %did,
                    %collection,
                    %rkey,
                    timeout_seconds = self.event_timeout.as_secs_f64(),
                    "Jetstream event processing timed out"
                );
                Err(IngestError::WebSocket(
                    "event processing timed out".to_owned(),
                ))
            }
            Ok(Err(error)) => Err(IngestError::WebSocket(format!(
                "Jetstream event processing task failed: {error}"
            ))),
            Ok(Ok(result)) => result,
        }
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
            let base_url = &urls[next_url % urls.len()];
            next_url += 1;
            let ingestor = self.ingestor.clone();
            let stale_threshold = self.stale_cursor_threshold;
            let base_url = base_url.clone();
            let url = tokio::task::spawn_blocking(move || {
                prepare_jetstream_url(&ingestor, stale_threshold, &base_url)
            })
            .await
            .map_err(|error| {
                IngestError::WebSocket(format!("Jetstream URL worker failed: {error}"))
            })??;
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
                Err(error) => tracing::warn!(%error, %endpoint, "Jetstream connection ended"),
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
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join(format!(
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
        let writer = SqliteWriter::new(connection).unwrap();
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
        let writer = SqliteWriter::new(connection).unwrap();
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
