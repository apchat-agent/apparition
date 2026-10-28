use futures_util::StreamExt;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    cmp::Reverse,
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use thiserror::Error;
use tokio::sync::watch;
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

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PostInteractionSummary {
    pub like_count: usize,
    pub repost_count: usize,
    pub reply_count: usize,
    pub viewer_like: Option<String>,
    pub viewer_repost: Option<String>,
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
    fn put_record(&self, record: &IndexedRecord) -> Result<(), StorageError>;
    fn replace_repo_snapshot(
        &self,
        did: &str,
        records: &[IndexedRecord],
        following: &[String],
    ) -> Result<(), StorageError>;
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
    fn notifications_for(
        &self,
        did: &str,
        before: Option<&NotificationCursor>,
        limit: usize,
    ) -> Result<Vec<IndexedNotification>, StorageError>;
    fn notification_count(&self, did: &str) -> Result<usize, StorageError>;
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
    fn set_follow(&self, actor: &str, subject: &str, following: bool) -> Result<(), StorageError>;
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
    fn is_displayed_did(&self, did: &str) -> Result<bool, StorageError>;
    fn stale_handle_dids(&self, ttl: Duration, limit: usize) -> Result<Vec<String>, StorageError>;
    fn queue_handle_refresh(&self, did: &str) -> Result<(), StorageError>;
    fn pending_handle_refreshes(&self, limit: usize) -> Result<Vec<String>, StorageError>;
    fn complete_handle_refresh(&self, did: &str) -> Result<(), StorageError>;
    fn bytes_on_disk(&self) -> u64;
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
}

#[derive(Default)]
struct MemoryState {
    records: HashMap<(String, String, String), IndexedRecord>,
    cursor: Option<u64>,
    registered: HashSet<String>,
    web_session_generations: HashMap<(String, String), u64>,
    follows: HashSet<(String, String)>,
    backfill_queue: HashSet<String>,
    backfill_retries: HashMap<String, BackfillRetry>,
    backfill_owners: HashSet<(String, String)>,
    backfill_completed: HashSet<String>,
    verified_handles: HashMap<String, (String, Instant)>,
    invalid_handles: HashMap<String, Instant>,
    handle_refresh_queue: HashSet<String>,
}

struct BackfillRetry {
    failures: u32,
    retry_at: Instant,
}

impl Storage for MemoryStorage {
    fn put_record(&self, record: &IndexedRecord) -> Result<(), StorageError> {
        let key = (
            record.did.clone(),
            record.collection.clone(),
            record.rkey.clone(),
        );
        self.inner
            .lock()
            .map_err(|_| StorageError::Poisoned)?
            .records
            .insert(key, record.clone());
        Ok(())
    }

    fn replace_repo_snapshot(
        &self,
        did: &str,
        records: &[IndexedRecord],
        following: &[String],
    ) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        inner
            .records
            .retain(|(record_did, _, _), _| record_did != did);
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
        }
        for subject in following {
            inner.follows.insert((did.to_owned(), subject.clone()));
            if inner.registered.contains(did) {
                inner
                    .backfill_owners
                    .insert((did.to_owned(), subject.clone()));
                inner.backfill_queue.insert(subject.clone());
                inner.backfill_completed.remove(subject);
            }
        }
        Ok(())
    }

    fn delete_record(&self, did: &str, collection: &str, rkey: &str) -> Result<(), StorageError> {
        self.inner
            .lock()
            .map_err(|_| StorageError::Poisoned)?
            .records
            .remove(&(did.to_owned(), collection.to_owned(), rkey.to_owned()));
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
        Ok(self
            .inner
            .lock()
            .map_err(|_| StorageError::Poisoned)?
            .records
            .values()
            .find(|record| record.collection == collection && record.uri == uri)
            .cloned())
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
        notifications.truncate(limit.clamp(1, 101));
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
        inner.registered.insert(did.to_owned());
        inner.backfill_queue.insert(did.to_owned());
        inner
            .backfill_owners
            .insert((did.to_owned(), did.to_owned()));
        inner.backfill_completed.remove(did);
        Ok(())
    }

    fn is_registered(&self, did: &str) -> Result<bool, StorageError> {
        Ok(self
            .inner
            .lock()
            .map_err(|_| StorageError::Poisoned)?
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

    fn unregister_user(&self, did: &str) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        inner.registered.remove(did);
        inner.follows.retain(|(actor, _)| actor != did);
        inner.backfill_owners.retain(|(owner, _)| owner != did);
        if !inner
            .follows
            .iter()
            .any(|(actor, subject)| subject == did && inner.registered.contains(actor))
        {
            inner.backfill_queue.remove(did);
        }
        Ok(())
    }

    fn set_follow(&self, actor: &str, subject: &str, following: bool) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        let edge = (actor.to_owned(), subject.to_owned());
        if following {
            inner.follows.insert(edge);
            if inner.registered.contains(actor) {
                inner
                    .backfill_owners
                    .insert((actor.to_owned(), subject.to_owned()));
            }
        } else {
            inner.follows.remove(&edge);
            inner
                .backfill_owners
                .remove(&(actor.to_owned(), subject.to_owned()));
        }
        Ok(())
    }

    fn is_followed_by_registered(&self, did: &str) -> Result<bool, StorageError> {
        let inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        Ok(inner
            .follows
            .iter()
            .any(|(actor, subject)| subject == did && inner.registered.contains(actor)))
    }

    fn enqueue_backfill(&self, did: &str) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        inner.backfill_queue.insert(did.to_owned());
        inner.backfill_completed.remove(did);
        Ok(())
    }

    fn enqueue_backfill_for(&self, owner_did: &str, did: &str) -> Result<(), StorageError> {
        let mut inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
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
        Ok(inner.registered.contains(did)
            || inner
                .follows
                .iter()
                .any(|(actor, subject)| subject == did && inner.registered.contains(actor)))
    }

    fn is_displayed_did(&self, did: &str) -> Result<bool, StorageError> {
        let inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        Ok(inner.records.values().any(|record| record.did == did))
    }

    fn stale_handle_dids(&self, ttl: Duration, limit: usize) -> Result<Vec<String>, StorageError> {
        let inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        let is_tracked = |did: &str| {
            inner.registered.contains(did)
                || inner.records.values().any(|record| record.did == did)
                || inner
                    .follows
                    .iter()
                    .any(|(actor, subject)| subject == did && inner.registered.contains(actor))
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
        self.inner
            .lock()
            .map_err(|_| StorageError::Poisoned)?
            .handle_refresh_queue
            .insert(did.to_owned());
        Ok(())
    }

    fn pending_handle_refreshes(&self, limit: usize) -> Result<Vec<String>, StorageError> {
        let inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        let mut dids = inner
            .handle_refresh_queue
            .iter()
            .cloned()
            .collect::<Vec<_>>();
        dids.sort();
        dids.truncate(limit.max(1));
        Ok(dids)
    }

    fn complete_handle_refresh(&self, did: &str) -> Result<(), StorageError> {
        self.inner
            .lock()
            .map_err(|_| StorageError::Poisoned)?
            .handle_refresh_queue
            .remove(did);
        Ok(())
    }

    fn bytes_on_disk(&self) -> u64 {
        0
    }
}

pub struct SqliteStorage {
    connection: Mutex<Connection>,
    path: PathBuf,
}

impl SqliteStorage {
    pub fn open(path: impl AsRef<Path>) -> Result<Self, StorageError> {
        let path = path.as_ref().to_path_buf();
        if let Some(parent) = path.parent() {
            fs::create_dir_all(parent)?;
        }
        ensure_private_database_file(&path)?;
        let connection = Connection::open(&path)?;
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
                 requested_at INTEGER NOT NULL DEFAULT (unixepoch())
             );",
        )?;
        migrate_notification_events(&connection)?;
        Ok(Self {
            connection: Mutex::new(connection),
            path,
        })
    }

    fn conn(&self) -> Result<std::sync::MutexGuard<'_, Connection>, StorageError> {
        self.connection.lock().map_err(|_| StorageError::Poisoned)
    }
}

impl Storage for SqliteStorage {
    fn put_record(&self, record: &IndexedRecord) -> Result<(), StorageError> {
        let record_json = serde_json::to_string(&record.record)?;
        let connection = self.conn()?;
        connection.execute(
            "INSERT INTO records (did, collection, rkey, uri, cid, time_us, record_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(did, collection, rkey) DO UPDATE SET
                 uri=excluded.uri, cid=excluded.cid, time_us=excluded.time_us, record_json=excluded.record_json",
            params![record.did, record.collection, record.rkey, record.uri, record.cid, record.time_us, record_json],
        )?;
        write_notification_events(&connection, record)?;
        Ok(())
    }

    fn replace_repo_snapshot(
        &self,
        did: &str,
        records: &[IndexedRecord],
        following: &[String],
    ) -> Result<(), StorageError> {
        let mut connection = self.conn()?;
        let transaction = connection.transaction()?;
        transaction.execute("DELETE FROM records WHERE did=?1", [did])?;
        transaction.execute("DELETE FROM notification_events WHERE source_did=?1", [did])?;
        transaction.execute("DELETE FROM follows WHERE actor=?1", [did])?;
        transaction.execute(
            "DELETE FROM backfill_scope WHERE owner_did=?1 AND target_did!=?1",
            [did],
        )?;
        let is_registered: bool = transaction.query_row(
            "SELECT EXISTS(SELECT 1 FROM registered_users WHERE did=?1)",
            [did],
            |row| row.get(0),
        )?;
        {
            let mut insert = transaction.prepare(
                "INSERT INTO records (did, collection, rkey, uri, cid, time_us, record_json)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
            )?;
            for record in records {
                insert.execute(params![
                    record.did,
                    record.collection,
                    record.rkey,
                    record.uri,
                    record.cid,
                    record.time_us,
                    serde_json::to_string(&record.record)?,
                ])?;
                write_notification_events(&transaction, record)?;
            }
        }
        {
            let mut insert_follow = transaction
                .prepare("INSERT OR IGNORE INTO follows(actor,subject) VALUES(?1,?2)")?;
            let mut insert_scope = transaction.prepare(
                "INSERT OR IGNORE INTO backfill_scope(owner_did,target_did) VALUES(?1,?2)",
            )?;
            let mut enqueue =
                transaction.prepare("INSERT OR IGNORE INTO backfill_queue(did) VALUES(?1)")?;
            let mut set_pending = transaction.prepare(
                "INSERT INTO backfill_status(did,done) VALUES(?1,0)
                 ON CONFLICT(did) DO UPDATE SET done=0",
            )?;
            for subject in following {
                insert_follow.execute(params![did, subject])?;
                if is_registered {
                    insert_scope.execute(params![did, subject])?;
                    enqueue.execute([subject])?;
                    set_pending.execute([subject])?;
                }
            }
        }
        transaction.commit()?;
        Ok(())
    }

    fn delete_record(&self, did: &str, collection: &str, rkey: &str) -> Result<(), StorageError> {
        let connection = self.conn()?;
        connection.execute(
            "DELETE FROM records WHERE did=?1 AND collection=?2 AND rkey=?3",
            params![did, collection, rkey],
        )?;
        connection.execute(
            "DELETE FROM notification_events WHERE source_did=?1 AND source_collection=?2 AND source_rkey=?3",
            params![did, collection, rkey],
        )?;
        Ok(())
    }

    fn get_record(
        &self,
        did: &str,
        collection: &str,
        rkey: &str,
    ) -> Result<Option<IndexedRecord>, StorageError> {
        self.conn()?
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
        self.conn()?
            .query_row(
                "SELECT did, collection, rkey, uri, cid, time_us, record_json FROM records WHERE collection=?1 AND uri=?2",
                params![collection, uri],
                row_to_record,
            )
            .optional()
            .map_err(Into::into)
    }

    fn replies_to(
        &self,
        parent_uri: &str,
        limit: usize,
    ) -> Result<Vec<IndexedRecord>, StorageError> {
        let conn = self.conn()?;
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
        let conn = self.conn()?;
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

    fn follows_for_actor(
        &self,
        did: &str,
        limit: usize,
    ) -> Result<Vec<IndexedRecord>, StorageError> {
        let conn = self.conn()?;
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
        let conn = self.conn()?;
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
        let counts: (i64, i64, i64) = self.conn()?.query_row(
            "SELECT
                (SELECT COUNT(*) FROM records WHERE collection='app.bsky.graph.follow' AND did=?1),
                (SELECT COUNT(*) FROM records WHERE collection='app.bsky.graph.follow' AND json_extract(record_json, '$.subject')=?1),
                (SELECT COUNT(*) FROM records WHERE collection='app.bsky.feed.post' AND did=?1)",
            [did],
            |row| Ok((row.get(0)?, row.get(1)?, row.get(2)?)),
        )?;
        Ok((counts.0 as usize, counts.1 as usize, counts.2 as usize))
    }

    fn notifications_for(
        &self,
        did: &str,
        before: Option<&NotificationCursor>,
        limit: usize,
    ) -> Result<Vec<IndexedNotification>, StorageError> {
        let conn = self.conn()?;
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
                    limit.clamp(1, 101) as i64,
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
        Ok(self.conn()?.query_row(
            "SELECT COUNT(*) FROM notification_events WHERE subject_did=?1",
            [did],
            |row| row.get(0),
        )?)
    }

    fn author_posts(
        &self,
        did: &str,
        cursor: Option<&RecordCursor>,
        limit: usize,
        include_replies: bool,
    ) -> Result<Vec<IndexedRecord>, StorageError> {
        let conn = self.conn()?;
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
        let conn = self.conn()?;
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
        let conn = self.conn()?;
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
        let conn = self.conn()?;
        let mut statement = conn.prepare(
            "SELECT did, collection, rkey, uri, cid, time_us, record_json FROM records ORDER BY time_us DESC",
        )?;
        let records = statement
            .query_map([], row_to_record)?
            .collect::<Result<Vec<_>, _>>()?;
        Ok(records)
    }

    fn cursor(&self) -> Result<Option<u64>, StorageError> {
        self.conn()?
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
        self.conn()?.execute(
            "INSERT INTO appview_state(key,value) VALUES('jetstream_cursor',?1)
             ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            [cursor.to_string()],
        )?;
        Ok(())
    }

    fn register_user(&self, did: &str) -> Result<(), StorageError> {
        let mut connection = self.conn()?;
        let transaction = connection.transaction()?;
        transaction.execute(
            "INSERT OR IGNORE INTO registered_users(did) VALUES(?1)",
            [did],
        )?;
        transaction.execute(
            "INSERT OR IGNORE INTO backfill_queue(did) VALUES(?1)",
            [did],
        )?;
        transaction.execute(
            "INSERT INTO backfill_status(did,done) VALUES(?1,0)
             ON CONFLICT(did) DO UPDATE SET done=0",
            [did],
        )?;
        transaction.execute(
            "INSERT OR IGNORE INTO backfill_scope(owner_did,target_did) VALUES(?1,?1)",
            [did],
        )?;
        transaction.commit()?;
        Ok(())
    }

    fn unregister_user(&self, did: &str) -> Result<(), StorageError> {
        let mut connection = self.conn()?;
        let transaction = connection.transaction()?;
        transaction.execute("DELETE FROM registered_users WHERE did=?1", [did])?;
        transaction.execute("DELETE FROM follows WHERE actor=?1", [did])?;
        transaction.execute("DELETE FROM backfill_scope WHERE owner_did=?1", [did])?;
        transaction.execute(
            "DELETE FROM backfill_queue WHERE did=?1 AND NOT EXISTS (
                 SELECT 1 FROM follows f JOIN registered_users r ON f.actor=r.did WHERE f.subject=?1
             )",
            [did],
        )?;
        transaction.commit()?;
        Ok(())
    }

    fn is_registered(&self, did: &str) -> Result<bool, StorageError> {
        Ok(self.conn()?.query_row(
            "SELECT EXISTS(SELECT 1 FROM registered_users WHERE did=?1)",
            [did],
            |row| row.get(0),
        )?)
    }

    fn set_follow(&self, actor: &str, subject: &str, following: bool) -> Result<(), StorageError> {
        let connection = self.conn()?;
        if following {
            connection.execute(
                "INSERT OR IGNORE INTO follows(actor,subject) VALUES(?1,?2)",
                params![actor, subject],
            )?;
            connection.execute(
                "INSERT OR IGNORE INTO backfill_scope(owner_did,target_did)
                 SELECT ?1,?2 WHERE EXISTS(SELECT 1 FROM registered_users WHERE did=?1)",
                params![actor, subject],
            )?;
        } else {
            connection.execute(
                "DELETE FROM follows WHERE actor=?1 AND subject=?2",
                params![actor, subject],
            )?;
            connection.execute(
                "DELETE FROM backfill_scope WHERE owner_did=?1 AND target_did=?2",
                params![actor, subject],
            )?;
        }
        Ok(())
    }

    fn is_followed_by_registered(&self, did: &str) -> Result<bool, StorageError> {
        Ok(self.conn()?.query_row(
            "SELECT EXISTS(SELECT 1 FROM follows f JOIN registered_users r ON f.actor=r.did WHERE f.subject=?1)",
            [did],
            |row| row.get(0),
        )?)
    }

    fn enqueue_backfill(&self, did: &str) -> Result<(), StorageError> {
        let mut connection = self.conn()?;
        let transaction = connection.transaction()?;
        transaction.execute(
            "INSERT OR IGNORE INTO backfill_queue(did) VALUES(?1)",
            [did],
        )?;
        transaction.execute(
            "INSERT INTO backfill_status(did,done) VALUES(?1,0)
             ON CONFLICT(did) DO UPDATE SET done=0",
            [did],
        )?;
        transaction.commit()?;
        Ok(())
    }

    fn enqueue_backfill_for(&self, owner_did: &str, did: &str) -> Result<(), StorageError> {
        let mut connection = self.conn()?;
        let transaction = connection.transaction()?;
        transaction.execute(
            "INSERT OR IGNORE INTO backfill_scope(owner_did,target_did) VALUES(?1,?2)",
            params![owner_did, did],
        )?;
        transaction.execute(
            "INSERT OR IGNORE INTO backfill_queue(did) VALUES(?1)",
            [did],
        )?;
        transaction.execute(
            "INSERT INTO backfill_status(did,done) VALUES(?1,0)
             ON CONFLICT(did) DO UPDATE SET done=0",
            [did],
        )?;
        transaction.commit()?;
        Ok(())
    }

    fn pending_backfills(&self) -> Result<Vec<String>, StorageError> {
        let conn = self.conn()?;
        let mut statement = conn.prepare(
            "SELECT q.did FROM backfill_queue q
             LEFT JOIN backfill_retry r ON r.did=q.did
             WHERE r.did IS NULL OR r.retry_at_ms<=?1
             ORDER BY q.enqueued_at,q.did",
        )?;
        Ok(statement
            .query_map([epoch_millis()], |row| row.get(0))?
            .collect::<Result<Vec<_>, _>>()?)
    }

    fn complete_backfill(&self, did: &str) -> Result<(), StorageError> {
        let mut connection = self.conn()?;
        let transaction = connection.transaction()?;
        transaction.execute("DELETE FROM backfill_queue WHERE did=?1", [did])?;
        transaction.execute("DELETE FROM backfill_retry WHERE did=?1", [did])?;
        transaction.execute(
            "INSERT INTO backfill_status(did,done) VALUES(?1,1)
             ON CONFLICT(did) DO UPDATE SET done=1",
            [did],
        )?;
        transaction.commit()?;
        Ok(())
    }

    fn record_backfill_failure(&self, did: &str) -> Result<Duration, StorageError> {
        let mut connection = self.conn()?;
        let transaction = connection.transaction()?;
        let failures = transaction
            .query_row(
                "SELECT failures FROM backfill_retry WHERE did=?1",
                [did],
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
            params![did, failures, retry_at_ms],
        )?;
        transaction.commit()?;
        Ok(delay)
    }

    fn backfill_progress(&self, owner_did: &str) -> Result<(usize, usize), StorageError> {
        let (total, done): (i64, i64) = self.conn()?.query_row(
            "SELECT COUNT(*), COALESCE(SUM(CASE WHEN s.done=1 THEN 1 ELSE 0 END),0)
             FROM backfill_scope b LEFT JOIN backfill_status s ON s.did=b.target_did
             WHERE b.owner_did=?1",
            [owner_did],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;
        Ok((total as usize, done as usize))
    }

    fn backfill_failure_count(&self, owner_did: &str) -> Result<usize, StorageError> {
        let count: i64 = self.conn()?.query_row(
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
        let connection = self.conn()?;
        connection.execute(
            "INSERT INTO verified_handles(did,handle,updated_at) VALUES(?1,?2,unixepoch())
             ON CONFLICT(did) DO UPDATE SET handle=excluded.handle,updated_at=unixepoch()",
            params![did, handle],
        )?;
        connection.execute("DELETE FROM invalid_handles WHERE did=?1", [did])?;
        Ok(())
    }

    fn clear_verified_handle(&self, did: &str) -> Result<(), StorageError> {
        let connection = self.conn()?;
        connection.execute("DELETE FROM verified_handles WHERE did=?1", [did])?;
        connection.execute(
            "INSERT INTO invalid_handles(did,checked_at) VALUES(?1,unixepoch())
             ON CONFLICT(did) DO UPDATE SET checked_at=unixepoch()",
            [did],
        )?;
        Ok(())
    }

    fn verified_handle(&self, did: &str) -> Result<Option<String>, StorageError> {
        self.conn()?
            .query_row(
                "SELECT handle FROM verified_handles WHERE did=?1",
                [did],
                |row| row.get(0),
            )
            .optional()
            .map_err(Into::into)
    }

    fn handle_is_invalid(&self, did: &str) -> Result<bool, StorageError> {
        Ok(self.conn()?.query_row(
            "SELECT EXISTS(SELECT 1 FROM invalid_handles WHERE did=?1)",
            [did],
            |row| row.get(0),
        )?)
    }

    fn rotate_web_session(&self, did: &str, session_id: &str) -> Result<u64, StorageError> {
        let mut connection = self.conn()?;
        let transaction = connection.transaction()?;
        let current = transaction
            .query_row(
                "SELECT generation FROM web_sessions WHERE did=?1 AND session_id=?2",
                params![did, session_id],
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
            params![did, session_id, next],
        )?;
        transaction.commit()?;
        u64::try_from(next).map_err(|_| rusqlite::Error::InvalidQuery.into())
    }

    fn web_session_generation(
        &self,
        did: &str,
        session_id: &str,
    ) -> Result<Option<u64>, StorageError> {
        self.conn()?
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

    fn did_by_verified_handle(&self, handle: &str) -> Result<Option<String>, StorageError> {
        self.conn()?
            .query_row(
                "SELECT did FROM verified_handles WHERE lower(handle)=lower(?1)",
                [handle],
                |row| row.get(0),
            )
            .optional()
            .map_err(Into::into)
    }

    fn is_tracked_did(&self, did: &str) -> Result<bool, StorageError> {
        Ok(self.conn()?.query_row(
            "SELECT EXISTS(SELECT 1 FROM registered_users WHERE did=?1)
                OR EXISTS(SELECT 1 FROM follows f JOIN registered_users r ON r.did=f.actor WHERE f.subject=?1)",
            [did],
            |row| row.get(0),
        )?)
    }

    fn is_displayed_did(&self, did: &str) -> Result<bool, StorageError> {
        Ok(self.conn()?.query_row(
            "SELECT EXISTS(SELECT 1 FROM records WHERE did=?1)",
            [did],
            |row| row.get(0),
        )?)
    }

    fn stale_handle_dids(&self, ttl: Duration, limit: usize) -> Result<Vec<String>, StorageError> {
        let ttl_seconds = i64::try_from(ttl.as_secs()).unwrap_or(i64::MAX);
        let connection = self.conn()?;
        let mut statement = connection.prepare(
            "SELECT did FROM (
                 SELECT h.did AS did,h.updated_at AS checked_at FROM verified_handles h
                 UNION ALL
                 SELECT i.did AS did,i.checked_at AS checked_at FROM invalid_handles i
             ) h
             WHERE h.checked_at<=unixepoch()-?1
               AND (EXISTS(SELECT 1 FROM registered_users r WHERE r.did=h.did)
                    OR EXISTS(SELECT 1 FROM records x WHERE x.did=h.did)
                    OR EXISTS(SELECT 1 FROM follows f JOIN registered_users r ON r.did=f.actor WHERE f.subject=h.did))
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
        self.conn()?.execute(
            "INSERT OR IGNORE INTO handle_refresh_queue(did) VALUES(?1)",
            [did],
        )?;
        Ok(())
    }

    fn pending_handle_refreshes(&self, limit: usize) -> Result<Vec<String>, StorageError> {
        let connection = self.conn()?;
        let mut statement = connection
            .prepare("SELECT did FROM handle_refresh_queue ORDER BY requested_at,did LIMIT ?1")?;
        let dids = statement
            .query_map([i64::try_from(limit.max(1)).unwrap_or(i64::MAX)], |row| {
                row.get(0)
            })?
            .collect::<Result<Vec<String>, _>>()?;
        Ok(dids)
    }

    fn complete_handle_refresh(&self, did: &str) -> Result<(), StorageError> {
        self.conn()?
            .execute("DELETE FROM handle_refresh_queue WHERE did=?1", [did])?;
        Ok(())
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
    scope: ScopeMode,
    cursor: AtomicU64,
    processed: AtomicU64,
}

impl<S: Storage> Ingestor<S> {
    pub fn new(storage: Arc<S>, scope: ScopeMode) -> Self {
        let cursor = storage.cursor().ok().flatten().unwrap_or_default();
        Self {
            storage,
            scope,
            cursor: AtomicU64::new(cursor),
            processed: AtomicU64::new(0),
        }
    }

    pub fn process_json(&self, json: &str) -> Result<(), IngestError> {
        let event: JetstreamEvent = serde_json::from_str(json)?;
        if matches!(event.kind.as_str(), "identity" | "account")
            && (self.storage.is_tracked_did(&event.did)?
                || self.storage.is_displayed_did(&event.did)?)
        {
            self.storage.queue_handle_refresh(&event.did)?;
        }
        if event.kind == "commit"
            && let Some(commit) = event.commit
            && SUPPORTED_COLLECTIONS.contains(&commit.collection.as_str())
        {
            self.process_commit(event.did, event.time_us, commit)?;
        }
        self.storage.set_cursor(event.time_us)?;
        self.cursor.store(event.time_us, Ordering::Relaxed);
        self.processed.fetch_add(1, Ordering::Relaxed);
        Ok(())
    }

    fn process_commit(
        &self,
        did: String,
        time_us: u64,
        commit: JetstreamCommit,
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
                    .map(|target_did| self.storage.is_tracked_did(target_did))
                    .transpose()?
                    .unwrap_or(false),
                "app.bsky.feed.post" => self.post_references_tracked(commit.record.as_ref())?,
                _ => false,
            }
        };
        let actor_tracked = match self.scope {
            ScopeMode::Full => true,
            ScopeMode::Scoped => self.storage.is_tracked_did(&did)?,
        };
        // Avoid a database record lookup for unrelated creates and updates. For a
        // delete, inspect the key so previously indexed interactions can be removed.
        let previous =
            if commit.operation == "delete" || commit.collection == "app.bsky.graph.follow" {
                self.storage
                    .get_record(&did, &commit.collection, &commit.rkey)?
            } else {
                None
            };
        let should_index = actor_tracked || referenced_tracked || previous.is_some();

        if should_index {
            if commit.operation == "delete" {
                self.storage
                    .delete_record(&did, &commit.collection, &commit.rkey)?;
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
                self.storage.put_record(&indexed)?;
            }
        }

        if commit.collection == "app.bsky.graph.follow" && self.storage.is_registered(&did)? {
            if let Some(old) = previous.as_ref()
                && let Some(subject) = old.record.get("subject").and_then(Value::as_str)
            {
                self.storage.set_follow(&did, subject, false)?;
            }
            if commit.operation != "delete"
                && let Some(subject) = commit
                    .record
                    .as_ref()
                    .and_then(|record| record.get("subject"))
                && let Some(subject) = subject.as_str()
            {
                self.storage.set_follow(&did, subject, true)?;
                if self.scope == ScopeMode::Scoped {
                    self.storage.enqueue_backfill_for(&did, subject)?;
                }
            }
        }
        Ok(())
    }

    fn post_references_tracked(&self, record: Option<&Value>) -> Result<bool, IngestError> {
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
                && self.storage.is_tracked_did(did)?
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
            if self.storage.is_tracked_did(did)? {
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn record_metrics(&self, interval: Duration) {
        let events = self.processed.swap(0, Ordering::Relaxed);
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
}

struct JetstreamConnectionLog(String);

impl Drop for JetstreamConnectionLog {
    fn drop(&mut self) {
        tracing::info!(url = %self.0, "Jetstream disconnected");
    }
}

impl<S: Storage + 'static> JetstreamConsumer<S> {
    pub fn new(ingestor: Ingestor<S>, reconnect_delay: Duration) -> Self {
        Self {
            ingestor: Arc::new(ingestor),
            reconnect_delay,
            idle_timeout: Duration::from_secs(60),
            event_timeout: Duration::from_secs(60),
            metrics_interval: Duration::from_secs(10),
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

    pub async fn consume_connection(&self, url: &str) -> Result<(), IngestError> {
        let (mut websocket, _) = connect_async(url)
            .await
            .map_err(|error| IngestError::WebSocket(error.to_string()))?;
        tracing::info!(%url, "Jetstream connected");
        let _connection_log = JetstreamConnectionLog(url.to_owned());
        async {
            let mut idle_deadline = tokio::time::Instant::now() + self.idle_timeout;
            loop {
                let remaining =
                    idle_deadline.saturating_duration_since(tokio::time::Instant::now());
                let message = tokio::time::timeout(remaining, websocket.next())
                    .await
                    .map_err(|_| {
                        IngestError::WebSocket(format!(
                            "no Jetstream event received for {} seconds",
                            self.idle_timeout.as_secs_f64()
                        ))
                    })?;
                let Some(message) = message else {
                    break;
                };
                match message.map_err(|error| IngestError::WebSocket(error.to_string()))? {
                    Message::Text(text) => {
                        self.process_event_text(text.to_string()).await?;
                        idle_deadline = tokio::time::Instant::now() + self.idle_timeout;
                    }
                    Message::Binary(bytes) => {
                        let text = std::str::from_utf8(&bytes)
                            .map_err(|error| IngestError::WebSocket(error.to_string()))?;
                        self.process_event_text(text.to_owned()).await?;
                        idle_deadline = tokio::time::Instant::now() + self.idle_timeout;
                    }
                    Message::Close(_) => break,
                    Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {}
                }
            }
            Ok(())
        }
        .await
    }

    async fn process_event_text(&self, text: String) -> Result<(), IngestError> {
        let event: Value = serde_json::from_str(&text)?;
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
        let task = tokio::task::spawn_blocking(move || ingestor.process_json(&text));
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
            let url = self.url_with_cursor(base_url)?;
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
                Err(error) => tracing::warn!(%error, %url, "Jetstream connection ended"),
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

    fn url_with_cursor(&self, base_url: &str) -> Result<String, IngestError> {
        let mut url = reqwest::Url::parse(base_url)
            .map_err(|error| IngestError::WebSocket(error.to_string()))?;
        if let Some(cursor) = self.ingestor.storage.cursor()? {
            url.query_pairs_mut()
                .append_pair("cursor", &cursor.to_string());
        }
        for collection in SUPPORTED_COLLECTIONS {
            url.query_pairs_mut()
                .append_pair("wantedCollections", collection);
        }
        Ok(url.to_string())
    }
}
