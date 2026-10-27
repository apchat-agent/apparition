use futures_util::StreamExt;
use rusqlite::{Connection, OptionalExtension, params};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{
    collections::{HashMap, HashSet},
    fs,
    path::{Path, PathBuf},
    sync::{Arc, Mutex},
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
    fn did_by_verified_handle(&self, handle: &str) -> Result<Option<String>, StorageError>;
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
        self.inner
            .lock()
            .map_err(|_| StorageError::Poisoned)?
            .verified_handles
            .insert(did.to_owned(), (handle.to_owned(), Instant::now()));
        Ok(())
    }

    fn clear_verified_handle(&self, did: &str) -> Result<(), StorageError> {
        self.inner
            .lock()
            .map_err(|_| StorageError::Poisoned)?
            .verified_handles
            .remove(did);
        Ok(())
    }

    fn verified_handle(&self, did: &str) -> Result<Option<String>, StorageError> {
        let inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        Ok(inner
            .verified_handles
            .get(did)
            .filter(|(_, verified_at)| verified_at.elapsed() < Duration::from_secs(1800))
            .map(|(handle, _)| handle.clone()))
    }

    fn did_by_verified_handle(&self, handle: &str) -> Result<Option<String>, StorageError> {
        let inner = self.inner.lock().map_err(|_| StorageError::Poisoned)?;
        Ok(inner
            .verified_handles
            .iter()
            .find(|(_, (stored, verified_at))| {
                verified_at.elapsed() < Duration::from_secs(1800)
                    && stored.eq_ignore_ascii_case(handle)
            })
            .map(|(did, _)| did.clone()))
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
             CREATE TABLE IF NOT EXISTS appview_state (key TEXT PRIMARY KEY, value TEXT NOT NULL);
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
             );",
        )?;
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
        self.conn()?.execute(
            "INSERT INTO records (did, collection, rkey, uri, cid, time_us, record_json)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)
             ON CONFLICT(did, collection, rkey) DO UPDATE SET
                 uri=excluded.uri, cid=excluded.cid, time_us=excluded.time_us, record_json=excluded.record_json",
            params![record.did, record.collection, record.rkey, record.uri, record.cid, record.time_us, record_json],
        )?;
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
        self.conn()?.execute(
            "DELETE FROM records WHERE did=?1 AND collection=?2 AND rkey=?3",
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
        self.conn()?.execute(
            "INSERT INTO verified_handles(did,handle,updated_at) VALUES(?1,?2,unixepoch())
             ON CONFLICT(did) DO UPDATE SET handle=excluded.handle,updated_at=unixepoch()",
            params![did, handle],
        )?;
        Ok(())
    }

    fn clear_verified_handle(&self, did: &str) -> Result<(), StorageError> {
        self.conn()?
            .execute("DELETE FROM verified_handles WHERE did=?1", [did])?;
        Ok(())
    }

    fn verified_handle(&self, did: &str) -> Result<Option<String>, StorageError> {
        self.conn()?
            .query_row(
                "SELECT handle FROM verified_handles WHERE did=?1 AND updated_at>=unixepoch()-1800",
                [did],
                |row| row.get(0),
            )
            .optional()
            .map_err(Into::into)
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
                "SELECT did FROM verified_handles WHERE lower(handle)=lower(?1) AND updated_at>=unixepoch()-1800",
                [handle], |row| row.get(0),
            )
            .optional()
            .map_err(Into::into)
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
    started: Instant,
    processed: Mutex<u64>,
    last_metrics: Mutex<Instant>,
}

impl<S: Storage> Ingestor<S> {
    pub fn new(storage: Arc<S>, scope: ScopeMode) -> Self {
        Self {
            storage,
            scope,
            started: Instant::now(),
            processed: Mutex::new(0),
            last_metrics: Mutex::new(Instant::now()),
        }
    }

    pub fn process_json(&self, json: &str) -> Result<(), IngestError> {
        let event: JetstreamEvent = serde_json::from_str(json)?;
        if event.kind == "commit"
            && let Some(commit) = event.commit
            && SUPPORTED_COLLECTIONS.contains(&commit.collection.as_str())
        {
            self.process_commit(event.did, event.time_us, commit)?;
        }
        self.storage.set_cursor(event.time_us)?;
        self.record_metrics();
        Ok(())
    }

    fn process_commit(
        &self,
        did: String,
        time_us: u64,
        commit: JetstreamCommit,
    ) -> Result<(), IngestError> {
        let previous = self
            .storage
            .get_record(&did, &commit.collection, &commit.rkey)?;
        let should_index = match self.scope {
            ScopeMode::Full => true,
            ScopeMode::Scoped => {
                if matches!(
                    commit.collection.as_str(),
                    "app.bsky.feed.like" | "app.bsky.feed.repost"
                ) {
                    let target_did = commit
                        .record
                        .as_ref()
                        .and_then(|record| record.pointer("/subject/uri"))
                        .and_then(Value::as_str)
                        .and_then(uri_did);
                    match target_did {
                        Some(target_did) => self.storage.is_registered(target_did)?,
                        None => false,
                    }
                } else {
                    self.storage.is_registered(&did)?
                        || self.storage.is_followed_by_registered(&did)?
                }
            }
        };

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

    fn record_metrics(&self) {
        if let (Ok(mut processed), Ok(mut last_metrics)) =
            (self.processed.lock(), self.last_metrics.lock())
        {
            *processed += 1;
            if last_metrics.elapsed() >= Duration::from_secs(10) {
                let elapsed = self.started.elapsed().as_secs_f64().max(0.001);
                tracing::info!(
                    events_per_second = *processed as f64 / elapsed,
                    bytes_on_disk = self.storage.bytes_on_disk(),
                    events = *processed,
                    "Jetstream ingestion metrics"
                );
                *last_metrics = Instant::now();
            }
        }
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
}

impl<S: Storage + 'static> JetstreamConsumer<S> {
    pub fn new(ingestor: Ingestor<S>, reconnect_delay: Duration) -> Self {
        Self {
            ingestor: Arc::new(ingestor),
            reconnect_delay,
            idle_timeout: Duration::from_secs(60),
        }
    }

    pub fn with_idle_timeout(mut self, idle_timeout: Duration) -> Self {
        self.idle_timeout = idle_timeout;
        self
    }

    pub async fn consume_connection(&self, url: &str) -> Result<(), IngestError> {
        let (mut websocket, _) = connect_async(url)
            .await
            .map_err(|error| IngestError::WebSocket(error.to_string()))?;
        tracing::info!(%url, "Jetstream connected");
        let result = async {
            loop {
                let message = tokio::time::timeout(self.idle_timeout, websocket.next())
                    .await
                    .map_err(|_| {
                        IngestError::WebSocket(format!(
                            "no websocket message received for {} seconds",
                            self.idle_timeout.as_secs_f64()
                        ))
                    })?;
                let Some(message) = message else {
                    break;
                };
                match message.map_err(|error| IngestError::WebSocket(error.to_string()))? {
                    Message::Text(text) => self.ingestor.process_json(&text)?,
                    Message::Binary(bytes) => {
                        let text = std::str::from_utf8(&bytes)
                            .map_err(|error| IngestError::WebSocket(error.to_string()))?;
                        self.ingestor.process_json(text)?;
                    }
                    Message::Close(_) => break,
                    Message::Ping(_) | Message::Pong(_) | Message::Frame(_) => {}
                }
            }
            Ok(())
        }
        .await;
        match &result {
            Ok(()) => tracing::info!(%url, "Jetstream disconnected"),
            Err(error) => tracing::info!(%url, %error, "Jetstream disconnected"),
        }
        result
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
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            let base_url = &urls[next_url % urls.len()];
            next_url += 1;
            let url = self.url_with_cursor(base_url)?;
            tokio::select! {
                result = self.consume_connection(&url) => match result {
                    Ok(()) => backoff = self.reconnect_delay,
                    Err(error) => tracing::warn!(%error, %url, "Jetstream connection ended"),
                },
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
            }
            tokio::select! {
                _ = tokio::time::sleep(backoff) => {},
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
