//! Background "recent posts" fetching for profile pages (owner 2026-11-01:
//! "an unfollowed profile lookup should load their posts at least the most
//! recent ones in a worker" / "And cache").
//!
//! A profile page for an account whose recent posts are not cached queues one
//! deduplicated job on a bounded queue and renders what is local immediately;
//! it never waits for the fetch. [`RecentPostsWorker`] drains the queue: it
//! asks the OAuth flow for the account's newest posts (one unauthenticated
//! `com.atproto.repo.listRecords` page against the account's own PDS, newest
//! first), stores them as normal records through the existing writer, and
//! records a per-account fetch timestamp. Within [`RECENT_POSTS_TTL`] no
//! refetch is queued; a failed fetch is retried only after
//! [`RECENT_POSTS_RETRY_BACKOFF`]. The queue is bounded by
//! [`crate::ingest::MAX_RECENT_POSTS_QUEUE`]: overflow is dropped and counted,
//! never blocked. This is separate from the full-repo backfill, whose pacing
//! this module does not touch.

use crate::ingest::{RecentPostsEnqueue, Storage, StorageError};
use crate::oauth::OAuthFlow;
use std::{sync::Arc, time::Duration};

/// How long one account's fetched recent posts stay authoritative: a visit
/// inside this window queues no refetch, after it the next visit queues a
/// refresh. Chosen 15 min for the owner's "and cache" (2026-11-01): long
/// enough that browsing a profile does not re-hit the author's PDS, short
/// enough that "most recent posts" stays recent.
pub const RECENT_POSTS_TTL: Duration = Duration::from_secs(15 * 60);
/// A failed fetch is recorded and retried only after this shorter backoff,
/// not on every visit and not waiting out the whole TTL.
pub const RECENT_POSTS_RETRY_BACKOFF: Duration = Duration::from_secs(60);
/// How many newest posts one fetch asks for — the owner's "newest ~50-100
/// posts" as one page: a single `com.atproto.repo.listRecords` request with
/// `limit=100` and `reverse=true`.
pub const RECENT_POSTS_FETCH_LIMIT: usize = 100;
/// Queued jobs one worker poll processes before it sleeps again.
pub const RECENT_POSTS_WORKER_BATCH: usize = 8;
/// A page showing fewer posts than this counts as "few/no posts" and shows
/// the short loading note while its recent-posts job is queued or running.
pub const RECENT_POSTS_NOTE_MAX_LOCAL_POSTS: usize = 5;

/// Wall-clock milliseconds since the UNIX epoch; the recent-posts cache is
/// compared against wall clock because it is written once and read by another
/// process lifetime (restarts included).
pub fn now_wall_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(i64::MAX as u128) as i64
}

/// What the profile page does about recent posts on this visit.
pub struct RecentPostsVisit {
    /// `cached` | `queued` | `fresh`, as the page footer and the
    /// `web page phases` line report `profile_recent_posts`.
    pub label: &'static str,
    /// True when a job is queued or running and the page has few/no posts,
    /// so the page shows the short loading note.
    pub show_loading_note: bool,
}

/// One profile page visit: read the per-account recent-posts state, queue one
/// deduplicated job when the cache is not fresh (bounded; overflow is dropped
/// and counted), and decide the footer label and the loading note. Every
/// storage call here is an in-memory read or a fire-and-forget write: the page
/// never waits on the fetcher or the writer.
pub fn on_profile_visit<S: Storage>(
    storage: &S,
    subject: &str,
    local_post_count: usize,
) -> RecentPostsVisit {
    // A tracked account (registered or followed by a registered user) already
    // arrives through the firehose; fetching it again would only add PDS load
    // and writer traffic for records the index has.
    if storage.is_tracked_did(subject).unwrap_or(false) {
        return RecentPostsVisit {
            label: "cached",
            show_loading_note: false,
        };
    }
    let now_ms = now_wall_ms();
    let status = match storage.recent_posts_status(subject) {
        Ok(status) => status,
        Err(error) => {
            tracing::warn!(
                %error,
                subject = %subject,
                "could not read the recent-posts status; serving the local page"
            );
            return RecentPostsVisit {
                label: "cached",
                show_loading_note: false,
            };
        }
    };
    let ttl_ms = RECENT_POSTS_TTL.as_millis() as i64;
    if status.is_some_and(|status| {
        status.fetched_at_ms > 0 && now_ms.saturating_sub(status.fetched_at_ms) < ttl_ms
    }) {
        return RecentPostsVisit {
            label: "fresh",
            show_loading_note: false,
        };
    }
    let already_queued = status.is_some_and(|status| status.queued);
    let in_backoff = status.is_some_and(|status| status.next_retry_at_ms > now_ms);
    if already_queued {
        return RecentPostsVisit {
            label: "queued",
            show_loading_note: local_post_count < RECENT_POSTS_NOTE_MAX_LOCAL_POSTS,
        };
    }
    if !in_backoff {
        match storage.enqueue_recent_posts_fetch(subject) {
            Ok(RecentPostsEnqueue::Queued) | Ok(RecentPostsEnqueue::Duplicate) => {
                return RecentPostsVisit {
                    label: "queued",
                    show_loading_note: local_post_count < RECENT_POSTS_NOTE_MAX_LOCAL_POSTS,
                };
            }
            Ok(RecentPostsEnqueue::Overflowed) => {
                tracing::debug!(
                    subject = %subject,
                    "the recent-posts queue is full; dropped this fetch (counted)"
                );
            }
            Err(error) => {
                tracing::warn!(
                    %error,
                    subject = %subject,
                    "could not queue a recent-posts fetch; serving the local page"
                );
            }
        }
    }
    RecentPostsVisit {
        label: "cached",
        show_loading_note: false,
    }
}

/// Microseconds since the UNIX epoch from an RFC 3339 `createdAt` value, the
/// same format [`crate::backfill`] parses for repository records.
pub fn parse_created_at_micros(value: &str) -> Option<u64> {
    let parsed =
        time::OffsetDateTime::parse(value, &time::format_description::well_known::Rfc3339).ok()?;
    u64::try_from(parsed.unix_timestamp_nanos() / 1_000).ok()
}

/// Drains the bounded recent-posts queue in the background: for each queued
/// account whose cache is not fresh and not in backoff, fetch the newest posts
/// and store them as normal records through the existing writer. The web page
/// that queued the job never waits for this worker.
pub struct RecentPostsWorker<F, S> {
    flow: F,
    storage: Arc<S>,
    poll_interval: Duration,
}

impl<F: OAuthFlow, S: Storage + 'static> RecentPostsWorker<F, S> {
    pub fn new(flow: F, storage: Arc<S>, poll_interval: Duration) -> Self {
        Self {
            flow,
            storage,
            poll_interval: poll_interval.max(Duration::from_millis(1)),
        }
    }

    pub async fn run_once(&self) -> Result<usize, StorageError> {
        crate::net::with_background_request_pool(self.run_once_inner()).await
    }

    async fn run_once_inner(&self) -> Result<usize, StorageError> {
        // Scan deeper than the attempt budget so accounts still inside their
        // retry backoff cannot starve the accounts queued behind them.
        let candidates = self
            .storage
            .pending_recent_posts_fetches(RECENT_POSTS_WORKER_BATCH * 4)?;
        let now_ms = now_wall_ms();
        let ttl_ms = RECENT_POSTS_TTL.as_millis() as i64;
        let mut attempted = 0_usize;
        let mut completed = 0;
        for did in candidates {
            if attempted >= RECENT_POSTS_WORKER_BATCH {
                break;
            }
            let status = self.storage.recent_posts_status(&did)?;
            if status.is_some_and(|status| {
                status.fetched_at_ms > 0 && now_ms.saturating_sub(status.fetched_at_ms) < ttl_ms
            }) {
                // A page queued this account although its cache is fresh
                // (or a second page did while the first was in flight); the
                // job has nothing left to do.
                self.storage.complete_recent_posts_fetch(&did)?;
                completed += 1;
                continue;
            }
            if status.is_some_and(|status| status.next_retry_at_ms > now_ms) {
                // Failed recently: retry only after the backoff, and keep the
                // queue slot so pages do not queue a second job for it.
                continue;
            }
            attempted += 1;
            match self.flow.fetch_recent_posts(&did).await {
                Ok(records) => {
                    let expected_prefix = format!("at://{did}/app.bsky.feed.post/");
                    let mut stored = 0_usize;
                    let mut skipped = 0_usize;
                    for record in records.iter().take(RECENT_POSTS_FETCH_LIMIT) {
                        if record.did != did
                            || record.collection != "app.bsky.feed.post"
                            || !record.uri.starts_with(expected_prefix.as_str())
                        {
                            tracing::warn!(
                                did = %did,
                                uri = %record.uri,
                                "the recent-posts fetch returned a record outside this account's post collection; skipped it"
                            );
                            skipped += 1;
                            continue;
                        }
                        // Normal records through the existing writer: the page
                        // that queued this job never waits on it.
                        self.storage.put_record(record)?;
                        stored += 1;
                    }
                    self.storage.record_recent_posts_fetched(&did, now_ms)?;
                    tracing::debug!(
                        did = %did,
                        stored,
                        skipped,
                        "stored a recent-posts page for one account"
                    );
                    self.storage.complete_recent_posts_fetch(&did)?;
                    completed += 1;
                }
                Err(error) => {
                    tracing::warn!(
                        did = %did,
                        error = %error,
                        "the recent-posts fetch failed"
                    );
                    self.storage.record_recent_posts_failure(
                        &did,
                        now_ms + RECENT_POSTS_RETRY_BACKOFF.as_millis() as i64,
                    )?;
                }
            }
        }
        Ok(completed)
    }

    pub async fn run(
        &self,
        mut shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Result<(), StorageError> {
        loop {
            if *shutdown.borrow() {
                return Ok(());
            }
            if let Err(error) = self.run_once().await {
                tracing::warn!(%error, "the recent-posts worker poll failed");
            }
            tokio::select! {
                _ = tokio::time::sleep(self.poll_interval) => {},
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
            }
        }
    }
}
