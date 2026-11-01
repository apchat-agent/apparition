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
/// How many posts one `listRecords` page asks for — the recent-posts fetch
/// and the older-on-scroll walk share it: one `com.atproto.repo.listRecords`
/// request with this `limit` and NO `reverse` (see [`list_records_url`]).
/// Lane av75q step 4 sets it to the owner's page of 25; the local profile
/// page size (50) is unchanged.
pub const RECENT_POSTS_FETCH_LIMIT: usize = 100;

fn list_records_page_url(
    pds_endpoint: &str,
    did: &str,
    cursor: Option<&str>,
    limit: usize,
) -> Result<reqwest::Url, String> {
    let mut url = reqwest::Url::parse(&format!("{pds_endpoint}/xrpc/com.atproto.repo.listRecords"))
        .map_err(|_| "author PDS URL is invalid".to_owned())?;
    let mut pairs = url.query_pairs_mut();
    pairs
        .append_pair("repo", did)
        .append_pair("collection", "app.bsky.feed.post")
        .append_pair("limit", &limit.to_string());
    if let Some(cursor) = cursor {
        pairs.append_pair("cursor", cursor);
    }
    drop(pairs);
    Ok(url)
}

/// The one `listRecords` page a recent-posts fetch asks the author's PDS for.
/// listRecords is newest-first by default; `reverse=true` turns it into
/// OLDEST-first (measured on a bsky.network PDS 2026-11-01: reverse=true gave
/// Oct 2023 posts, the default gave today's), so the flag must stay off.
pub(crate) fn list_records_url(pds_endpoint: &str, did: &str) -> Result<reqwest::Url, String> {
    list_records_page_url(pds_endpoint, did, None, RECENT_POSTS_FETCH_LIMIT)
}

/// The `listRecords` page an older-posts fetch asks for: the same newest-first
/// page, one page past `cursor` — the PDS cursor saved by the previous older
/// page, or (for the first one) the oldest rkey this account's posts we hold.
/// A PDS that runs out of older history answers with no cursor (step 3).
pub(crate) fn older_posts_url(
    pds_endpoint: &str,
    did: &str,
    cursor: Option<&str>,
) -> Result<reqwest::Url, String> {
    list_records_page_url(pds_endpoint, did, cursor, RECENT_POSTS_FETCH_LIMIT)
}

/// One page of the older-posts walk: the posts (newest first) and the PDS's
/// next-page cursor. `cursor == None` means the PDS has no older history:
/// from here on the account is complete locally and never fetched again.
#[derive(Clone, Debug, Default)]
pub struct OlderPostsPage {
    pub records: Vec<crate::ingest::IndexedRecord>,
    pub cursor: Option<String>,
}

/// Parse one `com.atproto.repo.listRecords` response of post records for one
/// account, keeping only records inside that account's post collection and
/// carrying the response's next-page cursor through. Shared by the recent-
/// posts fetch and the older-on-scroll fetch.
pub(crate) fn parse_list_records_posts(
    value: &serde_json::Value,
    did: &str,
    limit: usize,
) -> (Vec<crate::ingest::IndexedRecord>, Option<String>) {
    let records = value.get("records").and_then(serde_json::Value::as_array);
    let Some(records) = records else {
        return (Vec::new(), None);
    };
    let expected_prefix = format!("at://{did}/app.bsky.feed.post/");
    let mut posts = Vec::new();
    for entry in records.iter().take(limit) {
        let Some(uri) = entry.get("uri").and_then(serde_json::Value::as_str) else {
            tracing::debug!("skipping an author PDS recent-posts record without a uri");
            continue;
        };
        let Some(rkey) = uri.strip_prefix(expected_prefix.as_str()) else {
            tracing::debug!(
                uri,
                "skipping an author PDS record outside the requested post collection"
            );
            continue;
        };
        if rkey.is_empty() || rkey.contains('/') {
            tracing::debug!(uri, "skipping an author PDS record with an invalid rkey");
            continue;
        }
        let Some(record) = entry.get("value") else {
            tracing::debug!(uri, "skipping an author PDS record without a value");
            continue;
        };
        let time_us = record
            .get("createdAt")
            .and_then(serde_json::Value::as_str)
            .and_then(parse_created_at_micros)
            .unwrap_or_default();
        posts.push(crate::ingest::IndexedRecord {
            did: did.to_owned(),
            collection: "app.bsky.feed.post".to_owned(),
            rkey: rkey.to_owned(),
            uri: uri.to_owned(),
            cid: entry
                .get("cid")
                .and_then(serde_json::Value::as_str)
                .map(str::to_owned),
            time_us,
            record: record.clone(),
        });
    }
    let cursor = value
        .get("cursor")
        .and_then(serde_json::Value::as_str)
        .filter(|cursor| !cursor.is_empty())
        .map(str::to_owned);
    (posts, cursor)
}
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

/// What the profile page did about older posts on this request.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OlderFetch {
    /// No fetch was due: this request has no cursor, the local store still
    /// had a full page, or the account's history is already complete.
    Skipped,
    /// One fetch was attempted and the author PDS failed; the page renders
    /// what is local and the real error was logged.
    Failed,
    /// One page arrived, was stored through the normal writer, and the page
    /// must be re-read from storage so the new posts render in this request.
    Fetched { stored: usize },
}

/// The bounded older-posts fetch a profile page makes while someone scrolls
/// an account (owner 2026-11-01, lane av75q step 3): at most one
/// `com.atproto.repo.listRecords` call per page request, newest first, one
/// page past the saved PDS cursor (or, for the first older request, past the
/// oldest rkey we hold). Records land through [`Storage::put_record`], the
/// same idempotent (did, collection, rkey) upsert the Jetstream writer uses,
/// so no duplicates are possible. The cursor the PDS returns is saved for the
/// next request; a response without a cursor marks the account complete, and
/// those accounts are never fetched again. A failed fetch is logged with its
/// real cause and changes nothing else: the page still renders what is local.
pub async fn older_posts_while_scrolling<F, S>(
    flow: &F,
    storage: &S,
    subject: &str,
    oldest_local_rkey: Option<&str>,
) -> OlderFetch
where
    F: crate::oauth::OAuthFlow,
    S: Storage,
{
    let state = match storage.older_posts_state(subject) {
        Ok(state) => state,
        Err(error) => {
            tracing::warn!(
                %error,
                subject = %subject,
                "could not read the older-posts state; serving the local page"
            );
            return OlderFetch::Skipped;
        }
    };
    if state.as_ref().is_some_and(|state| state.exhausted) {
        return OlderFetch::Skipped;
    }
    let saved_cursor = state.and_then(|state| state.cursor);
    let cursor = match saved_cursor.or_else(|| oldest_local_rkey.map(str::to_owned)) {
        Some(cursor) => Some(cursor),
        // No saved cursor and no local posts at all: nothing safely marks
        // where "older" begins, and a fresh newest-first page would only
        // refetch what the visitor has already seen.
        None => return OlderFetch::Skipped,
    };
    match flow.fetch_older_posts(subject, cursor.as_deref()).await {
        Err(error) => {
            tracing::warn!(
                %error,
                subject = %subject,
                "the older-posts fetch from the author PDS failed; serving the local page"
            );
            OlderFetch::Failed
        }
        Ok(page) => {
            let expected_prefix = format!("at://{subject}/app.bsky.feed.post/");
            let mut stored = 0_usize;
            for record in page.records.iter().take(RECENT_POSTS_FETCH_LIMIT) {
                if record.did != subject
                    || record.collection != "app.bsky.feed.post"
                    || !record.uri.starts_with(expected_prefix.as_str())
                {
                    tracing::warn!(
                        did = %subject,
                        uri = %record.uri,
                        "the older-posts fetch returned a record outside this account's post collection; skipped it"
                    );
                    continue;
                }
                // The normal writer path, idempotent on (did, collection,
                // rkey) exactly like a Jetstream create for the same record.
                if let Err(error) = storage.put_record(record) {
                    tracing::warn!(
                        %error,
                        did = %subject,
                        uri = %record.uri,
                        "a fetched older post could not be stored; the page continues with what we have"
                    );
                    continue;
                }
                stored += 1;
            }
            let exhausted = page.cursor.is_none();
            if let Err(error) =
                storage.record_older_posts_page(subject, page.cursor.as_deref(), exhausted)
            {
                tracing::warn!(
                    %error,
                    subject = %subject,
                    "the older-posts cursor could not be saved; the next Older request restarts from the oldest local rkey"
                );
            }
            tracing::debug!(
                did = %subject,
                stored,
                exhausted,
                "stored an older-posts page for one account"
            );
            OlderFetch::Fetched { stored }
        }
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_recent_posts_page_asks_for_the_newest_posts_not_the_oldest() {
        let url = list_records_url("https://pds.example", "did:plc:recent").unwrap();
        let pairs: Vec<(String, String)> = url.query_pairs().into_owned().collect();
        assert_eq!(url.path(), "/xrpc/com.atproto.repo.listRecords");
        assert!(pairs.contains(&("repo".into(), "did:plc:recent".into())));
        assert!(pairs.contains(&("collection".into(), "app.bsky.feed.post".into())));
        // Literal 25 on purpose (owner 2026-11-01, lane av75q step 4): one
        // page of 25 per PDS call, recent and older-on-scroll alike. Not the
        // constant — the literal is what fails if the page size drifts.
        assert!(
            pairs.contains(&("limit".into(), "25".into())),
            "the owner asked for a page of 25 newest posts per fetch: {url}"
        );
        // listRecords is newest-first unless asked otherwise; any `reverse`
        // flips it to the account's oldest posts (the 2026-11-01 bug).
        assert!(
            pairs.iter().all(|(key, _)| key != "reverse"),
            "a recent-posts fetch must not pass reverse: {url}"
        );
    }
}
