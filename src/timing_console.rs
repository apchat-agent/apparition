//! In-memory history for the per-page timing console (lane av58q, 2026-10-31).
//!
//! Every web request gets an unguessable id, echoed to the browser as the `X-Timing-Id` response
//! header (and as `<body data-timing-id="...">` on HTML pages). When the request finishes, one
//! record goes into a small in-memory ring; the owner taps the on-page button and `/timing/recent`
//! answers with the viewer's own newest records. Nothing is ever written to disk.

use crate::net::OutboundCallRecord;
use rand::{RngCore, thread_rng};
use serde::Serialize;
use serde_json::Value;
use std::{
    collections::VecDeque,
    sync::Mutex,
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};

/// Path of the JSON endpoint the on-page panel reads; its own requests are never recorded.
pub(crate) const TIMING_RECENT_PATH: &str = "/timing/recent";
/// Newest records kept overall, across all viewers.
pub(crate) const TIMING_HISTORY_CAPACITY: usize = 200;
/// Most records one viewer may see in a single response.
pub(crate) const TIMING_PAGE_LIMIT: usize = 30;
/// Minimum spacing of the "timing console metrics" info line.
pub(crate) const TIMING_METRICS_REPORT_INTERVAL: Duration = Duration::from_secs(60);

pub(crate) fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .min(u64::MAX as u128) as u64
}

/// A random, unguessable 16-hex-char request id.
pub(crate) fn new_timing_id() -> String {
    let mut bytes = [0u8; 8];
    thread_rng().fill_bytes(&mut bytes);
    bytes
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>()
}

/// One finished web request: who made it, what it cost, and which outbound calls made it slow.
#[derive(Clone, Debug, Serialize)]
pub(crate) struct TimingRecord {
    pub id: String,
    pub viewer: Option<String>,
    pub method: String,
    pub path: String,
    pub status: u16,
    pub started_at_ms: u64,
    pub total_ms: u64,
    pub page: Option<String>,
    pub phases: Option<Value>,
    pub calls: Vec<OutboundCallRecord>,
    pub calls_overflow: usize,
}

#[derive(Default)]
struct TimingRing {
    records: VecDeque<TimingRecord>,
    stored: usize,
    evicted: usize,
    last_report: Option<Instant>,
}

pub(crate) struct TimingConsole {
    report_every: Duration,
    ring: Mutex<TimingRing>,
}

impl Default for TimingConsole {
    fn default() -> Self {
        Self::with_report_interval(TIMING_METRICS_REPORT_INTERVAL)
    }
}

impl TimingConsole {
    pub(crate) fn with_report_interval(report_every: Duration) -> Self {
        Self {
            report_every,
            ring: Mutex::new(TimingRing::default()),
        }
    }

    pub(crate) fn record(&self, record: TimingRecord) {
        let mut metrics = None;
        match self.ring.lock() {
            Ok(mut ring) => {
                ring.records.push_back(record);
                ring.stored = ring.stored.saturating_add(1);
                while ring.records.len() > TIMING_HISTORY_CAPACITY {
                    ring.records.pop_front();
                    ring.evicted = ring.evicted.saturating_add(1);
                }
                if ring
                    .last_report
                    .is_none_or(|last| last.elapsed() >= self.report_every)
                {
                    ring.last_report = Some(Instant::now());
                    metrics = Some((ring.stored, ring.evicted, ring.records.len()));
                }
            }
            Err(error) => tracing::warn!(
                error = %error,
                timing_id = %record.id,
                "could not store a timing console record"
            ),
        }
        if let Some((stored, evicted, held)) = metrics {
            tracing::info!(
                timing_records_stored = stored,
                timing_records_evicted = evicted,
                timing_records_held = held,
                "timing console metrics"
            );
        }
    }

    /// The viewer's own newest records, newest first. Records of other viewers and records
    /// without a viewer are never returned.
    pub(crate) fn recent_for_viewer(&self, did: &str, limit: usize) -> Vec<TimingRecord> {
        match self.ring.lock() {
            Ok(ring) => ring
                .records
                .iter()
                .rev()
                .filter(|record| record.viewer.as_deref() == Some(did))
                .take(limit)
                .cloned()
                .collect(),
            Err(error) => {
                tracing::warn!(
                    error = %error,
                    did = %did,
                    "could not read the timing console history"
                );
                Vec::new()
            }
        }
    }

    #[cfg(test)]
    pub(crate) fn held_count(&self) -> usize {
        self.ring
            .lock()
            .map(|ring| ring.records.len())
            .unwrap_or_default()
    }

    #[cfg(test)]
    pub(crate) fn evicted_count(&self) -> usize {
        self.ring
            .lock()
            .map(|ring| ring.evicted)
            .unwrap_or_default()
    }
}
