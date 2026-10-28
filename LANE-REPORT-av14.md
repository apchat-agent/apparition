# Lane av14 report

## Design

File-backed `SqliteStorage` now opens four query-only reader connections and selects them round-robin, separate from the single writer connection. Page/API reads therefore do not wait for the writer mutex while a backfill writes. `:memory:` databases keep the single-connection fallback because separate connections would not share that database.

Repository snapshots delete old records, notification events, follow edges, and backfill-scope entries in 500-row transactions, then insert snapshot records and follow edges in 500-row transactions. The configurable aggregate write budget (default 300 records/s) paces work between batches, and each batch releases the writer before the next one. Follow-state tombstones and rowid watermarks preserve live follow/unfollow updates that arrive during cleanup. OAuth callback/logout writes and backfill/identity worker writes run through `spawn_blocking`. Handle refresh requests that encounter a busy writer are held in a deferred in-memory set and merged into the worker's pending queue.

Repository backfill is disabled by default. `APPVIEW_BACKFILL_ENABLED=true` enables it, while the existing `APPVIEW_BACKFILL_PAUSED` switch still prevents startup of the worker. The optional JSON payload reduction was skipped: notification, interaction-summary, and other read paths consume the full record JSON, and compact payloads were not shown to preserve all existing API behavior.

## Red/green record

Red commits below record the failing steps; the green commits record the fixes. Related config failures were grouped before the green config change.

| Commit | Step |
| --- | --- |
| `420f3bf` | Red: page reads and an API runtime timer stall while the writer is held. |
| `bea84a7` | Green: add independent query-only reader connections. |
| `f6e6364` | Red: demonstrate a large repo snapshot exceeds the write-rate budget. |
| `2fb15b3` | Red: require a configurable, nonzero rate setting. |
| `2e1a65d` | Green: pace backfill batches using the configured rate. |
| `725f3e9` | Red: require backfill to be off by default. |
| `4426683` | Green: disable startup backfill unless explicitly enabled; preserve the pause override. |
| `63bad5f` | Red: OAuth callback storage blocks the current-thread runtime timer. |
| `2ce31e4` | Green: move callback session writes to blocking workers. |
| `d64bbb6` | Red: logout storage blocks the current-thread runtime timer. |
| `25f1fd2` | Green: move logout/session cleanup writes to blocking workers. |
| `aaea884` | Red: a session write waits behind a large snapshot cleanup. |
| `dab4937` | Green: chunk snapshot cleanup, offload backfill/identity storage writes, and defer busy handle-refresh queue writes. |
| `3816ecf` | Red: a session write waits behind rebuilding 100,000 follow edges. |
| `91099e6` | Green: batch follow/scope cleanup and insertion while retaining concurrent live follow changes. |

Observed pre-fix red results included page reads waiting about 864 ms, an API timer firing after about 962 ms, an unpaced 1,000-record snapshot completing in about 41 ms, callback/logout timers delayed about 921/893 ms, a session write waiting about 728 ms during record cleanup, and about 892 ms during follow rebuilding. These tests failed on the relevant pre-fix code and pass with the changes.

## Verification

- `cargo test --offline` — passed (all unit, integration, and doc tests).
- `cargo fmt --all -- --check` — passed.
- `cargo clippy --offline -- -D warnings` — passed.
- No live server was accessed and no changes were pushed.

## Remaining limits

- The four-connection read pool can make readers wait for a reader slot under more than four simultaneous long-running reads; reads do not share the writer lock.
- Deferred handle refresh requests are in memory until the worker persists/completes them, so a process crash in that interval can lose those requests; the periodic refresh scan can rediscover stale handles.
