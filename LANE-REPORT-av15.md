# Lane av15 report

## Changes

- Successful OAuth `createRecord` responses now return the PDS URI/CID and submitted record. Posts, likes, and reposts are written through to SQLite before redirecting. Successful `deleteRecord` responses remove the local like by URI. Jetstream replay updates the same record key and replaces its notification rows, so it does not duplicate records or notification counts.
- Jetstream batches up to 200 events or 250 ms into one SQLite transaction. Records, follow/scope updates, handle refresh requests, notifications, and the saved cursor commit together. A failed write rolls back the full batch and leaves the cursor unchanged. Batch planning applies earlier follow and record changes to later events in that batch.
- On reconnect, a cursor older than `jetstream_stale_cursor_threshold_secs` (default 600; TOML and `APPVIEW_JETSTREAM_STALE_CURSOR_THRESHOLD_SECS` configurable) moves to 30 seconds ago. The skipped `{from,to}` window is logged at WARN and retained in the `jetstream_skipped_windows` appview state entry, capped at the latest 128 windows.
- Every HTTP request now gets one INFO line with method, path, status, and duration in milliseconds. The path is taken without the query string. Requests over two seconds also get a WARN line. Cookie and query values are not logged.

## Red/green commits

| Commit | Step and observed red result |
| --- | --- |
| `4b9eb91` | Red: safe request timing logs. The targeted test failed because the middleware emitted no request log. |
| `406f069` | Green: add method/path/status/duration logging and the slow-request warning. |
| `e4d2d09` | Red: write-through and replay test. It failed because the successful fake PDS post was absent from the local index. |
| `21a1290` | Green: carry PDS create metadata through OAuth and index successful creates/deletes off the async runtime. |
| `06c031d` | Red: stale-cursor and atomic-batch tests. The reconnect URL retained the 30-minute-old cursor; a forced second-event failure left cursor `Some(100)` instead of rolling back to `None`. |
| `5c1e040` | Red: configurable catch-up threshold. The config test failed because the ten-minute setting was missing. |
| `faf1593` | Red: same-batch scope test. It failed because the post from a newly followed account was not indexed. |
| `9b70f1c` | Green: configurable stale-cursor recovery, transactional event batches, and sequential scope projection within a batch. |

Red runs used `cargo test --offline <test-name> -- --nocapture`; each failed on the behavior above before its green change. The write-through replay test now uses `SqliteStorage` and a fake OAuth/PDS flow.

## Throughput measurement

Command:

```text
cargo test --offline --release --test m3_ingest jetstream_sqlite_batch_throughput_benchmark -- --ignored --nocapture
```

The local optimized-build benchmark processed 5,000 SQLite post events. The pre-batch path used one transaction per event; the batched path used production-sized groups of 200:

| Path | Throughput | Elapsed |
| --- | ---: | ---: |
| One event per transaction | 7,916 events/s | 631.6 ms |
| Groups of 200 | 52,161 events/s | 95.9 ms |

That is 6.59× faster in this local benchmark. It isolates SQLite ingest on this machine and does not model appview-host’s slower HDD RAID or websocket/network costs.

## Verification

- `cargo test --offline` — passed: 151 tests, 1 ignored manual benchmark.
- `cargo fmt --all -- --check` — passed.
- `cargo clippy --offline -- -D warnings` — passed.
- No live server was accessed and nothing was pushed.

One intermediate full test run hit the existing timing-sensitive snapshot lock assertion; it passed on isolated rerun and in the final full run.
