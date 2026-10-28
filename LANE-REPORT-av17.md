# Lane av17 report: in-memory tracked DID counters

## Change

`SqliteStorage::open` now builds a `TrackedSet` from registered users and active follows, then shares it through `Arc<RwLock<_>>`. It contains the registered DID set and a `HashMap<DID, u32>` count of registered users following each DID. Startup logs the number of registered users, followed DIDs, active follow edges, and the build time.

Scoped ingest lookups now read this table instead of querying SQLite for registered followers. Follow changes staged earlier in a Jetstream batch are applied as per-DID deltas for later events in that batch. SQLite batch changes update the table only after commit; register/logout, direct follow changes, and repository snapshot follow cleanup/refill also update it after their corresponding database transaction commits. Zero-count DIDs are removed, and decrements clamp at zero.

`SqliteStorage::tracked_set_matches_sqlite` rebuilds the set from SQLite and compares it with the live table. The seeded operation test invokes it after every registration, unregistration, follow, and injected rollback.

## Synthetic benchmark

Ran the ignored av15b release benchmark against its synthetic 1.7 GiB database and the same 600-event stream in three batches of 200. The database contains 1,000,000 records, 1,200,000 follow rows, and 200,000 notifications.

| Measurement | av15b baseline | av17 |
| --- | ---: | ---: |
| Ingest throughput | 64,260 events/s | 131,130 events/s |
| `SqliteStorage::open` | 0.933 ms for an already-indexed DB | 10.372 ms |
| Tracked-set build | — | 9.402 ms |

The startup build loaded 200 registered users, 300 followed DIDs, and 60,000 active registered follow edges. The new ingest run was about 2.04× the av15b instrumented baseline. The one-time tracker build accounts for most of the additional startup time. The benchmark completed successfully; no live server or database was accessed.

Run it with:

```text
cargo test --offline --release --test m3_ingest jetstream_large_database_batch_ingest_benchmark -- --ignored --nocapture
```

## Red/green commits

| Commit | Step |
| --- | --- |
| `57b523c` | Red: add two-follower count, rollback, and seeded rebuild-equality tests. Before the implementation, the test target failed to compile because the tracked-count and rebuild-comparison APIs did not exist. |
| `edd25c5` | Green: build and incrementally maintain the shared tracked set; use it in scoped ingest; keep same-batch follow staging and rollback behavior. |

## Verification

- `RUST_TEST_THREADS=1 cargo test --offline` — passed: 165 tests, 2 ignored benchmarks.
- `cargo clippy --offline -- -D warnings` — passed.
- `cargo fmt --all -- --check` — passed.
- `cargo test --offline --release --test m3_ingest jetstream_large_database_batch_ingest_benchmark -- --ignored --nocapture` — passed; throughput and startup measurements above.
- The existing same-batch follow-scope test and SQLite rollback/cursor test remain green.

No changes were made outside this repository, and nothing was pushed.
