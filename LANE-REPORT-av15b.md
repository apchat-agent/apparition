# Lane av15b report

## Findings and change

The per-event scope check called `registered_followers`, whose query filtered on `follows.subject`. The schema only had the primary key `(actor, subject)`, so SQLite scanned all follow rows for ordinary Jetstream events. On the synthetic database below, the pre-change plan was `SCAN f`; this was the measured bottleneck.

Added this startup migration after `migrate_follow_state` so legacy databases have the `following` column before index creation:

```sql
CREATE INDEX IF NOT EXISTS follows_active_subject_actor
ON follows(subject, actor) WHERE following=1;
```

The partial index covers only active follows. It serves both the `registered_followers` scope query and the `is_tracked_did` query used for identity/account events. The planner now searches this covering index. The existing record-key and registration lookups already use their primary-key indexes, so no tracked-DID cache or behavior change was needed.

Added per-batch DEBUG timings for JSON parse, scope/tracking work, record lookups, batch apply, SQLite transaction commit, and total duration. SQLite reports apply and commit separately. Each INFO metrics interval now includes batch count, average and maximum batch duration, and the phase with the largest accumulated time.

## Synthetic database and benchmark

The ignored test `jetstream_large_database_batch_ingest_benchmark` and helper `tests/support/synthetic_db.rs` create a persistent synthetic database at `target/av15b-synthetic.sqlite3`. It contains 2.4 million base rows:

- 1,000,000 records: 20,000 tracked posts, 780,000 untracked posts, and 200,000 likes/reposts by untracked authors pointing to tracked posts. 98% of record rows are by untracked authors.
- 1,200,000 follow rows across 200 registered accounts. There are 300 active target DIDs; historical inactive follows remain in the table.
- 200,000 notification rows.

The SQLite file occupied about 1.7 GiB on this machine. The benchmark sends 600 events in three batches of 200, including occasional interactions on tracked posts. Both throughput runs used the same generated database and event stream. The pre-change run took 21.306 s (28.2 events/s). With the index and phase logging, it took 9.337 ms (64,260 events/s), about 2,280× faster and above the 2,000 events/s target. The index-only run between those measurements reached 81,397 events/s; timing instrumentation adds some overhead.

The release benchmark’s final DEBUG phase samples were:

| Batch | Parse | Scope/tracking | Record lookup | Apply | Commit | Total |
| ---: | ---: | ---: | ---: | ---: | ---: | ---: |
| 1 | 0.122 ms | 1.856 ms | 0.003 ms | 3.969 ms | 0.189 ms | 6.154 ms |
| 2 | 0.093 ms | 1.285 ms | 0.003 ms | 0.064 ms | 0.113 ms | 1.569 ms |
| 3 | 0.091 ms | 1.279 ms | 0.003 ms | 0.056 ms | 0.069 ms | 1.510 ms |

The first batch includes initial SQLite write work; the following batches show scope/tracking as the largest phase.

Measured with the bundled SQLite build, creating the new index on 1.2 million follow rows took 54.139 ms. Opening the already-indexed database took 0.933 ms in the same run. A separate first open that installed the index took 54.354 ms overall. The live 5.9 GB database is on a slower HDD, so its startup cost may differ; no live server or database was accessed.

Run the benchmark with:

```text
APPVIEW_BENCH_MEASURE_FOLLOW_INDEX=1 cargo test --offline --release --test m3_ingest jetstream_large_database_batch_ingest_benchmark -- --ignored --nocapture
```

The environment variable makes the ignored test drop and recreate the new index to time its creation. Without it, the test only measures the current open and ingest path. The synthetic database is not committed.

## Per-event query plans

Plans below were collected with `EXPLAIN QUERY PLAN` against the baseline schema and the indexed schema. `SCAN CONSTANT ROW` is SQLite’s one-row expression evaluation, not a table scan. Before/after plans are unchanged for lookups other than those that filter follows by subject.

| Ingest query | Before index | After index | Use |
| --- | --- | --- | --- |
| `SELECT EXISTS(SELECT 1 FROM registered_users WHERE did=?1)` | `SCAN CONSTANT ROW`; scalar subquery; `SEARCH registered_users USING COVERING INDEX sqlite_autoindex_registered_users_1 (did=?)` | Same | Scoped registration lookup for each commit; also follow updates |
| `SELECT f.actor FROM follows f JOIN registered_users r ON r.did=f.actor WHERE f.subject=?1 AND f.following=1` | `SCAN f`; `SEARCH r USING COVERING INDEX sqlite_autoindex_registered_users_1 (did=?)` | `SEARCH f USING COVERING INDEX follows_active_subject_actor (subject=?)`; `SEARCH r USING COVERING INDEX sqlite_autoindex_registered_users_1 (did=?)` | Main scoped event lookup; this full-table scan caused the regression |
| `SELECT EXISTS(registered DID) OR EXISTS(registered follower)` (`is_tracked_did`) | `SCAN CONSTANT ROW`; registered-user PK search; second subquery `SCAN f`; registered-user PK search | `SCAN CONSTANT ROW`; registered-user PK search; second subquery `SEARCH f USING COVERING INDEX follows_active_subject_actor (subject=?)`; registered-user PK search | Identity/account events |
| `SELECT EXISTS(SELECT 1 FROM records WHERE did=?1)` | `SCAN CONSTANT ROW`; `SEARCH records USING COVERING INDEX sqlite_autoindex_records_1 (did=?)` | Same | Identity/account event handle-refresh check |
| `SELECT ... FROM records WHERE did=?1 AND collection=?2 AND rkey=?3` | `SEARCH records USING INDEX sqlite_autoindex_records_1 (did=? AND collection=? AND rkey=?)` | Same | Previous record lookup for deletes and follow updates only |

The DML statements executed while applying a batch were also checked. Their plans are the same before and after unless noted; VALUES-only INSERT/UPSERT statements return no `EXPLAIN QUERY PLAN` rows.

| Batch write query | Plan |
| --- | --- |
| Insert handle refresh queue row | VALUES-only; no plan rows |
| Upsert record | VALUES-only; conflict key is the records primary key; no plan rows |
| Delete notification edges by source DID/collection/rkey | `SEARCH notification_events USING INDEX sqlite_autoindex_notification_events_1 (source_did=? AND source_collection=? AND source_rkey=?)` |
| Insert notification edge | VALUES-only; conflict key is the notification-events primary key; no plan rows |
| Delete record by DID/collection/rkey | `SEARCH records USING INDEX sqlite_autoindex_records_1 (did=? AND collection=? AND rkey=?)` |
| Upsert follow | VALUES-only; conflict key is the follows primary key; no plan rows |
| Delete backfill scope row by owner/target | `SEARCH backfill_scope USING INDEX sqlite_autoindex_backfill_scope_1 (owner_did=? AND target_did=?)` |
| Insert backfill queue row with capacity/scope checks | Registered user, queue membership, and actor/subject follow checks use primary-key searches. `COUNT(*) FROM backfill_queue` scans the queue primary-key index; this queue is capped at 1,000 rows. This bounded scan remains. |
| Insert backfill scope row with queue/scope checks | Searches the queue primary key, registered-user primary key, and follows primary key `(actor,subject)` |
| Upsert backfill status with queue membership check | `SEARCH backfill_queue USING COVERING INDEX sqlite_autoindex_backfill_queue_1 (did=?)` |
| Upsert Jetstream cursor | VALUES-only; no plan rows |

The red plan test asserts both subject-filtered follow queries search an index and do not scan `follows`. It failed on the pre-change code with `plan=["SCAN f", "SEARCH r USING COVERING INDEX sqlite_autoindex_registered_users_1 (did=?)"]` and passes with the new index.

## Red/green commits

| Commit | Step |
| --- | --- |
| `3b4afed` | Add the multi-million-row generator and ignored release benchmark; capture the pre-change result. |
| `bdf1ce0` | Red: query-plan test fails on the follows table scan. |
| `9c4e4b9` | Green: create the partial follows index with `IF NOT EXISTS`; query-plan test passes. |
| `ec76b73` | Red: phase-log and interval-summary assertions fail because those fields/logs are absent. |
| `b7600c3` | Green: add phase timing and metrics interval summaries; the logging tests pass. |

## Verification

- Red query-plan assertion failed before the index change; it passes after.
- `RUST_TEST_THREADS=1 cargo test --offline` — passed: 153 tests, 2 ignored benchmarks.
- `cargo fmt --all -- --check` — passed.
- `cargo clippy --offline -- -D warnings` — passed.
- `cargo deny check licenses` — passed (`licenses ok`).
- Default-parallel runs intermittently hit the existing snapshot-lock test's 200 ms timing threshold under concurrent system load; the unchanged test passed in isolation and in the serial full-suite run.
- No live server was touched and nothing was pushed.
