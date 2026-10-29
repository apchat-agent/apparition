# Lane av28 report

## Reply parent cards

Reply cards on the home timeline, feed timelines, profile posts, and notifications now show the parent post above the reply in a smaller, muted card. A connector joins the two; opening the parent card goes to its thread. If the parent is already the immediately previous timeline post, it is not repeated. Missing or unavailable parents get a short fallback line.

Parent records are fetched from local storage in one batch per page. Missing local records use the existing thread hydration path under the page deadline. Parent author identity comes from cached handles and profile records; a cache miss renders a short DID while the existing background profile refresh fills the cache.

## `/app` timing

I measured the `/app` router in-process using a local memory fixture containing 50 timeline posts, of which 25 are replies. Requests alternated between parent rendering enabled and a temporary measurement-only path with parent rendering skipped. Each result includes handling the request and collecting its response body. The values below are medians from 24 samples per mode:

| Fixture | Parent cards | Median |
| --- | ---: | ---: |
| 50 posts, 25 replies, parents rendered | 25 | 10.875 ms |
| Same 50 posts, parent rendering skipped | 0 | 9.984 ms |

The measured difference was 0.891 ms (about 8.9%). This is a local in-memory fixture measurement, not a production latency estimate. All author profiles were already cached for the measurement.

## Screenshot

Captured from the local fixture response with the repository stylesheet at 1440 × 1200. No live server was used.

![Local timeline fixture showing parent cards above replies](artifacts/av28-timeline.png)

## Hygiene fixes

`tests/snapshot_lock.rs` now uses a drop guard for its temporary fixture directory. This cleans up the directory on normal completion and during unwinding after a failed test, preventing the roughly 294 MB fixture from accumulating after failures.

The av22 flake involved concurrent SQLite cache creation and restart. The database path used only a nanosecond timestamp, which could collide for concurrent callers and make them open the same SQLite file; a deterministic constant-clock test reproduced one path for 32 callers. That led to intermittent `SQLITE_BUSY` errors. A process-local atomic sequence now makes generated paths unique. The cache writer also has a flush barrier so restart tests wait until queued writes are persisted before reopening the cache. No sleeps or retry loops were added.

## Verification

- `CARGO_NET_OFFLINE=true CARGO_BUILD_JOBS=4 nice -n 10 cargo test -- --test-threads=1`: passed. This included 52 library tests; av11 5; av22 7; av26 3; av27 5; m1 4; m2 5; m3 21 passed and 2 ignored; m4 11; m5 config 10; m5 read 22; m6 OAuth 76; OAuth protocol 13; OAuth DB split 2; snapshot lock 9; doc tests 0.
- av22 identity cache suite repeated 20 times serially: 20/20 runs passed (7 tests each).
- `deno test --allow-read tests/app_js_test.js`: 7/7 passed.
- `cargo clippy -- -D warnings`: passed.
- `cargo fmt --check`: passed.
- Parent rendering tests covered the parent card and connector, missing-parent fallback, adjacent-parent deduplication, one batched lookup for 25 replies, and nonblocking parent author identity refresh. A deliberate per-record-lookup mutant failed the batch lookup test.

## Commits

- `13ba014` red: specify parent context on timeline reply cards
- `7251306` green: render parent context above reply cards
- `ea92caa` red: guard batched parent lookups against N+1
- `801149a` green: batch parent post lookups per page
- `a1dd50f` red: cover nonblocking parent author identity refresh
- `c9d76a0` green: queue uncached parent authors for refresh
- `6a0798f` red: cover snapshot tempdir cleanup on unwind
- `50972cc` green: clean snapshot fixture directories on drop
- `1a166fd` red: synchronize av22 cache restarts with writes
- `5a8b261` green: add a barrier for av22 identity cache writes
- `377b8af` red: detect colliding av22 database paths
- `c1b786a` green: uniquify av22 SQLite cache paths
- `2a948b4` green: update stylesheet cache key assertions
- `4fe75a5` refactor: group timeline card render options

No commits were pushed.
