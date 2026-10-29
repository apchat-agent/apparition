# Lane report av19

Date: 2026-10-29

## Completed

- Quote reposts now use the ATProto record embed strong reference, appear in the local index after a successful PDS write, and create one quote notification for the quoted author. The composer previews the quoted post, and the repost control offers both “Repost” and “Quote post”. This also works when the selected post is itself a quote.
- `quoteCount` is read from indexed embedded record references. SQLite creates `records_collection_embed_record_uri` with `IF NOT EXISTS`.
- Page rendering hydrates missing or stale profiles from each author’s resolved PDS with guarded requests. It deduplicates authors in visible order, runs at most eight concurrent requests under a 1.5 second page budget, caches negative results for an hour, queues timed-out profile and handle lookups in page order, and refreshes queued profiles in the background. The in-memory DID profile index is built at startup, logged with its count and build time, and updated by record writes, Jetstream batches, snapshots, deletes, and live fetches.
- The thread page now has a focused large post, formatted date and reply policy, nonzero statistics, action counts, same-author continuation lines, a sticky reply bar, and mobile navigation matching the supplied sample. The teal-green accent remains `#087e8b`; the stylesheet cache key is now `av19`.

## Red runs

- `95a8d59` failed because indexed quotes were reported as `quoteCount: 0`.
- `5d8526c` failed because the fake PDS had no quote action and received no record embed.
- `2adb7da` failed because the timeline had no “Quote post” action or quote composer.
- `65a4887` failed because a page with a missing profile rendered only the DID fallback and did not fetch the profile.
- `a365d2a` failed because the thread page had no focused `thread-topbar` layout.
- `448df82` failed because the thread mobile navigation had no search tab.

## Quote index measurement

Measured with:

```sh
APPVIEW_BENCH_MEASURE_QUOTE_INDEX=1 cargo test --locked --offline --release --test m3_ingest jetstream_large_database_batch_ingest_benchmark -- --ignored --nocapture
```

On `target/av15b-synthetic.sqlite3` (`generated=false`, 1,000,006 records, 1,200,000 follows, 200,006 notifications), creating `records_collection_embed_record_uri` took **3,640.546 ms**.

## Verification

- `cargo test --locked --offline -- --test-threads=1`: **181 passed, 0 failed, 2 ignored**.
- `cargo clippy --locked --offline -- -D warnings`: passed.
- `cargo fmt --all -- --check`: passed.
- `cargo deny check licenses`: passed.
- Tests use local fakes and synthetic data. No live server was touched.

## Commits

All commits are local; nothing was pushed.

- `95a8d59` red: require indexed quote counts
- `74e4b4d` green: index and count quoted posts
- `5d8526c` red: require quote record embed strong refs
- `4812726` green: create quote posts with record embeds
- `2adb7da` red: require quote composer and notification write-through
- `96a0911` green: add quote composer and repost menu
- `65a4887` red: require live profile hydration on page render
- `8ea7ba4` green: fetch and cache missing page profiles
- `a365d2a` red: require mobile focused thread layout
- `e13223d` green: restyle thread focus and reply actions
- `448df82` red: require thread mobile navigation tabs
- `a116ccd` green: match thread mobile navigation to sample
- `ebf2d8a` test: verify profile index and live PDS hydration
- `efb4f9e` refactor: keep clippy warning free
