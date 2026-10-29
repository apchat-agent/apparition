# appview-rs lane av24: like heart and feed author hydration

Date: 2026-10-29

## Completed

- The like handler now toggles `is-liked` from the response state in both directions, keeping the visible heart in sync without reloading. The no-JS form fallback is unchanged. The app script URL was bumped to `js4` to invalidate cached copies.
- Feed page profile hydration now waits for its existing `PAGE_PROFILE_BUDGET` deadline. Authors still unresolved at the deadline use the existing fallback and remain queued for the profile refresh worker. Profile and quote hydration still run concurrently, and the profile fetch concurrency remains 8.
- `profile_hydration_ms` reports the time spent waiting. Fake-backend coverage verifies ten 60 ms profile lookups render with all display names, fresh indexed profiles trigger no fetch and little wait, and delayed profiles render fallback names within budget before a refresh worker stores them.
- No live server was contacted. Nothing was pushed.

## Red runs

- Like heart: test-only commit `7b4298e` (`red: like handler toggles filled heart`). `deno test --allow-read=static/app.js tests/app_js_test.js` failed because a successful like did not add `is-liked`.
- Feed authors: test-only commit `7ac090d` (`red: feed waits for new author profiles`). `cargo test --offline --test m6_oauth app_timeline_waits_for_new_author_profiles_before_rendering -- --nocapture` failed with fallback DIDs instead of the ten hydrated display names.

## Verification

- `cargo test --offline -- --test-threads=1`: **208 passed, 0 failed, 2 ignored**. The ignored tests are manual SQLite throughput benchmarks.
- `cargo clippy --offline --all-targets -- -D warnings`: passed.
- `cargo fmt --check`: passed.
- `deno test --allow-read=static/app.js tests/app_js_test.js`: **1 passed**. Deno is the JS test harness; the test uses local DOM and fetch fakes with no network dependency.
- `rg -n 'from_millis\(25\)' src/oauth.rs`: no matches.
- `static/app.js` toggles `is-liked` from `state.liked`.

## Commits

- `7b4298e` red: like handler toggles filled heart
- `8bcd326` green: toggle filled heart after like
- `7ac090d` red: feed waits for new author profiles
- `46539f6` green: await profile hydration before rendering
- `eccd892` test: expect bumped app script cache key
