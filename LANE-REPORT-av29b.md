# Lane av29b report

`appendCards()` no longer removes an earlier reply-parent card when a later page contains that URI as a full post. Incoming full-post and parent-card deduplication still applies; the parent stays in place and the full post is appended once.

## Red/green

- Red: `deno test --allow-read --filter 'incoming full post keeps' tests/app_js_test.js` failed because the earlier parent card was removed.
- Green: the same regression test passes after the fix.

## Verification

- `CARGO_NET_OFFLINE=true CARGO_BUILD_JOBS=4 nice -n 10 cargo test --locked -- --test-threads=1`: 245 passed, 2 ignored benchmark tests.
- `deno test --allow-read tests/app_js_test.js`: 12 passed.
- `CARGO_NET_OFFLINE=true CARGO_BUILD_JOBS=4 nice -n 10 cargo clippy --locked -- --deny warnings`: passed.
- `cargo fmt --check`: passed.

## Commits

- `1a48fbb` red: keep earlier reply parent cards on screen
- `1900eb1` green: preserve existing reply parent cards during pagination
