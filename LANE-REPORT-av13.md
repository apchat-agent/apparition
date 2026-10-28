# Lane report av13

## Changes

- Added an embedded `/static/app.css` route with `text/css` content type and one-year immutable caching. Pages use the versioned `/static/app.css?v=av13` URL.
- Added a shared HTML shell with the viewport declaration, semantic navigation, skip link, and stylesheet link for the login, timeline, profile, thread, notifications, and sign-in error pages.
- Added a phone-first, centered layout with light and dark color schemes, visible keyboard focus, readable post cards and embeds, responsive images, and 44px navigation and action targets.
- Improved the login form labels and controls. Post cards now show a CDN avatar or placeholder, display name, handle, machine-readable timestamp plus visible relative time, engagement counts, and accessible reply/like/repost controls.
- Kept the CSP and remote-string HTML escaping behavior unchanged. Did not edit ingest, backfill, main, or storage code.

## Commits

- `126c6a2` — `red: require mobile styling on web pages`
- `f2ad8c7` — `red: cover error shell and post metadata`
- `80dd463` — `green: style the mobile web pages`

## Verification

- `cargo test --offline` — passed, 134 tests.
- `cargo clippy --offline -- -D warnings` — passed.
- `rustfmt --edition 2024 --check src/oauth.rs tests/m6_oauth.rs` — passed.
- `cargo fmt --check` — reports formatting differences in the pre-existing `src/ingest.rs` and `tests/snapshot_lock.rs`; both files are outside this lane's edit scope and were left unchanged.
- Screenshots were not produced.
- No live server was touched and nothing was pushed.
