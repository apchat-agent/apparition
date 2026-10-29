# appview-rs lane av23: authenticated personalised feeds

Date: 2026-10-29

## Completed

- Logged-in feed pages obtain the cached service-auth JWT for the viewer and generator before requesting the feed skeleton. Each page load makes one skeleton request with that JWT.
- If token retrieval fails, the page requests the skeleton anonymously; the existing once-per-feed warning remains. Logged-out pages continue directly to an anonymous skeleton request without calling `getServiceAuth`.
- Feed page and skeleton caches remain keyed by viewer, feed URI, and cursor. The existing feed timing log now records time spent waiting for service authentication in `auth_ms`.
- Added fake-PDS and fake-generator coverage for the authenticated three-post result, one JWT-bearing skeleton request, token reuse on another cursor, anonymous fallback and once-per-feed warning, and logged-out behavior.
- No live server was contacted. Nothing was pushed.

## Red run

- Base production revision: `96b02a3`.
- Test-only commit: `b36334b` (`red: authenticated feeds use viewer service JWT`).
- `cargo test --offline oauth::tests::authenticated_feed_uses_service_jwt_for_single_skeleton_request -- --nocapture` failed as expected: the page contained only `This feed requires authentication` instead of the three personalised posts.

## Verification

- `cargo test --offline -- --test-threads=1`: **207 passed, 0 failed, 2 ignored**. The two ignored tests are manual SQLite throughput benchmarks.
- `cargo clippy --offline --all-targets -- -D warnings`: passed.
- `cargo fmt --check`: passed.
- `grep -n "anonymous_skeleton" src/oauth.rs`: no matches; the logged-in path no longer requests an anonymous skeleton first.

## Commits

- `b36334b` red: authenticated feeds use viewer service JWT
- `d6cd8c6` green: authenticate feed skeleton requests
