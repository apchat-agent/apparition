# Lane av16: custom feeds

## Implementation

- Added viewer preference parsing for `savedFeedsPrefV2`, retaining unknown item types and preserving item order. V2 takes precedence over the legacy preference; the legacy pinned/saved lists remain readable. The OAuth session requests raw JSON so an unknown future preference union cannot make Atrium reject the entire response. Parsed preferences are cached per viewer for five minutes.
- Added feed generator lookup from the creator's `app.bsky.feed.generator` record, followed by DID-document resolution of the `#bsky_fg` `BskyFeedGenerator` service. Supports did:plc and did:web records, reports a clear missing-service error, checks the endpoint with `SafeHttpClient`, and caches generator metadata for one hour.
- Added `getFeedSkeleton` requests with `feed`, `limit=30`, and cursor parameters. The viewer's OAuth session requests a service JWT with the generator service DID as `aud` and `app.bsky.feed.getFeedSkeleton` as `lxm`. If that request fails, skeleton retrieval continues without authorization and emits one warning per feed. The JWT is never included in logs.
- Hydrates in skeleton order, preferring indexed posts and fetching missing records from the author's PDS through the SSRF guard. Live records use a 256-entry, ten-minute cache and are not written to the main index. Hydration is limited to eight concurrent lookups and an eight-second page budget; failures and timeouts are counted. Repost reasons include a resolved reposter when available.
- Added authenticated `/feeds` and `/app/feed?uri=...` pages, the Feeds navigation link, escaped feed-generator text and URIs, feed avatar blob thumbnails, and cursor-based Load more links. Following remains the default `/app` timeline. `ingest.rs` was not changed.

## Red/green commits

| Red commit | Failing evidence | Green commit |
| --- | --- | --- |
| `257b12e` | `cargo test --offline saved_feed_preferences -- --nocapture`: both new tests failed because parsing returned zero items instead of five V2 items or two legacy items. | `28e383f` |
| `7d72a45` | `cargo test --offline generator_ -- --nocapture`: feed resolution returned `feed generator resolution is not implemented`; did:web/did:plc lookup, missing service, and blocked endpoint checks failed. | `0663b4b` |
| `040adf3` | `cargo test --offline skeleton_request_ -- --nocapture`: failed with `feed skeleton requests are not implemented`. | `80fa7e2` |
| `197f640` | `cargo test --offline feed_hydration_keeps_skeleton_order -- --nocapture`: failed with `feed hydration is not implemented`. | `d262034` |
| `4de86e6` | `cargo test --offline pinned_feed_list_and_feed_timeline_routes -- --nocapture`: `/feeds` returned 404 instead of 200. | `ef2d64f` |
| `88b0e66` | `cargo test --offline generator_text_and_uri_are_escaped -- --nocapture`: the test found an unescaped `<script>` in feed generator output. | `ef2d64f` |
| `761cc20` | `cargo test --offline feed_generator_blob_avatar -- --nocapture`: blob avatar conversion returned `None` instead of a feed thumbnail URL. | `c6a528d` |

The skeleton test captures tracing output while sending a sentinel service JWT and confirms the value is absent from logs; it also verifies the bearer header, endpoint path, cursor, and limit. The hydration test mixes indexed and live posts, includes a missing post, and asserts preserved order and the dropped count. The feed HTML test exercises both the saved-feed card and feed-page heading.

## Verification

- `RUST_TEST_THREADS=1 cargo test --offline` — passed: 160 tests, one ignored local benchmark.
- `cargo fmt --all -- --check` — passed.
- `cargo clippy --offline -- -D warnings` — passed.
- Tests use a local fake PDS, fake identity documents, local HTTP, and in-memory storage. No live server was accessed and nothing was pushed. Build artifacts stayed under this worktree's `target/`.
- Two default-parallel full-suite runs each hit one of the timing-sensitive av14 snapshot lock assertions (220 ms and 211 ms against their threshold). The removal assertion passed isolated; the full suite passed with test threads serialized.

## Not done

- List-type saved feeds and timeline-type saved feeds are retained by the parser but not displayed.
- Live-fetched posts are not written back to the main index.
- The default Following timeline and other XRPC feed endpoints were not changed.
