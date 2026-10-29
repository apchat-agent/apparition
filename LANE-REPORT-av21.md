# Lane report av21

Date: 2026-10-29

## Completed

- `/app` now renders pinned feed tabs from the saved URI list and cached generator metadata. An unresolved feed uses a title-cased rkey fallback and resolves in the background with per-URI deduplication and concurrency capped at eight. Successful metadata persists in the OAuth SQLite store for 24 hours and refreshes in the background after an hour; failures persist for an hour. Saved-feed preferences use the same stale-while-refresh approach.
- Feed skeleton/page responses are cached for 60 seconds by viewer, feed URI, and cursor. A repeated page is served from the page cache. When a cold skeleton request starts, service-token refresh runs alongside the anonymous skeleton request; post hydration follows once the skeleton returns.
- Profile hydration runs in a background task. The page waits at most 25 ms for it, while quote hydration can continue within the existing shared 1.5-second page deadline. The first response can show a fallback author name until a later request sees the saved profile.
- HTML, CSS, JavaScript, and JSON responses negotiate gzip or Brotli through `tower-http`. Each selected web page emits one structured `web page phases` INFO event with phase durations.
- The page shell includes one shared SVG symbol sprite and action rows reference its symbols. Avatar markup uses the CDN thumbnail path, explicit dimensions, async decoding, and eager loading for the first six top-level posts; later posts load lazily. The stylesheet cache key is now `m5`; JavaScript is unchanged at `js3`.
- The iPhone fixed body and bottom navigation shell remain in place. No OAuth scope changed.

## Red runs

- `8c987b5`: five delayed feed-generator resolutions held `/app` for **5.006 s**.
- `c1e53b0`: a delayed author-profile fetch held `/app` for **1.006 s**.
- `7f98df8`: the repeated custom-feed request still took **2.005 s** with a delayed fake generator.
- `a883c75`: compression negotiation failed because the response had no `Content-Encoding` header.
- `20cfe58`: the page had no shared icon sprite, and avatar markup lacked thumbnail URLs, dimensions, and the first-six loading policy.

## Measurements and findings

- With 38 pinned feeds and five 5-second fake resolutions, `/app` now returns in the test's **<200 ms** budget with all 38 tabs. The five unresolved entries render fallback labels. Background resolution persists results so a new `FeedClient` on the same database starts warm.
- With a 1-second fake author-profile delay, `/app` now returns in **<200 ms**; profile storage completes in the background. The old warm-load outliers came from profile hydration on the render path. The `/app` handler verifies a local session cookie and reads `timeline_posts` from local storage; it does not refresh OAuth tokens or call `getTimeline` on that route. Quote hydration remains separately timed and can still use the page deadline.
- The live custom-feed observation was **4.080–4.328 s** on a cold request. The delayed fake test measured **2.005 s** for the old repeated request; with the 60-second page cache, the second request is required to complete in **<100 ms**. Cold feed requests still wait for skeleton retrieval and post hydration.
- The 50-post `/app` gzip body, with one repeated author avatar reference, measured **4,936 bytes before** and **4,850 bytes after** the sprite/avatar changes, a reduction of **86 bytes (1.74%)** for the same fixture. This is well below the pre-registered **61,440-byte** budget. The test measures the compressed HTML response; the referenced CDN image bytes are separate network resources. Gzip HTML and Brotli JSON negotiation are both covered by tests.
- The per-page timing event reports `session_token_refresh_ms`, `preferences_ms`, `tabs_ms`, `timeline_fetch_index_ms`, `timeline_fetch_live_ms`, `profile_hydration_ms`, `quote_hydration_ms`, `render_ms`, and `total_ms`. Thread pages also report indexed/live/timed-out ancestor counts.
- The `web_cache` table is created in the OAuth database alongside OAuth session tables; this migration does not access the records database or add an index to it. A fresh local SQLite database measured **1 ms** for the cache-schema migration.

## Verification

- `cargo test --offline -- --test-threads=1`: **194 passed, 0 failed, 2 ignored**.
- `cargo clippy --offline --all-targets -- -D warnings`: passed.
- `cargo fmt --check`: passed.
- No live server was touched. No changes were pushed.

## Commits

- `8c987b5` red: keep app tabs off slow feed resolution
- `4bbace9` green: render pinned tabs from persistent metadata
- `c1e53b0` red: keep app render ahead of profile hydration
- `b7cc74d` green: move page profile hydration off the render path
- `7f98df8` red: check repeated feed page cache latency
- `a883c75` red: verify gzip and brotli negotiation
- `c912a69` green: cache feed pages and compress responses
- `20cfe58` red: assert smaller repeated icons and avatar payloads
- `fd68e6f` test: register compressed app page weight budget
- `83ab430` green: reduce repeated page icon and avatar payloads
- `69bccd1` test: include avatars in page weight budget
