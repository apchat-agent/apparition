# Lane report av20

Date: 2026-10-29

## Completed

- Page rendering now gathers quoted post references in page order, deduplicates them, and fetches missing posts through `OAuthFlow::fetch_post` (the production flow uses the existing `FeedClient` live-post cache). Up to 16 quotes fetch concurrently. Quote and author-profile hydration use one shared 1.5-second page deadline.
- Quoted posts render as escaped cards with avatar, name, truncated handle, relative time, text, and a real thread link. Unavailable, not found, and over-budget references have distinct linked cards. Nested quotes stop at one linked “Quoted post” line. Cached feed generators, lists, and starter packs render as linked type cards without live fetches. Record-with-media images remain above the quote card.
- The same hydration path is used on the home timeline, custom feeds, notifications, profiles, and thread pages. Tests cover quote order, deduplication, profile order, media, escaping, status cards, and shared-budget behavior.
- Thread ancestry now prefetches the known root and parent concurrently, then follows remaining parents through storage or the shared live-post path. A thread-page INFO event includes ancestor counts from the index, live fetches, timeouts, and total milliseconds.
- Post action rows use the requested reply, repost, like, share, and more order; zero counts are omitted and liked controls use the teal accent. Trailing `1/2` text receives a muted pill without changing its text.
- Fixed three `clippy::await_holding_lock` warnings in `tests/m3_ingest.rs` by using a Tokio mutex for async tracing tests and its blocking lock for the synchronous test. No lint allowances were added.

## Red runs

- `2527e39` failed because an unindexed quote produced no live-post request.
- `ad3d457` failed because the normal post action row lacked the requested order, share/more controls, liked state, and thread-number styling.
- `20325dd` failed the thread latency threshold at **611.497 ms**.

## Timing and verification

- Fake-PDS thread ancestry test, with 200 ms fetch delays: **420 ms** after the change (four-post chain; target in the local index).
- Quote and author profile each delayed 1,200 ms: **1.21 s** total, with both hydrated before the shared deadline.
- Over-budget quote placeholder test: **1.51 s** total.
- `cargo test --locked --offline -- --test-threads=1`: **187 passed, 0 failed, 2 ignored**.
- `cargo clippy --locked --offline -- -D warnings`: passed.
- `cargo clippy --all-targets --locked --offline -- -D warnings`: passed, including all three `m3_ingest` warnings.
- `cargo fmt --all -- --check`: passed.

No OAuth scope or SQLite index changed. The live server was not touched. All work is local; nothing was pushed.

## Commits

- `2527e39` red: require live quoted post cards
- `be7f3eb` green: fetch and render quoted post cards
- `ad3d457` red: require ordered action controls and thread pills
- `7c7b355` green: order post actions and style thread numbers
- `20325dd` red: require fast concurrent thread ancestors
- `3f3abc3` green: fetch thread roots and parents concurrently
- `ae99088` test: verify quote ordering and shared page budget
- `66b05c9` fix: clear all-target Clippy warnings
