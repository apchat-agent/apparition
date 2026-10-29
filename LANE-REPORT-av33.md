# av33: timeline videos and deferred quoted posts

## Before-fix Firefox reproduction

I used Firefox 153.0.3 in headless mode with geckodriver on port 4446. The local fixture server loaded this repo's real
`static/app.js`, `static/app.css`, and bundled `hls.min.js`; its response policy matched `src/http_security.rs`. It used
the public playlist `https://video.bsky.app/watch/did%3Aplc%3Az72i7hdynmk6r22z27h6tvur/bafkreiew3laj2to7km3kli6g5dmb7xbqygqa7otzirszqvnousjallw6eq/playlist.m3u8`.
The test clicked each target video and sampled it three seconds later.

| Firefox case | `readyState` | `currentTime` | Error code | Source / player state |
| --- | ---: | ---: | --- | --- |
| (a) initial timeline video | 0 | 0 s | none | HLS.js attached a blob URL (`data-video-ready=true`) |
| (b) video appended by `appendCards` | 0 | 0 s | none | no `src`, no HLS attachment (`data-video-ready` absent) |
| (c) video in av32 thread group | 0 | 0 s | none | HLS.js attached a blob URL |
| (c) video in quoted post under `recordWithMedia` | 0 | 0 s | none | HLS.js attached a blob URL |

The page's `securitypolicyviolation` listener recorded `connect-src` failures while HLS.js fetched `.ts` segments for
(a) and both videos in (c). No page JavaScript errors or `console.error`/`console.warn` messages were recorded. Case (b)
had no CSP event because the appended video never received a source. An HTTP HEAD to one of the segment URLs showed
`video.bsky.app` responding with a redirect to `https://video.cdn.bsky.app/...`; the current CSP only allowed the
original `video.bsky.app` host, so Firefox rejected the redirected segment request.

The browser fixture and capture driver are in [`artifacts/av33-fixture/`](artifacts/av33-fixture/).

## After-fix Firefox reproduction

The same Firefox 153.0.3 run and public playlist played every target. Each video was clicked and sampled three seconds
later. The grouped and quoted-post videos are both part of case (c).

| Firefox case | `readyState` | `currentTime` | Error code |
| --- | ---: | ---: | --- |
| (a) initial timeline video | 4 | 2.87 s | none |
| (b) video appended by `appendCards` | 4 | 3.55 s | none |
| (c) video in av32 thread group | 4 | 3.52 s | none |
| (c) video in quoted post under `recordWithMedia` | 4 | 3.75 s | none |

The page console/error listeners and `securitypolicyviolation` listener recorded no errors or CSP violations in (a), (b),
or (c). Safari's native HLS path was not run in a Safari browser; the Deno regression test sets
`canPlayType("application/vnd.apple.mpegurl")` to `"maybe"` and verifies that an appended card receives the playlist URL.

![Firefox playing the fixture video after the fix](screenshots/av33-browser-after.png)

## Changes

- Added `https://video.cdn.bsky.app` to the CSP image, media, and connection sources. The public playlist host redirects
  media segment requests to that CDN host, which Firefox had rejected.
- Added one idempotent video initializer for initial page content and cards inserted by infinite scroll. It uses native
  HLS when `canPlayType` reports support, and HLS.js otherwise. New posts merged into an existing thread group are also
  initialized.
- Included page posts and all hydrated parent/ancestor records in the same deduplicated quote hydration pass. Profile
  hydration remains scoped to the page records, preserving the existing page deadline behavior.
- Replaced the bare pending quote link with a loading card and thread link. The client fetches a same-origin fragment;
  the authenticated `/quote-fragment` endpoint fetches and renders the post card. Failed fragment requests leave a
  useful unavailable/thread link. Nested quote links include a verified author handle when one is known.
- Added regression tests for appended native-HLS videos, CDN CSP, thread-group ancestor quote hydration and fetch counts,
  pending quote placeholders, nested quote handles, client fragment replacement, and the fragment response.

Red commits: `eb3c6be` (video, CSP, and quote regressions) and `4169ba1` (client quote fill regression). The green
implementation and updated report are committed after those tests.

## Verification

- `nice -n 10 env CARGO_BUILD_JOBS=4 cargo test --locked --offline -- --test-threads=1` — 274 passed, 2 ignored.
- `deno test --allow-read=static/app.js tests/app_js_test.js` — 17 passed.
- `nice -n 10 env CARGO_BUILD_JOBS=4 cargo clippy --locked --offline --all-targets -- -D warnings` — passed.
- `cargo fmt --all -- --check` — passed.
- Headless Firefox reproduction — all four videos reached `readyState 4` and `currentTime > 0`; no CSP or page errors.
