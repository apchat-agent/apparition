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

The browser fixture and capture driver are in [`artifacts/av33-fixture/`](artifacts/av33-fixture/). The screenshot below
will be updated with the post-fix Firefox run.

## Changes

Pending.

## Verification

Pending.
