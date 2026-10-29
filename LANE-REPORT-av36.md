# av36: profile page polish and production label batching test

## Changes

- Reworked the profile header with a banner image or plain fallback, overlapping avatar, display name, handle, “Follows you” pill,
  local follower/following/post counts, and linked description text.
- Reduced profile actions to one Follow/Following button and an overflow menu. The menu holds list navigation, mute/unmute, and
  block/unblock. Blocking opens a named confirmation dialog with JavaScript and a separate confirmation page without it.
- Replaced per-list buttons with one dropdown and Add/Remove action. Options show a check for current membership; the server
  reads current membership when choosing whether to create or delete the list item.
- Kept account-label chips, with muted colors and a subtle alert style.
- Reduced the mobile navigation row from 33.6px to 32px and its safe-area padding to one quarter of the inset, with a 4px
  minimum. At a simulated 34px inset, Firefox measured the navigation bar at 56.5px before and 44.7px after, a reduction of
  11.8px. The capture keeps the icons above the home-indicator area.
- Bumped the stylesheet and script asset versions to `m19` and `js15`.
- Added a production-fetch test with a fake HTTP server. It returns labels for three authors and verifies one `getProfiles`
  request contains all three actors and the `atproto-accept-labelers` header. The test failed with three requests under the
  `chunks(1)` mutant and passes with the configured batch size.

The profile layout follows the public [Bluesky profile](https://bsky.app/profile/bsky.app) shape.

## Firefox captures

Headless Firefox at 390×844 rendered a local fixture using this repository’s CSS and JavaScript. Its mock routes stay on
localhost; no running AppView or Bluesky service was contacted.

| State | Capture |
| --- | --- |
| Before | ![Profile before av36](screenshots/av36-profile-before.png) |
| After, following account | ![Profile after av36](screenshots/av36-profile-after.png) |
| Overflow menu open | ![Profile menu open](screenshots/av36-profile-menu.png) |
| Block confirmation open | ![Block confirmation](screenshots/av36-profile-block-confirm.png) |
| List dropdown with Add selection | ![Profile list selector](screenshots/av36-profile-lists.png) |

The fixture and capture driver are in [`artifacts/av36-fixture/`](artifacts/av36-fixture/).

## Red/green commits

- `840b127` — `red: specify av36 profile polish and mobile bar`
- `127e78d` — `red: catch production label profile fanout`
- `green: polish profile actions labels and mobile bar` — implementation and checks

## Verification

- `nice -n 10 env CARGO_BUILD_JOBS=4 cargo test --locked --offline -- --test-threads=1` — 290 passed, 2 ignored.
- `deno test --allow-read=static/app.js tests/app_js_test.js` — 17 passed.
- `nice -n 10 env CARGO_BUILD_JOBS=4 cargo clippy --locked --offline --all-targets -- -D warnings` — passed.
- `cargo fmt --all -- --check` — passed.
- Browser fixture confirmed the block dialog opens and submits in place, and choosing another list changes Add to Remove after
  the server response without navigating away.

No live server was touched. Nothing was pushed.
