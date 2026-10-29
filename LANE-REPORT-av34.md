# av34: more compact mobile bottom bar

## Headless Firefox capture

I captured the local fixture in headless Firefox 153.0.3 with a 390×844 CSS viewport and a simulated 34px bottom inset.
The fixture substitutes `34px` for `env(safe-area-inset-bottom)` while serving this repo's stylesheet. Firefox enforces
a 500px minimum headless outer window here, so the app page runs in a 390×844 iframe and the capture is clipped to that
frame. The fixture is local only and does not contact the live server.

| State | Bar height | Bar top | Compose bottom offset | Center bottom padding | Item width |
| --- | ---: | ---: | ---: | ---: | ---: |
| Before | 79.8px | 764.2px | 91.6px | 72px | 56px |
| After | 56.5px | 787.5px | 68.5px | 68.5px | 44px |

The bar is 23.3px shorter after the change. Both screenshots are 390×844 pixels:

**Before**

![Firefox screenshot before av34](screenshots/av34-browser-before.png)

**After**

![Firefox screenshot after av34](screenshots/av34-browser-after.png)

The reproducible fixture and capture driver are in [`artifacts/av34-fixture/`](artifacts/av34-fixture/).

## Changes

- Reduced mobile navigation icons from 1.3rem to 1.2rem, the profile avatar from 1.55rem to 1.4rem, and item height from
  2.6rem to 2.1rem. Each item keeps a 44px minimum width.
- Set bottom padding to `calc(0.1rem + env(safe-area-inset-bottom) * 0.55)`. The CSS comment explains that 55% of the
  iPhone's 34px inset leaves the icons clear of the home indicator.
- Derived the compose button offset from the bar height plus a 0.75rem gap. The center column reserves the larger of
  that clearance or the compose button's 3.5rem height plus the same gap, keeping the final content visible while
  scrolling.
- Bumped the stylesheet URL from `app.css?v=m14` to `app.css?v=m15`.

Red commit: `86d55ab` (`red: pin compact mobile bottom bar geometry`). The new CSS regression test failed there against
the old avatar size, then passed after the implementation.

## Verification

- `nice -n 10 env CARGO_BUILD_JOBS=4 cargo test --locked --offline -- --test-threads=1` — 275 passed, 2 ignored.
- `deno test --allow-read=static/app.js tests/app_js_test.js` — 17 passed.
- `nice -n 10 env CARGO_BUILD_JOBS=4 cargo clippy --locked --offline --all-targets -- -D warnings` — passed.
- `cargo fmt --all -- --check` — passed.
- Headless Firefox at 390×844 with the 34px inset substitution — bar height decreased from 79.8px to 56.5px; all five
  navigation targets measured at least 44px wide.
