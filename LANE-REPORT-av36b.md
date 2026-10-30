# Lane report av36b: profile header layout

## Delivered

- Reworked the profile header so its banner spans the profile content width, the roughly 90px avatar overlaps the banner edge,
  and the right-aligned Follow/Following and overflow controls sit below the banner beside the avatar's lower half.
- Moved the display name, handle, “Follows you” pill, and label chips below the avatar. Description, counts, and the list selector
  now follow in that order. Long display names wrap without crossing the action row.
- Kept the no-banner fallback, overflow menu, block dialog, no-JavaScript block confirmation page, list Add/Remove controls, and
  account-label chips working.
- The browser fixture now writes pages using the same production profile-content and page-shell renderers. Its capture driver
  regenerates them before each run, substitutes only a local banner URL, and checks the banner, avatar, actions, heading, and
  horizontal overflow using `getBoundingClientRect()` at both required viewports.
- Bumped the stylesheet URL from `m20` to `m21`.

## Red/green commits

- Red: `6fb7f75` — `red: specify profile header geometry`. The geometry check failed on the old layout because the display name
  and action row overlapped the banner.
- Green: implementation and verification recorded in the commit containing this report.

## Firefox captures at 390×844

| State | Capture |
| --- | --- |
| Following account | ![Following profile](screenshots/av36b-profile-following.png) |
| No banner | ![Profile with fallback banner color](screenshots/av36b-profile-no-banner.png) |
| Long display name | ![Profile with wrapped display name](screenshots/av36b-profile-long-name.png) |
| Overflow menu open | ![Profile overflow menu](screenshots/av36b-profile-menu.png) |
| List dropdown with Add selection | ![Profile list dropdown](screenshots/av36b-profile-lists.png) |

The driver also opened and submitted the block dialog, confirmed the updated Unblock action, exercised the list Add/Remove flow,
and submitted the generated block-confirmation form with JavaScript disabled.

## Geometry results

- At 390×844, the banner measured 390×150px, the avatar measured 89.6×89.6px and crossed the banner bottom, and the action row
  began 5.6px below the banner bottom.
- At 1280×900, the profile content banner measured 598×224px, the avatar measured 89.6×89.6px and crossed the banner bottom,
  and the action row began 5.6px below it.
- The following, no-banner, and long-name profiles passed those checks at both sizes. All six had no horizontal overflow. The
  long name wrapped and remained below the action buttons.

## Verification

- `nice -n 10 env CARGO_BUILD_JOBS=4 cargo test --locked --offline -- --test-threads=1` — passed, 292 passed and 2 ignored.
- `deno test --allow-read` — passed, 19 tests.
- `nice -n 10 env CARGO_BUILD_JOBS=4 cargo clippy --locked --offline --all-targets -- -D warnings` — passed.
- `cargo fmt --all -- --check` — passed.
- Headless Firefox with geckodriver on port 4446 — passed at 390×844 and 1280×900. The fixture stayed on localhost; no live
  AppView server was touched.

Nothing was pushed.
