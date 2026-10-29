# Lane report av37

## Delivered

- Replaced the separate picture and video controls with one “Add media” picker in new post, reply, and quote composers.
- Routed image selections through the existing resize, per-image alt text, and upload flow; routed a single video through the existing metadata validation and upload progress flow. The preview strip supports removal, shows image thumbnails, and extracts a video poster frame with its duration.
- Mixed selections, cross-type additions, extra videos, and media outside the granted OAuth scopes show inline feedback while preserving the previous selection.
- The no-JavaScript `/post` path now accepts native image parts and reads dimensions from supported image headers. Empty media parts are skipped, and a real video part returns “Enable JavaScript to validate and upload videos.”
- Kept the old-scope video gate, unchanged strict CSP, and bumped CSS/JS asset versions to `m20` and `js16`.

## Red/green commits

- Red: `e0caf34` — `red: specify unified composer media selection`
- Green: recorded in the commit containing this report.

## Verification

- `CARGO_BUILD_JOBS=4 nice -n 10 cargo test -- --test-threads=1` — passed, 293 Rust tests.
- `deno test --allow-read` — passed, 19 tests.
- `CARGO_BUILD_JOBS=4 nice -n 10 cargo clippy -- -D warnings` — passed.
- `cargo fmt --all --check` — passed.
- Headless Firefox via the local av37 fixture and geckodriver on port 4446, at 390x844:
  - Two images: `screenshots/av37-two-images.png`
  - Video poster and duration: `screenshots/av37-video.png`
  - Mixed selection message: `screenshots/av37-mixed-selection.png`

The screenshots were captured from the local fixture. The live server was not used, and nothing was pushed.
