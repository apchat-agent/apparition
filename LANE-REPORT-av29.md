# Lane av29 report

## Seamless infinite scroll

Timeline, feed, profile-post, and notification pages now progressively load the next server-rendered page from their existing `rel="next"` link. Search has no paginated route or Older link. The link remains the no-JavaScript fallback and supplies the URL used by the client.

The loader observes the middle card from the most recently appended page. On desktop it listens to the viewport; at the mobile breakpoint it uses `.app-layout`, the actual pinned-frame scroll container. A scroll-position check makes the boundary precise: 49% does not load and 51% starts one request. Requests are serialized, errors wait for an explicit Retry click, duplicate post URIs and duplicate av28 parent cards are removed, and the list stops at 500 retained items. If the next page would exceed that cap, the loader leaves Older pointing to that page for a full navigation.

The page shows “You’re all caught up” at the end. The existing document-level like handler works on appended cards; server-rendered image alt text and reply-parent connector markup arrive with the appended HTML. The notification badge remains page-level and continues using its existing SSE or polling behavior. The current UI has no separate JavaScript image lightbox to rebind.

Adaptive early loading was skipped. Back navigation relies on the browser’s normal back-forward cache, which preserves the loaded DOM and scroll position when used; serializing appended pages for a reconstructed history entry remains a follow-up.

## Red/green evidence

The first Deno red run failed with `ReferenceError: installInfiniteScroll is not defined`. The committed tests now cover the midpoint boundary, the pinned phone scroll root, a repeated observer callback during an in-flight request, ordered append and end-of-list behavior, URI and parent-card dedupe, retry without request storms, the 500-card cap, and like interaction on an appended card. The Rust parent-card test also checks the URI markers used by dedupe.

## Local fixture screenshots

Captured with Firefox 153.0.3 at a 500 × 759 viewport against static fixture pages served from this repository. The capture script scrolls the `.app-layout` container through the midpoint, waits for page two to append, then captures the bottom of the list. No AppView server or live service was used.

![Before scrolling: initial timeline cards in the pinned mobile frame](artifacts/av29-fixture/before.png)

![After scrolling: appended cards and the caught-up status](artifacts/av29-fixture/after.png)

Fixture files and the repeatable capture script are in [artifacts/av29-fixture](artifacts/av29-fixture/).

## Verification

- `CARGO_NET_OFFLINE=true CARGO_BUILD_JOBS=4 nice -n 10 cargo test -- --test-threads=1`: 245 passed, 2 ignored benchmark tests.
- `deno test --allow-read tests/app_js_test.js`: 11 passed.
- `CARGO_NET_OFFLINE=true CARGO_BUILD_JOBS=4 nice -n 10 cargo clippy -- -D warnings`: passed.
- `cargo fmt --check`: passed.

## Commits

- `04e6800` red: specify infinite scroll midpoint loading
- `153d50c` green: add midpoint infinite scroll loading
- `35dd6fc` red: catch duplicate posts and reply parents across pages
- `c91595c` red: expose post URIs for cross-page dedupe
- `8081bac` green: deduplicate appended post and parent cards
- `cf525de` red: bound infinite scroll memory and retry errors
- `15e1caf` green: cap infinite scroll at 500 cards
- `b0dec97` red: cover infinite scroll feedback styles
- `881648e` green: style infinite scroll status and retry controls
- `576b0fd` test: verify appended like cards stay interactive
- `3f2019d` red: keep successful retry controls hidden
- `bf02d01` green: hide retry button when there is no error
- `4c6ac2f` red: report actual card count at memory limit
- `6d82ad8` red: invalidate infinite scroll script cache
- `486d55c` green: report retained cards at the memory cap

No commits were pushed.
