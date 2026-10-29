# av32: grouped reply chains

## Implementation and choices

Timeline, saved feed, and profile post lists now render a linear reply chain as one block when at least two posts from it are on the page. Posts appear in reply order, oldest first, and the block stays at the newest page post's position. Known ancestors returned by parent hydration are included as full-size cards, so a chain root just outside the page can still lead the group.

The 24-hour window is measured from the oldest known ancestor through the newest post. Parent hydration uses the existing batched lookup, then at most two additional batches, one per ancestor level. If the chain remains unresolved at that depth, the renderer keeps the existing parent-card presentation rather than guessing its start time. Missing or deleted parents end the known chain. At a fork, replies remain separate because they no longer form one linear chain.

Same-author chains render in full through 10 posts. Longer chains show the first two and newest three, then link to the thread view. Mixed-author chains show the top post and newest two, with a link for skipped replies. Grouped page posts are suppressed from their original standalone positions.

When an infinite-scroll page overlaps an on-screen group, new posts join that group, duplicate posts are discarded, and the existing group stays attached. The “more” count is reduced as posts become visible. CSS and JS asset versions were bumped to invalidate cached styles and scripts.

## Regression commits

Each listed behavior has a failing test commit followed by a passing implementation commit:

| Behavior | Red | Green |
| --- | --- | --- |
| Group a three-post self-thread | `5cdf763` | `da11c57` |
| Cap a mixed-author chain at three cards | `0e8f6ca` | `235d337` |
| Enforce the 24-hour chain window, including ancestors outside the page | `0326d06` | `2cea59e` |
| Cap a long self-thread and link to the thread view | `f46807f` | `2da39b6` |
| Batch ancestor lookups and stop after two extra levels | `ac2e5d6` | `6feb15e` |
| Merge overlapping groups during infinite scroll | `12c10ff` | `5c4de37` |
| Include a hydrated root in a page-local group | `7f315cf` | `cc1da72` |
| Update the “more” count after a pagination merge | `e402bbf` | `5e9563f` |

Supporting commits updated legacy renderer assertions for the new grouping, refreshed asset versions, and resolved a Clippy warning: `cab3f6c`, `32daf31`, `61d9f9e`.

## Verification

- `nice -n 10 env CARGO_BUILD_JOBS=4 cargo test -- --test-threads=1` — 267 passed, 2 ignored.
- `deno test --allow-read=static/app.js tests/app_js_test.js` — 15 passed.
- `nice -n 10 env CARGO_BUILD_JOBS=4 cargo clippy --all-targets -- -D warnings` — passed.
- `cargo fmt --all -- --check` — passed.

## Local fixture screenshot

The screenshot was captured with Firefox headless from the local [fixture page](screenshots/av32-thread-fixture.html), using the repository stylesheet. It shows a mixed-author group with its root, newest replies, connectors, and “more replies” link.

![Grouped reply thread on the local timeline fixture](screenshots/av32-thread-group.png)
