# Lane report av27b: SSE streams must not block shutdown

## Changes

- Added a shared web shutdown signal. Notification SSE streams select on it and finish promptly, releasing their per-viewer stream guards.
- On Ctrl-C or SIGTERM, the server signals the web streams and background workers. The server and worker drain share a five-second deadline; if it expires, the process logs `connections_cut` with the remaining open notification stream count and exits.
- Jetstream shutdown continues to use the shared worker shutdown watch. The existing idle-WebSocket regression test confirms that the consumer run future ends and the connection is released.
- Added a real local HTTP server test that opens an authenticated notification stream, triggers shutdown, checks the stream ends within one second, and checks the serve future completes.

## Red/green commits

- `0c4c5f3 red: keep notification stream open through shutdown`
- `ad13643 green: close streams and bound shutdown drain`

The red test connected successfully and received its initial event, then failed because the open stream did not end within one second after shutdown.

## Verification

- `cargo test --offline -- --test-threads=1`: 238 passed, 2 ignored.
- `deno test --allow-read tests/app_js_test.js`: 7 passed.
- `cargo clippy --offline -- -D warnings`: passed.
- `cargo fmt --check`: passed.
- Targeted shutdown regression test: passed after implementation.

No live server was touched. Nothing was pushed.
