# Lane report av26b

## Changes

- Restored the default request body cap to 32 KiB. Only `POST /post`, `POST /reply`, and `POST /quote` receive the 5 MiB cap; the limit wraps the streamed request body and rejects oversized `Content-Length` values before handlers run.
- Restored the login regression test to 40,000 bytes and added checks for a 40 KB `/like` form, a 1 MiB multipart body on `/post`, and a `/post` body over 5 MiB.
- Extended the real OAuth client and local mock PDS protocol tests. They record upload method, NSID path, content type, and bytes; assert that the returned blob is embedded in `createRecord`; and cover upload failure and a successful JSON response missing `blob.ref`.

The existing offline production OAuth/mock-PDS harness was sufficient; no new network harness was needed. No live server was touched and nothing was pushed.

## Red/green commits

- `50e906c red: assert route body limits and real PDS blob contract`
- `cb7b48a green: scope large request bodies to picture routes`

## Test evidence

Red runs against the old 5 MiB global cap:

- The 40 KB login test returned 400 instead of 413.
- The 40 KB `/like` test returned 303 instead of 413.
- The 1 MiB and over-5-MiB picture body checks passed under the old global cap.
- The malformed `blob.ref` test passed before the code change because the existing implementation already rejected that response.

Final verification:

- `cargo test --offline -- --test-threads=1`: 230 passed, 0 failed, 2 ignored.
- `deno test --allow-read=static/app.js tests/app_js_test.js`: 3 passed.
- `cargo clippy --offline --all-targets -- -D warnings`: passed.
- `cargo fmt --check`: passed.

Mutation checks, each reverted after the run:

- Changed `uploadBlob` to `uploadedBlob`: the real PDS upload test failed (502 instead of 303).
- Removed the `blob.ref` filter: the missing-ref test failed (303 instead of 502).
- Raised the default 32 KiB cap to 5 MiB: the `/like` body-limit test failed (303 instead of 413).
