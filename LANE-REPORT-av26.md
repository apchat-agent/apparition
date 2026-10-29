# appview-rs lane av26: attach pictures to posts

Date: 2026-10-29

## Completed

- Added the OAuth blob permission `blob:image/*` while retaining every existing permission. The scope uses the blob permission's MIME-type `accept` value described by the [AT Protocol permission spec](https://atproto.com/specs/permission).
- New browser sessions record whether the PDS granted that scope. Old cookies default to no picture permission. The main composer, quote composer, and reply composer show a `Log in again to enable pictures` link instead of the image picker when the grant is missing; their text forms remain available.
- Added a multiple-image picker for up to four images, previews, removal buttons, and optional alt text capped at 2,000 characters. Before a JavaScript-enabled submit, each image is decoded and drawn onto a fresh canvas, then encoded as JPEG with quality and size reduction until it is at most 950,000 bytes and its long side is at most 2,000 pixels. Re-encoding strips EXIF, including GPS location; this privacy property is documented beside the canvas code. The image element and canvas path also handles iPhone Safari's decoded HEIC images.
- Post, reply, and quote forms accept multipart, and URL-encoded text-only posts still work. The server checks same-origin and write access, refuses images without the blob grant, caps protected web requests at 5 MiB, allows only JPEG/PNG/WebP/GIF with matching file signatures, limits each image to 1,000,000 bytes, and validates alt text and dimensions before upload.
- Images upload sequentially through the viewer's OAuth/DPoP PDS session. A failed upload stops before `createRecord`, so no partial post is created. Image posts use `app.bsky.embed.images`; quote-plus-image posts use `app.bsky.embed.recordWithMedia`.

## Red/green commits

- `29cf5ab` `red: require picture upload permission handling` — old-scope composer lacked the re-login hint. `5bc835f` `green: request image blob permission` — added the requested scope, session grant tracking, and old-session hint.
- `4ca9c54` `red: require client image resizing` — Deno could not find the canvas resize function. `774f46a` `green: resize composer images before upload` — added the bounded JPEG canvas path and composer client handling.
- `16d8626` `red: require multipart image validation` — multipart forms returned 415. `dec6611` `red: require PDS image uploads and embeds` — fake-PDS image post, upload failure, and quote-image requests also returned 415. `3766205` `green: support image posts through OAuth PDS` — added multipart parsing, validation, uploads, post embeds, and composer controls.
- `7f65f5a` `red: allow bounded picture request bodies` — a 4 MiB protected web request was rejected with 413 by the old 32 KiB outer limit. `bbf43f8` `green: bound web requests at five mebibytes` — kept the 32 KiB default on other protected routers and raised the web router cap to 5 MiB.

## Verification

- Red runs were recorded before their green changes.
- `cargo test --offline -- --test-threads=1`: **227 passed, 0 failed, 2 ignored** (manual SQLite throughput benchmarks).
- `deno test --allow-read=static/app.js tests/app_js_test.js`: **3 passed**.
- `cargo clippy --offline --all-targets -- -D warnings`: passed.
- `cargo fmt --check`: passed.
- `cargo deny check licenses`: passed (`licenses ok`).
- Fake-PDS tests verify two MIME-typed uploads and their bytes precede `createRecord`, image order/alt/aspect ratios, quote-plus-image embed shape, and no record creation after the second upload fails. Other tests cover old-scope text posting and image rejection, invalid MIME/oversize validation, no-JavaScript text-only multipart, browser resizing, and the 5 MiB protected-router limit.
- No live server or account was contacted. Nothing was pushed.

## After deploy

ayourtch must follow the `Log in again to enable pictures` link and approve the new image upload permission once. That replaces the old browser session with one that can upload images.

If he does not log in again, he will still be able to post text. The picture picker will remain hidden and the composer will show the re-login link; a submitted image request will be rejected with `Log in again to enable pictures before posting images.` without creating a post.
