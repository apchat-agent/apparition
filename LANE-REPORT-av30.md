# Lane av30 report

## Connector/avatar geometry

Reply connectors now end 1.6 px before the reply author's avatar in the parent-card and reply-to-previous layouts. The notification connector stops at the parent context boundary. Timeline, feed, profile, notification, and connected-reply fixtures use the shared stylesheet.

Firefox ran the av28 and av29 fixtures at 1440 px and 390 px CSS viewport widths. The headless Firefox window has a 500 px minimum, so the phone fixture uses a 390 px iframe viewport; the script measures `contentWindow.innerWidth` and the connector/avatar rectangles inside that frame.

The first pass injected the original 2038453 values (`bottom: -2.375rem`, previous-line height `4.75rem`) and failed: the normal reply line overlapped the avatar by 31.6 px and the previous-reply line by 29.2 px. The updated CSS passed at both widths. The parent-card and previous-reply layouts leave a 1.6 px gap. The notification line also remains above the following notification's avatar.

![Before: original connector values overlap the reply avatars](artifacts/av30-fixture/before.png)

![After: fixed connector spacing in the 390 px av28 fixture](artifacts/av30-fixture/after.png)

![After: fixed connector spacing at desktop width](artifacts/av30-fixture/desktop-after.png)

Repeat the geometry check with `python3 artifacts/av30-fixture/capture_connectors.py`. It also captures the av29 feed/profile/notification fixtures and composer screenshots.

## Link cards

The post, reply, and quote composers now have a webpage URL field and a Preview link button. The preview shows the fetched title, description, and optional thumbnail, with a Remove webpage button. If pictures and a webpage are both selected, the composer explains that pictures will be attached and the link will be omitted; the server applies the same precedence.

`POST /link-preview` requires a same-origin request and a signed-in writer. Page and thumbnail requests use the guarded outbound client with DNS checks at every hop, at most three redirects, a five-second total preview deadline, and separate 1 MiB response caps. Page responses must be `text/html`; thumbnails must be `image/*`. Outbound requests carry no cookies. Relative `og:image` URLs resolve against the final page URL. Link thumbnails are uploaded through `com.atproto.repo.uploadBlob` and included as the external embed's `thumb` blob. Posts without a thumbnail omit that field.

The preview card uses the same external-card styling as rendered embeds. Rendered external embeds now display either the stored blob thumbnail or a safe image URL from an embed view. The mobile composer capture checks the preview card's fit at phone width.

![Composer link-card preview on desktop](artifacts/av30-fixture/composer-desktop.png)

![Composer link-card preview at phone width](artifacts/av30-fixture/composer-phone-card.png)

![Rendered external embed with its thumbnail on the phone fixture](artifacts/av30-fixture/external-phone.png)

The regression tests cover OG title, description and image parsing; missing fields; relative image URLs; HTML entities; private IP literals and DNS answers (including 127/8, 10/8, 192.168/16, 169.254/16, 100.64/10, `::1`, and IPv6 ULA); private redirect targets; body-size limits; wrong content types; external record JSON with and without a thumb; image-over-link precedence; and upload-before-create ordering through the production OAuth flow.

## Verification

- `CARGO_NET_OFFLINE=true CARGO_BUILD_JOBS=4 nice -n 10 cargo test -- --test-threads=1`: 250 passed, 2 ignored.
- `deno test --allow-read tests/app_js_test.js`: 13 passed. Deno needs read access to load `static/app.js`.
- `CARGO_NET_OFFLINE=true CARGO_BUILD_JOBS=4 nice -n 10 cargo clippy -- -D warnings`: passed.
- `cargo fmt --check`: passed.
- `python3 artifacts/av30-fixture/capture_connectors.py`: red on the original geometry and green at 1440 px and 390 px.

No push was made.
