# av31: video upload and playback

## Implementation

Video posting uses the dedicated `app.bsky.video.uploadVideo` service endpoint. It accepts the upload for processing and stores the processed blob on the PDS; its response and `app.bsky.video.getJobStatus` expose the job state and the completed blob. This lets the app wait for processing and stop on a terminal failure before it creates the post record. The client obtains service-auth tokens for `did:web:video.bsky.app`, with each video RPC as the `lxm`, and checks `getUploadLimits` before streaming the file. The official client follows the same service-auth audience and upload-limits flow. A regular `com.atproto.repo.uploadBlob` upload does not provide this video processing job lifecycle, so using it could publish a record before the video is ready. That comparison is an inference from the two APIs' documented responsibilities.

The composer accepts MP4, WebM, and MOV, previews file size and duration, caps uploads at 300,000,000 bytes and 10 minutes, offers optional alt text, and reports byte progress followed by processing status. The server streams multipart file chunks to a mode-0600 temporary file and then streams that file to the video service. `/video` has a separate 301 MB request limit; picture uploads keep their 5 MiB route limit. The service's daily video and byte quotas are checked before sending the video bytes upstream.

New OAuth requests include `blob:video/*` and the audience-scoped `getUploadLimits`, `uploadVideo`, and `getJobStatus` RPC scopes. Signed cookies now retain the video grant. Cookies from before that field existed decode with video access disabled, while previous write and picture grants remain usable. Upload requests from these sessions get the “sign in again to post videos” message.

Timeline, thread, and quoted-post rendering handles video embeds and record-with-media embeds. It uses the Bluesky video playlist and thumbnail URLs, adds accessible alt text and aspect ratio, and renders native controls without autoplay. Safari uses native HLS; other MediaSource browsers use locally vendored hls.js 1.7.3, with a direct playlist link as a fallback. The CSP allows media and HLS connections only to the video service while scripts remain same-origin.

The implementation follows the official [uploadVideo lexicon](https://raw.githubusercontent.com/bluesky-social/atproto/main/packages/api/lexicons/app/bsky/video/uploadVideo.json), [getJobStatus lexicon](https://raw.githubusercontent.com/bluesky-social/atproto/main/packages/api/lexicons/app/bsky/video/getJobStatus.json), and [job status definitions](https://raw.githubusercontent.com/bluesky-social/atproto/main/packages/api/lexicons/app/bsky/video/defs.json). The [getUploadLimits lexicon](https://raw.githubusercontent.com/bluesky-social/atproto/main/packages/api/lexicons/app/bsky/video/getUploadLimits.json) defines the account's remaining daily video and byte quotas. The official [video upload helper](https://raw.githubusercontent.com/bluesky-social/social-app/main/src/lib/media/video/upload.shared.ts) uses service auth for the video service audience. The current [Bluesky app constants](https://raw.githubusercontent.com/bluesky-social/social-app/main/src/lib/constants.ts) specify 300 MB, 10 minutes, and the supported MIME types. The [video embed lexicon](https://raw.githubusercontent.com/bluesky-social/atproto/main/packages/api/lexicons/app/bsky/embed/video.json) defines the processed blob, alt text, and aspect ratio carried by the post.

## Regression coverage

- OAuth request scope contains the video scopes; legacy scope and cookie tests prove video access stays disabled while picture and text posting remain available.
- Oversize and overlong files are rejected before the service upload call; daily quota checks cover exhausted video and byte allowances.
- Fake job polling covers processing-to-complete and processing-to-failed paths.
- Record tests cover video, alt text, aspect ratio, reply, and quote embed shapes.
- Renderer tests cover playlist, poster, alt text, dimensions, controls, and no autoplay.
- Route body-limit tests keep the 5 MiB image cap separate from `/video`; Deno tests cover browser-side metadata and upload progress.

## Verification

- `cargo test -- --test-threads=1` — passed.
- `deno test --allow-read=static/app.js tests/app_js_test.js` — 14 passed. The installed Deno version requires the explicit read permission for this existing test harness.
- `cargo clippy --all-targets -- -D warnings` — passed.
- `cargo fmt --all -- --check` — passed.

The red regression commit is `b877bc2` (`test: add av31 video upload and playback regressions`). The implementation and this report are committed separately as the green change.

## Screenshots

These Firefox captures use the production stylesheet and the rendered composer/player markup in a local preview page. The bicycle image is a repository test fixture used as the video poster; the captures demonstrate layout and controls, not a live upload or network playback session.

![Video composer with selected file metadata and alt text](screenshots/av31-video-composer.png)

![Timeline video player with poster, controls, and playlist fallback](screenshots/av31-video-playback.png)
