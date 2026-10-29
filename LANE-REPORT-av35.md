# av35: account labels and profile actions

## Result

Added profile controls for follow/unfollow, mute/unmute, block/unblock, and adding or removing the viewer's own curated and
moderation lists. The profile page shows the current local relationship, including whether either account follows the
other, local block/mute state, and list membership. Actions write only to the logged-in viewer's repository or private
preferences; the viewer's own profile has no action controls. Block and list removal require explicit confirmation.
Forms work without JavaScript, and JavaScript refreshes the action panel in place.

Account labels from the viewer's subscribed labelers now appear as compact chips on profile headers and post cards. Tapping
a chip opens the labeler's display name and the label's severity and blur metadata. Bluesky's moderation labeler is included
alongside the user's subscriptions. The home timeline and custom feed page omit authors blocked or muted by the viewer.

## Implementation notes

- New OAuth grants include the follow, block, and list-item repository permissions, plus mute, unmute, and get-mutes RPC
  permissions. The RPC scopes use `aud=*` with their specific `lxm`; older sessions keep the per-action “Sign in again”
  prompts until they authorize the new scopes.
- Profile follow, block, and list membership are repository records under the viewer DID. Mute state uses
  `app.bsky.graph.muteActor` and `unmuteActor` through the PDS proxy to the Bluesky AppView; it is not stored as a repo
  record.
- Label retrieval uses `app.bsky.actor.getProfiles` against the public AppView, batched in groups of 25 actors and sent
  with the `atproto-accept-labelers` header. The viewer's `labelersPref` is read from OAuth preferences; Bluesky's
  moderation DID is always included. Labeler names and custom value presentation come from
  `app.bsky.labeler.getServices` service declarations. Preference results are cached for five minutes, service
  presentations for six hours, and page labels for one minute. Every public HTTP request goes through `SafeHttpClient`.
- Global values `!hide`, `!warn`, `porn`, `sexual`, `nudity`, and `graphic-media` have built-in display defaults. This
  lane displays labels and their metadata only. Applying blur or hide behavior would require applying each labeler's
  configured visibility preference and handling `content` and `media` effects separately; posts and media remain visible.
- The home timeline and custom feed route filter rendered posts against local block records and the viewer's muted-account
  RPC result. This is a rendering filter; it does not delete or rewrite indexed posts.

## Source notes

The AppView header is the supported way to ask for a set of labelers. I chose its `getProfiles` endpoint over querying each
labeler directly because one request can return labels from all accepted labelers for a batch of actors, avoiding per-labeler
fan-out and DID-document service resolution. Calls are bounded to 25 actors per page batch. The official
[getProfiles lexicon](https://github.com/bluesky-social/atproto/blob/main/lexicons/app/bsky/actor/getProfiles.json) and
[moderation guide](https://github.com/bluesky-social/bsky-docs/blob/main/docs/advanced-guides/moderation.md) document the
endpoint, labeler header, labeler service records, label definitions (`severity`, `blurs`, and localized names), global
labels, and the 20-labeler limit. The global fallback display metadata follows the official
[AT Protocol label guide](https://atproto.com/guides/labels).
The preference shape is defined by the official
[actor definitions lexicon](https://github.com/bluesky-social/atproto/blob/main/lexicons/app/bsky/actor/defs.json).
The labeler service lookup follows the official
[getServices lexicon](https://github.com/bluesky-social/atproto/blob/main/lexicons/app/bsky/labeler/getServices.json).
The public AppView endpoint and service routing choices follow the official
[API directory](https://github.com/bluesky-social/bsky-docs/blob/main/docs/advanced-guides/api-directory.mdx).

The new RPC scopes follow the audience and method rules in the
[AT Protocol permissions specification](https://atproto.com/specs/permission). Follow, block, and list membership use the
official [follow](https://github.com/bluesky-social/atproto/blob/main/lexicons/app/bsky/graph/follow.json),
[block](https://github.com/bluesky-social/atproto/blob/main/lexicons/app/bsky/graph/block.json), and
[list-item](https://github.com/bluesky-social/atproto/blob/main/lexicons/app/bsky/graph/listitem.json) record lexicons.

## Screenshot

Captured with headless Firefox from a local-only fixture using this repo's stylesheet. It shows profile actions, an expanded
label chip, and a matching chip beside the post author. The fixture does not contact the running app or public services.

![Profile actions and account label fixture](screenshots/av35-profile-actions-labels.png)

Fixture and capture driver: [`artifacts/av35-fixture/`](artifacts/av35-fixture/).

## Red/green commits

- `500890e` / `e9f0760`: OAuth scopes red/green.
- `56de6c6`, `6b5c021`, `1a85f55` / `43ba490`, `a857236`: profile relationships, viewer-owned record writes, and fake-PDS
  request shapes.
- `60b3a23` / `aa3f22d`: batched label rendering and home/feed filtering.
- `1d7575f` / `aa3f22d`: muted and blocked account filtering.
- `38af6fb` / `aa3f22d`: server-side block confirmation.
- `859a56d` / `cd75484`: compact confirmation checkbox styling.
- `4426d33`: test fixture and renderer refactor after the label index gained its label data.

## Verification

- `nice -n 10 env CARGO_BUILD_JOBS=4 cargo test --locked --offline -- --test-threads=1` — 283 passed, 2 ignored.
- `deno test --allow-read=static/app.js tests/app_js_test.js` — 17 passed.
- `nice -n 10 env CARGO_BUILD_JOBS=4 cargo clippy --locked --offline --all-targets -- -D warnings` — passed.
- `cargo fmt --all -- --check` — passed.
- Firefox fixture capture at 1280×1015 confirmed the profile action buttons and two rendered label chips.

No live server was used or modified. Nothing was pushed.
