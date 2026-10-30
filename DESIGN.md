# Design

## Architecture

The binary runs an Axum HTTP server, a Jetstream WebSocket consumer, and an optional repo backfill worker. `Storage` isolates indexed protocol data. Application records and OAuth state/session storage use separate SQLite databases, and the persistent identity cache has its own database. `IdentityBackend` isolates DID and handle lookups so network resolution can be replaced by deterministic test fixtures.

The AppView DID document advertises the `#bsky_appview` service. The XRPC service verifies inter-service JWT signatures using the issuer's DID document, then checks audience, expiry, issued-at time, and lexicon method. Anonymous public reads are accepted on methods that do not require account context; timeline and notification methods require a service JWT.

## Storage

SQLite is the embedded store. It needs no separate service and has a small idle footprint. `appview.db` stores records under a composite `(did, collection, rkey)` primary key with a collection/time index, plus cursors, registered accounts, verified handles, follow edges, and backfill requests. `oauth.db` stores OAuth authorization state and refreshable sessions. `identity-cache.db` stores cached DID documents and handle resolutions. WAL mode allows readers while ingestion writes. `MemoryStorage` provides deterministic API and integration tests. `Storage` is the production boundary if a different engine is needed after load measurements.

OAuth client signing material is stored separately as `oauth-client-key.json` under the configured data directory, created with Unix mode `0600`. Keep the data directory private, including backups of all three databases and the signing key.

## Scope and ingestion

`scoped` is the default. It indexes records authored by registered users and accounts they follow. Likes and reposts from any actor are indexed when their subject URI belongs to a registered user's repository. Follow edges created by registered users are maintained as scope inputs. A successful OAuth login registers the account and queues its repository for backfill. `full` indexes all six supported Jetstream collections for every actor.

The consumer subscribes to posts, likes, reposts, follows, blocks, and profiles; persists the latest `time_us` cursor; and reconnects to configured endpoints with exponential backoff. SQLite upserts make event replay after a crash safe. Structured log events report event rate and SQLite/WAL bytes. The active-ingest rate depends on the selected stream and scope; the idle measurement in the README is not a load result.

## Backfill

Accounts entering scope are queued for `com.atproto.sync.getRepo` only when `backfill_enabled` is true; it defaults to false. Set `APPVIEW_BACKFILL_ENABLED=true` (or the TOML field) to opt in. `backfill_records_per_second` defaults to 300 and limits import work. The worker gets the PDS from the account's DID document, spaces requests per PDS host, and rejects oversized CAR responses. The CAR reader checks block hashes and MST completeness; the signed commit is checked against the document's `#atproto` key and DID. A verified snapshot replaces that DID's local records and follow edges in one SQLite transaction.

## XRPC API

The server implements:

- `app.bsky.actor.getProfile` and `getProfiles`
- `app.bsky.feed.getAuthorFeed`, `getPosts`, `getPostThread`, `getTimeline`, `getLikes`, and `getRepostedBy`
- `app.bsky.graph.getFollows` and `getFollowers`
- `app.bsky.notification.listNotifications` and `getUnreadCount`

Response shapes are checked against recorded public API fixtures. The API is a small local index: pagination, aggregates, notification state, and some hydrated-view details are intentionally limited. The user's home timeline and notification views require a service JWT and are filtered to the JWT issuer DID.

## OAuth and web UI

The web client publishes confidential OAuth metadata with a stable P-256 key. It resolves the login handle or DID and PDS, discovers OAuth metadata, then delegates PAR, PKCE, DPoP proofs and nonce retries, code exchange, issuer validation, and token refresh to Atrium OAuth. Authorization state and refreshable sessions are persisted in `oauth.db`. The callback validates the returned token scope and subject DID before registering the account. Browser sessions use an HMAC-signed, HttpOnly, SameSite cookie; HTTPS deployments also mark it Secure. Each browser login gets a random session ID and its own generation counter, so signing in on another device does not invalidate existing sessions. Logout advances only the current session's counter.

The `/app` timeline uses the same local indexed-record query as `app.bsky.feed.getTimeline`; profile and thread pages also read only the local index. An empty timeline reports the number of in-scope accounts and completed repo backfills. Successful login registers the account and queues its repo; importing that repo queues each followed account for backfill.

OAuth requests `atproto`, the post/like/repost repo scopes, `blob:image/*`, the feed preference and skeleton RPC scopes, the video blob/upload RPC scopes, follow and block repo scopes, the list-item repo scope, and mute/unmute/get-mutes RPC scopes. These permissions cover the built-in UI's post and media creation, custom-feed preferences, follow/block/list/mute controls, and feed selection. The callback accepts a valid `atproto` session when optional permissions are only partly granted; features whose scopes were not granted remain unavailable until the user signs in again. Record writes go through the logged-in user's OAuth session to their PDS.

## Current limits

- This is an early AppView, not full parity with Bluesky's hosted service. It does not provide search, blob proxying, moderation labels, or every optional field and behavior in Bluesky's views.
- Notification responses are reconstructed from indexed interactions and do not maintain server-side seen state or the complete production notification model.
- Cursor pagination and retention/compaction are not implemented; a full-mode database will grow continuously.
- The README footprint is one idle startup sample. There is no measured throughput, concurrent-load, or long-running disk-growth benchmark yet.
- The offline OAuth protocol test drives Atrium's OAuth client against a local fake PDS/authorization server; the HTTP UI tests use a fake `OAuthFlow`. There is no external live-account OAuth check in the test suite.
- Official social-app is not bundled. A client or social-app fork must be configured with the AppView URL and DID.
