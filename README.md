# apparition

An app that is mostly a ghost: every line here was written by AI coding agents (OpenAI Codex lanes, briefed and
reviewed by a Claude agent), with a human choosing what to build and trying it on his phone. No human wrote the code.

apparition (crate name `appview-rs`) is a small, self-hostable AT Protocol AppView written in Rust. It indexes selected Jetstream events into SQLite, serves Bluesky read XRPCs, and includes a browser UI that signs in with ATProto OAuth and writes records to the user's PDS.

This is an early implementation, not a feature-complete replacement for Bluesky's hosted AppView. See [DESIGN.md](DESIGN.md) for implemented routes, scope rules, and current limits.

Licence: MIT OR Apache-2.0.

## Build and run

Rust stable is required. Build and run from the repository root:

```sh
cargo build --release --locked
cp appview.example.toml appview.toml
APPVIEW_CONFIG=./appview.toml ./target/release/appview-rs
```

The sample config uses the public east Jetstream and indexes in `scoped` mode. The server binds to `127.0.0.1:3000`; put it behind a TLS reverse proxy for a public deployment. Per-peer rate limits use the TCP peer address. When all traffic comes through one proxy, clients share that proxy's rate-limit bucket; forwarded client-IP headers are not used. Keep the app's listener private to the proxy and account for the shared bucket. Keep the SQLite databases and OAuth key in a persistent, private data directory. Back up `appview.db`, `oauth.db` (sign-in sessions, kept separate so backfill writes cannot lock sign-in out), `identity-cache.db`, and `oauth-client-key.json` together. The key is created with owner-only permissions on Unix and must remain stable across restarts. Confidential mode uses it for client assertions; loopback mode uses it only to sign local browser cookies.

Check the server and public service documents:

```sh
curl http://127.0.0.1:3000/xrpc/_health
curl http://127.0.0.1:3000/.well-known/did.json
curl http://127.0.0.1:3000/.well-known/oauth-client-metadata.json
```

Set `APPVIEW_PUBLIC_URL` to the externally reachable HTTPS origin and `APPVIEW_DID` to its `did:web` identifier. The DID hostname, public URL hostname, and reverse proxy host must agree. For example, `did:web:appview.example.com` should be served at `https://appview.example.com/.well-known/did.json`. Terminate TLS at the proxy and forward requests to the configured local listen address.

## Running in a container

The multi-stage image compiles the locked Rust dependencies in a build stage and runs the binary as UID 10001 in a Debian slim image. The runtime listens on `0.0.0.0:3000`, stores SQLite and the OAuth client key under `/data`, and checks `/healthz`. The process only needs write access to the mounted data volume; run the container with a read-only root filesystem:

```sh
docker build -t appview-rs:local .
docker volume create appview-data
docker run --detach --name appview-rs \
  --publish 127.0.0.1:3000:3000 \
  --read-only --tmpfs /tmp:rw,noexec,nosuid,size=16m \
  --mount source=appview-data,target=/data \
  --env APPVIEW_PUBLIC_URL=https://appview.example.com \
  --env APPVIEW_DID=did:web:appview.example.com \
  appview-rs:local
curl http://127.0.0.1:3000/healthz
curl http://127.0.0.1:3000/login
```

Replace the example URL and DID with the public HTTPS origin and its matching `did:web`. Put a TLS reverse proxy in front of the container and expose its port only to that proxy. Bind another host port with `--publish 3000:3000` only when direct access is intended. TOML configuration can be supplied by mounting a read-only file and setting `APPVIEW_CONFIG`; environment variables can override individual options. The OAuth signing key is created in `/data/oauth-client-key.json`, so keep the data volume private and persistent and back it up with the database. Do not put OAuth keys in the image, build arguments, or source tree.

## Try it locally

On the same computer that runs the browser, start the app in loopback OAuth mode. It uses the default public east Jetstream and indexes registered accounts and their network in `scoped` mode:

```sh
APPVIEW_LISTEN=127.0.0.1:3000 APPVIEW_SCOPE=scoped APPVIEW_OAUTH_MODE=loopback cargo run --locked --offline -- --dev
```

Open [http://127.0.0.1:3000/login](http://127.0.0.1:3000/login), enter an AT Protocol handle or DID, and approve the request at the authorization page. The browser returns to the local callback and the AppView opens the signed-in account's timeline. The `--dev` log line prints the login URL. Loopback mode uses a public `http://localhost` OAuth client and does not publish confidential client metadata or a client key to the authorization server. The server rejects non-loopback listen addresses in this mode.

## Configuration

`APPVIEW_CONFIG` selects a TOML file. Non-empty environment variables override its values.

| TOML field | Environment variable | Default | Purpose |
| --- | --- | --- | --- |
| `listen` | `APPVIEW_LISTEN` | `127.0.0.1:3000` | HTTP bind address |
| `data_dir` | `APPVIEW_DATA_DIR` | `./data` | `appview.db` records, `oauth.db` sessions, `identity-cache.db` identities, and the OAuth client key |
| `log_filter` | `APPVIEW_LOG_FILTER` | `info` | `tracing` filter |
| `scope_mode` | `APPVIEW_SCOPE` | `scoped` | `scoped` or `full` indexing |
| `backfill_enabled` | `APPVIEW_BACKFILL_ENABLED` | `false` | Opt in to repository backfill for registered and followed accounts |
| `backfill_records_per_second` | `APPVIEW_BACKFILL_RECORDS_PER_SECOND` | `300` | Maximum repository records imported per second |
| `jetstream_urls` | `APPVIEW_JETSTREAM_URLS` | east Jetstream URL | TOML string array, or comma-separated URLs in the environment |
| `jetstream_idle_timeout_secs` | `APPVIEW_JETSTREAM_IDLE_TIMEOUT_SECS` | `60` | Reconnect when a Jetstream connection sends no websocket messages for this long |
| `jetstream_event_timeout_secs` | `APPVIEW_JETSTREAM_EVENT_TIMEOUT_SECS` | `60` | Drop and reconnect if one event takes longer than this to process |
| `jetstream_stale_cursor_threshold_secs` | `APPVIEW_JETSTREAM_STALE_CURSOR_THRESHOLD_SECS` | `600` | Skip and record an older cursor window when it is beyond this age |
| `appview_did` | `APPVIEW_DID` | `did:web:localhost` | AppView service identity |
| `public_url` | `APPVIEW_PUBLIC_URL` | `https://appview.example` | HTTPS origin and OAuth client ID origin in confidential mode; ignored in loopback mode |
| `oauth_mode` | `APPVIEW_OAUTH_MODE` | `confidential` | `confidential` for the public HTTPS client, or `loopback` for local development OAuth |

Use `scoped` for a small self-hosted instance. It indexes registered users, their follows, and interactions on their posts. `full` indexes every supported account and will use substantially more disk as the firehose continues. Neither mode currently has a retention or compaction policy.

Repository backfill is off by default. To import existing records for registered accounts and their follows, set `backfill_enabled = true` in `appview.toml` or `APPVIEW_BACKFILL_ENABLED=true`. The worker fetches each repository from its PDS and imports it at up to `backfill_records_per_second` records per second (default 300). Backfill uses network bandwidth and database space; keep it off if you only want to index events observed after startup.

In scoped mode, incoming likes and reposts are indexed when their subject is a registered or followed DID's post. Replies, quotes, and facet mentions are also indexed when they point to a registered or followed DID. This makes counts and notifications accurate for events seen from the current Jetstream cursor onward; older likes, reposts, and replies are not backfilled.

Jetstream writes commit in batches of up to 200 events or 250 ms. The saved cursor advances in the same transaction as the batch. On reconnect, a cursor older than the configured threshold is reset to 30 seconds ago; the skipped range is logged and saved in `appview_state` for future repair.

A future count backfill could enqueue rate-limited, paginated `getLikes`, `getRepostedBy`, and thread snapshots for each tracked post, store those as count baselines, and reconcile the snapshots against Jetstream create/delete events using a captured cursor and record URI deduplication. That backfill is not implemented.

Verified handles stay visible after their 30-minute freshness window while a background worker rechecks them. It refreshes stale handles for registered, followed, and indexed DIDs, and queues checks when Jetstream reports an identity or account change for a tracked or displayed DID. A new profile without a verified handle shows its DID while the first check is pending. A definitive verification failure changes the profile to `handle.invalid`; transient lookup errors leave the last verified handle in place. Jetstream's legacy subscribe contract sends identity and account events even when `wantedCollections` is set ([Jetstream documentation](https://github.com/bluesky-social/jetstream-legacy)).

## Point a client at the AppView

An XRPC client that allows a configurable service URL can use the AppView origin directly for public reads. For example:

```sh
curl 'https://appview.example.com/xrpc/app.bsky.feed.getAuthorFeed?actor=bsky.app&limit=10'
```

The API also publishes the `did:web` document with the `#bsky_appview` service entry. Authenticated home-timeline and notification methods require a service JWT from the user's PDS. The client obtains it with `com.atproto.server.getServiceAuth`, using this AppView's DID as the audience and the requested XRPC method as `lxm`, then sends it as a Bearer token to this AppView. Other public read methods can be called without that token.

The upstream Bluesky social-app currently points public reads at its hosted AppView. A social-app fork or development build must configure both its AppView host and AppView DID to use this service; setting only the host does not configure service-proxy authentication. The current source has a development URL override named `EXPO_PUBLIC_DEV_ENV_APPVIEW_URL`; production builds can change the `PUBLIC_APPVIEW` and `PUBLIC_APPVIEW_DID` constants and the proxy DID configuration in the fork. This repo does not build or patch the social-app client.

The built-in UI is available at `/login`. It resolves a handle or DID, starts OAuth, and shows the signed-in account's timeline from this AppView's local index. When backfill is enabled, the worker imports the signed-in account and its followed accounts; while the timeline is empty, `/app` reports backfill progress. Posting, liking, and reposting send records to the user's PDS. Replies create indexed reply records with root and parent references; `/app/thread` shows the parent chain and locally indexed replies. The signed-in UI has a responsive three-column desktop layout and a single-column mobile layout, with the current account's pinned feeds in its navigation. If the authorization server grants `atproto` but omits a requested write or feed RPC permission, the UI explains which access is unavailable. Existing sessions created before the feed RPC permissions were added must sign in again to allow feeds.

## OAuth permissions

The built-in UI requests these scopes in both confidential and loopback OAuth modes. `atproto` identifies the account; the other scopes enable the specific actions and views provided by the UI.

| Scope | Purpose |
| --- | --- |
| `atproto` | Sign in and identify the account. |
| `repo:app.bsky.feed.post?action=create` | Create posts and replies. |
| `repo:app.bsky.feed.like?action=create&action=delete` | Like and unlike posts. |
| `repo:app.bsky.feed.repost?action=create&action=delete` | Repost and undo reposts. |
| `blob:image/*` | Upload images attached to posts. |
| `rpc:app.bsky.actor.getPreferences?aud=*` | Read account preferences used for pinned feeds. |
| `rpc:app.bsky.feed.getFeedSkeleton?aud=*` | Load the skeleton for a selected custom feed. |
| `blob:video/*`, `rpc:app.bsky.video.getUploadLimits?aud=*`, `rpc:com.atproto.repo.uploadBlob?aud=*` | Check video limits and upload video posts. |
| `repo:app.bsky.graph.follow?action=create&action=delete` | Follow and unfollow accounts. |
| `repo:app.bsky.graph.block?action=create&action=delete` | Block and unblock accounts. |
| `repo:app.bsky.graph.listitem?action=create&action=delete` | Add and remove accounts from lists. |
| `rpc:app.bsky.graph.muteActor?aud=*`, `rpc:app.bsky.graph.unmuteActor?aud=*`, `rpc:app.bsky.graph.getMutes?aud=*` | Mute, unmute, and check muted accounts. |

The UI uses these permissions for its visible write and account-management features. An authorization server may grant only a subset: sign-in still works with `atproto`, while unavailable actions are disabled or prompt for a new sign-in.

## Checks

Run the offline suite and static checks with:

```sh
cargo test --locked --offline
deno test --allow-read=static
cargo fmt --all -- --check
cargo clippy --locked --offline -- -D warnings
cargo deny check licenses
```

The OAuth protocol test uses a local fake PDS and authorization server; tests do not need live Bluesky credentials or network access.

## Measured starting footprint

One idle-start sample was taken on Darwin 25.5.0 arm64 using the optimized release binary. Jetstream pointed to an unavailable loopback port, the database had no indexed events, and the process was sampled about six seconds after launch:

| Measurement | Sample |
| --- | ---: |
| Release executable on disk | 15,207,792 bytes (14.5 MiB) |
| Process RSS | 12,000 KiB (about 11.7 MiB) |
| Empty data directory while running | 148 KiB allocated (SQLite DB/WAL/SHM plus OAuth key) |
| SQLite DB after clean shutdown | 73,728 bytes |

This is a single idle sample, not a load benchmark. It says nothing about disk growth under `full` ingestion or memory under concurrent traffic. Ingest logs include event-rate and database/WAL byte measurements for an active instance.
