# appview-rs: a small Bluesky AppView in Rust (task for a codex lane)
Owner: ayourtch. Written by claude-helper 2026-10-27 at his ask ("make a new repo, write up a task and give it to codex to implement in red green TDD; it should include oauth authentication").

## Goal
A small, self-hostable ATProto AppView in Rust that the official Bluesky client (bsky.app / social-app) can use for the
everyday views, plus its own minimal web UI with ATProto OAuth login. Single binary, embedded storage, modest disk.

## Licence and clean room (hard rules)
- This repo is MIT OR Apache-2.0 (add both LICENSE files in the first commit).
- Do NOT read, copy or port AppViewLite (alnkesq/AppViewLite, AGPL-3.0) or any other AGPL/GPL code. Don't clone it, don't open it.
- Allowed sources: the ATProto specs and docs (atproto.com, docs.bsky.app), the lexicon JSON files
  (bluesky-social/atproto `lexicons/`, MIT/Apache), and observed behaviour of the public API (public.api.bsky.app)
  to record test fixtures. The official appview (bluesky-social/atproto `packages/bsky`) is MIT/Apache: you may read it
  to understand behaviour, but write the code yourself.
- Dependencies must be permissively licensed (MIT/Apache/BSD/ISC/MPL-2.0 ok; no GPL/AGPL). Good candidates:
  atrium-api / atrium-identity / atrium-oauth (MIT), rsky-repo / rsky-identity / rsky-lexicon (Apache-2.0),
  tokio, axum, reqwest, tokio-tungstenite, serde, rusqlite or fjall. Run `cargo deny check licenses` (add deny.toml) in CI-style tests.

## Method: red/green TDD, strictly
For every behaviour: first commit a failing test (subject starts `red:`), then the smallest code that makes it pass
(subject starts `green:`), then optional `refactor:`. Never weaken or delete a test to make it pass. Tests run offline:
use local fakes (a fake Jetstream websocket server, a fake PLC directory, a fake PDS that is also the OAuth
authorization server) and recorded JSON fixtures. Live-network checks are separate, opt-in (`--ignored`), read-only.
`cargo test` must be green at every `green:` commit; `cargo clippy -- -D warnings` and `cargo fmt --check` clean.

## Milestones (do them in order; commit as you go; stop cleanly at any milestone boundary if you run out of time)
M1 Skeleton: workspace, config (TOML + env), axum server, `/xrpc/_health`, structured logging, LICENSE files, deny.toml.
M2 Identity: DID resolution (did:plc via PLC directory, did:web), handle <-> DID resolution (DNS TXT + /.well-known), caching with TTL; bidirectional handle verification.
M3 Ingest: Jetstream consumer (configurable URLs, cursor persisted, reconnect with backoff) for app.bsky.feed.post/like/repost, app.bsky.graph.follow/block, app.bsky.actor.profile, including deletes. Storage behind a trait; pick an embedded engine and justify it in DESIGN.md.
   Scope modes: `scoped` (default: index only registered users, the accounts they follow, and interactions on their posts)
   and `full` (everything). Measure and log events/s and bytes on disk.
M4 Backfill: for accounts entering scope, fetch the repo (com.atproto.sync.getRepo CAR) from their PDS, verify, and index; rate-limited per host.
M5 XRPC read API matching the lexicons, so the official client works through it: app.bsky.actor.getProfile(s),
   app.bsky.feed.getAuthorFeed, getPostThread, getPosts, getTimeline, getLikes, getRepostedBy, app.bsky.graph.getFollows/getFollowers,
   app.bsky.notification.listNotifications + getUnreadCount. Response shapes tested against recorded public-API fixtures.
   Requests proxied from a user's PDS carry an inter-service JWT: verify it (signature against the issuer's DID-doc key, aud = this appview's did:web, exp, lxm).
   Serve /.well-known/did.json for the appview's did:web with the `#bsky_appview` service entry.
M6 OAuth: the built-in web UI logs users in with ATProto OAuth as a confidential client: client metadata document,
   handle -> DID -> PDS -> authorization server discovery, PAR, PKCE, DPoP-bound tokens (with nonce handling), token refresh,
   session storage, logout. Test the whole flow against the fake PDS/authorization server, including bad state, wrong issuer, expired tokens and DPoP nonce retry.
   A logged-in user becomes "registered" (enters scope). Minimal pages: login, timeline, profile, thread, post/like/repost via the user's PDS.
M7 Ops: README (run it, point a client at it, measured disk/RAM), Dockerfile optional.

## Deliverables
Code + tests in this repo; DESIGN.md (architecture, storage choice, scope rules, what's left); `LANE-REPORT.md` at the end
of each session with milestone status, test counts, and anything that's stubbed. Never push anywhere (no remote exists).
