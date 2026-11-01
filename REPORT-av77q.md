# REPORT av77q — feed posts through the signed-in viewer's account, not the anonymous public API

## Cause, in my own words

A signed-in Discover page spent 3,608 ms of its 5,246 ms in a single anonymous
`public.api.bsky.app/xrpc/app.bsky.feed.getPosts` call (normally ~0.3 s). Every
page-load hydration of feed posts went to the anonymous public AppView, which the
owner suspects is being throttled — so the viewer's own request, from their own
account, waited on the throttled anonymous route. Search already avoids this by
running through the viewer's stored session against their PDS, proxied to the
Bluesky AppView with `atproto-proxy: did:web:api.bsky.app#bsky_appview`. Feed
hydration (and the parent/reply-context fetch) had never been moved onto that
route; this lane moves them.

## Design

- **Scope**: `rpc:app.bsky.feed.getPosts?aud=*` joins `REQUESTED_SCOPE` (the
  client-metadata maximum) and `READ_ONLY_SCOPES`, so every sign-in — no checkbox —
  is granted it, exactly like `searchPosts`. A new `GET_POSTS_SCOPES` constant is
  checked against the *stored* token set before any proxied call is spent, so a
  sign-in made before this change keeps the old anonymous route without a failing
  call (`has_all_scopes(...) -> Ok(None)`).
- **Route** (`src/oauth.rs`): `AtriumOAuthFlow::proxied_get_post_views` mirrors
  `signed_search_posts_body`: stored-session check → scope check → `restored_session`
  → `session_agent(&session).api_with_proxy(BSKY_APPVIEW_DID, BSKY_APPVIEW_SERVICE)`
  → atrium `app.bsky.feed.get_posts`, uris batched at 25 by the existing
  `FeedClient::fetch_posts` batching. It implements a small new trait
  `feeds::ProxiedPostsFetcher` (`Ok(None)` = route unavailable, no warn; `Err` =
  call failed, one WARN per batch then fall back), injected as
  `Option<Arc<dyn ProxiedPostsFetcher>>` into `hydrate_skeleton` and, via
  `OAuthFlow::fetch_public_posts(viewer_did, uris)`, into the parent-hydration path
  (`parent_public_ms` phase) and quote hydration.
- **Fallback** (`src/feeds.rs`): per batch, proxied first; unanswered URIs (or the
  whole batch when the route is unavailable or errored) go to the existing anonymous
  `fetch_public_posts_batch`; still-unanswered posts fall back to their authors'
  PDSes exactly as before. Batches are assembled from the skeleton order and posts
  are inserted by URI, so no fallback can lose or reorder a post. Both routes parse
  through one shared `public_posts_from_views` (same PostView shape); viewer-only
  fields (liked/reposted) are deliberately not read, which keeps the shared
  long-lived post cache safe across viewers and changes nothing rendered.
- **Timing** (`src/net.rs`, `src/oauth.rs`): an outbound call carrying an
  `atproto-proxy` header gets its own kind `pds_proxy` in the per-call timing list.
  `WebPageTiming`'s log + footer list gains route counters:
  `feed_posts_index / feed_posts_proxy / feed_posts_public / feed_posts_pds` and
  `parent_posts_proxy / parent_posts_public / parent_posts_pds`.

## Commits (branch av77q, on de8d23e)

- `949dcae` — RED: the six tests plus fake-server recording of getPosts calls by
  proxy header. (Amended once from `4d7f4e2`: the fake's PostView carried
  `"cid": "bafyfeedpostview"`, which atrium's real CID validator rejects, so test
  (a) could not pass even with a correct implementation; the fake now carries a
  lexicon-valid CID.)
- `c7d0e9c` — fix: the proxied route above, scope constants, `pds_proxy` call kind,
  per-route counters, and the scope-string expectations in `tests/m6_oauth.rs` /
  `tests/m6_oauth_protocol.rs` that track `REQUESTED_SCOPE`.

## Tests (all in `oauth::tests`, reusing the `feed_test_flow` fake PDS/public-AppView
server that records each getPosts call with its `atproto-proxy` header)

- (a) `feed_hydration_for_a_signed_in_viewer_hydrates_through_the_pds_proxy` —
  scope granted: exactly one getPosts on the fake with the
  `did:web:api.bsky.app#bsky_appview` proxy header, ZERO anonymous getPosts, same
  three posts in the same order.
- (b) `feed_hydration_without_the_get_posts_scope_keeps_using_the_public_appview` —
  session stored with bare `atproto` scope: zero proxied calls, one public batch.
- (c) `a_failed_proxied_get_posts_falls_back_to_the_public_appview_keeping_posts_and_order`
  — proxied call answers 500: PDS fake tried first, public fake answers, identical
  posts and order, exactly one WARN line.
- (d) `anonymous_feed_hydration_never_uses_the_pds_proxy` — empty viewer DID:
  public fake only.
- (e) `the_sign_in_scope_requests_the_public_get_posts_rpc` — `REQUESTED_SCOPE`
  contains `rpc:app.bsky.feed.getPosts?aud=*`.
- (f) `the_page_timing_list_labels_the_proxied_get_posts_call_with_its_own_kind` —
  under `RequestOutboundAccounting`, the proxied `/xrpc/app.bsky.feed.getPosts`
  call is listed with kind `pds_proxy`.

## Gate

- **RED on `949dcae`** (`cargo test --release --lib -- feed_hydration_for_a_signed_in
  feed_hydration_without_the_get_posts a_failed_proxied anonymous_feed_hydration
  the_sign_in_scope page_timing_list_labels`):
  `test result: FAILED. 2 passed; 4 failed` — (a), (c), (e), (f) fail for the right
  reason (proxied.len() 0 != 1, "one batched getPosts goes through the viewer's PDS";
  scope missing); (b) and (d) are guards asserting today's behavior and pass before
  and after.
- **Head `c7d0e9c`**, same filter: `test result: ok. 6 passed; 0 failed`.
- `cargo test --release`: **552 passed, 0 failed, 5 ignored** (base de8d23e: 546
  passed, 5 ignored; +6 = the six new tests). Note: `m3_ingest`'s timing-sensitive
  `metrics_are_emitted_during_idle_periods_with_zero_events_and_cursor_lag` failed
  once in a full parallel run under machine load, passes in isolation and in every
  subsequent full run; unrelated to this lane.
- `cargo clippy --release --all-targets -- -D warnings`: clean.
- `cargo fmt --check`: clean.
- `deno test --allow-read=static,src/oauth.rs`: **45 passed, 0 failed**.

LANE-DONE
