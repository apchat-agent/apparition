# Lane report

## 2026-10-27 status

Completed milestones M1–M7 in order. Implementation steps were committed as red tests followed by green changes, with refactors in separate commits where needed. No push was made.

| Milestone | Status | Notes |
| --- | --- | --- |
| M1 Skeleton | Complete | Config, Axum server, health route, structured logs, licenses, and license policy. |
| M2 Identity | Complete | DID/handle resolution, TTL caching, DNS and well-known verification. |
| M3 Ingest | Complete | Configurable Jetstream consumer, cursor persistence, scoped/full SQLite indexing, deletes, event-rate and DB/WAL-size logging. |
| M4 Backfill | Complete | Verified `getRepo` CAR import, scope queue, and per-host rate limiting. |
| M5 XRPC | Complete | Requested profile/feed/graph/notification routes, fixture-shaped responses, AppView DID document, and signed service-JWT checks. |
| M6 OAuth | Complete | Confidential metadata, OAuth login/session/logout, token-scope and identity validation, user-PDS timeline and writes, and offline fake-server protocol coverage. |
| M7 Ops | Complete | Run/config/client guidance, design limits, and idle footprint sample documented. |

### Verification

- `cargo test --locked --offline`: passed, 35 tests (M1: 2, M2: 4, M3: 6, M4: 3, M5: 8, M6: 12).
- `cargo fmt --all -- --check`: passed.
- `cargo clippy --locked --offline -- -D warnings`: passed.
- `cargo deny check licenses`: passed.
- `cargo build --release --locked --offline`: passed.
- Local fake PDS/authorization-server coverage exercises PAR DPoP nonce retry, PKCE/state, bad state, wrong issuer, token exchange, expired-token refresh, and resource-server nonce retry.
- Idle sample on Darwin 25.5.0 arm64: 15,207,792-byte release executable; 12,000 KiB process RSS; 148 KiB allocated empty data directory while running; 73,728-byte SQLite database after clean shutdown. Jetstream was pointed to an unavailable loopback port, with no indexed events.

### Remaining limits

- No throughput, concurrent-load, or long-running disk-growth benchmark was run. The footprint numbers are one idle sample.
- No live-account OAuth run or end-to-end social-app build was performed. The protocol test drives Atrium OAuth against local fake PDS/authorization endpoints; web-route tests inject a fake OAuth flow.
- The AppView remains an early implementation. Search, blob proxying, moderation labels, cursor pagination on routes other than author feed, and data retention/compaction are not implemented. See `DESIGN.md` for detail.

## av2 review fixes (2026-10-27)

### Blocking findings

| Finding | Status | Evidence |
| --- | --- | --- |
| B1 OAuth callback query scope | Fixed | `production_oauth_flow_accepts_standard_callback_and_binds_subject_and_scope` exchanges a callback containing code, state, and issuer without query scope; token response scope is checked. |
| B2 Production OAuth glue coverage | Fixed | The same test instantiates `AtriumOAuthFlow` with a local fake PLC/PDS/authorization server and verifies success, subject mismatch rejection, and missing-scope rejection. Removing either production check makes this test fail. |
| B3 JWT signature and session-cookie MAC coverage | Fixed | `proxy_jwt_rejects_a_valid_claim_set_signed_by_another_key` and `session_cookie_rejects_a_tampered_hmac`. Mutation checks made the JWT test return 200 instead of 401 and the cookie test return 200 instead of redirecting; both checks were restored. |
| B4 XRPC repeated array keys | Fixed | `repeated_xrpc_array_parameters_return_all_values` sends repeated `actors` and `uris` keys and requires both values in each response. The route fixture test also uses repeated `actors`. |
| B5 Login CSRF state binding | Fixed | `callback_rejects_valid_state_without_the_matching_browser_cookie` rejects a valid callback state without the signed pre-login cookie. Atrium state also carries the browser binding and checks it after callback validation. |

### Follow-up findings from the brief

| Finding | Status | Evidence |
| --- | --- | --- |
| F1 Atrium callback panic | Fixed | `callback_turns_an_upstream_panic_into_an_error_response` verifies a 502 HTML error page. |
| F2 Full database loads on read routes | Not fixed | Read routes still use full-record scans; this needs a broader storage-query refactor. |
| F3 Author-feed cursor repeats | Fixed | `author_feed_cursor_advances_between_pages` verifies three distinct pages and advancing cursors. |
| F4 SQLite file permissions | Fixed | `sqlite_databases_are_created_with_owner_only_permissions` verifies 0600 for both storage and OAuth database files; `sqlite_memory_storage_does_not_create_a_disk_file` covers in-memory storage semantics. |
| F5 Logout leaves cookies valid | Fixed | `logging_in_again_invalidates_a_cookie_from_before_logout` exercises the SQLite-backed web flow and confirms the old cookie is rejected after logout and a later login. |
| F6 OAuth key reused for cookie HMAC | Fixed | `oauth::tests::cookie_hmac_key_is_distinct_from_the_oauth_signing_scalar` verifies a domain-separated HKDF-derived key. |
| F8 Private-address SSRF | Not fixed | DID-document and handle-driven fetches still need a configurable private-address policy. |

F7 and F9–F13 were not addressed in av2: write-route CSRF, DID-document caching/token lifetime bounds, fixture completeness, reverse handle verification coverage, Jetstream trust documentation, and configurable identity endpoints.

### Verification

- `cargo fmt --check`: passed.
- `cargo test --locked --offline`: passed, 46 tests.
- `cargo clippy --locked --offline -- -D warnings`: passed.
- `cargo deny check licenses`: passed (`licenses ok`).
- Mutation checks for the JWT signature, cookie HMAC, production OAuth subject-DID check, and production token-scope check each made their corresponding test fail; temporary mutations were restored.
- All changes are committed locally. Nothing was pushed and no remote was used.

## av3 follow-ups (2026-10-27)

| # | Status | Tests | Mutations / red evidence |
| --- | --- | --- | --- |
| 1. Browser binding controls | Done | `production_oauth_flow_rejects_a_foreign_browser_binding`, `web_callback_requires_a_login_cookie_independently_of_the_flow`, `web_callback_rejects_a_forged_login_cookie_independently_of_the_flow` | M10 removed the production binding comparison and the production-flow test accepted the foreign binding. M11 accepted a callback without the pre-login cookie and returned 303. M12 skipped login-cookie MAC validation and the forged-cookie test returned 303. Restored all checks. |
| 2. Clean up exchanged tokens on binding mismatch | Done | `production_oauth_flow_rejects_a_foreign_browser_binding` also checks that the stored session is deleted and the fake server receives `/revoke`. | The new assertions failed before the cleanup change: the exchanged session remained stored. |
| 3. Catch `/oauth/authorize` panic | Done | `authorize_turns_an_upstream_panic_into_an_error_page` | The test failed before the catch was added because the simulated panic escaped the route. The route now returns a 502 HTML page. |
| 4. Protect forced login | Done | `login_page_accepts_an_atproto_handle_or_did`, `authorize_rejects_cross_origin_and_cross_site_requests` | Removing the authorization-start origin check let the cross-origin POST start OAuth and return 303; the test expected 403. Login initiation is now a same-origin POST. |
| 5. Protect write routes and logout | Done | `web_post_actions_and_logout_reject_same_site_cross_origin_requests` | Removed the guard from `/post`, `/like`, `/repost`, and `/logout` one at a time; each mutation made the test fail on that route. Restored every guard. |
| 6. Cookie expiry and JWT algorithm checks | Done | `expired_login_cookie_is_rejected`, `expired_session_cookie_is_rejected`, `proxy_jwt_rejects_an_algorithm_that_does_not_match_the_issuer_key` | M17 ignored login-cookie expiry and its test failed. M16 ignored session-cookie expiry and its test failed. M3 ignored the JWT algorithm check and the validly signed mismatched-algorithm token returned 200. Restored all checks. |
| 7. Independent sessions per device | Done | `logging_in_on_another_device_keeps_the_first_session_valid` runs against both memory and SQLite storage. | The red test returned 303 for the first device after a second login under the old per-DID counter. |
| 8. Private-address fetch policy and indexed read queries | Not done (optional in the brief) | — | DID-document/handle fetch address restrictions and indexed replacements for whole-database read scans remain follow-ups. No mutations were run. |

### av3 verification

- `cargo fmt --check`: passed.
- `cargo test --locked --offline`: passed, 56 tests (unit: 3, M1: 2, M2: 4, M3: 7, M4: 3, M5: 12, M6: 25).
- `cargo clippy --locked --offline -- -D warnings`: passed.
- `cargo deny check licenses`: passed (`licenses ok`).
- Changes were committed locally in red/green steps. Nothing was pushed; no remote is configured.

## av4 localhost development OAuth (2026-10-27)

| Item | Status | Evidence |
| --- | --- | --- |
| OAuth mode configuration | Complete | `oauth_mode` / `APPVIEW_OAUTH_MODE` defaults to confidential, accepts `loopback`, and rejects unknown values. |
| Loopback bind safety | Complete | Config rejects non-loopback binds; a process-level test verifies loopback mode exits on `0.0.0.0`. The callback uses the actual bound address and port. |
| Public localhost client | Complete | The loopback client uses Atrium's `AtprotoLocalhostClientMetadata`, `http://localhost` client ID with redirect and scope query parameters, `none` client auth, and no published JWKS. |
| Browser callback security | Complete | HTTP loopback cookies retain `HttpOnly` and `SameSite=Lax` without `Secure`; HTTP Origin and `Sec-Fetch-Site` checks are tested. |
| OAuth protocol | Complete | The fake PDS/authorization server test exercises loopback PAR, PKCE, DPoP, state, browser binding, issuer, subject, scope, and token exchange. It checks no client assertion or metadata fetch; a confidential-flow assertion verifies the existing client assertion remains. |
| Local run instructions | Complete | README includes the scoped Jetstream command, login URL, and expected redirect/timeline behavior. `--dev` logs the login URL. |

### av4 verification

- `cargo fmt --all -- --check`: passed.
- `cargo test --locked --offline`: passed, 63 tests (unit: 3, M1: 2, M2: 4, M3: 7, M4: 3, M5 config: 5, M5 read API: 11, M6 web OAuth: 24, M6 protocol: 4).
- `cargo clippy --locked --offline -- -D warnings`: passed.
- `cargo deny check licenses`: passed (`licenses ok`).
- No live account login was attempted. Changes are committed locally; nothing was pushed.

## av5 PLC DID URL fix (2026-10-27)

| Item | Status | Evidence |
| --- | --- | --- |
| PLC DID document URL construction | Fixed | `HttpIdentityBackend` appends PLC DIDs as path segments, preserving the configured base path and avoiding `Url::join` treating `did:plc:…` as an absolute URL. |
| Base URL shapes | Covered | `plc_did_is_appended_as_a_path_segment_for_each_base_shape` checks root and prefixed bases with and without trailing slashes. |
| Production HTTP path | Covered | `http_identity_backend_reaches_fake_plc_server_using_did_path` exercises `HttpIdentityBackend` against a local fake PLC server and checks the requested DID path. |
| Related URL construction audit | Complete | Backfill puts DIDs in encoded query parameters; OAuth parses and validates origins and serializes query data; JWT has no DID-based URL construction. Handle and `did:web` identity endpoints are built from validated identity inputs. No other `Url::join` with a DID or handle was found. |

### av5 verification

- Red test step failed against the original code with the `did:plc:…` builder error; green implementation passed both URL construction and fake-server regressions.
- `cargo fmt --check`: passed.
- `cargo test --locked --offline`: passed, 65 tests.
- `cargo clippy --locked --offline --all-targets -- -D warnings`: passed.
- `cargo deny check licenses`: passed (`licenses ok`).
- Red and green changes are committed locally. No live-account login was attempted, and nothing was pushed.

## av6 local-index web UI and granular OAuth permissions (2026-10-27)

| Item | Status | Evidence |
| --- | --- | --- |
| Web timeline, profile, and thread reads | Complete | `/app` shares the local timeline selection with XRPC `getTimeline`; profile and thread read indexed records. `logged_in_web_views_render_from_the_local_index_without_pds_reads` seeds the index and asserts the OAuth/PDS timeline read path is never called. |
| Registration and followed-account backfill | Complete | OAuth registration queues the signed-in DID. A verified repo snapshot for a registered DID queues the accounts it follows. `registered_repo_backfill_queues_followed_accounts_and_tracks_completion` covers both memory and SQLite storage. |
| Empty timeline progress | Complete | `/app` shows the in-scope account count and completed backfills while its local timeline is empty. `empty_timeline_reports_backfill_progress_for_registered_account` checks the initial `Indexing 1 account, 0 done` state. |
| Requested OAuth scope | Complete | Exact scope in confidential metadata, loopback client ID, and confidential/loopback PAR requests: `atproto repo:app.bsky.feed.post?action=create repo:app.bsky.feed.like?action=create&action=delete repo:app.bsky.feed.repost?action=create&action=delete`. No `transition:generic` or RPC scopes are requested. |
| Partial permission grant | Complete | `callback_keeps_partial_write_grants_read_only_and_blocks_actions` verifies an `atproto`-only grant signs in read-only, explains the missing repo grants, hides write controls, and blocks writes with 403. |

### av6 verification

- Red/green commits were made for local web reads, granular scopes/read-only fallback, and backfill/progress behavior.
- `cargo fmt --check`: passed.
- `cargo test --locked --offline`: passed, 68 tests (unit: 4, M1: 2, M2: 5, M3: 7, M4: 4, M5 config: 5, M5 read API: 11, M6 web OAuth: 26, M6 protocol: 4).
- `cargo clippy --locked --offline -- -D warnings`: passed.
- `cargo deny check licenses`: passed (`licenses ok`).
- No external live-account OAuth run was made in this lane; the web and OAuth protocol checks use local fakes. Nothing was pushed.

## av7 repository CAR decoding and retry handling (2026-10-27)

| Item | Status | Evidence |
| --- | --- | --- |
| DAG-CBOR records with CID links and bytes | Fixed | `backfill_decodes_cid_links_and_bytes_into_atproto_json` builds a signed CAR with an image embed blob, a profile avatar, a CID link, and bytes. The verifier returns the ATProto JSON forms `$link` and unpadded standard-base64 `$bytes`, matching the JSON record shape used by Jetstream ingest. |
| One unreadable record | Fixed | `verifier_skips_an_undecodable_record_and_keeps_the_rest_of_the_repo` puts a validly hashed record block with an invalid CID tag in a signed repo CAR. The verifier warns with collection, rkey, and CID, skips that record, and returns the readable post. Commit/signature/MST verification errors still fail the repo. |
| Repeated fetch failures | Fixed | `backfill_worker_backs_off_after_a_failed_repository_fetch` observed 14 requests in 100 ms before the change and one after it. Failure delays double from 5 seconds and cap at one hour; `failed_repository_retry_delay_doubles_and_caps_at_one_hour` checks the sequence against both memory and SQLite storage. |
| Failed progress | Fixed | `empty_timeline_reports_failed_backfill_accounts` verifies `/app` reports `Indexing 1 account, 0 done, 1 failed`. |

### av7 verification

- `cargo test --locked --offline`: passed, 73 tests (unit: 4, M1: 2, M2: 5, M3: 7, M4: 8, M5 config: 5, M5 read API: 11, M6 web OAuth: 27, M6 protocol: 4).
- `cargo fmt --all -- --check`: passed.
- `cargo clippy --locked --offline --all-targets -- -D warnings`: passed.
- `cargo deny check licenses`: passed (`licenses ok`).
- All CAR and backoff tests use local in-memory or local fake components; no live PDS request was made. Changes are committed locally and nothing was pushed.

## av8 web post authors and embeds (2026-10-27)

| Item | Status | Evidence |
| --- | --- | --- |
| Post authors and times | Complete | `web_posts_show_hydrated_authors_and_times_on_timeline_profile_and_thread` verifies display name, verified handle, profile link, and timestamp on timeline, profile, and thread pages. The web renderer shares `profile_view_from_storage` with XRPC profile/post hydration; unknown handles fall back to the DID. |
| Images and record-with-media | Complete | `web_embeds_render_images_cards_quotes_and_escape_user_content` checks image-only and `recordWithMedia` blobs render as CDN full-size `<img>` URLs with escaped alt text. |
| External cards and quoted posts | Complete | The same test checks external title/URL and locally indexed quoted text are displayed. |
| HTML escaping | Complete | The embed regression includes markup in alt, title, and quoted text, and verifies it stays escaped. External links are restricted to HTTP(S). |

### av8 verification

- Author and embed behaviors were added in red/green commits. Their initial failing checks reproduced the missing author and image rendering.
- `cargo test --locked --offline`: passed, 75 tests (unit: 4, M1: 2, M2: 5, M3: 7, M4: 8, M5 config: 5, M5 read API: 11, M6 web OAuth: 29, M6 protocol: 4).
- `cargo fmt --all -- --check`: passed.
- `cargo clippy --locked --offline --all-targets -- -D warnings`: passed.
- `cargo deny check licenses`: passed (`licenses ok`).
- Tests use local indexed records and in-memory storage. No live account or network check was made; nothing was pushed.

## av9 Jetstream silent-read recovery (2026-10-27)

The app-side root cause of the observed stall was that `JetstreamConsumer::consume_connection` had no deadline tied to Jetstream event delivery: after the TLS/WebSocket handshake it waited indefinitely for `websocket.next()` (`src/ingest.rs:1200–1215`). An established peer can keep a connection open, including with control PING/PONG frames, without delivering Jetstream events; ingestion then leaves the cursor stale. Metrics were also triggered only by processed events, so silence produced no metrics line. Code review ruled out the suspected local lock/channel causes: SQLite and OAuth mutexes cover synchronous database calls only, the backfill worker releases its Tokio mutex before awaiting network work, and ingestion has no bounded channel. The exact upstream reason the live peer stopped delivering was not determined; this lane reproduced the app failure mode with a local fake that sent heartbeat frames but no events, and did not access the live server or `live-data`.

| Item | Status | Evidence |
| --- | --- | --- |
| Silent connection watchdog and failover | Fixed | Jetstream data reads now time out after 60 seconds by default, configurable with `jetstream_idle_timeout_secs` / `APPVIEW_JETSTREAM_IDLE_TIMEOUT_SECS`; PING/PONG control frames do not reset the deadline. The consumer drops the connection and rotates through configured URLs. Each reconnect builds its URL from the persisted cursor. `consumer_reconnects_after_a_silent_websocket_and_rotates_urls` uses a heartbeat-only fake followed by a local event server and confirms the second endpoint's event is ingested. |
| Single-event processing deadline | Fixed | Synchronous event processing runs on Tokio's blocking pool and is bounded by 60 seconds by default, configurable with `jetstream_event_timeout_secs` / `APPVIEW_JETSTREAM_EVENT_TIMEOUT_SECS`. A timeout logs the event DID, collection, and rkey and closes the connection. `event_processing_timeout_releases_connection_and_logs_event_identity` holds a local SQLite write transaction to reproduce a stalled storage write, then releases it and checks recovery. |
| Ingestion observability | Complete | Connection and disconnection are logged at INFO. A 10-second timer emits metrics even with `events=0`, including event rate, bytes on disk, and cursor lag in seconds. `metrics_are_emitted_during_idle_periods_with_zero_events_and_cursor_lag` and `websocket_connection_lifecycle_is_logged_at_info` cover these signals. |
| Configuration and docs | Complete | Both timeout settings are available in TOML and environment variables and appear in the example config and README. |

### av9 verification

- Red/green commits: `c452037` / `dcc3d3b` for silent-peer recovery and URL rotation, `8d2cfa7` / `7e249ae` for event deadlines, timeout configuration, and idle metrics, then `1bad44c` / `4596805` to make the watchdog ignore heartbeat-only traffic. `eb1ac48` is a style refactor.
- `cargo test --locked --offline`: passed, 80 tests (unit: 4, M1: 2, M2: 5, M3: 12, M4: 8, M5 config: 5, M5 read API: 11, M6 web OAuth: 29, M6 protocol: 4).
- `cargo fmt --all -- --check`: passed.
- `cargo clippy --locked --offline --all-targets -- -D warnings`: passed.
- `cargo deny check licenses`: passed (`licenses ok`).
- All new regression tests use local fake WebSockets and a SQLite file under `target/`; no live server or external account was used. Changes are committed locally; nothing was pushed.

## av10 reading experience (2026-10-27)

| Item | Status | Evidence |
| --- | --- | --- |
| Indexed post counts and viewer state | Complete | Web and XRPC posts show like, repost, and reply counts. Authenticated XRPC views include `viewer.like` / `viewer.repost`; the web Like button changes to `Liked` and deletes the viewer's indexed like through OAuth `deleteRecord`. Regressions: `post_views_count_indexed_interactions_and_include_the_viewers_own_records`, `web_posts_show_local_counts_and_liked_state_and_submit_unlike`. |
| Reply context | Complete | Timeline replies show the verified parent handle and link into the reply thread. `timeline_replies_name_the_parent_author_and_link_to_the_thread`. |
| Thread traversal | Complete | Web and XRPC thread views render the parent chain, focused post, time-ordered replies, and nested replies. XRPC accepts `depth` and `parentHeight` and returns `threadViewPost` wrappers. `post_thread_returns_parent_chain_and_time_ordered_nested_replies`, `web_thread_renders_the_parent_chain_and_nested_replies_in_time_order`. |
| Notifications | Complete | Logged-in `/notifications` lists likes, reposts, replies, facet mentions, quotes, and follows. XRPC `listNotifications` returns these reasons in the official notification shape; `seenAt` is returned, `updateSeen` is accepted, and unread count covers every reason. `notifications_include_likes_reposts_replies_mentions_quotes_and_follows`, `notifications_page_is_linked_and_shows_all_local_notification_reasons`. |
| Paging and author filters | Complete | Timeline, profile feed, and notifications accept cursor-based Older pages. XRPC `getTimeline` and `getAuthorFeed` honor limit/cursor; author feed supports `posts_with_replies` (default) and `posts_no_replies`. Composite cursors preserve posts and notices with equal timestamps. Regressions: `timeline_paginates_by_cursor_and_author_feed_filters_replies`, `timeline_profile_and_notification_pages_offer_older_cursor_links`, `notification_cursor_keeps_same_timestamp_events_on_later_pages`. |

### av10 SQLite index map

- `records_collection_subject_uri` serves post like/repost counts.
- `records_author_subject_uri` serves the viewer's own like/repost lookup.
- `records_collection_parent_uri` serves reply counts; `records_replies_order` serves thread children ordered by time and URI.
- `records_collection_uri` serves parent/root URI lookups.
- `records_author_time` serves profile feeds and author feeds; `records_author_reply_time` serves `posts_no_replies` filtering.
- `records_follow_actor_subject` serves followed-account selection for timelines.
- `notification_events_subject_time` serves notification listing and unread counts by target DID. The notification event index is maintained on record writes/deletes and backfilled once for existing databases.

### av10 verification

- Red/green commits were made for counts/viewer state, reply context, thread traversal, notification reasons/page, feed and web paging, author filters, and equal-time notification cursors. Subjects end in `(codex av10)`.
- `cargo test --locked --offline`: passed, 90 tests (unit: 4, M1: 2, M2: 5, M3: 12, M4: 8, M5 config: 5, M5 read API: 16, M6 web OAuth: 34, M6 protocol: 4).
- `cargo fmt --all -- --check`: passed.
- `cargo clippy --locked --offline --all-targets -- -D warnings`: passed.
- `cargo deny check licenses`: passed (`licenses ok`).
- All fixtures are local. No live-data or server access occurred, and nothing was pushed.

## av11 handles and scoped interaction counts (2026-10-27)

| Item | Status | Evidence |
| --- | --- | --- |
| Stale verified handles | Fixed | `verified_handle` and reverse handle lookup keep the last verified value past its 30-minute refresh age. `HandleRefreshWorker` periodically refreshes stale handles for registered, followed, and indexed DIDs; transient backend errors keep the old value queued, while a completed failed verification or missing DID document marks it invalid. `stale_sqlite_handle_remains_available_until_refresh_verifies_a_change`, `stale_handle_refresh_updates_after_bidirectional_verification`, `failed_bidirectional_verification_clears_the_last_handle`, and `missing_did_document_invalidates_that_handle_and_does_not_block_the_batch`. |
| Identity and account events | Fixed | Jetstream identity/account events enqueue re-verification when their DID is registered, followed, or already represented in indexed records. A profile request with no prior verification queues a lookup and shows the DID while pending. `tracked_identity_event_queues_handle_reverification` and `profile_shows_the_did_while_initial_handle_verification_is_pending`. The legacy Jetstream subscribe endpoint delivers these event kinds despite the collection filter ([Jetstream documentation](https://github.com/bluesky-social/jetstream-legacy)). |
| Scoped interaction records | Fixed | Scoped ingest stores likes and reposts on registered or followed authors' posts, and posts whose reply parent/root, quote, or facet mention points at one of those DIDs. The target DID is parsed before record-key lookup; previously stored out-of-scope delete events still remove their records and counts. `scoped_ingest_indexes_interactions_and_post_references_on_followed_accounts`. |
| Historical interaction counts | Not backfilled | Scoped mode sees interactions from its current Jetstream cursor onward. Existing older likes, reposts, and replies are not reconstructed. A possible future design is to enqueue rate-limited paginated `getLikes`, `getRepostedBy`, and thread snapshots per tracked post, store snapshot count baselines, and reconcile them with Jetstream create/delete events using a captured cursor and record URI deduplication. No count-backfill code was added. |
| Synthetic ingest and SQLite growth | Measured | A release build processed 25,000 serialized fake `app.bsky.feed.post` commit events through `Ingestor::process_json` into SQLite in 3.019 seconds: 496,874 stored events/minute. The checkpointed database grew by 29,649,184 bytes, about 1,186 bytes per unique stored post. That projects to about 11.3 MiB/day at 10,000 newly stored records/day, before retention; the WAL plus database reached 34,193,656 bytes during the run. This is a local synthetic full-scope run, not a live-stream or network benchmark. |

### av11 verification

- Red/green commits cover handle refresh, scoped interaction capture, pending profile identity, and missing-DID refresh handling; every av11 commit subject ends in `(codex av11)`.
- `cargo test --locked --offline`: passed, 97 tests (unit: 4, M1: 2, M2: 5, M3: 13, M4: 8, M5 config: 5, M5 read API: 17, M6 web OAuth: 34, M6 protocol: 4).
- `cargo fmt --all -- --check`: passed.
- `cargo clippy --locked --offline --all-targets -- -D warnings`: passed.
- `cargo deny check licenses`: passed (`licenses ok`).
- Refresh and interaction regressions use local fake backends and local events. The throughput sample used a generated local event stream and SQLite under this repository's `target/`; no live server or live data was touched. Nothing was pushed.

## av12 public exposure hardening and container preparation (2026-10-28)

The required anonymous-attacker inventory was committed first as `ba73fb3` (`tmp/HARDENING.md`), before the implementation changes. The route-by-route and outbound-fetch inventory remains there.

| Threat surface | Fixed | Evidence |
| --- | --- | --- |
| Outbound HTTP and SSRF | All outbound HTTP is routed through `SafeHttpClient`: HTTPS by default, safe DNS resolution and IP checks, checked-IP pinning, redirect revalidation and hop limits, time and response limits, and a global concurrency bound. This covers identity and handle lookup, OAuth metadata and token calls, Atrium PDS reads/writes, and repository backfill. | `rejects_non_public_ipv4_and_ipv6_ranges`, `resolver_policy_rejects_mixed_and_rebound_dns_answers`, `resolver_policy_allows_a_public_dns_answer`, `guarded_http_request_rechecks_redirect_destinations`, `guarded_http_request_pins_dns_and_caps_response_size`. Source grep for reqwest/hyper client construction finds `reqwest::Client::builder` only in `src/net.rs`; no other direct client/fetch construction was found under `src/`. |
| Public request work | SQLite read routes use bounded indexed storage queries instead of materializing the records table. XRPC pages cap at 100, thread traversal has depth and node budgets, request targets cap at 8 KiB, and bodies cap at 32 KiB. Per-IP and global rate buckets cover login, reads, writes, and outbound-triggering routes; a full bucket map preserves active buckets and denies new peers until expired entries can be pruned. Backfill accepts only registered users' own repositories and recorded follows, holds at most 1,000 DIDs (including queue trimming when an older database opens), and returns eight pending DIDs per worker batch. | `author_feed_caps_a_million_item_page_at_one_hundred`, `extreme_thread_depth_and_reply_fanout_stay_within_a_node_budget`, `eleventh_login_attempt_from_one_ip_returns_429`, `oversized_login_form_body_returns_413`, `active_rate_buckets_are_preserved_when_the_bucket_map_is_full`, `backfill_queue_has_a_global_storage_cap`, `opening_a_database_trims_an_oversized_legacy_backfill_queue`, `pending_backfill_batches_are_bounded`, `backfill_enqueue_requires_a_registered_owner_and_existing_follow`, `registered_owner_cannot_queue_repository_outside_follow_scope`. |
| Sessions, OAuth, and CSRF | Same-origin and fetch-metadata checks cover state-changing POSTs. Cookies are HttpOnly and SameSite=Lax, and Secure on HTTPS. Login state, PKCE, DPoP nonce handling, random session IDs, 0600 SQLite/key files, per-session invalidation, and logout token revocation remain covered. Confidential mode remains available for public URLs; its signing key is stored outside the repo. | Existing regressions include cross-origin authorization/write/logout rejection, cookie Secure policy, state/browser binding, DPoP nonce retry, session rotation, token revocation, and owner-only file permissions. |
| HTML and browser policy | Remote text is HTML-escaped and capped at 4,096 characters. External links allow only HTTP(S) and carry `nofollow noopener noreferrer ugc`. CSP disallows scripts and frames; nosniff, no-referrer, frame denial, and HTTPS-only HSTS are set. | `web_embeds_render_images_cards_quotes_and_escape_user_content`, `web_views_truncate_oversized_remote_post_text`, `health_endpoint_returns_json_ok` (checks both health paths and response headers). |
| Errors and logs | OAuth/upstream/storage errors and JWT verification failures return generic messages. Jetstream lifecycle logs include only endpoint origins, not URL paths or query strings; connection failures are generic. | `oauth_and_write_errors_do_not_echo_tokens_or_internal_paths`, `proxy_jwt_rejects_a_valid_claim_set_signed_by_another_key`, `websocket_connection_lifecycle_is_logged_at_info` (uses a secret query parameter and asserts it is absent from logs). |
| Container | Added a two-stage Debian slim Dockerfile, UID/GID 10001, `/data` volume, configurable listener via `APPVIEW_LISTEN`, environment or mounted TOML configuration, and a `/healthz` Docker healthcheck. README documents the read-only-root run command, TLS proxy placement, data persistence, and key handling. | `health_endpoint_returns_json_ok` covers `/healthz`. No Docker, Podman, Colima, or Buildah executable is installed in this environment, so image build/run and host curls could not be performed. The Dockerfile was reviewed and `git diff --check` passed. |

### Red/green evidence

- `f3d9de8` reproduced a queue size of 1,001; `08bb84c` reproduced unbounded batches and anonymous enqueue; `e3b674a` reproduced a registered owner queueing a DID outside its follows. Green fix: `41b23a1`.
- `83548ef` made the callback echo `access_token=secret` and `/srv/appview/oauth.sqlite`; green fix: `9c29a05`. `ce68cd6` showed the JWT signature diagnostic in an XRPC response; green fix: `5ab90eb`.
- `30b69c3` added the million-item page regression. Temporarily removing the existing clamp made the route return 101 items instead of the required 100; the hard cap was restored and made explicit in `862f1ee`.
- `1e26cd4` added the container probe route check. Temporarily removing `/healthz` made the regression return 404. The route is present in the server wrapper and is checked alongside `/xrpc/_health`.
- `8d9648f` showed the rate limiter accepted a new bucket and evicted an active bucket at capacity; `2b9c1a6` prunes expired buckets and denies new peers while the map remains full. `f319791` captured a Jetstream access token in connection logs; `7101645` logs only the endpoint origin.
- `b275ea5` showed a previously persisted queue with 1,001 items stayed oversized after startup; `3753117` trims old queues to the 1,000 oldest entries.
- Earlier av12 red commits cover SSRF ranges/redirects/rebinding, hostile and oversized rendered content, security headers, login rate limiting, and oversized request bodies. Each fix was kept in a subsequent green commit; no test was removed or weakened.

### Verification

- `cargo test --locked --offline`: passed, 120 tests (unit: 17, av11 identity refresh: 5, M1: 4, M2: 5, M3: 13, M4: 11, M5 config: 5, M5 read API: 20, M6 web OAuth: 36, M6 protocol: 4).
- `cargo fmt --all -- --check`: passed.
- `cargo clippy --locked --offline --all-targets -- -D warnings`: passed.
- `cargo deny check licenses`: passed (`licenses ok`).
- `cargo build --release --locked --offline`: passed; the release binary compiled under the repo's `target/` but does not substitute for the unavailable image build.
- No live AppView, account, or live data was accessed. No cloud resource or image was created, and nothing was pushed.

### Remaining limits and cloud deployment inputs

- No container engine was available, so image construction, `docker run`, `/healthz`, and host-side `/login` checks remain unverified here. The Dockerfile requires the build host to fetch the Rust and Debian base images and Cargo dependencies.
- The per-IP limiter uses the TCP peer address and does not trust forwarded-IP headers. Behind a TLS reverse proxy or load balancer, requests share that peer's bucket; enforce client-IP rate limits at the trusted edge and prevent direct public access to the app port.
- The bounded queue drops new scheduling work while its 1,000-item capacity is full; queueing resumes when space is available and new follow events are processed. This keeps memory and storage bounded but can delay initial account backfills during a sustained full queue.
- Before cloud deployment, the owner must choose a provider, provide the public HTTPS domain/URL and matching `did:web`, and decide how the persistent `/data` volume (including `/data/oauth-client-key.json`) will be backed up or supplied through a secret-management process. No provider, domain, or key secret was available in this lane.
- No public-load, concurrency, or disk-growth benchmark was run. Search and blob-proxy routes do not exist in this version.

## av16 custom feeds (2026-10-28)

Implemented saved custom feed preferences, guarded generator resolution, service-authenticated skeleton requests with anonymous fallback, ordered hydration, and the `/feeds` and `/app/feed` pages. Full red/green commit evidence and remaining work are in [LANE-REPORT-av16.md](LANE-REPORT-av16.md).

- `RUST_TEST_THREADS=1 cargo test --offline`: passed, 160 tests, 1 ignored benchmark. Default-parallel runs hit the previously documented timing-sensitive av14 snapshot lock assertion; serial full suite passed.
- `cargo fmt --all -- --check`: passed.
- `cargo clippy --offline -- -D warnings`: passed.
- No live server was accessed and nothing was pushed.
