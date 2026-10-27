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
