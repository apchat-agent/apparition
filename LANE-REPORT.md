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
