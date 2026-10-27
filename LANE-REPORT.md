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
- The AppView remains an early implementation. Search, blob proxying, moderation labels, full notification state, cursor pagination, and data retention/compaction are not implemented. See `DESIGN.md` for detail.
