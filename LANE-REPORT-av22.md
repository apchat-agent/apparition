# Lane report av22

Date: 2026-10-29

## Completed

- `SafeHttpClient` resolves and vets DNS on every request, then reuses a `reqwest::Client` keyed by the exact lowercase hostname and its sorted vetted socket-address set. Preserving the trailing dot in the host key is required because reqwest applies DNS overrides to the exact host spelling. A changed address set uses a different client; a private or mixed DNS answer is rejected before client selection. Redirect destinations continue to be resolved and vetted on every hop.
- The client cache is shared by clones, uses a 512-entry LRU bound, and expires entries after 90 seconds idle. Reqwest's own idle connection timeout is also 90 seconds. HTTP/2 support is enabled, so concurrent requests to the same PDS origin can multiplex when that origin negotiates HTTP/2.
- Verified identity results are persisted in the records SQLite database: handle-to-DID assertion, DID document (including its PDS endpoint), and DID-to-verified-handle mapping. Positive results live for 24 hours; failures live for 5 minutes. Persisted results are used across resolver restarts and are checked before any resolver-local cache, so separate app components see Jetstream invalidations immediately.
- Jetstream `identity` and `account` events invalidate all persisted rows for their DID. Tracked or displayed DIDs continue through the existing handle re-verification queue. The current v1 subscription already delivers these event kinds independently of `wantedCollections`; the filter applies to commit events, so no subscription change was needed. See the [Jetstream event delivery contract](https://github.com/bluesky-social/jetstream/blob/main/docs/README.md).
- Feed skeleton hydration already fans out up to 16 individual post fetches concurrently. Those requests now share the vetted client for the PDS host and address set, and can use one HTTP/2 connection when supported. `getRecord` remains one XRPC request per post.
- No Slingshot or `public.api.bsky.app` fetch source was added. No live server was contacted.

## Red runs

- `156f77c`: `repeated_requests_to_one_host_reuse_one_connection` failed with **5 accepts**, expected **1**.
- `4fe5375`: the new persistent identity tests failed to compile because `SqliteIdentityCache`, `with_persistent_cache`, and `with_identity_cache` did not exist yet.
- `902b414`: `client_pool_keeps_reqwest_dns_override_names_separate` failed with **1 cached client**, expected **2** for `safe.example.` and `safe.example`.

## Measurements

- Fake HTTP/1.1 server, five sequential requests to one fake host: **5 TCP accepts before**, **1 TCP accept after** pooling (80% fewer accepts). The after count is asserted in `src/net.rs`.
- The same vetted address set reuses its client. The exact-host regression test verifies that distinct reqwest DNS override names cannot share a client. Existing private, mixed-answer, rebinding, and redirect SSRF tests pass unchanged.
- For live connection counts, reqwest emits one `DEBUG` event from `reqwest::connect` when it starts each new connection. `init_tracing` bridges `log` events to tracing; run with `RUST_LOG=reqwest::connect=debug,appview_rs=info` to expose those lines. The additional `created outbound HTTP connection pool` event marks client-cache misses, not socket opens.
- The 16-way fan-out was already present in av21. This lane did not repeat appview-host timings or touch the live server; av21's phase-timing events remain the live latency instrument.

## Verification

- `cargo test --offline -- --test-threads=1`: **199 passed, 0 failed, 2 ignored**.
- `cargo clippy --offline --all-targets -- -D warnings`: passed.
- `cargo fmt --check`: passed.
- The prior SSRF test cases were not edited or weakened.

## Commits

- `156f77c` red: assert outbound requests reuse connections
- `1c5eb5a` green: pool vetted outbound HTTP clients
- `4fe5375` red: cover persistent identity cache behavior
- `13a8da8` green: persist and refresh verified identities
- `902b414` red: keep DNS override hostnames distinct
- `9f2fd8e` green: preserve exact DNS pool keys

No changes were pushed.
