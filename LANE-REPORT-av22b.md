# appview-rs lane av22b: identity cache request isolation

Date: 2026-10-29

## Completed

- The persistent identity cache now lives at `identity-cache.db` in `data_dir`, separate from the high-write `appview.db` records database and `oauth.db`. The cache file uses SQLite WAL mode and is created with its schema on startup without loading cached rows eagerly.
- Identity lookups check a shared in-memory tier first. Cache misses read through a read-only SQLite connection on `spawn_blocking`; database reads do not share a connection or mutex with writes.
- Cache saves and DID invalidations go to one background writer over a bounded 256-entry channel. Callers use nonblocking `try_send`; a full queue drops the update and logs the cumulative drop count at DEBUG.
- Jetstream `identity` and `account` events immediately evict matching memory entries, then queue the SQLite deletion. An invalidation barrier prevents a read already in flight from restoring a stale row.
- No live server was contacted. Nothing was pushed.

## Red runs

- `638d692`: `identity_cache_writer_lock_never_blocks_resolution_or_tokio_progress` failed after **3.229 s** waiting on SQLite instead of completing under 100 ms.
- `0c37b2f`: `account_event_evicts_identity_cache_immediately_while_sqlite_is_locked` failed after **3.350 s** waiting for Jetstream invalidation instead of returning under 100 ms.
- `63c9846`: the separate-path test failed to compile because `identity_cache_path` did not exist.

## Verification

- `cargo test --offline -- --test-threads=1`: **203 passed, 0 failed, 2 ignored**. This includes the existing SSRF, storage, av22, and identity tests unchanged, plus checks for cache writer contention, both-tier invalidation, records database lock isolation, and the dedicated cache path.
- The lock regression tests assert resolution and invalidation complete in under **100 ms** while SQLite holds `BEGIN IMMEDIATE`; the current-thread test also asserts Tokio continues ticking.
- `cargo clippy --offline --all-targets -- -D warnings`: passed.
- `cargo fmt --check`: passed.
- `grep -n "database_path" src/main.rs` shows the records path is only opened by `SqliteStorage`; `SqliteIdentityCache` opens `identity_cache_path(&config.data_dir)`.

## Commits

- `638d692` red: keep identity resolution responsive under cache locks
- `0c37b2f` red: evict identity cache without waiting on sqlite
- `63c9846` red: separate the persistent identity database path
- `b24c56f` green: isolate identity cache IO from records and requests
