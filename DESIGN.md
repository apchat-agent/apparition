# Design

## Architecture

The binary uses Axum for HTTP, a `Storage` trait for indexed protocol data, and a Jetstream WebSocket consumer for firehose events. Identity lookups are isolated behind `IdentityBackend`, which keeps network behavior replaceable in offline tests. HTTP-facing XRPC handlers and OAuth will build on these interfaces.

## Storage

SQLite is the embedded store. It is permissively licensed, works without a separate service, and keeps the single-host deployment footprint modest. Records use a composite `(did, collection, rkey)` primary key with a collection/time index; cursors, registered accounts, and follow edges are stored alongside them. WAL mode allows readers while ingestion writes. `MemoryStorage` provides deterministic unit and integration tests. The production interface is `Storage`, so a different engine can be substituted if measured load requires it.

## Scope rules

`scoped` is the default. It indexes records authored by registered users and accounts they follow. Likes and reposts by any actor are indexed when their subject URI belongs to a registered user's repository. Follow edges created by registered users are maintained as scope inputs. `full` indexes all six supported collections from Jetstream. Registration is stored so OAuth can register a DID later.

## Ingest operations

The consumer subscribes to posts, likes, reposts, follows, blocks, and profiles; stores the latest `time_us` cursor; and reconnects to configured endpoints with exponential backoff. SQLite upserts make replay after a crash safe. Periodic structured log events report average events per second and SQLite/WAL bytes on disk.

## Backfill

M4 queues registered users and accounts newly followed by registered users for backfill. The worker fetches `com.atproto.sync.getRepo` from the PDS advertised by each DID document. The CAR reader checks block hashes and MST completeness; the signed commit is checked against the document's `#atproto` key and DID. A verified snapshot replaces the local records and follow edges for that DID in one SQLite transaction. Requests are spaced per PDS host, and oversized CAR files are rejected.

## Current limits

XRPC read APIs and OAuth are not implemented yet. Retention/compaction policy and production load measurements remain open.
