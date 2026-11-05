#!/bin/sh
set -eu

if [ "$#" -ne 1 ]; then
    printf 'usage: %s DBPATH\n' "$0" >&2
    exit 2
fi

dbpath=$1
if [ ! -r "$dbpath" ]; then
    printf 'database is not readable: %s\n' "$dbpath" >&2
    exit 2
fi

case "$dbpath" in
    /*) ;;
    *) dbpath=$(cd "$(dirname "$dbpath")" && pwd)/$(basename "$dbpath") ;;
esac

# This script deliberately contains EXPLAIN QUERY PLAN only. Open via SQLite's
# URI read-only mode so running the plan inventory cannot alter a production DB.
# A closed WAL database may have no -wal/-shm files, which SQLite cannot open in
# ordinary read-only WAL mode. In that case immutable mode reads the checkpointed
# main file; it is still opened read-only and never runs a workload query.
db_uri="file:${dbpath}?mode=ro"
if ! sqlite3 -bail "$db_uri" 'EXPLAIN QUERY PLAN SELECT 1;' >/dev/null 2>&1; then
    db_uri="file:${dbpath}?mode=ro&immutable=1"
fi
exec sqlite3 -bail "$db_uri" <<'SQL'
.headers off
.mode column
.print 'getPosts: requested records_by_uri batch (25 URIs)'
EXPLAIN QUERY PLAN
SELECT did, collection, rkey, uri, cid, time_us, record_json FROM records
WHERE collection=?1 AND uri IN (?2,?3,?4,?5,?6,?7,?8,?9,?10,?11,?12,?13,?14,?15,?16,?17,?18,?19,?20,?21,?22,?23,?24,?25,?26);
.print 'getAuthorFeed / profile post page'
EXPLAIN QUERY PLAN
SELECT did, collection, rkey, uri, cid, time_us, record_json FROM records
WHERE collection='app.bsky.feed.post' AND did=?1
AND (?2 IS NULL OR time_us<?2 OR (time_us=?2 AND uri<?3))
ORDER BY time_us DESC,uri DESC LIMIT ?4;
.print 'profile post page excluding replies'
EXPLAIN QUERY PLAN
SELECT did, collection, rkey, uri, cid, time_us, record_json FROM records
WHERE collection='app.bsky.feed.post' AND did=?1
AND json_type(record_json,'$.reply') IS NULL
AND (?2 IS NULL OR time_us<?2 OR (time_us=?2 AND uri<?3))
ORDER BY time_us DESC,uri DESC LIMIT ?4;
.print 'getFollows: actor ordered page'
EXPLAIN QUERY PLAN
SELECT did,collection,rkey,uri,cid,time_us,record_json FROM records
WHERE collection='app.bsky.graph.follow' AND did=?1
ORDER BY time_us DESC,uri DESC LIMIT ?2;
.print 'getPostThread: ancestor and root exact URI lookup'
EXPLAIN QUERY PLAN
SELECT did,collection,rkey,uri,cid,time_us,record_json FROM records
WHERE collection='app.bsky.feed.post' AND uri=?1;
.print 'getTimeline: followed-author discovery'
EXPLAIN QUERY PLAN
SELECT DISTINCT json_extract(record_json,'$.subject') FROM records
WHERE collection='app.bsky.graph.follow' AND did=?1
AND json_extract(record_json,'$.subject') IS NOT NULL;
.print 'getTimeline: per-author post page'
EXPLAIN QUERY PLAN
SELECT did,collection,rkey,uri,cid,time_us,record_json FROM records
WHERE collection='app.bsky.feed.post' AND did=?1
AND (?2 IS NULL OR time_us<?2 OR (time_us=?2 AND uri<?3))
ORDER BY time_us DESC,uri DESC LIMIT ?4;
.print 'getTimeline post_sort: filled author range'
EXPLAIN QUERY PLAN
SELECT r.did,r.collection,r.rkey,r.uri,r.cid,r.time_us,r.record_json,p.sort_us
FROM post_sort AS p INDEXED BY post_sort_author_order
JOIN records AS r INDEXED BY records_collection_uri
ON r.collection='app.bsky.feed.post' AND r.uri=p.uri
WHERE p.did=?1 AND (p.sort_us,p.time_us,p.uri)<(?2,?3,?4)
ORDER BY p.sort_us DESC,p.time_us DESC,p.uri DESC LIMIT ?5;
.print 'getTimeline post_sort: unfilled fallback (only while backfill remains)'
EXPLAIN QUERY PLAN
SELECT r.did,r.collection,r.rkey,r.uri,r.cid,r.time_us,r.record_json FROM records AS r
INDEXED BY records_author_time
WHERE r.collection='app.bsky.feed.post' AND r.did=?1
AND NOT EXISTS(SELECT 1 FROM post_sort AS p WHERE p.uri=r.uri)
AND (?2 IS NULL OR r.time_us<?2 OR (r.time_us=?2 AND (r.time_us<?3 OR (r.time_us=?3 AND r.uri<?4))))
ORDER BY r.time_us DESC,r.uri DESC LIMIT ?5;
.print 'getFollowers: subject ordered page'
EXPLAIN QUERY PLAN
SELECT did,collection,rkey,uri,cid,time_us,record_json FROM records
INDEXED BY records_follow_subject_time
WHERE collection='app.bsky.graph.follow' AND json_extract(record_json,'$.subject')=?1
ORDER BY time_us DESC,uri DESC LIMIT ?2;
.print 'getPostThread: reply page'
EXPLAIN QUERY PLAN
SELECT did,collection,rkey,uri,cid,time_us,record_json FROM records
WHERE collection='app.bsky.feed.post' AND json_extract(record_json,'$.reply.parent.uri')=?1
ORDER BY time_us ASC,uri ASC LIMIT ?2;
.print 'getLikes / getRepostedBy: subject page'
EXPLAIN QUERY PLAN
SELECT did,collection,rkey,uri,cid,time_us,record_json FROM records
WHERE collection=?1 AND json_extract(record_json,'$.subject.uri')=?2
ORDER BY time_us DESC,uri DESC LIMIT ?3;
.print 'getProfile: three local counts'
EXPLAIN QUERY PLAN
SELECT
(SELECT COUNT(*) FROM records WHERE collection='app.bsky.graph.follow' AND did=?1),
(SELECT COUNT(*) FROM records WHERE collection='app.bsky.graph.follow' AND json_extract(record_json,'$.subject')=?1),
(SELECT COUNT(*) FROM records WHERE collection='app.bsky.feed.post' AND did=?1);
.print 'notifications list: ordered event page plus source record join'
EXPLAIN QUERY PLAN
SELECT r.did,r.collection,r.rkey,r.uri,r.cid,r.time_us,r.record_json,n.reason,n.reason_subject
FROM notification_events n
JOIN records r ON r.did=n.source_did AND r.collection=n.source_collection AND r.rkey=n.source_rkey
WHERE n.subject_did=?1 AND (?2 IS NULL OR n.time_us<?2 OR (n.time_us=?2 AND (n.source_uri<?3 OR (n.source_uri=?3 AND n.reason<?4))))
ORDER BY n.time_us DESC,n.source_uri DESC,n.reason DESC LIMIT ?5;
.print 'notifications list: event-window page'
EXPLAIN QUERY PLAN
SELECT source_did,source_collection,source_rkey,source_uri,reason,reason_subject,time_us
FROM notification_events
WHERE subject_did=?1 AND (?2 IS NULL OR time_us<?2 OR (time_us=?2 AND (source_uri<?3 OR (source_uri=?3 AND reason<?4))))
ORDER BY time_us DESC,source_uri DESC,reason DESC LIMIT ?5;
.print 'post page cached interaction counts: batched URI lookup'
EXPLAIN QUERY PLAN
SELECT uri,like_count,repost_count,reply_count,quote_count FROM post_counts
WHERE uri IN (SELECT value FROM json_each(?1));
.print 'post interaction cache fill: likes or reposts, one indexed aggregation per relation and URI batch'
EXPLAIN QUERY PLAN
SELECT json_extract(record_json,'$.subject.uri'),COUNT(*) FROM records
INDEXED BY records_collection_subject_uri
WHERE collection=?1 AND json_extract(record_json,'$.subject.uri') IN (SELECT value FROM json_each(?2))
GROUP BY 1;
.print 'post interaction cache fill: replies, one indexed aggregation per URI batch'
EXPLAIN QUERY PLAN
SELECT json_extract(record_json,'$.reply.parent.uri'),COUNT(*) FROM records
INDEXED BY records_collection_parent_uri
WHERE collection='app.bsky.feed.post' AND json_extract(record_json,'$.reply.parent.uri') IN (SELECT value FROM json_each(?1))
GROUP BY 1;
.print 'post interaction cache fill: quotes, one indexed aggregation per URI batch'
EXPLAIN QUERY PLAN
SELECT json_extract(record_json,'$.embed.record.uri'),COUNT(*) FROM records
INDEXED BY records_collection_embed_record_uri
WHERE collection='app.bsky.feed.post' AND json_extract(record_json,'$.embed.record.uri') IN (SELECT value FROM json_each(?1))
GROUP BY 1;
.print 'signed post page: viewer like/repost URI lookup'
EXPLAIN QUERY PLAN
SELECT subject_uri,uri FROM (
SELECT json_extract(record_json,'$.subject.uri') AS subject_uri,uri,
ROW_NUMBER() OVER(PARTITION BY json_extract(record_json,'$.subject.uri') ORDER BY time_us DESC,uri DESC) AS newest
FROM records INDEXED BY records_author_subject_uri
WHERE collection=?1 AND did=?2 AND json_extract(record_json,'$.subject.uri') IN (SELECT value FROM json_each(?3))
) WHERE newest=1;
SQL
