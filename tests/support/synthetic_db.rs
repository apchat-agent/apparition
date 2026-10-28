use rusqlite::{Connection, params};
use std::{error::Error, path::Path};

use appview_rs::ingest::SqliteStorage;

const TRACKED_DIDS: usize = 300;
const REGISTERED_DIDS: usize = 200;
const UNTRACKED_POSTS: usize = 780_000;
const TRACKED_POSTS: usize = 20_000;
const LIKES: usize = 100_000;
const REPOSTS: usize = 100_000;
const FOLLOW_HISTORY_PER_REGISTERED: usize = 5_700;
const NOTIFICATIONS: usize = 200_000;

pub fn ensure_database(path: &Path) -> Result<bool, Box<dyn Error>> {
    if path.exists() {
        let connection = Connection::open(path)?;
        let count: i64 =
            connection.query_row("SELECT COUNT(*) FROM records", [], |row| row.get(0))?;
        if count > 0 {
            return Ok(false);
        }
    }

    drop(SqliteStorage::open(path)?);
    let mut connection = Connection::open(path)?;
    connection.pragma_update(None, "synchronous", "OFF")?;
    connection.pragma_update(None, "cache_size", -262_144_i64)?;
    let transaction = connection.transaction()?;

    {
        let mut insert =
            transaction.prepare_cached("INSERT INTO registered_users(did) VALUES(?1)")?;
        for index in 0..REGISTERED_DIDS {
            insert.execute([registered_did(index)])?;
        }
    }

    {
        let mut insert = transaction
            .prepare_cached("INSERT INTO follows(actor,subject,following) VALUES(?1,?2,?3)")?;
        for actor in 0..REGISTERED_DIDS {
            for target in 0..TRACKED_DIDS {
                insert.execute(params![registered_did(actor), tracked_did(target), 1_i64])?;
            }
            for old_follow in 0..FOLLOW_HISTORY_PER_REGISTERED {
                let subject = format!(
                    "did:plc:old-follow-{}",
                    actor * FOLLOW_HISTORY_PER_REGISTERED + old_follow
                );
                insert.execute(params![registered_did(actor), subject, 0_i64])?;
            }
        }
    }

    {
        let mut insert = transaction.prepare_cached(
            "INSERT INTO records(did,collection,rkey,uri,cid,time_us,record_json)
             VALUES(?1,?2,?3,?4,?5,?6,?7)",
        )?;
        let tracked_post = r#"{"$type":"app.bsky.feed.post","text":"synthetic tracked post"}"#;
        let plain_post = r#"{"$type":"app.bsky.feed.post","text":"synthetic untracked post"}"#;
        for index in 0..TRACKED_POSTS {
            let author = index % TRACKED_DIDS;
            let rkey = format!("tracked-post-{index}");
            let uri = format!(
                "at://{}/{}/{}",
                tracked_did(author),
                "app.bsky.feed.post",
                rkey
            );
            insert.execute(params![
                tracked_did(author),
                "app.bsky.feed.post",
                rkey,
                uri,
                format!("cid-tracked-{index}"),
                1_700_000_000_000_000_i64 + index as i64,
                tracked_post,
            ])?;
        }
        for index in 0..UNTRACKED_POSTS {
            let author = index % 20_000;
            let did = format!("did:plc:synthetic-untracked-{author:05}");
            let rkey = format!("untracked-post-{index}");
            let uri = format!("at://{did}/app.bsky.feed.post/{rkey}");
            insert.execute(params![
                did,
                "app.bsky.feed.post",
                rkey,
                uri,
                format!("cid-untracked-{index}"),
                1_700_000_100_000_000_i64 + index as i64,
                plain_post,
            ])?;
        }
        for (collection, count, prefix) in [
            ("app.bsky.feed.like", LIKES, "like"),
            ("app.bsky.feed.repost", REPOSTS, "repost"),
        ] {
            for index in 0..count {
                let author = index % 20_000;
                let did = format!("did:plc:synthetic-untracked-{author:05}");
                let rkey = format!("{prefix}-{index}");
                let subject_author = index % TRACKED_DIDS;
                let target_uri = format!(
                    "at://{}/{}/tracked-post-{subject_author}",
                    tracked_did(subject_author),
                    "app.bsky.feed.post"
                );
                let uri = format!("at://{did}/{collection}/{rkey}");
                let record =
                    format!("{{\"subject\":{{\"uri\":\"{target_uri}\",\"cid\":\"cid-target\"}}}}");
                insert.execute(params![
                    did,
                    collection,
                    rkey,
                    uri,
                    format!("cid-{prefix}-{index}"),
                    1_700_000_200_000_000_i64 + index as i64,
                    record,
                ])?;
            }
        }
    }

    {
        let mut insert = transaction.prepare_cached(
            "INSERT INTO notification_events
             (source_did,source_collection,source_rkey,source_uri,reason,subject_did,reason_subject,time_us)
             VALUES(?1,?2,?3,?4,?5,?6,?7,?8)",
        )?;
        for index in 0..NOTIFICATIONS {
            let author = index % 20_000;
            let source_did = format!("did:plc:synthetic-untracked-{author:05}");
            let collection = if index % 2 == 0 {
                "app.bsky.feed.like"
            } else {
                "app.bsky.feed.repost"
            };
            let reason = if index % 2 == 0 { "like" } else { "repost" };
            let source_rkey = format!("synthetic-notification-{index}");
            let source_uri = format!("at://{source_did}/{collection}/{source_rkey}");
            let target = index % TRACKED_DIDS;
            let reason_subject = format!(
                "at://{}/{}/tracked-post-{target}",
                tracked_did(target),
                "app.bsky.feed.post"
            );
            insert.execute(params![
                source_did,
                collection,
                source_rkey,
                source_uri,
                reason,
                tracked_did(target),
                reason_subject,
                1_700_000_300_000_000_i64 + index as i64,
            ])?;
        }
    }

    transaction.commit()?;
    connection.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")?;
    Ok(true)
}

pub fn row_counts(path: &Path) -> Result<(i64, i64, i64), Box<dyn Error>> {
    let connection = Connection::open(path)?;
    Ok((
        connection.query_row("SELECT COUNT(*) FROM records", [], |row| row.get(0))?,
        connection.query_row("SELECT COUNT(*) FROM follows", [], |row| row.get(0))?,
        connection.query_row("SELECT COUNT(*) FROM notification_events", [], |row| {
            row.get(0)
        })?,
    ))
}

fn registered_did(index: usize) -> String {
    format!("did:plc:synthetic-registered-{index:03}")
}

fn tracked_did(index: usize) -> String {
    format!("did:plc:synthetic-tracked-{index:03}")
}
