use appview_rs::ingest::{IndexedRecord, SqliteStorage, Storage, JetstreamStorageUpdate};
use std::path::{Path, PathBuf};

const VIEWER: &str = "did:plc:viewer";
const FOLLOWED_A: &str = "did:plc:followed-a";
const FOLLOWED_B: &str = "did:plc:followed-b";

fn post_uri(did: &str, rkey: &str) -> String {
    format!("at://{did}/app.bsky.feed.post/{rkey}")
}

fn database_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(format!(
        "target/timeline-sort-av80d-{}-{name}.sqlite3",
        std::process::id()
    ))
}

fn remove_database(path: &Path) {
    let _ = std::fs::remove_file(path);
    let _ = std::fs::remove_file(format!("{}-wal", path.display()));
    let _ = std::fs::remove_file(format!("{}-shm", path.display()));
}

fn post_record(did: &str, rkey: &str, time_us: u64, created_at: &str, text: &str) -> IndexedRecord {
    IndexedRecord {
        did: did.into(),
        collection: "app.bsky.feed.post".into(),
        rkey: rkey.into(),
        uri: post_uri(did, rkey).into(),
        cid: format!("cid-{rkey}").into(),
        time_us,
        record: serde_json::json!({
            "$type": "app.bsky.feed.post",
            "text": text,
            "createdAt": created_at,
        }),
    }
}

fn seed(path: &Path) {
    remove_database(path);
    let storage = SqliteStorage::open(path).unwrap();
    for subject in [FOLLOWED_A, FOLLOWED_B] {
        let follow = IndexedRecord {
            did: VIEWER.into(),
            collection: "app.bsky.graph.follow".into(),
            rkey: format!("f-{}", subject).into(),
            uri: format!("at://{VIEWER}/app.bsky.graph.follow/f-{}", subject).into(),
            cid: format!("cid-f-{}", subject).into(),
            time_us: 10,
            record: serde_json::json!({ "subject": subject }),
        };
        storage.put_record(&follow).unwrap();
    }
    drop(storage);
}

#[test]
fn future_created_at_sorts_at_arrival_not_above_newer_real_post() {
    let path = database_path("future");
    seed(&path);
    let storage = SqliteStorage::open(&path).unwrap();
    let future = post_record(
        FOLLOWED_A,
        "future",
        1_793_700_000_000_000,
        "2099-01-01T00:00:00Z",
        "future-dated",
    );
    let newer = post_record(
        FOLLOWED_B,
        "newer",
        1_793_800_000_000_000,
        "2026-11-04T06:42:08Z",
        "newer",
    );
    storage.apply_jetstream_batch(&[
        JetstreamStorageUpdate {
            time_us: future.time_us,
            upsert: Some(future.clone()),
            ..JetstreamStorageUpdate::default()
        },
        JetstreamStorageUpdate {
            time_us: newer.time_us,
            upsert: Some(newer.clone()),
            ..JetstreamStorageUpdate::default()
        },
    ]).unwrap();
    let page = storage.timeline_posts_by_post_time(VIEWER, None, 10).unwrap();
    let uris = page
        .iter()
        .map(|record| record.uri.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        uris,
        vec![post_uri(FOLLOWED_B, "newer"), post_uri(FOLLOWED_A, "future")],
        "the future-dated post must sort at its arrival time below the newer real post: {uris:?}"
    );
    drop(storage);
    remove_database(&path);
}

#[test]
fn offset_and_fractional_created_at_order_by_real_instant() {
    let path = database_path("offset");
    seed(&path);
    let storage = SqliteStorage::open(&path).unwrap();
    let offset = post_record(
        FOLLOWED_A,
        "offset",
        1_793_800_000_000_000,
        "2026-11-04T06:42:08+02:00",
        "offset",
    );
    let fractional = post_record(
        FOLLOWED_B,
        "fractional",
        1_793_800_000_000_000,
        "2026-11-04T05:00:00.123Z",
        "fractional",
    );
    storage.apply_jetstream_batch(&[
        JetstreamStorageUpdate {
            time_us: offset.time_us,
            upsert: Some(offset.clone()),
            ..JetstreamStorageUpdate::default()
        },
        JetstreamStorageUpdate {
            time_us: fractional.time_us,
            upsert: Some(fractional.clone()),
            ..JetstreamStorageUpdate::default()
        },
    ]).unwrap();
    let page = storage.timeline_posts_by_post_time(VIEWER, None, 10).unwrap();
    let uris = page
        .iter()
        .map(|record| record.uri.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        uris,
        vec![post_uri(FOLLOWED_B, "fractional"), post_uri(FOLLOWED_A, "offset")],
        "the fractional timestamp is a later instant than the +02:00 offset: {uris:?}"
    );
    drop(storage);
    remove_database(&path);
}

#[test]
fn unparseable_created_at_falls_back_to_arrival_and_does_not_sort_on_top() {
    let path = database_path("junk");
    seed(&path);
    let storage = SqliteStorage::open(&path).unwrap();
    let junk = post_record(
        FOLLOWED_A,
        "junk",
        1_793_700_000_000_000,
        "zzz",
        "junk",
    );
    let real = post_record(
        FOLLOWED_B,
        "real",
        1_793_800_000_000_000,
        "2026-11-04T06:42:08Z",
        "real",
    );
    storage.apply_jetstream_batch(&[
        JetstreamStorageUpdate {
            time_us: junk.time_us,
            upsert: Some(junk.clone()),
            ..JetstreamStorageUpdate::default()
        },
        JetstreamStorageUpdate {
            time_us: real.time_us,
            upsert: Some(real.clone()),
            ..JetstreamStorageUpdate::default()
        },
    ]).unwrap();
    let page = storage.timeline_posts_by_post_time(VIEWER, None, 10).unwrap();
    let uris = page
        .iter()
        .map(|record| record.uri.as_str())
        .collect::<Vec<_>>();
    assert_eq!(
        uris,
        vec![post_uri(FOLLOWED_B, "real"), post_uri(FOLLOWED_A, "junk")],
        "an unparseable createdAt must fall back to arrival and not sort on top: {uris:?}"
    );
    drop(storage);
    remove_database(&path);
}
