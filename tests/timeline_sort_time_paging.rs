use appview_rs::ingest::{
    FILTERED_POST_PAGE_SCAN_LIMIT, FUTURE_POST_HIDE_WINDOW_US, IndexedRecord,
    JetstreamStorageUpdate, SortTimeCursor, SqliteStorage, Storage, filtered_post_page,
    post_is_too_far_in_future, post_sort_us,
};
use std::collections::HashSet;
use std::path::{Path, PathBuf};

const VIEWER: &str = "did:plc:viewer";
const FOLLOWED_A: &str = "did:plc:followed-a";

fn post_uri(did: &str, rkey: &str) -> String {
    format!("at://{did}/app.bsky.feed.post/{rkey}")
}

fn database_path(name: &str) -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join(format!(
        "target/timeline-sort-page-av80d-{}-{name}.sqlite3",
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
        uri: post_uri(did, rkey),
        cid: format!("cid-{rkey}").into(),
        time_us,
        record: serde_json::json!({
            "$type": "app.bsky.feed.post",
            "text": text,
            "createdAt": created_at,
        }),
    }
}

fn follow_record(did: &str, subject: &str) -> IndexedRecord {
    IndexedRecord {
        did: did.into(),
        collection: "app.bsky.graph.follow".into(),
        rkey: format!("f-{}", subject),
        uri: format!("at://{did}/app.bsky.graph.follow/f-{}", subject),
        cid: format!("cid-f-{}", subject).into(),
        time_us: 10,
        record: serde_json::json!({ "subject": subject }),
    }
}

fn seed(path: &Path) {
    remove_database(path);
    let storage = SqliteStorage::open(path).unwrap();
    storage
        .apply_jetstream_batch(&[
            JetstreamStorageUpdate {
                time_us: 10,
                upsert: Some(follow_record(VIEWER, FOLLOWED_A)),
                ..JetstreamStorageUpdate::default()
            },
            JetstreamStorageUpdate {
                time_us: 1_793_800_000_000_003,
                upsert: Some(post_record(
                    FOLLOWED_A,
                    "p1",
                    1_793_800_000_000_003,
                    "2026-11-04T06:42:08Z",
                    "p1",
                )),
                ..JetstreamStorageUpdate::default()
            },
            JetstreamStorageUpdate {
                time_us: 1_793_800_000_000_002,
                upsert: Some(post_record(
                    FOLLOWED_A,
                    "p2",
                    1_793_800_000_000_002,
                    "2026-11-04T06:42:08Z",
                    "p2",
                )),
                ..JetstreamStorageUpdate::default()
            },
            JetstreamStorageUpdate {
                time_us: 1_793_800_000_000_001,
                upsert: Some(post_record(
                    FOLLOWED_A,
                    "p3",
                    1_793_800_000_000_001,
                    "2026-11-04T06:42:08Z",
                    "p3",
                )),
                ..JetstreamStorageUpdate::default()
            },
        ])
        .unwrap();
    drop(storage);
}

#[test]
fn post_sort_timeline_pages_across_equal_sort_times_without_duplicates_or_gaps() {
    let path = database_path("equal");
    seed(&path);
    let storage = SqliteStorage::open(&path).unwrap();

    let first = storage
        .timeline_posts_by_post_time(VIEWER, None, 1)
        .unwrap();
    assert_eq!(
        first
            .iter()
            .map(|record| record.uri.as_str())
            .collect::<Vec<_>>(),
        vec![post_uri(FOLLOWED_A, "p1")],
        "the first page must start at the newest equal-sort-time post"
    );

    let last = first[0].clone();
    let cursor = SortTimeCursor {
        sort_us: storage.sort_us_for_record(&last).unwrap(),
        time_us: last.time_us,
        uri: last.uri.clone(),
    };
    let second = storage
        .timeline_posts_by_post_time(VIEWER, Some(&cursor), 10)
        .unwrap();
    assert_eq!(
        second
            .iter()
            .map(|record| record.uri.as_str())
            .collect::<Vec<_>>(),
        vec![post_uri(FOLLOWED_A, "p2"), post_uri(FOLLOWED_A, "p3")],
        "equal sort times must resume by time_us then uri with no gaps"
    );

    let mut all = first
        .iter()
        .map(|record| record.uri.as_str())
        .collect::<Vec<_>>();
    all.extend(second.iter().map(|record| record.uri.as_str()));
    assert_eq!(
        all.len(),
        all.iter().collect::<HashSet<_>>().len(),
        "pages must not repeat a post"
    );
    assert_eq!(all.len(), 3, "the two pages must cover all three posts");

    drop(storage);
    remove_database(&path);
}

#[test]
fn old_time_us_uri_cursor_does_not_parse_as_a_sort_cursor() {
    assert!(
        SortTimeCursor::parse("1793800000000000:at://did:plc:example/app.bsky.feed.post/p1",)
            .is_none(),
        "an old time_us:uri cursor must not be parsed as a sort cursor"
    );
    assert!(
        SortTimeCursor::parse(
            "1793800000000000|1793800000000000|at://did:plc:example/app.bsky.feed.post/p1",
        )
        .is_some(),
        "the new sort_us|time_us|uri cursor must parse"
    );
}

#[test]
fn sorted_home_timeline_page_visits_a_bounded_number_of_sqlite_steps() {
    let path = database_path("bounded-work");
    remove_database(&path);
    let storage = SqliteStorage::open(&path).unwrap();

    let mut updates = Vec::new();
    for author_index in 0..12 {
        let author = format!("did:plc:bulk-{author_index:02}");
        updates.push(JetstreamStorageUpdate {
            time_us: 10,
            upsert: Some(follow_record(VIEWER, &author)),
            ..JetstreamStorageUpdate::default()
        });
        for post_index in 0..256 {
            let time_us = 1_790_000_000_000_000 + post_index;
            updates.push(JetstreamStorageUpdate {
                time_us,
                upsert: Some(post_record(
                    &author,
                    &format!("p-{post_index:04}"),
                    time_us,
                    "2026-11-04T06:42:08Z",
                    "bulk post",
                )),
                ..JetstreamStorageUpdate::default()
            });
        }
    }
    storage.apply_jetstream_batch(&updates).unwrap();

    let (page, work) = storage
        .timeline_posts_by_post_time_counted(VIEWER, None, 50)
        .unwrap();
    assert_eq!(page.len(), 50);
    assert!(
        work.vm_steps < 25_000,
        "one 50-post page across 12 followed authors should not scan each author's 256-post history; SQLite executed {} VM steps ({} fullscan steps)",
        work.vm_steps,
        work.fullscan_steps
    );

    drop(storage);
    remove_database(&path);
}

fn fixed_now_us() -> u64 {
    let now = time::OffsetDateTime::parse(
        "2026-11-05T00:00:00Z",
        &time::format_description::well_known::Rfc3339,
    )
    .unwrap();
    (now.unix_timestamp_nanos() / 1_000) as u64
}

fn date_after_days(days: i64) -> String {
    let now = time::OffsetDateTime::parse(
        "2026-11-05T00:00:00Z",
        &time::format_description::well_known::Rfc3339,
    )
    .unwrap();
    (now + time::Duration::days(days))
        .format(&time::format_description::well_known::Rfc3339)
        .unwrap()
}

#[test]
fn future_visibility_uses_a_strict_thirty_day_boundary_and_ignores_bad_dates() {
    let now_us = fixed_now_us();
    assert_eq!(FUTURE_POST_HIDE_WINDOW_US, 30 * 24 * 60 * 60 * 1_000_000);
    for (days, expected_hidden) in [(29, false), (30, false), (31, true)] {
        let record = post_record(
            FOLLOWED_A,
            &format!("d{days}"),
            now_us,
            &date_after_days(days),
            "boundary fixture",
        );
        assert_eq!(
            post_is_too_far_in_future(&record, now_us),
            expected_hidden,
            "now + {days} days must have the expected visibility"
        );
    }
    for created_at in [None, Some("not-a-date")] {
        let mut record = post_record(
            FOLLOWED_A,
            "malformed",
            now_us,
            "2999-01-01T00:00:00Z",
            "malformed fixture",
        );
        if let Some(created_at) = created_at {
            record.record["createdAt"] = serde_json::json!(created_at);
        } else {
            record.record.as_object_mut().unwrap().remove("createdAt");
        }
        assert!(!post_is_too_far_in_future(&record, now_us));
    }
}

#[test]
fn future_posts_refill_three_home_pages_without_duplicates_or_gaps() {
    let path = database_path("future-pages");
    remove_database(&path);
    let storage = SqliteStorage::open(&path).unwrap();
    let now_us = fixed_now_us();
    let future_date = date_after_days(31);
    let ordinary_date = date_after_days(-1);
    let mut updates = vec![JetstreamStorageUpdate {
        time_us: 10,
        upsert: Some(follow_record(VIEWER, FOLLOWED_A)),
        ..JetstreamStorageUpdate::default()
    }];
    let mut future_count = 0;
    let mut ordinary_count = 0;
    for index in 0..41 {
        let time_us = now_us - index * 1_000_000;
        let is_future = index % 4 == 0 && future_count < 10;
        let (rkey, created_at, text) = if is_future {
            let rkey = format!("future-{future_count:02}");
            future_count += 1;
            (rkey, future_date.as_str(), "future fixture")
        } else {
            let rkey = format!("ordinary-{ordinary_count:02}");
            ordinary_count += 1;
            (rkey, ordinary_date.as_str(), "ordinary fixture")
        };
        updates.push(JetstreamStorageUpdate {
            time_us,
            upsert: Some(post_record(FOLLOWED_A, &rkey, time_us, created_at, text)),
            ..JetstreamStorageUpdate::default()
        });
    }
    assert_eq!(future_count, 10);
    storage.apply_jetstream_batch(&updates).unwrap();

    let mut cursor = None;
    let mut walked = Vec::new();
    for page_number in 0..3 {
        let (page, has_more, next_cursor) = filtered_post_page(
            cursor.clone(),
            10,
            now_us,
            |cursor, limit| storage.timeline_posts_by_post_time(VIEWER, cursor, limit),
            |record| SortTimeCursor {
                sort_us: post_sort_us(record),
                time_us: record.time_us,
                uri: record.uri.clone(),
            },
        )
        .unwrap();
        assert_eq!(
            page.len(),
            11,
            "page {page_number} has ten posts and lookahead"
        );
        walked.extend(page.iter().take(10).map(|record| record.uri.clone()));
        cursor = next_cursor;
        assert!(has_more, "ordinary posts remain after page {page_number}");
    }
    assert_eq!(walked.len(), 30);
    assert_eq!(walked.iter().collect::<HashSet<_>>().len(), 30);
    assert!(walked.iter().all(|uri| uri.contains("ordinary-")));

    drop(storage);
    remove_database(&path);
}

#[test]
fn five_thousand_future_posts_are_skipped_in_bounded_cursor_pages() {
    let path = database_path("future-bounded-work");
    remove_database(&path);
    let storage = SqliteStorage::open(&path).unwrap();
    let now_us = fixed_now_us();
    let future_date = date_after_days(31);
    let ordinary_date = date_after_days(-1);
    let mut updates = vec![JetstreamStorageUpdate {
        time_us: 10,
        upsert: Some(follow_record(VIEWER, FOLLOWED_A)),
        ..JetstreamStorageUpdate::default()
    }];
    for index in 0..5_000 {
        let time_us = now_us - index * 1_000_000;
        updates.push(JetstreamStorageUpdate {
            time_us,
            upsert: Some(post_record(
                FOLLOWED_A,
                &format!("future-{index:03}"),
                time_us,
                &future_date,
                "future fixture",
            )),
            ..JetstreamStorageUpdate::default()
        });
    }
    for index in 0..80 {
        let time_us = now_us - (10_000 + index) * 1_000_000;
        updates.push(JetstreamStorageUpdate {
            time_us,
            upsert: Some(post_record(
                FOLLOWED_A,
                &format!("ordinary-{index:03}"),
                time_us,
                &ordinary_date,
                "ordinary fixture",
            )),
            ..JetstreamStorageUpdate::default()
        });
    }
    storage.apply_jetstream_batch(&updates).unwrap();

    let mut work = appview_rs::ingest::StatementWork::default();
    let mut rows_read = 0usize;
    let (first_page, has_more, mut cursor) = filtered_post_page(
        None,
        50,
        now_us,
        |cursor, limit| {
            let (records, measured) =
                storage.timeline_posts_by_post_time_counted(VIEWER, cursor, limit)?;
            rows_read += records.len();
            work.vm_steps += measured.vm_steps;
            work.fullscan_steps += measured.fullscan_steps;
            Ok(records)
        },
        |record| SortTimeCursor {
            sort_us: post_sort_us(record),
            time_us: record.time_us,
            uri: record.uri.clone(),
        },
    )
    .unwrap();

    assert!(work.vm_steps < 25_000, "SQLite VM steps: {}", work.vm_steps);
    assert!(
        first_page.is_empty(),
        "the first bounded scan only saw future posts"
    );
    assert!(has_more);
    assert_eq!(rows_read, FILTERED_POST_PAGE_SCAN_LIMIT);
    assert!(
        cursor.is_some(),
        "the capped empty page must carry a cursor"
    );
    eprintln!(
        "future-filter home-page work: {} VM steps, {} fullscan steps",
        work.vm_steps, work.fullscan_steps
    );

    let expected = (0..30)
        .map(|index| format!("ordinary-{index:03}"))
        .collect::<Vec<_>>();
    let mut walked = Vec::new();
    while walked.len() < expected.len() {
        let (page, has_more, next_cursor) = filtered_post_page(
            cursor.clone(),
            10,
            now_us,
            |cursor, limit| storage.timeline_posts_by_post_time(VIEWER, cursor, limit),
            |record| SortTimeCursor {
                sort_us: post_sort_us(record),
                time_us: record.time_us,
                uri: record.uri.clone(),
            },
        )
        .unwrap();
        walked.extend(
            page.iter()
                .take(10)
                .map(|record| record.uri.rsplit('/').next().unwrap().to_owned()),
        );
        cursor = next_cursor;
        assert!(has_more || walked.len() >= expected.len());
    }
    assert_eq!(
        walked, expected,
        "resuming after capped scans has no gap or duplicate"
    );

    drop(storage);
    remove_database(&path);
}
