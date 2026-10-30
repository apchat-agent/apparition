use appview_rs::{
    ingest::{IndexedRecord, MemoryStorage, SqliteStorage, Storage},
    oauth::{OAuthAccount, OAuthCallback, OAuthFlow, OAuthPublic},
};
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use http_body_util::BodyExt;
use rusqlite::Connection;
use serde_json::json;
use std::{
    future::Future,
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
};
use tower::ServiceExt;

const VIEWER: &str = "did:plc:viewer";
const SUBJECT: &str = "did:plc:subject";
const SUBJECT_TWO: &str = "did:plc:subject-two";

struct TempDirGuard(PathBuf);

impl TempDirGuard {
    fn new(path: impl AsRef<Path>) -> Self {
        std::fs::create_dir_all(path.as_ref()).unwrap();
        Self(path.as_ref().to_owned())
    }
}

impl Drop for TempDirGuard {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn temp_dir(case: &str) -> TempDirGuard {
    TempDirGuard::new(std::env::temp_dir().join(format!(
        "appview-follow-integrity-{}-{}-{}",
        case,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    )))
}

fn follow_record(actor: &str, subject: &str, rkey: &str) -> IndexedRecord {
    IndexedRecord {
        did: actor.to_owned(),
        collection: "app.bsky.graph.follow".to_owned(),
        rkey: rkey.to_owned(),
        uri: format!("at://{actor}/app.bsky.graph.follow/{rkey}"),
        cid: None,
        time_us: 1,
        record: json!({"subject": subject}),
    }
}

#[derive(Clone)]
struct AcceptingFlow;

impl OAuthFlow for AcceptingFlow {
    fn authorize<'a>(
        &'a self,
        _: &'a str,
        _: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<String, String>> + Send + 'a>> {
        Box::pin(async { Ok("https://auth.example/authorize".to_owned()) })
    }
    fn callback<'a>(
        &'a self,
        _: OAuthCallback,
    ) -> Pin<Box<dyn Future<Output = Result<OAuthAccount, String>> + Send + 'a>> {
        Box::pin(async {
            Ok(OAuthAccount {
                did: VIEWER.to_owned(),
                verified_handle: Some("viewer.test".to_owned()),
                granted_scope: Some("atproto".to_owned()),
            })
        })
    }
    fn logout<'a>(
        &'a self,
        _: &'a str,
    ) -> Pin<Box<dyn Future<Output = Result<(), String>> + Send + 'a>> {
        Box::pin(async { Ok(()) })
    }
}

async fn login(router: &Router) -> String {
    let start = router
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/oauth/authorize")
                .header("origin", "https://appview.example")
                .header("sec-fetch-site", "same-origin")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from("handle=viewer.test"))
                .unwrap(),
        )
        .await
        .unwrap();
    let login_cookie = start.headers()["set-cookie"]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    let callback = router
        .clone()
        .oneshot(
            Request::builder()
                .uri("/oauth/callback?code=valid-code&state=valid-state&scope=atproto")
                .header("cookie", login_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(callback.status(), StatusCode::SEE_OTHER);
    callback
        .headers()
        .get_all("set-cookie")
        .iter()
        .find_map(|header| {
            let value = header.to_str().ok()?;
            value
                .starts_with("appview_session=")
                .then(|| value.split(';').next().unwrap().to_owned())
        })
        .unwrap()
}

async fn request_html(router: &Router, uri: &str, cookie: &str) -> String {
    let response = router
        .clone()
        .oneshot(
            Request::builder()
                .uri(uri)
                .header("cookie", cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK, "{uri}");
    String::from_utf8(
        response
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap()
}

// (a) The viewer has a follow record for the subject but no `follows` row:
// the profile page must show the Unfollow state, not "Follow".
#[tokio::test]
async fn profile_page_shows_unfollow_when_viewer_has_only_a_follow_record() {
    let storage = Arc::new(MemoryStorage::default());
    storage
        .put_record(&follow_record(VIEWER, SUBJECT, "follow-subject"))
        .unwrap();
    let app = OAuthPublic::new("https://appview.example")
        .unwrap()
        .router_with_flow(AcceptingFlow, storage.clone());
    let cookie = login(&app).await;
    assert!(!storage.is_following(VIEWER, SUBJECT).unwrap());

    let html = request_html(
        &app,
        "/profile?did=did%3Aplc%3Asubject",
        &cookie,
    )
    .await;
    assert!(
        html.contains("name=\"action\" value=\"unfollow\""),
        "follow record without a follows row must show Unfollow: {html}"
    );
    assert!(html.contains(">Following</button>"), "{html}");
    assert!(
        !html.contains("name=\"action\" value=\"follow\""),
        "page offered a Follow action despite the viewer's follow record: {html}"
    );
}

// (e) The logout path unregisters the viewer, which deleted his `follows` rows
// while his follow records stayed in `records`: his profiles then showed
// "Follow" everywhere until a re-backfill completed.
#[test]
fn unregistering_a_user_must_not_empty_follows_rows_their_records_still_back() {
    let dir = temp_dir("unregister");
    let path = dir.0.join("appview.db");
    let storage = SqliteStorage::open(&path).unwrap();
    storage.register_user(VIEWER).unwrap();
    storage
        .put_record(&follow_record(VIEWER, SUBJECT, "follow-subject"))
        .unwrap();
    storage
        .put_record(&follow_record(VIEWER, SUBJECT_TWO, "follow-two"))
        .unwrap();
    storage.set_follow(VIEWER, SUBJECT, true).unwrap();
    storage.set_follow(VIEWER, SUBJECT_TWO, true).unwrap();

    storage.unregister_user(VIEWER).unwrap();

    assert_eq!(
        storage
            .records_for_actor_collection(VIEWER, "app.bsky.graph.follow", 10_000)
            .unwrap()
            .len(),
        2,
        "the follow records must survive (they always did)"
    );
    assert!(
        storage.is_following(VIEWER, SUBJECT).unwrap(),
        "unregistering wiped the follows rows even though the follow records are intact"
    );
    assert!(storage.is_following(VIEWER, SUBJECT_TWO).unwrap());
    drop(storage);
}

// (e2) Registration rebuilds a user's `follows` rows from the follow records
// already present in `records`, with the normal insert path's backfill side
// effects, so a re-login after the wipe recovers without waiting for a
// re-backfill.
#[test]
fn registration_rebuilds_follow_rows_from_existing_follow_records() {
    let dir = temp_dir("register-rebuild");
    let path = dir.0.join("appview.db");
    let storage = SqliteStorage::open(&path).unwrap();
    storage
        .put_record(&follow_record(VIEWER, SUBJECT, "follow-subject"))
        .unwrap();
    storage
        .put_record(&follow_record(VIEWER, SUBJECT_TWO, "follow-two"))
        .unwrap();

    storage.register_user(VIEWER).unwrap();

    assert!(
        storage.is_following(VIEWER, SUBJECT).unwrap(),
        "registering with intact follow records must rebuild the follows row"
    );
    assert!(storage.is_following(VIEWER, SUBJECT_TWO).unwrap());
    assert_eq!(storage.tracked_follow_count(SUBJECT).unwrap(), 1);
    assert_eq!(storage.tracked_follow_count(SUBJECT_TWO).unwrap(), 1);
    let pending = storage.pending_backfills().unwrap();
    assert!(
        pending.iter().any(|did| did == SUBJECT),
        "rebuilt subjects keep the normal insert path's backfill queue side effect: {pending:?}"
    );
    assert!(pending.iter().any(|did| did == SUBJECT_TWO));
    drop(storage);
}

// (f) Startup repair: a registered user with N follow records and 0 `follows`
// rows ends with N following=1 rows after the storage is reopened.
#[test]
fn startup_rebuilds_missing_follow_rows_for_a_registered_user() {
    let dir = temp_dir("startup-repair");
    let path = dir.0.join("appview.db");
    {
        let storage = SqliteStorage::open(&path).unwrap();
        storage.register_user(VIEWER).unwrap();
        storage
            .put_record(&follow_record(VIEWER, SUBJECT, "follow-subject"))
            .unwrap();
        storage
            .put_record(&follow_record(VIEWER, SUBJECT_TWO, "follow-two"))
            .unwrap();
        assert_eq!(storage.records().unwrap().len(), 2);
    }
    {
        let connection = Connection::open(&path).unwrap();
        connection
            .execute("DELETE FROM follows WHERE actor=?1", [VIEWER])
            .unwrap();
        assert_eq!(
            connection
                .query_row(
                    "SELECT COUNT(*) FROM follows WHERE actor=?1 AND following=1",
                    [VIEWER],
                    |row| row.get::<_, i64>(0)
                )
                .unwrap(),
            0
        );
    }

    let storage = SqliteStorage::open(&path).unwrap();

    assert!(
        storage.is_following(VIEWER, SUBJECT).unwrap(),
        "startup must rebuild the missing follows rows from the follow records"
    );
    assert!(storage.is_following(VIEWER, SUBJECT_TWO).unwrap());
    assert_eq!(storage.tracked_follow_count(SUBJECT).unwrap(), 1);
    drop(storage);
}

#[test]
fn startup_keeps_a_healthy_user_rows_untouched() {
    let dir = temp_dir("startup-healthy");
    let path = dir.0.join("appview.db");
    {
        let storage = SqliteStorage::open(&path).unwrap();
        storage
            .put_record(&follow_record(VIEWER, SUBJECT, "follow-subject"))
            .unwrap();
        storage.register_user(VIEWER).unwrap();
        storage.set_follow(VIEWER, "did:plc:gone", false).unwrap();
    }

    let storage = SqliteStorage::open(&path).unwrap();

    assert!(storage.is_following(VIEWER, SUBJECT).unwrap());
    assert!(!storage.is_following(VIEWER, "did:plc:gone").unwrap());
    drop(storage);
}
