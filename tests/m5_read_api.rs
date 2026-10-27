use appview_rs::{
    api::api_router,
    identity::{
        DidDocument, IdentityBackend, IdentityResolver, ResolutionError, VerificationMethod,
    },
    ingest::{IndexedRecord, MemoryStorage, SqliteStorage, Storage},
    jwt::ServiceJwtVerifier,
};
use atrium_crypto::keypair::{Did as _, P256Keypair};
use axum::{
    body::Body,
    http::{Request, StatusCode},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use std::{
    sync::Arc,
    time::{Duration, SystemTime, UNIX_EPOCH},
};
use tower::ServiceExt;

const ALICE: &str = "did:plc:alice";
const FRIEND: &str = "did:plc:friend";
const ISSUER: &str = ALICE;
const APPVIEW: &str = "did:web:appview.test";

#[derive(Clone)]
struct FakeIdentity(DidDocument);
impl IdentityBackend for FakeIdentity {
    async fn did_document(&self, did: &str) -> Result<Option<DidDocument>, ResolutionError> {
        Ok((self.0.id == did).then(|| self.0.clone()))
    }
    async fn dns_txt_did(&self, _: &str) -> Result<Option<String>, ResolutionError> {
        Ok(None)
    }
    async fn well_known_did(&self, _: &str) -> Result<Option<String>, ResolutionError> {
        Ok(None)
    }
}

fn fixture_storage() -> Arc<MemoryStorage> {
    let storage = Arc::new(MemoryStorage::default());
    storage.register_user(ALICE).unwrap();
    storage.set_verified_handle(ALICE, "alice.test").unwrap();
    storage.set_verified_handle(FRIEND, "friend.test").unwrap();
    storage.set_follow(ALICE, FRIEND, true).unwrap();
    let fixtures = [
        (
            ALICE,
            "app.bsky.actor.profile",
            "self",
            json!({"displayName":"Alice","description":"Example","createdAt":"2024-01-01T00:00:00Z"}),
        ),
        (
            FRIEND,
            "app.bsky.actor.profile",
            "self",
            json!({"displayName":"Friend","createdAt":"2024-01-01T00:00:00Z"}),
        ),
        (
            ALICE,
            "app.bsky.feed.post",
            "p1",
            json!({"$type":"app.bsky.feed.post","text":"hello","createdAt":"2026-10-27T00:00:00Z"}),
        ),
        (
            FRIEND,
            "app.bsky.feed.post",
            "p2",
            json!({"$type":"app.bsky.feed.post","text":"friend post","createdAt":"2026-10-26T00:00:00Z"}),
        ),
        (
            FRIEND,
            "app.bsky.feed.like",
            "l1",
            json!({"subject":{"uri":"at://did:plc:alice/app.bsky.feed.post/p1","cid":"cid-p1"},"createdAt":"2026-10-27T00:00:01Z"}),
        ),
        (
            FRIEND,
            "app.bsky.feed.repost",
            "r1",
            json!({"subject":{"uri":"at://did:plc:alice/app.bsky.feed.post/p1","cid":"cid-p1"},"createdAt":"2026-10-27T00:00:02Z"}),
        ),
        (
            ALICE,
            "app.bsky.graph.follow",
            "f1",
            json!({"subject":FRIEND,"createdAt":"2026-10-27T00:00:03Z"}),
        ),
    ];
    for (index, (did, collection, rkey, record)) in fixtures.into_iter().enumerate() {
        storage
            .put_record(&IndexedRecord {
                did: did.into(),
                collection: collection.into(),
                rkey: rkey.into(),
                uri: format!("at://{did}/{collection}/{rkey}"),
                cid: Some(format!("cid-{rkey}")),
                time_us: 1_800_000_000_000_000 - index as u64,
                record,
            })
            .unwrap();
    }
    storage
}

fn api() -> (axum::Router, P256Keypair) {
    api_for_storage(fixture_storage())
}

fn api_for_storage<S: Storage + 'static>(storage: Arc<S>) -> (axum::Router, P256Keypair) {
    let key = P256Keypair::create(&mut rand::thread_rng());
    let signing_did_key = key.did();
    let issuer_doc = DidDocument {
        id: ISSUER.into(),
        also_known_as: vec![],
        verification_method: vec![VerificationMethod {
            id: "#atproto".into(),
            type_name: "Multikey".into(),
            controller: ISSUER.into(),
            public_key_multibase: signing_did_key.strip_prefix("did:key:").unwrap().into(),
        }],
        service: vec![],
    };
    let resolver = Arc::new(IdentityResolver::new(
        FakeIdentity(issuer_doc),
        Duration::from_secs(30),
    ));
    (
        api_router(
            storage,
            Arc::new(ServiceJwtVerifier::new(resolver, APPVIEW)),
            APPVIEW,
        ),
        key,
    )
}

fn token(key: &P256Keypair, aud: &str, lxm: &str, exp: u64, sub: Option<&str>) -> String {
    token_with_algorithm(key, "ES256", aud, lxm, exp, sub)
}

fn token_with_algorithm(
    key: &P256Keypair,
    algorithm: &str,
    aud: &str,
    lxm: &str,
    exp: u64,
    sub: Option<&str>,
) -> String {
    let header =
        URL_SAFE_NO_PAD.encode(serde_json::to_vec(&json!({"alg":algorithm,"typ":"JWT"})).unwrap());
    let payload = json!({"iss":ISSUER,"aud":aud,"exp":exp,"iat":expiry(0),"lxm":lxm,"sub":sub});
    let payload = URL_SAFE_NO_PAD.encode(serde_json::to_vec(&payload).unwrap());
    let message = format!("{header}.{payload}");
    let signature = key.sign(message.as_bytes()).unwrap();
    format!("{message}.{}", URL_SAFE_NO_PAD.encode(signature))
}

async fn request(router: axum::Router, uri: &str, jwt: Option<&str>) -> (StatusCode, Value) {
    let mut builder = Request::builder().uri(uri);
    if let Some(jwt) = jwt {
        builder = builder.header("authorization", format!("Bearer {jwt}"));
    }
    let response = router
        .oneshot(builder.body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = response.into_body().collect().await.unwrap().to_bytes();
    (status, serde_json::from_slice(&body).unwrap_or(Value::Null))
}

fn expiry(seconds: u64) -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
        + seconds
}

#[tokio::test]
async fn actor_profiles_and_author_feed_match_recorded_public_api_shapes() {
    let (router, _) = api();
    let profile_fixture: Value =
        serde_json::from_str(include_str!("fixtures/get_profile.json")).unwrap();
    let (status, profile) = request(
        router.clone(),
        "/xrpc/app.bsky.actor.getProfile?actor=did%3Aplc%3Aalice",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    for key in [
        "did",
        "handle",
        "displayName",
        "description",
        "followersCount",
        "followsCount",
        "postsCount",
    ] {
        assert!(
            profile_fixture.get(key).is_some(),
            "public fixture missing {key}"
        );
        assert!(profile.get(key).is_some(), "profile response missing {key}");
    }
    assert_eq!(profile["did"], ALICE);
    assert_eq!(profile["handle"], "alice.test");

    let feed_fixture: Value =
        serde_json::from_str(include_str!("fixtures/get_author_feed.json")).unwrap();
    let (status, feed) = request(
        router,
        "/xrpc/app.bsky.feed.getAuthorFeed?actor=did%3Aplc%3Aalice&limit=1",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(feed.get("feed").is_some());
    for key in [
        "uri",
        "cid",
        "author",
        "record",
        "replyCount",
        "repostCount",
        "likeCount",
        "quoteCount",
        "indexedAt",
        "labels",
    ] {
        assert!(
            feed_fixture["feed"][0]["post"].get(key).is_some(),
            "public fixture missing {key}"
        );
        assert!(
            feed["feed"][0]["post"].get(key).is_some(),
            "feed response missing {key}"
        );
    }
}

#[tokio::test]
async fn posts_threads_graph_notifications_and_appview_did_are_served() {
    let (router, signing_key) = api();
    let endpoints = [
        (
            "/xrpc/app.bsky.actor.getProfiles?actors=did%3Aplc%3Aalice&actors=did%3Aplc%3Afriend",
            "profiles",
        ),
        (
            "/xrpc/app.bsky.feed.getPosts?uris=at%3A%2F%2Fdid%3Aplc%3Aalice%2Fapp.bsky.feed.post%2Fp1",
            "posts",
        ),
        (
            "/xrpc/app.bsky.feed.getPostThread?uri=at%3A%2F%2Fdid%3Aplc%3Aalice%2Fapp.bsky.feed.post%2Fp1",
            "thread",
        ),
        (
            "/xrpc/app.bsky.feed.getLikes?uri=at%3A%2F%2Fdid%3Aplc%3Aalice%2Fapp.bsky.feed.post%2Fp1",
            "likes",
        ),
        (
            "/xrpc/app.bsky.feed.getRepostedBy?uri=at%3A%2F%2Fdid%3Aplc%3Aalice%2Fapp.bsky.feed.post%2Fp1",
            "repostedBy",
        ),
        (
            "/xrpc/app.bsky.graph.getFollows?actor=did%3Aplc%3Aalice",
            "follows",
        ),
        (
            "/xrpc/app.bsky.graph.getFollowers?actor=did%3Aplc%3Afriend",
            "followers",
        ),
    ];
    for (uri, key) in endpoints {
        let (status, body) = request(router.clone(), uri, None).await;
        assert_eq!(status, StatusCode::OK, "{uri}: {body}");
        assert!(
            body.get(key).is_some(),
            "{uri} did not return {key}: {body}"
        );
    }
    let (status, did_doc) = request(router.clone(), "/.well-known/did.json", None).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(did_doc["id"], APPVIEW);
    assert_eq!(
        did_doc["service"][0]["id"],
        format!("{APPVIEW}#bsky_appview")
    );

    let good = token(
        &signing_key,
        APPVIEW,
        "app.bsky.feed.getTimeline",
        expiry(60),
        Some(ALICE),
    );
    let (status, timeline) = request(
        router.clone(),
        "/xrpc/app.bsky.feed.getTimeline?limit=10",
        Some(&good),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{timeline}");
    assert!(timeline["feed"].is_array());
    let (status, notifications) = request(
        router.clone(),
        "/xrpc/app.bsky.notification.listNotifications",
        Some(&token(
            &signing_key,
            APPVIEW,
            "app.bsky.notification.listNotifications",
            expiry(60),
            Some(ALICE),
        )),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{notifications}");
    assert!(notifications["notifications"].is_array());
    let (status, unread) = request(
        router.clone(),
        "/xrpc/app.bsky.notification.getUnreadCount",
        Some(&token(
            &signing_key,
            APPVIEW,
            "app.bsky.notification.getUnreadCount",
            expiry(60),
            Some(ALICE),
        )),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{unread}");
    assert!(unread["count"].is_number());
}

#[tokio::test]
async fn repeated_xrpc_array_parameters_return_all_values() {
    let (router, _) = api();
    let (status, profiles) = request(
        router.clone(),
        "/xrpc/app.bsky.actor.getProfiles?actors=did%3Aplc%3Aalice&actors=did%3Aplc%3Afriend",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(profiles["profiles"].as_array().unwrap().len(), 2);

    let (status, posts) = request(
        router,
        "/xrpc/app.bsky.feed.getPosts?uris=at%3A%2F%2Fdid%3Aplc%3Aalice%2Fapp.bsky.feed.post%2Fp1&uris=at%3A%2F%2Fdid%3Aplc%3Afriend%2Fapp.bsky.feed.post%2Fp2",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(posts["posts"].as_array().unwrap().len(), 2);
}

#[tokio::test]
async fn author_feed_cursor_advances_between_pages() {
    let storage = Arc::new(SqliteStorage::open(":memory:").unwrap());
    for (rkey, time_us) in [("a", 300), ("b", 200), ("c", 100)] {
        storage
            .put_record(&IndexedRecord {
                did: ALICE.to_owned(),
                collection: "app.bsky.feed.post".to_owned(),
                rkey: rkey.to_owned(),
                uri: format!("at://{ALICE}/app.bsky.feed.post/{rkey}"),
                cid: Some(format!("cid-{rkey}")),
                time_us,
                record: json!({"text":rkey}),
            })
            .unwrap();
    }
    let (router, _) = api_for_storage(storage);
    let (status, first) = request(
        router.clone(),
        "/xrpc/app.bsky.feed.getAuthorFeed?actor=did%3Aplc%3Aalice&limit=1",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    let first_cursor = first["cursor"].as_str().unwrap();
    let first_uri = first["feed"][0]["post"]["uri"].as_str().unwrap();

    let (status, second) = request(
        router.clone(),
        &format!(
            "/xrpc/app.bsky.feed.getAuthorFeed?actor=did%3Aplc%3Aalice&limit=1&cursor={first_cursor}"
        ),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert!(!second["feed"].as_array().unwrap().is_empty());
    assert_ne!(second["feed"][0]["post"]["uri"], first_uri);
    let second_cursor = second["cursor"].as_str().unwrap();
    assert_ne!(second_cursor, first_cursor);

    let (status, third) = request(
        router,
        &format!(
            "/xrpc/app.bsky.feed.getAuthorFeed?actor=did%3Aplc%3Aalice&limit=1&cursor={second_cursor}"
        ),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_ne!(
        third["feed"][0]["post"]["uri"],
        second["feed"][0]["post"]["uri"]
    );
}

#[tokio::test]
async fn post_views_count_indexed_interactions_and_include_the_viewers_own_records() {
    let storage = Arc::new(SqliteStorage::open(":memory:").unwrap());
    let post_uri = format!("at://{ALICE}/app.bsky.feed.post/p1");
    for (did, collection, rkey, record) in [
        (
            ALICE,
            "app.bsky.feed.post",
            "p1",
            json!({"text":"original"}),
        ),
        (
            FRIEND,
            "app.bsky.feed.like",
            "other-like",
            json!({"subject":{"uri":post_uri,"cid":"cid-p1"}}),
        ),
        (
            ALICE,
            "app.bsky.feed.like",
            "own-like",
            json!({"subject":{"uri":post_uri,"cid":"cid-p1"}}),
        ),
        (
            FRIEND,
            "app.bsky.feed.repost",
            "other-repost",
            json!({"subject":{"uri":post_uri,"cid":"cid-p1"}}),
        ),
        (
            ALICE,
            "app.bsky.feed.repost",
            "own-repost",
            json!({"subject":{"uri":post_uri,"cid":"cid-p1"}}),
        ),
        (
            FRIEND,
            "app.bsky.feed.post",
            "reply",
            json!({"text":"reply","reply":{"root":{"uri":post_uri},"parent":{"uri":post_uri}}}),
        ),
    ] {
        storage
            .put_record(&IndexedRecord {
                did: did.into(),
                collection: collection.into(),
                rkey: rkey.into(),
                uri: format!("at://{did}/{collection}/{rkey}"),
                cid: Some(format!("cid-{rkey}")),
                time_us: 100,
                record,
            })
            .unwrap();
    }
    let (router, signing_key) = api_for_storage(storage);
    let jwt = token(
        &signing_key,
        APPVIEW,
        "app.bsky.feed.getPosts",
        expiry(60),
        Some(ALICE),
    );
    let (status, body) = request(
        router,
        "/xrpc/app.bsky.feed.getPosts?uris=at%3A%2F%2Fdid%3Aplc%3Aalice%2Fapp.bsky.feed.post%2Fp1",
        Some(&jwt),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let post = &body["posts"][0];
    assert_eq!(post["likeCount"], 2);
    assert_eq!(post["repostCount"], 2);
    assert_eq!(post["replyCount"], 1);
    assert_eq!(
        post["viewer"]["like"],
        "at://did:plc:alice/app.bsky.feed.like/own-like"
    );
    assert_eq!(
        post["viewer"]["repost"],
        "at://did:plc:alice/app.bsky.feed.repost/own-repost"
    );
}

#[tokio::test]
async fn post_thread_returns_parent_chain_and_time_ordered_nested_replies() {
    let storage = Arc::new(SqliteStorage::open(":memory:").unwrap());
    let root_uri = format!("at://{ALICE}/app.bsky.feed.post/root");
    let parent_uri = format!("at://{ALICE}/app.bsky.feed.post/parent");
    let focus_uri = format!("at://{ALICE}/app.bsky.feed.post/focus");
    let early_reply_uri = format!("at://{ALICE}/app.bsky.feed.post/early");
    for (rkey, time_us, text, parent, root) in [
        ("root", 10, "root", None, None),
        (
            "parent",
            20,
            "parent",
            Some(root_uri.as_str()),
            Some(root_uri.as_str()),
        ),
        (
            "focus",
            30,
            "focus",
            Some(parent_uri.as_str()),
            Some(root_uri.as_str()),
        ),
        (
            "late",
            50,
            "late reply",
            Some(focus_uri.as_str()),
            Some(root_uri.as_str()),
        ),
        (
            "early",
            40,
            "early reply",
            Some(focus_uri.as_str()),
            Some(root_uri.as_str()),
        ),
        (
            "nested",
            60,
            "nested reply",
            Some(early_reply_uri.as_str()),
            Some(root_uri.as_str()),
        ),
    ] {
        let reply =
            parent.map(|parent| json!({"parent":{"uri":parent},"root":{"uri":root.unwrap()}}));
        let record = if let Some(reply) = reply {
            json!({"text":text,"reply":reply})
        } else {
            json!({"text":text})
        };
        storage
            .put_record(&IndexedRecord {
                did: ALICE.into(),
                collection: "app.bsky.feed.post".into(),
                rkey: rkey.into(),
                uri: format!("at://{ALICE}/app.bsky.feed.post/{rkey}"),
                cid: Some(format!("cid-{rkey}")),
                time_us,
                record,
            })
            .unwrap();
    }
    let (router, _) = api_for_storage(storage);
    let encoded_uri =
        url::form_urlencoded::byte_serialize(focus_uri.as_bytes()).collect::<String>();
    let (status, body) = request(
        router,
        &format!("/xrpc/app.bsky.feed.getPostThread?uri={encoded_uri}&depth=2&parentHeight=2"),
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let thread = &body["thread"];
    assert_eq!(thread["post"]["record"]["text"], "focus");
    assert_eq!(thread["parent"]["post"]["record"]["text"], "parent");
    assert_eq!(thread["parent"]["parent"]["post"]["record"]["text"], "root");
    assert_eq!(
        thread["replies"][0]["post"]["record"]["text"],
        "early reply"
    );
    assert_eq!(thread["replies"][1]["post"]["record"]["text"], "late reply");
    assert_eq!(
        thread["replies"][0]["replies"][0]["post"]["record"]["text"],
        "nested reply"
    );
}

#[tokio::test]
async fn notifications_include_likes_reposts_replies_mentions_quotes_and_follows() {
    let storage = Arc::new(SqliteStorage::open(":memory:").unwrap());
    let post_uri = format!("at://{ALICE}/app.bsky.feed.post/own");
    let notifications = [
        (
            "like",
            "app.bsky.feed.like",
            json!({"subject":{"uri":post_uri,"cid":"cid-own"}}),
        ),
        (
            "repost",
            "app.bsky.feed.repost",
            json!({"subject":{"uri":post_uri,"cid":"cid-own"}}),
        ),
        (
            "reply",
            "app.bsky.feed.post",
            json!({"text":"reply","reply":{"parent":{"uri":post_uri,"cid":"cid-own"},"root":{"uri":post_uri,"cid":"cid-own"}}}),
        ),
        (
            "mention",
            "app.bsky.feed.post",
            json!({"text":"hello","facets":[{"features":[{"$type":"app.bsky.richtext.facet#mention","did":ALICE}]}]}),
        ),
        (
            "quote",
            "app.bsky.feed.post",
            json!({"text":"quote","embed":{"$type":"app.bsky.embed.record","record":{"uri":post_uri,"cid":"cid-own"}}}),
        ),
        ("follow", "app.bsky.graph.follow", json!({"subject":ALICE})),
    ];
    storage
        .put_record(&IndexedRecord {
            did: ALICE.into(),
            collection: "app.bsky.feed.post".into(),
            rkey: "own".into(),
            uri: post_uri,
            cid: Some("cid-own".into()),
            time_us: 10,
            record: json!({"text":"my post"}),
        })
        .unwrap();
    for (index, (rkey, collection, record)) in notifications.into_iter().enumerate() {
        storage
            .put_record(&IndexedRecord {
                did: FRIEND.into(),
                collection: collection.into(),
                rkey: rkey.into(),
                uri: format!("at://{FRIEND}/{collection}/{rkey}"),
                cid: Some(format!("cid-{rkey}")),
                time_us: 100 + index as u64,
                record,
            })
            .unwrap();
    }
    let (router, signing_key) = api_for_storage(storage);
    let jwt = token(
        &signing_key,
        APPVIEW,
        "app.bsky.notification.listNotifications",
        expiry(60),
        Some(ALICE),
    );
    let (status, body) = request(
        router,
        "/xrpc/app.bsky.notification.listNotifications?limit=20",
        Some(&jwt),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let notifications = body["notifications"].as_array().unwrap();
    let reasons = notifications
        .iter()
        .filter_map(|notice| notice["reason"].as_str())
        .collect::<Vec<_>>();
    for expected in ["like", "repost", "reply", "mention", "quote", "follow"] {
        assert!(
            reasons.contains(&expected),
            "missing {expected}: {reasons:?}"
        );
    }
    assert_eq!(reasons.first().copied(), Some("follow"));
    assert_eq!(body["seenAt"].as_str().is_some(), true);
}

#[tokio::test]
async fn proxy_jwt_requires_signature_audience_method_and_expiration() {
    let (router, signing_key) = api();
    let protected = "/xrpc/app.bsky.feed.getTimeline";
    for jwt in [
        token(
            &signing_key,
            "did:web:wrong.test",
            "app.bsky.feed.getTimeline",
            expiry(60),
            Some(ALICE),
        ),
        token(
            &signing_key,
            APPVIEW,
            "app.bsky.feed.getPosts",
            expiry(60),
            Some(ALICE),
        ),
        token(
            &signing_key,
            APPVIEW,
            "app.bsky.feed.getTimeline",
            expiry(0),
            Some(ALICE),
        ),
    ] {
        let (status, _) = request(router.clone(), protected, Some(&jwt)).await;
        assert_eq!(status, StatusCode::UNAUTHORIZED);
    }
    let (status, _) = request(router, protected, None).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn proxy_jwt_rejects_a_valid_claim_set_signed_by_another_key() {
    let (router, _) = api();
    let forged_key = P256Keypair::create(&mut rand::thread_rng());
    let forged = token(
        &forged_key,
        APPVIEW,
        "app.bsky.feed.getTimeline",
        expiry(60),
        Some(ALICE),
    );
    let (status, _) = request(router, "/xrpc/app.bsky.feed.getTimeline", Some(&forged)).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn proxy_jwt_rejects_an_algorithm_that_does_not_match_the_issuer_key() {
    let (router, signing_key) = api();
    let mismatched = token_with_algorithm(
        &signing_key,
        "ES256K",
        APPVIEW,
        "app.bsky.feed.getTimeline",
        expiry(60),
        Some(ALICE),
    );
    let (status, _) = request(router, "/xrpc/app.bsky.feed.getTimeline", Some(&mismatched)).await;
    assert_eq!(status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn profile_handles_are_taken_from_verified_identity_data() {
    let storage = Arc::new(MemoryStorage::default());
    storage.set_verified_handle(ALICE, "alice.test").unwrap();
    storage
        .put_record(&IndexedRecord {
            did: ALICE.into(),
            collection: "app.bsky.actor.profile".into(),
            rkey: "self".into(),
            uri: format!("at://{ALICE}/app.bsky.actor.profile/self"),
            cid: Some("profile-cid".into()),
            time_us: 1,
            record: json!({"displayName":"Alice"}),
        })
        .unwrap();
    let (router, _) = api_for_storage(storage);
    let (status, profile) = request(
        router,
        "/xrpc/app.bsky.actor.getProfile?actor=alice.test",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{profile}");
    assert_eq!(profile["did"], ALICE);
    assert_eq!(profile["handle"], "alice.test");
}

#[tokio::test]
async fn profile_records_cannot_claim_unverified_handles() {
    let storage = Arc::new(MemoryStorage::default());
    storage
        .put_record(&IndexedRecord {
            did: ALICE.into(),
            collection: "app.bsky.actor.profile".into(),
            rkey: "self".into(),
            uri: format!("at://{ALICE}/app.bsky.actor.profile/self"),
            cid: Some("profile-cid".into()),
            time_us: 1,
            record: json!({"handle":"attacker.example","displayName":"Alice"}),
        })
        .unwrap();
    let (router, _) = api_for_storage(storage);
    let (status, profile) = request(
        router.clone(),
        "/xrpc/app.bsky.actor.getProfile?actor=did%3Aplc%3Aalice",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(profile["handle"], "handle.invalid");
    let (status, _) = request(
        router,
        "/xrpc/app.bsky.actor.getProfile?actor=attacker.example",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn remaining_read_responses_preserve_recorded_public_api_keys() {
    let (router, _) = api();
    let route_fixtures = [
        (
            "/xrpc/app.bsky.feed.getPosts?uris=at%3A%2F%2Fdid:plc:alice%2Fapp.bsky.feed.post%2Fp1",
            include_str!("fixtures/get_posts.json"),
            "posts",
        ),
        (
            "/xrpc/app.bsky.feed.getLikes?uri=at%3A%2F%2Fdid:plc:alice%2Fapp.bsky.feed.post%2Fp1",
            include_str!("fixtures/get_likes.json"),
            "likes",
        ),
        (
            "/xrpc/app.bsky.feed.getRepostedBy?uri=at%3A%2F%2Fdid:plc:alice%2Fapp.bsky.feed.post%2Fp1",
            include_str!("fixtures/get_reposted_by.json"),
            "repostedBy",
        ),
        (
            "/xrpc/app.bsky.graph.getFollows?actor=did%3Aplc%3Aalice",
            include_str!("fixtures/get_follows.json"),
            "follows",
        ),
        (
            "/xrpc/app.bsky.graph.getFollowers?actor=did%3Aplc%3Afriend",
            include_str!("fixtures/get_followers.json"),
            "followers",
        ),
    ];
    for (uri, fixture_json, list_key) in route_fixtures {
        let fixture: Value = serde_json::from_str(fixture_json).unwrap();
        let (status, body) = request(router.clone(), uri, None).await;
        assert_eq!(status, StatusCode::OK, "{uri}: {body}");
        for key in fixture.as_object().unwrap().keys() {
            assert!(body.get(key).is_some(), "{uri} omitted fixture key {key}");
        }
        assert!(body[list_key].is_array());
        if fixture[list_key]
            .as_array()
            .and_then(|items| items.first())
            .is_some()
        {
            let response_item = body[list_key].as_array().unwrap().first().unwrap();
            let required_item_keys: &[&str] = match list_key {
                "posts" => &[
                    "uri",
                    "cid",
                    "author",
                    "record",
                    "replyCount",
                    "repostCount",
                    "likeCount",
                    "quoteCount",
                    "labels",
                ],
                "likes" => &["actor", "createdAt", "indexedAt"],
                _ => &["did", "handle", "labels"],
            };
            for key in required_item_keys {
                assert!(
                    response_item.get(*key).is_some(),
                    "{uri} item omitted required fixture key {key}"
                );
            }
        }
    }

    let thread_fixture: Value =
        serde_json::from_str(include_str!("fixtures/get_post_thread.json")).unwrap();
    let (status, body) = request(
        router,
        "/xrpc/app.bsky.feed.getPostThread?uri=at%3A%2F%2Fdid:plc:alice%2Fapp.bsky.feed.post%2Fp1",
        None,
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    for key in thread_fixture["thread"].as_object().unwrap().keys() {
        assert!(
            body["thread"].get(key).is_some(),
            "thread omitted fixture key {key}"
        );
    }
}

#[tokio::test]
async fn proxied_timeline_uses_the_jwt_issuer_as_the_account_did() {
    let storage = Arc::new(MemoryStorage::default());
    storage.register_user(ALICE).unwrap();
    for (index, did) in [ALICE, FRIEND].into_iter().enumerate() {
        storage
            .put_record(&IndexedRecord {
                did: did.into(),
                collection: "app.bsky.feed.post".into(),
                rkey: "post".into(),
                uri: format!("at://{did}/app.bsky.feed.post/post"),
                cid: Some(format!("cid-{did}")),
                time_us: 1_800_000_000_000_000 - index as u64,
                record: json!({"text":"test"}),
            })
            .unwrap();
    }
    let (router, signing_key) = api_for_storage(storage);
    let jwt = token(
        &signing_key,
        APPVIEW,
        "app.bsky.feed.getTimeline",
        expiry(60),
        Some(FRIEND),
    );
    let (status, timeline) = request(router, "/xrpc/app.bsky.feed.getTimeline", Some(&jwt)).await;
    assert_eq!(status, StatusCode::OK, "{timeline}");
    assert!(
        timeline["feed"]
            .as_array()
            .unwrap()
            .iter()
            .all(|item| { item["post"]["author"]["did"] == ALICE })
    );
}
