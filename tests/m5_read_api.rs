use appview_rs::{
    api::api_router,
    identity::{
        DidDocument, IdentityBackend, IdentityResolver, ResolutionError, VerificationMethod,
    },
    ingest::{IndexedRecord, MemoryStorage, Storage},
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
const ISSUER: &str = "did:plc:issuer";
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
    storage.set_follow(ALICE, FRIEND, true).unwrap();
    let fixtures = [
        (
            ALICE,
            "app.bsky.actor.profile",
            "self",
            json!({"handle":"alice.test","displayName":"Alice","description":"Example","createdAt":"2024-01-01T00:00:00Z"}),
        ),
        (
            FRIEND,
            "app.bsky.actor.profile",
            "self",
            json!({"handle":"friend.test","displayName":"Friend","createdAt":"2024-01-01T00:00:00Z"}),
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
            fixture_storage(),
            Arc::new(ServiceJwtVerifier::new(resolver, APPVIEW)),
            APPVIEW,
        ),
        key,
    )
}

fn token(key: &P256Keypair, aud: &str, lxm: &str, exp: u64, sub: Option<&str>) -> String {
    let header = URL_SAFE_NO_PAD.encode(br#"{"alg":"ES256K","typ":"JWT"}"#);
    let payload = json!({"iss":ISSUER,"aud":aud,"exp":exp,"lxm":lxm,"sub":sub});
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
            "/xrpc/app.bsky.actor.getProfiles?actors=did%3Aplc%3Aalice,did%3Aplc%3Afriend",
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

    let signing_key = P256Keypair::create(&mut rand::thread_rng());
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
