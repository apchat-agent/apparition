use appview_rs::{
    identity::{
        DidDocument as AppDidDocument, DidService, IdentityBackend, IdentityResolver,
        ResolutionError,
    },
    ingest::{IndexedRecord, MemoryStorage, Storage},
    link_card::LinkThumbnail,
    net::SafeHttpClient,
    oauth::{OAuthCallback, OAuthFlow, OAuthPublic, PostExternal, PostStrongRef, SqliteOAuthStore},
};
use atrium_api::{
    agent::Agent,
    did_doc::{DidDocument, Service},
    types::string::{Did, Handle},
};
use atrium_common::{
    resolver::Resolver,
    store::{Store, memory::MemoryStore},
};
use atrium_oauth::store::{session::Session, state::InternalStateData};
use atrium_oauth::{
    AtprotoClientMetadata, AuthMethod, AuthorizeOptions, CallbackParams, GrantType, KnownScope,
    OAuthClient, OAuthClientConfig, OAuthResolverConfig, Scope,
};
use axum::{
    Router,
    body::Body,
    http::{Request, StatusCode},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use http_body_util::BodyExt;
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    io::{self, Write},
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};
use tower::ServiceExt;
use tracing::instrument::WithSubscriber;
use tracing_subscriber::fmt::MakeWriter;

const DID: &str = "did:plc:abcdefghijklmnopqrstuvwx";
const EXPECTED_OAUTH_SCOPE: &str = "atproto repo:app.bsky.feed.post?action=create repo:app.bsky.feed.like?action=create&action=delete repo:app.bsky.feed.repost?action=create&action=delete blob:image/* rpc:app.bsky.actor.getPreferences?aud=* rpc:app.bsky.feed.getFeedSkeleton?aud=* blob:video/* rpc:app.bsky.video.getUploadLimits?aud=* rpc:com.atproto.repo.uploadBlob?aud=* repo:app.bsky.graph.follow?action=create&action=delete repo:app.bsky.graph.block?action=create&action=delete repo:app.bsky.graph.listitem?action=create&action=delete rpc:app.bsky.graph.muteActor?aud=* rpc:app.bsky.graph.unmuteActor?aud=* rpc:app.bsky.graph.getMutes?aud=*";

#[derive(Clone)]
struct FakeDidResolver {
    pds: String,
}

impl Resolver for FakeDidResolver {
    type Input = Did;
    type Output = DidDocument;
    type Error = atrium_identity::Error;

    async fn resolve(&self, did: &Did) -> Result<Self::Output, Self::Error> {
        Ok(DidDocument {
            context: None,
            id: did.as_ref().to_owned(),
            also_known_as: None,
            verification_method: None,
            service: Some(vec![Service {
                id: "#atproto_pds".to_owned(),
                r#type: "AtprotoPersonalDataServer".to_owned(),
                service_endpoint: self.pds.clone(),
            }]),
        })
    }
}

struct UnusedHandleResolver;

impl Resolver for UnusedHandleResolver {
    type Input = Handle;
    type Output = Did;
    type Error = atrium_identity::Error;

    async fn resolve(&self, _: &Handle) -> Result<Did, Self::Error> {
        Err(atrium_identity::Error::NotFound)
    }
}

#[derive(Default)]
struct FakeServerState {
    requests: Vec<CapturedRequest>,
    par_count: usize,
    timeline_count: usize,
    token_count: usize,
    token_sub: Option<String>,
    token_scope: Option<String>,
    create_count: usize,
    create_nonce_required: bool,
    create_nonce_challenges: usize,
    upload_count: usize,
    upload_fail_on: Option<usize>,
    upload_missing_ref_on: Option<usize>,
    create_bodies: Vec<Value>,
    // Feed-page wiring: when set, com.atproto.repo.getRecord answers for
    // app.bsky.feed.generator records with this service DID.
    feed_generator_service_did: Option<String>,
    // When set, com.atproto.server.getServiceAuth only accepts these access tokens and answers
    // DPoP error="invalid_token" to every other one.
    service_auth_valid_access_tokens: Option<Vec<String>>,
    // When set, app.bsky.feed.getFeedSkeleton only answers when it carries this bearer token.
    skeleton_required_jwt: Option<String>,
    // When set, com.atproto.repo.createRecord answers this status with an
    // {"error":"InternalFailure","message":..} body instead of creating anything.
    create_failure: Option<(u16, String)>,
    // When set, the refresh_token grant answers this status with
    // {"error":<error>,"error_description":..} instead of a new token set.
    refresh_failure: Option<(u16, String)>,
    // When set, com.atproto.repo.createRecord answers DPoP error="invalid_token" unless the
    // request carries one of these access tokens.
    create_required_access_tokens: Option<Vec<String>>,
    // Access token issued by the first (authorization-code) token exchange; defaults to
    // "access-expired".
    initial_access_token: Option<String>,
    // expires_in for the first token exchange; defaults to 0.
    initial_expires_in: Option<u32>,
    // Successive access tokens handed out by refresh_token exchanges; "access-fresh" when empty.
    refresh_access_tokens: std::collections::VecDeque<String>,
}

#[derive(Clone)]
struct ProductionFlowIdentity {
    document: AppDidDocument,
}

impl IdentityBackend for ProductionFlowIdentity {
    async fn did_document(&self, did: &str) -> Result<Option<AppDidDocument>, ResolutionError> {
        Ok((self.document.id == did).then(|| self.document.clone()))
    }

    async fn dns_txt_did(&self, _: &str) -> Result<Option<String>, ResolutionError> {
        Ok(None)
    }

    async fn well_known_did(&self, _: &str) -> Result<Option<String>, ResolutionError> {
        Ok(None)
    }
}

#[derive(Clone, Debug)]
struct CapturedRequest {
    method: String,
    path: String,
    headers: HashMap<String, String>,
    body: String,
    body_bytes: Vec<u8>,
}

#[tokio::test]
async fn oauth_protocol_checks_state_issuer_nonce_and_refresh_against_fake_pds() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let origin = format!("http://{address}");
    let shared = Arc::new(Mutex::new(FakeServerState::default()));
    let server_state = shared.clone();
    let server_origin = origin.clone();
    let server = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let state = server_state.clone();
            let origin = server_origin.clone();
            tokio::spawn(async move {
                serve_one(stream, &origin, state).await;
            });
        }
    });

    let public = OAuthPublic::new("https://appview.example").unwrap();
    let did_resolver = FakeDidResolver {
        pds: origin.clone(),
    };
    let client = OAuthClient::new(OAuthClientConfig {
        client_metadata: AtprotoClientMetadata {
            client_id: "https://appview.example/.well-known/oauth-client-metadata.json".to_owned(),
            client_uri: Some("https://appview.example".to_owned()),
            redirect_uris: vec!["https://appview.example/oauth/callback".to_owned()],
            token_endpoint_auth_method: AuthMethod::PrivateKeyJwt,
            grant_types: vec![GrantType::AuthorizationCode, GrantType::RefreshToken],
            scopes: vec![Scope::Known(KnownScope::Atproto)],
            jwks_uri: None,
            token_endpoint_auth_signing_alg: Some("ES256".to_owned()),
        },
        keys: Some(vec![public.private_key().clone()]),
        state_store: MemoryStore::<String, InternalStateData>::default(),
        session_store: MemoryStore::<Did, Session>::default(),
        resolver: OAuthResolverConfig {
            did_resolver,
            handle_resolver: UnusedHandleResolver,
            authorization_server_metadata: Default::default(),
            protected_resource_metadata: Default::default(),
        },
        http_client: SafeHttpClient::new().with_http_test_hosts(["127.0.0.1".to_owned()]),
    })
    .unwrap();

    let authorize_url = client
        .authorize(DID, AuthorizeOptions::default())
        .await
        .unwrap();
    assert!(authorize_url.starts_with(&format!("{origin}/authorize?")));
    let par = shared
        .lock()
        .unwrap()
        .requests
        .iter()
        .find(|request| request.path == "/par")
        .unwrap()
        .clone();
    let state = form_field(&par.body, "state").unwrap();
    assert_eq!(form_field(&par.body, "scope").as_deref(), Some("atproto"));
    assert_eq!(
        form_field(&par.body, "code_challenge_method").as_deref(),
        Some("S256")
    );

    let bad_state = client
        .callback(CallbackParams {
            code: "unused".to_owned(),
            state: Some("attacker-state".to_owned()),
            iss: Some(origin.clone()),
        })
        .await;
    assert!(bad_state.is_err());

    let wrong_issuer = client
        .callback(CallbackParams {
            code: "unused".to_owned(),
            state: Some(state.clone()),
            iss: Some("https://wrong-issuer.example".to_owned()),
        })
        .await;
    assert!(wrong_issuer.is_err());
    assert_eq!(shared.lock().unwrap().token_count, 0);

    let authorize_url = client
        .authorize(DID, AuthorizeOptions::default())
        .await
        .unwrap();
    assert!(authorize_url.contains("request_uri="));
    let callback_state = latest_state(&shared);
    let (session, _) = client
        .callback(CallbackParams {
            code: "valid-code".to_owned(),
            state: Some(callback_state),
            iss: Some(origin.clone()),
        })
        .await
        .unwrap();
    let agent = Agent::new(session);
    let output = agent
        .api
        .app
        .bsky
        .feed
        .get_timeline(
            atrium_api::app::bsky::feed::get_timeline::ParametersData {
                algorithm: None,
                cursor: None,
                limit: Some(25u8.try_into().unwrap()),
            }
            .into(),
        )
        .await
        .unwrap();
    assert!(output.feed.is_empty());

    let requests = shared.lock().unwrap().requests.clone();
    let par_requests = requests
        .iter()
        .filter(|request| request.path == "/par")
        .collect::<Vec<_>>();
    assert_eq!(
        par_requests.len(),
        3,
        "PAR must retry once after the DPoP nonce challenge"
    );
    let first_proof = par_requests[0].headers.get("dpop").unwrap();
    let retry_proof = par_requests[1].headers.get("dpop").unwrap();
    assert_ne!(first_proof, retry_proof);
    assert_eq!(
        jwt_claim(retry_proof, "nonce").as_deref(),
        Some("auth-nonce")
    );

    let timeline_requests = requests
        .iter()
        .filter(|request| request.path.starts_with("/xrpc/app.bsky.feed.getTimeline"))
        .collect::<Vec<_>>();
    assert_eq!(
        timeline_requests.len(),
        3,
        "expired access token must refresh and retry"
    );
    assert_eq!(
        jwt_claim(timeline_requests[1].headers.get("dpop").unwrap(), "nonce").as_deref(),
        Some("resource-nonce")
    );
    assert!(
        timeline_requests[2]
            .headers
            .get("authorization")
            .is_some_and(|value| value.contains("access-fresh"))
    );
    assert_eq!(shared.lock().unwrap().token_count, 2);

    server.abort();
}

#[tokio::test]
async fn production_oauth_flow_accepts_standard_callback_and_binds_subject_and_scope() {
    let (flow, _, shared, server) = production_flow(None, None).await;
    flow.authorize(DID, "browser-binding-one").await.unwrap();
    let confidential_par = shared
        .lock()
        .unwrap()
        .requests
        .iter()
        .find(|request| request.path == "/par")
        .unwrap()
        .clone();
    assert!(decoded_form_field(&confidential_par.body, "client_assertion").is_some());
    assert_eq!(
        decoded_form_field(&confidential_par.body, "client_id").as_deref(),
        Some("https://appview.example/.well-known/oauth-client-metadata.json")
    );
    assert_eq!(
        decoded_form_field(&confidential_par.body, "scope").as_deref(),
        Some(EXPECTED_OAUTH_SCOPE)
    );
    let state = latest_state(&shared);
    let result = flow
        .callback(OAuthCallback {
            code: "valid-code".to_owned(),
            state,
            issuer: Some(server_origin(&shared)),
            scope: None,
            browser_binding: Some("browser-binding-one".to_owned()),
        })
        .await;
    assert!(
        result.is_ok(),
        "standard callback should succeed: {result:?}"
    );
    assert_eq!(shared.lock().unwrap().token_count, 1);
    server.abort();

    let (flow, _, shared, server) =
        production_flow(Some("did:plc:zyxwvutsrqponmlkjihgfedcba"), Some("atproto")).await;
    flow.authorize(DID, "browser-binding-two").await.unwrap();
    let state = latest_state(&shared);
    let result = flow
        .callback(OAuthCallback {
            code: "valid-code".to_owned(),
            state,
            issuer: Some(server_origin(&shared)),
            scope: None,
            browser_binding: Some("browser-binding-two".to_owned()),
        })
        .await;
    assert!(result.unwrap_err().contains("subject DID"));
    server.abort();

    let (flow, _, shared, server) = production_flow(Some(DID), Some("profile")).await;
    flow.authorize(DID, "browser-binding-three").await.unwrap();
    let state = latest_state(&shared);
    let result = flow
        .callback(OAuthCallback {
            code: "valid-code".to_owned(),
            state,
            issuer: Some(server_origin(&shared)),
            scope: None,
            browser_binding: Some("browser-binding-three".to_owned()),
        })
        .await;
    assert!(result.unwrap_err().contains("atproto scope"));
    server.abort();
}

#[tokio::test]
async fn production_post_with_two_images_uploads_blobs_before_creating_record() {
    let public = OAuthPublic::new("https://appview.example").unwrap();
    let (flow, _, shared, server) =
        production_flow_with_public(public.clone(), None, Some(EXPECTED_OAUTH_SCOPE)).await;
    let storage = Arc::new(MemoryStorage::default());
    let app = public.router_with_flow(flow, storage);
    let cookie = finish_production_login(&app, &shared).await;
    let boundary = "protocol-two-pictures";
    let first = b"\xff\xd8\xfffirst-image";
    let second = b"\xff\xd8\xffsecond-image";
    let body = picture_multipart(
        boundary,
        "two pictures",
        None,
        None,
        &[
            ("a.jpg", first, "first alt", 1200, 800),
            ("b.jpg", second, "second alt", 2000, 1000),
        ],
    );
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/post")
                .header("origin", "https://appview.example")
                .header("sec-fetch-site", "same-origin")
                .header("cookie", cookie)
                .header(
                    "content-type",
                    format!("multipart/form-data; boundary={boundary}"),
                )
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SEE_OTHER);

    let state = shared.lock().unwrap();
    let upload_requests = state
        .requests
        .iter()
        .filter(|request| {
            request
                .path
                .starts_with("/xrpc/com.atproto.repo.uploadBlob")
        })
        .collect::<Vec<_>>();
    assert_eq!(upload_requests.len(), 2);
    assert_eq!(upload_requests[0].method, "POST");
    assert!(
        upload_requests[0]
            .path
            .starts_with("/xrpc/com.atproto.repo.uploadBlob")
    );
    assert_eq!(
        upload_requests[0]
            .headers
            .get("content-type")
            .map(String::as_str),
        Some("image/jpeg")
    );
    assert_eq!(upload_requests[0].body_bytes, first);
    assert_eq!(
        upload_requests[1]
            .headers
            .get("content-type")
            .map(String::as_str),
        Some("image/jpeg")
    );
    assert_eq!(upload_requests[1].body_bytes, second);
    let create_index = state
        .requests
        .iter()
        .position(|request| {
            request
                .path
                .starts_with("/xrpc/com.atproto.repo.createRecord")
        })
        .unwrap();
    let upload_indices = state
        .requests
        .iter()
        .enumerate()
        .filter_map(|(index, request)| {
            request
                .path
                .starts_with("/xrpc/com.atproto.repo.uploadBlob")
                .then_some(index)
        })
        .collect::<Vec<_>>();
    assert!(upload_indices.iter().all(|index| *index < create_index));
    assert_eq!(state.create_bodies.len(), 1);
    let record = &state.create_bodies[0]["record"];
    assert_eq!(record["embed"]["$type"], "app.bsky.embed.images");
    assert_eq!(record["embed"]["images"].as_array().unwrap().len(), 2);
    assert_eq!(record["embed"]["images"][0]["alt"], "first alt");
    assert_eq!(
        record["embed"]["images"][0]["image"],
        json!({
            "$type":"blob",
            "ref":{"$link":"bafyreiedv4rjxnzxvlvlgqbi4vpqi3s4iuz4buywtkgfjpfjn2gix5cocu"},
            "mimeType":"image/jpeg",
            "size":first.len()
        })
    );
    assert_eq!(
        record["embed"]["images"][0]["aspectRatio"],
        json!({"width":1200,"height":800})
    );
    assert_eq!(record["embed"]["images"][1]["alt"], "second alt");
    assert_eq!(
        record["embed"]["images"][1]["image"],
        json!({
            "$type":"blob",
            "ref":{"$link":"bafyreiedv4rjxnzxvlvlgqbi4vpqi3s4iuz4buywtkgfjpfjn2gix5cocu"},
            "mimeType":"image/jpeg",
            "size":second.len()
        })
    );
    assert_eq!(
        record["embed"]["images"][1]["aspectRatio"],
        json!({"width":2000,"height":1000})
    );
    drop(state);
    server.abort();
}

// current_thread + a thread-local capture guard around each awaited request: the task-scoped
// `.with_subscriber` guard alone loses events to the thread's default dispatcher when the
// multithreaded runtime polls this future on a worker thread another test left busy.
#[tokio::test(flavor = "current_thread")]
async fn write_timing_line_counts_a_fake_pds_dpop_nonce_retry() {
    let public = OAuthPublic::new("https://appview.example").unwrap();
    let (flow, _, shared, server) =
        production_flow_with_public(public.clone(), None, Some(EXPECTED_OAUTH_SCOPE)).await;
    let app = public.router_with_flow(flow, Arc::new(MemoryStorage::default()));
    let cookie = finish_production_login(&app, &shared).await;
    shared.lock().unwrap().create_nonce_required = true;

    let output = Arc::new(Mutex::new(Vec::new()));
    let make_subscriber = || {
        tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_max_level(tracing::Level::INFO)
            .with_writer(CapturedMakeWriter(output.clone()))
            .finish()
    };
    let subscriber = make_subscriber();
    let _thread_capture = tracing::subscriber::set_default(make_subscriber());
    let before = shared.lock().unwrap().requests.len();
    let boundary = "nonce-write";
    let body = picture_multipart(boundary, "nonce accounting", None, None, &[]);
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/post")
                .header("origin", "https://appview.example")
                .header("sec-fetch-site", "same-origin")
                .header("cookie", cookie.clone())
                .header(
                    "content-type",
                    format!("multipart/form-data; boundary={boundary}"),
                )
                .body(Body::from(body))
                .unwrap(),
        )
        .with_subscriber(subscriber)
        .await
        .unwrap();
    drop(_thread_capture);
    assert_eq!(response.status(), StatusCode::SEE_OTHER);

    let first_write_end = shared.lock().unwrap().requests.len();
    let second_output = Arc::new(Mutex::new(Vec::new()));
    let second_subscriber = tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .with_max_level(tracing::Level::INFO)
        .with_writer(CapturedMakeWriter(second_output.clone()))
        .finish();
    let _second_thread_capture = tracing::subscriber::set_default(
        tracing_subscriber::fmt()
            .without_time()
            .with_ansi(false)
            .with_max_level(tracing::Level::INFO)
            .with_writer(CapturedMakeWriter(second_output.clone()))
            .finish(),
    );
    let second_before = first_write_end;
    let second_boundary = "nonce-write-second";
    let second_body =
        picture_multipart(second_boundary, "nonce accounting second", None, None, &[]);
    let second_response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/post")
                .header("origin", "https://appview.example")
                .header("sec-fetch-site", "same-origin")
                .header("cookie", cookie)
                .header(
                    "content-type",
                    format!("multipart/form-data; boundary={second_boundary}"),
                )
                .body(Body::from(second_body))
                .unwrap(),
        )
        .with_subscriber(second_subscriber)
        .await
        .unwrap();
    drop(_second_thread_capture);
    assert_eq!(second_response.status(), StatusCode::SEE_OTHER);

    let requests = shared.lock().unwrap().requests.clone();
    let write_requests = &requests[before..first_write_end];
    let create_requests = write_requests
        .iter()
        .filter(|request| {
            request
                .path
                .starts_with("/xrpc/com.atproto.repo.createRecord")
        })
        .collect::<Vec<_>>();
    assert_eq!(
        create_requests.len(),
        2,
        "fake PDS challenges createRecord once"
    );
    assert_eq!(
        create_requests[1]
            .headers
            .get("dpop")
            .and_then(|proof| jwt_claim(proof, "nonce"))
            .as_deref(),
        Some("write-nonce")
    );

    let second_write_requests = &requests[second_before..];
    let second_create_requests = second_write_requests
        .iter()
        .filter(|request| {
            request
                .path
                .starts_with("/xrpc/com.atproto.repo.createRecord")
        })
        .collect::<Vec<_>>();
    assert_eq!(
        second_create_requests.len(),
        1,
        "a later write by the same user reuses the resource-server DPoP nonce"
    );
    assert_eq!(
        second_create_requests[0]
            .headers
            .get("dpop")
            .and_then(|proof| jwt_claim(proof, "nonce"))
            .as_deref(),
        Some("write-nonce")
    );

    let logs = String::from_utf8(output.lock().unwrap().clone()).unwrap();
    let lines = logs
        .lines()
        .filter(|line| line.contains("web write"))
        .collect::<Vec<_>>();
    assert_eq!(lines.len(), 1, "one timing line per write: {logs}");
    let line = lines[0];
    assert!(line.contains("route=\"post\""), "{line}");
    assert!(line.contains("dpop_nonce_retry=true"), "{line}");
    assert!(line.contains("dpop_nonce_retry:1/"), "{line}");
    assert!(
        line.contains(&format!("outbound_call_count={}", write_requests.len())),
        "{line}"
    );
    let second_logs = String::from_utf8(second_output.lock().unwrap().clone()).unwrap();
    let second_line = second_logs
        .lines()
        .find(|line| line.contains("web write"))
        .expect("second write timing line");
    assert!(
        second_line.contains("dpop_nonce_retries=0"),
        "{second_line}"
    );
    assert!(
        second_line.contains("dpop_nonce_retry=false"),
        "{second_line}"
    );
    server.abort();
}

#[tokio::test]
async fn production_link_card_uploads_og_image_blob_before_creating_record() {
    let public = OAuthPublic::new("https://appview.example").unwrap();
    let (flow, _, shared, server) =
        production_flow_with_public(public.clone(), None, Some(EXPECTED_OAUTH_SCOPE)).await;
    let write_flow = flow.clone();
    let app = public.router_with_flow(flow, Arc::new(MemoryStorage::default()));
    let cookie = finish_production_login(&app, &shared).await;
    let thumbnail = b"\xff\xd8\xffog-image";

    write_flow
        .create_post_with_external(
            DID,
            "A link card post",
            PostExternal {
                uri: "https://example.test/article".to_owned(),
                title: "Example article".to_owned(),
                description: "A short description".to_owned(),
                thumbnail: Some(LinkThumbnail {
                    bytes: thumbnail.to_vec(),
                    mime_type: "image/jpeg".to_owned(),
                }),
            },
        )
        .await
        .unwrap();

    let state = shared.lock().unwrap();
    let upload = state
        .requests
        .iter()
        .find(|request| {
            request
                .path
                .starts_with("/xrpc/com.atproto.repo.uploadBlob")
        })
        .unwrap();
    assert_eq!(upload.method, "POST");
    assert_eq!(
        upload.headers.get("content-type").map(String::as_str),
        Some("image/jpeg")
    );
    assert_eq!(upload.body_bytes, thumbnail);
    let upload_index = state
        .requests
        .iter()
        .position(|request| {
            request
                .path
                .starts_with("/xrpc/com.atproto.repo.uploadBlob")
        })
        .unwrap();
    let create_index = state
        .requests
        .iter()
        .position(|request| {
            request
                .path
                .starts_with("/xrpc/com.atproto.repo.createRecord")
        })
        .unwrap();
    assert!(upload_index < create_index);
    let external = &state.create_bodies[0]["record"]["embed"]["external"];
    assert_eq!(external["uri"], "https://example.test/article");
    assert_eq!(external["title"], "Example article");
    assert_eq!(external["description"], "A short description");
    assert_eq!(
        external["thumb"],
        json!({
            "$type":"blob",
            "ref":{"$link":"bafyreiedv4rjxnzxvlvlgqbi4vpqi3s4iuz4buywtkgfjpfjn2gix5cocu"},
            "mimeType":"image/jpeg",
            "size":thumbnail.len()
        })
    );
    assert!(cookie.contains("appview_session="));
    assert_eq!(state.upload_count, 1);
    assert_eq!(state.create_count, 1);
    drop(state);
    server.abort();
}

#[tokio::test]
async fn second_image_upload_failure_does_not_create_a_post() {
    let public = OAuthPublic::new("https://appview.example").unwrap();
    let (flow, _, shared, server) =
        production_flow_with_public(public.clone(), None, Some(EXPECTED_OAUTH_SCOPE)).await;
    shared.lock().unwrap().upload_fail_on = Some(2);
    let app = public.router_with_flow(flow, Arc::new(MemoryStorage::default()));
    let cookie = finish_production_login(&app, &shared).await;
    let boundary = "protocol-failed-picture";
    let body = picture_multipart(
        boundary,
        "no half post",
        None,
        None,
        &[
            ("a.jpg", b"\xff\xd8\xffone", "", 10, 10),
            ("b.jpg", b"\xff\xd8\xfftwo", "", 20, 20),
        ],
    );
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/post")
                .header("origin", "https://appview.example")
                .header("sec-fetch-site", "same-origin")
                .header("cookie", cookie)
                .header(
                    "content-type",
                    format!("multipart/form-data; boundary={boundary}"),
                )
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    let state = shared.lock().unwrap();
    assert_eq!(state.upload_count, 2);
    assert_eq!(state.create_count, 0);
    drop(state);
    server.abort();
}

#[tokio::test]
async fn upload_blob_response_without_ref_does_not_create_a_post() {
    let public = OAuthPublic::new("https://appview.example").unwrap();
    let (flow, _, shared, server) =
        production_flow_with_public(public.clone(), None, Some(EXPECTED_OAUTH_SCOPE)).await;
    shared.lock().unwrap().upload_missing_ref_on = Some(1);
    let app = public.router_with_flow(flow, Arc::new(MemoryStorage::default()));
    let cookie = finish_production_login(&app, &shared).await;
    let boundary = "protocol-missing-blob-ref";
    let body = picture_multipart(
        boundary,
        "no dangling image post",
        None,
        None,
        &[("missing-ref.jpg", b"\xff\xd8\xffimage", "image alt", 10, 10)],
    );
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/post")
                .header("origin", "https://appview.example")
                .header("sec-fetch-site", "same-origin")
                .header("cookie", cookie)
                .header(
                    "content-type",
                    format!("multipart/form-data; boundary={boundary}"),
                )
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    let state = shared.lock().unwrap();
    assert_eq!(state.upload_count, 1);
    assert_eq!(state.create_count, 0);
    drop(state);
    server.abort();
}

#[tokio::test]
async fn quote_with_an_image_uses_record_with_media() {
    let public = OAuthPublic::new("https://appview.example").unwrap();
    let (flow, _, shared, server) =
        production_flow_with_public(public.clone(), None, Some(EXPECTED_OAUTH_SCOPE)).await;
    let storage = Arc::new(MemoryStorage::default());
    let quote_uri = "at://did:plc:quoted/app.bsky.feed.post/target";
    storage
        .put_record(&IndexedRecord {
            did: "did:plc:quoted".into(),
            collection: "app.bsky.feed.post".into(),
            rkey: "target".into(),
            uri: quote_uri.into(),
            cid: Some("quoted-cid".into()),
            time_us: 1,
            record: json!({"$type":"app.bsky.feed.post","text":"quoted"}),
        })
        .unwrap();
    let app = public.router_with_flow(flow, storage);
    let cookie = finish_production_login(&app, &shared).await;
    let boundary = "protocol-quote-picture";
    let body = picture_multipart(
        boundary,
        "quote caption",
        Some(quote_uri),
        None,
        &[("quote.jpg", b"\xff\xd8\xffquote", "quote alt", 640, 480)],
    );
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/quote")
                .header("origin", "https://appview.example")
                .header("sec-fetch-site", "same-origin")
                .header("cookie", cookie)
                .header(
                    "content-type",
                    format!("multipart/form-data; boundary={boundary}"),
                )
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    let state = shared.lock().unwrap();
    let embed = &state.create_bodies[0]["record"]["embed"];
    assert_eq!(embed["$type"], "app.bsky.embed.recordWithMedia");
    assert_eq!(embed["record"]["$type"], "app.bsky.embed.record");
    assert_eq!(
        embed["record"]["record"],
        json!({"uri":quote_uri,"cid":"quoted-cid"})
    );
    assert_eq!(embed["media"]["$type"], "app.bsky.embed.images");
    assert_eq!(embed["media"]["images"][0]["alt"], "quote alt");
    drop(state);
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn pds_4xx_logs_safe_error_details_without_credentials() {
    let (flow, _, shared, server) = production_flow(None, None).await;
    flow.authorize(DID, "browser-binding-log-test")
        .await
        .unwrap();
    let state = latest_state(&shared);
    flow.callback(OAuthCallback {
        code: "valid-code".to_owned(),
        state,
        issuer: Some(server_origin(&shared)),
        scope: None,
        browser_binding: Some("browser-binding-log-test".to_owned()),
    })
    .await
    .unwrap();

    let output = Arc::new(Mutex::new(Vec::new()));
    let subscriber = tracing_subscriber::fmt()
        .with_ansi(false)
        .with_max_level(tracing::Level::WARN)
        .with_writer(CapturedMakeWriter(output.clone()))
        .finish();
    let dispatch = tracing::Dispatch::new(subscriber);
    let _guard = tracing::dispatcher::set_default(&dispatch);
    let result = flow.pinned_feeds(DID).await;
    assert!(result.is_err());
    drop(_guard);

    let logs = String::from_utf8(output.lock().unwrap().clone()).unwrap();
    assert!(logs.contains("403"), "{logs}");
    assert!(logs.contains("MissingPermission"), "{logs}");
    assert!(
        logs.contains("saved feed preferences are not permitted"),
        "{logs}"
    );
    assert!(!logs.contains("access-expired"), "{logs}");
    let request = shared
        .lock()
        .unwrap()
        .requests
        .iter()
        .find(|request| {
            request
                .path
                .starts_with("/xrpc/app.bsky.actor.getPreferences")
        })
        .cloned()
        .unwrap();
    assert!(
        !logs.contains(request.headers.get("dpop").unwrap()),
        "{logs}"
    );
    assert!(!logs.contains("browser-binding-log-test"), "{logs}");
    server.abort();
}

#[tokio::test]
async fn old_granted_scope_in_production_session_shows_feed_reauthorization_page() {
    let old_scope = "atproto repo:app.bsky.feed.post?action=create repo:app.bsky.feed.like?action=create&action=delete repo:app.bsky.feed.repost?action=create&action=delete";
    let public = OAuthPublic::new("https://appview.example").unwrap();
    let (flow, _, shared, server) =
        production_flow_with_public(public.clone(), None, Some(old_scope)).await;
    let app = public.router_with_flow(flow, Arc::new(MemoryStorage::default()));
    let authorize = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/oauth/authorize")
                .header("origin", "https://appview.example")
                .header("sec-fetch-site", "same-origin")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from("handle=did%3Aplc%3Aabcdefghijklmnopqrstuvwx"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(authorize.status(), StatusCode::SEE_OTHER);
    let login_cookie = authorize.headers()["set-cookie"]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    let state = latest_state(&shared);
    let issuer = server_origin(&shared);
    let callback = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/oauth/callback?code=valid-code&state={state}&iss={}",
                    url::form_urlencoded::byte_serialize(issuer.as_bytes()).collect::<String>()
                ))
                .header("cookie", login_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(callback.status(), StatusCode::SEE_OTHER);
    let session_cookie = callback
        .headers()
        .get_all("set-cookie")
        .iter()
        .find_map(|header| {
            let value = header.to_str().ok()?;
            value
                .starts_with("appview_session=")
                .then(|| value.split(';').next().unwrap().to_owned())
        })
        .unwrap();
    let feeds = app
        .oneshot(
            Request::builder()
                .uri("/feeds")
                .header("cookie", session_cookie)
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(feeds.status(), StatusCode::OK);
    let html = String::from_utf8(
        feeds
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert!(
        html.to_ascii_lowercase()
            .contains("sign in again to allow feeds")
    );
    assert!(html.contains("href=\"/login\""));
    server.abort();
}

#[tokio::test]
async fn fake_pds_receives_correct_reply_root_and_parent_strong_refs() {
    let (flow, _, shared, server) = production_flow(None, Some(EXPECTED_OAUTH_SCOPE)).await;
    flow.authorize(DID, "reply-test-binding").await.unwrap();
    let state = latest_state(&shared);
    flow.callback(OAuthCallback {
        code: "valid-code".to_owned(),
        state,
        issuer: Some(server_origin(&shared)),
        scope: None,
        browser_binding: Some("reply-test-binding".to_owned()),
    })
    .await
    .unwrap();

    let root = PostStrongRef {
        uri: format!("at://{DID}/app.bsky.feed.post/root"),
        cid: "root-cid".to_owned(),
    };
    let parent = PostStrongRef {
        uri: root.uri.clone(),
        cid: root.cid.clone(),
    };
    flow.create_reply(DID, "Top level reply", root.clone(), parent)
        .await
        .unwrap();
    let reply_parent = PostStrongRef {
        uri: format!("at://{DID}/app.bsky.feed.post/reply-one"),
        cid: "reply-one-cid".to_owned(),
    };
    flow.create_reply(DID, "Nested reply", root.clone(), reply_parent.clone())
        .await
        .unwrap();

    let requests = shared.lock().unwrap().requests.clone();
    let creates = requests
        .iter()
        .filter(|request| {
            request
                .path
                .starts_with("/xrpc/com.atproto.repo.createRecord")
        })
        .collect::<Vec<_>>();
    assert_eq!(creates.len(), 2);
    let top: Value = serde_json::from_str(&creates[0].body).unwrap();
    let nested: Value = serde_json::from_str(&creates[1].body).unwrap();
    let root_json = json!({"uri":root.uri,"cid":root.cid});
    assert_eq!(top["record"]["reply"]["root"], root_json);
    assert_eq!(top["record"]["reply"]["parent"], root_json);
    assert_eq!(nested["record"]["reply"]["root"], root_json);
    assert_eq!(
        nested["record"]["reply"]["parent"],
        json!({"uri":reply_parent.uri,"cid":reply_parent.cid})
    );
    server.abort();
}

#[tokio::test]
async fn fake_pds_receives_quote_post_with_record_embed_strong_ref() {
    let (flow, _, shared, server) = production_flow(None, Some(EXPECTED_OAUTH_SCOPE)).await;
    flow.authorize(DID, "quote-test-binding").await.unwrap();
    let state = latest_state(&shared);
    flow.callback(OAuthCallback {
        code: "valid-code".to_owned(),
        state,
        issuer: Some(server_origin(&shared)),
        scope: None,
        browser_binding: Some("quote-test-binding".to_owned()),
    })
    .await
    .unwrap();

    let quoted = PostStrongRef {
        uri: format!("at://{DID}/app.bsky.feed.post/original"),
        cid: "original-cid".to_owned(),
    };
    flow.create_quote(DID, "with some context", quoted.clone())
        .await
        .unwrap();

    let request = shared
        .lock()
        .unwrap()
        .requests
        .iter()
        .find(|request| {
            request
                .path
                .starts_with("/xrpc/com.atproto.repo.createRecord")
        })
        .cloned()
        .unwrap();
    let body: Value = serde_json::from_str(&request.body).unwrap();
    assert_eq!(body["collection"], "app.bsky.feed.post");
    assert_eq!(body["record"]["$type"], "app.bsky.feed.post");
    assert_eq!(body["record"]["text"], "with some context");
    assert_eq!(body["record"]["embed"]["$type"], "app.bsky.embed.record");
    assert_eq!(
        body["record"]["embed"]["record"],
        json!({"uri":quoted.uri,"cid":quoted.cid})
    );
    server.abort();
}

#[tokio::test]
async fn fake_pds_records_profile_graph_shapes_and_proxy_mute_rpc() {
    let (flow, _, shared, server) = production_flow(None, Some(EXPECTED_OAUTH_SCOPE)).await;
    flow.authorize(DID, "profile-actions-binding")
        .await
        .unwrap();
    let state = latest_state(&shared);
    flow.callback(OAuthCallback {
        code: "valid-code".to_owned(),
        state,
        issuer: Some(server_origin(&shared)),
        scope: None,
        browser_binding: Some("profile-actions-binding".to_owned()),
    })
    .await
    .unwrap();

    for (collection, record) in [
        (
            "app.bsky.graph.follow",
            json!({"$type":"app.bsky.graph.follow","subject":"did:plc:friend","createdAt":"2026-10-29T00:00:00Z"}),
        ),
        (
            "app.bsky.graph.block",
            json!({"$type":"app.bsky.graph.block","subject":"did:plc:friend","createdAt":"2026-10-29T00:00:00Z"}),
        ),
        (
            "app.bsky.graph.listitem",
            json!({"$type":"app.bsky.graph.listitem","subject":"did:plc:friend","list":format!("at://{DID}/app.bsky.graph.list/friends"),"createdAt":"2026-10-29T00:00:00Z"}),
        ),
    ] {
        flow.create_profile_graph_record(DID, collection, record)
            .await
            .unwrap();
    }
    for (collection, rkey) in [
        ("app.bsky.graph.follow", "follow-record"),
        ("app.bsky.graph.block", "block-record"),
        ("app.bsky.graph.listitem", "listitem-record"),
    ] {
        flow.delete_profile_graph_record(
            DID,
            collection,
            &format!("at://{DID}/{collection}/{rkey}"),
        )
        .await
        .unwrap();
    }
    flow.set_actor_muted(DID, "did:plc:friend", true)
        .await
        .unwrap();
    flow.set_actor_muted(DID, "did:plc:friend", false)
        .await
        .unwrap();
    flow.muted_actor_dids(DID).await.unwrap();

    let state = shared.lock().unwrap();
    let created = state
        .requests
        .iter()
        .filter(|request| {
            request
                .path
                .starts_with("/xrpc/com.atproto.repo.createRecord")
        })
        .map(|request| serde_json::from_str::<Value>(&request.body).unwrap())
        .collect::<Vec<_>>();
    assert_eq!(created.len(), 3);
    assert_eq!(created[0]["collection"], "app.bsky.graph.follow");
    assert_eq!(created[0]["record"]["subject"], "did:plc:friend");
    assert_eq!(created[1]["collection"], "app.bsky.graph.block");
    assert_eq!(created[1]["record"]["subject"], "did:plc:friend");
    assert_eq!(created[2]["collection"], "app.bsky.graph.listitem");
    assert_eq!(created[2]["record"]["subject"], "did:plc:friend");
    assert_eq!(
        created[2]["record"]["list"],
        format!("at://{DID}/app.bsky.graph.list/friends")
    );
    let deletes = state
        .requests
        .iter()
        .filter(|request| {
            request
                .path
                .starts_with("/xrpc/com.atproto.repo.deleteRecord")
        })
        .collect::<Vec<_>>();
    assert_eq!(deletes.len(), 3);
    let mute = state
        .requests
        .iter()
        .find(|request| request.path.starts_with("/xrpc/app.bsky.graph.muteActor"))
        .unwrap();
    assert_eq!(mute.method, "POST");
    assert_eq!(
        mute.headers.get("atproto-proxy").map(String::as_str),
        Some("did:web:api.bsky.app#bsky_appview")
    );
    assert_eq!(
        serde_json::from_str::<Value>(&mute.body).unwrap()["actor"],
        "did:plc:friend"
    );
    let unmute = state
        .requests
        .iter()
        .find(|request| request.path.starts_with("/xrpc/app.bsky.graph.unmuteActor"))
        .unwrap();
    assert_eq!(
        unmute.headers.get("atproto-proxy").map(String::as_str),
        Some("did:web:api.bsky.app#bsky_appview")
    );
    assert!(
        state
            .requests
            .iter()
            .any(|request| request.path.starts_with("/xrpc/app.bsky.graph.getMutes"))
    );
    server.abort();
}

#[tokio::test]
async fn live_author_profile_read_uses_the_resolved_pds_and_indexes_record_shape() {
    let (flow, _, shared, server) = production_flow(None, None).await;
    let fetched = flow.fetch_author_profile(DID).await.unwrap();
    let profile = fetched
        .profile
        .expect("fake PDS returns the profile record");
    assert_eq!(profile.did, DID);
    assert_eq!(profile.collection, "app.bsky.actor.profile");
    assert_eq!(profile.rkey, "self");
    assert_eq!(profile.record["displayName"], "From fake PDS");
    assert_eq!(fetched.verified_handle, None);

    let request = shared
        .lock()
        .unwrap()
        .requests
        .iter()
        .find(|request| request.path.starts_with("/xrpc/com.atproto.repo.getRecord"))
        .cloned()
        .expect("profile should be fetched from the resolved PDS");
    assert!(request.path.contains("collection=app.bsky.actor.profile"));
    assert!(request.path.contains("rkey=self"));
    assert!(!request.headers.contains_key("authorization"));
    server.abort();
}

#[tokio::test]
async fn loopback_production_oauth_completes_a_public_client_login() {
    let (flow, _, shared, server) = production_loopback_flow().await;
    let callback_uri = "http://127.0.0.1:43123/oauth/callback";
    let browser_binding = "loopback-browser-binding";
    let authorize_url = flow.authorize(DID, browser_binding).await.unwrap();
    assert!(authorize_url.starts_with(&format!("{}/authorize?", server_origin(&shared))));

    let par = shared
        .lock()
        .unwrap()
        .requests
        .iter()
        .find(|request| request.path == "/par")
        .unwrap()
        .clone();
    let client_id = decoded_form_field(&par.body, "client_id").unwrap();
    let client_id_url = reqwest::Url::parse(&client_id).unwrap();
    assert_eq!(client_id_url.scheme(), "http");
    assert_eq!(client_id_url.host_str(), Some("localhost"));
    assert_eq!(
        decoded_form_field(&par.body, "redirect_uri").as_deref(),
        Some(callback_uri)
    );
    assert_eq!(
        decoded_form_field(&par.body, "code_challenge_method").as_deref(),
        Some("S256")
    );
    assert_eq!(
        decoded_form_field(&par.body, "scope").as_deref(),
        Some(EXPECTED_OAUTH_SCOPE)
    );
    assert!(decoded_form_field(&par.body, "client_assertion").is_none());
    assert!(par.headers.contains_key("dpop"));
    assert!(
        !shared
            .lock()
            .unwrap()
            .requests
            .iter()
            .any(|request| { request.path == "/.well-known/oauth-client-metadata.json" })
    );

    let state = latest_state(&shared);
    let account = flow
        .callback(OAuthCallback {
            code: "valid-code".to_owned(),
            state,
            issuer: Some(server_origin(&shared)),
            scope: None,
            browser_binding: Some(browser_binding.to_owned()),
        })
        .await
        .unwrap();
    assert_eq!(account.did, DID);
    assert_eq!(account.granted_scope.as_deref(), Some("atproto"));

    let requests = shared.lock().unwrap().requests.clone();
    let token = requests
        .iter()
        .find(|request| request.path == "/token")
        .unwrap();
    assert!(decoded_form_field(&token.body, "client_assertion").is_none());
    assert!(decoded_form_field(&token.body, "code_verifier").is_some());
    assert!(token.headers.contains_key("dpop"));
    server.abort();
}

#[tokio::test]
async fn production_oauth_flow_rejects_a_foreign_browser_binding() {
    let (flow, store, shared, server) = production_flow(None, Some("atproto")).await;
    flow.authorize(DID, "browser-binding-expected")
        .await
        .unwrap();
    let state = latest_state(&shared);
    let result = flow
        .callback(OAuthCallback {
            code: "valid-code".to_owned(),
            state,
            issuer: Some(server_origin(&shared)),
            scope: None,
            browser_binding: Some("browser-binding-from-another-browser".to_owned()),
        })
        .await;
    assert!(
        result.unwrap_err().contains("browser binding"),
        "production callback must reject a state bound to another browser"
    );
    assert!(
        store
            .get(&DID.parse::<Did>().unwrap())
            .await
            .unwrap()
            .is_none(),
        "production callback must delete the exchanged OAuth session on a binding mismatch"
    );
    assert!(
        shared
            .lock()
            .unwrap()
            .requests
            .iter()
            .any(|request| request.path == "/revoke"),
        "production callback must revoke the exchanged OAuth tokens on a binding mismatch"
    );
    server.abort();
}

async fn production_flow(
    token_sub: Option<&str>,
    token_scope: Option<&str>,
) -> (
    appview_rs::oauth::AtriumOAuthFlow<ProductionFlowIdentity>,
    SqliteOAuthStore,
    Arc<Mutex<FakeServerState>>,
    tokio::task::JoinHandle<()>,
) {
    let public = OAuthPublic::new("https://appview.example").unwrap();
    production_flow_with_public(public, token_sub, token_scope).await
}

async fn production_loopback_flow() -> (
    appview_rs::oauth::AtriumOAuthFlow<ProductionFlowIdentity>,
    SqliteOAuthStore,
    Arc<Mutex<FakeServerState>>,
    tokio::task::JoinHandle<()>,
) {
    let public = OAuthPublic::new_loopback("127.0.0.1:43123".parse().unwrap()).unwrap();
    production_flow_with_public(public, None, None).await
}

async fn production_flow_with_public(
    public: OAuthPublic,
    token_sub: Option<&str>,
    token_scope: Option<&str>,
) -> (
    appview_rs::oauth::AtriumOAuthFlow<ProductionFlowIdentity>,
    SqliteOAuthStore,
    Arc<Mutex<FakeServerState>>,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let origin = format!("http://{address}");
    let shared = Arc::new(Mutex::new(FakeServerState {
        token_sub: token_sub.map(str::to_owned),
        token_scope: token_scope.map(str::to_owned),
        ..Default::default()
    }));
    let server_state = shared.clone();
    let server_origin = origin.clone();
    let server = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let state = server_state.clone();
            let origin = server_origin.clone();
            tokio::spawn(async move {
                serve_one(stream, &origin, state).await;
            });
        }
    });

    let identity = ProductionFlowIdentity {
        document: AppDidDocument {
            id: DID.to_owned(),
            also_known_as: vec![],
            verification_method: vec![],
            service: vec![DidService {
                id: "#atproto_pds".to_owned(),
                type_name: "AtprotoPersonalDataServer".to_owned(),
                service_endpoint: origin.clone(),
            }],
        },
    };
    let resolver = Arc::new(IdentityResolver::new(identity, Duration::from_secs(30)));
    let store = SqliteOAuthStore::open(":memory:").unwrap();
    let flow = public
        .atrium_flow_with_plc_directory(resolver, store.clone(), &format!("{origin}/"))
        .unwrap();
    (flow, store, shared, server)
}

fn server_origin(shared: &Arc<Mutex<FakeServerState>>) -> String {
    shared
        .lock()
        .unwrap()
        .requests
        .iter()
        .find(|request| request.path == "/.well-known/oauth-authorization-server")
        .and_then(|request| request.headers.get("host"))
        .map(|host| format!("http://{host}"))
        .unwrap()
}

async fn finish_production_login(app: &Router, shared: &Arc<Mutex<FakeServerState>>) -> String {
    let authorize = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/oauth/authorize")
                .header("origin", "https://appview.example")
                .header("sec-fetch-site", "same-origin")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from("handle=did%3Aplc%3Aabcdefghijklmnopqrstuvwx"))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(authorize.status(), StatusCode::SEE_OTHER);
    let login_cookie = authorize.headers()["set-cookie"]
        .to_str()
        .unwrap()
        .split(';')
        .next()
        .unwrap()
        .to_owned();
    let state = latest_state(shared);
    let encoded_state = url::form_urlencoded::byte_serialize(state.as_bytes()).collect::<String>();
    let encoded_issuer =
        url::form_urlencoded::byte_serialize(server_origin(shared).as_bytes()).collect::<String>();
    let callback = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!(
                    "/oauth/callback?code=valid-code&state={encoded_state}&iss={encoded_issuer}"
                ))
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

fn picture_multipart(
    boundary: &str,
    text: &str,
    quote_uri: Option<&str>,
    parent_uri: Option<&str>,
    images: &[(&str, &[u8], &str, u32, u32)],
) -> Vec<u8> {
    let mut body = Vec::new();
    append_multipart_text(&mut body, boundary, "text", text);
    if let Some(uri) = quote_uri {
        append_multipart_text(&mut body, boundary, "quote_uri", uri);
    }
    if let Some(uri) = parent_uri {
        append_multipart_text(&mut body, boundary, "parent_uri", uri);
    }
    for (filename, bytes, alt, width, height) in images {
        body.extend_from_slice(
            format!(
                "--{boundary}\r\nContent-Disposition: form-data; name=\"images\"; filename=\"{filename}\"\r\nContent-Type: image/jpeg\r\n\r\n"
            )
            .as_bytes(),
        );
        body.extend_from_slice(bytes);
        body.extend_from_slice(b"\r\n");
        append_multipart_text(&mut body, boundary, "alts", alt);
        append_multipart_text(&mut body, boundary, "widths", &width.to_string());
        append_multipart_text(&mut body, boundary, "heights", &height.to_string());
    }
    body.extend_from_slice(format!("--{boundary}--\r\n").as_bytes());
    body
}

fn append_multipart_text(body: &mut Vec<u8>, boundary: &str, name: &str, value: &str) {
    body.extend_from_slice(
        format!(
            "--{boundary}\r\nContent-Disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n"
        )
        .as_bytes(),
    );
}

fn latest_state(shared: &Arc<Mutex<FakeServerState>>) -> String {
    shared
        .lock()
        .unwrap()
        .requests
        .iter()
        .rev()
        .find(|request| request.path == "/par")
        .and_then(|request| form_field(&request.body, "state"))
        .unwrap()
}

fn form_field(body: &str, key: &str) -> Option<String> {
    body.split('&').find_map(|pair| {
        let (candidate, value) = pair.split_once('=')?;
        (candidate == key).then(|| value.replace('+', " "))
    })
}

fn decoded_form_field(body: &str, key: &str) -> Option<String> {
    url::form_urlencoded::parse(body.as_bytes())
        .find_map(|(candidate, value)| (candidate == key).then(|| value.into_owned()))
}

fn jwt_claim(jwt: &str, claim: &str) -> Option<String> {
    let payload = jwt.split('.').nth(1)?;
    let decoded = URL_SAFE_NO_PAD.decode(payload).ok()?;
    serde_json::from_slice::<Value>(&decoded)
        .ok()?
        .get(claim)?
        .as_str()
        .map(str::to_owned)
}

async fn serve_one(mut stream: TcpStream, origin: &str, state: Arc<Mutex<FakeServerState>>) {
    let request = read_request(&mut stream).await.unwrap();
    let response = {
        let mut state = state.lock().unwrap();
        state.requests.push(request.clone());
        match request.path.as_str() {
            path if path.starts_with("/did:plc:") => (
                200,
                vec![],
                json!({
                    "id":path.trim_start_matches('/'),
                    "alsoKnownAs":[],
                    "verificationMethod":[],
                    "service":[{"id":"#atproto_pds","type":"AtprotoPersonalDataServer","serviceEndpoint":origin}]
                }).to_string(),
            ),
            "/.well-known/oauth-protected-resource" => (
                200,
                vec![],
                json!({"resource":origin,"authorization_servers":[origin],"scopes_supported":["atproto"]}).to_string(),
            ),
            "/.well-known/oauth-authorization-server" => (
                200,
                vec![],
                json!({
                    "issuer":origin,
                    "authorization_endpoint":format!("{origin}/authorize"),
                    "token_endpoint":format!("{origin}/token"),
                    "pushed_authorization_request_endpoint":format!("{origin}/par"),
                    "revocation_endpoint":format!("{origin}/revoke"),
                    "scopes_supported":["atproto"],
                    "response_types_supported":["code"],
                    "grant_types_supported":["authorization_code","refresh_token"],
                    "token_endpoint_auth_methods_supported":["none","private_key_jwt"],
                    "token_endpoint_auth_signing_alg_values_supported":["ES256"],
                    "authorization_response_iss_parameter_supported":true,
                    "dpop_signing_alg_values_supported":["ES256"],
                    "protected_resources":[origin]
                }).to_string(),
            ),
            "/par" => {
                state.par_count += 1;
                if state.par_count == 1 {
                    (
                        400,
                        vec![("DPoP-Nonce", "auth-nonce")],
                        json!({"error":"use_dpop_nonce"}).to_string(),
                    )
                } else {
                    (
                        201,
                        vec![("DPoP-Nonce", "auth-nonce")],
                        json!({"request_uri":"urn:example:request","expires_in":60}).to_string(),
                    )
                }
            }
            "/token" => {
                state.token_count += 1;
                let sub = state
                    .token_sub
                    .as_deref()
                    .unwrap_or(DID)
                    .to_owned();
                let scope = state
                    .token_scope
                    .as_deref()
                    .unwrap_or("atproto")
                    .to_owned();
                if request.body.contains("grant_type=refresh_token") {
                    if let Some((status, error)) = state.refresh_failure.clone() {
                        (
                            status,
                            vec![],
                            json!({
                                "error":error,
                                "error_description":"the refresh token was rejected by the fake authorization server"
                            })
                            .to_string(),
                        )
                    } else {
                        let access = state
                            .refresh_access_tokens
                            .pop_front()
                            .unwrap_or_else(|| "access-fresh".to_owned());
                        (
                            200,
                            vec![],
                            json!({"access_token":access,"refresh_token":"refresh-fresh","token_type":"DPoP","expires_in":3600,"scope":scope,"sub":sub}).to_string(),
                        )
                    }
                } else {
                    let access = state
                        .initial_access_token
                        .clone()
                        .unwrap_or_else(|| "access-expired".to_owned());
                    let expires_in = state.initial_expires_in.unwrap_or(0);
                    (
                        200,
                        vec![],
                        json!({"access_token":access,"refresh_token":"refresh-initial","token_type":"DPoP","expires_in":expires_in,"scope":scope,"sub":sub}).to_string(),
                    )
                }
            }
            path
                if path.starts_with("/xrpc/com.atproto.server.getServiceAuth")
                    && state.service_auth_valid_access_tokens.is_some() =>
            {
                let valid = state
                    .service_auth_valid_access_tokens
                    .clone()
                    .unwrap_or_default();
                let authorization = request
                    .headers
                    .get("authorization")
                    .map(String::as_str)
                    .unwrap_or_default();
                let token = authorization.trim_start_matches("DPoP ").trim();
                if valid.iter().any(|valid| valid == token) {
                    (200, vec![], json!({"token":"for-you-service-jwt"}).to_string())
                } else {
                    (
                        401,
                        vec![("WWW-Authenticate", "DPoP error=\"invalid_token\"")],
                        json!({"error":"invalid_token"}).to_string(),
                    )
                }
            }
            path
                if path.starts_with("/xrpc/app.bsky.feed.getFeedSkeleton")
                    && state.skeleton_required_jwt.is_some() =>
            {
                let required = state
                    .skeleton_required_jwt
                    .clone()
                    .unwrap_or_default();
                let authorization = request
                    .headers
                    .get("authorization")
                    .map(String::as_str)
                    .unwrap_or_default();
                if authorization == format!("Bearer {required}") {
                    (200, vec![], json!({"feed":[]}).to_string())
                } else {
                    (
                        403,
                        vec![],
                        json!({"error":"AuthenticationRequired","message":"this feed requires service auth"}).to_string(),
                    )
                }
            }
            "/revoke" => (200, vec![], "{}".to_owned()),
            path if path.starts_with("/xrpc/app.bsky.feed.getTimeline") => {
                state.timeline_count += 1;
                match state.timeline_count {
                    1 => (
                        401,
                        vec![
                            ("DPoP-Nonce", "resource-nonce"),
                            ("WWW-Authenticate", "DPoP error=\"use_dpop_nonce\""),
                        ],
                        json!({"error":"use_dpop_nonce"}).to_string(),
                    ),
                    2 => (
                        401,
                        vec![("WWW-Authenticate", "DPoP error=\"invalid_token\"")],
                        json!({"error":"invalid_token"}).to_string(),
                    ),
                    _ => (200, vec![], json!({"feed":[]}).to_string()),
                }
            }
            path if path.starts_with("/xrpc/app.bsky.actor.getPreferences") => (
                403,
                vec![],
                json!({
                    "error":"MissingPermission",
                    "message":"saved feed preferences are not permitted"
                })
                .to_string(),
            ),
            path if path.starts_with("/xrpc/com.atproto.repo.uploadBlob") => {
                state.upload_count += 1;
                if state.upload_fail_on == Some(state.upload_count) {
                    (500, vec![], json!({"error":"UploadFailed","message":"fake upload failure"}).to_string())
                } else if state.upload_missing_ref_on == Some(state.upload_count) {
                    (
                        200,
                        vec![],
                        json!({
                            "blob": {
                                "$type":"blob",
                                "mimeType":request.headers.get("content-type").map(String::as_str).unwrap_or("application/octet-stream"),
                                "size":request.body_bytes.len()
                            }
                        })
                        .to_string(),
                    )
                } else {
                    let mime = request.headers.get("content-type").map(String::as_str).unwrap_or("application/octet-stream");
                    (
                        200,
                        vec![],
                        json!({"blob":{"$type":"blob","ref":{"$link":"bafyreiedv4rjxnzxvlvlgqbi4vpqi3s4iuz4buywtkgfjpfjn2gix5cocu"},"mimeType":mime,"size":request.body_bytes.len()}}).to_string(),
                    )
                }
            }
            path if path.starts_with("/xrpc/com.atproto.repo.createRecord") => {
                let nonce = request
                    .headers
                    .get("dpop")
                    .and_then(|proof| jwt_claim(proof, "nonce"));
                let token_gate = state
                    .create_required_access_tokens
                    .as_ref()
                    .is_some_and(|valid| {
                        let authorization = request
                            .headers
                            .get("authorization")
                            .map(String::as_str)
                            .unwrap_or_default();
                        let token = authorization.trim_start_matches("DPoP ").trim();
                        !valid.iter().any(|valid| valid == token)
                    });
                if token_gate {
                    (
                        401,
                        vec![("WWW-Authenticate", "DPoP error=\"invalid_token\"")],
                        json!({"error":"invalid_token"}).to_string(),
                    )
                } else if let Some((status, message)) = state.create_failure.clone() {
                    (
                        status,
                        vec![],
                        json!({"error":"InternalFailure","message":message}).to_string(),
                    )
                } else if state.create_nonce_required
                    && nonce.as_deref() != Some("write-nonce")
                {
                    state.create_nonce_challenges += 1;
                    (
                        401,
                        vec![
                            ("DPoP-Nonce", "write-nonce"),
                            ("WWW-Authenticate", "DPoP error=\"use_dpop_nonce\""),
                        ],
                        json!({"error":"use_dpop_nonce"}).to_string(),
                    )
                } else {
                state.create_count += 1;
                let body: Value = serde_json::from_str(&request.body).unwrap();
                state.create_bodies.push(body.clone());
                let number = state.create_count;
                let collection = body["collection"].as_str().unwrap_or("app.bsky.feed.post");
                (
                    200,
                    vec![],
                    json!({
                        "uri":format!("at://{DID}/{collection}/record-{number}"),
                        "cid":"bafyreiedv4rjxnzxvlvlgqbi4vpqi3s4iuz4buywtkgfjpfjn2gix5cocu",
                        "commit":{"cid":"bafyreiedv4rjxnzxvlvlgqbi4vpqi3s4iuz4buywtkgfjpfjn2gix5cocu","rev":"3lq6p4xgrb22a"}
                    })
                    .to_string(),
                )
                }
            }
            path if path.starts_with("/xrpc/com.atproto.repo.deleteRecord") => {
                (
                    200,
                    vec![],
                    json!({"commit":{"cid":"bafyreiedv4rjxnzxvlvlgqbi4vpqi3s4iuz4buywtkgfjpfjn2gix5cocu","rev":"3lq6p4xgrb22a"}}).to_string(),
                )
            }
            path if path.starts_with("/xrpc/app.bsky.graph.getMutes") => {
                let revoked = state.service_auth_valid_access_tokens.clone().is_some_and(|valid| {
                    let authorization = request
                        .headers
                        .get("authorization")
                        .map(String::as_str)
                        .unwrap_or_default();
                    let token = authorization.trim_start_matches("DPoP ").trim();
                    !valid.iter().any(|valid| valid == token)
                });
                if revoked {
                    (
                        401,
                        vec![("WWW-Authenticate", "DPoP error=\"invalid_token\"")],
                        json!({"error":"invalid_token"}).to_string(),
                    )
                } else {
                    (200, vec![], json!({"mutes":[]}).to_string())
                }
            }
            path if path.starts_with("/xrpc/app.bsky.graph.muteActor")
                || path.starts_with("/xrpc/app.bsky.graph.unmuteActor") =>
            {
                (
                    200,
                    vec![("Content-Type", "application/octet-stream")],
                    String::new(),
                )
            }
            path if path.starts_with("/xrpc/com.atproto.repo.getRecord") => {
                if state.feed_generator_service_did.is_some()
                    && path.contains("collection=app.bsky.feed.generator")
                {
                    let service_did = state
                        .feed_generator_service_did
                        .clone()
                        .unwrap_or_default();
                    (
                        200,
                        vec![],
                        json!({
                            "uri":format!("at://{DID}/app.bsky.feed.generator/for-you"),
                            "cid":"bafyreifakegenerator",
                            "value":{"$type":"app.bsky.feed.generator","did":service_did,"displayName":"For you"}
                        })
                        .to_string(),
                    )
                } else {
                    (
                        200,
                        vec![],
                        json!({
                            "uri":format!("at://{DID}/app.bsky.actor.profile/self"),
                            "cid":"bafyreifakeprofile",
                            "value":{"$type":"app.bsky.actor.profile","displayName":"From fake PDS"}
                        })
                        .to_string(),
                    )
                }
            }
            _ => (404, vec![], json!({"error":"not_found"}).to_string()),
        }
    };
    write_response(&mut stream, response.0, &response.1, &response.2).await;
}

async fn read_request(stream: &mut TcpStream) -> std::io::Result<CapturedRequest> {
    let mut bytes = Vec::new();
    let mut chunk = [0; 2048];
    let header_end = loop {
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "request ended",
            ));
        }
        bytes.extend_from_slice(&chunk[..read]);
        if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break index + 4;
        }
    };
    let head = String::from_utf8_lossy(&bytes[..header_end]);
    let mut lines = head.split("\r\n");
    let first = lines.next().unwrap_or_default();
    let method = first
        .split_whitespace()
        .next()
        .unwrap_or_default()
        .to_owned();
    let path = first.split_whitespace().nth(1).unwrap_or("/").to_owned();
    let mut headers = HashMap::new();
    let content_length = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| {
            let name = name.to_ascii_lowercase();
            let value = value.trim().to_owned();
            headers.insert(name.clone(), value.clone());
            (name, value)
        })
        .find_map(|(name, value)| {
            (name == "content-length")
                .then(|| value.parse::<usize>().ok())
                .flatten()
        })
        .unwrap_or(0);
    while bytes.len() < header_end + content_length {
        let read = stream.read(&mut chunk).await?;
        if read == 0 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::UnexpectedEof,
                "body ended",
            ));
        }
        bytes.extend_from_slice(&chunk[..read]);
    }
    Ok(CapturedRequest {
        method,
        path,
        headers,
        body: String::from_utf8_lossy(&bytes[header_end..header_end + content_length]).into_owned(),
        body_bytes: bytes[header_end..header_end + content_length].to_vec(),
    })
}

async fn write_response(stream: &mut TcpStream, status: u16, headers: &[(&str, &str)], body: &str) {
    let reason = match status {
        200 => "OK",
        201 => "Created",
        400 => "Bad Request",
        401 => "Unauthorized",
        403 => "Forbidden",
        _ => "Not Found",
    };
    let content_type = headers
        .iter()
        .find(|(name, _)| name.eq_ignore_ascii_case("content-type"))
        .map(|(_, value)| *value)
        .unwrap_or("application/json");
    let mut response = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    );
    for (name, value) in headers {
        if !name.eq_ignore_ascii_case("content-type") {
            response.push_str(&format!("{name}: {value}\r\n"));
        }
    }
    response.push_str("\r\n");
    response.push_str(body);
    let _ = stream.write_all(response.as_bytes()).await;
}

struct CapturedMakeWriter(Arc<Mutex<Vec<u8>>>);

struct CapturedWriter(Arc<Mutex<Vec<u8>>>);

impl<'a> MakeWriter<'a> for CapturedMakeWriter {
    type Writer = CapturedWriter;

    fn make_writer(&'a self) -> Self::Writer {
        CapturedWriter(self.0.clone())
    }
}

impl Write for CapturedWriter {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

// A reply form may submit an empty video file input when no video is selected; ignore that part
// and continue to create the post.
#[tokio::test]
async fn production_post_with_an_empty_video_input_is_accepted() {
    let public = OAuthPublic::new("https://appview.example").unwrap();
    let (flow, _, shared, server) =
        production_flow_with_public(public.clone(), None, Some(EXPECTED_OAUTH_SCOPE)).await;
    let storage = Arc::new(MemoryStorage::default());
    let app = public.router_with_flow(flow, storage);
    let cookie = finish_production_login(&app, &shared).await;
    let boundary = "protocol-empty-video";
    let body = format!(
        "--{boundary}\r\nContent-Disposition: form-data; name=\"text\"\r\n\r\njust text\r\n\
         --{boundary}\r\nContent-Disposition: form-data; name=\"video\"; filename=\"\"\r\nContent-Type: application/octet-stream\r\n\r\n\r\n\
         --{boundary}--\r\n"
    );
    let response = app
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/post")
                .header("origin", "https://appview.example")
                .header("sec-fetch-site", "same-origin")
                .header("cookie", cookie)
                .header(
                    "content-type",
                    format!("multipart/form-data; boundary={boundary}"),
                )
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::SEE_OTHER);
    let state = shared.lock().unwrap();
    assert!(state.requests.iter().any(|request| {
        request
            .path
            .starts_with("/xrpc/com.atproto.repo.createRecord")
    }));
    drop(state);
    server.abort();
}

#[derive(Clone)]
struct MultiDocumentIdentity {
    documents: Vec<AppDidDocument>,
}

impl IdentityBackend for MultiDocumentIdentity {
    async fn did_document(&self, did: &str) -> Result<Option<AppDidDocument>, ResolutionError> {
        Ok(self
            .documents
            .iter()
            .find(|document| document.id == did)
            .cloned())
    }

    async fn dns_txt_did(&self, _: &str) -> Result<Option<String>, ResolutionError> {
        Ok(None)
    }

    async fn well_known_did(&self, _: &str) -> Result<Option<String>, ResolutionError> {
        Ok(None)
    }
}

const FEEDGEN_SERVICE_DID: &str = "did:web:feed.bsky.app";
const FEED_PAGE_URI: &str = "at://did:plc:abcdefghijklmnopqrstuvwx/app.bsky.feed.generator/for-you";

fn encoded_feed_page_uri() -> String {
    url::form_urlencoded::byte_serialize(FEED_PAGE_URI.as_bytes()).collect()
}

fn feed_page_request(cookie: &str, cursor: &str) -> Request<Body> {
    let mut uri = format!("/app/feed?uri={}", encoded_feed_page_uri());
    if !cursor.is_empty() {
        uri.push_str(&format!("&cursor={cursor}"));
    }
    Request::builder()
        .uri(uri)
        .header("cookie", cookie.to_owned())
        .body(Body::empty())
        .unwrap()
}

async fn production_feed_flow(
    public: OAuthPublic,
) -> (
    appview_rs::oauth::AtriumOAuthFlow<MultiDocumentIdentity>,
    SqliteOAuthStore,
    Arc<Mutex<FakeServerState>>,
    tokio::task::JoinHandle<()>,
) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let origin = format!("http://{address}");
    let shared = Arc::new(Mutex::new(FakeServerState {
        token_scope: Some(EXPECTED_OAUTH_SCOPE.to_owned()),
        initial_access_token: Some("access-initial".to_owned()),
        initial_expires_in: Some(3600),
        feed_generator_service_did: Some(FEEDGEN_SERVICE_DID.to_owned()),
        skeleton_required_jwt: Some("for-you-service-jwt".to_owned()),
        service_auth_valid_access_tokens: Some(vec![
            "access-initial".to_owned(),
            "access-second".to_owned(),
            "access-third".to_owned(),
        ]),
        ..FakeServerState::default()
    }));
    let server_state = shared.clone();
    let server_origin = origin.clone();
    let server = tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = listener.accept().await else {
                return;
            };
            let state = server_state.clone();
            let origin = server_origin.clone();
            tokio::spawn(async move {
                serve_one(stream, &origin, state).await;
            });
        }
    });

    let identity = MultiDocumentIdentity {
        documents: vec![
            AppDidDocument {
                id: DID.to_owned(),
                also_known_as: vec![],
                verification_method: vec![],
                service: vec![DidService {
                    id: "#atproto_pds".to_owned(),
                    type_name: "AtprotoPersonalDataServer".to_owned(),
                    service_endpoint: origin.clone(),
                }],
            },
            AppDidDocument {
                id: FEEDGEN_SERVICE_DID.to_owned(),
                also_known_as: vec![],
                verification_method: vec![],
                service: vec![DidService {
                    id: "#bsky_fg".to_owned(),
                    type_name: "BskyFeedGenerator".to_owned(),
                    service_endpoint: origin.clone(),
                }],
            },
        ],
    };
    let resolver = Arc::new(IdentityResolver::new(identity, Duration::from_secs(30)));
    let store = SqliteOAuthStore::open(":memory:").unwrap();
    let flow = public
        .atrium_flow_with_plc_directory(resolver, store.clone(), &format!("{origin}/"))
        .unwrap();
    (flow, store, shared, server)
}

#[tokio::test]
async fn feed_page_after_the_pds_revokes_the_stored_access_token_refreshes_on_first_load() {
    let public = OAuthPublic::new("https://appview.example").unwrap();
    let (flow, _store, shared, server) = production_feed_flow(public.clone()).await;
    let app = public.router_with_flow(flow, Arc::new(MemoryStorage::default()));
    let cookie = finish_production_login(&app, &shared).await;
    assert_eq!(shared.lock().unwrap().token_count, 1);

    // Overnight the PDS started answering invalid_token for the session's stored access token.
    // The local expires_at is still in the future, so only a forced refresh can recover.
    shared.lock().unwrap().service_auth_valid_access_tokens =
        Some(vec!["access-second".to_owned()]);
    shared
        .lock()
        .unwrap()
        .refresh_access_tokens
        .push_back("access-second".to_owned());

    let response = app
        .clone()
        .oneshot(feed_page_request(&cookie, ""))
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::OK,
        "the first feed page load after the PDS started answering invalid_token must refresh and succeed with the new token"
    );

    let state = shared.lock().unwrap();
    assert_eq!(
        state.token_count, 2,
        "the recovery must POST grant_type=refresh_token exactly once"
    );
    let service_auth_requests = state
        .requests
        .iter()
        .filter(|request| {
            request
                .path
                .starts_with("/xrpc/com.atproto.server.getServiceAuth")
        })
        .collect::<Vec<_>>();
    assert!(
        service_auth_requests.iter().any(|request| request
            .headers
            .get("authorization")
            .is_some_and(|value| value.as_str() == "DPoP access-second")),
        "getServiceAuth must be retried with the refreshed access token"
    );
    let skeleton = state
        .requests
        .iter()
        .find(|request| {
            request
                .path
                .starts_with("/xrpc/app.bsky.feed.getFeedSkeleton")
        })
        .expect("the feed skeleton must be requested");
    assert_eq!(
        skeleton.headers.get("authorization").map(String::as_str),
        Some("Bearer for-you-service-jwt"),
        "the auth-required feed must not be requested anonymously"
    );
    drop(state);
    server.abort();
}

#[tokio::test]
async fn session_cache_never_hands_out_a_pre_refresh_access_token() {
    let public = OAuthPublic::new("https://appview.example").unwrap();
    let (flow, _store, shared, server) = production_feed_flow(public.clone()).await;
    let app = public.router_with_flow(flow, Arc::new(MemoryStorage::default()));
    let cookie = finish_production_login(&app, &shared).await;

    shared.lock().unwrap().service_auth_valid_access_tokens =
        Some(vec!["access-second".to_owned()]);
    shared
        .lock()
        .unwrap()
        .refresh_access_tokens
        .push_back("access-second".to_owned());

    let response = app
        .clone()
        .oneshot(feed_page_request(&cookie, ""))
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    // The same page load then calls getMutes through restored_session, which reuses the session
    // cache. After the refresh the cache must hand out the NEW token, never the pre-refresh one.
    let state = shared.lock().unwrap();
    let mute_requests = state
        .requests
        .iter()
        .filter(|request| request.path.starts_with("/xrpc/app.bsky.graph.getMutes"))
        .collect::<Vec<_>>();
    assert!(
        !mute_requests.is_empty(),
        "the feed page must still load the mute list"
    );
    for request in &mute_requests {
        assert_eq!(
            request.headers.get("authorization").map(String::as_str),
            Some("DPoP access-second"),
            "no request after the refresh may replay the revoked access token"
        );
    }
    drop(state);
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn a_failed_like_names_the_pds_error_in_the_response_and_the_logs() {
    let public = OAuthPublic::new("https://appview.example").unwrap();
    let (flow, _store, shared, server) =
        production_flow_with_public(public.clone(), None, Some(EXPECTED_OAUTH_SCOPE)).await;
    let app = public.router_with_flow(flow, Arc::new(MemoryStorage::default()));
    let cookie = finish_production_login(&app, &shared).await;
    shared.lock().unwrap().create_failure = Some((500, "fake PDS refused the like".to_owned()));

    let output = Arc::new(Mutex::new(Vec::new()));
    let capture = tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .with_max_level(tracing::Level::INFO)
        .with_writer(CapturedMakeWriter(output.clone()))
        .finish();
    let _capture_guard = tracing::subscriber::set_default(capture);
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/like")
                .header("origin", "https://appview.example")
                .header("sec-fetch-site", "same-origin")
                .header("cookie", cookie)
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "uri=at%3A%2F%2Fdid%3Aplc%3Aauthor%2Fapp.bsky.feed.post%2Fr1&cid=bafyreifake",
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    drop(_capture_guard);

    assert_eq!(response.status(), StatusCode::BAD_GATEWAY);
    let body: Value =
        serde_json::from_slice(&response.into_body().collect().await.unwrap().to_bytes()).unwrap();
    assert_eq!(body["error"], "OAuthError");
    let message = body["message"].as_str().unwrap_or_default();
    assert!(
        message.starts_with("Bluesky did not accept the like:"),
        "the failed like must name what failed: {message}"
    );
    assert!(
        message.contains("fake PDS refused the like"),
        "the message must carry the real PDS reason: {message}"
    );

    let logs = String::from_utf8(output.lock().unwrap().clone()).unwrap();
    let warn_line = logs
        .lines()
        .find(|line| line.contains("web write failed"))
        .unwrap_or_else(|| panic!("a warn line naming the failure is required: {logs}"));
    assert!(warn_line.contains("WARN"), "{warn_line}");
    assert!(warn_line.contains("route=\"like\""), "{warn_line}");
    assert!(warn_line.contains(DID), "{warn_line}");
    assert!(
        warn_line.contains("fake PDS refused the like"),
        "the warn line must carry the real error text: {warn_line}"
    );

    let write_line = logs
        .lines()
        .find(|line| line.contains("web write ") && !line.contains("web write failed"))
        .unwrap_or_else(|| panic!("the web write timing line is required: {logs}"));
    for field in [
        "route=\"like\"",
        "total_ms=",
        "outbound_call_count=",
        "outbound_call_ms=",
        "dpop_nonce_retries=",
        "dpop_nonce_retry=",
        "outbound_calls=",
    ] {
        assert!(
            write_line.contains(field),
            "{field} must stay on the web write line: {write_line}"
        );
    }
    assert!(
        write_line.contains("fake PDS refused the like"),
        "the web write line must count the failure with its real text: {write_line}"
    );
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn a_refresh_rejected_with_invalid_grant_is_logged_with_the_authorization_servers_reason() {
    let public = OAuthPublic::new("https://appview.example").unwrap();
    let (flow, store, shared, server) =
        production_flow_with_public(public.clone(), None, Some(EXPECTED_OAUTH_SCOPE)).await;
    let app = public.router_with_flow(flow, Arc::new(MemoryStorage::default()));
    let cookie = finish_production_login(&app, &shared).await;
    let issuer = server_origin(&shared);
    // The PDS rejects the stored access token, and the authorization server now refuses the
    // refresh token outright: exactly the 2026-10-31 incident, where this was invisible.
    shared.lock().unwrap().create_required_access_tokens = Some(vec![]);
    shared.lock().unwrap().refresh_failure = Some((400, "invalid_grant".to_owned()));

    let output = Arc::new(Mutex::new(Vec::new()));
    let capture = tracing_subscriber::fmt()
        .without_time()
        .with_ansi(false)
        .with_max_level(tracing::Level::WARN)
        .with_writer(CapturedMakeWriter(output.clone()))
        .finish();
    let _capture_guard = tracing::subscriber::set_default(capture);
    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/like")
                .header("origin", "https://appview.example")
                .header("sec-fetch-site", "same-origin")
                .header("cookie", cookie)
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "uri=at%3A%2F%2Fdid%3Aplc%3Aauthor%2Fapp.bsky.feed.post%2Fr1&cid=bafyreifake",
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    drop(_capture_guard);
    // A rejected refresh kills the session, so the user is sent to sign in again; the warn
    // line below is the point of this test.
    assert_eq!(response.status(), StatusCode::SEE_OTHER);

    let logs = String::from_utf8(output.lock().unwrap().clone()).unwrap();
    let refresh_line = logs
        .lines()
        .find(|line| line.contains("invalid_grant"))
        .unwrap_or_else(|| panic!("a warn line naming the authorization server's reason: {logs}"));
    assert!(refresh_line.contains("WARN"), "{refresh_line}");
    assert!(refresh_line.contains(DID), "{refresh_line}");
    assert!(refresh_line.contains(&issuer), "{refresh_line}");
    assert!(refresh_line.contains("400"), "{refresh_line}");
    assert!(
        refresh_line.contains("refresh=true"),
        "the line must say it was a refresh: {refresh_line}"
    );
    let state = shared.lock().unwrap();
    assert!(
        state
            .requests
            .iter()
            .any(|request| request.path == "/token"
                && request.body.contains("grant_type=refresh_token")),
        "the refresh must actually have been attempted"
    );
    assert!(
        store.oauth_refresh_failure_count() >= 1,
        "every rejected refresh must raise the minute-metric counter"
    );
    server.abort();
}

#[tokio::test(flavor = "current_thread")]
async fn a_dead_session_sends_the_browser_to_sign_in_again_and_says_why() {
    let public = OAuthPublic::new("https://appview.example").unwrap();
    let (flow, store, shared, server) =
        production_flow_with_public(public.clone(), None, Some(EXPECTED_OAUTH_SCOPE)).await;
    let app = public.router_with_flow(flow, Arc::new(MemoryStorage::default()));
    let cookie = finish_production_login(&app, &shared).await;
    // Access token dead at the PDS and the authorization server refuses the refresh token:
    // nothing can revive this session, so the user must be sent back to sign in.
    shared.lock().unwrap().create_required_access_tokens = Some(vec![]);
    shared.lock().unwrap().refresh_failure = Some((400, "invalid_grant".to_owned()));

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/like")
                .header("origin", "https://appview.example")
                .header("sec-fetch-site", "same-origin")
                .header("cookie", cookie.clone())
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "uri=at%3A%2F%2Fdid%3Aplc%3Aauthor%2Fapp.bsky.feed.post%2Fr1&cid=bafyreifake",
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(
        response.status(),
        StatusCode::SEE_OTHER,
        "a dead session must redirect to sign in, not stay signed in"
    );
    assert_eq!(
        response.headers()[axum::http::header::LOCATION],
        "/login?session_expired=1"
    );
    let set_cookie = response
        .headers()
        .get_all(axum::http::header::SET_COOKIE)
        .iter()
        .map(|value| value.to_str().unwrap().to_owned())
        .collect::<Vec<_>>()
        .join("\n");
    assert!(
        set_cookie.contains("appview_session=;") && set_cookie.contains("Max-Age=0"),
        "the dead session cookie must be cleared: {set_cookie}"
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/like")
                .header("origin", "https://appview.example")
                .header("sec-fetch-site", "same-origin")
                .header("cookie", cookie.clone())
                .header("x-appview-fetch", "1")
                .header("content-type", "application/x-www-form-urlencoded")
                .body(Body::from(
                    "uri=at%3A%2F%2Fdid%3Aplc%3Aauthor%2Fapp.bsky.feed.post%2Fr2&cid=bafyreifake",
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
    let body = BodyExt::collect(response.into_body())
        .await
        .unwrap()
        .to_bytes();
    let body: Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(body["error"], "SessionExpired");
    assert!(
        body["message"]
            .as_str()
            .unwrap_or_default()
            .contains("sign in again"),
        "{body}"
    );

    let response = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/login?session_expired=1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let html = String::from_utf8(
        BodyExt::collect(response.into_body())
            .await
            .unwrap()
            .to_bytes()
            .to_vec(),
    )
    .unwrap();
    assert!(
        html.contains("Your Bluesky sign-in expired"),
        "the login page must say why the user is back here"
    );

    let stored = store
        .get(&DID.parse::<Did>().unwrap())
        .await
        .expect("session store readable");
    assert!(
        stored.is_none(),
        "the dead session must be deleted, not left to be retried"
    );
    server.abort();
}
