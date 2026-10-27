use appview_rs::{
    identity::{
        DidDocument as AppDidDocument, DidService, IdentityBackend, IdentityResolver,
        ResolutionError,
    },
    oauth::{OAuthCallback, OAuthFlow, OAuthPublic, SqliteOAuthStore},
};
use atrium_api::{
    agent::Agent,
    did_doc::{DidDocument, Service},
    types::string::{Did, Handle},
};
use atrium_common::{resolver::Resolver, store::memory::MemoryStore};
use atrium_oauth::store::{session::Session, state::InternalStateData};
use atrium_oauth::{
    AtprotoClientMetadata, AuthMethod, AuthorizeOptions, CallbackParams, GrantType, KnownScope,
    OAuthClient, OAuthClientConfig, OAuthResolverConfig, Scope,
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

const DID: &str = "did:plc:abcdefghijklmnopqrstuvwx";

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
    path: String,
    headers: HashMap<String, String>,
    body: String,
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
    let (flow, shared, server) = production_flow(None, None).await;
    flow.authorize(DID).await.unwrap();
    let state = latest_state(&shared);
    let result = flow
        .callback(OAuthCallback {
            code: "valid-code".to_owned(),
            state,
            issuer: Some(server_origin(&shared)),
            scope: None,
        })
        .await;
    assert!(
        result.is_ok(),
        "standard callback should succeed: {result:?}"
    );
    assert_eq!(shared.lock().unwrap().token_count, 1);
    server.abort();

    let (flow, shared, server) = production_flow(Some("did:plc:attacker"), Some("atproto")).await;
    flow.authorize(DID).await.unwrap();
    let state = latest_state(&shared);
    let result = flow
        .callback(OAuthCallback {
            code: "valid-code".to_owned(),
            state,
            issuer: Some(server_origin(&shared)),
            scope: None,
        })
        .await;
    assert!(result.unwrap_err().contains("subject DID"));
    server.abort();

    let (flow, shared, server) = production_flow(Some(DID), Some("profile")).await;
    flow.authorize(DID).await.unwrap();
    let state = latest_state(&shared);
    let result = flow
        .callback(OAuthCallback {
            code: "valid-code".to_owned(),
            state,
            issuer: Some(server_origin(&shared)),
            scope: None,
        })
        .await;
    assert!(result.unwrap_err().contains("atproto scope"));
    server.abort();
}

async fn production_flow(
    token_sub: Option<&str>,
    token_scope: Option<&str>,
) -> (
    appview_rs::oauth::AtriumOAuthFlow<ProductionFlowIdentity>,
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
    let public = OAuthPublic::new("https://appview.example").unwrap();
    let flow = public
        .atrium_flow_with_plc_directory(
            resolver,
            SqliteOAuthStore::open(":memory:").unwrap(),
            &format!("{origin}/"),
        )
        .unwrap();
    (flow, shared, server)
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
            path if path == format!("/{DID}") => (
                200,
                vec![],
                json!({
                    "id":DID,
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
                    "token_endpoint_auth_methods_supported":["private_key_jwt"],
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
                let sub = state.token_sub.as_deref().unwrap_or(DID);
                let scope = state.token_scope.as_deref().unwrap_or("atproto");
                if request.body.contains("grant_type=refresh_token") {
                    (
                        200,
                        vec![],
                        json!({"access_token":"access-fresh","refresh_token":"refresh-fresh","token_type":"DPoP","expires_in":3600,"scope":scope,"sub":sub}).to_string(),
                    )
                } else {
                    (
                        200,
                        vec![],
                        json!({"access_token":"access-expired","refresh_token":"refresh-initial","token_type":"DPoP","expires_in":0,"scope":scope,"sub":sub}).to_string(),
                    )
                }
            }
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
        path,
        headers,
        body: String::from_utf8_lossy(&bytes[header_end..header_end + content_length]).into_owned(),
    })
}

async fn write_response(stream: &mut TcpStream, status: u16, headers: &[(&str, &str)], body: &str) {
    let reason = match status {
        200 => "OK",
        201 => "Created",
        400 => "Bad Request",
        401 => "Unauthorized",
        _ => "Not Found",
    };
    let mut response = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    );
    for (name, value) in headers {
        response.push_str(&format!("{name}: {value}\r\n"));
    }
    response.push_str("\r\n");
    response.push_str(body);
    let _ = stream.write_all(response.as_bytes()).await;
}
