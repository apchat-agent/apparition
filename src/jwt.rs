use crate::identity::{IdentityBackend, IdentityResolver};
use atrium_crypto::{Algorithm, did::parse_did_key, verify::Verifier};
use base64::{Engine as _, engine::general_purpose::URL_SAFE_NO_PAD};
use serde::Deserialize;
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum ServiceJwtError {
    #[error("malformed JWT")]
    Malformed,
    #[error("unsupported JWT algorithm")]
    Algorithm,
    #[error("JWT signature is invalid")]
    Signature,
    #[error("JWT claims are invalid")]
    Claims,
    #[error("issuer DID document is invalid: {0}")]
    Issuer(String),
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct ServiceClaims {
    pub iss: String,
    #[serde(default)]
    pub sub: Option<String>,
    pub aud: Audience,
    pub exp: u64,
    pub iat: u64,
    pub lxm: String,
}

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
#[serde(untagged)]
pub enum Audience {
    One(String),
    Many(Vec<String>),
}

impl Audience {
    fn contains(&self, expected: &str) -> bool {
        match self {
            Self::One(value) => value == expected,
            Self::Many(values) => values.iter().any(|value| value == expected),
        }
    }
}

#[derive(Deserialize)]
struct JwtHeader {
    alg: String,
}

pub struct ServiceJwtVerifier<B: IdentityBackend> {
    resolver: Arc<IdentityResolver<B>>,
    audience: String,
}

impl<B: IdentityBackend> ServiceJwtVerifier<B> {
    pub fn new(resolver: Arc<IdentityResolver<B>>, audience: impl Into<String>) -> Self {
        Self {
            resolver,
            audience: audience.into(),
        }
    }

    pub async fn verify(
        &self,
        token: &str,
        lexicon: &str,
    ) -> Result<ServiceClaims, ServiceJwtError> {
        if token.len() > 32 * 1024 {
            return Err(ServiceJwtError::Malformed);
        }
        let mut parts = token.split('.');
        let (Some(header_part), Some(payload_part), Some(signature_part), None) =
            (parts.next(), parts.next(), parts.next(), parts.next())
        else {
            return Err(ServiceJwtError::Malformed);
        };
        let header_bytes = URL_SAFE_NO_PAD
            .decode(header_part)
            .map_err(|_| ServiceJwtError::Malformed)?;
        let header: JwtHeader =
            serde_json::from_slice(&header_bytes).map_err(|_| ServiceJwtError::Malformed)?;
        let payload_bytes = URL_SAFE_NO_PAD
            .decode(payload_part)
            .map_err(|_| ServiceJwtError::Malformed)?;
        let claims: ServiceClaims =
            serde_json::from_slice(&payload_bytes).map_err(|_| ServiceJwtError::Malformed)?;
        let signature = URL_SAFE_NO_PAD
            .decode(signature_part)
            .map_err(|_| ServiceJwtError::Malformed)?;
        let service_audience = format!("{}#bsky_appview", self.audience);
        if (!claims.aud.contains(&self.audience) && !claims.aud.contains(&service_audience))
            || claims.lxm != lexicon
            || claims.exp <= unix_time()
            || claims.iat > unix_time().saturating_add(30)
        {
            return Err(ServiceJwtError::Claims);
        }

        let document = self
            .resolver
            .resolve_did_document(&claims.iss)
            .await
            .map_err(|error| ServiceJwtError::Issuer(error.to_string()))?;
        let signing_key = document.atproto_signing_key().ok_or_else(|| {
            ServiceJwtError::Issuer("DID document has no valid #atproto key".into())
        })?;
        let (algorithm, public_key) = parse_did_key(&signing_key)
            .map_err(|error| ServiceJwtError::Issuer(error.to_string()))?;
        let expected_alg = match algorithm {
            Algorithm::P256 => "ES256",
            Algorithm::Secp256k1 => "ES256K",
        };
        if header.alg != expected_alg {
            return Err(ServiceJwtError::Algorithm);
        }
        let signing_input = format!("{header_part}.{payload_part}");
        Verifier::default()
            .verify(algorithm, &public_key, signing_input.as_bytes(), &signature)
            .map_err(|_| ServiceJwtError::Signature)?;
        Ok(claims)
    }
}

fn unix_time() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}
