use crate::ingest::ScopeMode;
use serde::Deserialize;
use std::{env, fs, net::SocketAddr, path::PathBuf};
use thiserror::Error;

#[derive(Clone, Debug, Deserialize, PartialEq, Eq)]
pub struct Config {
    #[serde(default = "default_listen")]
    pub listen: String,
    #[serde(default = "default_data_dir")]
    pub data_dir: String,
    #[serde(default = "default_log_filter")]
    pub log_filter: String,
    #[serde(default)]
    pub scope_mode: ScopeMode,
    #[serde(default = "default_jetstream_urls")]
    pub jetstream_urls: Vec<String>,
    #[serde(default = "default_jetstream_idle_timeout_secs")]
    pub jetstream_idle_timeout_secs: u64,
    #[serde(default = "default_jetstream_event_timeout_secs")]
    pub jetstream_event_timeout_secs: u64,
    #[serde(default = "default_jetstream_stale_cursor_threshold_secs")]
    pub jetstream_stale_cursor_threshold_secs: u64,
    #[serde(default = "default_backfill_records_per_second")]
    pub backfill_records_per_second: u64,
    #[serde(default)]
    pub backfill_enabled: bool,
    #[serde(default = "default_appview_did")]
    pub appview_did: String,
    #[serde(default = "default_public_url")]
    pub public_url: String,
    #[serde(default)]
    pub oauth_mode: OAuthMode,
}

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "lowercase")]
pub enum OAuthMode {
    #[default]
    Confidential,
    Loopback,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen: default_listen(),
            data_dir: default_data_dir(),
            log_filter: default_log_filter(),
            scope_mode: ScopeMode::default(),
            jetstream_urls: default_jetstream_urls(),
            jetstream_idle_timeout_secs: default_jetstream_idle_timeout_secs(),
            jetstream_event_timeout_secs: default_jetstream_event_timeout_secs(),
            jetstream_stale_cursor_threshold_secs: default_jetstream_stale_cursor_threshold_secs(),
            backfill_records_per_second: default_backfill_records_per_second(),
            backfill_enabled: false,
            appview_did: default_appview_did(),
            public_url: default_public_url(),
            oauth_mode: OAuthMode::default(),
        }
    }
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("invalid TOML configuration: {0}")]
    Toml(#[from] toml::de::Error),
    #[error("could not read config file {path}: {source}")]
    Read {
        path: PathBuf,
        source: std::io::Error,
    },
    #[error("APPVIEW_SCOPE must be 'scoped' or 'full', got {0}")]
    InvalidScope(String),
    #[error("APPVIEW_OAUTH_MODE must be 'confidential' or 'loopback', got {0}")]
    InvalidOAuthMode(String),
    #[error("{variable} must be an unsigned integer number of seconds, got {value}")]
    InvalidTimeout {
        variable: &'static str,
        value: String,
    },
    #[error("APPVIEW_BACKFILL_RECORDS_PER_SECOND must be a positive integer, got {0}")]
    InvalidBackfillRate(String),
    #[error("APPVIEW_BACKFILL_ENABLED must be 'true' or 'false', got {0}")]
    InvalidBackfillEnabled(String),
    #[error("loopback OAuth requires APPVIEW_LISTEN to be a loopback socket address, got {0}")]
    UnsafeLoopbackListen(String),
}

impl Config {
    pub fn from_toml_and_env<I, K, V>(toml_source: &str, values: I) -> Result<Self, ConfigError>
    where
        I: IntoIterator<Item = (K, V)>,
        K: AsRef<str>,
        V: AsRef<str>,
    {
        let mut config: Self = toml::from_str(toml_source)?;
        for (key, value) in values {
            let value = value.as_ref();
            match key.as_ref() {
                "APPVIEW_LISTEN" => config.listen = value.to_owned(),
                "APPVIEW_DATA_DIR" => config.data_dir = value.to_owned(),
                "APPVIEW_LOG_FILTER" => config.log_filter = value.to_owned(),
                "APPVIEW_SCOPE" => {
                    config.scope_mode = match value {
                        "scoped" => ScopeMode::Scoped,
                        "full" => ScopeMode::Full,
                        _ => return Err(ConfigError::InvalidScope(value.to_owned())),
                    }
                }
                "APPVIEW_JETSTREAM_URLS" => {
                    config.jetstream_urls = value
                        .split(',')
                        .map(str::trim)
                        .filter(|url| !url.is_empty())
                        .map(str::to_owned)
                        .collect();
                }
                "APPVIEW_JETSTREAM_IDLE_TIMEOUT_SECS" => {
                    config.jetstream_idle_timeout_secs =
                        parse_timeout("APPVIEW_JETSTREAM_IDLE_TIMEOUT_SECS", value)?;
                }
                "APPVIEW_JETSTREAM_EVENT_TIMEOUT_SECS" => {
                    config.jetstream_event_timeout_secs =
                        parse_timeout("APPVIEW_JETSTREAM_EVENT_TIMEOUT_SECS", value)?;
                }
                "APPVIEW_JETSTREAM_STALE_CURSOR_THRESHOLD_SECS" => {
                    config.jetstream_stale_cursor_threshold_secs =
                        parse_timeout("APPVIEW_JETSTREAM_STALE_CURSOR_THRESHOLD_SECS", value)?;
                }
                "APPVIEW_BACKFILL_RECORDS_PER_SECOND" => {
                    config.backfill_records_per_second = parse_backfill_rate(value)?;
                }
                "APPVIEW_BACKFILL_ENABLED" => config.backfill_enabled = parse_bool(value)?,
                "APPVIEW_DID" => config.appview_did = value.to_owned(),
                "APPVIEW_PUBLIC_URL" => config.public_url = value.to_owned(),
                "APPVIEW_OAUTH_MODE" => {
                    config.oauth_mode = parse_oauth_mode(value)?;
                }
                _ => {}
            }
        }
        config.validate()?;
        Ok(config)
    }

    pub fn validate(&self) -> Result<(), ConfigError> {
        if self.oauth_mode == OAuthMode::Loopback
            && !self
                .listen
                .parse::<SocketAddr>()
                .is_ok_and(|address| address.ip().is_loopback())
        {
            return Err(ConfigError::UnsafeLoopbackListen(self.listen.clone()));
        }
        Ok(())
    }

    pub fn load() -> Result<Self, ConfigError> {
        let path = env::var_os("APPVIEW_CONFIG").map(PathBuf::from);
        let source = match path {
            Some(path) => {
                fs::read_to_string(&path).map_err(|source| ConfigError::Read { path, source })?
            }
            None => String::new(),
        };
        let env_values = [
            (
                "APPVIEW_LISTEN",
                env::var("APPVIEW_LISTEN").unwrap_or_default(),
            ),
            (
                "APPVIEW_DATA_DIR",
                env::var("APPVIEW_DATA_DIR").unwrap_or_default(),
            ),
            (
                "APPVIEW_LOG_FILTER",
                env::var("APPVIEW_LOG_FILTER").unwrap_or_default(),
            ),
            (
                "APPVIEW_SCOPE",
                env::var("APPVIEW_SCOPE").unwrap_or_default(),
            ),
            (
                "APPVIEW_JETSTREAM_URLS",
                env::var("APPVIEW_JETSTREAM_URLS").unwrap_or_default(),
            ),
            (
                "APPVIEW_JETSTREAM_IDLE_TIMEOUT_SECS",
                env::var("APPVIEW_JETSTREAM_IDLE_TIMEOUT_SECS").unwrap_or_default(),
            ),
            (
                "APPVIEW_JETSTREAM_EVENT_TIMEOUT_SECS",
                env::var("APPVIEW_JETSTREAM_EVENT_TIMEOUT_SECS").unwrap_or_default(),
            ),
            (
                "APPVIEW_BACKFILL_RECORDS_PER_SECOND",
                env::var("APPVIEW_BACKFILL_RECORDS_PER_SECOND").unwrap_or_default(),
            ),
            (
                "APPVIEW_BACKFILL_ENABLED",
                env::var("APPVIEW_BACKFILL_ENABLED").unwrap_or_default(),
            ),
            ("APPVIEW_DID", env::var("APPVIEW_DID").unwrap_or_default()),
            (
                "APPVIEW_PUBLIC_URL",
                env::var("APPVIEW_PUBLIC_URL").unwrap_or_default(),
            ),
            (
                "APPVIEW_OAUTH_MODE",
                env::var("APPVIEW_OAUTH_MODE").unwrap_or_default(),
            ),
        ];
        let nonempty = env_values
            .into_iter()
            .filter(|(_, value)| !value.is_empty());
        if source.is_empty() && env::var_os("APPVIEW_CONFIG").is_none() {
            let mut config = Self::default();
            for (key, value) in nonempty {
                match key {
                    "APPVIEW_LISTEN" => config.listen = value,
                    "APPVIEW_DATA_DIR" => config.data_dir = value,
                    "APPVIEW_LOG_FILTER" => config.log_filter = value,
                    "APPVIEW_SCOPE" => {
                        config.scope_mode = match value.as_str() {
                            "scoped" => ScopeMode::Scoped,
                            "full" => ScopeMode::Full,
                            _ => return Err(ConfigError::InvalidScope(value)),
                        }
                    }
                    "APPVIEW_JETSTREAM_URLS" => {
                        config.jetstream_urls = value
                            .split(',')
                            .map(str::trim)
                            .filter(|url| !url.is_empty())
                            .map(str::to_owned)
                            .collect();
                    }
                    "APPVIEW_JETSTREAM_IDLE_TIMEOUT_SECS" => {
                        config.jetstream_idle_timeout_secs = parse_timeout(key, &value)?;
                    }
                    "APPVIEW_JETSTREAM_EVENT_TIMEOUT_SECS" => {
                        config.jetstream_event_timeout_secs = parse_timeout(key, &value)?;
                    }
                    "APPVIEW_BACKFILL_RECORDS_PER_SECOND" => {
                        config.backfill_records_per_second = parse_backfill_rate(&value)?;
                    }
                    "APPVIEW_BACKFILL_ENABLED" => config.backfill_enabled = parse_bool(&value)?,
                    "APPVIEW_DID" => config.appview_did = value,
                    "APPVIEW_PUBLIC_URL" => config.public_url = value,
                    "APPVIEW_OAUTH_MODE" => config.oauth_mode = parse_oauth_mode(&value)?,
                    _ => unreachable!(),
                }
            }
            config.validate()?;
            return Ok(config);
        }
        Self::from_toml_and_env(&source, nonempty)
    }
}

fn parse_oauth_mode(value: &str) -> Result<OAuthMode, ConfigError> {
    match value {
        "confidential" => Ok(OAuthMode::Confidential),
        "loopback" => Ok(OAuthMode::Loopback),
        _ => Err(ConfigError::InvalidOAuthMode(value.to_owned())),
    }
}

fn parse_backfill_rate(value: &str) -> Result<u64, ConfigError> {
    value
        .parse::<u64>()
        .ok()
        .filter(|rate| *rate > 0)
        .ok_or_else(|| ConfigError::InvalidBackfillRate(value.to_owned()))
}

fn parse_bool(value: &str) -> Result<bool, ConfigError> {
    match value {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(ConfigError::InvalidBackfillEnabled(value.to_owned())),
    }
}

fn default_listen() -> String {
    "127.0.0.1:3000".to_owned()
}

fn default_data_dir() -> String {
    "./data".to_owned()
}

fn default_log_filter() -> String {
    "info".to_owned()
}

fn default_jetstream_urls() -> Vec<String> {
    vec!["wss://jetstream1.us-east.bsky.network/subscribe".to_owned()]
}

fn default_jetstream_idle_timeout_secs() -> u64 {
    60
}

fn default_jetstream_event_timeout_secs() -> u64 {
    60
}

fn default_jetstream_stale_cursor_threshold_secs() -> u64 {
    10 * 60
}

fn default_backfill_records_per_second() -> u64 {
    300
}

fn parse_timeout(variable: &'static str, value: &str) -> Result<u64, ConfigError> {
    value.parse().map_err(|_| ConfigError::InvalidTimeout {
        variable,
        value: value.to_owned(),
    })
}

fn default_appview_did() -> String {
    "did:web:localhost".to_owned()
}

fn default_public_url() -> String {
    "https://appview.example".to_owned()
}

/// Paths of the record database and the OAuth session database inside `data_dir`.
///
/// Sign-in state lives in its own file: repository backfill holds long write transactions on
/// the record database, and a shared file made sign-in fail with "database is locked"
/// (2026-10-28, while a follow backfill was running).
pub fn database_paths(data_dir: &str) -> (PathBuf, PathBuf) {
    let dir = PathBuf::from(data_dir);
    (dir.join("appview.db"), dir.join("oauth.db"))
}

/// Path of the persistent identity cache, isolated from the high-write records database.
pub fn identity_cache_path(data_dir: &str) -> PathBuf {
    PathBuf::from(data_dir).join("identity-cache.db")
}
