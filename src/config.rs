use crate::ingest::ScopeMode;
use serde::Deserialize;
use std::{env, fs, path::PathBuf};
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
    #[serde(default = "default_appview_did")]
    pub appview_did: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            listen: default_listen(),
            data_dir: default_data_dir(),
            log_filter: default_log_filter(),
            scope_mode: ScopeMode::default(),
            jetstream_urls: default_jetstream_urls(),
            appview_did: default_appview_did(),
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
                "APPVIEW_DID" => config.appview_did = value.to_owned(),
                _ => {}
            }
        }
        Ok(config)
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
                    "APPVIEW_DID" => config.appview_did = value,
                    _ => unreachable!(),
                }
            }
            return Ok(config);
        }
        Self::from_toml_and_env(&source, nonempty)
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

fn default_appview_did() -> String {
    "did:web:localhost".to_owned()
}
