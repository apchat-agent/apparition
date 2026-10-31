use crate::{
    feeds::{FetchedPublicPost, parse_post_uri, parse_public_profile_view},
    ingest::IndexedRecord,
    net::SafeHttpClient,
    profile_counts::LABELLED_APPVIEW_BASE,
};
use http::HeaderMap;
use serde_json::Value;
use std::time::Duration;
use url::Url;

const MAX_SEARCH_RESPONSE_BYTES: usize = 2 * 1024 * 1024;
const SEARCH_REQUEST_TIMEOUT: Duration = Duration::from_secs(3);
const SEARCH_RESULT_LIMIT: usize = 25;
const MAX_LOG_BODY_EXCERPT_CHARS: usize = 200;

/// A search upstream failure carrying what the page and the logs need: the real status
/// code (when there was one) and a body/transport excerpt, never the raw JSON itself.
#[derive(Debug)]
pub struct SearchCallError {
    pub status: Option<u16>,
    pub detail: String,
}

#[derive(Debug, Default)]
pub struct SearchPostsPage {
    pub posts: Vec<FetchedPublicPost>,
}

#[derive(Debug)]
pub struct SearchActor {
    pub did: String,
    pub handle: Option<String>,
    pub display_name: Option<String>,
    pub avatar: Option<String>,
    pub description: Option<String>,
}

#[derive(Debug, Default)]
pub struct SearchActorsPage {
    pub actors: Vec<SearchActor>,
}

fn optional_str(value: &Value, key: &str) -> Option<String> {
    value
        .get(key)
        .and_then(Value::as_str)
        .filter(|text| !text.is_empty())
        .map(str::to_owned)
}

fn unix_time_us() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_micros()
        .min(u64::MAX as u128) as u64
}

#[derive(Clone)]
pub struct SearchClient {
    http: SafeHttpClient,
    base: String,
}

impl SearchClient {
    /// Production always uses [`LABELLED_APPVIEW_BASE`]: measured 2026-10-31, api.bsky.app
    /// answers `app.bsky.feed.searchPosts` and `app.bsky.actor.searchActors` WITHOUT auth
    /// (200), while public.api.bsky.app answers them with 403. Do NOT point these calls at
    /// [`crate::profile_counts::PUBLIC_APPVIEW_BASE`] — it rejects anonymous search.
    pub fn production() -> Self {
        Self::new(SafeHttpClient::new(), LABELLED_APPVIEW_BASE)
    }

    pub fn new(http: SafeHttpClient, base: impl Into<String>) -> Self {
        Self {
            http,
            base: base.into(),
        }
    }

    pub async fn search_posts(
        &self,
        query: &str,
        cursor: Option<&str>,
    ) -> Result<SearchPostsPage, SearchCallError> {
        let body = self
            .fetch("app.bsky.feed.searchPosts", query, cursor)
            .await?;
        let mut page = SearchPostsPage::default();
        let Some(posts) = body.get("posts").and_then(Value::as_array) else {
            return Ok(page);
        };
        for item in posts {
            let Some(view) = item.get("post") else {
                continue;
            };
            let Some(uri) = view.get("uri").and_then(Value::as_str) else {
                continue;
            };
            let Ok((did, collection, rkey)) = parse_post_uri(uri) else {
                continue;
            };
            let Some(record) = view
                .get("record")
                .filter(|record| record.is_object())
                .cloned()
            else {
                continue;
            };
            let author = view
                .get("author")
                .and_then(parse_public_profile_view)
                .filter(|author| author.did == did);
            page.posts.push(FetchedPublicPost {
                record: IndexedRecord {
                    did,
                    collection,
                    rkey,
                    uri: uri.to_owned(),
                    cid: view.get("cid").and_then(Value::as_str).map(str::to_owned),
                    time_us: unix_time_us(),
                    record,
                },
                author,
            });
        }
        Ok(page)
    }

    pub async fn search_actors(
        &self,
        query: &str,
        cursor: Option<&str>,
    ) -> Result<SearchActorsPage, SearchCallError> {
        let body = self
            .fetch("app.bsky.actor.searchActors", query, cursor)
            .await?;
        let mut page = SearchActorsPage::default();
        let Some(actors) = body.get("actors").and_then(Value::as_array) else {
            return Ok(page);
        };
        for actor in actors {
            let Some(did) = actor.get("did").and_then(Value::as_str).map(str::to_owned) else {
                continue;
            };
            if !did.starts_with("did:") {
                continue;
            }
            page.actors.push(SearchActor {
                handle: optional_str(actor, "handle"),
                display_name: optional_str(actor, "displayName"),
                avatar: optional_str(actor, "avatar"),
                description: optional_str(actor, "description"),
                did,
            });
        }
        Ok(page)
    }

    async fn fetch(
        &self,
        method: &str,
        query: &str,
        cursor: Option<&str>,
    ) -> Result<Value, SearchCallError> {
        let mut url =
            Url::parse(&format!("{}{method}", self.base)).map_err(|error| SearchCallError {
                status: None,
                detail: error.to_string(),
            })?;
        {
            let mut pairs = url.query_pairs_mut();
            pairs.append_pair("q", query);
            pairs.append_pair("limit", &SEARCH_RESULT_LIMIT.to_string());
            if let Some(cursor) = cursor {
                pairs.append_pair("cursor", cursor);
            }
        }
        // SafeHttpClient's own timeout starts after an outbound permit is acquired, so keep
        // an outer deadline around permit acquisition and DNS too (same shape as the
        // profile-counts client).
        let transport = tokio::time::timeout(
            SEARCH_REQUEST_TIMEOUT,
            self.http.get_with_limits(
                &url,
                HeaderMap::new(),
                MAX_SEARCH_RESPONSE_BYTES,
                0,
                SEARCH_REQUEST_TIMEOUT,
            ),
        )
        .await
        .map_err(|_| "search request timed out".to_owned())
        .and_then(|result| result.map(|(response, _)| response));
        let response = match transport {
            Ok(response) => response,
            Err(error) => {
                tracing::warn!(method, url = %url, %error, "search request failed");
                return Err(SearchCallError {
                    status: None,
                    detail: error,
                });
            }
        };
        if !response.status().is_success() {
            let status = response.status().as_u16();
            let excerpt: String = String::from_utf8_lossy(response.body())
                .chars()
                .take(MAX_LOG_BODY_EXCERPT_CHARS)
                .collect();
            tracing::warn!(
                method,
                status,
                url = %url,
                body = %excerpt,
                "search request failed"
            );
            return Err(SearchCallError {
                status: Some(status),
                detail: excerpt,
            });
        }
        serde_json::from_slice(response.body()).map_err(|error| {
            tracing::warn!(method, url = %url, %error, "search request returned unreadable JSON");
            SearchCallError {
                status: None,
                detail: "search response was not valid JSON".to_owned(),
            }
        })
    }
}
