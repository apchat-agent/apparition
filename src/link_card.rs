use crate::net::SafeHttpClient;
use axum::http::{HeaderMap, header};
use serde::Serialize;
use std::{collections::HashMap, time::Duration};
use url::Url;

const MAX_PAGE_BYTES: usize = 1024 * 1024;
const MAX_IMAGE_BYTES: usize = 1024 * 1024;
const MAX_REDIRECTS: usize = 3;
const FETCH_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct LinkCardFetcher {
    client: SafeHttpClient,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LinkCard {
    pub uri: String,
    pub title: String,
    pub description: String,
    pub thumbnail: Option<LinkThumbnail>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LinkThumbnail {
    pub bytes: Vec<u8>,
    pub mime_type: String,
}

#[derive(Serialize)]
pub struct LinkCardPreview {
    pub uri: String,
    pub title: String,
    pub description: String,
    pub thumbnail: Option<String>,
}

impl LinkCardFetcher {
    pub fn new() -> Self {
        Self {
            client: SafeHttpClient::new().allow_http_schemes(),
        }
    }

    pub fn with_client(client: SafeHttpClient) -> Self {
        Self { client }
    }

    pub async fn fetch(&self, input: &str) -> Result<LinkCard, String> {
        tokio::time::timeout(FETCH_TIMEOUT, self.fetch_inner(input))
            .await
            .map_err(|_| "webpage preview fetch timed out".to_owned())?
    }

    async fn fetch_inner(&self, input: &str) -> Result<LinkCard, String> {
        let uri = input.trim();
        if uri.is_empty() || uri.chars().count() > 2048 {
            return Err("Enter a webpage URL no longer than 2,048 characters.".to_owned());
        }
        let mut page_url = Url::parse(uri)
            .map_err(|_| "Enter a complete http or https webpage URL.".to_owned())?;
        if !matches!(page_url.scheme(), "http" | "https")
            || !page_url.username().is_empty()
            || page_url.password().is_some()
        {
            return Err("Only public http and https webpage URLs are supported.".to_owned());
        }
        page_url.set_fragment(None);

        let (response, final_url) = self
            .client
            .get_with_limits(
                &page_url,
                HeaderMap::new(),
                MAX_PAGE_BYTES,
                MAX_REDIRECTS,
                FETCH_TIMEOUT,
            )
            .await?;
        if !response.status().is_success() {
            return Err("The webpage could not be loaded.".to_owned());
        }
        if !is_content_type(&response, "text/html") {
            return Err("The URL must return a text/html webpage.".to_owned());
        }
        let html = String::from_utf8_lossy(response.body());
        let metadata = parse_open_graph(&html, &final_url);
        let thumbnail = match metadata.image_url {
            Some(image_url) => Some(self.fetch_thumbnail(image_url).await?),
            None => None,
        };
        Ok(LinkCard {
            uri: uri.to_owned(),
            title: limit_chars(&metadata.title, 1000),
            description: limit_chars(&metadata.description, 3000),
            thumbnail,
        })
    }

    async fn fetch_thumbnail(&self, image_url: Url) -> Result<LinkThumbnail, String> {
        let (response, _) = self
            .client
            .get_with_limits(
                &image_url,
                HeaderMap::new(),
                MAX_IMAGE_BYTES,
                MAX_REDIRECTS,
                FETCH_TIMEOUT,
            )
            .await?;
        if !response.status().is_success() {
            return Err("The webpage thumbnail could not be loaded.".to_owned());
        }
        let mime_type = response
            .headers()
            .get(header::CONTENT_TYPE)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.split(';').next())
            .map(str::trim)
            .filter(|value| value.to_ascii_lowercase().starts_with("image/"))
            .ok_or_else(|| "The webpage thumbnail must have an image/* content type.".to_owned())?
            .to_ascii_lowercase();
        Ok(LinkThumbnail {
            bytes: response.into_body(),
            mime_type,
        })
    }
}

impl Default for LinkCardFetcher {
    fn default() -> Self {
        Self::new()
    }
}

impl LinkCard {
    pub fn preview(&self) -> LinkCardPreview {
        use base64::Engine as _;
        let thumbnail = self.thumbnail.as_ref().map(|image| {
            format!(
                "data:{};base64,{}",
                image.mime_type,
                base64::engine::general_purpose::STANDARD.encode(&image.bytes)
            )
        });
        LinkCardPreview {
            uri: self.uri.clone(),
            title: self.title.clone(),
            description: self.description.clone(),
            thumbnail,
        }
    }
}

struct OpenGraphMetadata {
    title: String,
    description: String,
    image_url: Option<Url>,
}

fn parse_open_graph(html: &str, base_url: &Url) -> OpenGraphMetadata {
    let mut og_title = None;
    let mut title_fallback = None;
    let mut og_description = None;
    let mut description_fallback = None;
    let mut image = None;
    let lowercase = html.to_ascii_lowercase();

    for tag in find_opening_tags(html, &lowercase, "meta") {
        let attributes = parse_attributes(tag);
        let key = attributes
            .get("property")
            .or_else(|| attributes.get("name"))
            .map(|value| value.to_ascii_lowercase());
        let Some(key) = key else { continue };
        let Some(content) = attributes.get("content") else {
            continue;
        };
        let content = decode_html_entities(content).trim().to_owned();
        match key.as_str() {
            "og:title" if !content.is_empty() => og_title = Some(content),
            "twitter:title" if title_fallback.is_none() && !content.is_empty() => {
                title_fallback = Some(content)
            }
            "og:description" if !content.is_empty() => og_description = Some(content),
            "description" | "twitter:description"
                if description_fallback.is_none() && !content.is_empty() =>
            {
                description_fallback = Some(content)
            }
            "og:image" | "og:image:url" | "twitter:image" if image.is_none() => {
                if let Ok(mut image_url) = base_url.join(&content)
                    && matches!(image_url.scheme(), "http" | "https")
                    && image_url.username().is_empty()
                    && image_url.password().is_none()
                {
                    image_url.set_fragment(None);
                    image = Some(image_url);
                }
            }
            _ => {}
        }
    }

    let title_tag = find_title(html, &lowercase);
    let title = og_title
        .or(title_fallback)
        .or(title_tag)
        .unwrap_or_else(|| base_url.host_str().unwrap_or("Web page").to_owned());
    let description = og_description.or(description_fallback).unwrap_or_default();
    OpenGraphMetadata {
        title,
        description,
        image_url: image,
    }
}

fn find_opening_tags<'a>(html: &'a str, lowercase: &str, name: &str) -> Vec<&'a str> {
    let needle = format!("<{name}");
    let mut tags = Vec::new();
    let mut offset = 0;
    while let Some(found) = lowercase[offset..].find(&needle) {
        let start = offset + found + needle.len();
        let boundary = lowercase.as_bytes().get(start).copied();
        if boundary.is_some_and(|byte| byte.is_ascii_whitespace() || byte == b'>' || byte == b'/')
            && let Some(end) = find_tag_end(lowercase, start)
        {
            tags.push(&html[start..end]);
            offset = end + 1;
        } else {
            offset = start;
        }
    }
    tags
}

fn find_tag_end(source: &str, start: usize) -> Option<usize> {
    let mut quote = None;
    for (relative, byte) in source.as_bytes()[start..].iter().copied().enumerate() {
        match (quote, byte) {
            (Some(active), value) if active == value => quote = None,
            (None, b'\'' | b'"') => quote = Some(byte),
            (None, b'>') => return Some(start + relative),
            _ => {}
        }
    }
    None
}

fn find_title(html: &str, lowercase: &str) -> Option<String> {
    let start = lowercase.find("<title")?;
    let content_start = start + lowercase[start..].find('>')? + 1;
    let content_end = content_start + lowercase[content_start..].find("</title")?;
    let title = decode_html_entities(&html[content_start..content_end]);
    let title = title.split_whitespace().collect::<Vec<_>>().join(" ");
    (!title.is_empty()).then_some(title)
}

fn parse_attributes(tag: &str) -> HashMap<String, String> {
    let bytes = tag.as_bytes();
    let mut attributes = HashMap::new();
    let mut offset = 0;
    while offset < bytes.len() {
        while bytes.get(offset).is_some_and(u8::is_ascii_whitespace)
            || bytes.get(offset) == Some(&b'/')
        {
            offset += 1;
        }
        let name_start = offset;
        while bytes
            .get(offset)
            .is_some_and(|byte| !byte.is_ascii_whitespace() && !matches!(*byte, b'=' | b'/' | b'>'))
        {
            offset += 1;
        }
        if offset == name_start {
            offset += 1;
            continue;
        }
        let name = tag[name_start..offset].to_ascii_lowercase();
        while bytes.get(offset).is_some_and(u8::is_ascii_whitespace) {
            offset += 1;
        }
        if bytes.get(offset) != Some(&b'=') {
            continue;
        }
        offset += 1;
        while bytes.get(offset).is_some_and(u8::is_ascii_whitespace) {
            offset += 1;
        }
        let (value_start, value_end) = match bytes.get(offset).copied() {
            Some(quote @ (b'\'' | b'"')) => {
                offset += 1;
                let value_start = offset;
                while bytes.get(offset).is_some_and(|byte| *byte != quote) {
                    offset += 1;
                }
                let value_end = offset;
                offset += usize::from(bytes.get(offset) == Some(&quote));
                (value_start, value_end)
            }
            Some(_) => {
                let value_start = offset;
                while bytes
                    .get(offset)
                    .is_some_and(|byte| !byte.is_ascii_whitespace() && *byte != b'>')
                {
                    offset += 1;
                }
                (value_start, offset)
            }
            None => break,
        };
        attributes.insert(name, tag[value_start..value_end].to_owned());
    }
    attributes
}

fn decode_html_entities(value: &str) -> String {
    let mut output = String::with_capacity(value.len());
    let mut remaining = value;
    while let Some(ampersand) = remaining.find('&') {
        output.push_str(&remaining[..ampersand]);
        let entity_start = ampersand + 1;
        let Some(semicolon) = remaining[entity_start..].find(';') else {
            output.push_str(&remaining[ampersand..]);
            return output;
        };
        let entity_end = entity_start + semicolon;
        let entity = &remaining[entity_start..entity_end];
        let decoded = match entity {
            "amp" => Some('&'),
            "quot" => Some('"'),
            "apos" => Some('\''),
            "lt" => Some('<'),
            "gt" => Some('>'),
            "nbsp" => Some('\u{00a0}'),
            "copy" => Some('©'),
            "reg" => Some('®'),
            "trade" => Some('™'),
            "ndash" => Some('–'),
            "mdash" => Some('—'),
            "hellip" => Some('…'),
            "lsquo" => Some('‘'),
            "rsquo" => Some('’'),
            "ldquo" => Some('“'),
            "rdquo" => Some('”'),
            "euro" => Some('€'),
            "pound" => Some('£'),
            "yen" => Some('¥'),
            "cent" => Some('¢'),
            numeric if numeric.starts_with("#x") || numeric.starts_with("#X") => {
                u32::from_str_radix(&numeric[2..], 16)
                    .ok()
                    .and_then(char::from_u32)
            }
            numeric if numeric.starts_with('#') => {
                numeric[1..].parse::<u32>().ok().and_then(char::from_u32)
            }
            _ => None,
        };
        if let Some(decoded) = decoded {
            output.push(decoded);
        } else {
            output.push_str(&remaining[ampersand..=entity_end]);
        }
        remaining = &remaining[entity_end + 1..];
    }
    output.push_str(remaining);
    output
}

fn is_content_type(response: &http::Response<Vec<u8>>, expected: &str) -> bool {
    response
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.split(';').next())
        .is_some_and(|value| value.trim().eq_ignore_ascii_case(expected))
}

fn limit_chars(value: &str, max: usize) -> String {
    value.chars().take(max).collect()
}

#[cfg(test)]
mod tests {
    use super::{LinkCardFetcher, parse_open_graph};
    use crate::net::{HostResolver, SafeHttpClient};
    use axum::{
        Router,
        extract::State,
        http::header,
        response::{IntoResponse, Redirect, Response},
        routing::get,
    };
    use std::{
        future::Future,
        net::{IpAddr, Ipv4Addr},
        pin::Pin,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };
    use tokio::net::TcpListener;
    use url::Url;

    struct LocalResolver;

    impl HostResolver for LocalResolver {
        fn resolve<'a>(
            &'a self,
            _: &'a str,
            _: u16,
        ) -> Pin<Box<dyn Future<Output = Result<Vec<IpAddr>, String>> + Send + 'a>> {
            Box::pin(async { Ok(vec![IpAddr::V4(Ipv4Addr::LOCALHOST)]) })
        }
    }

    fn local_fetcher() -> LinkCardFetcher {
        LinkCardFetcher::with_client(
            SafeHttpClient::with_resolver(LocalResolver)
                .allow_http_schemes()
                .with_http_test_hosts(["site.test".to_owned()]),
        )
    }

    async fn test_server() -> (u16, tokio::task::JoinHandle<()>, Arc<AtomicUsize>) {
        async fn page() -> impl IntoResponse {
            (
                [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
                "<meta property='og:title' content='Relative image'><meta property='og:image' content='../thumb.png'>",
            )
        }
        async fn bad_thumbnail_page() -> impl IntoResponse {
            (
                [(header::CONTENT_TYPE, "text/html")],
                "<meta property='og:image' content='/text.png'>",
            )
        }
        async fn large_thumbnail_page() -> impl IntoResponse {
            (
                [(header::CONTENT_TYPE, "text/html")],
                "<meta property='og:image' content='/large.png'>",
            )
        }
        async fn missing_thumbnail_page() -> impl IntoResponse {
            (
                [(header::CONTENT_TYPE, "text/html")],
                "<meta property='og:title' content='Card without image'><meta property='og:image' content='/missing.png'>",
            )
        }
        async fn missing_image() -> impl IntoResponse {
            axum::http::StatusCode::NOT_FOUND
        }
        async fn image() -> impl IntoResponse {
            ([(header::CONTENT_TYPE, "image/png")], vec![1, 2, 3])
        }
        async fn text_image() -> impl IntoResponse {
            ([(header::CONTENT_TYPE, "text/plain")], "not an image")
        }
        async fn large_image() -> impl IntoResponse {
            let body = vec![b'x'; 1024 * 1024 + 1];
            ([(header::CONTENT_TYPE, "image/png")], body)
        }
        async fn wrong_type() -> impl IntoResponse {
            ([(header::CONTENT_TYPE, "application/json")], "{}")
        }
        async fn oversized() -> Response {
            let body = vec![b'x'; 1024 * 1024 + 1];
            ([(header::CONTENT_TYPE, "text/html")], body).into_response()
        }
        async fn redirect_1() -> Redirect {
            Redirect::temporary("/redirect-2")
        }
        async fn redirect_2() -> Redirect {
            Redirect::temporary("/redirect-3")
        }
        async fn redirect_3() -> Redirect {
            Redirect::temporary("/redirect-4")
        }
        async fn redirect_4() -> Redirect {
            Redirect::temporary("/dir/page")
        }
        async fn private(State(requests): State<Arc<AtomicUsize>>) -> &'static str {
            requests.fetch_add(1, Ordering::Relaxed);
            "must not be reached"
        }

        let private_requests = Arc::new(AtomicUsize::new(0));
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let app = Router::new()
            .route("/dir/page", get(page))
            .route("/bad-thumbnail-page", get(bad_thumbnail_page))
            .route("/large-thumbnail-page", get(large_thumbnail_page))
            .route("/missing-thumbnail-page", get(missing_thumbnail_page))
            .route("/missing.png", get(missing_image))
            .route("/thumb.png", get(image))
            .route("/text.png", get(text_image))
            .route("/large.png", get(large_image))
            .route("/wrong", get(wrong_type))
            .route("/large", get(oversized))
            .route(
                "/redirect",
                get(move || async move {
                    Redirect::temporary(&format!("http://private.test:{port}/private"))
                }),
            )
            .route("/redirect-1", get(redirect_1))
            .route("/redirect-2", get(redirect_2))
            .route("/redirect-3", get(redirect_3))
            .route("/redirect-4", get(redirect_4))
            .route("/private", get(private));
        let app = app.with_state(private_requests.clone());
        let server = tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        (port, server, private_requests)
    }

    #[test]
    fn parses_open_graph_fallbacks_relative_images_and_entities() {
        let base = Url::parse("https://example.com/articles/story").unwrap();
        let parsed = parse_open_graph(
            "<html><head><title>Fallback title</title><meta property='og:title' content='Cats &amp; dogs &#x1F431;'><meta name=description content='A &quot;short&quot; story'><meta property=og:image content='../images/cat.png'></head></html>",
            &base,
        );
        assert_eq!(parsed.title, "Cats & dogs 🐱");
        assert_eq!(parsed.description, "A \"short\" story");
        assert_eq!(
            parsed.image_url.unwrap().as_str(),
            "https://example.com/images/cat.png"
        );

        let missing = parse_open_graph("<title>Fallback &amp; title</title>", &base);
        assert_eq!(missing.title, "Fallback & title");
        assert_eq!(missing.description, "");
        assert!(missing.image_url.is_none());
    }

    #[tokio::test]
    async fn fetches_relative_og_image_and_refuses_bad_types_oversize_and_private_redirects() {
        let (port, server, private_requests) = test_server().await;
        let fetcher = local_fetcher();
        let base = format!("http://site.test:{port}");
        let card = fetcher.fetch(&format!("{base}/dir/page")).await.unwrap();
        assert_eq!(card.title, "Relative image");
        assert_eq!(card.thumbnail.unwrap().bytes, [1, 2, 3]);
        assert!(fetcher.fetch(&format!("{base}/wrong")).await.is_err());
        assert!(fetcher.fetch(&format!("{base}/large")).await.is_err());
        assert!(
            fetcher
                .fetch(&format!("{base}/bad-thumbnail-page"))
                .await
                .is_err()
        );
        assert!(
            fetcher
                .fetch(&format!("{base}/large-thumbnail-page"))
                .await
                .is_err()
        );
        assert!(fetcher.fetch(&format!("{base}/redirect")).await.is_err());
        assert_eq!(
            private_requests.load(Ordering::Relaxed),
            0,
            "a redirect to a private address must be rejected before connecting"
        );
        assert!(fetcher.fetch(&format!("{base}/redirect-1")).await.is_err());
        server.abort();
    }

    #[tokio::test]
    async fn thumbnail_http_404_keeps_link_card_without_thumbnail() {
        let (port, server, _) = test_server().await;
        let fetcher = local_fetcher();
        let card = fetcher
            .fetch(&format!("http://site.test:{port}/missing-thumbnail-page"))
            .await
            .expect("a missing thumbnail must not discard valid page metadata");

        assert_eq!(card.title, "Card without image");
        assert!(card.thumbnail.is_none());
        server.abort();
    }

    #[tokio::test]
    #[ignore = "requires live public internet"]
    async fn live_github_link_preview_fetches() {
        let result = LinkCardFetcher::new()
            .fetch("https://github.com/apchat-agent/apparition")
            .await;
        assert!(result.is_ok(), "GitHub preview fetch failed: {result:?}");
    }

    #[tokio::test]
    async fn refuses_private_ip_literals_and_private_dns_answers() {
        let fetcher = LinkCardFetcher::new();
        for address in [
            "127.0.0.1",
            "10.1.2.3",
            "192.168.1.2",
            "169.254.1.2",
            "100.64.1.2",
            "::1",
            "fd00::1",
        ] {
            let url = format!("http://[{address}]/")
                .replace("http://[127.0.0.1]/", "http://127.0.0.1/")
                .replace("http://[10.1.2.3]/", "http://10.1.2.3/")
                .replace("http://[192.168.1.2]/", "http://192.168.1.2/")
                .replace("http://[169.254.1.2]/", "http://169.254.1.2/")
                .replace("http://[100.64.1.2]/", "http://100.64.1.2/");
            assert!(
                fetcher.fetch(&url).await.is_err(),
                "{address} must be refused"
            );
        }

        struct PrivateDns;
        impl HostResolver for PrivateDns {
            fn resolve<'a>(
                &'a self,
                _: &'a str,
                _: u16,
            ) -> Pin<Box<dyn Future<Output = Result<Vec<IpAddr>, String>> + Send + 'a>>
            {
                Box::pin(async { Ok(vec![IpAddr::V4(Ipv4Addr::new(10, 0, 0, 8))]) })
            }
        }
        let dns_fetcher = LinkCardFetcher::with_client(
            SafeHttpClient::with_resolver(PrivateDns).allow_http_schemes(),
        );
        assert!(dns_fetcher.fetch("http://private.example/").await.is_err());
    }
}
