//! Static, advisory server-card and AI Catalog HTTP documents.
use axum::{
    Router,
    body::Body,
    http::{HeaderMap, HeaderValue, StatusCode, header},
    response::Response,
    routing::get,
};
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::sync::Arc;

/// Media type of the domain-level AI Catalog.
pub const AI_CATALOG_MEDIA_TYPE: &str = "application/ai-catalog+json";

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct Catalog<'a> {
    spec_version: &'static str,
    entries: [Entry<'a>; 1],
}
#[derive(Serialize)]
struct Entry<'a> {
    identifier: &'a str,
    #[serde(rename = "type")]
    media_type: &'static str,
    url: &'a str,
}
struct Document {
    body: Vec<u8>,
    etag: String,
    media_type: &'static str,
}
impl Document {
    fn new(body: Vec<u8>, media_type: &'static str) -> Self {
        // Same digest construction as pinned Go; byte encodings may differ.
        let digest = Sha256::digest(&body);
        let hex = format!("{digest:x}");
        let etag = format!("\"{}\"", &hex[..32]);
        Self {
            body,
            etag,
            media_type,
        }
    }
    fn response(&self, headers: &HeaderMap) -> Response {
        let matched = headers.get_all(header::IF_NONE_MATCH).iter().any(|value| {
            value.to_str().is_ok_and(|value| {
                value.split(',').any(|tag| {
                    let tag = tag.trim();
                    tag == "*" || tag.strip_prefix("W/").unwrap_or(tag) == self.etag
                })
            })
        });
        let mut response = Response::new(if matched {
            Body::empty()
        } else {
            Body::from(self.body.clone())
        });
        *response.status_mut() = if matched {
            StatusCode::NOT_MODIFIED
        } else {
            StatusCode::OK
        };
        let headers = response.headers_mut();
        headers.insert(
            header::CONTENT_TYPE,
            HeaderValue::from_static(self.media_type),
        );
        headers.insert(
            header::CACHE_CONTROL,
            HeaderValue::from_static("public, max-age=3600"),
        );
        // The digest is always a quoted ASCII hexadecimal value.
        if let Ok(etag) = self.etag.parse() {
            headers.insert(header::ETAG, etag);
        }
        headers.insert(
            header::ACCESS_CONTROL_ALLOW_ORIGIN,
            HeaderValue::from_static("*"),
        );
        headers.insert(
            header::ACCESS_CONTROL_ALLOW_METHODS,
            HeaderValue::from_static("GET, HEAD"),
        );
        headers.insert(
            header::ACCESS_CONTROL_ALLOW_HEADERS,
            HeaderValue::from_static("Content-Type, If-None-Match"),
        );
        headers.insert(
            header::ACCESS_CONTROL_EXPOSE_HEADERS,
            HeaderValue::from_static("ETag"),
        );
        response
    }
}

/// Build immutable public discovery routes, to merge with [`super::http_router`].
///
/// Serves `/mcp/server-card`, `/.well-known/ai-catalog.json`, and the
/// `/.well-known/mcp/catalog.json` alias. GET/HEAD support ETag revalidation.
/// The card is advisory; clients must reconcile it with live MCP discovery.
/// The host supplies the public card URL and catalog identifier explicitly;
/// forwarded headers and the request Host are never used to construct them.
/// Serialization snapshots the card, so later registry changes do not alter it.
///
/// # Errors
/// Returns a serialization error if a document cannot be encoded.
pub fn router(
    card: &crate::server_card::ServerCard,
    card_url: &str,
    catalog_identifier: &str,
) -> Result<Router, serde_json::Error> {
    let card = Arc::new(Document::new(
        serde_json::to_vec(card)?,
        crate::server_card::SERVER_CARD_MEDIA_TYPE,
    ));
    let catalog = Arc::new(Document::new(
        serde_json::to_vec(&Catalog {
            spec_version: "1.0",
            entries: [Entry {
                identifier: catalog_identifier,
                media_type: crate::server_card::SERVER_CARD_MEDIA_TYPE,
                url: card_url,
            }],
        })?,
        AI_CATALOG_MEDIA_TYPE,
    ));
    let catalog_alias = catalog.clone();
    Ok(Router::new()
        .route(
            "/mcp/server-card",
            get(move |headers: HeaderMap| {
                let card = card.clone();
                async move { card.response(&headers) }
            }),
        )
        .route(
            "/.well-known/ai-catalog.json",
            get(move |headers: HeaderMap| {
                let catalog = catalog.clone();
                async move { catalog.response(&headers) }
            }),
        )
        .route(
            "/.well-known/mcp/catalog.json",
            get(move |headers: HeaderMap| {
                let catalog = catalog_alias.clone();
                async move { catalog.response(&headers) }
            }),
        ))
}
