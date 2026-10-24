#![allow(clippy::unwrap_used)]
//! Serialization and normalization coverage for static server cards.

use mcp_rs::model::{Icon, Implementation};
use mcp_rs::server_card::{
    KeyValueInput, Remote, RemoteInput, Repository, SERVER_CARD_MEDIA_TYPE, SERVER_CARD_SCHEMA_URI,
    ServerCard,
};
use serde_json::{Value, json};
use std::collections::BTreeMap;

#[test]
fn identity_conversion_normalizes_name_and_preserves_metadata() {
    let mut identity = Implementation::new("  acme/weather  ", "2.3.0");
    identity.title = Some("Weather".into());
    identity.description = Some("Forecasts".into());
    identity.website_url = Some("https://example.test".into());
    identity.icons = Some(vec![Icon::new("https://example.test/icon.svg")]);

    let card = ServerCard::from_identity(&identity);
    assert_eq!(card.name, "acme/weather");
    assert_eq!(card.version, "2.3.0");
    let encoded = serde_json::to_value(card).unwrap();
    assert_eq!(encoded["$schema"], SERVER_CARD_SCHEMA_URI);
    assert_eq!(encoded["title"], "Weather");
    assert_eq!(encoded["websiteUrl"], "https://example.test");
    assert_eq!(encoded["icons"][0]["src"], "https://example.test/icon.svg");
    assert!(encoded.get("repository").is_none());
    assert!(encoded.get("_meta").is_none());
    assert_eq!(SERVER_CARD_MEDIA_TYPE, "application/mcp-server-card+json");
}

#[test]
fn names_descriptions_and_unicode_truncation_are_safe() {
    let mut identity = Implementation::new("demo", "1");
    identity.description = Some("é🙂".repeat(60));
    let card = ServerCard::from_identity(&identity);
    assert_eq!(card.name, "local/demo");
    assert_eq!(card.description.chars().count(), 100);

    identity.name = " \t ".into();
    identity.description = Some(String::new());
    let card = ServerCard::from_identity(&identity).with_name("  ");
    assert_eq!(card.name, "local/unnamed");
    assert_eq!(card.description, " \t ");
}

#[test]
fn fluent_metadata_and_remote_emit_only_current_transport_version() {
    let mut meta = BTreeMap::new();
    meta.insert("vendor/example".to_owned(), json!({"enabled": true}));
    let repository = Repository::new("https://example.test/repo", "github")
        .with_subfolder("server")
        .with_id("repo-1");
    let input = RemoteInput {
        description: Some("Access token".into()),
        is_required: Some(true),
        is_secret: Some(true),
        ..RemoteInput::default()
    };
    let remote = Remote::streamable_http("https://example.test/mcp")
        .with_header(KeyValueInput::new("Authorization").with_input(input));
    let card = ServerCard::from_identity(&Implementation::new("demo", "1"))
        .with_repository(repository)
        .with_meta(meta)
        .with_remote(remote);
    let wire: Value = serde_json::to_value(card).unwrap();
    assert_eq!(wire["repository"]["subfolder"], "server");
    assert_eq!(wire["repository"]["id"], "repo-1");
    assert_eq!(wire["_meta"]["vendor/example"]["enabled"], true);
    assert_eq!(wire["remotes"][0]["type"], "streamable-http");
    assert_eq!(wire["remotes"][0]["url"], "https://example.test/mcp");
    assert_eq!(wire["remotes"][0]["headers"][0]["isSecret"], true);
    assert_eq!(
        wire["remotes"][0]["supportedProtocolVersions"],
        json!(["2026-07-28"])
    );
    assert!(wire.to_string().find("sse").is_none());
    assert!(wire.to_string().find("2025-").is_none());
}
