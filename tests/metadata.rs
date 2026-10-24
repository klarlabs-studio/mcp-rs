//! Metadata decoding remains available without client or transport features.
#![allow(clippy::unwrap_used)]
use mcp_rs::{BuildInfo, ServerMetadata, model::*};
use serde_json::json;

#[test]
fn modern_icon_sizes_decode_without_mutating_wire_data() {
    for (sizes, expected) in [
        (json!("any"), json!(["any"])),
        (
            json!("48x48  96x96\t128x128"),
            json!(["48x48", "96x96", "128x128"]),
        ),
        (json!(["48x48", "96x96"]), json!(["48x48", "96x96"])),
        (json!(""), json!([])),
    ] {
        let identity = json!({"name":"server","version":"1","icons":[{
            "src":"https://example.test/icon.svg","sizes":sizes,"theme":"dark"
        }],"buildInfo":{"commit":"abc123","buildDate":"2026-10-17T10:00:00Z"}});
        let original = identity.clone();
        let metadata = ServerMetadata::from_identity_value(&identity).unwrap();
        assert_eq!(
            serde_json::to_value(metadata.identity).unwrap()["icons"][0]["sizes"],
            expected
        );
        assert_eq!(
            metadata.build_info,
            Some(BuildInfo::new("abc123", "2026-10-17T10:00:00Z"))
        );
        assert_eq!(identity, original);
    }
}
#[test]
fn malformed_advertised_metadata_is_an_error() {
    for identity in [
        json!({"name":"server","version":"1","buildInfo":7}),
        json!({"name":"server","version":"1","buildInfo":{"commit":false}}),
        json!({"name":"server","version":"1","icons":[{"src":"x","sizes":12}]}),
        json!({"name":"server","version":"1","icons":[{"src":"x","sizes":["any",7]}]}),
        json!({"name":"server"}),
        json!(null),
    ] {
        assert!(
            ServerMetadata::from_identity_value(&identity).is_err(),
            "{identity}"
        );
    }
}
#[test]
fn absent_and_partial_build_information_preserve_optional_fields() {
    let minimal = json!({"name":"server","version":"1"});
    assert!(
        ServerMetadata::from_identity_value(&minimal)
            .unwrap()
            .build_info
            .is_none()
    );
    let partial = json!({"name":"server","version":"1","buildInfo":{"commit":"abc123"}});
    let metadata = ServerMetadata::from_identity_value(&partial).unwrap();
    assert_eq!(metadata.build_info, Some(BuildInfo::new("abc123", "")));
    assert_eq!(
        serde_json::to_value(BuildInfo::default()).unwrap(),
        json!({})
    );
    assert_eq!(
        serde_json::to_value(BuildInfo::new("abc123", "")).unwrap(),
        json!({"commit":"abc123"})
    );
}
#[test]
fn missing_and_malformed_discovery_identity_are_distinct() {
    let mut discovery = DiscoverResult::new(
        vec![ProtocolVersion::V_2026_07_28],
        ServerCapabilities::default(),
    );
    assert!(
        ServerMetadata::from_discovery(&discovery)
            .unwrap()
            .is_none()
    );
    discovery
        .meta
        .get_or_insert_default()
        .insert("io.modelcontextprotocol/serverInfo".into(), json!(7));
    assert!(ServerMetadata::from_discovery(&discovery).is_err());
}
