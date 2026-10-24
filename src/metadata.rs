//! Modern server identity metadata, including the upstream build information extension.
use crate::model::{DiscoverResult, Implementation, MetaObject, ServerPeerInfo};
use serde::{Deserialize, Serialize};
use serde_json::Value;

pub(crate) const SERVER_INFO_KEY: &str = "io.modelcontextprotocol/serverInfo";

/// Optional build provenance, matching modern Go's `serverInfo.buildInfo` extension.
/// Values are supplied by the host; the framework never reads Git or the environment.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct BuildInfo {
    /// Source commit identifier, omitted when empty.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub commit: String,
    /// Host-supplied build timestamp, omitted when empty.
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub build_date: String,
}
impl BuildInfo {
    /// Set build provenance without inventing or validating a timestamp format.
    pub fn new(commit: impl Into<String>, build_date: impl Into<String>) -> Self {
        Self {
            commit: commit.into(),
            build_date: build_date.into(),
        }
    }
}

/// A typed view of modern server identity and its build information extension.
///
/// Reading accepts SDK array-valued icon sizes and pinned Go whitespace-separated
/// size strings. Normalization changes only this owned view, never discovery data.
/// Outgoing framework identities retain native SDK array-valued icon sizes.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct ServerMetadata {
    /// Native SDK identity, with any Go size strings expanded into size arrays.
    pub identity: Implementation,
    /// Upstream build information extension, when explicitly advertised.
    pub build_info: Option<BuildInfo>,
}
impl ServerMetadata {
    /// Read identity from modern discovery metadata. Missing identity returns `None`.
    /// # Errors
    /// Returns a JSON decoding error for malformed identity or build information.
    pub fn from_discovery(result: &DiscoverResult) -> Result<Option<Self>, serde_json::Error> {
        Self::from_meta(result.meta.as_ref())
    }
    /// Read identity retained by the SDK's modern discovery handshake.
    /// # Errors
    /// Returns a JSON decoding error for malformed identity or build information.
    pub fn from_peer_info(info: &ServerPeerInfo) -> Result<Option<Self>, serde_json::Error> {
        Self::from_meta(info.meta.as_ref())
    }
    fn from_meta(meta: Option<&MetaObject>) -> Result<Option<Self>, serde_json::Error> {
        meta.and_then(|meta| meta.get(SERVER_INFO_KEY))
            .map(Self::from_identity_value)
            .transpose()
    }
    /// Decode a `serverInfo` object using either modern icon sizes encoding.
    /// Unknown extension fields are ignored, matching the native SDK model.
    /// # Errors
    /// Returns a JSON decoding error for malformed identity or build information.
    pub fn from_identity_value(value: &Value) -> Result<Self, serde_json::Error> {
        let mut normalized = value.clone();
        if let Some(icons) = normalized.get_mut("icons").and_then(Value::as_array_mut) {
            for icon in icons {
                if let Some(sizes) = icon.get_mut("sizes")
                    && let Some(text) = sizes.as_str()
                {
                    *sizes = Value::Array(
                        text.split_ascii_whitespace()
                            .map(|size| Value::String(size.into()))
                            .collect(),
                    );
                }
            }
        }
        let build_info = value
            .get("buildInfo")
            .map(|value| serde_json::from_value(value.clone()))
            .transpose()?
            .flatten();
        Ok(Self {
            identity: serde_json::from_value(normalized)?,
            build_info,
        })
    }
}
