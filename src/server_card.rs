//! Typed support for static MCP Server Cards (SEP-2127).

use crate::model::{Icon, Implementation};
use serde::Serialize;
use serde_json::Value;
use std::collections::BTreeMap;

/// JSON schema advertised by Server Card documents.
pub const SERVER_CARD_SCHEMA_URI: &str =
    "https://static.modelcontextprotocol.io/schemas/v1/server-card.schema.json";
/// Media type used to serve a Server Card document.
pub const SERVER_CARD_MEDIA_TYPE: &str = "application/mcp-server-card+json";
const CURRENT_PROTOCOL_VERSION: &str = "2026-07-28";

/// A static metadata document describing an MCP server before connection.
#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ServerCard {
    /// Schema URI for this document.
    #[serde(rename = "$schema")]
    pub schema: String,
    /// Reverse-DNS style server name, normalized to include a namespace.
    pub name: String,
    /// Server release version.
    pub version: String,
    /// Human-readable description, limited to 100 Unicode characters.
    pub description: String,
    /// Optional human-facing title.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    /// Optional project website.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub website_url: Option<String>,
    /// Optional source repository metadata.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub repository: Option<Repository>,
    /// Optional server icons.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub icons: Vec<Icon>,
    /// Available remote connection endpoints.
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub remotes: Vec<Remote>,
    /// Optional namespaced extension metadata.
    #[serde(rename = "_meta", skip_serializing_if = "Option::is_none")]
    pub meta: Option<BTreeMap<String, Value>>,
}

impl ServerCard {
    /// Build a card from the server identity used by protocol discovery.
    pub fn from_identity(identity: &Implementation) -> Self {
        let name = normalize_name(&identity.name);
        let description = identity
            .description
            .as_deref()
            .filter(|text| !text.is_empty())
            .unwrap_or(&identity.name);
        Self {
            schema: SERVER_CARD_SCHEMA_URI.to_owned(),
            name,
            version: identity.version.clone(),
            description: description.chars().take(100).collect(),
            title: identity.title.clone(),
            website_url: identity.website_url.clone(),
            repository: None,
            icons: identity.icons.clone().unwrap_or_default(),
            remotes: Vec::new(),
            meta: None,
        }
    }

    /// Replace the card name, applying the standard namespace normalization.
    pub fn with_name(mut self, name: impl AsRef<str>) -> Self {
        self.name = normalize_name(name.as_ref());
        self
    }

    /// Append a modern Streamable HTTP endpoint.
    pub fn with_remote(mut self, remote: Remote) -> Self {
        self.remotes.push(remote);
        self
    }

    /// Attach source repository metadata.
    pub fn with_repository(mut self, repository: Repository) -> Self {
        self.repository = Some(repository);
        self
    }

    /// Attach namespaced extension metadata.
    pub fn with_meta(mut self, meta: BTreeMap<String, Value>) -> Self {
        self.meta = Some(meta);
        self
    }
}

fn normalize_name(name: &str) -> String {
    let name = name.trim();
    if name.is_empty() {
        "local/unnamed".to_owned()
    } else if name.contains('/') {
        name.to_owned()
    } else {
        format!("local/{name}")
    }
}

/// Source repository information for a server.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Repository {
    /// Repository URL.
    pub url: String,
    /// Hosting or source provider name.
    pub source: String,
    /// Optional repository subfolder containing the server.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub subfolder: Option<String>,
    /// Optional provider-specific repository identifier.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub id: Option<String>,
}

impl Repository {
    /// Create repository metadata from its URL and source provider.
    pub fn new(url: impl Into<String>, source: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            source: source.into(),
            subfolder: None,
            id: None,
        }
    }

    /// Set the repository subfolder.
    pub fn with_subfolder(mut self, subfolder: impl Into<String>) -> Self {
        self.subfolder = Some(subfolder.into());
        self
    }

    /// Set the provider-specific repository identifier.
    pub fn with_id(mut self, id: impl Into<String>) -> Self {
        self.id = Some(id.into());
        self
    }
}

/// User-provided or server-provided value for a remote configuration field.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct RemoteInput {
    /// Human-readable explanation of the input.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    /// Whether a value must be supplied.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_required: Option<bool>,
    /// Whether a value is secret.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub is_secret: Option<bool>,
    /// Expected input format.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub format: Option<String>,
    /// Default value.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub default: Option<String>,
    /// Example value shown in an input control.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub placeholder: Option<String>,
    /// Explicit value to use.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub value: Option<String>,
    /// Allowed values for a selection input.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub choices: Option<Vec<String>>,
}

/// A named input commonly used to configure a request header.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct KeyValueInput {
    /// Header or input name.
    pub name: String,
    /// Input configuration.
    #[serde(flatten)]
    pub input: RemoteInput,
    /// Optional variables used to interpolate this input.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub variables: Option<BTreeMap<String, RemoteInput>>,
}

impl KeyValueInput {
    /// Create a named input with default configuration.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            input: RemoteInput::default(),
            variables: None,
        }
    }

    /// Set the input configuration.
    pub fn with_input(mut self, input: RemoteInput) -> Self {
        self.input = input;
        self
    }

    /// Set variables used to interpolate this input.
    pub fn with_variables(mut self, variables: BTreeMap<String, RemoteInput>) -> Self {
        self.variables = Some(variables);
        self
    }
}

/// A Streamable HTTP remote endpoint, pinned to the current MCP protocol version.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Remote {
    #[serde(rename = "type")]
    remote_type: &'static str,
    url: String,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    headers: Vec<KeyValueInput>,
    #[serde(skip_serializing_if = "BTreeMap::is_empty")]
    variables: BTreeMap<String, RemoteInput>,
    supported_protocol_versions: [&'static str; 1],
}

impl Remote {
    /// Create a modern Streamable HTTP endpoint.
    pub fn streamable_http(url: impl Into<String>) -> Self {
        Self {
            remote_type: "streamable-http",
            url: url.into(),
            headers: Vec::new(),
            variables: BTreeMap::new(),
            supported_protocol_versions: [CURRENT_PROTOCOL_VERSION],
        }
    }

    /// Add a configurable HTTP header.
    pub fn with_header(mut self, header: KeyValueInput) -> Self {
        self.headers.push(header);
        self
    }

    /// Set variables for the endpoint URL or its headers.
    pub fn with_variables(mut self, variables: BTreeMap<String, RemoteInput>) -> Self {
        self.variables = variables;
        self
    }

    /// Return this endpoint's URL.
    pub fn url(&self) -> &str {
        &self.url
    }
}
