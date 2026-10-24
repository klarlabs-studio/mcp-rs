// Derived from rmcp 3.5.1; maintained and modified by mcp-rs. See ../../../UPSTREAM.md.
use serde::{Deserialize, Serialize};

use super::{
    Annotations, ContentBlock, Icon, MetaObject, Role,
    content::{EmbeddedResource, TextContent},
    resource::ResourceContents,
};

#[cfg(feature = "base64")]
use super::content::{AudioContent, ImageContent};

/// A prompt or prompt template that the server offers (spec `Prompt`).
#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[non_exhaustive]
pub struct Prompt {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub arguments: Option<Vec<PromptArgument>>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub icons: Option<Vec<Icon>>,
    #[serde(rename = "_meta", skip_serializing_if = "Option::is_none")]
    pub meta: Option<MetaObject>,
}

impl Prompt {
    pub fn new<N, D>(
        name: N,
        description: Option<D>,
        arguments: Option<Vec<PromptArgument>>,
    ) -> Self
    where
        N: Into<String>,
        D: Into<String>,
    {
        Prompt {
            name: name.into(),
            title: None,
            description: description.map(Into::into),
            arguments,
            icons: None,
            meta: None,
        }
    }

    pub fn from_raw(
        name: impl Into<String>,
        description: Option<impl Into<String>>,
        arguments: Option<Vec<PromptArgument>>,
    ) -> Self {
        Prompt {
            name: name.into(),
            title: None,
            description: description.map(Into::into),
            arguments,
            icons: None,
            meta: None,
        }
    }

    pub fn with_title(mut self, title: impl Into<String>) -> Self {
        self.title = Some(title.into());
        self
    }

    pub fn with_icons(mut self, icons: Vec<Icon>) -> Self {
        self.icons = Some(icons);
        self
    }

    pub fn with_meta(mut self, meta: MetaObject) -> Self {
        self.meta = Some(meta);
        self
    }
}

/// Describes an argument that a prompt can accept (spec `PromptArgument`).
#[derive(Default, Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[non_exhaustive]
pub struct PromptArgument {
    pub name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub title: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub description: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub required: Option<bool>,
}

impl PromptArgument {
    pub fn new<N: Into<String>>(name: N) -> Self {
        PromptArgument {
            name: name.into(),
            title: None,
            description: None,
            required: None,
        }
    }

    pub fn with_title<T: Into<String>>(mut self, title: T) -> Self {
        self.title = Some(title.into());
        self
    }

    pub fn with_description<D: Into<String>>(mut self, description: D) -> Self {
        self.description = Some(description.into());
        self
    }

    pub fn with_required(mut self, required: bool) -> Self {
        self.required = Some(required);
        self
    }
}

/// A message returned as part of a prompt (spec `PromptMessage`).
///
/// Uses the unified `ContentBlock` for its content (text | image | audio | resource_link | resource).
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[cfg_attr(feature = "schemars", derive(schemars::JsonSchema))]
#[non_exhaustive]
pub struct PromptMessage {
    pub role: Role,
    pub content: ContentBlock,
}

impl PromptMessage {
    pub fn new(role: Role, content: ContentBlock) -> Self {
        Self { role, content }
    }

    pub fn new_text<S: Into<String>>(role: Role, text: S) -> Self {
        Self {
            role,
            content: ContentBlock::text(text),
        }
    }

    #[cfg(feature = "base64")]
    pub fn new_image(
        role: Role,
        data: &[u8],
        mime_type: &str,
        meta: Option<MetaObject>,
        annotations: Option<Annotations>,
    ) -> Self {
        use base64::{Engine, prelude::BASE64_STANDARD};

        let base64 = BASE64_STANDARD.encode(data);
        Self {
            role,
            content: ContentBlock::Image(ImageContent {
                data: base64,
                mime_type: mime_type.into(),
                meta,
                annotations,
            }),
        }
    }

    #[cfg(feature = "base64")]
    pub fn new_audio(
        role: Role,
        data: &[u8],
        mime_type: &str,
        meta: Option<MetaObject>,
        annotations: Option<Annotations>,
    ) -> Self {
        use base64::{Engine, prelude::BASE64_STANDARD};

        let base64 = BASE64_STANDARD.encode(data);
        Self {
            role,
            content: ContentBlock::Audio(AudioContent {
                data: base64,
                mime_type: mime_type.into(),
                meta,
                annotations,
            }),
        }
    }

    pub fn new_resource(
        role: Role,
        uri: String,
        mime_type: Option<String>,
        text: Option<String>,
        resource_meta: Option<MetaObject>,
        resource_content_meta: Option<MetaObject>,
        annotations: Option<Annotations>,
    ) -> Self {
        let resource_contents = match text {
            Some(t) => ResourceContents::TextResourceContents {
                uri,
                mime_type,
                text: t,
                meta: resource_content_meta,
            },
            None => ResourceContents::BlobResourceContents {
                uri,
                mime_type,
                blob: String::new(),
                meta: resource_content_meta,
            },
        };
        Self {
            role,
            content: ContentBlock::Resource(EmbeddedResource {
                meta: resource_meta,
                resource: resource_contents,
                annotations,
            }),
        }
    }

    pub fn new_text_with_meta<S: Into<String>>(
        role: Role,
        text: S,
        meta: Option<MetaObject>,
    ) -> Self {
        Self {
            role,
            content: ContentBlock::Text(TextContent {
                text: text.into(),
                meta,
                annotations: None,
            }),
        }
    }

    pub fn new_resource_link(role: Role, resource: super::resource::Resource) -> Self {
        Self {
            role,
            content: ContentBlock::ResourceLink(resource),
        }
    }
}
