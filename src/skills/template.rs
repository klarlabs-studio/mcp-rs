//! Atomic registration of dynamic skill entrypoint resources.
use std::{future::Future, sync::Arc};

use super::{SKILL_MARKDOWN_MIME, SkillError, SkillIndexEntry, SkillIndexType};
use crate::{
    Context, Server,
    model::{ErrorData, ResourceContents},
    resource::UriParameters,
};

/// Configuration for a dynamic skill entrypoint using named path segments.
///
/// Nothing is registered until a handler is supplied. Dropping this builder
/// leaves the server unchanged. Handlers receive raw, non-percent-decoded
/// parameters and own the returned skill document's metadata and validity.
#[derive(Debug)]
#[must_use = "call handler or content_handler to register the skill template"]
pub struct SkillTemplateBuilder<'a> {
    server: &'a mut Server,
    uri: String,
    description: String,
}

impl Server {
    /// Configure a parameterized `skill://.../SKILL.md` resource and index entry.
    ///
    /// Supports literal ASCII path segments and whole-segment `{name}` variables,
    /// with at least one variable. Names use ASCII letters, digits and `_`, and
    /// cannot start with a digit. Operators, repeated variables, query strings,
    /// fragments, percent escapes and partial-segment variables are rejected.
    /// This is a subset of RFC 6570, matching the framework's simple routing.
    /// # Errors
    /// Returns validation errors without changing the server.
    pub fn skill_template(
        &mut self,
        uri_template: impl AsRef<str>,
        description: impl AsRef<str>,
    ) -> Result<SkillTemplateBuilder<'_>, SkillError> {
        let uri = uri_template.as_ref().trim();
        let description = description.as_ref().trim();
        validate_template(uri)?;
        if description.is_empty() {
            return Err(SkillError::InvalidTemplateDescription);
        }
        Ok(SkillTemplateBuilder {
            server: self,
            uri: uri.to_owned(),
            description: description.to_owned(),
        })
    }

    /// Remove one skill template and its owned resource; snapshots stay intact.
    /// Returns false for unknown templates or identifiers belonging to bundles.
    pub fn remove_skill_template(&mut self, uri_template: &str) -> bool {
        let uri = uri_template.trim();
        if !self
            .inner
            .skills
            .get(uri)
            .is_some_and(|entry| entry.entry_type == SkillIndexType::Template)
        {
            return false;
        }
        self.remove_skill(uri)
    }
}

impl SkillTemplateBuilder<'_> {
    /// Atomically register a Markdown handler receiving URI and raw parameters.
    /// # Errors
    /// Returns namespace, URI-template or resource conflicts without changing the server.
    pub fn handler<F, Fut>(self, handler: F) -> Result<(), SkillError>
    where
        F: Fn(Context, String, UriParameters) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<String, ErrorData>> + Send + 'static,
    {
        self.content_handler(move |ctx, uri, params| {
            let future = handler(ctx, uri.clone(), params);
            async move {
                Ok(vec![
                    ResourceContents::text(future.await?, uri).with_mime_type(SKILL_MARKDOWN_MIME),
                ])
            }
        })
    }

    /// Atomically register a handler returning native resource content blocks.
    ///
    /// Advertised MIME is Markdown; individual blocks keep caller-supplied MIME
    /// types. As with ordinary resources, contents are not parsed or validated.
    /// # Errors
    /// Returns namespace, URI-template or resource conflicts without changing the server.
    pub fn content_handler<F, Fut>(self, handler: F) -> Result<(), SkillError>
    where
        F: Fn(Context, String, UriParameters) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<ResourceContents>, ErrorData>> + Send + 'static,
    {
        if self.server.inner.skills.contains_key(&self.uri) {
            return Err(SkillError::DuplicateSkill(self.uri));
        }
        let mut candidate = self.server.clone();
        candidate.ensure_skill_index()?;
        candidate
            .resource(&self.uri)
            .name("skill-template")
            .description(&self.description)
            .mime_type(SKILL_MARKDOWN_MIME)
            .content_handler(handler)?;
        let state = Arc::make_mut(&mut candidate.inner);
        state
            .skill_owners
            .insert(self.uri.clone(), self.uri.clone());
        state.skills.insert(
            self.uri.clone(),
            SkillIndexEntry {
                name: String::new(),
                entry_type: SkillIndexType::Template,
                description: self.description,
                url: self.uri,
            },
        );
        *self.server = candidate;
        Ok(())
    }
}

fn validate_template(uri: &str) -> Result<(), SkillError> {
    let invalid = || SkillError::InvalidTemplate(uri.to_owned());
    let path = uri
        .strip_prefix("skill://")
        .and_then(|value| value.strip_suffix("/SKILL.md"))
        .ok_or_else(invalid)?;
    let mut variables = std::collections::BTreeSet::new();
    for segment in path.split('/') {
        if let Some(name) = segment
            .strip_prefix('{')
            .and_then(|value| value.strip_suffix('}'))
        {
            if name.is_empty()
                || !name
                    .bytes()
                    .all(|value| value.is_ascii_alphanumeric() || value == b'_')
                || name.as_bytes()[0].is_ascii_digit()
                || !variables.insert(name)
            {
                return Err(invalid());
            }
        } else if segment.is_empty()
            || !segment
                .bytes()
                .all(|value| value.is_ascii_alphanumeric() || matches!(value, b'_' | b'-'))
        {
            return Err(invalid());
        }
    }
    if variables.is_empty() {
        return Err(invalid());
    }
    Ok(())
}
