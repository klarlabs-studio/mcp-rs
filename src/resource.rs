use crate::model::{
    ErrorData, ReadResourceResponse, ReadResourceResult, Resource, ResourceContents,
    ResourceTemplate,
};
use crate::{Context, RegistrationError, Server, error, server::RegisteredResource};
use futures_util::{FutureExt, future::BoxFuture};
use std::{collections::BTreeMap, future::Future, sync::Arc};

/// Raw URI-template parameters, matching upstream's non-percent-decoding behavior.
pub type UriParameters = BTreeMap<String, String>;
pub(crate) type ResourceHandler = Arc<
    dyn Fn(
            Context,
            String,
            UriParameters,
        ) -> BoxFuture<'static, Result<ReadResourceResponse, ErrorData>>
        + Send
        + Sync,
>;

#[derive(Clone, Debug)]
pub(crate) struct UriMatcher {
    pattern: regex::Regex,
    parameters: Vec<String>,
}
impl UriMatcher {
    fn compile(template: &str) -> Result<Option<Self>, RegistrationError> {
        if !template.contains(['{', '}']) {
            return Ok(None);
        }
        let mut pattern = String::from("^");
        let mut parameters = vec![];
        let mut rest = template;
        while let Some(start) = rest.find('{') {
            if rest[..start].contains('}') {
                return Err(RegistrationError::InvalidUriTemplate(template.into()));
            }
            pattern.push_str(&regex::escape(&rest[..start]));
            let end = rest[start + 1..]
                .find('}')
                .map(|i| i + start + 1)
                .ok_or_else(|| RegistrationError::InvalidUriTemplate(template.into()))?;
            let name = &rest[start + 1..end];
            if name.is_empty() || name.contains('{') || parameters.iter().any(|p| p == name) {
                return Err(RegistrationError::InvalidUriTemplate(template.into()));
            }
            parameters.push(name.to_owned());
            pattern.push_str("([^/]+)");
            rest = &rest[end + 1..];
        }
        if rest.contains('}') {
            return Err(RegistrationError::InvalidUriTemplate(template.into()));
        }
        pattern.push_str(&regex::escape(rest));
        pattern.push('$');
        Ok(Some(Self {
            pattern: regex::Regex::new(&pattern)
                .map_err(|e| RegistrationError::InvalidUriTemplate(e.to_string()))?,
            parameters,
        }))
    }
    pub(crate) fn parameters(&self, uri: &str) -> Option<UriParameters> {
        let captures = self.pattern.captures(uri)?;
        self.parameters
            .iter()
            .enumerate()
            .map(|(index, name)| {
                captures
                    .get(index + 1)
                    .map(|value| (name.clone(), value.as_str().to_owned()))
            })
            .collect()
    }
}

/// Fluent configuration for a static or `{parameter}` URI-template resource.
#[derive(Debug)]
#[must_use = "call handler, template_handler, content_handler, or protocol_handler to register the resource"]
pub struct ResourceBuilder<'a> {
    pub(crate) server: &'a mut Server,
    pub(crate) uri: String,
    pub(crate) name: String,
    pub(crate) description: String,
    pub(crate) mime_type: String,
}
impl ResourceBuilder<'_> {
    /// Set the resource display name.
    pub fn name(mut self, name: impl Into<String>) -> Self {
        self.name = name.into();
        self
    }
    /// Set the description shown to clients.
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = description.into();
        self
    }
    /// Set the advertised MIME type, also inherited by text convenience handlers.
    pub fn mime_type(mut self, mime: impl Into<String>) -> Self {
        self.mime_type = mime.into();
        self
    }
    /// Register a text handler that receives the requested URI.
    ///
    /// # Errors
    /// Fails on invalid or duplicate identifiers and malformed URI templates.
    pub fn handler<F, Fut>(self, handler: F) -> Result<(), RegistrationError>
    where
        F: Fn(Context, String) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<String, ErrorData>> + Send + 'static,
    {
        self.template_handler(move |ctx, uri, _| handler(ctx, uri))
    }
    /// Register a text handler receiving raw extracted template parameters.
    ///
    /// # Errors
    /// Fails on invalid or duplicate identifiers and malformed URI templates.
    pub fn template_handler<F, Fut>(self, handler: F) -> Result<(), RegistrationError>
    where
        F: Fn(Context, String, UriParameters) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<String, ErrorData>> + Send + 'static,
    {
        let mime_type = self.mime_type.clone();
        self.content_handler(move |ctx, uri, params| {
            let future = handler(ctx, uri.clone(), params);
            let mime_type = mime_type.clone();
            async move {
                Ok(vec![
                    ResourceContents::text(future.await?, uri).with_mime_type(mime_type),
                ])
            }
        })
    }
    /// Register a handler returning native text or base64 binary content blocks.
    ///
    /// Multiple blocks and their individual MIME types are preserved.
    /// # Errors
    /// Fails on invalid or duplicate identifiers and malformed URI templates.
    pub fn content_handler<F, Fut>(self, handler: F) -> Result<(), RegistrationError>
    where
        F: Fn(Context, String, UriParameters) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<Vec<ResourceContents>, ErrorData>> + Send + 'static,
    {
        self.protocol_handler(move |ctx, uri, params| {
            let future = handler(ctx, uri, params);
            async move {
                future
                    .await
                    .map(|contents| ReadResourceResult::new(contents).into())
            }
        })
    }
    /// Register a native response handler supporting multi-round input requests.
    ///
    /// Receives raw URI-template parameters. Use [`crate::Session::resource_request`]
    /// to inspect input responses and opaque request state on each retry.
    /// # Errors
    /// Fails on invalid or duplicate identifiers and malformed URI templates.
    pub fn protocol_handler<F, Fut>(self, handler: F) -> Result<(), RegistrationError>
    where
        F: Fn(Context, String, UriParameters) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<ReadResourceResponse, ErrorData>> + Send + 'static,
    {
        error::check_identifier(
            "resource",
            &self.uri,
            self.server.inner.resources.contains_key(&self.uri),
        )?;
        let matcher = UriMatcher::compile(&self.uri)?;
        let definition = Resource::new(self.uri.clone(), self.name.clone())
            .with_description(self.description.clone())
            .with_mime_type(self.mime_type.clone());
        let template = matcher.as_ref().map(|_| {
            ResourceTemplate::new(self.uri.clone(), self.name)
                .with_description(self.description)
                .with_mime_type(self.mime_type)
        });
        Arc::make_mut(&mut self.server.inner).resources.insert(
            self.uri,
            RegisteredResource {
                definition,
                template,
                matcher,
                handler: Arc::new(move |ctx, uri, params| handler(ctx, uri, params).boxed()),
            },
        );
        Ok(())
    }
}
