use crate::model::{ErrorData, GetPromptResponse, GetPromptResult, Prompt, PromptArgument};
use crate::{Context, RegistrationError, Server, error, server::RegisteredPrompt};
use futures_util::{FutureExt, future::BoxFuture};
use std::{collections::BTreeMap, future::Future, sync::Arc};
pub(crate) type PromptHandler = Arc<
    dyn Fn(
            Context,
            BTreeMap<String, String>,
        ) -> BoxFuture<'static, Result<GetPromptResponse, ErrorData>>
        + Send
        + Sync,
>;
/// Fluent configuration for a parameterized prompt.
#[derive(Debug)]
#[must_use = "call handler or protocol_handler to register the prompt"]
pub struct PromptBuilder<'a> {
    pub(crate) server: &'a mut Server,
    pub(crate) name: String,
    pub(crate) description: String,
    pub(crate) arguments: Vec<PromptArgument>,
}
impl PromptBuilder<'_> {
    /// Set the description shown to clients.
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = description.into();
        self
    }
    /// Declare a string argument. Empty or duplicate names fail registration.
    pub fn argument(
        mut self,
        name: impl Into<String>,
        description: impl Into<String>,
        required: bool,
    ) -> Self {
        self.arguments.push(
            PromptArgument::new(name)
                .with_description(description)
                .with_required(required),
        );
        self
    }
    /// Register the asynchronous handler.
    ///
    /// # Errors
    /// Returns a registration error for invalid or duplicate identifiers.
    pub fn handler<F, Fut>(self, handler: F) -> Result<(), RegistrationError>
    where
        F: Fn(Context, BTreeMap<String, String>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<GetPromptResult, ErrorData>> + Send + 'static,
    {
        self.protocol_handler(move |ctx, args| {
            let future = handler(ctx, args);
            async move { future.await.map(Into::into) }
        })
    }
    /// Register a native response handler supporting multi-round input requests.
    ///
    /// Use [`crate::Session::prompt_request`] to inspect the original request,
    /// including its input responses and opaque request state on each retry.
    /// # Errors
    /// Returns a registration error for invalid or duplicate identifiers.
    pub fn protocol_handler<F, Fut>(self, handler: F) -> Result<(), RegistrationError>
    where
        F: Fn(Context, BTreeMap<String, String>) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<GetPromptResponse, ErrorData>> + Send + 'static,
    {
        error::check_identifier(
            "prompt",
            &self.name,
            self.server.inner.prompts.contains_key(&self.name),
        )?;
        let mut names = std::collections::BTreeSet::new();
        for argument in &self.arguments {
            if argument.name.trim().is_empty() || !names.insert(&argument.name) {
                return Err(RegistrationError::InvalidPromptArgument(
                    argument.name.clone(),
                ));
            }
        }
        let definition = Prompt::new(
            self.name.clone(),
            Some(self.description),
            Some(self.arguments),
        );
        Arc::make_mut(&mut self.server.inner).prompts.insert(
            self.name,
            RegisteredPrompt {
                definition,
                handler: Arc::new(move |ctx, args| handler(ctx, args).boxed()),
            },
        );
        Ok(())
    }
}
