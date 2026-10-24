use crate::model::{
    CallToolResponse, CallToolResult, ContentBlock, ErrorData, JsonObject, MetaObject, Tool,
    ToolAnnotations,
};
use crate::{
    Context, Next, RegistrationError, Response, Server, ToolError, error, server::RegisteredTool,
};
use futures_util::FutureExt;
use schemars::JsonSchema;
use serde::{Serialize, de::DeserializeOwned};
use serde_json::{Value, json};
use std::{future::Future, sync::Arc};

/// Fluent configuration for a typed tool.
#[derive(Debug)]
#[must_use = "call handler or rich_handler to register the tool"]
pub struct ToolBuilder<'a> {
    pub(crate) server: &'a mut Server,
    pub(crate) name: String,
    pub(crate) description: String,
    pub(crate) meta: Option<MetaObject>,
    pub(crate) output_schema: Option<Arc<JsonObject>>,
    pub(crate) annotations: Option<ToolAnnotations>,
    pub(crate) title: Option<String>,
    pub(crate) skip_validation: bool,
    pub(crate) discovery: crate::discovery::DiscoveryOptions,
}
impl ToolBuilder<'_> {
    /// Set the group used by [`Server::discover_tools`]. This is not an access policy.
    pub fn group(mut self, group: impl Into<String>) -> Self {
        self.discovery.group = group.into();
        self
    }
    /// Replace the tags used by [`Server::discover_tools`]; requested tags match with AND.
    pub fn tags(mut self, tags: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.discovery.tags = tags.into_iter().map(Into::into).collect();
        self
    }
    /// Hide schemas in the host catalog unless full detail is explicitly requested.
    /// Standard SDK `tools/list` still includes schemas. Calls retain full validation.
    pub fn defer_schema(mut self) -> Self {
        self.discovery.defer_schema = true;
        self
    }
    /// Set the description shown to clients.
    pub fn description(mut self, description: impl Into<String>) -> Self {
        self.description = description.into();
        self
    }
    /// Set a human-readable title.
    pub fn title(mut self, title: impl Into<String>) -> Self {
        self.title = Some(title.into());
        self
    }
    /// Set client-facing tool annotations.
    pub fn annotations(mut self, annotations: ToolAnnotations) -> Self {
        self.annotations = Some(annotations);
        self
    }
    /// Attach arbitrary tool metadata. UI resource configuration merges into it.
    pub fn meta(mut self, meta: MetaObject) -> Self {
        self.meta = Some(meta);
        self
    }
    /// Link an MCP App resource. Register the HTML resource separately.
    pub fn ui_resource(mut self, uri: impl Into<String>) -> Self {
        self.meta
            .get_or_insert_with(|| MetaObject(Default::default()))
            .0
            .insert("ui".into(), json!({"resourceUri":uri.into()}));
        self
    }
    /// Declare an object output schema. Successful results must provide matching structured content.
    ///
    /// # Errors
    /// Returns an error if the output type is not an object schema.
    pub fn output_schema<O: JsonSchema + 'static>(mut self) -> Result<Self, RegistrationError> {
        self.output_schema = Some(
            mcp_protocol::handler::server::tool::schema_for_input::<O>()
                .map_err(RegistrationError::InvalidOutputSchema)?,
        );
        Ok(self)
    }
    /// Skip validation of argument values against JSON Schema.
    /// Serde decoding and routing-annotation schema validation still apply.
    pub fn skip_validation(mut self) -> Self {
        self.skip_validation = true;
        self
    }
    /// Register a typed handler, automatically converting values to text content.
    /// Object results also populate structured content when an output schema is declared.
    ///
    /// # Errors
    /// Returns a registration error for invalid identifiers or schemas.
    pub fn handler<I, R, F, Fut>(self, handler: F) -> Result<(), RegistrationError>
    where
        I: DeserializeOwned + JsonSchema + Send + 'static,
        R: Serialize + Send + 'static,
        F: Fn(Context, I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<R, ToolError>> + Send + 'static,
    {
        let structured = self.output_schema.is_some();
        self.rich_handler(move |ctx, input| {
            let future = handler(ctx, input);
            async move {
                let value = serde_json::to_value(future.await?)
                    .map_err(|_| ToolError::new("Could not serialize tool result"))?;
                let text = value
                    .as_str()
                    .map(str::to_owned)
                    .unwrap_or_else(|| value.to_string());
                let mut result = CallToolResult::success(vec![ContentBlock::text(text)]);
                if structured && value.is_object() {
                    result.structured_content = Some(value);
                }
                Ok(result)
            }
        })
    }
    /// Register a typed-input handler returning native MCP content and structured results.
    ///
    /// Error results may omit structured content, even when an output schema exists.
    /// Empty object structured content is preserved distinctly from absent content.
    ///
    /// # Errors
    /// Returns a registration error for invalid identifiers or schemas.
    pub fn rich_handler<I, F, Fut>(self, handler: F) -> Result<(), RegistrationError>
    where
        I: DeserializeOwned + JsonSchema + Send + 'static,
        F: Fn(Context, I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<CallToolResult, ToolError>> + Send + 'static,
    {
        self.protocol_handler(move |ctx, input| {
            let future = handler(ctx, input);
            async move { future.await.map(Into::into) }
        })
    }
    /// Register a task-only tool using the installed SDK task manager.
    ///
    /// Admission requires the client to declare the tasks extension. Task futures
    /// catch panics, validate declared outputs, and stop on task cancellation.
    /// Context cancellation is independent of the creating request and is signalled
    /// when the task operation ends, including expiry or manager shutdown.
    /// The manager owns TTL and polling; the caller owns manager shutdown.
    /// # Errors
    /// Returns a missing-manager, identifier, or schema registration error.
    pub fn task_handler<I, F, Fut>(
        self,
        options: mcp_protocol::task_manager::TaskOptions,
        handler: F,
    ) -> Result<(), RegistrationError>
    where
        I: DeserializeOwned + JsonSchema + Send + 'static,
        F: Fn(Context, mcp_protocol::task_manager::TaskContext, I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<CallToolResult, mcp_protocol::task_manager::TaskExit>>
            + Send
            + 'static,
    {
        if self.server.inner.tasks.is_none() {
            return Err(RegistrationError::MissingTaskManager);
        }
        let output_validator = self
            .output_schema
            .as_ref()
            .map(|schema| {
                jsonschema::validator_for(&Value::Object((**schema).clone()))
                    .map(Arc::new)
                    .map_err(|e| RegistrationError::InvalidOutputSchema(e.to_string()))
            })
            .transpose()?;
        let metadata = self.meta.clone();
        let handler = Arc::new(handler);
        self.protocol_handler(move |mut context, input| {
            let tasks = context.extensions.get::<mcp_protocol::task_manager::TaskManager>().cloned();
            let handler = handler.clone();
            let options = options.clone();
            let output_validator = output_validator.clone();
            let metadata = metadata.clone();
            async move {
                if !context.client_capabilities().is_some_and(|caps| caps.supports_tasks()) {
                    return Err(ToolError::new("Client must declare the tasks extension"));
                }
                let tasks = tasks.ok_or_else(|| ToolError::new("Task manager unavailable"))?;
                context.ct = tokio_util::sync::CancellationToken::new();
                let task = tasks.spawn(options, move |task_context| {
                    Box::pin(async move {
                        // Task work outlives the creating request. End its independent
                        // helper scope even when the manager aborts this future.
                        let _scope = context.ct.clone().drop_guard();
                        let cancellation = task_context.clone();
                        let future = std::panic::AssertUnwindSafe(async {
                            handler(context, task_context, input).await
                        }).catch_unwind();
                        let mut result = tokio::select! {
                            biased;
                            _ = cancellation.cancelled() => {
                                return Err(mcp_protocol::task_manager::TaskExit::Cancelled);
                            }
                            outcome = future => match outcome {
                                Ok(result) => result?,
                                Err(_) => return Err(ErrorData::internal_error("Task handler panicked", None).into()),
                            },
                        };
                        if result.is_error != Some(true)
                            && let Some(validator) = output_validator
                            && !result.structured_content.as_ref().is_some_and(|value| validator.is_valid(value))
                        {
                            return Err(ErrorData::internal_error("Task result did not match its output schema", None).into());
                        }
                        if let Some(metadata) = metadata {
                            let meta = result.meta.get_or_insert_with(|| MetaObject(Default::default()));
                            for (key, value) in metadata.0 {
                                meta.0.entry(key).or_insert(value);
                            }
                        }
                        Ok(result)
                    })
                });
                Ok(CallToolResponse::Task(crate::model::CreateTaskResult::new(task)))
            }
        })
    }
    /// Register a native response handler for final results, MRTR, or task handles.
    ///
    /// The complete tool request is available in `Context::extensions` as
    /// `CallToolRequestParams`, including input responses and opaque request state.
    /// The SDK enforces protocol and capability requirements for advanced results.
    /// # Errors
    /// Returns invalid identifier or schema errors during registration.
    pub fn protocol_handler<I, F, Fut>(self, handler: F) -> Result<(), RegistrationError>
    where
        I: DeserializeOwned + JsonSchema + Send + 'static,
        F: Fn(Context, I) -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<CallToolResponse, ToolError>> + Send + 'static,
    {
        error::check_identifier(
            "tool",
            &self.name,
            self.server.inner.tools.contains_key(&self.name),
        )?;
        let schema = mcp_protocol::handler::server::tool::schema_for_input::<I>()
            .map_err(RegistrationError::InvalidInputSchema)?;
        mcp_protocol::model::validate_tool_header_annotations(&schema)
            .map_err(RegistrationError::InvalidInputSchema)?;
        let input_validator = if self.skip_validation {
            None
        } else {
            Some(Arc::new(
                jsonschema::validator_for(&Value::Object((*schema).clone()))
                    .map_err(|e| RegistrationError::InvalidInputSchema(e.to_string()))?,
            ))
        };
        let output_validator = self
            .output_schema
            .as_ref()
            .map(|schema| {
                jsonschema::validator_for(&Value::Object((**schema).clone()))
                    .map(Arc::new)
                    .map_err(|e| RegistrationError::InvalidOutputSchema(e.to_string()))
            })
            .transpose()?;
        let mut definition = Tool::new(self.name.clone(), self.description, schema);
        if let Some(meta) = self.meta {
            definition = definition.with_meta(meta);
        }
        if let Some(schema) = self.output_schema {
            definition = definition.with_raw_output_schema(schema);
        }
        if let Some(annotations) = self.annotations {
            definition = definition.with_annotations(annotations);
        }
        if let Some(title) = self.title {
            definition = definition.with_title(title);
        }
        let result_meta = definition.meta.clone();
        let handler = Arc::new(handler);
        let invoke: Next = Arc::new(move |ctx, args| {
            let handler = handler.clone();
            let meta = result_meta.clone();
            let input_validator = input_validator.clone();
            let output_validator = output_validator.clone();
            async move {
                if let Some(validator) = input_validator
                    && let Err(error) = validator.validate(&args)
                {
                    return Ok(Response::Tool(tool_error(format!(
                        "Invalid tool input at {}: {}",
                        error.instance_path(),
                        error
                    ))));
                }
                let input = match serde_json::from_value::<I>(args) {
                    Ok(input) => input,
                    Err(error) => {
                        return Ok(Response::Tool(tool_error(format!(
                            "Invalid tool input: {error}"
                        ))));
                    }
                };
                let response = match handler(ctx, input).await {
                    Ok(result) => result,
                    Err(error) => tool_error(error.to_string()).into(),
                };
                let mut result = match response {
                    CallToolResponse::Complete(result) => result,
                    other => return Ok(Response::Protocol(Box::new(other))),
                };
                if result.is_error != Some(true)
                    && let Some(validator) = output_validator
                {
                    let valid = result
                        .structured_content
                        .as_ref()
                        .is_some_and(|value| validator.is_valid(value));
                    if !valid {
                        return Err(ErrorData::internal_error(
                            "Tool result did not match its output schema",
                            None,
                        ));
                    }
                }
                if let Some(meta) = meta {
                    let result_meta = result
                        .meta
                        .get_or_insert_with(|| MetaObject(Default::default()));
                    for (key, value) in meta.0 {
                        result_meta.0.entry(key).or_insert(value);
                    }
                }
                Ok(Response::Tool(result))
            }
            .boxed()
        });
        Arc::make_mut(&mut self.server.inner).tools.insert(
            self.name,
            RegisteredTool {
                definition,
                discovery: self.discovery,
                handler: invoke,
            },
        );
        Ok(())
    }
}
fn tool_error(message: impl Into<String>) -> CallToolResult {
    CallToolResult::error(vec![ContentBlock::text(message.into())])
}
