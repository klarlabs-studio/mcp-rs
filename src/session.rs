//! Request-bound peer operations and monotonic progress reporting.
use crate::{Context, model::*};
use mcp_protocol::{ErrorData, RoleServer, service::ServiceError};
use std::sync::Arc;
use tokio::sync::Mutex;

/// Request-bound access to a peer, cancellation, and shared progress state.
///
/// Clone this value to share progress ordering between subtasks of one request.
#[derive(Clone, Debug)]
pub struct Session {
    context: Context,
    last_progress: Arc<Mutex<f64>>,
}
impl Session {
    /// Create a session facade for the currently executing handler.
    pub fn from_context(context: &Context) -> Self {
        Self {
            context: context.clone(),
            last_progress: Arc::new(Mutex::new(0.0)),
        }
    }
    /// Access the SDK peer for native callbacks and notifications.
    pub fn peer(&self) -> &mcp_protocol::Peer<RoleServer> {
        &self.context.peer
    }
    /// Access native tool request state, including MRTR input responses.
    pub fn tool_request(&self) -> Option<&CallToolRequestParams> {
        self.context.extensions.get::<CallToolRequestParams>()
    }
    /// Access the original resource request, including MRTR state and input responses.
    pub fn resource_request(&self) -> Option<&ReadResourceRequestParams> {
        self.context.extensions.get::<ReadResourceRequestParams>()
    }
    /// Access the original prompt request, including MRTR state and input responses.
    pub fn prompt_request(&self) -> Option<&GetPromptRequestParams> {
        self.context.extensions.get::<GetPromptRequestParams>()
    }
    /// Wait until the request ends or is cancelled.
    ///
    /// Inside a task handler this follows the task operation instead: returning
    /// its creation handle does not cancel the scope. Completion, failure, task
    /// cancellation, expiry or manager shutdown signals cancellation to helpers.
    /// Helpers must observe this signal; spawned work is not automatically joined.
    pub async fn cancelled(&self) {
        self.context.ct.cancelled().await
    }
    /// Send a progress update when the caller supplied a progress token.
    ///
    /// Repeated/decreasing values advance by 0.1, or to the next representable
    /// value when floating-point precision requires a larger step. Updates are
    /// serialized using an async mutex so concurrent clones remain ordered.
    /// No-token requests are a no-op. Caller-supplied string/numeric tokens are kept.
    /// # Errors
    /// Returns invalid-parameter or notification delivery errors, including when
    /// progress cannot increase without overflowing the finite range.
    pub async fn progress(
        &self,
        progress: f64,
        total: Option<f64>,
        message: Option<String>,
    ) -> Result<(), ServiceError> {
        let Some(token) = self.context.meta.get_progress_token() else {
            return Ok(());
        };
        if !progress.is_finite() || total.is_some_and(|total| !total.is_finite() || total < 0.0) {
            return Err(ServiceError::McpError(ErrorData::invalid_params(
                "Progress and total must be finite; total must be nonnegative",
                None,
            )));
        }
        let mut last = self.last_progress.lock().await;
        let progress = if progress <= *last {
            // At large magnitudes adding 0.1 can round back to `last`.
            (*last + 0.1).max(last.next_up())
        } else {
            progress
        };
        if !progress.is_finite() {
            return Err(ServiceError::McpError(ErrorData::invalid_params(
                "Progress overflow",
                None,
            )));
        }
        let mut params = ProgressNotificationParam::new(token, progress);
        params.total = total;
        params.message = message;
        self.context.peer.notify_progress(params).await?;
        *last = progress;
        Ok(())
    }
    /// Request a typed form from a client supporting elicitation.
    ///
    /// # Errors
    /// Returns SDK capability, rejection, validation, or transport errors.
    #[cfg(feature = "elicitation")]
    pub async fn elicit<T>(
        &self,
        message: impl Into<String>,
    ) -> Result<Option<T>, mcp_protocol::service::ElicitationError>
    where
        T: serde::de::DeserializeOwned + JsonSchema + mcp_protocol::service::ElicitationSafe,
    {
        use mcp_protocol::service::ElicitationError;
        // Modern capabilities belong to the current request, not connection-wide
        // initialize state. Require the explicit modern form capability.
        if !self
            .context
            .client_capabilities()
            .and_then(|caps| caps.elicitation)
            .is_some_and(|capability| capability.form.is_some())
        {
            return Err(ElicitationError::CapabilityNotSupported);
        }
        let schema = ElicitationSchema::from_type::<T>().map_err(|_| {
            ElicitationError::Service(ServiceError::McpError(ErrorData::invalid_params(
                "Invalid elicitation schema",
                None,
            )))
        })?;
        let response = self
            .context
            .peer
            .create_elicitation(ElicitRequestParams::FormElicitationParams {
                meta: None,
                message: message.into(),
                requested_schema: schema,
            })
            .await?;
        match response.action {
            ElicitationAction::Accept => {
                let content = response.content.ok_or(ElicitationError::NoContent)?;
                serde_json::from_value(content.clone())
                    .map(Some)
                    .map_err(|error| ElicitationError::ParseError {
                        error,
                        data: content,
                    })
            }
            ElicitationAction::Decline => Err(ElicitationError::UserDeclined),
            ElicitationAction::Cancel => Err(ElicitationError::UserCancelled),
            _ => Err(ElicitationError::Service(ServiceError::McpError(
                ErrorData::internal_error("Unsupported elicitation action", None),
            ))),
        }
    }
}
#[cfg(feature = "elicitation")]
use schemars::JsonSchema;
