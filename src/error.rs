//! Errors produced while configuring an application.

/// A registration failure detected before the server starts.
///
/// Configuration failures are separate from JSON-RPC errors returned to clients.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum RegistrationError {
    /// A tool, prompt, or resource has an empty identifier.
    #[error("{kind} identifier must not be empty")]
    EmptyIdentifier {
        /// The registration kind.
        kind: &'static str,
    },
    /// The identifier is already registered in this server configuration.
    #[error("{kind} '{name}' is already registered")]
    Duplicate {
        /// The registration kind.
        kind: &'static str,
        /// The conflicting identifier.
        name: String,
    },
    /// The input type cannot be represented as an MCP object schema.
    #[error("invalid tool input schema: {0}")]
    InvalidInputSchema(String),
    /// The output type cannot be represented or compiled as an object schema.
    #[error("invalid tool output schema: {0}")]
    InvalidOutputSchema(String),
    /// A resource URI template could not be compiled.
    #[error("invalid resource URI template: {0}")]
    InvalidUriTemplate(String),
    /// Task tools require an installed shared task manager.
    #[error("install a task manager before registering task handlers")]
    MissingTaskManager,
    /// Prompt argument names must be nonempty and unique.
    #[error("prompt argument '{0}' must have a nonempty, unique name")]
    InvalidPromptArgument(String),
}

pub(crate) fn check_identifier(
    kind: &'static str,
    name: &str,
    exists: bool,
) -> Result<(), RegistrationError> {
    if name.trim().is_empty() {
        return Err(RegistrationError::EmptyIdentifier { kind });
    }
    if exists {
        return Err(RegistrationError::Duplicate {
            kind,
            name: name.to_owned(),
        });
    }
    Ok(())
}
