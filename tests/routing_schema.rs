//! Registration must not advertise routing schemas that the HTTP client rejects.
#![allow(clippy::unwrap_used)]
use mcp_rs::{RegistrationError, Server, model::CallToolResult};
use schemars::JsonSchema;
use serde::Deserialize;

#[derive(Deserialize, JsonSchema)]
struct BadToken {
    #[schemars(extend("x-mcp-header" = "bad:name"))]
    value: String,
}
#[derive(Deserialize, JsonSchema)]
struct Collision {
    #[schemars(extend("x-mcp-header" = "Region"))]
    first: String,
    #[schemars(extend("x-mcp-header" = "region"))]
    second: String,
}
#[derive(Deserialize, JsonSchema)]
struct Nested {
    child: BadToken,
}
#[derive(Deserialize, JsonSchema)]
struct Valid {
    #[schemars(extend("x-mcp-header" = "Region"))]
    region: String,
}

fn invalid(result: Result<(), RegistrationError>) {
    assert!(
        matches!(result, Err(RegistrationError::InvalidInputSchema(_))),
        "{result:?}"
    );
}
fn reuse_name(server: &mut Server) {
    server
        .tool("deploy")
        .handler(|_, value: Valid| async move { Ok(value.region) })
        .unwrap();
}

#[test]
fn invalid_routing_token_is_rejected_without_reserving_the_name() {
    let mut server = Server::new("schema", "1");
    invalid(
        server
            .tool("deploy")
            .handler(|_, value: BadToken| async move { Ok(value.value) }),
    );
    reuse_name(&mut server);
}

#[test]
fn case_insensitive_header_collisions_are_rejected_for_rich_handlers() {
    let mut server = Server::new("schema", "1");
    invalid(
        server
            .tool("deploy")
            .rich_handler(|_, value: Collision| async move {
                Ok(text(format!("{}{}", value.first, value.second)))
            }),
    );
    reuse_name(&mut server);
}

#[test]
fn referenced_nested_routing_annotations_are_rejected_for_protocol_handlers() {
    let mut server = Server::new("schema", "1");
    invalid(
        server
            .tool("deploy")
            .protocol_handler(|_, value: Nested| async move { Ok(text(value.child.value).into()) }),
    );
    reuse_name(&mut server);
}

#[test]
fn skipping_instance_validation_does_not_skip_routing_schema_validation() {
    let mut server = Server::new("schema", "1");
    invalid(
        server
            .tool("deploy")
            .skip_validation()
            .handler(|_, value: BadToken| async move { Ok(value.value) }),
    );
    reuse_name(&mut server);
}

#[tokio::test]
async fn task_registration_validates_routing_before_installing_the_tool() {
    use mcp_rs::protocol::task_manager::{TaskManager, TaskOptions};
    let tasks = TaskManager::new();
    let mut server = Server::new("schema", "1").with_task_manager(tasks.clone());
    invalid(
        server
            .tool("deploy")
            .task_handler(TaskOptions::default(), |_, _, value: BadToken| async move {
                Ok(text(value.value))
            }),
    );
    reuse_name(&mut server);
    tasks.shutdown();
}

fn text(value: impl Into<String>) -> CallToolResult {
    CallToolResult::success(vec![mcp_rs::model::ContentBlock::text(value)])
}
