//! Deterministic modern HTTP task fixture for cross-language update checks.
use mcp_rs::{
    Server, ToolError,
    model::{CallToolResult, ContentBlock, InputRequest},
    protocol::ErrorData,
    protocol::task_manager::{TaskManager, TaskOptions},
    transport,
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;
use std::sync::Arc;
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

#[derive(Deserialize, JsonSchema)]
struct Input {
    key: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut args = std::env::args().skip(1);
    if args.next().as_deref() != Some("--http") {
        return Err("expected --http <address>".into());
    }
    let address = args.next().ok_or("missing address")?;
    let gate = Arc::new(Semaphore::new(0));
    let tasks = TaskManager::new();
    let mut server = Server::new("task-wire", "1").with_task_manager(tasks.clone());
    let wait = gate.clone();
    server
        .tool("gated")
        .task_handler(TaskOptions::default(), move |_, task, input: Input| {
            let wait = wait.clone();
            async move {
                if input.key != "input" {
                    return Err(ErrorData::invalid_params("unknown gate", None).into());
                }
                let permit = wait
                    .acquire()
                    .await
                    .map_err(|_| ErrorData::internal_error("gate closed", None))?;
                permit.forget();
                let request: InputRequest = serde_json::from_value(json!({
                    "method":"elicitation/create",
                    "params":{"mode":"form","message":"What is your name?",
                        "requestedSchema":{"type":"object","properties":{"name":{"type":"string"}}}}
                }))
                .map_err(|_| ErrorData::internal_error("invalid fixture request", None))?;
                let answer = task.request_input("name", request).await?;
                let name = answer["content"]["name"]
                    .as_str()
                    .filter(|s| !s.is_empty())
                    .ok_or_else(|| ErrorData::invalid_params("accepted name required", None))?;
                if answer["action"] != "accept" {
                    return Err(ErrorData::invalid_params("accepted name required", None).into());
                }
                Ok(CallToolResult::success(vec![ContentBlock::text(format!(
                    "hello {name}"
                ))]))
            }
        })?;
    server.tool("release").handler(move |_, input: Input| {
        let gate = gate.clone();
        async move {
            if input.key != "input" {
                return Err(ToolError::new("unknown gate"));
            }
            gate.add_permits(1);
            Ok("released")
        }
    })?;
    server.tool("ask").task_handler(
        TaskOptions::default(),
        |_, task, input: Input| async move {
            let params = match input.key.as_str() {
                "form" => json!({"mode":"form","message":"What is your name?",
                    "requestedSchema":{"type":"object","properties":{"name":{"type":"string"}}}}),
                "url" => json!({"mode":"url","message":"Open the consent page",
                    "url":"https://example.test/consent","elicitationId":"consent-1"}),
                _ => return Err(ErrorData::invalid_params("unknown input mode", None).into()),
            };
            let request: InputRequest = serde_json::from_value(json!({
                "method":"elicitation/create", "params":params
            }))
            .map_err(|_| ErrorData::internal_error("invalid fixture request", None))?;
            let answer = task.request_input("answer", request).await?;
            let action = answer["action"]
                .as_str()
                .filter(|action| matches!(*action, "accept" | "decline" | "cancel"))
                .ok_or_else(|| ErrorData::invalid_params("unknown action", None))?;
            if input.key == "form" && action == "accept" && answer["content"]["name"] != "Ada" {
                return Err(ErrorData::invalid_params("expected Ada", None).into());
            }
            Ok(CallToolResult::success(vec![ContentBlock::text(format!(
                "{}:{action}",
                input.key
            ))]))
        },
    )?;
    server.tool("sequence").task_handler(
        TaskOptions::default(),
        |_, task, _: Input| async move {
            let name_request: InputRequest = serde_json::from_value(json!({
                "method":"elicitation/create",
                "params":{"mode":"form","message":"Choose a name",
                    "requestedSchema":{"type":"object","properties":{"name":{"type":"string"}},
                        "required":["name"]}}
            }))
            .map_err(|_| ErrorData::internal_error("invalid fixture request", None))?;
            let answer = task.request_input("name", name_request).await?;
            let name = answer["content"]["name"]
                .as_str()
                .filter(|name| matches!(*name, "Ada" | "Grace"))
                .ok_or_else(|| ErrorData::invalid_params("expected Ada or Grace", None))?;
            if answer["action"] != "accept" {
                return Err(ErrorData::invalid_params("expected acceptance", None).into());
            }
            let confirm_request: InputRequest = serde_json::from_value(json!({
                "method":"elicitation/create",
                "params":{"mode":"form","message":format!("Confirm {name}"),
                    "requestedSchema":{"type":"object","properties":{"confirm":{"type":"boolean"}},
                        "required":["confirm"]}}
            }))
            .map_err(|_| ErrorData::internal_error("invalid fixture request", None))?;
            let confirmation = task.request_input("confirm", confirm_request).await?;
            if confirmation["action"] != "accept" || confirmation["content"]["confirm"] != true {
                return Err(ErrorData::invalid_params("expected confirmation", None).into());
            }
            Ok(CallToolResult::success(vec![ContentBlock::text(name)]))
        },
    )?;
    // TTL belongs to the host. No retired client-side task options are needed.
    for name in ["retained", "overdue"] {
        let manager = tasks.clone();
        server.tool(name).handler(move |_, _: Input| {
            let manager = manager.clone();
            async move {
                let task = manager.spawn(TaskOptions::new().with_ttl_ms(1500), move |_| {
                    Box::pin(async move {
                        if name == "overdue" {
                            std::future::pending::<()>().await;
                        }
                        Ok(CallToolResult::success(vec![ContentBlock::text(
                            "retained",
                        )]))
                    })
                });
                Ok::<_, ToolError>(task.task_id)
            }
        })?;
    }
    let shutdown = CancellationToken::new();
    let stop = shutdown.clone();
    let signal = tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            stop.cancel();
        }
    });
    let listener = tokio::net::TcpListener::bind(address).await?;
    let result = transport::serve_http(server, listener, shutdown).await;
    tasks.shutdown();
    signal.abort();
    result?;
    Ok(())
}
