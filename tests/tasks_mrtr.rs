//! Modern SDK-backed tasks and multi-round request lifecycle integration.
#![allow(clippy::unwrap_used, clippy::expect_used)]
use mcp_rs::{
    Server, Session,
    model::*,
    protocol::{
        ClientLifecycleMode, ClientServiceExt, ServiceExt,
        task_manager::{TaskManager, TaskOptions},
    },
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;
use std::{collections::BTreeMap, time::Duration};

fn assert_invalid_params(error: mcp_rs::protocol::service::ServiceError) {
    let mcp_rs::protocol::service::ServiceError::McpError(error) = error else {
        panic!("expected MCP invalid-params error, got {error:?}")
    };
    assert_eq!(error.code, ErrorCode::INVALID_PARAMS);
}
#[derive(Deserialize, JsonSchema)]
struct Input {}
async fn modern<H: mcp_rs::protocol::ServerHandler>(
    server: H,
) -> (
    mcp_rs::protocol::service::RunningService<mcp_rs::protocol::RoleClient, ClientConfig>,
    tokio::task::JoinHandle<()>,
) {
    let (client_io, server_io) = tokio::io::duplex(65536);
    let task = tokio::spawn(async move {
        let service = server.serve(server_io).await.unwrap();
        service.waiting().await.unwrap();
    });
    let config = ClientConfig::new(
        ClientCapabilities::builder().enable_tasks().build(),
        Implementation::new("task-client", "1"),
    );
    let client = config
        .serve_with_lifecycle(
            client_io,
            ClientLifecycleMode::Discover {
                preferred_versions: vec![ProtocolVersion::V_2026_07_28],
            },
        )
        .await
        .unwrap();
    (client, task)
}
async fn until(
    client: &mcp_rs::protocol::service::RunningService<mcp_rs::protocol::RoleClient, ClientConfig>,
    id: &str,
    status: TaskStatus,
) -> GetTaskResult {
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            let task = client.get_task(GetTaskParams::new(id)).await.unwrap();
            if task.task.status() == status {
                return task;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap()
}
fn request(name: &str) -> CallToolRequestParams {
    CallToolRequestParams::new(name.to_owned()).with_arguments(Default::default())
}
#[tokio::test]
async fn tasks_complete_accept_inputs_and_cancel_through_the_same_manager() {
    let tasks = TaskManager::new();
    let mut server = Server::new("tasks", "1").with_task_manager(tasks.clone());
    server
        .tool("complete")
        .task_handler(TaskOptions::default(), |_, _, _: Input| async {
            Ok(CallToolResult::success(vec![ContentBlock::text("done")]))
        })
        .unwrap();
    server
        .tool("cancel")
        .task_handler(TaskOptions::default(), |_, _, _: Input| async {
            std::future::pending().await
        })
        .unwrap();
    server.tool("input").task_handler(TaskOptions::default(),|_,task, _:Input|async move {
  let input:InputRequest=serde_json::from_value(json!({"method":"elicitation/create","params":{"mode":"form","message":"Name?","requestedSchema":{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}}})).unwrap();
  let answer=task.request_input("name",input).await?;
  Ok(CallToolResult::success(vec![ContentBlock::text(answer["content"]["name"].as_str().unwrap_or_default())]))
 }).unwrap();
    let (client, task) = modern(server).await;
    let CallToolResponse::Task(created) = client.call_tool_once(request("complete")).await.unwrap()
    else {
        panic!("expected task handle")
    };
    let completed = until(&client, &created.task.task_id, TaskStatus::Completed).await;
    let result = serde_json::to_value(completed).unwrap();
    assert_eq!(result["result"]["content"][0]["text"], "done");
    let CallToolResponse::Task(created) = client.call_tool_once(request("cancel")).await.unwrap()
    else {
        panic!("expected task handle")
    };
    client
        .cancel_task(CancelTaskParams::new(&created.task.task_id))
        .await
        .unwrap();
    until(&client, &created.task.task_id, TaskStatus::Cancelled).await;
    let CallToolResponse::Task(created) = client.call_tool_once(request("input")).await.unwrap()
    else {
        panic!("expected task handle")
    };
    until(&client, &created.task.task_id, TaskStatus::InputRequired).await;
    client
        .update_task(UpdateTaskParams::new(
            &created.task.task_id,
            BTreeMap::from([(
                "name".into(),
                json!({"action":"accept","content":{"name":"Felix"}}),
            )]),
        ))
        .await
        .unwrap();
    let completed = until(&client, &created.task.task_id, TaskStatus::Completed).await;
    assert_eq!(
        serde_json::to_value(completed).unwrap()["result"]["content"][0]["text"],
        "Felix"
    );
    assert!(
        client
            .get_task(GetTaskParams::new("unknown"))
            .await
            .is_err()
    );
    client.cancel().await.unwrap();
    task.await.unwrap();
    tasks.shutdown();
}
#[tokio::test]
async fn task_admission_rejects_clients_without_capability_before_spawning() {
    let tasks = TaskManager::new();
    let mut server = Server::new("tasks", "1").with_task_manager(tasks.clone());
    server
        .tool("task")
        .task_handler(TaskOptions::default(), |_, _, _: Input| async {
            Ok(CallToolResult::success(vec![]))
        })
        .unwrap();
    let (client_io, server_io) = tokio::io::duplex(65536);
    let task = tokio::spawn(async move {
        let service = server.serve(server_io).await.unwrap();
        service.waiting().await.unwrap();
    });
    let client = ()
        .serve_with_lifecycle(
            client_io,
            mcp_rs::protocol::ClientLifecycleMode::Discover {
                preferred_versions: vec![ProtocolVersion::V_2026_07_28],
            },
        )
        .await
        .unwrap();
    let result = client.call_tool(request("task")).await.unwrap();
    assert_eq!(result.is_error, Some(true));
    assert_eq!(tasks.running_task_count(), 0);
    client.cancel().await.unwrap();
    task.await.unwrap();
    tasks.shutdown();
}
#[tokio::test]
async fn native_mrtr_resumes_with_opaque_state_and_client_input() {
    let mut server = Server::new("mrtr", "1");
    server.tool("rounds").protocol_handler(|ctx,_:Input|async move {
  let session=Session::from_context(&ctx);let request=session.tool_request().unwrap();
  if request.request_state.as_deref()==Some("awaiting-name") && let Some(answer)=request.input_responses.as_ref().and_then(|responses|responses.get("name")) {
   return Ok(CallToolResult::success(vec![ContentBlock::text(answer["content"]["name"].as_str().unwrap_or_default())]).into());
  }
  let input:InputRequest=serde_json::from_value(json!({"method":"elicitation/create","params":{"mode":"form","message":"Name?","requestedSchema":{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}}})).unwrap();
  Ok(CallToolResponse::InputRequired(InputRequiredResult::new(Some(BTreeMap::from([("name".into(),input)])),Some("awaiting-name".into()))))
 }).unwrap();
    let (client, task) = modern(server).await;
    let CallToolResponse::InputRequired(required) =
        client.call_tool_once(request("rounds")).await.unwrap()
    else {
        panic!("expected input request")
    };
    assert_eq!(required.request_state.as_deref(), Some("awaiting-name"));
    assert!(required.input_requests.unwrap().contains_key("name"));
    let request = request("rounds")
        .with_request_state("awaiting-name")
        .with_input_responses(BTreeMap::from([(
            "name".into(),
            json!({"action":"accept","content":{"name":"Felix"}}),
        )]));
    let CallToolResponse::Complete(completed) = client.call_tool_once(request).await.unwrap()
    else {
        panic!("expected final result")
    };
    assert_eq!(completed.content[0].as_text().unwrap().text, "Felix");
    client.cancel().await.unwrap();
    task.await.unwrap();
}

#[derive(Deserialize, JsonSchema)]
#[allow(dead_code)]
struct Output {
    name: String,
}
#[tokio::test]
async fn task_panics_invalid_outputs_and_expiry_have_observable_failed_states() {
    let tasks = TaskManager::new();
    let mut server = Server::new("task-failures", "1").with_task_manager(tasks.clone());
    server
        .tool("panic")
        .task_handler(TaskOptions::default(), |_, _, _: Input| async {
            panic!("private task detail");
            #[allow(unreachable_code)]
            Ok(CallToolResult::success(vec![]))
        })
        .unwrap();
    server
        .tool("invalid")
        .output_schema::<Output>()
        .unwrap()
        .task_handler(TaskOptions::default(), |_, _, _: Input| async {
            let mut result = CallToolResult::success(vec![]);
            result.structured_content = Some(json!({"wrong":true}));
            Ok(result)
        })
        .unwrap();
    server
        .tool("expire")
        .task_handler(
            TaskOptions::default().with_ttl_ms(Some(1_u64)),
            |_, _, _: Input| async { std::future::pending().await },
        )
        .unwrap();
    let (client, task) = modern(server).await;
    for name in ["panic", "invalid", "expire"] {
        let CallToolResponse::Task(created) = client.call_tool_once(request(name)).await.unwrap()
        else {
            panic!("expected task")
        };
        let failed = until(&client, &created.task.task_id, TaskStatus::Failed).await;
        assert!(
            !serde_json::to_string(&failed)
                .unwrap()
                .contains("private task detail")
        );
    }
    client.cancel().await.unwrap();
    task.await.unwrap();
    tasks.shutdown();
}

#[tokio::test]
async fn task_handlers_use_the_manager_from_the_active_configuration() {
    let old = TaskManager::new();
    let current = TaskManager::new();
    let mut server = Server::new("managers", "1").with_task_manager(old.clone());
    server
        .tool("work")
        .task_handler(TaskOptions::default(), |_, _, _: Input| async {
            Ok(CallToolResult::success(vec![ContentBlock::text("current")]))
        })
        .unwrap();
    let server = server.with_task_manager(current.clone());
    let (client, task) = modern(server).await;
    let CallToolResponse::Task(created) = client.call_tool_once(request("work")).await.unwrap()
    else {
        panic!("expected task")
    };
    until(&client, &created.task.task_id, TaskStatus::Completed).await;
    assert!(old.get_task(&created.task.task_id).is_err());
    client.cancel().await.unwrap();
    task.await.unwrap();
    old.shutdown();
    current.shutdown();
}

#[tokio::test]
async fn task_updates_ignore_unknown_keys_but_reject_unknown_task_ids() {
    let tasks = TaskManager::new();
    let mut server = Server::new("task-updates", "1").with_task_manager(tasks.clone());
    server
        .tool("input")
        .task_handler(TaskOptions::default(), |_, task, _: Input| async move {
            let input: InputRequest = serde_json::from_value(json!({
                "method": "elicitation/create",
                "params": {"mode": "form", "message": "Name?", "requestedSchema": {"type": "object", "properties": {"name": {"type": "string"}}, "required": ["name"]}}
            }))
            .unwrap();
            let answer = task.request_input("known", input).await?;
            Ok(CallToolResult::success(vec![ContentBlock::text(
                answer["content"]["name"].as_str().unwrap_or_default(),
            )]))
        })
        .unwrap();

    let (client, server_task) = modern(server).await;
    let CallToolResponse::Task(created) = client.call_tool_once(request("input")).await.unwrap()
    else {
        panic!("expected task handle")
    };
    until(&client, &created.task.task_id, TaskStatus::InputRequired).await;

    // Empty and unknown-only updates are acknowledged without answering the
    // outstanding request.
    client
        .update_task(UpdateTaskParams::new(
            &created.task.task_id,
            BTreeMap::new(),
        ))
        .await
        .unwrap();
    client
        .update_task(UpdateTaskParams::new(
            &created.task.task_id,
            BTreeMap::from([("unknown".into(), json!({"action":"accept"}))]),
        ))
        .await
        .unwrap();
    let still_waiting = client
        .get_task(GetTaskParams::new(&created.task.task_id))
        .await
        .unwrap();
    assert_eq!(still_waiting.task.status(), TaskStatus::InputRequired);
    assert!(
        serde_json::to_value(still_waiting).unwrap()["inputRequests"]
            .get("known")
            .is_some()
    );

    client
        .update_task(UpdateTaskParams::new(
            &created.task.task_id,
            BTreeMap::from([
                (
                    "unknown".into(),
                    json!({"action":"accept","content":{"name":"ignored"}}),
                ),
                (
                    "known".into(),
                    json!({"action":"accept","content":{"name":"Ada"}}),
                ),
            ]),
        ))
        .await
        .unwrap();
    let completed = until(&client, &created.task.task_id, TaskStatus::Completed).await;
    assert_eq!(
        serde_json::to_value(completed).unwrap()["result"]["content"][0]["text"],
        "Ada"
    );

    assert_invalid_params(
        client
            .update_task(UpdateTaskParams::new("missing", BTreeMap::new()))
            .await
            .unwrap_err(),
    );
    assert_invalid_params(
        client
            .cancel_task(CancelTaskParams::new("missing"))
            .await
            .unwrap_err(),
    );
    client.cancel().await.unwrap();
    server_task.await.unwrap();
    tasks.shutdown();
}

#[tokio::test]
async fn elicitation_decline_and_cancel_responses_reach_task_and_terminal_updates_are_noops() {
    let tasks = TaskManager::new();
    let mut server = Server::new("task-input-responses", "1").with_task_manager(tasks.clone());
    server
        .tool("input")
        .task_handler(TaskOptions::default(), |_, task, _: Input| async move {
            let input: InputRequest = serde_json::from_value(json!({
                "method": "elicitation/create",
                "params": {"mode": "form", "message": "Continue?", "requestedSchema": {"type": "object", "properties": {"confirm": {"type": "boolean"}}, "required": ["confirm"]}}
            }))
            .unwrap();
            let answer = task.request_input("choice", input).await?;
            Ok(CallToolResult::success(vec![ContentBlock::text(
                answer["action"].as_str().unwrap_or("missing"),
            )]))
        })
        .unwrap();
    let (client, server_task) = modern(server).await;

    for action in ["decline", "cancel"] {
        let CallToolResponse::Task(created) =
            client.call_tool_once(request("input")).await.unwrap()
        else {
            panic!("expected task handle")
        };
        until(&client, &created.task.task_id, TaskStatus::InputRequired).await;
        client
            .update_task(UpdateTaskParams::new(
                &created.task.task_id,
                BTreeMap::from([("choice".into(), json!({"action": action}))]),
            ))
            .await
            .unwrap();
        let completed = until(&client, &created.task.task_id, TaskStatus::Completed).await;
        assert_eq!(
            serde_json::to_value(completed).unwrap()["result"]["content"][0]["text"],
            action
        );

        // The SDK acknowledges cancellation on an already terminal task,
        // while preserving its completed result.
        client
            .cancel_task(CancelTaskParams::new(&created.task.task_id))
            .await
            .unwrap();
        assert_eq!(
            client
                .get_task(GetTaskParams::new(&created.task.task_id))
                .await
                .unwrap()
                .task
                .status(),
            TaskStatus::Completed
        );
        // A late response for a terminal task is accepted and has no effect.
        client
            .update_task(UpdateTaskParams::new(
                &created.task.task_id,
                BTreeMap::from([("choice".into(), json!({"action":"accept"}))]),
            ))
            .await
            .unwrap();
        let after_late_update = client
            .get_task(GetTaskParams::new(&created.task.task_id))
            .await
            .unwrap();
        assert_eq!(after_late_update.task.status(), TaskStatus::Completed);
        assert_eq!(
            serde_json::to_value(after_late_update).unwrap()["result"]["content"][0]["text"],
            action
        );
    }

    client.cancel().await.unwrap();
    server_task.await.unwrap();
    tasks.shutdown();
}

#[tokio::test]
async fn terminal_task_is_retained_for_a_ttl_window_then_evicted_on_get() {
    let tasks = TaskManager::new();
    let mut server = Server::new("task-retention", "1").with_task_manager(tasks.clone());
    server
        .tool("quick")
        .task_handler(
            TaskOptions::default().with_ttl_ms(Some(500)),
            |_, _, _: Input| async {
                Ok(CallToolResult::success(vec![ContentBlock::text(
                    "retained",
                )]))
            },
        )
        .unwrap();
    let (client, server_task) = modern(server).await;
    let CallToolResponse::Task(created) = client.call_tool_once(request("quick")).await.unwrap()
    else {
        panic!("expected task handle")
    };
    until(&client, &created.task.task_id, TaskStatus::Completed).await;
    assert_eq!(
        client
            .get_task(GetTaskParams::new(&created.task.task_id))
            .await
            .unwrap()
            .task
            .status(),
        TaskStatus::Completed
    );
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            match client
                .get_task(GetTaskParams::new(&created.task.task_id))
                .await
            {
                Ok(_) => tokio::time::sleep(Duration::from_millis(10)).await,
                Err(error) => {
                    assert_invalid_params(error);
                    break;
                }
            }
        }
    })
    .await
    .expect("terminal task should be evicted after its TTL retention window");
    client.cancel().await.unwrap();
    server_task.await.unwrap();
    tasks.shutdown();
}

#[tokio::test]
async fn malformed_task_requests_report_invalid_params_on_static_and_live_servers() {
    for live in [false, true] {
        let tasks = TaskManager::new();
        let server = Server::new("malformed-tasks", "1").with_task_manager(tasks.clone());
        let (client, running) = if live {
            modern(server.into_live()).await
        } else {
            modern(server).await
        };
        for method in ["tasks/get", "tasks/cancel", "tasks/update"] {
            for params in [json!({}), json!({"taskId":42}), json!({"taskId":null})] {
                assert_invalid_params(
                    client
                        .send_request(ClientRequest::CustomRequest(CustomRequest::new(
                            method,
                            Some(params),
                        )))
                        .await
                        .unwrap_err(),
                );
            }
        }
        for responses in [
            serde_json::Value::Null,
            json!([]),
            json!(42),
            json!(true),
            json!("invalid"),
        ] {
            assert_invalid_params(
                client
                    .send_request(ClientRequest::CustomRequest(CustomRequest::new(
                        "tasks/update",
                        Some(json!({"taskId":"missing", "inputResponses":responses})),
                    )))
                    .await
                    .unwrap_err(),
            );
        }
        for method in ["tasks/list", "tasks/result", "unknown/method"] {
            let error = client
                .send_request(ClientRequest::CustomRequest(CustomRequest::new(
                    method,
                    Some(json!({})),
                )))
                .await
                .unwrap_err();
            let mcp_rs::protocol::service::ServiceError::McpError(error) = error else {
                panic!("expected method-not-found");
            };
            assert_eq!(error.code, ErrorCode::METHOD_NOT_FOUND);
        }
        client.cancel().await.unwrap();
        running.await.unwrap();
        tasks.shutdown();
    }
    // Without the extension, even the malformed task method remains unavailable.
    let (client, running) = modern(Server::new("no-tasks", "1")).await;
    let error = client
        .send_request(ClientRequest::CustomRequest(CustomRequest::new(
            "tasks/update",
            Some(json!({})),
        )))
        .await
        .unwrap_err();
    let mcp_rs::protocol::service::ServiceError::McpError(error) = error else {
        panic!("expected method-not-found");
    };
    assert_eq!(error.code, ErrorCode::METHOD_NOT_FOUND);
    client.cancel().await.unwrap();
    running.await.unwrap();
}

// Real elapsed time is intentional: the SDK uses std::time::Instant for TTLs.
async fn retention_lifecycle(live: bool) {
    use std::sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    };
    struct Dropped(Arc<AtomicBool>);
    impl Drop for Dropped {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    let tasks = TaskManager::new();
    let mut server = Server::new("retention-lifecycle", "1").with_task_manager(tasks.clone());
    let options = TaskOptions::default().with_ttl_ms(Some(1000));
    for name in ["completed", "unlimited"] {
        server
            .tool(name)
            .task_handler(
                if name == "unlimited" {
                    TaskOptions::default().with_ttl_ms(None)
                } else {
                    options.clone()
                },
                |_, _, _: Input| async {
                    Ok(CallToolResult::success(vec![ContentBlock::text(
                        "retained result",
                    )]))
                },
            )
            .unwrap();
    }
    server
        .tool("failed")
        .task_handler(options.clone(), |_, _, _: Input| async {
            Err(
                ErrorData::internal_error("retained failure", Some(json!({"reason": "test"})))
                    .into(),
            )
        })
        .unwrap();
    let mut dropped = BTreeMap::new();
    for name in ["cancelled", "expired"] {
        let flag = Arc::new(AtomicBool::new(false));
        dropped.insert(name, flag.clone());
        server.tool(name).task_handler(options.clone(), move |_, task, _: Input| {
            let guard = Dropped(flag.clone());
            async move {
                let _guard = guard;
                let input = serde_json::from_value(json!({"method":"elicitation/create","params":{
                    "mode":"form","message":"Pending input","requestedSchema":{"type":"object","properties":{}}
                }})).unwrap();
                task.request_input("pending", input).await?;
                std::future::pending().await
            }
        }).unwrap();
    }
    let (client, running) = if live {
        modern(server.into_live()).await
    } else {
        modern(server).await
    };
    let mut ids = BTreeMap::new();
    for name in ["completed", "failed", "cancelled", "expired", "unlimited"] {
        let CallToolResponse::Task(created) = client.call_tool_once(request(name)).await.unwrap()
        else {
            panic!("expected task handle");
        };
        ids.insert(name, created.task.task_id);
    }
    for name in ["cancelled", "expired"] {
        let pending = until(&client, &ids[name], TaskStatus::InputRequired).await;
        assert!(serde_json::to_value(pending).unwrap()["inputRequests"]["pending"].is_object());
    }
    client
        .cancel_task(CancelTaskParams::new(&ids["cancelled"]))
        .await
        .unwrap();
    let mut retained = BTreeMap::new();
    for (name, status) in [
        ("completed", TaskStatus::Completed),
        ("failed", TaskStatus::Failed),
        ("cancelled", TaskStatus::Cancelled),
        ("unlimited", TaskStatus::Completed),
    ] {
        let value = serde_json::to_value(until(&client, &ids[name], status).await).unwrap();
        assert!(value.get("inputRequests").is_none());
        retained.insert(name, value);
    }
    assert_eq!(
        retained["completed"]["result"]["content"][0]["text"],
        "retained result"
    );
    assert_eq!(retained["failed"]["error"]["message"], "retained failure");
    assert_eq!(retained["failed"]["error"]["data"]["reason"], "test");
    assert!(retained["cancelled"].get("result").is_none());
    assert!(retained["cancelled"].get("error").is_none());
    assert!(dropped["cancelled"].load(Ordering::SeqCst));
    // Polling retained tasks must preserve their complete terminal payload.
    for (name, expected) in &retained {
        let actual = client
            .get_task(GetTaskParams::new(&ids[name]))
            .await
            .unwrap();
        assert_eq!(serde_json::to_value(actual).unwrap(), *expected);
    }
    tokio::time::sleep(Duration::from_millis(1100)).await;
    let expired = until(&client, &ids["expired"], TaskStatus::Failed).await;
    let expired = serde_json::to_value(expired).unwrap();
    assert!(
        expired["error"]["message"]
            .as_str()
            .unwrap()
            .contains("TTL elapsed")
    );
    assert!(expired.get("inputRequests").is_none());
    tokio::time::timeout(Duration::from_secs(1), async {
        while !dropped["expired"].load(Ordering::SeqCst) {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("expiry must drop the pending operation");
    for name in ["completed", "failed", "cancelled"] {
        assert_invalid_params(
            client
                .get_task(GetTaskParams::new(&ids[name]))
                .await
                .unwrap_err(),
        );
        assert_invalid_params(
            client
                .cancel_task(CancelTaskParams::new(&ids[name]))
                .await
                .unwrap_err(),
        );
        assert_invalid_params(
            client
                .update_task(UpdateTaskParams::new(&ids[name], BTreeMap::new()))
                .await
                .unwrap_err(),
        );
    }
    // Expiry starts a fresh retention window; it is not deleted with older terminals.
    assert_eq!(
        serde_json::to_value(
            client
                .get_task(GetTaskParams::new(&ids["expired"]))
                .await
                .unwrap()
        )
        .unwrap(),
        expired
    );
    tokio::time::sleep(Duration::from_millis(1100)).await;
    assert_invalid_params(
        client
            .get_task(GetTaskParams::new(&ids["expired"]))
            .await
            .unwrap_err(),
    );
    assert_eq!(
        serde_json::to_value(
            client
                .get_task(GetTaskParams::new(&ids["unlimited"]))
                .await
                .unwrap()
        )
        .unwrap(),
        retained["unlimited"]
    );
    assert_eq!(tasks.running_task_count(), 0);
    tasks.shutdown();
    assert_invalid_params(
        client
            .get_task(GetTaskParams::new(&ids["unlimited"]))
            .await
            .unwrap_err(),
    );
    client.cancel().await.unwrap();
    running.await.unwrap();
}

#[tokio::test]
async fn static_task_retention_preserves_terminal_payloads_and_cleans_expired_work() {
    retention_lifecycle(false).await;
}

#[tokio::test]
async fn live_task_retention_preserves_terminal_payloads_and_cleans_expired_work() {
    retention_lifecycle(true).await;
}
