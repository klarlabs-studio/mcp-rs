//! Modern prompt/resource multi-round requests through builders and middleware.
#![allow(clippy::unwrap_used)]
use futures_util::FutureExt;
use mcp_rs::{
    Response, Server, Session,
    model::*,
    protocol::{
        self, ClientHandler, ClientLifecycleMode, ClientServiceExt, ServerHandler, ServiceExt,
    },
};
use serde_json::json;
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex},
    time::Duration,
};

async fn connect<H: ServerHandler, C: ClientHandler>(
    server: H,
    client: C,
) -> (
    protocol::service::RunningService<protocol::RoleClient, C>,
    tokio::task::JoinHandle<()>,
) {
    let (client_io, server_io) = tokio::io::duplex(65536);
    let task = tokio::spawn(async move {
        server
            .serve(server_io)
            .await
            .unwrap()
            .waiting()
            .await
            .unwrap();
    });
    let client = tokio::time::timeout(
        Duration::from_secs(3),
        client.serve_with_lifecycle(
            client_io,
            ClientLifecycleMode::Discover {
                preferred_versions: vec![ProtocolVersion::V_2026_07_28],
            },
        ),
    )
    .await
    .unwrap()
    .unwrap();
    (client, task)
}
fn config() -> ClientConfig {
    ClientConfig::new(
        ClientCapabilities::builder().enable_elicitation().build(),
        Implementation::new("mrtr-client", "1"),
    )
}
fn input() -> InputRequiredResult {
    let request: InputRequest = serde_json::from_value(json!({"method":"elicitation/create","params":{"mode":"form","message":"Name?","requestedSchema":{"type":"object","properties":{"name":{"type":"string"}},"required":["name"]}}})).unwrap();
    InputRequiredResult::new(
        Some(BTreeMap::from([("answer".into(), request)])),
        Some("opaque-state".into()),
    )
}
fn responses() -> InputResponses {
    BTreeMap::from([(
        "answer".into(),
        json!({"action":"accept","content":{"name":"Ada"}}),
    )])
}
fn answer(state: &Option<String>, responses: &Option<InputResponses>) -> Option<String> {
    responses.as_ref().map(|responses| {
        assert_eq!(state.as_deref(), Some("opaque-state"));
        responses["answer"]["content"]["name"]
            .as_str()
            .unwrap()
            .to_owned()
    })
}
fn app(seen: Arc<Mutex<Vec<&'static str>>>) -> Server {
    let mut server = Server::new("mrtr", "1")
        .with_result_cache(123, CacheScope::Public)
        .with_middleware(Arc::new(move |ctx, args, next| {
            let seen = seen.clone();
            async move {
                let response = next(ctx, args).await?;
                seen.lock().unwrap().push(match &response {
                    Response::ResourceProtocol(_) => "resource-input",
                    Response::PromptProtocol(_) => "prompt-input",
                    Response::Resource(_) => "resource-complete",
                    Response::Prompt(_) => "prompt-complete",
                    _ => panic!("unexpected response"),
                });
                Ok(response)
            }
            .boxed()
        }));
    server
        .resource("person://{id}")
        .protocol_handler(|ctx, uri, params| async move {
            assert_eq!(params["id"], "raw%20id");
            let session = Session::from_context(&ctx);
            let request = session.resource_request().unwrap();
            assert_eq!(request.uri, uri);
            assert!(session.prompt_request().is_none());
            match answer(&request.request_state, &request.input_responses) {
                None => Ok(input().into()),
                Some(name) => {
                    let mut result =
                        ReadResourceResult::new(vec![ResourceContents::text(name, uri)]);
                    result.ttl_ms = Some(77);
                    Ok(result.into())
                }
            }
        })
        .unwrap();
    server
        .prompt("greet")
        .argument("topic", "Topic", true)
        .protocol_handler(|ctx, args| async move {
            assert_eq!(args["topic"], "Rust");
            let session = Session::from_context(&ctx);
            let request = session.prompt_request().unwrap();
            assert_eq!(request.name, "greet");
            assert!(session.resource_request().is_none());
            match answer(&request.request_state, &request.input_responses) {
                None => Ok(input().into()),
                Some(name) => Ok(GetPromptResult::new(vec![]).with_description(name).into()),
            }
        })
        .unwrap();
    server
}
fn prompt() -> GetPromptRequestParams {
    GetPromptRequestParams::new("greet")
        .with_arguments(serde_json::from_value(json!({"topic":"Rust"})).unwrap())
}
async fn round_trips<H: ServerHandler>(server: H, seen: Arc<Mutex<Vec<&'static str>>>) {
    let (client, task) = connect(server, config()).await;
    let request = ReadResourceRequestParams::new("person://raw%20id");
    let ReadResourceResponse::InputRequired(first) =
        client.read_resource_once(request.clone()).await.unwrap()
    else {
        panic!("expected input")
    };
    let wire = serde_json::to_value(&first).unwrap();
    assert!(wire.get("ttlMs").is_none());
    assert!(wire.get("cacheScope").is_none());
    assert_eq!(first.request_state.as_deref(), Some("opaque-state"));
    let ReadResourceResponse::Complete(result) = client
        .read_resource_once(
            request
                .with_request_state(first.request_state.unwrap())
                .with_input_responses(responses()),
        )
        .await
        .unwrap()
    else {
        panic!("expected complete")
    };
    assert_eq!(result.ttl_ms, Some(77));
    assert_eq!(result.cache_scope, Some(CacheScope::Public));
    assert_eq!(
        serde_json::to_value(result).unwrap()["contents"][0]["text"],
        "Ada"
    );
    let GetPromptResponse::InputRequired(first) = client.get_prompt_once(prompt()).await.unwrap()
    else {
        panic!("expected input")
    };
    let GetPromptResponse::Complete(result) = client
        .get_prompt_once(
            prompt()
                .with_request_state(first.request_state.unwrap())
                .with_input_responses(responses()),
        )
        .await
        .unwrap()
    else {
        panic!("expected complete")
    };
    assert_eq!(result.description.as_deref(), Some("Ada"));
    assert_eq!(
        *seen.lock().unwrap(),
        [
            "resource-input",
            "resource-complete",
            "prompt-input",
            "prompt-complete"
        ]
    );
    client.cancel().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), task)
        .await
        .unwrap()
        .unwrap();
}
#[tokio::test]
async fn static_round_trips_preserve_context_templates_middleware_and_cache() {
    let seen = Arc::new(Mutex::new(vec![]));
    round_trips(app(seen.clone()), seen).await;
}
#[tokio::test]
async fn live_round_trips_preserve_context_templates_middleware_and_cache() {
    let seen = Arc::new(Mutex::new(vec![]));
    round_trips(app(seen.clone()).into_live(), seen).await;
}
struct Accept;
impl ClientHandler for Accept {
    fn get_info(&self) -> ClientConfig {
        config()
    }
    async fn create_elicitation(
        &self,
        _: ElicitRequestParams,
        _: protocol::service::RequestContext<protocol::RoleClient>,
    ) -> Result<ElicitResult, ErrorData> {
        Ok(ElicitResult::new(ElicitationAction::Accept).with_content(json!({"name":"Ada"})))
    }
}
#[tokio::test]
async fn sdk_automatic_loop_fulfills_both_methods() {
    let seen = Arc::new(Mutex::new(vec![]));
    let (client, task) = connect(app(seen.clone()), Accept).await;
    let result = tokio::time::timeout(
        Duration::from_secs(3),
        client.read_resource(ReadResourceRequestParams::new("person://raw%20id")),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(result.ttl_ms, Some(77));
    let result = tokio::time::timeout(Duration::from_secs(3), client.get_prompt(prompt()))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result.description.as_deref(), Some("Ada"));
    assert_eq!(seen.lock().unwrap().len(), 4);
    client.cancel().await.unwrap();
    task.await.unwrap();
}
#[tokio::test]
async fn retries_recheck_visibility_and_required_arguments() {
    let seen = Arc::new(Mutex::new(vec![]));
    let live = app(seen.clone()).into_live();
    let (client, task) = connect(live.clone(), config()).await;
    client
        .read_resource_once(ReadResourceRequestParams::new("person://raw%20id"))
        .await
        .unwrap();
    client.get_prompt_once(prompt()).await.unwrap();
    let error = client
        .get_prompt_once(
            GetPromptRequestParams::new("greet")
                .with_request_state("opaque-state")
                .with_input_responses(responses()),
        )
        .await
        .unwrap_err();
    assert!(
        matches!(error, protocol::service::ServiceError::McpError(error) if error.code == ErrorCode::INVALID_PARAMS)
    );
    live.replace(live.snapshot().with_visibility_policy(|_, _| false));
    assert!(
        client
            .get_prompt_once(
                prompt()
                    .with_request_state("opaque-state")
                    .with_input_responses(responses())
            )
            .await
            .is_err()
    );
    assert!(
        client
            .read_resource_once(
                ReadResourceRequestParams::new("person://raw%20id")
                    .with_request_state("opaque-state")
                    .with_input_responses(responses())
            )
            .await
            .is_err()
    );
    assert_eq!(*seen.lock().unwrap(), ["resource-input", "prompt-input"]);
    client.cancel().await.unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn middleware_can_finish_an_input_round_without_bypassing_cache_policy() {
    let mut server = Server::new("middleware", "1")
        .with_result_cache(42, CacheScope::Private)
        .with_middleware(Arc::new(|ctx, args, next| {
            async move {
                match next(ctx, args).await? {
                    Response::ResourceProtocol(_) => {
                        let mut result = ReadResourceResult::new(vec![ResourceContents::text(
                            "from middleware",
                            "about://input",
                        )]);
                        result.meta =
                            Some(serde_json::from_value(json!({"source":"middleware"})).unwrap());
                        Ok(Response::ResourceProtocol(Box::new(result.into())))
                    }
                    Response::PromptProtocol(_) => {
                        Err(ErrorData::invalid_params("Input denied", None))
                    }
                    _ => panic!("expected intermediate response"),
                }
            }
            .boxed()
        }));
    server
        .resource("about://input")
        .protocol_handler(|_, _, _| async { Ok(input().into()) })
        .unwrap();
    server
        .prompt("input")
        .protocol_handler(|_, _| async { Ok(input().into()) })
        .unwrap();
    let (client, task) = connect(server, config()).await;
    let ReadResourceResponse::Complete(result) = client
        .read_resource_once(ReadResourceRequestParams::new("about://input"))
        .await
        .unwrap()
    else {
        panic!("expected middleware result")
    };
    assert_eq!(result.ttl_ms, Some(42));
    assert_eq!(result.cache_scope, Some(CacheScope::Private));
    assert_eq!(result.meta.unwrap()["source"], "middleware");
    let error = client
        .get_prompt_once(GetPromptRequestParams::new("input"))
        .await
        .unwrap_err();
    assert!(
        matches!(error, protocol::service::ServiceError::McpError(error) if error.code == ErrorCode::INVALID_PARAMS && error.message == "Input denied")
    );
    client.cancel().await.unwrap();
    task.await.unwrap();
}

#[tokio::test]
async fn malformed_retry_parameters_are_invalid_params_on_static_and_live_servers() {
    for live in [false, true] {
        let seen = Arc::new(Mutex::new(vec![]));
        let server = app(seen.clone());
        let (client, task) = if live {
            connect(server.into_live(), config()).await
        } else {
            connect(server, config()).await
        };
        for (method, base) in [
            ("resources/read", json!({"uri":"person://raw%20id"})),
            (
                "prompts/get",
                json!({"name":"greet","arguments":{"topic":"Rust"}}),
            ),
        ] {
            let mut cases = vec![json!({}), json!({"uri":42,"name":42})];
            for invalid in [json!("invalid"), json!(42), json!(true), json!([])] {
                let mut params = base.clone();
                params["inputResponses"] = invalid;
                cases.push(params);
            }
            for invalid in [json!(42), json!(true), json!({}), json!([])] {
                let mut params = base.clone();
                params["requestState"] = invalid;
                cases.push(params);
            }
            for params in cases {
                let error = client
                    .send_request(ClientRequest::CustomRequest(CustomRequest::new(
                        method,
                        Some(params),
                    )))
                    .await
                    .unwrap_err();
                let protocol::service::ServiceError::McpError(error) = error else {
                    panic!("expected MCP error")
                };
                assert_eq!(error.code, ErrorCode::INVALID_PARAMS);
                assert!(error.data.is_none());
            }
        }
        assert!(seen.lock().unwrap().is_empty());
        client.cancel().await.unwrap();
        task.await.unwrap();
    }
}

#[cfg(feature = "client")]
mod typed_client {
    use super::*;
    use mcp_rs::{Client, ClientError, MrtrLimits};
    use protocol::service::ServiceError;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingAccept(Arc<AtomicUsize>);
    impl ClientHandler for CountingAccept {
        fn get_info(&self) -> ClientConfig {
            config()
        }
        async fn create_elicitation(
            &self,
            params: ElicitRequestParams,
            context: protocol::service::RequestContext<protocol::RoleClient>,
        ) -> Result<ElicitResult, ErrorData> {
            self.0.fetch_add(1, Ordering::SeqCst);
            Accept.create_elicitation(params, context).await
        }
    }

    #[tokio::test]
    async fn automatic_prompt_and_resource_helpers_preserve_static_and_live_results() {
        for live in [false, true] {
            let seen = Arc::new(Mutex::new(vec![]));
            let (running, task) = if live {
                connect(app(seen.clone()).into_live(), Accept).await
            } else {
                connect(app(seen.clone()), Accept).await
            };
            let client = Client::from_service(running).unwrap();
            let resource = ReadResourceRequestParams::new("person://raw%20id");
            let result = client.read_resource(resource.clone()).await.unwrap();
            assert_eq!(result.ttl_ms, Some(77));
            assert_eq!(result.cache_scope, Some(CacheScope::Public));
            assert_eq!(
                serde_json::to_value(result).unwrap()["contents"][0]["text"],
                "Ada"
            );
            assert_eq!(
                client
                    .get_prompt(prompt())
                    .await
                    .unwrap()
                    .description
                    .as_deref(),
                Some("Ada")
            );

            let limits = MrtrLimits::default()
                .with_max_rounds(2)
                .with_max_inputs_per_round(1);
            let result = client
                .read_resource_with_mrtr_limits(resource, limits)
                .await
                .unwrap();
            assert_eq!(result.ttl_ms, Some(77));
            assert_eq!(result.cache_scope, Some(CacheScope::Public));
            assert_eq!(
                client
                    .get_prompt_with_mrtr_limits(prompt(), limits)
                    .await
                    .unwrap()
                    .description
                    .as_deref(),
                Some("Ada")
            );
            // Includes both MRTR stages, template/argument assertions and middleware dispatch.
            let seen = seen.lock().unwrap().clone();
            assert!(seen.contains(&"resource-input") && seen.contains(&"resource-complete"));
            assert!(seen.contains(&"prompt-input") && seen.contains(&"prompt-complete"));
            client.close().await.unwrap();
            task.await.unwrap();
        }
    }

    #[tokio::test]
    async fn explicit_budgets_and_single_round_helpers_do_not_invoke_callbacks() {
        let seen = Arc::new(Mutex::new(vec![]));
        let callbacks = Arc::new(AtomicUsize::new(0));
        let (running, task) = connect(app(seen.clone()), CountingAccept(callbacks.clone())).await;
        let client = Client::from_service(running).unwrap();
        let resource = ReadResourceRequestParams::new("person://raw%20id");
        let zero = MrtrLimits::default().with_max_rounds(0);
        let errors = [
            client
                .get_prompt_with_mrtr_limits(prompt(), zero)
                .await
                .unwrap_err(),
            client
                .read_resource_with_mrtr_limits(resource.clone(), zero)
                .await
                .unwrap_err(),
        ];
        for error in errors {
            assert!(
                matches!(error, ClientError::Service(error) if matches!(*error, ServiceError::InputRequiredRoundsExceeded { max_rounds: 0 }))
            );
        }
        assert!(seen.lock().unwrap().is_empty());
        let GetPromptResponse::InputRequired(prompt_input) =
            client.get_prompt_once(prompt()).await.unwrap()
        else {
            panic!("expected input")
        };
        let ReadResourceResponse::InputRequired(resource_input) =
            client.read_resource_once(resource.clone()).await.unwrap()
        else {
            panic!("expected input")
        };
        assert_eq!(prompt_input.request_state.as_deref(), Some("opaque-state"));
        assert_eq!(
            resource_input.request_state.as_deref(),
            Some("opaque-state")
        );

        let no_inputs = MrtrLimits::default().with_max_inputs_per_round(0);
        let errors = [
            client
                .get_prompt_with_mrtr_limits(prompt(), no_inputs)
                .await
                .unwrap_err(),
            client
                .read_resource_with_mrtr_limits(resource.clone(), no_inputs)
                .await
                .unwrap_err(),
        ];
        for error in errors {
            assert!(
                matches!(error, ClientError::Service(error) if matches!(*error, ServiceError::InputRequiredRequestsExceeded { limit: 0 }))
            );
        }
        let final_round = MrtrLimits::default().with_max_rounds(1);
        let errors = [
            client
                .get_prompt_with_mrtr_limits(prompt(), final_round)
                .await
                .unwrap_err(),
            client
                .read_resource_with_mrtr_limits(resource.clone(), final_round)
                .await
                .unwrap_err(),
        ];
        for error in errors {
            assert!(
                matches!(error, ClientError::Service(error) if matches!(*error, ServiceError::InputRequiredRoundsExceeded { max_rounds: 1 }))
            );
        }
        assert_eq!(callbacks.load(Ordering::SeqCst), 0);
        // Hosts can explicitly resume both single-round response types without callbacks.
        assert!(matches!(
            client
                .get_prompt_once(
                    prompt()
                        .with_request_state(prompt_input.request_state.unwrap())
                        .with_input_responses(responses())
                )
                .await
                .unwrap(),
            GetPromptResponse::Complete(_)
        ));
        assert!(matches!(
            client
                .read_resource_once(
                    resource
                        .with_request_state(resource_input.request_state.unwrap())
                        .with_input_responses(responses())
                )
                .await
                .unwrap(),
            ReadResourceResponse::Complete(_)
        ));
        assert_eq!(callbacks.load(Ordering::SeqCst), 0);
        client.close().await.unwrap();
        task.await.unwrap();
    }

    #[tokio::test]
    async fn skill_helpers_drive_resource_inputs_and_preserve_native_metadata() {
        let mut server = Server::new("skill-mrtr", "1");
        for uri in ["skill://index.json", "skill://demo/SKILL.md"] {
            server
                .resource(uri)
                .protocol_handler(|ctx, uri, _| async move {
                    let session = Session::from_context(&ctx);
                    let request = session.resource_request().unwrap();
                    if answer(&request.request_state, &request.input_responses).is_none() {
                        return Ok(input().into());
                    }
                    let text = if uri == "skill://index.json" {
                        json!({"$schema": mcp_rs::skills::SKILL_INDEX_SCHEMA, "skills": []})
                            .to_string()
                    } else {
                        "# Demo\n\nSkill content after consent.".into()
                    };
                    let mut result =
                        ReadResourceResult::new(vec![ResourceContents::text(text, uri)]);
                    result.meta =
                        Some(serde_json::from_value(json!({"source": "skill-mrtr"})).unwrap());
                    result.ttl_ms = Some(37);
                    result.cache_scope = Some(CacheScope::Private);
                    Ok(result.into())
                })
                .unwrap();
        }
        let callbacks = Arc::new(AtomicUsize::new(0));
        let (running, task) = connect(server, CountingAccept(callbacks.clone())).await;
        let client = Client::from_service(running).unwrap();
        assert!(client.list_skills().await.unwrap().is_empty());
        let result = client
            .read_skill_uri("skill://demo/SKILL.md")
            .await
            .unwrap();
        assert_eq!(result.meta.as_ref().unwrap()["source"], "skill-mrtr");
        assert_eq!(result.ttl_ms, Some(37));
        assert_eq!(result.cache_scope, Some(CacheScope::Private));
        let content = serde_json::to_value(result).unwrap();
        assert_eq!(content["contents"][0]["uri"], "skill://demo/SKILL.md");
        assert_eq!(
            content["contents"][0]["text"],
            "# Demo\n\nSkill content after consent."
        );
        assert_eq!(callbacks.load(Ordering::SeqCst), 2);
        client.close().await.unwrap();
        task.await.unwrap();
    }
}
