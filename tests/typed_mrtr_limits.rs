//! Per-call MRTR limits reach the owned engine through every typed tool entry point.
#![cfg(feature = "client")]
#![allow(clippy::unwrap_used)]
use mcp_rs::{
    Client, ClientError, MrtrLimits,
    model::*,
    protocol::{
        ClientHandler, RoleClient, RoleServer, ServerHandler, ServiceExt,
        service::{RequestContext, ServiceError},
    },
};
use serde_json::{Value, json};
use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

#[derive(Default)]
struct Counts {
    requests: AtomicUsize,
    callbacks: AtomicUsize,
    active: AtomicUsize,
    peak: AtomicUsize,
}
struct BatchServer(Arc<Counts>);
impl ServerHandler for BatchServer {
    async fn call_tool(
        &self,
        params: CallToolRequestParams,
        _: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        self.0.requests.fetch_add(1, Ordering::SeqCst);
        let arguments = params.arguments.unwrap();
        assert_eq!(arguments["marker"], "original");
        let count = arguments["count"].as_u64().unwrap() as usize;
        if params.request_state.is_none() {
            let input: InputRequest = serde_json::from_value(json!({
                "method": "elicitation/create",
                "params": {"mode": "form", "message": "Answer?", "requestedSchema": {"type": "object", "properties": {}}}
            })).unwrap();
            return Ok(InputRequiredResult::new(
                Some((0..count).map(|n| (n.to_string(), input.clone())).collect()),
                Some("resume".into()),
            )
            .into());
        }
        assert_eq!(params.request_state.as_deref(), Some("resume"));
        let responses = params.input_responses.unwrap();
        assert_eq!(responses.len(), count);
        for n in 0..count {
            assert_eq!(responses[&n.to_string()]["action"], "accept");
        }
        Ok(match params.name.as_ref() {
            "object" => CallToolResult::structured(json!({"answer": 42})),
            "text" => CallToolResult::success(vec![ContentBlock::text("\"quoted\"")]),
            "number" => CallToolResult::success(vec![ContentBlock::text("42")]),
            "fail" => CallToolResult::error(vec![ContentBlock::text("expected failure")]),
            "missing" => CallToolResult::success(vec![]),
            _ => CallToolResult::success(vec![ContentBlock::text("invalid json")]),
        }
        .into())
    }
}
struct Answers(Arc<Counts>);
impl ClientHandler for Answers {
    fn get_info(&self) -> ClientConfig {
        ClientConfig::new(
            ClientCapabilities::builder().enable_elicitation().build(),
            Implementation::new("typed-limits", "1"),
        )
    }
    async fn create_elicitation(
        &self,
        _: ElicitRequestParams,
        _: RequestContext<RoleClient>,
    ) -> Result<ElicitResult, ErrorData> {
        self.0.callbacks.fetch_add(1, Ordering::SeqCst);
        let active = self.0.active.fetch_add(1, Ordering::SeqCst) + 1;
        self.0.peak.fetch_max(active, Ordering::SeqCst);
        tokio::task::yield_now().await;
        self.0.active.fetch_sub(1, Ordering::SeqCst);
        Ok(ElicitResult::new(ElicitationAction::Accept))
    }
}
async fn connect() -> (Client<Answers>, Arc<Counts>, tokio::task::JoinHandle<()>) {
    let counts = Arc::new(Counts::default());
    let (client_io, server_io) = tokio::io::duplex(65536);
    let server_counts = counts.clone();
    let server = tokio::spawn(async move {
        BatchServer(server_counts)
            .serve(server_io)
            .await
            .unwrap()
            .waiting()
            .await
            .unwrap();
    });
    let client =
        Client::from_service(Answers(counts.clone()).serve(client_io).await.unwrap()).unwrap();
    (client, counts, server)
}
fn input(count: usize) -> Value {
    json!({"count": count, "marker": "original"})
}
fn limits(count: usize) -> MrtrLimits {
    MrtrLimits::default()
        .with_max_rounds(2)
        .with_max_inputs_per_round(count)
        .with_max_concurrent_inputs(NonZeroUsize::new(1).unwrap())
}
fn batch_error(error: ClientError, limit: usize) {
    let ClientError::Service(error) = error else {
        panic!("expected service error")
    };
    assert!(
        matches!(*error, ServiceError::InputRequiredRequestsExceeded { limit: actual } if actual == limit)
    );
}
fn round_error(error: ClientError) {
    let ClientError::Service(error) = error else {
        panic!("expected service error")
    };
    assert!(matches!(
        *error,
        ServiceError::InputRequiredRoundsExceeded { .. }
    ));
}

#[tokio::test]
async fn all_typed_entry_points_forward_overrides_without_changing_defaults() {
    let (client, counts, server) = connect().await;
    let handle = client.tool::<Value, Value>("object");
    batch_error(
        client
            .call::<_, Value>("object", &input(65))
            .await
            .unwrap_err(),
        64,
    );
    batch_error(client.call_raw("object", &input(65)).await.unwrap_err(), 64);
    batch_error(handle.call(&input(65)).await.unwrap_err(), 64);
    assert_eq!(counts.callbacks.load(Ordering::SeqCst), 0);

    let output: Value = client
        .call_with_mrtr_limits("object", &input(65), limits(65))
        .await
        .unwrap();
    assert_eq!(output, json!({"answer": 42}));
    let raw = client
        .call_raw_with_mrtr_limits("object", &input(65), limits(65))
        .await
        .unwrap();
    assert_eq!(raw.structured_content, Some(output.clone()));
    assert_eq!(
        handle
            .call_with_mrtr_limits(&input(65), limits(65))
            .await
            .unwrap(),
        output
    );
    assert_eq!(counts.callbacks.load(Ordering::SeqCst), 195);
    assert_eq!(counts.peak.load(Ordering::SeqCst), 1);
    batch_error(handle.call(&input(65)).await.unwrap_err(), 64);
    assert_eq!(counts.callbacks.load(Ordering::SeqCst), 195);
    assert_eq!(counts.requests.load(Ordering::SeqCst), 10);
    client.close().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn typed_overrides_preserve_result_decoding_and_execution_status() {
    let (client, _, server) = connect().await;
    let args = input(2);
    let output: String = client
        .call_with_mrtr_limits("text", &args, limits(2))
        .await
        .unwrap();
    assert_eq!(output, "\"quoted\"");
    let output: u64 = client
        .tool::<Value, u64>("number")
        .call_with_mrtr_limits(&args, limits(2))
        .await
        .unwrap();
    assert_eq!(output, 42);
    assert!(
        matches!(client.call_with_mrtr_limits::<_, Value>("fail", &args, limits(2)).await,
        Err(ClientError::ToolFailure(message)) if message == "expected failure")
    );
    let raw = client
        .call_raw_with_mrtr_limits("fail", &args, limits(2))
        .await
        .unwrap();
    assert_eq!(raw.is_error, Some(true));
    assert_eq!(raw.content[0].as_text().unwrap().text, "expected failure");
    assert!(matches!(
        client
            .call_with_mrtr_limits::<_, Value>("missing", &args, limits(2))
            .await,
        Err(ClientError::MissingOutput)
    ));
    assert!(matches!(
        client
            .call_with_mrtr_limits::<_, Value>("bad", &args, limits(2))
            .await,
        Err(ClientError::Json(_))
    ));
    client.close().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn per_call_policies_are_isolated_and_zero_rounds_and_invalid_inputs_send_nothing() {
    let (client, counts, server) = connect().await;
    let handle = client.tool::<Value, Value>("object");
    let zero = limits(2).with_max_rounds(0);
    round_error(
        client
            .call_with_mrtr_limits::<_, Value>("object", &input(2), zero)
            .await
            .unwrap_err(),
    );
    round_error(
        client
            .call_raw_with_mrtr_limits("object", &input(2), zero)
            .await
            .unwrap_err(),
    );
    round_error(
        handle
            .call_with_mrtr_limits(&input(2), zero)
            .await
            .unwrap_err(),
    );
    assert!(matches!(
        client
            .call_with_mrtr_limits::<_, Value>("object", &42, zero)
            .await,
        Err(ClientError::InvalidInput)
    ));
    assert!(matches!(
        client.call_raw_with_mrtr_limits("object", &42, zero).await,
        Err(ClientError::InvalidInput)
    ));
    assert!(matches!(
        handle.call_with_mrtr_limits(&json!(42), zero).await,
        Err(ClientError::InvalidInput)
    ));
    assert_eq!(counts.requests.load(Ordering::SeqCst), 0);
    assert_eq!(counts.callbacks.load(Ordering::SeqCst), 0);

    let args = input(2);
    let (rejected, accepted) = tokio::join!(
        client.call_raw_with_mrtr_limits("object", &args, limits(1)),
        handle.call_with_mrtr_limits(&args, limits(2)),
    );
    batch_error(rejected.unwrap_err(), 1);
    assert_eq!(accepted.unwrap(), json!({"answer": 42}));
    assert_eq!(counts.requests.load(Ordering::SeqCst), 3);
    assert_eq!(counts.callbacks.load(Ordering::SeqCst), 2);
    round_error(
        handle
            .call_with_mrtr_limits(&args, limits(2).with_max_rounds(1))
            .await
            .unwrap_err(),
    );
    assert_eq!(counts.callbacks.load(Ordering::SeqCst), 2);
    assert!(matches!(
        client.call_once("object", &args).await.unwrap(),
        CallToolResponse::InputRequired(_)
    ));
    assert_eq!(counts.callbacks.load(Ordering::SeqCst), 2);
    client.close().await.unwrap();
    server.await.unwrap();
}
