//! Automatic MRTR limits apply consistently to all three native operations.
#![cfg(feature = "client")]
#![allow(clippy::unwrap_used)]
use mcp_rs::{
    model::*,
    protocol::{
        ClientHandler, RoleClient, RoleServer, ServerHandler, ServiceExt,
        service::{MrtrLimits, RequestContext, ServiceError},
    },
};
use serde_json::json;
use std::{
    num::NonZeroUsize,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

struct BatchServer {
    count: usize,
    requests: Arc<AtomicUsize>,
}
impl BatchServer {
    fn response(
        &self,
        state: Option<String>,
        responses: Option<InputResponses>,
    ) -> Option<InputRequiredResult> {
        self.requests.fetch_add(1, Ordering::SeqCst);
        if let Some(state) = state {
            assert_eq!(state, "resume");
            let responses = responses.unwrap_or_default();
            assert_eq!(responses.len(), self.count);
            for value in responses.values() {
                assert_eq!(value["action"], "accept");
            }
            return None;
        }
        let input: InputRequest = serde_json::from_value(json!({"method":"elicitation/create","params":{"mode":"form","message":"Answer?","requestedSchema":{"type":"object","properties":{}}}})).unwrap();
        Some(InputRequiredResult::new(
            Some(
                (0..self.count)
                    .map(|id| (id.to_string(), input.clone()))
                    .collect(),
            ),
            Some("resume".into()),
        ))
    }
}
impl ServerHandler for BatchServer {
    async fn call_tool(
        &self,
        params: CallToolRequestParams,
        _: RequestContext<RoleServer>,
    ) -> Result<CallToolResponse, ErrorData> {
        Ok(
            match self.response(params.request_state, params.input_responses) {
                Some(required) => CallToolResponse::InputRequired(required),
                None => CallToolResult::success(vec![]).into(),
            },
        )
    }
    async fn get_prompt(
        &self,
        params: GetPromptRequestParams,
        _: RequestContext<RoleServer>,
    ) -> Result<GetPromptResponse, ErrorData> {
        Ok(
            match self.response(params.request_state, params.input_responses) {
                Some(required) => GetPromptResponse::InputRequired(required),
                None => GetPromptResult::new(vec![]).into(),
            },
        )
    }
    async fn read_resource(
        &self,
        params: ReadResourceRequestParams,
        _: RequestContext<RoleServer>,
    ) -> Result<ReadResourceResponse, ErrorData> {
        Ok(
            match self.response(params.request_state, params.input_responses) {
                Some(required) => ReadResourceResponse::InputRequired(required),
                None => ReadResourceResult::new(vec![]).into(),
            },
        )
    }
}
struct Answers(Arc<AtomicUsize>);
impl ClientHandler for Answers {
    fn get_info(&self) -> ClientConfig {
        ClientConfig::new(
            ClientCapabilities::builder().enable_elicitation().build(),
            Implementation::new("limits", "1"),
        )
    }
    async fn create_elicitation(
        &self,
        _: ElicitRequestParams,
        _: RequestContext<RoleClient>,
    ) -> Result<ElicitResult, ErrorData> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(ElicitResult::new(ElicitationAction::Accept))
    }
}

#[tokio::test]
async fn all_operations_reject_before_callbacks_accept_exact_batches_and_skip_zero_rounds() {
    let requests = Arc::new(AtomicUsize::new(0));
    let answers = Arc::new(AtomicUsize::new(0));
    let (client_io, server_io) = tokio::io::duplex(65536);
    let counter = requests.clone();
    let server = tokio::spawn(async move {
        BatchServer {
            count: 2,
            requests: counter,
        }
        .serve(server_io)
        .await
        .unwrap()
        .waiting()
        .await
        .unwrap();
    });
    let client = Answers(answers.clone()).serve(client_io).await.unwrap();
    let limits = MrtrLimits::default()
        .with_max_rounds(2)
        .with_max_inputs_per_round(1)
        .with_max_concurrent_inputs(NonZeroUsize::new(1).unwrap());
    let errors = [
        client
            .call_tool_with_mrtr_limits(CallToolRequestParams::new("batch"), limits)
            .await
            .unwrap_err(),
        client
            .get_prompt_with_mrtr_limits(GetPromptRequestParams::new("batch"), limits)
            .await
            .unwrap_err(),
        client
            .read_resource_with_mrtr_limits(ReadResourceRequestParams::new("batch://value"), limits)
            .await
            .unwrap_err(),
    ];
    assert!(errors.into_iter().all(|error| matches!(
        error,
        ServiceError::InputRequiredRequestsExceeded { limit: 1 }
    )));
    assert_eq!(requests.load(Ordering::SeqCst), 3);
    assert_eq!(answers.load(Ordering::SeqCst), 0);
    let limits = limits.with_max_inputs_per_round(2);
    client
        .call_tool_with_mrtr_limits(CallToolRequestParams::new("batch"), limits)
        .await
        .unwrap();
    client
        .get_prompt_with_mrtr_limits(GetPromptRequestParams::new("batch"), limits)
        .await
        .unwrap();
    client
        .read_resource_with_mrtr_limits(ReadResourceRequestParams::new("batch://value"), limits)
        .await
        .unwrap();
    assert_eq!(requests.load(Ordering::SeqCst), 9);
    assert_eq!(answers.load(Ordering::SeqCst), 6);
    let last = limits.with_max_rounds(1);
    let errors = [
        client
            .call_tool_with_mrtr_limits(CallToolRequestParams::new("batch"), last)
            .await
            .unwrap_err(),
        client
            .get_prompt_with_mrtr_limits(GetPromptRequestParams::new("batch"), last)
            .await
            .unwrap_err(),
        client
            .read_resource_with_mrtr_limits(ReadResourceRequestParams::new("batch://value"), last)
            .await
            .unwrap_err(),
    ];
    assert!(errors.into_iter().all(|error| matches!(
        error,
        ServiceError::InputRequiredRoundsExceeded { max_rounds: 1 }
    )));
    assert_eq!(requests.load(Ordering::SeqCst), 12);
    assert_eq!(answers.load(Ordering::SeqCst), 6);
    let limits = limits.with_max_rounds(0);
    let errors = [
        client
            .call_tool_with_mrtr_limits(CallToolRequestParams::new("batch"), limits)
            .await
            .unwrap_err(),
        client
            .get_prompt_with_mrtr_limits(GetPromptRequestParams::new("batch"), limits)
            .await
            .unwrap_err(),
        client
            .read_resource_with_mrtr_limits(ReadResourceRequestParams::new("batch://value"), limits)
            .await
            .unwrap_err(),
    ];
    assert!(errors.into_iter().all(|error| matches!(
        error,
        ServiceError::InputRequiredRoundsExceeded { max_rounds: 0 }
    )));
    assert_eq!(requests.load(Ordering::SeqCst), 12);
    assert_eq!(answers.load(Ordering::SeqCst), 6);
    client.cancel().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn max_rounds_convenience_helpers_inherit_default_batch_limits() {
    let requests = Arc::new(AtomicUsize::new(0));
    let answers = Arc::new(AtomicUsize::new(0));
    let (client_io, server_io) = tokio::io::duplex(65536);
    let server = tokio::spawn(async move {
        BatchServer {
            count: 65,
            requests,
        }
        .serve(server_io)
        .await
        .unwrap()
        .waiting()
        .await
        .unwrap();
    });
    let client = Answers(answers.clone()).serve(client_io).await.unwrap();
    let errors = [
        client
            .call_tool_with_mrtr_max_rounds(CallToolRequestParams::new("batch"), 2)
            .await
            .unwrap_err(),
        client
            .get_prompt_with_mrtr_max_rounds(GetPromptRequestParams::new("batch"), 2)
            .await
            .unwrap_err(),
        client
            .read_resource_with_mrtr_max_rounds(ReadResourceRequestParams::new("batch://value"), 2)
            .await
            .unwrap_err(),
    ];
    assert!(errors.into_iter().all(|error| matches!(
        error,
        ServiceError::InputRequiredRequestsExceeded { limit: 64 }
    )));
    assert_eq!(answers.load(Ordering::SeqCst), 0);
    client.cancel().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn zero_input_budget_preserves_state_only_retries() {
    let requests = Arc::new(AtomicUsize::new(0));
    let answers = Arc::new(AtomicUsize::new(0));
    let (client_io, server_io) = tokio::io::duplex(65536);
    let counter = requests.clone();
    let server = tokio::spawn(async move {
        BatchServer {
            count: 0,
            requests: counter,
        }
        .serve(server_io)
        .await
        .unwrap()
        .waiting()
        .await
        .unwrap();
    });
    let client = Answers(answers.clone()).serve(client_io).await.unwrap();
    let limits = MrtrLimits::default()
        .with_max_inputs_per_round(0)
        .with_max_rounds(2);
    client
        .call_tool_with_mrtr_limits(CallToolRequestParams::new("state"), limits)
        .await
        .unwrap();
    client
        .get_prompt_with_mrtr_limits(GetPromptRequestParams::new("state"), limits)
        .await
        .unwrap();
    client
        .read_resource_with_mrtr_limits(ReadResourceRequestParams::new("state://value"), limits)
        .await
        .unwrap();
    assert_eq!(requests.load(Ordering::SeqCst), 6);
    assert_eq!(answers.load(Ordering::SeqCst), 0);
    client.cancel().await.unwrap();
    server.await.unwrap();
}

#[derive(Clone, Copy)]
enum ScopeBehavior {
    Success,
    Error,
    Block,
    SiblingError,
}

struct ScopeAnswers {
    behavior: ScopeBehavior,
    started: tokio::sync::mpsc::UnboundedSender<tokio_util::sync::CancellationToken>,
    calls: AtomicUsize,
}

impl ClientHandler for ScopeAnswers {
    fn get_info(&self) -> ClientConfig {
        ClientConfig::new(
            ClientCapabilities::builder().enable_elicitation().build(),
            Implementation::new("callback-scope", "1"),
        )
    }

    async fn create_elicitation(
        &self,
        _: ElicitRequestParams,
        context: RequestContext<RoleClient>,
    ) -> Result<ElicitResult, ErrorData> {
        let call = self.calls.fetch_add(1, Ordering::SeqCst);
        assert!(!context.ct.is_cancelled());
        self.started.send(context.ct.clone()).unwrap();
        match self.behavior {
            ScopeBehavior::Success => Ok(ElicitResult::new(ElicitationAction::Accept)),
            ScopeBehavior::Error => Err(ErrorData::internal_error("expected callback error", None)),
            ScopeBehavior::SiblingError if call == 1 => {
                Err(ErrorData::internal_error("expected sibling error", None))
            }
            ScopeBehavior::Block | ScopeBehavior::SiblingError => std::future::pending().await,
        }
    }
}

async fn scoped_operation(
    client: &mcp_rs::protocol::service::RunningService<RoleClient, ScopeAnswers>,
    operation: usize,
) -> Result<(), ServiceError> {
    tokio::time::timeout(std::time::Duration::from_secs(5), async {
        match operation {
            0 => client
                .call_tool(CallToolRequestParams::new("batch"))
                .await
                .map(|_| ()),
            1 => client
                .get_prompt(GetPromptRequestParams::new("batch"))
                .await
                .map(|_| ()),
            _ => client
                .read_resource(ReadResourceRequestParams::new("batch://value"))
                .await
                .map(|_| ()),
        }
    })
    .await
    .unwrap_or_else(|_| panic!("native operation must finish or be dropped"))
}

async fn scope_client(
    behavior: ScopeBehavior,
    count: usize,
) -> (
    mcp_rs::protocol::service::RunningService<RoleClient, ScopeAnswers>,
    tokio::sync::mpsc::UnboundedReceiver<tokio_util::sync::CancellationToken>,
    tokio::task::JoinHandle<()>,
) {
    let (started, tokens) = tokio::sync::mpsc::unbounded_channel();
    let (client_io, server_io) = tokio::io::duplex(65536);
    let server = tokio::spawn(async move {
        BatchServer {
            count,
            requests: Arc::new(AtomicUsize::new(0)),
        }
        .serve(server_io)
        .await
        .unwrap()
        .waiting()
        .await
        .unwrap();
    });
    let client = ScopeAnswers {
        behavior,
        started,
        calls: AtomicUsize::new(0),
    }
    .serve(client_io)
    .await
    .unwrap();
    (client, tokens, server)
}

#[tokio::test]
async fn callback_scopes_end_on_success_and_error_for_all_native_operations() {
    for behavior in [ScopeBehavior::Success, ScopeBehavior::Error] {
        let (client, mut tokens, server) = scope_client(behavior, 1).await;
        for operation in 0..3 {
            let result = scoped_operation(&client, operation).await;
            match behavior {
                ScopeBehavior::Success => result.unwrap(),
                _ => assert!(matches!(result, Err(ServiceError::McpError(_)))),
            }
            assert!(tokens.try_recv().unwrap().is_cancelled());
        }
        client.cancel().await.unwrap();
        server.await.unwrap();
    }
}

#[tokio::test]
async fn dropping_operations_ends_only_their_own_callback_scope() {
    let (client, mut tokens, server) = scope_client(ScopeBehavior::Block, 1).await;
    for operation in 0..3 {
        // Both operations receive the same wire input key. Their scopes must remain independent.
        let mut first = Box::pin(scoped_operation(&client, operation));
        let first_token = tokio::select! {
            result = &mut first => panic!("callback should block: {result:?}"),
            token = tokio::time::timeout(std::time::Duration::from_secs(5), tokens.recv()) => token.unwrap().unwrap(),
        };
        let mut second = Box::pin(scoped_operation(&client, operation));
        let second_token = tokio::select! {
            result = &mut second => panic!("callback should block: {result:?}"),
            token = tokio::time::timeout(std::time::Duration::from_secs(5), tokens.recv()) => token.unwrap().unwrap(),
        };
        assert!(!first_token.is_cancelled());
        assert!(!second_token.is_cancelled());
        drop(first);
        assert!(first_token.is_cancelled());
        assert!(!second_token.is_cancelled());
        drop(second);
        assert!(second_token.is_cancelled());
    }
    client.cancel().await.unwrap();
    server.await.unwrap();
}

#[tokio::test]
async fn callback_failure_ends_active_sibling_scopes_without_starting_queued_inputs() {
    for operation in 0..3 {
        // Default concurrency is four, but the second callback fails while the first blocks.
        let (client, mut tokens, server) = scope_client(ScopeBehavior::SiblingError, 8).await;
        assert!(matches!(
            scoped_operation(&client, operation).await,
            Err(ServiceError::McpError(_))
        ));
        let first = tokens.try_recv().unwrap();
        let failed = tokens.try_recv().unwrap();
        assert!(first.is_cancelled());
        assert!(failed.is_cancelled());
        assert!(tokens.try_recv().is_err());
        client.cancel().await.unwrap();
        server.await.unwrap();
    }
}
