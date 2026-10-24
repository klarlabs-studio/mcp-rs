use std::num::NonZeroUsize;

use futures::{StreamExt, TryStreamExt};

use crate::{
    model::{DEFAULT_MRTR_MAX_ROUNDS, InputRequest, InputRequests, InputResponses},
    service::ServiceError,
};

/// Bounds for automatic multi-round tool, prompt and resource operations.
///
/// Defaults to ten outbound rounds, 64 input requests per round and four
/// concurrent input callbacks per operation. These are count limits; responses
/// are parsed before batch validation. Callback durations and payload bytes are
/// not bounded here. Separate operations have independent concurrency budgets.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[non_exhaustive]
pub struct MrtrLimits {
    /// Maximum outbound requests, including the initial request and final result.
    pub max_rounds: usize,
    /// Maximum inputs in a single response. Zero permits state-only retries.
    pub max_inputs_per_round: usize,
    /// Maximum simultaneously polled input callbacks in this operation.
    pub max_concurrent_inputs: NonZeroUsize,
}

impl Default for MrtrLimits {
    fn default() -> Self {
        Self {
            max_rounds: DEFAULT_MRTR_MAX_ROUNDS,
            max_inputs_per_round: 64,
            max_concurrent_inputs: NonZeroUsize::new(4).expect("four is nonzero"),
        }
    }
}

impl MrtrLimits {
    /// Set the outbound round cap. Zero rejects before sending.
    pub fn with_max_rounds(mut self, max_rounds: usize) -> Self {
        self.max_rounds = max_rounds;
        self
    }
    /// Set the per-round input cap. Zero allows state-only responses.
    pub fn with_max_inputs_per_round(mut self, max_inputs_per_round: usize) -> Self {
        self.max_inputs_per_round = max_inputs_per_round;
        self
    }
    /// Set the maximum callbacks polled concurrently per operation.
    pub fn with_max_concurrent_inputs(mut self, max_concurrent_inputs: NonZeroUsize) -> Self {
        self.max_concurrent_inputs = max_concurrent_inputs;
        self
    }
}

pub(super) async fn fulfill_inputs<F, Fut>(
    requests: InputRequests,
    limits: MrtrLimits,
    mut fulfill: F,
) -> Result<InputResponses, ServiceError>
where
    F: FnMut(String, InputRequest) -> Fut,
    Fut: Future<Output = Result<(String, serde_json::Value), ServiceError>>,
{
    if requests.len() > limits.max_inputs_per_round {
        return Err(ServiceError::InputRequiredRequestsExceeded {
            limit: limits.max_inputs_per_round,
        });
    }
    // Validate the entire batch before any callback is constructed or polled.
    // Retired sampling and roots requests remain decodable for explicit errors.
    for request in requests.values() {
        #[allow(deprecated)]
        match request {
            InputRequest::CreateMessage(_) => {
                return Err(ServiceError::McpError(
                    crate::ErrorData::method_not_found::<crate::model::CreateMessageRequestMethod>(
                    ),
                ));
            }
            InputRequest::ListRoots(_) => {
                return Err(ServiceError::McpError(
                    crate::ErrorData::method_not_found::<crate::model::ListRootsRequestMethod>(),
                ));
            }
            InputRequest::Elicitation(_) => {}
        }
    }
    let responses: Vec<_> = futures::stream::iter(requests)
        .map(|(key, request)| fulfill(key, request))
        .buffer_unordered(limits.max_concurrent_inputs.get())
        .try_collect()
        .await?;
    Ok(responses.into_iter().collect())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::{
        future::ready,
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
    };

    fn forbidden_callback(
        _: String,
        _: InputRequest,
    ) -> std::future::Ready<Result<(String, serde_json::Value), ServiceError>> {
        panic!("invalid or empty batch must not construct callbacks")
    }

    fn inputs(count: usize) -> InputRequests {
        let input: InputRequest = serde_json::from_value(json!({"method":"elicitation/create","params":{"mode":"form","message":"Answer?","requestedSchema":{"type":"object","properties":{}}}})).unwrap();
        (0..count)
            .map(|i| (format!("input-{i:03}"), input.clone()))
            .collect()
    }

    #[tokio::test]
    async fn batch_limit_and_retired_requests_fail_before_any_callback() {
        let defaults = MrtrLimits::default();
        assert_eq!(defaults.max_rounds, 10);
        assert_eq!(defaults.max_inputs_per_round, 64);
        assert_eq!(defaults.max_concurrent_inputs.get(), 4);
        let error = fulfill_inputs(inputs(65), defaults, forbidden_callback)
            .await
            .unwrap_err();
        assert!(matches!(
            error,
            ServiceError::InputRequiredRequestsExceeded { limit: 64 }
        ));
        for wire in [
            json!({"method":"roots/list"}),
            json!({"method":"sampling/createMessage","params":{"messages":[],"maxTokens":1}}),
        ] {
            let mut batch = inputs(1);
            batch.insert("z-retired".into(), serde_json::from_value(wire).unwrap());
            let error = fulfill_inputs(batch, defaults, forbidden_callback)
                .await
                .unwrap_err();
            assert!(
                matches!(error, ServiceError::McpError(error) if error.code == crate::model::ErrorCode::METHOD_NOT_FOUND)
            );
        }
        let empty = fulfill_inputs(
            inputs(0),
            defaults.with_max_inputs_per_round(0),
            forbidden_callback,
        )
        .await
        .unwrap();
        assert!(empty.is_empty());
        let error = fulfill_inputs(
            inputs(1),
            defaults.with_max_inputs_per_round(0),
            forbidden_callback,
        )
        .await
        .unwrap_err();
        assert!(matches!(
            error,
            ServiceError::InputRequiredRequestsExceeded { limit: 0 }
        ));
        let responses =
            fulfill_inputs(inputs(64), defaults, |key, _| ready(Ok((key, json!(true)))))
                .await
                .unwrap();
        assert_eq!(responses.len(), 64);
    }

    struct Active(Arc<AtomicUsize>);
    impl Drop for Active {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::SeqCst);
        }
    }

    #[tokio::test]
    async fn bounded_callbacks_preserve_every_key_and_response() {
        let active = Arc::new(AtomicUsize::new(0));
        let peak = AtomicUsize::new(0);
        let gate = tokio::sync::Semaphore::new(0);
        let work = fulfill_inputs(
            inputs(7),
            MrtrLimits::default()
                .with_max_inputs_per_round(7)
                .with_max_concurrent_inputs(NonZeroUsize::new(2).unwrap()),
            |key, _| {
                let active = &active;
                let peak = &peak;
                let gate = &gate;
                async move {
                    let _guard = Active(active.clone());
                    peak.fetch_max(active.fetch_add(1, Ordering::SeqCst) + 1, Ordering::SeqCst);
                    gate.acquire().await.unwrap().forget();
                    Ok((key.clone(), json!(key)))
                }
            },
        );
        tokio::pin!(work);
        assert!(futures::poll!(&mut work).is_pending());
        assert_eq!(active.load(Ordering::SeqCst), 2);
        gate.add_permits(7);
        let responses = work.await.unwrap();
        assert_eq!(peak.load(Ordering::SeqCst), 2);
        assert_eq!(active.load(Ordering::SeqCst), 0);
        assert_eq!(responses.len(), 7);
        for (key, value) in responses {
            assert_eq!(value, json!(key));
        }
    }

    #[tokio::test]
    async fn callback_failure_and_caller_drop_release_active_and_queued_work() {
        for fail in [false, true] {
            let active = Arc::new(AtomicUsize::new(0));
            let started = AtomicUsize::new(0);
            let gate = tokio::sync::Semaphore::new(0);
            {
                let work = fulfill_inputs(
                    inputs(7),
                    MrtrLimits::default().with_max_concurrent_inputs(NonZeroUsize::new(2).unwrap()),
                    |_, _| async {
                        started.fetch_add(1, Ordering::SeqCst);
                        active.fetch_add(1, Ordering::SeqCst);
                        let _guard = Active(active.clone());
                        gate.acquire().await.unwrap().forget();
                        Err(ServiceError::TransportClosed)
                    },
                );
                tokio::pin!(work);
                assert!(futures::poll!(&mut work).is_pending());
                assert_eq!(active.load(Ordering::SeqCst), 2);
                if fail {
                    gate.add_permits(1);
                    assert!(matches!(work.await, Err(ServiceError::TransportClosed)));
                }
            }
            assert_eq!(active.load(Ordering::SeqCst), 0);
            assert_eq!(started.load(Ordering::SeqCst), 2);
        }
    }
}
