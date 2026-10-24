//! Middleware wraps application handlers in registration order.
use crate::{Middleware, model::ErrorData};
use futures_util::FutureExt;
use std::sync::Arc;

/// Check caller policy using the request context before running a handler.
/// Authentication credentials remain the host/transport's responsibility.
pub fn authorize<F>(check: F) -> Middleware
where
    F: Fn(&crate::Context) -> bool + Send + Sync + 'static,
{
    Arc::new(move |ctx, args, next| {
        let allowed = check(&ctx);
        async move {
            if !allowed {
                return Err(ErrorData::invalid_request("Access denied", None));
            }
            next(ctx, args).await
        }
        .boxed()
    })
}

/// A completed request observation, without request or response payloads.
#[derive(Clone, Debug)]
pub struct Observation {
    /// The SDK request identifier, reused rather than inventing a second identity.
    pub request_id: crate::model::RequestId,
    /// Wall-clock handler duration, including downstream middleware.
    pub elapsed: std::time::Duration,
    /// Whether the handler returned a successful protocol and application result.
    pub success: bool,
}

/// Observe completed requests for metrics or caller-owned audit sinks.
///
/// Observers are synchronous and must not block. Cancelled/dropped requests do
/// not produce completion events; endpoint-level auditing belongs to the host.
pub fn observe<F>(observer: F) -> Middleware
where
    F: Fn(Observation) + Send + Sync + 'static,
{
    let observer = Arc::new(observer);
    Arc::new(move |ctx, args, next| {
        let request_id = ctx.id.clone();
        let observer = observer.clone();
        async move {
            let start = std::time::Instant::now();
            let result = next(ctx, args).await;
            let success = match &result {
                Ok(crate::Response::Tool(result)) => result.is_error != Some(true),
                Ok(_) => true,
                Err(_) => false,
            };
            observer(Observation {
                request_id,
                elapsed: start.elapsed(),
                success,
            });
            result
        }
        .boxed()
    })
}

/// Log request IDs, durations, and outcomes through tracing, without payloads.
pub fn logging() -> Middleware {
    observe(
        |event| tracing::info!(request_id=?event.request_id,elapsed_ms=event.elapsed.as_millis(),success=event.success,"MCP handler completed"),
    )
}

/// Limit serialized decoded arguments before application execution.
///
/// This measures compact JSON argument bytes, not raw transport framing. Configure
/// transport body limits separately when accepting HTTP or large stdio frames.
pub fn size_limit(max_bytes: usize) -> Middleware {
    Arc::new(move |ctx, args, next| {
        async move {
            let size = serde_json::to_vec(&args)
                .map_err(|_| ErrorData::invalid_params("Invalid argument encoding", None))?
                .len();
            if size > max_bytes {
                return Err(ErrorData::invalid_params(
                    "Request argument size limit exceeded",
                    None,
                ));
            }
            next(ctx, args).await
        }
        .boxed()
    })
}

/// A rate limiter cannot use an empty time window.
#[derive(Debug, thiserror::Error)]
#[error("rate-limit window must be greater than zero")]
pub struct InvalidRateLimit;

/// Construct a fixed-window limiter shared by all requests using this middleware.
///
/// Counters are shared across cloned servers/sessions. The async mutex is released
/// before the downstream handler runs. This is process-local, not distributed.
/// # Errors
/// Returns an error for a zero-length window.
pub fn rate_limit(
    requests: std::num::NonZeroU32,
    window: std::time::Duration,
) -> Result<Middleware, InvalidRateLimit> {
    if window.is_zero() {
        return Err(InvalidRateLimit);
    }
    let state = Arc::new(tokio::sync::Mutex::new((
        tokio::time::Instant::now(),
        0_u32,
    )));
    Ok(Arc::new(move |ctx, args, next| {
        let state = state.clone();
        async move {
            {
                let mut state = state.lock().await;
                if state.0.elapsed() >= window {
                    *state = (tokio::time::Instant::now(), 0);
                }
                if state.1 >= requests.get() {
                    return Err(ErrorData::new(
                        crate::model::ErrorCode(-32000),
                        "Rate limit exceeded",
                        None,
                    ));
                }
                state.1 += 1;
            }
            next(ctx, args).await
        }
        .boxed()
    }))
}
