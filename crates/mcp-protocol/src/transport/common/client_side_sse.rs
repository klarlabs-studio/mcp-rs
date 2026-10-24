// Derived from rmcp 3.5.1; maintained and modified by mcp-rs. See ../../../UPSTREAM.md.
use std::{
    pin::Pin,
    sync::Arc,
    task::{Poll, ready},
    time::Duration,
};

use bytes::Bytes;
use futures::{Stream, StreamExt, stream::BoxStream};
use sse_stream::{Error as SseError, Sse, SseStream};
use thiserror::Error;

use crate::model::ServerJsonRpcMessage;

// Bound work even when ignored frames and reconnect transitions stay ready.
const MAX_SSE_POLL_STEPS: usize = 32;
const MAX_SSE_BYTE_SLICE: usize = 16 * 1024;
const MAX_SSE_READY_SLICES: usize = 32;

pub type BoxedSseResponse = BoxStream<'static, Result<Sse, SseError>>;

/// Maximum raw size of one SSE event accepted from a remote server.
pub(crate) const DEFAULT_MAX_SSE_EVENT_SIZE: usize = 16 * 1024 * 1024;

#[derive(Debug, Error)]
enum BoundedSseStreamError {
    #[error(transparent)]
    Source(Box<dyn std::error::Error + Send + Sync>),
    #[error("SSE event exceeded the maximum size of {max_size} bytes")]
    EventTooLarge { max_size: usize },
}

#[derive(Debug)]
struct SseEventSizeLimiter {
    max_size: usize,
    retained_size: usize,
    line_size: usize,
    line_is_comment: bool,
    previous_was_cr: bool,
}

impl SseEventSizeLimiter {
    fn new(max_size: usize) -> Self {
        Self {
            max_size,
            retained_size: 0,
            line_size: 0,
            line_is_comment: false,
            previous_was_cr: false,
        }
    }

    fn observe(&mut self, chunk: &[u8]) -> Result<(), ()> {
        for &byte in chunk {
            if self.previous_was_cr {
                self.previous_was_cr = false;
                if byte == b'\n' {
                    continue;
                }
            }

            match byte {
                b'\r' => {
                    self.finish_line()?;
                    self.previous_was_cr = true;
                }
                b'\n' => self.finish_line()?,
                _ => {
                    if self.line_size == 0 {
                        self.line_is_comment = byte == b':';
                    }
                    self.line_size = self.line_size.saturating_add(1);
                    self.check_limit()?;
                }
            }
        }
        Ok(())
    }

    fn finish_line(&mut self) -> Result<(), ()> {
        if self.line_size == 0 {
            self.retained_size = 0;
        } else if !self.line_is_comment {
            // The SSE parser inserts a newline when joining multiple data fields.
            self.retained_size = self
                .retained_size
                .saturating_add(self.line_size)
                .saturating_add(1);
        }
        self.line_size = 0;
        self.line_is_comment = false;
        self.check_limit()
    }

    fn check_limit(&self) -> Result<(), ()> {
        if self.retained_size.saturating_add(self.line_size) > self.max_size {
            Err(())
        } else {
            Ok(())
        }
    }
}

pin_project_lite::pin_project! {
    struct BoundedSseByteStream<S> {
        #[pin]
        inner: S,
        limiter: SseEventSizeLimiter,
        pending: Option<Bytes>,
        remaining_slices: usize,
        failed: bool,
    }
}

impl<S, E> Stream for BoundedSseByteStream<S>
where
    S: Stream<Item = Result<Bytes, E>>,
    E: std::error::Error + Send + Sync + 'static,
{
    type Item = Result<Bytes, BoundedSseStreamError>;

    fn poll_next(
        self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut this = self.project();
        if *this.failed {
            return Poll::Ready(None);
        }

        if *this.remaining_slices == 0 {
            *this.remaining_slices = MAX_SSE_READY_SLICES;
            cx.waker().wake_by_ref();
            return Poll::Pending;
        }

        let next = match this.pending.take() {
            Some(chunk) => Some(Ok(chunk)),
            None => ready!(this.inner.as_mut().poll_next(cx)),
        };
        match next {
            Some(Ok(mut chunk)) => {
                // Bytes splitting retains the original allocation without copying.
                // Bound both limiter scanning and downstream parsing per slice.
                let slice = chunk.split_to(chunk.len().min(MAX_SSE_BYTE_SLICE));
                if !chunk.is_empty() {
                    *this.pending = Some(chunk);
                }
                *this.remaining_slices -= 1;
                if this.limiter.observe(&slice).is_err() {
                    *this.pending = None;
                    *this.failed = true;
                    Poll::Ready(Some(Err(BoundedSseStreamError::EventTooLarge {
                        max_size: this.limiter.max_size,
                    })))
                } else {
                    Poll::Ready(Some(Ok(slice)))
                }
            }
            Some(Err(error)) => {
                *this.failed = true;
                Poll::Ready(Some(Err(BoundedSseStreamError::Source(Box::new(error)))))
            }
            None => Poll::Ready(None),
        }
    }
}

pub(crate) fn bounded_sse_stream<S, E>(stream: S, max_event_size: usize) -> BoxedSseResponse
where
    S: Stream<Item = Result<Bytes, E>> + Send + 'static,
    E: std::error::Error + Send + Sync + 'static,
{
    let stream = BoundedSseByteStream {
        inner: stream,
        limiter: SseEventSizeLimiter::new(max_event_size),
        pending: None,
        remaining_slices: MAX_SSE_READY_SLICES,
        failed: false,
    };
    SseStream::from_bytes_stream(stream).boxed()
}

fn is_event_too_large_error(error: &SseError) -> bool {
    matches!(
        error,
        SseError::Body(error)
            if matches!(
                error.downcast_ref::<BoundedSseStreamError>(),
                Some(BoundedSseStreamError::EventTooLarge { .. })
            )
    )
}

pub trait SseRetryPolicy: std::fmt::Debug + Send + Sync {
    fn retry(&self, current_times: usize) -> Option<Duration>;
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct FixedInterval {
    pub max_times: Option<usize>,
    pub duration: Duration,
}

impl SseRetryPolicy for FixedInterval {
    fn retry(&self, current_times: usize) -> Option<Duration> {
        if let Some(max_times) = self.max_times
            && current_times >= max_times
        {
            return None;
        }
        Some(self.duration)
    }
}

impl FixedInterval {
    pub const DEFAULT_MIN_DURATION: Duration = Duration::from_millis(1000);
}

impl Default for FixedInterval {
    fn default() -> Self {
        Self {
            max_times: None,
            duration: Self::DEFAULT_MIN_DURATION,
        }
    }
}

#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ExponentialBackoff {
    pub max_times: Option<usize>,
    pub base_duration: Duration,
    /// Optional upper bound on a single reconnect delay. `None` (the default) preserves the
    /// pre-existing unbounded doubling behavior. Once the multiplier saturates near the bit
    /// width that can still produce very long sleeps, so callers that need the client to
    /// actually reconnect can set `Some(...)` to clamp the delay.
    pub max_delay: Option<Duration>,
}

impl ExponentialBackoff {
    pub const DEFAULT_DURATION: Duration = Duration::from_millis(1000);
}

impl Default for ExponentialBackoff {
    fn default() -> Self {
        Self {
            max_times: None,
            base_duration: Self::DEFAULT_DURATION,
            max_delay: None,
        }
    }
}

impl SseRetryPolicy for ExponentialBackoff {
    fn retry(&self, current_times: usize) -> Option<Duration> {
        if let Some(max_times) = self.max_times
            && current_times >= max_times
        {
            return None;
        }
        // `current_times` is unbounded when `max_times` is unset, so the exponent can reach
        // the bit width. Saturate the multiplier at `u32::MAX` and use saturating multiplication
        // for the base duration so the delay stays monotonic and panic-free instead of an
        // overflow panic (debug) or a wrapped-to-zero backoff (release).
        let exponent = u32::try_from(current_times).unwrap_or(u32::MAX);
        let multiplier = 2u32.saturating_pow(exponent);
        let delay = self.base_duration.saturating_mul(multiplier);
        Some(match self.max_delay {
            Some(max_delay) => delay.min(max_delay),
            None => delay,
        })
    }
}

#[derive(Debug, Clone, Copy, Default)]
#[non_exhaustive]
pub struct NeverRetry;

impl SseRetryPolicy for NeverRetry {
    fn retry(&self, _current_times: usize) -> Option<Duration> {
        None
    }
}

#[derive(Debug, Default)]
pub struct NeverReconnect<E> {
    error: Option<E>,
}

impl<E: std::error::Error + Send> SseStreamReconnect for NeverReconnect<E> {
    type Error = E;
    type Future = futures::future::Ready<Result<BoxedSseResponse, Self::Error>>;
    fn retry_connection(&mut self, _last_event_id: Option<&str>) -> Self::Future {
        futures::future::ready(Err(self.error.take().expect("should not be called again")))
    }
}

/// Abstraction for SSE reconnection logic. Implementors can hook into
/// [`handle_control_event`](Self::handle_control_event) to consume control
/// frames (e.g. `event: endpoint`) that arrive when a server restarts an SSE
/// stream. The default implementation is a no-op, keeping existing behaviour
/// intact.
pub(crate) trait SseStreamReconnect {
    type Error: std::error::Error;
    type Future: Future<Output = Result<BoxedSseResponse, Self::Error>> + Send;
    fn retry_connection(&mut self, last_event_id: Option<&str>) -> Self::Future;
    fn handle_control_event(&mut self, _event: &Sse) -> Result<(), Self::Error> {
        Ok(())
    }
    fn handle_stream_error(
        &mut self,
        error: &(dyn std::error::Error + 'static),
        last_event_id: Option<&str>,
    ) {
        if let Some(id) = last_event_id {
            tracing::warn!(%id, "sse stream error: {error}");
        } else {
            tracing::warn!("sse stream error: {error}");
        }
    }
    fn map_fatal_stream_error(&mut self, error: SseError) -> Option<Self::Error> {
        tracing::warn!("fatal sse stream error: {error}");
        None
    }
}

pin_project_lite::pin_project! {
    pub(crate) struct SseAutoReconnectStream<R>
    where R: SseStreamReconnect
     {
        retry_policy: Arc<dyn SseRetryPolicy>,
        reconnect_only_after_event_id: bool,
        response_complete: bool,
        last_event_id: Option<String>,
        server_retry_interval: Option<Duration>,
        connector: R,
        #[pin]
        state: SseAutoReconnectStreamState<R::Future>,
    }
}

impl<R: SseStreamReconnect> SseAutoReconnectStream<R> {
    pub fn new(
        stream: BoxedSseResponse,
        connector: R,
        retry_policy: Arc<dyn SseRetryPolicy>,
    ) -> Self {
        Self {
            retry_policy,
            reconnect_only_after_event_id: false,
            response_complete: false,
            last_event_id: None,
            server_retry_interval: None,
            connector,
            state: SseAutoReconnectStreamState::Connected { stream },
        }
    }

    pub fn new_after_event_id(
        stream: BoxedSseResponse,
        connector: R,
        retry_policy: Arc<dyn SseRetryPolicy>,
    ) -> Self {
        Self {
            retry_policy,
            reconnect_only_after_event_id: true,
            response_complete: false,
            last_event_id: None,
            server_retry_interval: None,
            connector,
            state: SseAutoReconnectStreamState::Connected { stream },
        }
    }
}

impl<E: std::error::Error + Send> SseAutoReconnectStream<NeverReconnect<E>> {
    #[allow(dead_code)]
    pub(crate) fn never_reconnect(stream: BoxedSseResponse, error_when_reconnect: E) -> Self {
        Self {
            retry_policy: Arc::new(NeverRetry),
            reconnect_only_after_event_id: false,
            response_complete: false,
            last_event_id: None,
            server_retry_interval: None,
            connector: NeverReconnect {
                error: Some(error_when_reconnect),
            },
            state: SseAutoReconnectStreamState::Connected { stream },
        }
    }
}

pin_project_lite::pin_project! {
    #[project = SseAutoReconnectStreamStateProj]
    #[non_exhaustive]
    pub enum SseAutoReconnectStreamState<F> {
        Connected {
            #[pin]
            stream: BoxedSseResponse,
        },
        Retrying {
            retry_times: usize,
            #[pin]
            retrying: F,
        },
        WaitingNextRetry {
            #[pin]
            sleep: tokio::time::Sleep,
            retry_times: usize,
        },
        Terminated,
    }
}

impl<R> Stream for SseAutoReconnectStream<R>
where
    R: SseStreamReconnect,
{
    type Item = Result<ServerJsonRpcMessage, R::Error>;
    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let mut remaining_steps = MAX_SSE_POLL_STEPS;
        loop {
            if remaining_steps == 0 {
                // We consumed ready work without producing a message. Schedule
                // another poll so cancellation and timers can run in between.
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            remaining_steps -= 1;
            let mut this = self.as_mut().project();
            // let this_state = this.state.as_mut().project()
            let state = this.state.as_mut().project();
            let next_state = match state {
                SseAutoReconnectStreamStateProj::Connected { stream } => {
                    match ready!(stream.poll_next(cx)) {
                        Some(Ok(sse)) => {
                            if let Some(new_server_retry) = sse.retry {
                                *this.server_retry_interval =
                                    Some(Duration::from_millis(new_server_retry));
                            }
                            if let Some(ref event_id) = sse.id {
                                *this.last_event_id = Some(event_id.clone());
                            }
                            // Only treat blank/`message` events as JSON-RPC payloads.
                            // Other control frames (endpoint, ping, etc.) are passed to
                            // the reconnection handler.
                            let is_message_event =
                                matches!(sse.event.as_deref(), None | Some("") | Some("message"));
                            if !is_message_event {
                                match this.connector.handle_control_event(&sse) {
                                    Ok(()) => continue,
                                    Err(e) => {
                                        this.state.set(SseAutoReconnectStreamState::Terminated);
                                        return Poll::Ready(Some(Err(e)));
                                    }
                                }
                            }
                            if let Some(data) = sse.data {
                                match serde_json::from_str::<ServerJsonRpcMessage>(&data) {
                                    Err(e) => {
                                        // Downgrade to debug to avoid noisy logs when servers emit
                                        // non-JSON payloads as message frames. Include last_event_id
                                        // to aid troubleshooting while keeping default behaviour.
                                        let last_id = this.last_event_id.as_deref().unwrap_or("");
                                        tracing::debug!(last_event_id=%last_id, "failed to deserialize server message: {e}");
                                        continue;
                                    }
                                    Ok(message) => {
                                        if *this.reconnect_only_after_event_id
                                            && matches!(
                                                message,
                                                ServerJsonRpcMessage::Response(_)
                                                    | ServerJsonRpcMessage::Error(_)
                                            )
                                        {
                                            // Keep the current body available for bounded draining,
                                            // but never resume a request that already completed.
                                            *this.response_complete = true;
                                        }
                                        return Poll::Ready(Some(Ok(message)));
                                    }
                                };
                            } else {
                                continue;
                            }
                        }
                        Some(Err(e)) => {
                            if *this.response_complete {
                                this.state.set(SseAutoReconnectStreamState::Terminated);
                                return Poll::Ready(None);
                            }
                            if is_event_too_large_error(&e) {
                                this.state.set(SseAutoReconnectStreamState::Terminated);
                                return Poll::Ready(
                                    this.connector.map_fatal_stream_error(e).map(Err),
                                );
                            }
                            if *this.reconnect_only_after_event_id && this.last_event_id.is_none() {
                                this.state.set(SseAutoReconnectStreamState::Terminated);
                                return Poll::Ready(
                                    this.connector.map_fatal_stream_error(e).map(Err),
                                );
                            }
                            this.connector
                                .handle_stream_error(&e, this.last_event_id.as_deref());
                            if let Some(interval) = this.retry_policy.retry(0) {
                                let interval = this
                                    .server_retry_interval
                                    .map_or(interval, |server| server.max(interval));
                                SseAutoReconnectStreamState::WaitingNextRetry {
                                    sleep: tokio::time::sleep(interval),
                                    retry_times: 0,
                                }
                            } else {
                                this.state.set(SseAutoReconnectStreamState::Terminated);
                                return Poll::Ready(
                                    this.connector.map_fatal_stream_error(e).map(Err),
                                );
                            }
                        }
                        None => {
                            if *this.response_complete {
                                this.state.set(SseAutoReconnectStreamState::Terminated);
                                return Poll::Ready(None);
                            }
                            if *this.reconnect_only_after_event_id && this.last_event_id.is_none() {
                                tracing::debug!(
                                    "sse response ended before an event ID was received; cannot resume"
                                );
                                this.state.set(SseAutoReconnectStreamState::Terminated);
                                return Poll::Ready(None);
                            }
                            // Per SEP-1699, a graceful stream close is
                            // reconnectable.  If the server sent a `retry` field
                            // we MUST wait that long before reconnecting.
                            let interval = this.retry_policy.retry(0).map(|interval| {
                                this.server_retry_interval
                                    .map_or(interval, |server| server.max(interval))
                            });
                            if let Some(interval) = interval {
                                tracing::debug!(
                                    ?interval,
                                    "sse stream ended gracefully, reconnecting"
                                );
                                SseAutoReconnectStreamState::WaitingNextRetry {
                                    sleep: tokio::time::sleep(interval),
                                    retry_times: 0,
                                }
                            } else {
                                tracing::debug!("sse stream terminated, no reconnect policy");
                                this.state.set(SseAutoReconnectStreamState::Terminated);
                                return Poll::Ready(None);
                            }
                        }
                    }
                }
                SseAutoReconnectStreamStateProj::Retrying {
                    retry_times,
                    retrying,
                } => {
                    let retry_result = ready!(retrying.poll(cx));
                    match retry_result {
                        Ok(new_stream) => {
                            SseAutoReconnectStreamState::Connected { stream: new_stream }
                        }
                        Err(e) => {
                            tracing::debug!("retry sse stream error: {e}");
                            *retry_times = retry_times.saturating_add(1);
                            if let Some(interval) = this.retry_policy.retry(*retry_times) {
                                let interval = this
                                    .server_retry_interval
                                    .map(|server_retry_interval| {
                                        server_retry_interval.max(interval)
                                    })
                                    .unwrap_or(interval);
                                let sleep = tokio::time::sleep(interval);
                                SseAutoReconnectStreamState::WaitingNextRetry {
                                    sleep,
                                    retry_times: *retry_times,
                                }
                            } else {
                                tracing::error!("sse stream error: {e}, max retry times reached");
                                this.state.set(SseAutoReconnectStreamState::Terminated);
                                return Poll::Ready(Some(Err(e)));
                            }
                        }
                    }
                }
                SseAutoReconnectStreamStateProj::WaitingNextRetry { sleep, retry_times } => {
                    ready!(sleep.poll(cx));
                    let retrying = this
                        .connector
                        .retry_connection(this.last_event_id.as_deref());
                    let retry_times = *retry_times;
                    SseAutoReconnectStreamState::Retrying {
                        retry_times,
                        retrying,
                    }
                }
                SseAutoReconnectStreamStateProj::Terminated => {
                    return Poll::Ready(None);
                }
            };
            // update the state
            this.state.set(next_state);
        }
    }
}

#[cfg(test)]
mod response_reconnection_tests {
    use super::*;
    use std::{collections::VecDeque, io, sync::Mutex};

    type Attempts = Arc<Mutex<Vec<Option<String>>>>;

    struct RecordingReconnect {
        attempts: Attempts,
        streams: VecDeque<BoxedSseResponse>,
    }

    impl SseStreamReconnect for RecordingReconnect {
        type Error = io::Error;
        type Future = futures::future::Ready<Result<BoxedSseResponse, Self::Error>>;

        fn retry_connection(&mut self, last_event_id: Option<&str>) -> Self::Future {
            self.attempts
                .lock()
                .unwrap()
                .push(last_event_id.map(str::to_owned));
            futures::future::ready(Ok(self
                .streams
                .pop_front()
                .unwrap_or_else(|| futures::stream::pending().boxed())))
        }
    }

    fn connector(streams: Vec<BoxedSseResponse>) -> (RecordingReconnect, Attempts) {
        let attempts = Attempts::default();
        (
            RecordingReconnect {
                attempts: attempts.clone(),
                streams: streams.into(),
            },
            attempts,
        )
    }

    fn frames(items: Vec<Result<Sse, SseError>>) -> BoxedSseResponse {
        futures::stream::iter(items).boxed()
    }

    fn event(data: &str, id: &str) -> Sse {
        Sse {
            data: Some(data.into()),
            id: Some(id.into()),
            retry: Some(0),
            ..Default::default()
        }
    }

    fn policy() -> Arc<dyn SseRetryPolicy> {
        Arc::new(FixedInterval {
            max_times: Some(1),
            duration: Duration::ZERO,
        })
    }

    const RESPONSE: &str = r#"{"jsonrpc":"2.0","id":7,"result":{"tools":[]}}"#;
    const ERROR: &str = r#"{"jsonrpc":"2.0","id":7,"error":{"code":-32603,"message":"failed"}}"#;
    const NOTICE: &str = r#"{"jsonrpc":"2.0","method":"notifications/tools/list_changed"}"#;

    #[tokio::test(start_paused = true)]
    async fn completed_request_never_reconnects_on_eof_or_body_error() {
        for final_message in [RESPONSE, ERROR] {
            for body_error in [false, true] {
                let mut items = vec![Ok(event(final_message, "final-1"))];
                if body_error {
                    items.push(Err(SseError::Body(Box::new(io::Error::other(
                        "body closed",
                    )))));
                }
                let (connector, attempts) = connector(vec![]);
                let mut stream = Box::pin(SseAutoReconnectStream::new_after_event_id(
                    frames(items),
                    connector,
                    policy(),
                ));
                assert!(stream.next().await.unwrap().is_ok());
                assert!(
                    tokio::time::timeout(Duration::from_millis(1), stream.next())
                        .await
                        .unwrap()
                        .is_none()
                );
                assert!(stream.next().await.is_none());
                assert!(attempts.lock().unwrap().is_empty());
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn completed_request_preserves_current_body_tail_for_drain() {
        let (connector, attempts) = connector(vec![]);
        let mut stream = Box::pin(SseAutoReconnectStream::new_after_event_id(
            frames(vec![
                Ok(event(RESPONSE, "final-1")),
                Ok(event(NOTICE, "tail-2")),
            ]),
            connector,
            policy(),
        ));
        assert!(matches!(
            stream.next().await,
            Some(Ok(ServerJsonRpcMessage::Response(_)))
        ));
        assert!(matches!(
            stream.next().await,
            Some(Ok(ServerJsonRpcMessage::Notification(_)))
        ));
        assert!(
            tokio::time::timeout(Duration::from_millis(1), stream.next())
                .await
                .unwrap()
                .is_none()
        );
        assert!(attempts.lock().unwrap().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn incomplete_request_still_resumes_with_its_last_event_id() {
        let (connector, attempts) = connector(vec![frames(vec![Ok(event(RESPONSE, "final-2"))])]);
        let mut stream = Box::pin(SseAutoReconnectStream::new_after_event_id(
            frames(vec![Ok(event(NOTICE, "before-1"))]),
            connector,
            policy(),
        ));
        assert!(matches!(
            stream.next().await,
            Some(Ok(ServerJsonRpcMessage::Notification(_)))
        ));
        assert!(matches!(
            tokio::time::timeout(Duration::from_millis(1), stream.next())
                .await
                .unwrap(),
            Some(Ok(ServerJsonRpcMessage::Response(_)))
        ));
        assert_eq!(*attempts.lock().unwrap(), vec![Some("before-1".to_owned())]);
    }

    #[tokio::test(start_paused = true)]
    async fn common_stream_still_resumes_after_response_or_error_messages() {
        for message in [RESPONSE, ERROR] {
            let (connector, attempts) =
                connector(vec![frames(vec![Ok(event(NOTICE, "update-2"))])]);
            let mut stream = Box::pin(SseAutoReconnectStream::new(
                frames(vec![Ok(event(message, "common-1"))]),
                connector,
                policy(),
            ));
            assert!(stream.next().await.unwrap().is_ok());
            assert!(matches!(
                tokio::time::timeout(Duration::from_millis(1), stream.next())
                    .await
                    .unwrap(),
                Some(Ok(ServerJsonRpcMessage::Notification(_)))
            ));
            assert_eq!(*attempts.lock().unwrap(), vec![Some("common-1".to_owned())]);
        }
    }

    #[derive(Default)]
    struct WakeCounter(std::sync::atomic::AtomicUsize);

    impl futures::task::ArcWake for WakeCounter {
        fn wake_by_ref(arc_self: &Arc<Self>) {
            arc_self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        }
    }

    #[test]
    fn ignored_ready_frames_yield_and_schedule_another_poll() {
        use std::sync::atomic::{AtomicUsize, Ordering};

        for frame in [
            Sse {
                event: Some("ping".into()),
                ..Default::default()
            },
            Sse::default(),
            event("invalid JSON", "ignored-1"),
        ] {
            let polls = Arc::new(AtomicUsize::new(0));
            let observed = polls.clone();
            let input = futures::stream::poll_fn(move |_| {
                // Keep regressions finite instead of hanging the test executor.
                assert!(observed.fetch_add(1, Ordering::SeqCst) < 128);
                Poll::Ready(Some(Ok(frame.clone())))
            })
            .boxed();
            let (connector, attempts) = connector(vec![]);
            let mut stream = Box::pin(SseAutoReconnectStream::new_after_event_id(
                input,
                connector,
                policy(),
            ));
            let wake = Arc::new(WakeCounter::default());
            let waker = futures::task::waker_ref(&wake);
            let mut cx = std::task::Context::from_waker(&waker);
            for expected in 1..=2 {
                assert!(stream.as_mut().poll_next(&mut cx).is_pending());
                assert_eq!(polls.load(Ordering::SeqCst), expected * MAX_SSE_POLL_STEPS);
                assert_eq!(wake.0.load(Ordering::SeqCst), expected);
            }
            assert!(attempts.lock().unwrap().is_empty());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn yielding_preserves_messages_and_resume_event_ids() {
        let (connector, attempts) = connector(vec![frames(vec![Ok(event(RESPONSE, "final-2"))])]);
        let mut input: Vec<_> = (0..MAX_SSE_POLL_STEPS)
            .map(|_| Ok(event("invalid JSON", "before-1")))
            .collect();
        input.push(Ok(event(NOTICE, "notice-2")));
        let mut stream = Box::pin(SseAutoReconnectStream::new_after_event_id(
            frames(input),
            connector,
            policy(),
        ));
        let wake = Arc::new(WakeCounter::default());
        let waker = futures::task::waker_ref(&wake);
        assert!(
            stream
                .as_mut()
                .poll_next(&mut std::task::Context::from_waker(&waker))
                .is_pending()
        );
        assert_eq!(wake.0.load(std::sync::atomic::Ordering::SeqCst), 1);
        assert!(matches!(
            stream.next().await,
            Some(Ok(ServerJsonRpcMessage::Notification(_)))
        ));
        assert!(matches!(
            stream.next().await,
            Some(Ok(ServerJsonRpcMessage::Response(_)))
        ));
        assert_eq!(*attempts.lock().unwrap(), vec![Some("notice-2".to_owned())]);
        assert!(stream.next().await.is_none());
    }

    #[tokio::test(start_paused = true)]
    async fn first_reconnect_honors_policy_and_server_minimum_after_eof_or_error() {
        for body_error in [false, true] {
            for (local_ms, server_ms) in [(25, 80), (80, 25)] {
                let mut priming = event(NOTICE, "before-1");
                priming.retry = Some(server_ms);
                let mut input = vec![Ok(priming)];
                if body_error {
                    input.push(Err(SseError::Body(Box::new(io::Error::other("closed")))));
                }
                let (connector, attempts) =
                    connector(vec![frames(vec![Ok(event(RESPONSE, "final-2"))])]);
                let policy = Arc::new(FixedInterval {
                    max_times: Some(1),
                    duration: Duration::from_millis(local_ms),
                });
                let mut stream = Box::pin(SseAutoReconnectStream::new_after_event_id(
                    frames(input),
                    connector,
                    policy,
                ));
                assert!(stream.next().await.unwrap().is_ok());
                let mut next = Box::pin(stream.next());
                assert!(futures::poll!(&mut next).is_pending());
                assert!(attempts.lock().unwrap().is_empty());
                tokio::time::advance(Duration::from_millis(local_ms.max(server_ms) - 1)).await;
                assert!(futures::poll!(&mut next).is_pending());
                assert!(attempts.lock().unwrap().is_empty());
                tokio::time::advance(Duration::from_millis(1)).await;
                assert!(matches!(
                    next.await,
                    Some(Ok(ServerJsonRpcMessage::Response(_)))
                ));
                assert_eq!(*attempts.lock().unwrap(), vec![Some("before-1".into())]);
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn disabled_retry_cannot_be_overridden_by_server_hint_or_body_error() {
        let policies: [Arc<dyn SseRetryPolicy>; 2] = [
            Arc::new(NeverRetry),
            Arc::new(FixedInterval {
                max_times: Some(0),
                duration: Duration::ZERO,
            }),
        ];
        for policy in policies {
            for scoped in [false, true] {
                for body_error in [false, true] {
                    let mut priming = event(NOTICE, "before-1");
                    priming.retry = Some(80);
                    let mut input = vec![Ok(priming)];
                    if body_error {
                        input.push(Err(SseError::Body(Box::new(io::Error::other("closed")))));
                    }
                    let (connector, attempts) = connector(vec![]);
                    let mut stream = Box::pin(if scoped {
                        SseAutoReconnectStream::new_after_event_id(
                            frames(input),
                            connector,
                            policy.clone(),
                        )
                    } else {
                        SseAutoReconnectStream::new(frames(input), connector, policy.clone())
                    });
                    assert!(stream.next().await.unwrap().is_ok());
                    assert!(
                        tokio::time::timeout(Duration::from_millis(1), stream.next())
                            .await
                            .unwrap()
                            .is_none()
                    );
                    assert!(stream.next().await.is_none());
                    assert!(attempts.lock().unwrap().is_empty());
                }
            }
        }
    }

    #[tokio::test(start_paused = true)]
    async fn server_retry_minimum_persists_across_successful_reconnections() {
        let mut priming = event(NOTICE, "before-1");
        priming.retry = Some(80);
        let mut update = event(NOTICE, "update-2");
        update.retry = None;
        let (connector, attempts) = connector(vec![
            frames(vec![Ok(update)]),
            frames(vec![Ok(event(RESPONSE, "final-3"))]),
        ]);
        let policy = Arc::new(FixedInterval {
            max_times: Some(1),
            duration: Duration::from_millis(25),
        });
        let mut stream = Box::pin(SseAutoReconnectStream::new_after_event_id(
            frames(vec![Ok(priming)]),
            connector,
            policy,
        ));
        assert!(stream.next().await.unwrap().is_ok());
        for expected in 1..=2 {
            let mut next = Box::pin(stream.next());
            assert!(futures::poll!(&mut next).is_pending());
            tokio::time::advance(Duration::from_millis(79)).await;
            assert!(futures::poll!(&mut next).is_pending());
            assert_eq!(attempts.lock().unwrap().len(), expected - 1);
            tokio::time::advance(Duration::from_millis(1)).await;
            assert!(next.await.unwrap().is_ok());
            assert_eq!(attempts.lock().unwrap().len(), expected);
        }
        assert_eq!(
            *attempts.lock().unwrap(),
            vec![Some("before-1".into()), Some("update-2".into())]
        );
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_a_waiting_reconnect_prevents_the_get() {
        let (connector, attempts) = connector(vec![]);
        let input = frames(vec![
            Ok(event(NOTICE, "before-1")),
            Err(SseError::Body(Box::new(io::Error::other("closed")))),
        ]);
        let policy = Arc::new(FixedInterval {
            max_times: Some(1),
            duration: Duration::from_millis(80),
        });
        let mut stream = Box::pin(SseAutoReconnectStream::new_after_event_id(
            input, connector, policy,
        ));
        assert!(stream.next().await.unwrap().is_ok());
        {
            let mut next = Box::pin(stream.next());
            assert!(futures::poll!(&mut next).is_pending());
        }
        assert!(attempts.lock().unwrap().is_empty());
        drop(stream);
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(attempts.lock().unwrap().is_empty());
    }

    #[tokio::test(start_paused = true)]
    async fn retry_counter_saturates_without_reopening_an_exhausted_budget() {
        let (connector, attempts) = connector(vec![]);
        let policy = Arc::new(FixedInterval {
            max_times: Some(usize::MAX),
            duration: Duration::ZERO,
        });
        let mut stream = SseAutoReconnectStream::new(frames(vec![]), connector, policy);
        stream.state = SseAutoReconnectStreamState::Retrying {
            retry_times: usize::MAX,
            retrying: futures::future::ready(Err(io::Error::other("reconnect failed"))),
        };
        let mut stream = Box::pin(stream);
        let error = stream.next().await.unwrap().unwrap_err();
        assert_eq!(error.to_string(), "reconnect failed");
        assert!(stream.next().await.is_none());
        assert!(attempts.lock().unwrap().is_empty());
    }
}

#[cfg(test)]
mod byte_polling_tests {
    use super::*;
    use std::{
        io,
        sync::atomic::{AtomicUsize, Ordering},
    };

    #[derive(Default)]
    struct WakeCounter(AtomicUsize);

    impl futures::task::ArcWake for WakeCounter {
        fn wake_by_ref(arc_self: &Arc<Self>) {
            arc_self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    #[test]
    fn continuous_non_event_bytes_yield_and_wake() {
        for chunk in [
            Bytes::new(),
            Bytes::from_static(b": keep alive\n"),
            Bytes::from_static(b"data: partial"),
        ] {
            let polls = Arc::new(AtomicUsize::new(0));
            let observed = polls.clone();
            let input = futures::stream::poll_fn(move |_| {
                // A finite safety guard makes the unbounded regression fail promptly.
                assert!(observed.fetch_add(1, Ordering::SeqCst) < 128);
                Poll::Ready(Some(Ok::<_, io::Error>(chunk.clone())))
            });
            let mut stream = bounded_sse_stream(input, DEFAULT_MAX_SSE_EVENT_SIZE);
            let wake = Arc::new(WakeCounter::default());
            let waker = futures::task::waker_ref(&wake);
            let mut cx = std::task::Context::from_waker(&waker);
            for expected in 1..=2 {
                assert!(stream.as_mut().poll_next(&mut cx).is_pending());
                assert_eq!(
                    polls.load(Ordering::SeqCst),
                    expected * MAX_SSE_READY_SLICES
                );
                assert_eq!(wake.0.load(Ordering::SeqCst), expected);
            }
        }
    }

    #[tokio::test]
    async fn large_chunk_yields_and_preserves_utf8_crlf_and_event_order() {
        let polls = Arc::new(AtomicUsize::new(0));
        let observed = polls.clone();
        let data = "é".repeat(MAX_SSE_BYTE_SLICE);
        let mut raw = ":\n".repeat(MAX_SSE_BYTE_SLICE * MAX_SSE_READY_SLICES / 2);
        raw.push_str(&format!(
            "id: first\r\ndata: {data}\r\n\r\nid: second\r\ndata: done\r\n\r\n"
        ));
        let mut body = Some(Bytes::from(raw));
        let input = futures::stream::poll_fn(move |_| {
            observed.fetch_add(1, Ordering::SeqCst);
            Poll::Ready(body.take().map(Ok::<_, io::Error>))
        });
        let mut stream = bounded_sse_stream(input, DEFAULT_MAX_SSE_EVENT_SIZE);
        let wake = Arc::new(WakeCounter::default());
        let waker = futures::task::waker_ref(&wake);
        assert!(
            stream
                .as_mut()
                .poll_next(&mut std::task::Context::from_waker(&waker))
                .is_pending()
        );
        assert_eq!(wake.0.load(Ordering::SeqCst), 1);
        assert_eq!(polls.load(Ordering::SeqCst), 1);
        let first = stream.next().await.unwrap().unwrap();
        assert_eq!(first.id.as_deref(), Some("first"));
        assert_eq!(first.data.as_deref(), Some(data.as_str()));
        let second = stream.next().await.unwrap().unwrap();
        assert_eq!(second.id.as_deref(), Some("second"));
        assert_eq!(second.data.as_deref(), Some("done"));
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn event_limit_still_rejects_an_oversized_split_chunk() {
        let max = MAX_SSE_BYTE_SLICE + 8;
        let input = futures::stream::iter([Ok::<_, io::Error>(Bytes::from("x".repeat(max + 1)))]);
        let mut stream = bounded_sse_stream(input, max);
        let error = stream.next().await.unwrap().unwrap_err();
        assert!(is_event_too_large_error(&error));
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn source_error_survives_a_cooperative_yield() {
        let mut input: Vec<Result<Bytes, io::Error>> = (0..MAX_SSE_READY_SLICES)
            .map(|_| Ok(Bytes::from_static(b": ping\n")))
            .collect();
        input.push(Err(io::Error::other("source failed")));
        let mut stream =
            bounded_sse_stream(futures::stream::iter(input), DEFAULT_MAX_SSE_EVENT_SIZE);
        let wake = Arc::new(WakeCounter::default());
        let waker = futures::task::waker_ref(&wake);
        assert!(
            stream
                .as_mut()
                .poll_next(&mut std::task::Context::from_waker(&waker))
                .is_pending()
        );
        let error = stream.next().await.unwrap().unwrap_err();
        assert!(matches!(error, SseError::Body(_)));
        assert!(error.to_string().contains("source failed"));
        assert!(stream.next().await.is_none());
    }
}

#[cfg(test)]
mod retry_bound_tests {
    use super::*;

    #[test]
    fn exponential_backoff_stays_at_its_plateau_for_large_counts() {
        let policy = ExponentialBackoff {
            base_duration: Duration::from_millis(7),
            max_times: None,
            max_delay: None,
        };
        assert_eq!(policy.retry(0), Some(Duration::from_millis(7)));
        assert_eq!(policy.retry(1), Some(Duration::from_millis(14)));
        assert_eq!(
            policy.retry(31),
            Some(Duration::from_millis(7 * (1_u64 << 31)))
        );
        let plateau = Some(Duration::from_millis(7 * u64::from(u32::MAX)));
        for count in [32, u32::MAX as usize, usize::MAX] {
            assert_eq!(policy.retry(count), plateau);
        }
        #[cfg(target_pointer_width = "64")]
        for count in [(u32::MAX as usize) + 1, (u32::MAX as usize) + 2] {
            assert_eq!(policy.retry(count), plateau);
        }
    }

    #[test]
    fn extreme_backoff_preserves_caps_zero_delay_and_attempt_limits() {
        let saturated = ExponentialBackoff {
            base_duration: Duration::MAX,
            max_times: None,
            max_delay: None,
        };
        for count in [0, 1, 32, usize::MAX] {
            assert_eq!(saturated.retry(count), Some(Duration::MAX));
        }
        let capped = ExponentialBackoff {
            base_duration: Duration::from_millis(1),
            max_delay: Some(Duration::from_millis(100)),
            ..saturated.clone()
        };
        assert_eq!(capped.retry(0), Some(Duration::from_millis(1)));
        for count in [7, 32, usize::MAX] {
            assert_eq!(capped.retry(count), Some(Duration::from_millis(100)));
        }
        #[cfg(target_pointer_width = "64")]
        assert_eq!(
            capped.retry((u32::MAX as usize) + 1),
            Some(Duration::from_millis(100))
        );
        let zero = ExponentialBackoff {
            base_duration: Duration::ZERO,
            ..saturated.clone()
        };
        assert_eq!(zero.retry(usize::MAX), Some(Duration::ZERO));
        let limited = ExponentialBackoff {
            max_times: Some(usize::MAX),
            ..saturated
        };
        assert_eq!(limited.retry(usize::MAX - 1), Some(Duration::MAX));
        assert_eq!(limited.retry(usize::MAX), None);
    }
}
