// Derived from rmcp 3.5.1; maintained and modified by mcp-rs. See ../../../UPSTREAM.md.
use std::{borrow::Cow, sync::OnceLock};

use futures::FutureExt;
#[cfg(not(feature = "local"))]
use futures::future::BoxFuture;
#[cfg(feature = "local")]
use futures::future::LocalBoxFuture;
use thiserror::Error;

// ---------------------------------------------------------------------------
// Conditional Send helpers
//
// `MaybeSend`       – supertrait alias: `Send + Sync` without `local`, empty with `local`
// `MaybeSendFuture` – future bound alias: `Send` without `local`, empty with `local`
// `MaybeBoxFuture`  – boxed future type: `BoxFuture` without `local`, `LocalBoxFuture` with `local`
// ---------------------------------------------------------------------------

#[cfg(not(feature = "local"))]
#[doc(hidden)]
pub trait MaybeSend: Send + Sync {}
#[cfg(not(feature = "local"))]
impl<T: Send + Sync> MaybeSend for T {}

#[cfg(feature = "local")]
#[doc(hidden)]
pub trait MaybeSend {}
#[cfg(feature = "local")]
impl<T> MaybeSend for T {}

#[cfg(not(feature = "local"))]
#[doc(hidden)]
pub trait MaybeSendFuture: Send {}
#[cfg(not(feature = "local"))]
impl<T: Send> MaybeSendFuture for T {}

#[cfg(feature = "local")]
#[doc(hidden)]
pub trait MaybeSendFuture {}
#[cfg(feature = "local")]
impl<T> MaybeSendFuture for T {}

#[cfg(not(feature = "local"))]
pub(crate) type MaybeBoxFuture<'a, T> = BoxFuture<'a, T>;
#[cfg(feature = "local")]
pub(crate) type MaybeBoxFuture<'a, T> = LocalBoxFuture<'a, T>;

#[cfg(feature = "server")]
use crate::model::ClientNotification;
#[cfg(feature = "server")]
use crate::model::ServerJsonRpcMessage;
#[cfg(feature = "client")]
use crate::model::ServerNotification;
use crate::{
    error::ErrorData as McpError,
    model::{
        CancelledNotification, CancelledNotificationParam, ClientCapabilities, Extensions,
        GetExtensions, GetMeta, Implementation, JsonRpcError, JsonRpcMessage, JsonRpcNotification,
        JsonRpcRequest, JsonRpcResponse, NotificationMetaObject, NumberOrString, ProgressToken,
        ProtocolVersion, RequestId, RequestMetaObject,
    },
    transport::{DynamicTransportError, IntoTransport, Transport},
};
#[cfg(feature = "client")]
mod client;
#[cfg(feature = "client")]
pub use client::*;
#[cfg(feature = "server")]
mod server;
#[cfg(feature = "server")]
pub use server::*;
#[cfg(feature = "tower")]
mod tower;
use tokio_util::sync::{CancellationToken, DropGuard};
#[cfg(feature = "tower")]
pub use tower::*;
use tracing::{Instrument as _, instrument};
#[derive(Error, Debug)]
#[non_exhaustive]
pub enum ServiceError {
    #[error("Mcp error: {0}")]
    McpError(McpError),
    #[error("Transport send error: {0}")]
    TransportSend(DynamicTransportError),
    #[error("Transport closed")]
    TransportClosed,
    /// A collection would need another page beyond the configured budget.
    #[error("pagination exceeded the {limit}-page limit")]
    PaginationPageLimitExceeded { limit: usize },
    /// A page would push the collected item count beyond the configured budget.
    #[error("pagination exceeded the {limit}-item limit")]
    PaginationItemLimitExceeded { limit: usize },
    /// The server returned a cursor already used by this collection.
    #[error("pagination returned a repeated cursor")]
    PaginationCursorCycle,
    #[error("Unexpected response type")]
    UnexpectedResponse,
    #[error("subscription consumer lagged behind its {capacity}-message buffer")]
    SubscriptionLagged { capacity: usize },
    #[error("task cancelled for reason {}", reason.as_deref().unwrap_or("<unknown>"))]
    Cancelled { reason: Option<String> },
    #[error("request timeout after {}", chrono::Duration::from_std(*timeout).unwrap_or_default())]
    Timeout { timeout: Duration },
    /// An input-required batch exceeded the per-round callback budget.
    #[error("input_required exceeded the {limit}-input per-round limit")]
    InputRequiredRequestsExceeded { limit: usize },
    /// The peer kept returning `input_required` beyond the configured round cap.
    #[error("input_required did not complete within {max_rounds} MRTR rounds")]
    InputRequiredRoundsExceeded { max_rounds: usize },
}

trait TransferObject:
    std::fmt::Debug + Clone + serde::Serialize + serde::de::DeserializeOwned + Send + Sync + 'static
{
}

impl<T> TransferObject for T where
    T: std::fmt::Debug
        + serde::Serialize
        + serde::de::DeserializeOwned
        + Send
        + Sync
        + 'static
        + Clone
{
}

#[allow(private_bounds, reason = "there's no the third implementation")]
pub trait ServiceRole: std::fmt::Debug + Send + Sync + 'static + Copy + Clone {
    type Req: TransferObject + GetMeta<Metadata = RequestMetaObject> + GetExtensions;
    type Resp: TransferObject;
    type Not: TryInto<CancelledNotification, Error = Self::Not>
        + From<CancelledNotification>
        + TransferObject;
    type PeerReq: TransferObject + GetMeta<Metadata = RequestMetaObject> + GetExtensions;
    type PeerResp: TransferObject;
    type PeerNot: TryInto<CancelledNotification, Error = Self::PeerNot>
        + From<CancelledNotification>
        + TransferObject
        + GetMeta<Metadata = NotificationMetaObject>
        + GetExtensions;
    type InitializeError;
    const IS_CLIENT: bool;
    type Info: TransferObject;
    type PeerInfo: TransferObject;
    #[doc(hidden)]
    fn configure_direct_peer(_peer: &Peer<Self>, _info: &Self::Info) {}
    #[doc(hidden)]
    fn peer_cancelled_params(_notification: &Self::PeerNot) -> Option<&CancelledNotificationParam> {
        None
    }
    #[doc(hidden)]
    fn peer_cancels_subscriptions(_peer: &Peer<Self>) -> bool {
        false
    }
    #[doc(hidden)]
    fn is_subscription_request(_request: &Self::Req) -> bool {
        false
    }
    /// Invalidate any response cache affected by an inbound peer notification.
    ///
    /// The serve loop calls this for every notification *before* subscription
    /// routing, so cache invalidation still runs when a notification is
    /// delivered through a `listen` subscription channel rather than the
    /// [`Service::handle_notification`] callbacks.
    #[doc(hidden)]
    fn invalidate_response_cache(
        _peer: &Peer<Self>,
        _notification: &Self::PeerNot,
    ) -> impl Future<Output = ()> + MaybeSendFuture {
        async {}
    }

    #[doc(hidden)]
    fn enforce_request_association(
        _request: &Self::Req,
        _peer_info: Option<&Self::PeerInfo>,
        _in_request_handler_scope: bool,
    ) -> Result<(), ServiceError> {
        Ok(())
    }

    /// Receive-side counterpart of [`Self::enforce_request_association`]:
    /// SEP-2260 says clients receiving a server-to-client request with no
    /// associated outbound request should reject it with invalid params. An
    /// error return is sent back to the peer instead of dispatching to the
    /// handler.
    #[doc(hidden)]
    fn enforce_peer_request_association(
        _peer_request: &Self::PeerReq,
        _peer_info: Option<&Self::PeerInfo>,
        _association: PeerRequestAssociation,
    ) -> Result<(), McpError> {
        Ok(())
    }
}

/// How an inbound peer request relates to this side's in-flight outbound
/// requests (SEP-2260).
///
/// SEP-2260 defines no wire field for association, so only stream-separating
/// transports (streamable HTTP) can observe it; other transports yield
/// [`Self::Unknown`].
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[expect(clippy::exhaustive_enums, reason = "intentionally exhaustive")]
pub enum PeerRequestAssociation {
    /// Arrived on the response stream of an in-flight outbound request.
    Associated,
    /// Arrived on a stream tied to no in-flight outbound request (e.g. the
    /// streamable HTTP standalone GET stream).
    Unassociated,
    /// The transport cannot distinguish streams; only the coarse in-flight
    /// signal is available.
    Unknown { has_pending_outbound_request: bool },
}

#[cfg(feature = "client")]
pub(crate) fn uses_legacy_lifecycle(
    _protocol_version: Option<&ProtocolVersion>,
    _uses_discover_lifecycle: bool,
) -> bool {
    false
}

pub(crate) fn peer_request_association<Req: crate::model::GetExtensions, V>(
    request: &Req,
    local_responder_pool: &std::collections::HashMap<RequestId, V>,
) -> PeerRequestAssociation {
    match request.extensions().get::<InboundStreamOrigin>() {
        None => PeerRequestAssociation::Unknown {
            has_pending_outbound_request: !local_responder_pool.is_empty(),
        },
        Some(InboundStreamOrigin::Unassociated) => PeerRequestAssociation::Unassociated,
        Some(InboundStreamOrigin::OutboundRequest(id)) => {
            if local_responder_pool.contains_key(id) {
                PeerRequestAssociation::Associated
            } else {
                PeerRequestAssociation::Unassociated
            }
        }
    }
}

tokio::task_local! {
    pub(crate) static ORIGINATING_REQUEST: RequestId;
}

pub(crate) fn in_request_handler_scope() -> bool {
    ORIGINATING_REQUEST.try_with(|_| ()).is_ok()
}

/// Marker in an outbound request's non-serialized [`Extensions`] identifying
/// the in-flight peer request it was issued from (SEP-2260). Attached for both
/// roles whenever a request is sent from within a request handler; the
/// streamable HTTP server reads it to deliver server-initiated requests on the
/// originating request's SSE stream. Never on the wire (SEP-2260 defines no
/// wire field), so session managers that serialize messages between processes
/// lose it and such requests fall back to the standalone stream with a warning.
///
/// # Caller requirements
///
/// From protocol version `2026-07-28`, server-to-client sampling, roots, and
/// elicitation requests must be issued while handling a client request;
/// outside a handler they return an `invalid_request` error. The association
/// is task-local and does not cross `tokio::spawn`, so use the task manager
/// for long-running work.
///
/// The client receive-side mirror is [`InboundStreamOrigin`].
#[derive(Debug, Clone, PartialEq, Eq)]
#[expect(clippy::exhaustive_structs, reason = "intentionally exhaustive")]
pub struct OriginatingRequestId(pub RequestId);

/// Marker in an inbound request's non-serialized [`Extensions`] recording
/// which HTTP response stream it arrived on: the receive-side mirror of
/// [`OriginatingRequestId`]. Never on the wire (SEP-2260 defines no wire
/// field); when absent, the coarse in-flight check applies.
#[derive(Debug, Clone, PartialEq, Eq)]
#[expect(clippy::exhaustive_enums, reason = "intentionally exhaustive")]
pub enum InboundStreamOrigin {
    /// The standalone GET stream, or a POST response stream not tied to an
    /// outbound request.
    Unassociated,
    /// The SSE response stream of the POST that carried this outbound request.
    OutboundRequest(RequestId),
}

pub type TxJsonRpcMessage<R> =
    JsonRpcMessage<<R as ServiceRole>::Req, <R as ServiceRole>::Resp, <R as ServiceRole>::Not>;
pub type RxJsonRpcMessage<R> = JsonRpcMessage<
    <R as ServiceRole>::PeerReq,
    <R as ServiceRole>::PeerResp,
    <R as ServiceRole>::PeerNot,
>;

#[cfg(not(feature = "local"))]
pub trait Service<R: ServiceRole>: Send + Sync + 'static {
    fn handle_request(
        &self,
        request: R::PeerReq,
        context: RequestContext<R>,
    ) -> impl Future<Output = Result<R::Resp, McpError>> + MaybeSendFuture + '_;
    fn handle_notification(
        &self,
        notification: R::PeerNot,
        context: NotificationContext<R>,
    ) -> impl Future<Output = Result<(), McpError>> + MaybeSendFuture + '_;
    fn get_info(&self) -> R::Info;
    /// The protocol versions this service can speak, bounding what `initialize`
    /// negotiation may agree to.
    ///
    /// Servers normally override
    /// [`ServerHandler::supported_protocol_versions`] instead of this method;
    /// the blanket `Service` impl forwards to it. This method exists so the
    /// transport and handshake layers, which see only a `Service`, can read the
    /// list and avoid agreeing to a version the server cannot serve.
    ///
    /// [`ServerHandler::supported_protocol_versions`]: crate::handler::server::ServerHandler::supported_protocol_versions
    fn supported_protocol_versions(&self) -> Cow<'static, [ProtocolVersion]> {
        Cow::Borrowed(&[ProtocolVersion::V_2026_07_28])
    }
}

#[cfg(feature = "local")]
pub trait Service<R: ServiceRole>: 'static {
    fn handle_request(
        &self,
        request: R::PeerReq,
        context: RequestContext<R>,
    ) -> impl Future<Output = Result<R::Resp, McpError>> + MaybeSendFuture + '_;
    fn handle_notification(
        &self,
        notification: R::PeerNot,
        context: NotificationContext<R>,
    ) -> impl Future<Output = Result<(), McpError>> + MaybeSendFuture + '_;
    fn get_info(&self) -> R::Info;
    /// The protocol versions this service can speak.
    ///
    /// See the non-`local` variant of this trait for details.
    fn supported_protocol_versions(&self) -> Cow<'static, [ProtocolVersion]> {
        Cow::Borrowed(&[ProtocolVersion::V_2026_07_28])
    }
}

pub trait ServiceExt<R: ServiceRole>: Service<R> + Sized {
    /// Convert this service to a dynamic boxed service
    ///
    /// This could be very helpful when you want to store the services in a collection
    fn into_dyn(self) -> Box<dyn DynService<R>> {
        Box::new(self)
    }
    fn serve<T, E, A>(
        self,
        transport: T,
    ) -> impl Future<Output = Result<RunningService<R, Self>, R::InitializeError>> + MaybeSendFuture
    where
        T: IntoTransport<R, E, A>,
        E: std::error::Error + Send + Sync + 'static,
        Self: Sized,
    {
        Self::serve_with_ct(self, transport, Default::default())
    }
    fn serve_with_ct<T, E, A>(
        self,
        transport: T,
        ct: CancellationToken,
    ) -> impl Future<Output = Result<RunningService<R, Self>, R::InitializeError>> + MaybeSendFuture
    where
        T: IntoTransport<R, E, A>,
        E: std::error::Error + Send + Sync + 'static,
        Self: Sized;
}

impl<R: ServiceRole> Service<R> for Box<dyn DynService<R>> {
    fn handle_request(
        &self,
        request: R::PeerReq,
        context: RequestContext<R>,
    ) -> impl Future<Output = Result<R::Resp, McpError>> + MaybeSendFuture + '_ {
        DynService::handle_request(self.as_ref(), request, context)
    }

    fn handle_notification(
        &self,
        notification: R::PeerNot,
        context: NotificationContext<R>,
    ) -> impl Future<Output = Result<(), McpError>> + MaybeSendFuture + '_ {
        DynService::handle_notification(self.as_ref(), notification, context)
    }

    fn get_info(&self) -> R::Info {
        DynService::get_info(self.as_ref())
    }

    fn supported_protocol_versions(&self) -> Cow<'static, [ProtocolVersion]> {
        DynService::supported_protocol_versions(self.as_ref())
    }
}

#[cfg(not(feature = "local"))]
pub trait DynService<R: ServiceRole>: Send + Sync {
    fn handle_request(
        &self,
        request: R::PeerReq,
        context: RequestContext<R>,
    ) -> MaybeBoxFuture<'_, Result<R::Resp, McpError>>;
    fn handle_notification(
        &self,
        notification: R::PeerNot,
        context: NotificationContext<R>,
    ) -> MaybeBoxFuture<'_, Result<(), McpError>>;
    fn get_info(&self) -> R::Info;
    /// See [`Service::supported_protocol_versions`].
    fn supported_protocol_versions(&self) -> Cow<'static, [ProtocolVersion]> {
        Cow::Borrowed(&[ProtocolVersion::V_2026_07_28])
    }
}

#[cfg(feature = "local")]
pub trait DynService<R: ServiceRole> {
    fn handle_request(
        &self,
        request: R::PeerReq,
        context: RequestContext<R>,
    ) -> MaybeBoxFuture<'_, Result<R::Resp, McpError>>;
    fn handle_notification(
        &self,
        notification: R::PeerNot,
        context: NotificationContext<R>,
    ) -> MaybeBoxFuture<'_, Result<(), McpError>>;
    fn get_info(&self) -> R::Info;
    /// See [`Service::supported_protocol_versions`].
    fn supported_protocol_versions(&self) -> Cow<'static, [ProtocolVersion]> {
        Cow::Borrowed(&[ProtocolVersion::V_2026_07_28])
    }
}

impl<R: ServiceRole, S: Service<R>> DynService<R> for S {
    fn handle_request(
        &self,
        request: R::PeerReq,
        context: RequestContext<R>,
    ) -> MaybeBoxFuture<'_, Result<R::Resp, McpError>> {
        Box::pin(self.handle_request(request, context))
    }
    fn handle_notification(
        &self,
        notification: R::PeerNot,
        context: NotificationContext<R>,
    ) -> MaybeBoxFuture<'_, Result<(), McpError>> {
        Box::pin(self.handle_notification(notification, context))
    }
    fn get_info(&self) -> R::Info {
        self.get_info()
    }
    fn supported_protocol_versions(&self) -> Cow<'static, [ProtocolVersion]> {
        Service::supported_protocol_versions(self)
    }
}

use std::{
    collections::{HashMap, VecDeque},
    ops::Deref,
    sync::{Arc, atomic::AtomicU64},
    time::Duration,
};

use tokio::sync::mpsc;

pub trait RequestIdProvider: Send + Sync + 'static {
    fn next_request_id(&self) -> RequestId;
}

pub trait ProgressTokenProvider: Send + Sync + 'static {
    fn next_progress_token(&self) -> ProgressToken;
}

pub type AtomicU32RequestIdProvider = AtomicU32Provider;
pub type AtomicU32ProgressTokenProvider = AtomicU32Provider;

pub(crate) fn remove_pending_request<T>(
    pending_requests: &mut HashMap<RequestId, T>,
    response_id: &RequestId,
) -> Option<T> {
    pending_requests.remove(response_id).or_else(|| {
        response_id
            .numeric_string_value()
            .and_then(|id| pending_requests.remove(&RequestId::Number(id)))
    })
}

#[derive(Debug, Default)]
pub struct AtomicU32Provider {
    id: AtomicU64,
}

impl RequestIdProvider for AtomicU32Provider {
    fn next_request_id(&self) -> RequestId {
        let id = self.id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        // Safe conversion: we start at 0 and increment by 1, so we won't overflow i64::MAX in practice
        RequestId::Number(id as i64)
    }
}

impl ProgressTokenProvider for AtomicU32Provider {
    fn next_progress_token(&self) -> ProgressToken {
        let id = self.id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        ProgressToken(NumberOrString::Number(id as i64))
    }
}

#[doc(hidden)]
pub trait ProgressNotificationToken {
    fn progress_token(&self) -> Option<&ProgressToken>;
}

#[cfg(feature = "server")]
impl ProgressNotificationToken for ClientNotification {
    fn progress_token(&self) -> Option<&ProgressToken> {
        match self {
            ClientNotification::ProgressNotification(notification) => {
                Some(&notification.params.progress_token)
            }
            _ => None,
        }
    }
}

#[cfg(feature = "client")]
impl ProgressNotificationToken for ServerNotification {
    fn progress_token(&self) -> Option<&ProgressToken> {
        match self {
            ServerNotification::ProgressNotification(notification) => {
                Some(&notification.params.progress_token)
            }
            _ => None,
        }
    }
}

type Responder<T> = tokio::sync::oneshot::Sender<T>;
struct PendingResponse<T> {
    responder: Responder<T>,
    is_subscription: bool,
}
type PendingResponseMap<T> = HashMap<RequestId, PendingResponse<Result<T, ServiceError>>>;
#[derive(Debug, Default, Clone)]
struct ProgressTimeoutWatchers(Arc<std::sync::Mutex<HashMap<ProgressToken, mpsc::Sender<()>>>>);

impl ProgressTimeoutWatchers {
    // Only short map operations run under this lock; no callbacks or awaits.
    // Synchronous removal lets request drops clean up without a runtime or spawn.
    fn lock(&self) -> std::sync::MutexGuard<'_, HashMap<ProgressToken, mpsc::Sender<()>>> {
        self.0
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn insert(&self, token: ProgressToken, sender: mpsc::Sender<()>) {
        self.lock().insert(token, sender);
    }

    fn remove(&self, token: &ProgressToken) {
        self.lock().remove(token);
    }

    fn sender(&self, token: &ProgressToken) -> Option<mpsc::Sender<()>> {
        self.lock().get(token).cloned()
    }

    #[cfg(all(test, feature = "client"))]
    fn is_empty(&self) -> bool {
        self.lock().is_empty()
    }
}
type SubscriptionChannel<N> = (mpsc::Sender<N>, usize);
type SubscriptionChannelMap<N> = HashMap<RequestId, SubscriptionChannel<N>>;

/// A handle to a remote request
///
/// You can cancel it by call [`RequestHandle::cancel`] with a reason,
///
/// or wait for response by call [`RequestHandle::await_response`].
///
/// Dropping the handle or its response future removes local progress and
/// subscription routing registrations. It does not cancel remote execution;
/// use [`RequestHandle::cancel`] to send a cancellation notification.
#[derive(Debug)]
#[non_exhaustive]
pub struct RequestHandle<R: ServiceRole> {
    pub rx: tokio::sync::oneshot::Receiver<Result<R::PeerResp, ServiceError>>,
    pub options: PeerRequestOptions,
    pub peer: Peer<R>,
    pub id: RequestId,
    pub progress_token: ProgressToken,
    progress_reset_rx: Option<mpsc::Receiver<()>>,
    routing_cleanup: RequestRoutingCleanup<R>,
}

// Local-only metadata lets response streams outlive their POST send without
// outliving the caller. Request extensions are never serialized.
#[cfg(feature = "transport-streamable-http-client")]
#[derive(Clone, Debug)]
pub(crate) struct OutboundRequestLifetime(pub(crate) CancellationToken);

// The shared response table must also be cleared when the service task is
// aborted or unwinds, since callers may retain Peer clones after the task exits.
struct PendingResponsesCleanup<R: ServiceRole>(Peer<R>);

impl<R: ServiceRole> Drop for PendingResponsesCleanup<R> {
    fn drop(&mut self) {
        self.0.clear_pending_responses();
    }
}

// A field guard preserves the handle's public movable fields while retaining the
// original registration keys, even if callers modify the public ID/token fields.
#[derive(Debug)]
struct RequestRoutingCleanup<R: ServiceRole> {
    peer: Peer<R>,
    id: RequestId,
    progress_token: Option<ProgressToken>,
    lifetime: CancellationToken,
}

impl<R: ServiceRole> RequestRoutingCleanup<R> {
    fn cleanup(&self) {
        self.lifetime.cancel();
        self.peer.take_pending_response(&self.id);
        if let Some(token) = &self.progress_token {
            self.peer.progress_timeout_watchers.remove(token);
        }
        self.peer.unregister_subscription(&self.id);
    }
}

impl<R: ServiceRole> Drop for RequestRoutingCleanup<R> {
    fn drop(&mut self) {
        self.cleanup();
    }
}

impl<R: ServiceRole> RequestHandle<R> {
    pub const REQUEST_TIMEOUT_REASON: &str = "request timeout";
    pub const REQUEST_MAX_TOTAL_TIMEOUT_REASON: &str = "maximum total timeout exceeded";

    /// Wait for the response using this handle's response-wait deadlines.
    ///
    /// On expiry, cancellation is enqueued without waiting for queue capacity or
    /// transport acknowledgment. A full/closed queue can discard cancellation;
    /// remote termination is not guaranteed. These deadlines start when this
    /// method is polled, after request queue admission, and do not cover MRTR
    /// callback work. Explicit [`Self::cancel`] still waits for notification delivery.
    pub async fn await_response(mut self) -> Result<R::PeerResp, ServiceError> {
        let timeout = self.options.timeout;
        let max_total_timeout = self.options.max_total_timeout;
        let reset_timeout_on_progress = self.options.reset_timeout_on_progress;

        match (timeout, max_total_timeout, reset_timeout_on_progress) {
            (Some(timeout), None, false) => match tokio::time::timeout(timeout, &mut self.rx).await
            {
                Ok(response) => response.map_err(|_e| ServiceError::TransportClosed)?,
                Err(_) => {
                    let error = Err(ServiceError::Timeout { timeout });
                    // cancel this request
                    self.send_timeout_cancel_notification(Self::REQUEST_TIMEOUT_REASON);
                    error
                }
            },
            (None, None, _) => (&mut self.rx)
                .await
                .map_err(|_e| ServiceError::TransportClosed)?,
            _ => {
                self.await_response_with_progress_timeout(
                    timeout,
                    max_total_timeout,
                    reset_timeout_on_progress,
                )
                .await
            }
        }
    }

    fn send_timeout_cancel_notification(&self, reason: &str) {
        self.peer.unregister_subscription(&self.id);
        // Timeout completion must not wait for queue capacity or transport acknowledgment.
        self.peer
            .try_cancel_request(self.id.clone(), Some(reason.to_owned()));
    }

    async fn await_response_with_progress_timeout(
        &mut self,
        timeout: Option<Duration>,
        max_total_timeout: Option<Duration>,
        reset_timeout_on_progress: bool,
    ) -> Result<R::PeerResp, ServiceError> {
        let mut idle_sleep =
            timeout.map(|timeout| (timeout, Box::pin(tokio::time::sleep(timeout))));
        let mut max_total_sleep =
            max_total_timeout.map(|timeout| (timeout, Box::pin(tokio::time::sleep(timeout))));

        loop {
            tokio::select! {
                biased;

                response = &mut self.rx => {
                    return response.map_err(|_e| ServiceError::TransportClosed)?;
                }
                _ = async {
                    if let Some((_, sleep)) = idle_sleep.as_mut() {
                        sleep.as_mut().await;
                    }
                }, if idle_sleep.is_some() => {
                    if let Some((timeout, _)) = idle_sleep.as_ref() {
                        self.send_timeout_cancel_notification(Self::REQUEST_TIMEOUT_REASON);
                        return Err(ServiceError::Timeout { timeout: *timeout });
                    }
                }
                _ = async {
                    if let Some((_, sleep)) = max_total_sleep.as_mut() {
                        sleep.as_mut().await;
                    }
                }, if max_total_sleep.is_some() => {
                    if let Some((timeout, _)) = max_total_sleep.as_ref() {
                        self.send_timeout_cancel_notification(Self::REQUEST_MAX_TOTAL_TIMEOUT_REASON);
                        return Err(ServiceError::Timeout { timeout: *timeout });
                    }
                }
                progress = async {
                    match self.progress_reset_rx.as_mut() {
                        Some(rx) => rx.recv().await,
                        None => None,
                    }
                }, if reset_timeout_on_progress && idle_sleep.is_some() && self.progress_reset_rx.is_some() => {
                    if progress.is_some()
                        && let Some((timeout, sleep)) = idle_sleep.as_mut() {
                            sleep.as_mut().reset(tokio::time::Instant::now() + *timeout);
                        }
                }
            }
        }
    }

    /// Cancel this request
    pub async fn cancel(self, reason: Option<String>) -> Result<(), ServiceError> {
        self.routing_cleanup.cleanup();
        let notification = CancelledNotification {
            params: CancelledNotificationParam {
                request_id: Some(self.id),
                reason,
                meta: None,
            },
            method: crate::model::CancelledNotificationMethod,
            extensions: Default::default(),
        };
        self.peer.send_notification(notification.into()).await?;
        Ok(())
    }
}

#[derive(Debug)]
pub(crate) enum PeerSinkMessage<R: ServiceRole> {
    Request {
        request: R::Req,
        id: RequestId,
        responder: Responder<Result<R::PeerResp, ServiceError>>,
        lifetime: CancellationToken,
    },
    Notification {
        notification: R::Not,
        responder: Responder<Result<(), ServiceError>>,
    },
}

#[derive(Debug, Clone)]
pub(crate) struct ClientRequestMetadata {
    pub protocol_version: ProtocolVersion,
    pub client_info: Implementation,
    pub client_capabilities: ClientCapabilities,
}

/// An interface to fetch the remote client or server
///
/// For general purpose, call [`Peer::send_request`] or [`Peer::send_notification`] to send message to remote peer.
///
/// To create a cancellable request, call [`Peer::send_request_with_option`].
pub struct Peer<R: ServiceRole> {
    tx: mpsc::Sender<PeerSinkMessage<R>>,
    request_id_provider: Arc<dyn RequestIdProvider>,
    progress_token_provider: Arc<dyn ProgressTokenProvider>,
    progress_timeout_watchers: ProgressTimeoutWatchers,
    pending_responses: Arc<std::sync::Mutex<PendingResponseMap<R::PeerResp>>>,
    info: Arc<std::sync::RwLock<Option<Arc<R::PeerInfo>>>>,
    client_request_metadata: Arc<OnceLock<ClientRequestMetadata>>,
    request_metadata_required: Arc<std::sync::atomic::AtomicBool>,
    subscription_channels: Arc<std::sync::RwLock<SubscriptionChannelMap<R::PeerNot>>>,
    #[cfg(feature = "client")]
    response_cache: client::cache::PeerResponseCache<R>,
}

impl<R: Clone + ServiceRole> Clone for Peer<R>
where
    R::PeerInfo: Clone,
{
    fn clone(&self) -> Peer<R> {
        Self {
            tx: self.tx.clone(),
            request_id_provider: self.request_id_provider.clone(),
            progress_token_provider: self.progress_token_provider.clone(),
            progress_timeout_watchers: self.progress_timeout_watchers.clone(),
            pending_responses: self.pending_responses.clone(),
            info: self.info.clone(),
            client_request_metadata: self.client_request_metadata.clone(),
            request_metadata_required: self.request_metadata_required.clone(),
            subscription_channels: self.subscription_channels.clone(),
            #[cfg(feature = "client")]
            response_cache: self.response_cache.clone(),
        }
    }
}

impl<R: ServiceRole> std::fmt::Debug for Peer<R> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PeerSink")
            .field("tx", &self.tx)
            .field("is_client", &R::IS_CLIENT)
            .finish()
    }
}

type ProxyOutbound<R> = mpsc::Receiver<PeerSinkMessage<R>>;

#[derive(Debug, Default)]
#[non_exhaustive]
/// Deadlines apply to response waiting, starting when `RequestHandle::await_response`
/// is polled. They exclude outgoing request queue admission and MRTR callbacks.
pub struct PeerRequestOptions {
    /// Response deadline, or idle deadline when reset on progress is enabled.
    pub timeout: Option<Duration>,
    pub meta: Option<RequestMetaObject>,
    /// Reset the request timeout when a matching progress notification is received.
    pub reset_timeout_on_progress: bool,
    /// Maximum response-wait duration, regardless of progress notifications.
    pub max_total_timeout: Option<Duration>,
}

impl PeerRequestOptions {
    pub fn no_options() -> Self {
        Self::default()
    }

    pub fn with_timeout(timeout: Duration) -> Self {
        Self {
            timeout: Some(timeout),
            ..Self::default()
        }
    }

    /// Adds request metadata while preserving any other configured options.
    ///
    /// Explicit values take precedence over discover-lifecycle metadata defaults.
    pub fn with_meta(mut self, meta: RequestMetaObject) -> Self {
        self.meta = Some(meta);
        self
    }

    pub fn reset_timeout_on_progress(mut self) -> Self {
        self.reset_timeout_on_progress = true;
        self
    }

    pub fn with_max_total_timeout(mut self, timeout: Duration) -> Self {
        self.max_total_timeout = Some(timeout);
        self
    }
}

impl<R: ServiceRole> Peer<R> {
    const CLIENT_CHANNEL_BUFFER_SIZE: usize = 1024;
    pub(crate) fn new(
        request_id_provider: Arc<dyn RequestIdProvider>,
        peer_info: Option<R::PeerInfo>,
    ) -> (Peer<R>, ProxyOutbound<R>) {
        let (tx, rx) = mpsc::channel(Self::CLIENT_CHANNEL_BUFFER_SIZE);
        (
            Self {
                tx,
                request_id_provider,
                progress_token_provider: Arc::new(AtomicU32ProgressTokenProvider::default()),
                progress_timeout_watchers: Default::default(),
                pending_responses: Default::default(),
                info: Arc::new(std::sync::RwLock::new(peer_info.map(Arc::new))),
                client_request_metadata: Default::default(),
                request_metadata_required: Default::default(),
                subscription_channels: Default::default(),
                #[cfg(feature = "client")]
                response_cache: Default::default(),
            },
            rx,
        )
    }
    pub async fn send_notification(&self, notification: R::Not) -> Result<(), ServiceError> {
        let (responder, receiver) = tokio::sync::oneshot::channel();
        self.tx
            .send(PeerSinkMessage::Notification {
                notification,
                responder,
            })
            .await
            .map_err(|_m| ServiceError::TransportClosed)?;
        receiver.await.map_err(|_e| ServiceError::TransportClosed)?
    }
    pub async fn send_request(&self, request: R::Req) -> Result<R::PeerResp, ServiceError> {
        self.send_request_with_option(request, PeerRequestOptions::no_options())
            .await?
            .await_response()
            .await
    }

    pub async fn send_cancellable_request(
        &self,
        request: R::Req,
        options: PeerRequestOptions,
    ) -> Result<RequestHandle<R>, ServiceError> {
        self.send_request_with_option(request, options).await
    }

    pub async fn send_request_with_option(
        &self,
        request: R::Req,
        options: PeerRequestOptions,
    ) -> Result<RequestHandle<R>, ServiceError> {
        self.send_request_with_option_and_subscription(request, options, None)
            .await
    }

    async fn send_request_with_option_and_subscription(
        &self,
        mut request: R::Req,
        options: PeerRequestOptions,
        subscription_sender: Option<SubscriptionChannel<R::PeerNot>>,
    ) -> Result<RequestHandle<R>, ServiceError> {
        R::enforce_request_association(
            &request,
            self.peer_info().as_deref(),
            in_request_handler_scope(),
        )?;
        if let Ok(originating) = ORIGINATING_REQUEST.try_with(|id| id.clone()) {
            request
                .extensions_mut()
                .insert(OriginatingRequestId(originating));
        }
        let id = self.request_id_provider.next_request_id();
        let progress_token = self.progress_token_provider.next_progress_token();
        if let Some(metadata) = self.client_request_metadata.get() {
            let meta = request.get_meta_mut();
            meta.set_protocol_version(metadata.protocol_version.clone());
            meta.set_client_info(metadata.client_info.clone());
            meta.set_client_capabilities(metadata.client_capabilities.clone());
        }
        if let Some(meta) = options.meta.clone() {
            request.get_meta_mut().extend(meta);
        }
        request
            .get_meta_mut()
            .set_progress_token(progress_token.clone());
        // Reserve before registering routing state: cancellation while the queue
        // is full must not leave an orphan subscription or progress watcher.
        let permit = self
            .tx
            .reserve()
            .await
            .map_err(|_| ServiceError::TransportClosed)?;
        let (responder, receiver) = tokio::sync::oneshot::channel();
        let progress_reset_rx = if options.reset_timeout_on_progress && options.timeout.is_some() {
            let (sender, receiver) = mpsc::channel(1);
            self.progress_timeout_watchers
                .insert(progress_token.clone(), sender);
            Some(receiver)
        } else {
            None
        };
        if let Some(channel) = subscription_sender {
            self.subscription_channels_write()
                .insert(id.clone(), channel);
        }
        let lifetime = CancellationToken::new();
        #[cfg(feature = "transport-streamable-http-client")]
        request
            .extensions_mut()
            .insert(OutboundRequestLifetime(lifetime.clone()));
        permit.send(PeerSinkMessage::Request {
            request,
            id: id.clone(),
            responder,
            lifetime: lifetime.clone(),
        });
        let routing_cleanup = RequestRoutingCleanup {
            lifetime,
            peer: self.clone(),
            id: id.clone(),
            progress_token: progress_reset_rx.as_ref().map(|_| progress_token.clone()),
        };
        Ok(RequestHandle {
            routing_cleanup,
            id,
            rx: receiver,
            progress_token,
            options,
            peer: self.clone(),
            progress_reset_rx,
        })
    }

    #[cfg(feature = "client")]
    pub(crate) async fn send_subscription_request(
        &self,
        request: R::Req,
        options: PeerRequestOptions,
        channel_capacity: usize,
    ) -> Result<(RequestHandle<R>, mpsc::Receiver<R::PeerNot>), ServiceError> {
        let (sender, receiver) = mpsc::channel(channel_capacity);
        let handle = self
            .send_request_with_option_and_subscription(
                request,
                options,
                Some((sender, channel_capacity)),
            )
            .await?;
        Ok((handle, receiver))
    }

    fn pending_responses_lock(&self) -> std::sync::MutexGuard<'_, PendingResponseMap<R::PeerResp>> {
        self.pending_responses
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn take_pending_response(
        &self,
        id: &RequestId,
    ) -> Option<PendingResponse<Result<R::PeerResp, ServiceError>>> {
        self.pending_responses_lock().remove(id)
    }

    fn take_wire_response(
        &self,
        id: &RequestId,
    ) -> Option<PendingResponse<Result<R::PeerResp, ServiceError>>> {
        remove_pending_request(&mut self.pending_responses_lock(), id)
    }

    fn clear_pending_responses(&self) {
        let responses = std::mem::take(&mut *self.pending_responses_lock());
        // Drop senders (and wake receivers) after releasing the map lock.
        drop(responses);
    }

    fn subscription_sender(&self, id: &RequestId) -> Option<SubscriptionChannel<R::PeerNot>> {
        self.subscription_channels_read().get(id).cloned()
    }

    pub(crate) fn unregister_subscription(&self, id: &RequestId) {
        self.subscription_channels_write().remove(id);
    }

    fn subscription_channels_read(
        &self,
    ) -> std::sync::RwLockReadGuard<'_, SubscriptionChannelMap<R::PeerNot>> {
        match self.subscription_channels.read() {
            Ok(channels) => channels,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    fn subscription_channels_write(
        &self,
    ) -> std::sync::RwLockWriteGuard<'_, SubscriptionChannelMap<R::PeerNot>> {
        match self.subscription_channels.write() {
            Ok(channels) => channels,
            Err(poisoned) => poisoned.into_inner(),
        }
    }

    pub(crate) fn try_cancel_request(&self, id: RequestId, reason: Option<String>) {
        let notification = CancelledNotification {
            params: CancelledNotificationParam {
                request_id: Some(id),
                reason,
                meta: None,
            },
            method: crate::model::CancelledNotificationMethod,
            extensions: Default::default(),
        };
        let (responder, _receiver) = tokio::sync::oneshot::channel();
        let _ = self.tx.try_send(PeerSinkMessage::Notification {
            notification: notification.into(),
            responder,
        });
    }

    fn notify_progress_timeout_watcher(&self, progress_token: &ProgressToken) {
        let sender = self.progress_timeout_watchers.sender(progress_token);
        if let Some(sender) = sender {
            match sender.try_send(()) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    tracing::trace!(?progress_token, "progress timeout watcher channel is full");
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    self.progress_timeout_watchers.remove(progress_token);
                }
            }
        }
    }

    /// Snapshot of the peer's handshake info.
    pub fn peer_info(&self) -> Option<Arc<R::PeerInfo>> {
        self.info.read().expect("peer info lock poisoned").clone()
    }

    /// Stores the peer's handshake info, overwriting any previous value.
    pub fn set_peer_info(&self, info: R::PeerInfo) {
        *self.info.write().expect("peer info lock poisoned") = Some(Arc::new(info));
    }

    #[cfg(feature = "client")]
    pub(crate) fn set_client_request_metadata(&self, metadata: ClientRequestMetadata) {
        let result = self.client_request_metadata.set(metadata);
        debug_assert!(result.is_ok(), "client request metadata set more than once");
    }

    #[cfg(feature = "server")]
    pub(crate) fn require_request_metadata(&self) {
        self.request_metadata_required
            .store(true, std::sync::atomic::Ordering::Release);
    }

    #[cfg(feature = "server")]
    pub(crate) fn request_metadata_required(&self) -> bool {
        self.request_metadata_required
            .load(std::sync::atomic::Ordering::Acquire)
    }

    pub fn is_transport_closed(&self) -> bool {
        self.tx.is_closed()
    }
}

#[derive(Debug)]
pub struct RunningService<R: ServiceRole, S: Service<R>> {
    service: Arc<S>,
    peer: Peer<R>,
    handle: Option<tokio::task::JoinHandle<QuitReason>>,
    cancellation_token: CancellationToken,
    dg: DropGuard,
}
impl<R: ServiceRole, S: Service<R>> Deref for RunningService<R, S> {
    type Target = Peer<R>;

    fn deref(&self) -> &Self::Target {
        &self.peer
    }
}

impl<R: ServiceRole, S: Service<R>> RunningService<R, S> {
    #[inline]
    pub fn peer(&self) -> &Peer<R> {
        &self.peer
    }
    #[inline]
    pub fn service(&self) -> &S {
        self.service.as_ref()
    }
    #[inline]
    pub fn cancellation_token(&self) -> RunningServiceCancellationToken {
        RunningServiceCancellationToken(self.cancellation_token.clone())
    }

    /// Returns true if the service has been closed or cancelled.
    #[inline]
    pub fn is_closed(&self) -> bool {
        self.handle.is_none() || self.cancellation_token.is_cancelled()
    }

    /// Wait for the service to complete.
    ///
    /// This will block until the service loop terminates (either due to
    /// cancellation, transport closure, or an error).
    #[inline]
    pub async fn waiting(mut self) -> Result<QuitReason, tokio::task::JoinError> {
        match self.handle.take() {
            Some(handle) => handle.await,
            None => Ok(QuitReason::Closed),
        }
    }

    /// Gracefully close the connection and wait for cleanup to complete.
    ///
    /// This method cancels the service, waits for the background task to finish
    /// (which includes calling `transport.close()`), and ensures all cleanup
    /// operations complete before returning.
    ///
    /// Unlike [`cancel`](Self::cancel), this method takes `&mut self` and can be
    /// called without consuming the `RunningService`. After calling this method,
    /// the service is considered closed and subsequent operations will fail.
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// let mut client = ().serve(transport).await?;
    /// // ... use the client ...
    /// client.close().await?;
    /// ```
    pub async fn close(&mut self) -> Result<QuitReason, tokio::task::JoinError> {
        if let Some(handle) = self.handle.take() {
            // Disarm the drop guard so it doesn't try to cancel again
            // We need to cancel manually and wait for completion
            self.cancellation_token.cancel();
            handle.await
        } else {
            // Already closed
            Ok(QuitReason::Closed)
        }
    }

    /// Gracefully close the connection with a timeout.
    ///
    /// Similar to [`close`](Self::close), but returns after the specified timeout
    /// if the cleanup doesn't complete in time. This is useful for ensuring
    /// a bounded shutdown time.
    ///
    /// Returns `Ok(Some(reason))` if shutdown completed within the timeout,
    /// `Ok(None)` if the timeout was reached, or `Err` if there was a join error.
    pub async fn close_with_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<Option<QuitReason>, tokio::task::JoinError> {
        if let Some(handle) = self.handle.take() {
            self.cancellation_token.cancel();
            match tokio::time::timeout(timeout, handle).await {
                Ok(result) => result.map(Some),
                Err(_elapsed) => {
                    tracing::warn!(
                        "close_with_timeout: cleanup did not complete within {:?}",
                        timeout
                    );
                    Ok(None)
                }
            }
        } else {
            Ok(Some(QuitReason::Closed))
        }
    }

    /// Cancel the service and wait for cleanup to complete.
    ///
    /// This consumes the `RunningService` and ensures the connection is properly
    /// closed. For a non-consuming alternative, see [`close`](Self::close).
    pub async fn cancel(mut self) -> Result<QuitReason, tokio::task::JoinError> {
        // Disarm the drop guard since we're handling cancellation explicitly
        let _ = std::mem::replace(&mut self.dg, self.cancellation_token.clone().drop_guard());
        self.close().await
    }
}

impl<R: ServiceRole, S: Service<R>> Drop for RunningService<R, S> {
    fn drop(&mut self) {
        if self.handle.is_some() && !self.cancellation_token.is_cancelled() {
            tracing::debug!(
                "RunningService dropped without explicit close(). \
                 The connection will be closed asynchronously. \
                 For guaranteed cleanup, call close() or cancel() before dropping."
            );
        }
        // The DropGuard will handle cancellation
    }
}

// use a wrapper type so we can tweak the implementation if needed
pub struct RunningServiceCancellationToken(CancellationToken);

impl RunningServiceCancellationToken {
    pub fn cancel(self) {
        self.0.cancel();
    }
}

#[derive(Debug)]
#[non_exhaustive]
pub enum QuitReason {
    Cancelled,
    Closed,
    JoinError(tokio::task::JoinError),
}

/// Request execution context
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct RequestContext<R: ServiceRole> {
    /// this token will be cancelled when the [`CancelledNotification`] is received.
    pub ct: CancellationToken,
    pub id: RequestId,
    pub meta: RequestMetaObject,
    pub extensions: Extensions,
    /// An interface to fetch the remote client or server
    pub peer: Peer<R>,
}

impl<R: ServiceRole> RequestContext<R> {
    /// Create a new RequestContext.
    pub fn new(id: RequestId, peer: Peer<R>) -> Self {
        Self {
            ct: CancellationToken::new(),
            id,
            meta: RequestMetaObject::default(),
            extensions: Extensions::default(),
            peer,
        }
    }
}

#[cfg(feature = "server")]
impl RequestContext<RoleServer> {
    /// The current request's protocol version, falling back to legacy handshake state.
    pub fn protocol_version(&self) -> Option<crate::model::ProtocolVersion> {
        self.meta.protocol_version().or_else(|| {
            self.peer
                .peer_info()
                .map(|info| info.protocol_version.clone())
        })
    }

    /// The current request's client implementation, falling back only for legacy sessions.
    pub fn client_info(&self) -> Option<Implementation> {
        if self.peer.request_metadata_required() {
            self.meta.client_info()
        } else {
            self.meta
                .client_info()
                .or_else(|| self.peer.peer_info().map(|info| info.client_info.clone()))
        }
    }

    /// The current request's client capabilities, falling back only for legacy sessions.
    pub fn client_capabilities(&self) -> Option<ClientCapabilities> {
        if self.peer.request_metadata_required() {
            self.meta.client_capabilities()
        } else {
            self.meta
                .client_capabilities()
                .or_else(|| self.peer.peer_info().map(|info| info.capabilities.clone()))
        }
    }
}

/// Request execution context
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct NotificationContext<R: ServiceRole> {
    pub meta: NotificationMetaObject,
    pub extensions: Extensions,
    /// An interface to fetch the remote client or server
    pub peer: Peer<R>,
}

/// Use this function to skip initialization process
pub fn serve_directly<R, S, T, E, A>(
    service: S,
    transport: T,
    peer_info: Option<R::PeerInfo>,
) -> RunningService<R, S>
where
    R: ServiceRole,
    R::PeerNot: ProgressNotificationToken,
    S: Service<R>,
    T: IntoTransport<R, E, A>,
    E: std::error::Error + Send + Sync + 'static,
{
    serve_directly_with_ct(service, transport, peer_info, Default::default())
}

/// Use this function to skip initialization process
pub fn serve_directly_with_ct<R, S, T, E, A>(
    service: S,
    transport: T,
    peer_info: Option<R::PeerInfo>,
    ct: CancellationToken,
) -> RunningService<R, S>
where
    R: ServiceRole,
    R::PeerNot: ProgressNotificationToken,
    S: Service<R>,
    T: IntoTransport<R, E, A>,
    E: std::error::Error + Send + Sync + 'static,
{
    let (peer, peer_rx) = Peer::new(Arc::new(AtomicU32RequestIdProvider::default()), peer_info);
    R::configure_direct_peer(&peer, &service.get_info());
    serve_inner(
        service,
        transport.into_transport(),
        peer,
        peer_rx,
        VecDeque::new(),
        ct,
    )
}

/// Spawn a task that may hold `!Send` state when the `local` feature is active.
///
/// Without the `local` feature this is `tokio::spawn` (requires `Future: Send + 'static`).
/// With `local` it uses `tokio::task::spawn_local` (requires only `Future: 'static`).
#[cfg(not(feature = "local"))]
fn spawn_service_task<F>(future: F) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + Send + 'static,
    F::Output: Send + 'static,
{
    tokio::spawn(future)
}

#[cfg(feature = "local")]
fn spawn_service_task<F>(future: F) -> tokio::task::JoinHandle<F::Output>
where
    F: Future + 'static,
    F::Output: 'static,
{
    tokio::task::spawn_local(future)
}

/// Run the service loop over `transport`.
///
/// `initial_messages` are messages the caller already read from `transport`
/// (e.g. the first request of an `initialize`-less session). They are
/// dispatched by the loop, in order, before anything else is read from the
/// transport, so that their handlers run with the loop draining `peer_rx`.
#[instrument(skip_all)]
fn serve_inner<R, S, T>(
    service: S,
    transport: T,
    peer: Peer<R>,
    mut peer_rx: tokio::sync::mpsc::Receiver<PeerSinkMessage<R>>,
    initial_messages: VecDeque<RxJsonRpcMessage<R>>,
    ct: CancellationToken,
) -> RunningService<R, S>
where
    R: ServiceRole,
    R::PeerNot: ProgressNotificationToken,
    S: Service<R>,
    T: Transport<R> + 'static,
{
    const SINK_PROXY_BUFFER_SIZE: usize = 64;
    let (sink_proxy_tx, mut sink_proxy_rx) =
        tokio::sync::mpsc::channel::<TxJsonRpcMessage<R>>(SINK_PROXY_BUFFER_SIZE);
    let peer_info = peer.peer_info();
    if R::IS_CLIENT {
        tracing::info!(?peer_info, "Service initialized as client");
    } else {
        tracing::info!(?peer_info, "Service initialized as server");
    }

    let mut local_ct_pool = HashMap::<RequestId, CancellationToken>::new();
    let shared_service = Arc::new(service);
    // for return
    let service = shared_service.clone();

    // let message_sink = tokio::sync::
    // let mut stream = std::pin::pin!(stream);
    let serve_loop_ct = ct.child_token();
    let peer_return: Peer<R> = peer.clone();
    let current_span = tracing::Span::current();
    let handle = spawn_service_task(async move {
        let _pending_cleanup = PendingResponsesCleanup(peer.clone());
        let mut transport = transport.into_transport();
        let mut batch_messages = initial_messages;
        let mut send_task_set = tokio::task::JoinSet::<SendTaskResult>::new();
        let mut response_send_tasks = tokio::task::JoinSet::<()>::new();
        #[derive(Debug)]
        enum SendTaskResult {
            Request {
                id: RequestId,
                result: Result<(), DynamicTransportError>,
            },
            Notification {
                responder: Responder<Result<(), ServiceError>>,
                cancellation_param: Option<CancelledNotificationParam>,
                result: Result<(), DynamicTransportError>,
            },
        }
        #[derive(Debug)]
        enum Event<R: ServiceRole> {
            ProxyMessage(PeerSinkMessage<R>),
            PeerMessage(RxJsonRpcMessage<R>),
            ToSink(TxJsonRpcMessage<R>),
            SendTaskResult(SendTaskResult),
            ResponseSendTaskResult(Result<(), tokio::task::JoinError>),
        }

        let quit_reason = loop {
            let evt = if let Some(m) = batch_messages.pop_front() {
                Event::PeerMessage(m)
            } else {
                tokio::select! {
                    m = sink_proxy_rx.recv(), if !sink_proxy_rx.is_closed() => {
                        if let Some(m) = m {
                            Event::ToSink(m)
                        } else {
                            continue
                        }
                    }
                    m = transport.receive() => {
                        if let Some(m) = m {
                            Event::PeerMessage(m)
                        } else {
                            // input stream closed
                            tracing::info!("input stream terminated");
                            break QuitReason::Closed
                        }
                    }
                    m = peer_rx.recv(), if !peer_rx.is_closed() => {
                        if let Some(m) = m {
                            Event::ProxyMessage(m)
                        } else {
                            continue
                        }
                    }
                    m = send_task_set.join_next(), if !send_task_set.is_empty() => {
                        let Some(result) = m else {
                            continue
                        };
                        match result {
                            Err(e) => {
                                // join error, which is serious, we should quit.
                                tracing::error!(%e, "send request task encounter a tokio join error");
                                break QuitReason::JoinError(e)
                            }
                            Ok(result) => {
                                Event::SendTaskResult(result)
                            }
                        }
                    }
                    result = response_send_tasks.join_next(), if !response_send_tasks.is_empty() => {
                        Event::ResponseSendTaskResult(
                            result.expect("non-empty response send task set")
                        )
                    }
                    _ = serve_loop_ct.cancelled() => {
                        tracing::info!("task cancelled");
                        break QuitReason::Cancelled
                    }
                }
            };

            tracing::trace!(?evt, "new event");
            match evt {
                Event::SendTaskResult(SendTaskResult::Request { id, result }) => {
                    if let Err(e) = result
                        && let Some(responder) = peer.take_pending_response(&id) {
                            let _ = responder.responder.send(Err(ServiceError::TransportSend(e)));
                        }
                }
                Event::SendTaskResult(SendTaskResult::Notification {
                    responder,
                    result,
                    cancellation_param,
                }) => {
                    let response = if let Err(e) = result {
                        Err(ServiceError::TransportSend(e))
                    } else {
                        Ok(())
                    };
                    let _ = responder.send(response);
                    if let Some(param) = cancellation_param
                        && let Some(request_id) = &param.request_id
                            && let Some(responder) = peer.take_pending_response(request_id) {
                                tracing::info!(id = %request_id, reason = param.reason, "cancelled");
                                let _response_result = responder.responder.send(Err(ServiceError::Cancelled {
                                    reason: param.reason.clone(),
                                }));
                            }
                }
                Event::ResponseSendTaskResult(result) => {
                    if let Err(error) = result {
                        tracing::error!(%error, "response send task failed");
                    }
                }
                // response and error
                Event::ToSink(m) => {
                    if let Some(id) = match &m {
                        JsonRpcMessage::Response(response) => Some(&response.id),
                        JsonRpcMessage::Error(error) => error.id.as_ref(),
                        _ => None,
                    } {
                        let Some(ct) = local_ct_pool.remove(id) else {
                            tracing::debug!(%id, "dropping response for cancelled request");
                            continue;
                        };
                        ct.cancel();
                        let send = transport.send(m);
                        let current_span = tracing::Span::current();
                        response_send_tasks.spawn(async move {
                            let send_result = send.await;
                            if let Err(error) = send_result {
                                tracing::error!(%error, "fail to response message");
                            }
                        }.instrument(current_span));
                    }
                }
                Event::ProxyMessage(PeerSinkMessage::Request {
                    request,
                    id,
                    responder,
                    lifetime,
                }) => {
                    let replaced = {
                        let mut responses = peer.pending_responses_lock();
                        // Admission and drop cleanup share this lock, so a drop cannot
                        // race a late insertion after its cleanup has already run.
                        if lifetime.is_cancelled() || responder.is_closed() {
                            continue;
                        }
                        responses.insert(id.clone(), PendingResponse {
                            responder,
                            is_subscription: R::is_subscription_request(&request),
                        })
                    };
                    drop(replaced);
                    let send = transport.send(JsonRpcMessage::request(request, id.clone()));
                    {
                        let id = id.clone();
                        let current_span = tracing::Span::current();
                        send_task_set.spawn(async move {
                            let result = if T::supports_request_send_cancellation() {
                                tokio::select! {
                                    biased;
                                    _ = lifetime.cancelled() => Ok(()),
                                    result = send => result.map_err(DynamicTransportError::new::<T, R>),
                                }
                            } else {
                                send.await.map_err(DynamicTransportError::new::<T, R>)
                            };
                            SendTaskResult::Request { id, result }
                        }.instrument(current_span));
                    }
                }
                Event::ProxyMessage(PeerSinkMessage::Notification {
                    notification,
                    responder,
                }) => {
                    // catch cancellation notification
                    let mut cancellation_param = None;
                    let notification = match notification.try_into() {
                        Ok::<CancelledNotification, _>(cancelled) => {
                            cancellation_param.replace(cancelled.params.clone());
                            cancelled.into()
                        }
                        Err(notification) => notification,
                    };
                    let send = transport.send(JsonRpcMessage::notification(notification));
                    let current_span = tracing::Span::current();
                    send_task_set.spawn(send.map(move |result| SendTaskResult::Notification {
                        responder,
                        cancellation_param,
                        result: result.map_err(DynamicTransportError::new::<T, R>),
                    }).instrument(current_span));
                }
                Event::PeerMessage(JsonRpcMessage::Request(JsonRpcRequest {
                    id,
                    mut request,
                    ..
                })) => {
                    tracing::debug!(%id, ?request, "received request");
                    let association = peer_request_association(&request, &peer.pending_responses_lock());
                    if let Err(error) = R::enforce_peer_request_association(
                        &request,
                        peer.peer_info().as_deref(),
                        association,
                    ) {
                        tracing::warn!(%id, message = %error.message, "rejected peer request");
                        // send directly: the sink proxy path would drop the
                        // error since the request was never registered in
                        // local_ct_pool
                        let send = transport.send(JsonRpcMessage::error(error, Some(id)));
                        let current_span = tracing::Span::current();
                        response_send_tasks.spawn(async move {
                            if let Err(error) = send.await {
                                tracing::error!(%error, "fail to send rejection error");
                            }
                        }.instrument(current_span));
                        continue;
                    }
                    {
                        let service = shared_service.clone();
                        let sink = sink_proxy_tx.clone();
                        let request_ct = serve_loop_ct.child_token();
                        let context_ct = request_ct.child_token();
                        local_ct_pool.insert(id.clone(), request_ct);
                        let mut extensions = Extensions::new();
                        let mut meta = RequestMetaObject::new();
                        // avoid clone
                        // swap meta firstly, otherwise progress token will be lost
                        std::mem::swap(&mut meta, request.get_meta_mut());
                        std::mem::swap(&mut extensions, request.extensions_mut());
                        let context = RequestContext {
                            ct: context_ct,
                            id: id.clone(),
                            peer: peer.clone(),
                            meta,
                            extensions,
                        };
                        let current_span = tracing::Span::current();
                        let handler_id = id.clone();
                        spawn_service_task(async move {
                            let result = ORIGINATING_REQUEST
                                .scope(handler_id, service.handle_request(request, context))
                                .await;
                            let response = match result {
                                Ok(result) => {
                                    tracing::debug!(%id, ?result, "response message");
                                    JsonRpcMessage::response(result, id)
                                }
                                Err(error) => {
                                    tracing::warn!(%id, ?error, "response error");
                                    JsonRpcMessage::error(error, Some(id))
                                }
                            };
                            let _send_result = sink.send(response).await;
                        }.instrument(current_span));
                    }
                }
                Event::PeerMessage(JsonRpcMessage::Notification(JsonRpcNotification {
                    notification,
                    ..
                })) => {
                    tracing::info!(?notification, "received notification");
                    R::invalidate_response_cache(&peer, &notification).await;
                    let subscription_id =
                        if let Some(cancelled) = R::peer_cancelled_params(&notification) {
                            let request_id = cancelled.request_id.clone();
                            // Modern servers cancel listen requests; legacy peers cancel
                            // requests they originated, even when both directions share an ID.
                            if R::peer_cancels_subscriptions(&peer) {
                                if let Some(request_id) = request_id.as_ref() {
                                    let pending = {
                                        let mut responses = peer.pending_responses_lock();
                                        if responses.get(request_id).is_some_and(|p| p.is_subscription) {
                                            responses.remove(request_id)
                                        } else {
                                            None
                                        }
                                    };
                                    if let Some(pending) = pending {
                                        let _ = pending.responder.send(Err(ServiceError::Cancelled {
                                            reason: cancelled.reason.clone(),
                                        }));
                                    } else {
                                        tracing::debug!(%request_id, "ignoring cancellation of unknown subscription");
                                        continue;
                                    }
                                }
                                request_id
                            } else {
                                if let Some(request_id) = request_id.as_ref()
                                    && let Some(ct) = local_ct_pool.remove(request_id)
                                {
                                    tracing::info!(id = %request_id, reason = cancelled.reason, "cancelled");
                                    ct.cancel();
                                }
                                None
                            }
                        } else {
                            notification.get_meta().subscription_id()
                        };
                    if let Some(subscription_id) = subscription_id
                        && let Some((sender, capacity)) =
                            peer.subscription_sender(&subscription_id)
                    {
                        match sender.try_send(notification) {
                            Ok(()) => {}
                            Err(mpsc::error::TrySendError::Full(_)) => {
                                tracing::warn!(
                                    id = %subscription_id,
                                    capacity,
                                    "subscription notification buffer full"
                                );
                                if R::IS_CLIENT
                                    && let Some(responder) =
                                        peer.take_pending_response(&subscription_id)
                                {
                                    let _ = responder.responder
                                        .send(Err(ServiceError::SubscriptionLagged { capacity }));
                                }
                                peer.unregister_subscription(&subscription_id);
                                peer.try_cancel_request(
                                    subscription_id,
                                    Some("subscription notification buffer full".to_owned()),
                                );
                            }
                            Err(mpsc::error::TrySendError::Closed(_)) => {
                                peer.unregister_subscription(&subscription_id);
                                peer.try_cancel_request(
                                    subscription_id,
                                    Some("subscription notification receiver closed".to_owned()),
                                );
                            }
                        }
                        continue;
                    }
                    let mut notification = notification;
                    if let Some(progress_token) = notification.progress_token() {
                        peer.notify_progress_timeout_watcher(progress_token);
                    }
                    {
                        let service = shared_service.clone();
                        let mut extensions = Extensions::new();
                        let mut meta = NotificationMetaObject::new();
                        // avoid clone
                        std::mem::swap(&mut extensions, notification.extensions_mut());
                        std::mem::swap(&mut meta, notification.get_meta_mut());
                        let context = NotificationContext {
                            peer: peer.clone(),
                            meta,
                            extensions,
                        };
                        let current_span = tracing::Span::current();
                        spawn_service_task(async move {
                            let result = service.handle_notification(notification, context).await;
                            if let Err(error) = result {
                                tracing::warn!(%error, "Error sending notification");
                            }
                        }.instrument(current_span));
                    }
                }
                Event::PeerMessage(JsonRpcMessage::Response(JsonRpcResponse {
                    result,
                    id,
                    ..
                })) => {
                    if let Some(responder) =
                        peer.take_wire_response(&id)
                    {
                        let response_result = responder.responder.send(Ok(result));
                        if let Err(_error) = response_result {
                            tracing::warn!(%id, "Error sending response");
                        }
                    }
                }
                Event::PeerMessage(JsonRpcMessage::Error(JsonRpcError { error, id, .. })) => {
                    let Some(id) = id else {
                        // MCP error responses without an id (e.g. Parse error / Invalid Request)
                        // can't be routed back to a pending request — log and drop.
                        tracing::debug!(?error, "received id-less peer error");
                        continue;
                    };
                    if let Some(responder) =
                        peer.take_wire_response(&id)
                    {
                        let service_error = if error.is_transport_closed() {
                            ServiceError::TransportClosed
                        } else {
                            ServiceError::McpError(error)
                        };
                        let _response_result = responder.responder.send(Err(service_error));
                        if let Err(_error) = _response_result {
                            tracing::warn!(%id, "Error sending response");
                        }
                    }
                }
            }
        };

        // Peers can outlive this service; release response senders on every exit.
        peer.clear_pending_responses();

        // Drain in-flight handler responses before closing the transport.
        // When stdin EOF or cancellation arrives, spawned handler tasks may still
        // be finishing. We need to:
        // 1. Wait for response sends that were already spawned in the main loop
        // 2. Drain any remaining handler responses from the channel
        let drain_timeout = match &quit_reason {
            QuitReason::Closed => Some(Duration::from_secs(5)),
            QuitReason::Cancelled => Some(Duration::from_secs(2)),
            _ => None,
        };
        if let Some(timeout_duration) = drain_timeout {
            // Drop our sender so the channel closes once all handler task
            // clones finish sending their responses (or are dropped).
            drop(sink_proxy_tx);
            let drain_result = tokio::time::timeout(timeout_duration, async {
                // First, wait for any response sends already dispatched by the
                // main loop (these hold transport write futures).
                while let Some(result) = response_send_tasks.join_next().await {
                    if let Err(error) = result {
                        tracing::error!(%error, "response send task failed during drain");
                    }
                }
                // Then drain any handler responses still in the channel
                // (handlers that finished after the loop broke).
                while let Some(m) = sink_proxy_rx.recv().await {
                    if let Some(id) = match &m {
                        JsonRpcMessage::Response(response) => Some(&response.id),
                        JsonRpcMessage::Error(error) => error.id.as_ref(),
                        _ => None,
                    } {
                        let Some(ct) = local_ct_pool.remove(id) else {
                            tracing::debug!(%id, "dropping response for cancelled request");
                            continue;
                        };
                        ct.cancel();
                    }
                    if let Err(error) = transport.send(m).await {
                        tracing::error!(%error, "failed to send pending response during drain");
                        break;
                    }
                }
            })
            .await;
            if drain_result.is_err() {
                tracing::warn!("timed out draining in-flight responses");
            }
        }

        let sink_close_result = transport.close().await;
        if let Err(e) = sink_close_result {
            tracing::error!(%e, "fail to close sink");
        }
        tracing::info!(?quit_reason, "serve finished");
        quit_reason
    }.instrument(current_span));
    RunningService {
        service,
        peer: peer_return,
        handle: Some(handle),
        cancellation_token: ct.clone(),
        dg: ct.drop_guard(),
    }
}

#[cfg(all(test, feature = "server"))]
mod sep2260_marker_tests {
    use std::sync::Arc;

    use super::*;
    use crate::model::{PingRequest, RequestId, ServerRequest};

    fn ping() -> ServerRequest {
        ServerRequest::PingRequest(PingRequest {
            method: Default::default(),
            extensions: Default::default(),
        })
    }

    async fn send_and_capture(scope: Option<RequestId>) -> <RoleServer as ServiceRole>::Req {
        // peer_info None keeps enforcement non-strict; only the sink message matters.
        let (peer, mut rx) =
            Peer::<RoleServer>::new(Arc::new(AtomicU32RequestIdProvider::default()), None);
        let send = peer.send_request_with_option(ping(), PeerRequestOptions::no_options());
        let _handle = match scope {
            Some(id) => ORIGINATING_REQUEST.scope(id, send).await.unwrap(),
            None => send.await.unwrap(),
        };
        let PeerSinkMessage::Request { request, .. } = rx.recv().await.expect("sink message")
        else {
            panic!("expected a request sink message");
        };
        request
    }

    #[tokio::test]
    async fn outbound_request_carries_originating_id_when_in_scope() {
        let request = send_and_capture(Some(RequestId::Number(7))).await;
        let marker = request
            .extensions()
            .get::<OriginatingRequestId>()
            .expect("marker attached");
        assert_eq!(marker.0, RequestId::Number(7));
    }

    #[tokio::test]
    async fn outbound_request_has_no_marker_outside_scope() {
        let request = send_and_capture(None).await;
        assert!(request.extensions().get::<OriginatingRequestId>().is_none());
    }

    #[test]
    #[expect(
        deprecated,
        reason = "Sampling is deprecated by SEP-2577 but remains the canonical restricted request"
    )]
    fn peer_request_association_maps_stream_origin() {
        use std::collections::HashMap;

        use crate::model::{
            CreateMessageRequest, CreateMessageRequestParams, SamplingMessage, ServerRequest,
        };

        fn sampling(origin: Option<InboundStreamOrigin>) -> ServerRequest {
            let mut request = CreateMessageRequest::new(CreateMessageRequestParams::new(
                vec![SamplingMessage::user_text("hi")],
                16,
            ));
            if let Some(origin) = origin {
                request.extensions.insert(origin);
            }
            ServerRequest::CreateMessageRequest(request)
        }

        let empty: HashMap<RequestId, ()> = HashMap::new();
        let in_flight: HashMap<RequestId, ()> = HashMap::from([(RequestId::Number(7), ())]);

        // No marker (stdio): coarse signal.
        assert_eq!(
            peer_request_association(&sampling(None), &in_flight),
            PeerRequestAssociation::Unknown {
                has_pending_outbound_request: true
            }
        );
        assert_eq!(
            peer_request_association(&sampling(None), &empty),
            PeerRequestAssociation::Unknown {
                has_pending_outbound_request: false
            }
        );
        // Standalone GET stream: unassociated even with requests in flight.
        assert_eq!(
            peer_request_association(
                &sampling(Some(InboundStreamOrigin::Unassociated)),
                &in_flight
            ),
            PeerRequestAssociation::Unassociated
        );
        // Originating POST stream of an in-flight request: associated.
        assert_eq!(
            peer_request_association(
                &sampling(Some(InboundStreamOrigin::OutboundRequest(
                    RequestId::Number(7)
                ))),
                &in_flight
            ),
            PeerRequestAssociation::Associated
        );
        // Stream of a request that is no longer in flight: unassociated.
        assert_eq!(
            peer_request_association(
                &sampling(Some(InboundStreamOrigin::OutboundRequest(
                    RequestId::Number(8)
                ))),
                &in_flight
            ),
            PeerRequestAssociation::Unassociated
        );
    }
}

#[cfg(all(test, feature = "client"))]
mod request_timeout_tests {
    use super::*;
    use crate::model::{ClientNotification, ClientRequest, ListToolsRequest, ServerResult};

    const DEADLINE: Duration = Duration::from_millis(100);

    fn policies() -> [PeerRequestOptions; 3] {
        [
            PeerRequestOptions::with_timeout(DEADLINE),
            PeerRequestOptions::with_timeout(DEADLINE).reset_timeout_on_progress(),
            PeerRequestOptions::default().with_max_total_timeout(DEADLINE),
        ]
    }

    async fn pending_request(
        options: PeerRequestOptions,
    ) -> (
        Peer<RoleClient>,
        ProxyOutbound<RoleClient>,
        RequestHandle<RoleClient>,
        Responder<Result<ServerResult, ServiceError>>,
    ) {
        let (peer, mut outgoing) =
            Peer::<RoleClient>::new(Arc::new(AtomicU32RequestIdProvider::default()), None);
        let handle = peer
            .send_request_with_option(
                ClientRequest::ListToolsRequest(ListToolsRequest {
                    method: Default::default(),
                    params: None,
                    extensions: Default::default(),
                }),
                options,
            )
            .await
            .unwrap();
        let PeerSinkMessage::Request { responder, .. } = outgoing.recv().await.unwrap() else {
            panic!("expected request");
        };
        (peer, outgoing, handle, responder)
    }

    async fn expires(handle: RequestHandle<RoleClient>, expected: Duration) {
        let started = tokio::time::Instant::now();
        let result = tokio::time::timeout(Duration::from_secs(1), handle.await_response())
            .await
            .expect("deadline must not wait for cancellation delivery");
        assert!(matches!(result, Err(ServiceError::Timeout { timeout }) if timeout == expected));
        assert_eq!(started.elapsed(), expected);
    }

    fn cancellation(outgoing: &mut ProxyOutbound<RoleClient>, id: RequestId, reason: &str) {
        // Keep its responder unanswered: timeout completion must not depend on it.
        let PeerSinkMessage::Notification {
            notification: ClientNotification::CancelledNotification(cancel),
            responder: _unacknowledged,
        } = outgoing.try_recv().unwrap()
        else {
            panic!("expected cancellation");
        };
        assert_eq!(cancel.params.request_id, Some(id));
        assert_eq!(cancel.params.reason.as_deref(), Some(reason));
        assert!(outgoing.try_recv().is_err());
    }

    #[tokio::test(start_paused = true)]
    async fn saturated_queue_does_not_delay_any_response_timeout_policy() {
        for options in policies() {
            let (peer, mut outgoing, handle, _responder) = pending_request(options).await;
            while let Ok(permit) = peer.tx.try_reserve() {
                let (responder, _) = tokio::sync::oneshot::channel();
                permit.send(PeerSinkMessage::Notification {
                    notification: ClientNotification::CancelledNotification(
                        CancelledNotification::new(CancelledNotificationParam {
                            request_id: None,
                            reason: None,
                            meta: None,
                        }),
                    ),
                    responder,
                });
            }
            expires(handle, DEADLINE).await;
            assert!(peer.progress_timeout_watchers.is_empty());
            let mut count = 0;
            while outgoing.try_recv().is_ok() {
                count += 1;
            }
            assert_eq!(count, Peer::<RoleClient>::CLIENT_CHANNEL_BUFFER_SIZE);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_is_correlated_but_never_awaits_transport_acknowledgment() {
        for (index, options) in policies().into_iter().enumerate() {
            let (peer, mut outgoing, handle, _responder) = pending_request(options).await;
            let id = handle.id.clone();
            expires(handle, DEADLINE).await;
            assert!(peer.progress_timeout_watchers.is_empty());
            let reason = if index == 2 {
                RequestHandle::<RoleClient>::REQUEST_MAX_TOTAL_TIMEOUT_REASON
            } else {
                RequestHandle::<RoleClient>::REQUEST_TIMEOUT_REASON
            };
            cancellation(&mut outgoing, id, reason);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn closed_queue_preserves_timeout_and_removes_progress_watchers() {
        for options in policies() {
            let (peer, mut outgoing, handle, _responder) = pending_request(options).await;
            outgoing.close();
            expires(handle, DEADLINE).await;
            assert!(peer.progress_timeout_watchers.is_empty());
            assert!(outgoing.try_recv().is_err());
        }
    }

    #[tokio::test(start_paused = true)]
    async fn progress_resets_idle_deadline_but_total_deadline_still_finishes_without_ack() {
        let total = Duration::from_millis(250);
        let options = PeerRequestOptions::with_timeout(DEADLINE)
            .reset_timeout_on_progress()
            .with_max_total_timeout(total);
        let (peer, mut outgoing, handle, _responder) = pending_request(options).await;
        let id = handle.id.clone();
        let token = handle.progress_token.clone();
        let sender = peer.clone();
        let progress = tokio::spawn(async move {
            for _ in 0..3 {
                tokio::time::sleep(Duration::from_millis(80)).await;
                sender.notify_progress_timeout_watcher(&token);
            }
        });
        expires(handle, total).await;
        progress.await.unwrap();
        assert!(peer.progress_timeout_watchers.is_empty());
        cancellation(
            &mut outgoing,
            id,
            RequestHandle::<RoleClient>::REQUEST_MAX_TOTAL_TIMEOUT_REASON,
        );
    }
}

#[cfg(all(test, feature = "client"))]
mod request_routing_cleanup_tests {
    use super::*;
    use crate::model::{ClientRequest, ListToolsRequest, ServerResult};

    fn options() -> PeerRequestOptions {
        PeerRequestOptions::with_timeout(Duration::from_secs(10)).reset_timeout_on_progress()
    }

    async fn tracked(
        peer: &Peer<RoleClient>,
        outgoing: &mut ProxyOutbound<RoleClient>,
        options: PeerRequestOptions,
    ) -> (
        RequestHandle<RoleClient>,
        Responder<Result<ServerResult, ServiceError>>,
        mpsc::Receiver<crate::model::ServerNotification>,
    ) {
        let (handle, notifications) = peer
            .send_subscription_request(
                ClientRequest::ListToolsRequest(ListToolsRequest {
                    method: Default::default(),
                    params: None,
                    extensions: Default::default(),
                }),
                options,
                1,
            )
            .await
            .unwrap();
        let PeerSinkMessage::Request { responder, .. } = outgoing.recv().await.unwrap() else {
            panic!("expected request");
        };
        assert!(peer.subscription_sender(&handle.id).is_some());
        (handle, responder, notifications)
    }

    fn peer() -> (Peer<RoleClient>, ProxyOutbound<RoleClient>) {
        Peer::new(Arc::new(AtomicU32RequestIdProvider::default()), None)
    }

    fn assert_clean(peer: &Peer<RoleClient>, id: &RequestId) {
        assert!(peer.subscription_sender(id).is_none());
        assert!(peer.progress_timeout_watchers.is_empty());
    }

    #[test]
    fn unpolled_handle_and_response_future_drop_clean_up_without_a_runtime() {
        for response_future in [false, true] {
            let (peer, mut outgoing) = peer();
            let runtime = tokio::runtime::Builder::new_current_thread()
                .enable_time()
                .build()
                .unwrap();
            let (handle, _responder, mut notifications) =
                runtime.block_on(tracked(&peer, &mut outgoing, options()));
            let id = handle.id.clone();
            drop(runtime);
            if response_future {
                drop(handle.await_response());
            } else {
                drop(handle);
            }
            assert_clean(&peer, &id);
            assert!(matches!(
                notifications.try_recv(),
                Err(mpsc::error::TryRecvError::Disconnected)
            ));
            assert!(
                outgoing.try_recv().is_err(),
                "local cleanup must not send cancellation"
            );
        }
    }

    #[tokio::test]
    async fn dropping_a_polled_response_future_cleans_original_keys_and_preserves_other_requests() {
        let (peer, mut outgoing) = peer();
        let (mut first, _first_response, _first_notifications) =
            tracked(&peer, &mut outgoing, options()).await;
        let (second, _second_response, _second_notifications) =
            tracked(&peer, &mut outgoing, options()).await;
        let first_id = first.id.clone();
        let first_token = first.progress_token.clone();
        let second_id = second.id.clone();
        let second_token = second.progress_token.clone();
        // Public fields must not redirect cleanup to another live registration.
        first.id = second_id.clone();
        first.progress_token = second_token.clone();
        let mut response = Box::pin(first.await_response());
        assert!(futures::poll!(&mut response).is_pending());
        drop(response);
        assert!(peer.subscription_sender(&first_id).is_none());
        assert!(
            peer.progress_timeout_watchers
                .sender(&first_token)
                .is_none()
        );
        assert!(peer.subscription_sender(&second_id).is_some());
        assert!(
            peer.progress_timeout_watchers
                .sender(&second_token)
                .is_some()
        );
        drop(second);
        assert_clean(&peer, &second_id);
        assert!(outgoing.try_recv().is_err());
    }

    #[tokio::test]
    async fn transport_closed_cleans_each_response_wait_branch() {
        for options in [
            PeerRequestOptions::default(),
            PeerRequestOptions::with_timeout(Duration::from_secs(10)),
            options(),
            PeerRequestOptions::default().with_max_total_timeout(Duration::from_secs(10)),
        ] {
            let (peer, mut outgoing) = peer();
            let (handle, responder, _notifications) = tracked(&peer, &mut outgoing, options).await;
            let id = handle.id.clone();
            drop(responder);
            assert!(matches!(
                handle.await_response().await,
                Err(ServiceError::TransportClosed)
            ));
            assert_clean(&peer, &id);
            assert!(outgoing.try_recv().is_err());
        }
    }

    #[tokio::test]
    async fn response_success_and_remote_error_both_clean_up() {
        for success in [true, false] {
            let (peer, mut outgoing) = peer();
            let (handle, responder, _notifications) =
                tracked(&peer, &mut outgoing, options()).await;
            let id = handle.id.clone();
            let response = if success {
                Ok(ServerResult::ListToolsResult(Default::default()))
            } else {
                Err(ServiceError::Cancelled {
                    reason: Some("remote cancellation".to_owned()),
                })
            };
            responder.send(response).unwrap();
            let actual = handle.await_response().await;
            if success {
                assert!(matches!(actual, Ok(ServerResult::ListToolsResult(_))));
            } else {
                assert!(matches!(actual, Err(ServiceError::Cancelled { reason })
                    if reason.as_deref() == Some("remote cancellation")));
            }
            assert_clean(&peer, &id);
            assert!(outgoing.try_recv().is_err());
        }
    }

    #[tokio::test]
    async fn explicit_cancel_cleans_up_before_transport_acknowledgment() {
        let (peer, mut outgoing) = peer();
        let (handle, _responder, _notifications) = tracked(&peer, &mut outgoing, options()).await;
        let id = handle.id.clone();
        let mut cancel = Box::pin(handle.cancel(Some("caller cancelled".to_owned())));
        assert!(futures::poll!(&mut cancel).is_pending());
        assert_clean(&peer, &id);
        let PeerSinkMessage::Notification {
            notification,
            responder,
        } = outgoing.try_recv().unwrap()
        else {
            panic!("expected cancellation");
        };
        let crate::model::ClientNotification::CancelledNotification(notification) = notification
        else {
            panic!("expected cancellation");
        };
        assert_eq!(notification.params.request_id, Some(id));
        assert_eq!(
            notification.params.reason.as_deref(),
            Some("caller cancelled")
        );
        responder.send(Ok(())).unwrap();
        cancel.await.unwrap();
        assert!(outgoing.try_recv().is_err());
    }
}

#[cfg(all(test, feature = "client"))]
mod pending_response_lifetime_tests {
    use super::*;
    use crate::model::{ClientRequest, ListToolsRequest, ServerResult};
    use std::sync::atomic::{AtomicBool, Ordering};

    type Sent = (TxJsonRpcMessage<RoleClient>, CancellationToken);

    struct ProbeTransport {
        incoming: mpsc::UnboundedReceiver<RxJsonRpcMessage<RoleClient>>,
        sent: mpsc::UnboundedSender<Sent>,
        stall: Arc<AtomicBool>,
    }

    impl Transport<RoleClient> for ProbeTransport {
        type Error = std::io::Error;

        fn supports_request_send_cancellation() -> bool {
            true
        }

        fn send(
            &mut self,
            message: TxJsonRpcMessage<RoleClient>,
        ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'static {
            let finished = CancellationToken::new();
            let guard = finished.clone().drop_guard();
            self.sent.send((message, finished)).unwrap();
            let stall = self.stall.load(Ordering::Relaxed);
            async move {
                let _guard = guard;
                if stall {
                    std::future::pending::<()>().await;
                }
                Ok(())
            }
        }

        async fn receive(&mut self) -> Option<RxJsonRpcMessage<RoleClient>> {
            self.incoming.recv().await
        }

        async fn close(&mut self) -> Result<(), Self::Error> {
            Ok(())
        }
    }

    // Deliberately uses the trait default: interruption safety is not promised.
    struct NonCancellableTransport(ProbeTransport);

    impl Transport<RoleClient> for NonCancellableTransport {
        type Error = std::io::Error;

        fn send(
            &mut self,
            message: TxJsonRpcMessage<RoleClient>,
        ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'static {
            self.0.send(message)
        }

        async fn receive(&mut self) -> Option<RxJsonRpcMessage<RoleClient>> {
            self.0.receive().await
        }

        async fn close(&mut self) -> Result<(), Self::Error> {
            self.0.close().await
        }
    }

    struct Harness {
        transport: ProbeTransport,
        incoming: mpsc::UnboundedSender<RxJsonRpcMessage<RoleClient>>,
        sent: mpsc::UnboundedReceiver<Sent>,
        stall: Arc<AtomicBool>,
    }

    fn harness(stall: bool) -> Harness {
        let (incoming, rx) = mpsc::unbounded_channel();
        let (tx, sent) = mpsc::unbounded_channel();
        let stall = Arc::new(AtomicBool::new(stall));
        Harness {
            transport: ProbeTransport {
                incoming: rx,
                sent: tx,
                stall: stall.clone(),
            },
            incoming,
            sent,
            stall,
        }
    }

    fn request() -> ClientRequest {
        ClientRequest::ListToolsRequest(ListToolsRequest {
            method: Default::default(),
            params: None,
            extensions: Default::default(),
        })
    }

    fn start(transport: impl Transport<RoleClient> + 'static) -> RunningService<RoleClient, ()> {
        let (peer, rx) = Peer::new(Arc::new(AtomicU32RequestIdProvider::default()), None);
        serve_inner(
            (),
            transport,
            peer,
            rx,
            VecDeque::new(),
            CancellationToken::new(),
        )
    }

    fn result(id: RequestId) -> RxJsonRpcMessage<RoleClient> {
        JsonRpcMessage::response(ServerResult::ListToolsResult(Default::default()), id)
    }

    async fn next_request(
        sent: &mut mpsc::UnboundedReceiver<Sent>,
        expected: &RequestId,
    ) -> CancellationToken {
        let (JsonRpcMessage::Request(request), finished) = sent.recv().await.unwrap() else {
            panic!("expected request");
        };
        assert_eq!(&request.id, expected);
        finished
    }

    async fn run(test: impl Future<Output = ()>) {
        tokio::time::timeout(
            Duration::from_secs(2),
            tokio::task::LocalSet::new().run_until(test),
        )
        .await
        .expect("service lifecycle must make progress");
    }

    #[cfg(feature = "transport-streamable-http-client")]
    #[tokio::test(start_paused = true)]
    async fn completed_sends_retain_isolated_local_caller_lifetimes() {
        run(async {
            let mut h = harness(false);
            let mut service = start(h.transport);
            let peer = service.peer().clone();
            let first = peer
                .send_request_with_option(request(), PeerRequestOptions::default())
                .await
                .unwrap();
            let second = peer
                .send_request_with_option(request(), PeerRequestOptions::default())
                .await
                .unwrap();
            let mut tokens = Vec::new();
            for expected in [&first.id, &second.id] {
                let (JsonRpcMessage::Request(request), finished) = h.sent.recv().await.unwrap()
                else {
                    panic!("expected request");
                };
                assert_eq!(&request.id, expected);
                let token = request
                    .request
                    .extensions()
                    .get::<OutboundRequestLifetime>()
                    .unwrap()
                    .0
                    .clone();
                // The lifetime is local metadata, not a wire parameter.
                let wire = serde_json::to_value(&request).unwrap();
                assert!(wire.get("extensions").is_none());
                finished.cancelled().await;
                assert!(!token.is_cancelled());
                tokens.push(token);
            }
            drop(first);
            assert!(tokens[0].is_cancelled());
            assert!(!tokens[1].is_cancelled());
            h.incoming.send(result(second.id.clone())).unwrap();
            second.await_response().await.unwrap();
            assert!(tokens[1].is_cancelled());
            service.close().await.unwrap();
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn dropped_queued_request_is_never_dispatched() {
        run(async {
            let mut h = harness(false);
            let (peer, rx) =
                Peer::<RoleClient>::new(Arc::new(AtomicU32RequestIdProvider::default()), None);
            let abandoned = peer
                .send_request_with_option(request(), PeerRequestOptions::default())
                .await
                .unwrap();
            drop(abandoned);
            let mut service = serve_inner(
                (),
                h.transport,
                peer.clone(),
                rx,
                VecDeque::new(),
                CancellationToken::new(),
            );
            let live = peer
                .send_request_with_option(request(), PeerRequestOptions::default())
                .await
                .unwrap();
            next_request(&mut h.sent, &live.id).await;
            h.incoming.send(result(live.id.clone())).unwrap();
            assert!(matches!(
                live.await_response().await,
                Ok(ServerResult::ListToolsResult(_))
            ));
            assert!(peer.pending_responses_lock().is_empty());
            service.close().await.unwrap();
            assert!(h.sent.try_recv().is_err());
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn dropping_pending_response_retires_association_and_stalled_send_with_sibling_isolation()
    {
        run(async {
            let mut h = harness(true);
            let mut service = start(h.transport);
            let peer = service.peer().clone();
            let first = peer
                .send_request_with_option(request(), PeerRequestOptions::default())
                .await
                .unwrap();
            let first_id = first.id.clone();
            let first_finished = next_request(&mut h.sent, &first_id).await;
            let second = peer
                .send_request_with_option(request(), PeerRequestOptions::default())
                .await
                .unwrap();
            let second_id = second.id.clone();
            let second_finished = next_request(&mut h.sent, &second_id).await;
            let mut first_response = Box::pin(first.await_response());
            assert!(futures::poll!(&mut first_response).is_pending());
            drop(first_response);
            assert_eq!(peer.pending_responses_lock().len(), 1);
            first_finished.cancelled().await;
            assert!(!second_finished.is_cancelled());
            let mut associated = request();
            associated
                .extensions_mut()
                .insert(InboundStreamOrigin::OutboundRequest(first_id.clone()));
            assert_eq!(
                peer_request_association(&associated, &peer.pending_responses_lock()),
                PeerRequestAssociation::Unassociated
            );
            associated
                .extensions_mut()
                .insert(InboundStreamOrigin::OutboundRequest(second_id.clone()));
            assert_eq!(
                peer_request_association(&associated, &peer.pending_responses_lock()),
                PeerRequestAssociation::Associated
            );
            // A late response to the abandoned request cannot consume the live one.
            h.incoming.send(result(first_id)).unwrap();
            h.incoming.send(result(second_id)).unwrap();
            second.await_response().await.unwrap();
            second_finished.cancelled().await;
            assert!(peer.pending_responses_lock().is_empty());
            assert_eq!(
                peer_request_association(&request(), &peer.pending_responses_lock()),
                PeerRequestAssociation::Unknown {
                    has_pending_outbound_request: false
                }
            );
            h.stall.store(false, Ordering::Relaxed);
            let live = peer
                .send_request_with_option(request(), PeerRequestOptions::default())
                .await
                .unwrap();
            next_request(&mut h.sent, &live.id).await;
            h.incoming.send(result(live.id.clone())).unwrap();
            live.await_response().await.unwrap();
            service.close().await.unwrap();
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn response_deadlines_remove_pending_entries_and_stop_stalled_request_sends() {
        run(async {
            let deadline = Duration::from_millis(100);
            for options in [PeerRequestOptions::with_timeout(deadline), PeerRequestOptions::with_timeout(deadline).reset_timeout_on_progress(), PeerRequestOptions::default().with_max_total_timeout(deadline)] {
                let mut h = harness(true);
                let mut service = start(h.transport);
                let peer = service.peer().clone();
                let handle = peer.send_request_with_option(request(), options).await.unwrap();
                let id = handle.id.clone();
                let sent = next_request(&mut h.sent, &id).await;
                assert!(matches!(handle.await_response().await, Err(ServiceError::Timeout { timeout }) if timeout == deadline));
                assert!(peer.pending_responses_lock().is_empty());
                sent.cancelled().await;
                let (JsonRpcMessage::Notification(notification), _) = h.sent.recv().await.unwrap() else { panic!("expected cancellation"); };
                let crate::model::ClientNotification::CancelledNotification(cancel) = notification.notification else { panic!("expected cancellation"); };
                assert_eq!(cancel.params.request_id, Some(id));
                service.close().await.unwrap();
            }
        }).await;
    }

    #[tokio::test(start_paused = true)]
    async fn wire_success_error_and_numeric_string_ids_remove_the_original_pending_response() {
        run(async {
            let mut h = harness(false);
            let mut service = start(h.transport);
            let peer = service.peer().clone();
            for success in [true, false] {
                let handle = peer.send_request_with_option(request(), PeerRequestOptions::default()).await.unwrap();
                next_request(&mut h.sent, &handle.id).await;
                let id = match handle.id.clone() { RequestId::Number(number) => RequestId::String(Arc::from(number.to_string())), id => id };
                h.incoming.send(if success { result(id) } else { JsonRpcMessage::error(McpError::invalid_request("peer rejected request", None), Some(id)) }).unwrap();
                let response = handle.await_response().await;
                if success { assert!(matches!(response, Ok(ServerResult::ListToolsResult(_)))); }
                else { assert!(matches!(response, Err(ServiceError::McpError(error)) if error.message == "peer rejected request")); }
                assert!(peer.pending_responses_lock().is_empty());
            }
            service.close().await.unwrap();
        }).await;
    }

    #[tokio::test(start_paused = true)]
    async fn service_shutdown_releases_responses_even_when_peer_clones_survive() {
        run(async {
            for close_input in [true, false] {
                let mut h = harness(false);
                let service = start(h.transport);
                let peer = service.peer().clone();
                let handle = peer
                    .send_request_with_option(request(), PeerRequestOptions::default())
                    .await
                    .unwrap();
                next_request(&mut h.sent, &handle.id).await;
                if close_input {
                    drop(h.incoming);
                } else {
                    service.cancellation_token().cancel();
                }
                assert!(matches!(
                    handle.await_response().await,
                    Err(ServiceError::TransportClosed)
                ));
                service.waiting().await.unwrap();
                assert!(peer.pending_responses_lock().is_empty());
            }
        })
        .await;
    }

    #[tokio::test(start_paused = true)]
    async fn aborted_service_task_releases_shared_response_table_and_stalled_sends() {
        run(async {
            let mut h = harness(true);
            let mut service = start(h.transport);
            let peer = service.peer().clone();
            let handle = peer
                .send_request_with_option(request(), PeerRequestOptions::default())
                .await
                .unwrap();
            let finished = next_request(&mut h.sent, &handle.id).await;
            let task = service.handle.take().unwrap();
            task.abort();
            assert!(task.await.unwrap_err().is_cancelled());
            assert!(peer.pending_responses_lock().is_empty());
            assert!(matches!(
                handle.await_response().await,
                Err(ServiceError::TransportClosed)
            ));
            finished.cancelled().await;
        })
        .await;
    }
    #[tokio::test(start_paused = true)]
    async fn transport_default_preserves_in_flight_send_but_releases_pending_response() {
        run(async {
            let mut h = harness(true);
            let mut service = start(NonCancellableTransport(h.transport));
            let peer = service.peer().clone();
            let handle = peer
                .send_request_with_option(request(), PeerRequestOptions::default())
                .await
                .unwrap();
            let finished = next_request(&mut h.sent, &handle.id).await;
            drop(handle);
            assert!(peer.pending_responses_lock().is_empty());
            assert!(
                tokio::time::timeout(Duration::from_millis(10), finished.cancelled())
                    .await
                    .is_err()
            );
            service.close().await.unwrap();
            finished.cancelled().await;
        })
        .await;
    }
}
