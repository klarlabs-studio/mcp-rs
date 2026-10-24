//! Opt-in out-of-band notification delivery to host-selected HTTP(S) endpoints.
//!
//! This mirrors the pinned Go delivery helper, not a standardized webhook protocol.
//! It does not install routes, subscribe to events, or retry failed POSTs. Hosts own
//! endpoint admission, authentication, signing, and any delivery persistence.

use futures_util::future::BoxFuture;
use reqwest::{
    Client, Url,
    header::{CONTENT_TYPE, HeaderMap, HeaderValue},
};
use serde::Serialize;
use serde_json::Value;
use std::{io::Write, num::NonZeroUsize, sync::Arc, time::Duration};

/// HTTP endpoint and delivery limits. Debug output omits the URL and headers.
#[derive(Clone)]
pub struct WebhookConfig {
    /// Host-selected HTTP(S) destination without URL credentials or a fragment.
    pub endpoint: Url,
    /// Caller-owned authentication or other headers; framing headers are reserved.
    pub headers: HeaderMap,
    /// Nonzero deadline for each POST through receipt of response headers.
    pub timeout: Duration,
    /// Maximum serialized JSON-RPC notification size, including the envelope.
    pub max_notification_bytes: NonZeroUsize,
}
impl WebhookConfig {
    /// Default to a ten-second deadline and a 1 MiB notification limit.
    pub fn new(endpoint: Url) -> Self {
        Self {
            endpoint,
            headers: HeaderMap::new(),
            timeout: Duration::from_secs(10),
            max_notification_bytes: NonZeroUsize::new(1 << 20).unwrap_or(NonZeroUsize::MIN),
        }
    }
    fn validate(&self) -> Result<(), WebhookError> {
        if !matches!(self.endpoint.scheme(), "http" | "https")
            || self.endpoint.host_str().is_none()
            || !self.endpoint.username().is_empty()
            || self.endpoint.password().is_some()
            || self.endpoint.fragment().is_some()
        {
            return Err(WebhookError::InvalidConfig(
                "expected an HTTP(S) endpoint without credentials or fragment",
            ));
        }
        if self.timeout.is_zero() {
            return Err(WebhookError::InvalidConfig("timeout must be nonzero"));
        }
        for name in [
            "content-type",
            "mcp-method",
            "content-length",
            "transfer-encoding",
            "host",
        ] {
            if self.headers.contains_key(name) {
                return Err(WebhookError::InvalidConfig(
                    "framing headers cannot be overridden",
                ));
            }
        }
        Ok(())
    }
}
impl std::fmt::Debug for WebhookConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebhookConfig")
            .field("timeout", &self.timeout)
            .field("max_notification_bytes", &self.max_notification_bytes)
            .finish_non_exhaustive()
    }
}

/// Configuration, encoding, delivery, or fan-out failure. HTTP URLs are redacted.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum WebhookError {
    /// Invalid endpoint, deadline, or reserved header configuration.
    #[error("invalid webhook configuration: {0}")]
    InvalidConfig(&'static str),
    /// Method is empty or cannot be carried in the Mcp-Method header.
    #[error("invalid webhook notification method")]
    InvalidMethod,
    /// A caller's parameters could not be encoded as JSON.
    #[error("could not serialize webhook notification: {0}")]
    Serialize(#[source] serde_json::Error),
    /// The complete notification exceeded the configured limit.
    #[error("webhook notification exceeds {limit} bytes")]
    TooLarge {
        /// Maximum encoded notification size.
        limit: usize,
    },
    /// HTTP client construction or delivery failed, without URL details.
    #[error(transparent)]
    Http(Box<reqwest::Error>),
    /// Response headers did not arrive before the configured deadline.
    #[error("webhook delivery timed out")]
    Timeout,
    /// The receiver returned a status outside 200–299. No retry is performed.
    #[error("webhook receiver returned HTTP {0}")]
    Status(u16),
    /// Every target was attempted, with one or more failures retained in order.
    #[error("{count} notification target(s) failed", count = .0.len())]
    Fanout(Vec<NotificationFailure>),
}
fn http_error(error: reqwest::Error) -> WebhookError {
    if error.is_timeout() {
        WebhookError::Timeout
    } else {
        WebhookError::Http(Box::new(error.without_url()))
    }
}

/// Object-safe async notification target for sequential fan-out.
///
/// Dropping the future cancels pending delivery; acceptance by a remote receiver
/// cannot be undone. Implementations should return errors rather than panic.
pub trait NotificationSender: Send + Sync {
    /// Deliver one JSON-RPC notification with already materialized parameters.
    fn send_notification<'a>(
        &'a self,
        method: &'a str,
        params: &'a Value,
    ) -> BoxFuture<'a, Result<(), WebhookError>>;
}

/// Reusable async HTTP notifier. Clones share the client's connection pool.
/// Debug output omits destination URLs and authentication headers.
#[derive(Clone)]
pub struct WebhookNotifier {
    client: Client,
    config: WebhookConfig,
}
impl std::fmt::Debug for WebhookNotifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WebhookNotifier")
            .field("config", &self.config)
            .finish_non_exhaustive()
    }
}
impl WebhookNotifier {
    /// Construct with default limits, platform-verified Rustls TLS, and no redirects.
    ///
    /// # Errors
    /// Returns an invalid-configuration or HTTP client construction error.
    pub fn new(endpoint: Url) -> Result<Self, WebhookError> {
        Self::from_config(WebhookConfig::new(endpoint))
    }
    /// Construct with configured headers/limits and the default redirect-free client.
    ///
    /// # Errors
    /// Returns an invalid-configuration or HTTP client construction error.
    pub fn from_config(config: WebhookConfig) -> Result<Self, WebhookError> {
        config.validate()?;
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .retry(reqwest::retry::never())
            .build()
            .map_err(http_error)?;
        Self::with_client(client, config)
    }
    /// Use a caller-owned client, including its TLS, proxy, redirect, and retry policy.
    /// The configured deadline and request-size limit still apply to every send.
    ///
    /// # Errors
    /// Returns an error for invalid configuration before any network activity.
    pub fn with_client(client: Client, config: WebhookConfig) -> Result<Self, WebhookError> {
        config.validate()?;
        Ok(Self { client, config })
    }
    /// POST a JSON-RPC 2.0 notification with no request ID. Any 2xx status succeeds.
    /// Response bodies are dropped without buffering; the helper does not retry.
    /// A caller-supplied client may have its own retry policy.
    /// Dropping this future cancels its pending request.
    ///
    /// # Errors
    /// Returns encoding/size/method errors before sending, or a delivery failure.
    pub async fn send_notification<P: Serialize + ?Sized>(
        &self,
        method: &str,
        params: &P,
    ) -> Result<(), WebhookError> {
        if method.is_empty() {
            return Err(WebhookError::InvalidMethod);
        }
        let method_header =
            HeaderValue::from_str(method).map_err(|_| WebhookError::InvalidMethod)?;
        #[derive(Serialize)]
        struct Notification<'a, P: ?Sized> {
            jsonrpc: &'static str,
            method: &'a str,
            params: &'a P,
        }
        let mut buffer = LimitedBuffer {
            bytes: Vec::new(),
            limit: self.config.max_notification_bytes.get(),
            exceeded: false,
        };
        if let Err(error) = serde_json::to_writer(
            &mut buffer,
            &Notification {
                jsonrpc: "2.0",
                method,
                params,
            },
        ) {
            return Err(if buffer.exceeded {
                WebhookError::TooLarge {
                    limit: buffer.limit,
                }
            } else {
                WebhookError::Serialize(error)
            });
        }
        let request = self
            .client
            .post(self.config.endpoint.clone())
            .headers(self.config.headers.clone())
            .header(CONTENT_TYPE, "application/json")
            .header("mcp-method", method_header)
            .body(buffer.bytes);
        let response = tokio::time::timeout(self.config.timeout, request.send())
            .await
            .map_err(|_| WebhookError::Timeout)?
            .map_err(http_error)?;
        if !response.status().is_success() {
            return Err(WebhookError::Status(response.status().as_u16()));
        }
        Ok(())
    }
    /// Send Go's `notifications/channel/message` shape (`channel` and `message`).
    ///
    /// # Errors
    /// Returns the same configuration/encoding/delivery errors as notification sends.
    pub async fn send_channel_message<P: Serialize + ?Sized>(
        &self,
        channel: &str,
        content: &P,
    ) -> Result<(), WebhookError> {
        #[derive(Serialize)]
        struct ChannelMessage<'a, P: ?Sized> {
            channel: &'a str,
            message: &'a P,
        }
        self.send_notification(
            "notifications/channel/message",
            &ChannelMessage {
                channel,
                message: content,
            },
        )
        .await
    }
}
impl NotificationSender for WebhookNotifier {
    fn send_notification<'a>(
        &'a self,
        method: &'a str,
        params: &'a Value,
    ) -> BoxFuture<'a, Result<(), WebhookError>> {
        Box::pin(WebhookNotifier::send_notification(self, method, params))
    }
}

struct LimitedBuffer {
    bytes: Vec<u8>,
    limit: usize,
    exceeded: bool,
}
impl Write for LimitedBuffer {
    fn write(&mut self, data: &[u8]) -> std::io::Result<usize> {
        if data.len() > self.limit.saturating_sub(self.bytes.len()) {
            self.exceeded = true;
            return Err(std::io::Error::other("notification size limit"));
        }
        self.bytes.extend_from_slice(data);
        Ok(data.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

/// One failed target in a sequential fan-out delivery.
#[derive(Debug)]
pub struct NotificationFailure {
    /// Zero-based target position in the configured notifier list.
    pub index: usize,
    /// Original typed target failure.
    pub error: WebhookError,
}

/// Sequential delivery to every target, preserving all failures without retries.
/// An empty set succeeds. Unlike Go, target entries cannot be null.
#[derive(Clone, Default)]
pub struct MultiNotifier {
    notifiers: Vec<Arc<dyn NotificationSender>>,
}
impl MultiNotifier {
    /// Collect an ordered set of shared notification targets.
    pub fn new(notifiers: impl IntoIterator<Item = Arc<dyn NotificationSender>>) -> Self {
        Self {
            notifiers: notifiers.into_iter().collect(),
        }
    }
    /// Attempt every target in order; return indexed failures after all attempts.
    /// Dropping this future cancels the active send and skips remaining targets.
    ///
    /// # Errors
    /// Returns [`WebhookError::Fanout`] when any target fails.
    pub async fn send_notification(
        &self,
        method: &str,
        params: &Value,
    ) -> Result<(), WebhookError> {
        let mut failures = Vec::new();
        for (index, notifier) in self.notifiers.iter().enumerate() {
            if let Err(error) = notifier.send_notification(method, params).await {
                failures.push(NotificationFailure { index, error });
            }
        }
        if failures.is_empty() {
            Ok(())
        } else {
            Err(WebhookError::Fanout(failures))
        }
    }
}
impl NotificationSender for MultiNotifier {
    fn send_notification<'a>(
        &'a self,
        method: &'a str,
        params: &'a Value,
    ) -> BoxFuture<'a, Result<(), WebhookError>> {
        Box::pin(MultiNotifier::send_notification(self, method, params))
    }
}
impl std::fmt::Debug for MultiNotifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MultiNotifier")
            .field("targets", &self.notifiers.len())
            .finish()
    }
}
