// Derived from rmcp 3.5.1; maintained and modified by mcp-rs. See ../../../UPSTREAM.md.
use std::{marker::PhantomData, sync::Arc};

use futures::SinkExt;
use serde::{Serialize, de::DeserializeOwned};
use thiserror::Error;
use tokio::{
    io::{AsyncBufReadExt, AsyncRead, AsyncWrite, BufReader},
    sync::Mutex,
};
use tokio_util::{
    bytes::{Buf, BufMut, BytesMut},
    codec::{Decoder, Encoder, FramedWrite},
};

use super::{IntoTransport, Transport};
use crate::{
    model::ErrorData,
    service::{RxJsonRpcMessage, ServiceRole, TxJsonRpcMessage},
};

#[non_exhaustive]
pub enum TransportAdapterAsyncRW {}

impl<Role, R, W> IntoTransport<Role, std::io::Error, TransportAdapterAsyncRW> for (R, W)
where
    Role: ServiceRole,
    R: AsyncRead + Send + 'static + Unpin,
    W: AsyncWrite + Send + 'static + Unpin,
{
    fn into_transport(self) -> impl Transport<Role, Error = std::io::Error> + 'static {
        AsyncRwTransport::new(self.0, self.1)
    }
}

#[non_exhaustive]
pub enum TransportAdapterAsyncCombinedRW {}
impl<Role, S> IntoTransport<Role, std::io::Error, TransportAdapterAsyncCombinedRW> for S
where
    Role: ServiceRole,
    S: AsyncRead + AsyncWrite + Send + 'static,
{
    fn into_transport(self) -> impl Transport<Role, Error = std::io::Error> + 'static {
        IntoTransport::<Role, std::io::Error, TransportAdapterAsyncRW>::into_transport(
            tokio::io::split(self),
        )
    }
}

pub type TransportWriter<Role, W> = FramedWrite<W, JsonRpcMessageCodec<TxJsonRpcMessage<Role>>>;

/// Default maximum incoming line payload (16 MiB), excluding its LF delimiter.
pub const DEFAULT_MAX_MESSAGE_SIZE: usize = 16 * 1024 * 1024;

/// Newline-delimited JSON transport with bounded, cancellation-safe input buffering.
///
/// An oversized input line ends reception without parsing or draining it. Outgoing
/// messages are not capped. The limit counts wire bytes before LF, including CR
/// when CRLF is used; it does not measure parsed JSON allocations or process memory.
pub struct AsyncRwTransport<Role: ServiceRole, R: AsyncRead, W: AsyncWrite> {
    read: BufReader<R>,
    line_buf: Vec<u8>,
    max_message_size: usize,
    read_closed: bool,
    write: Arc<Mutex<Option<TransportWriter<Role, W>>>>,
    _role: PhantomData<fn() -> Role>,
}

impl<Role: ServiceRole, R, W> AsyncRwTransport<Role, R, W>
where
    R: Send + AsyncRead + Unpin,
    W: Send + AsyncWrite + Unpin + 'static,
{
    /// Create a transport with the default 16 MiB incoming-message limit.
    pub fn new(read: R, write: W) -> Self {
        Self::new_with_max_message_size(read, write, DEFAULT_MAX_MESSAGE_SIZE)
    }

    /// Configure the maximum incoming bytes before LF. Zero permits only empty
    /// lines. Oversized lines end reception; subsequent bytes are not accepted.
    pub fn new_with_max_message_size(read: R, write: W, max_message_size: usize) -> Self {
        let read = BufReader::new(read);
        let write = Arc::new(Mutex::new(Some(FramedWrite::new(
            write,
            JsonRpcMessageCodec::<TxJsonRpcMessage<Role>>::default(),
        ))));
        Self {
            read,
            line_buf: Vec::new(),
            max_message_size,
            read_closed: false,
            write,
            _role: PhantomData,
        }
    }

    // Each completed fill_buf/copy/consume step has no cancellation point. A
    // cancelled read leaves already consumed bytes in the transport-owned buffer.
    async fn read_line(&mut self) -> std::io::Result<bool> {
        if self.read_closed {
            return Ok(false);
        }
        loop {
            let available = self.read.fill_buf().await?;
            if available.is_empty() {
                self.read_closed = true;
                // Preserve bounded unterminated EOF messages accepted by this transport.
                return Ok(!self.line_buf.is_empty());
            }
            let newline = available.iter().position(|byte| *byte == b'\n');
            let payload_len = newline.unwrap_or(available.len());
            if payload_len > self.max_message_size - self.line_buf.len() {
                self.read_closed = true;
                self.line_buf.clear();
                return Err(std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!(
                        "incoming JSON-RPC message exceeds the {}-byte limit",
                        self.max_message_size
                    ),
                ));
            }
            self.line_buf.extend_from_slice(&available[..payload_len]);
            let consumed = payload_len + usize::from(newline.is_some());
            self.read.consume(consumed);
            if newline.is_some() {
                return Ok(true);
            }
        }
    }
}

#[cfg(feature = "client")]
impl<R, W> AsyncRwTransport<crate::RoleClient, R, W>
where
    R: Send + AsyncRead + Unpin,
    W: Send + AsyncWrite + Unpin + 'static,
{
    pub fn new_client(read: R, write: W) -> Self {
        Self::new(read, write)
    }
}

#[cfg(feature = "server")]
impl<R, W> AsyncRwTransport<crate::RoleServer, R, W>
where
    R: Send + AsyncRead + Unpin,
    W: Send + AsyncWrite + Unpin + 'static,
{
    pub fn new_server(read: R, write: W) -> Self {
        Self::new(read, write)
    }
}

impl<Role: ServiceRole, R, W> Transport<Role> for AsyncRwTransport<Role, R, W>
where
    R: Send + AsyncRead + Unpin,
    W: Send + AsyncWrite + Unpin + 'static,
{
    type Error = std::io::Error;

    fn send(
        &mut self,
        item: TxJsonRpcMessage<Role>,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send + 'static {
        let lock = self.write.clone();
        async move {
            let mut write = lock.lock().await;
            if let Some(ref mut write) = *write {
                write.send(item).await.map_err(Into::into)
            } else {
                Err(std::io::Error::new(
                    std::io::ErrorKind::NotConnected,
                    "Transport is closed",
                ))
            }
        }
    }

    async fn receive(&mut self) -> Option<RxJsonRpcMessage<Role>> {
        loop {
            match self.read_line().await {
                Ok(false) => return None,
                Ok(true) => {}
                Err(error) => {
                    self.read_closed = true;
                    self.line_buf.clear();
                    tracing::error!("Error reading from stream: {error}");
                    return None;
                }
            }
            // Parse a bounded complete line (or trailing EOF message), then clear
            // it before the next receive. Partial reads survive cancellation.
            let parsed = {
                let line = without_carriage_return(&self.line_buf);
                if line.is_empty() {
                    self.line_buf.clear();
                    continue;
                }
                try_parse_with_compatibility::<RxJsonRpcMessage<Role>>(line, "receive")
            };
            self.line_buf.clear();
            match parsed {
                Ok(Some(msg)) => return Some(msg),
                Ok(None) => continue,
                Err(JsonRpcMessageCodecError::Serde(e)) => {
                    match e.classify() {
                        serde_json::error::Category::Syntax | serde_json::error::Category::Eof => {
                            // The input isn't valid JSON, so there's no message id to correlate a
                            // response to, and replying to invalid data can trigger an error storm
                            // if the peer echoes the response back as more invalid data. This
                            // matches the other official MCP SDKs, which ignore unparsable input.
                            // See https://github.com/modelcontextprotocol/rust-sdk/issues/938
                            tracing::debug!("Ignoring unparsable incoming message: {e}");
                        }
                        serde_json::error::Category::Data | serde_json::error::Category::Io => {
                            // Well-formed JSON that doesn't match the expected message shape is a
                            // real protocol error rather than unparsable input, so surface it with
                            // an Invalid Request response instead of silently dropping it.
                            tracing::debug!("Protocol error on incoming message: {e}");
                            let mut write = self.write.lock().await;
                            let framed = write.as_mut()?;
                            let response = TxJsonRpcMessage::<Role>::error(
                                ErrorData::invalid_request("Invalid request", None),
                                None,
                            );
                            if framed.send(response).await.is_err() {
                                return None;
                            }
                        }
                    }
                }
                Err(e) => {
                    tracing::error!("Error reading from stream: {}", e);
                    return None;
                }
            }
        }
    }

    async fn close(&mut self) -> Result<(), Self::Error> {
        self.read_closed = true;
        self.line_buf.clear();
        let mut write = self.write.lock().await;
        drop(write.take());
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct JsonRpcMessageCodec<T> {
    _marker: PhantomData<fn() -> T>,
    next_index: usize,
    max_length: usize,
    is_discarding: bool,
}

impl<T> Default for JsonRpcMessageCodec<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> JsonRpcMessageCodec<T> {
    pub fn new() -> Self {
        Self {
            _marker: PhantomData,
            next_index: 0,
            max_length: usize::MAX,
            is_discarding: false,
        }
    }

    pub fn new_with_max_length(max_length: usize) -> Self {
        Self {
            max_length,
            ..Self::new()
        }
    }

    pub fn max_length(&self) -> usize {
        self.max_length
    }
}

fn without_carriage_return(s: &[u8]) -> &[u8] {
    s.strip_suffix(b"\r").unwrap_or(s)
}

/// UTF-8 byte order mark. RFC 8259 §8.1 allows JSON parsers to ignore a leading BOM.
const UTF8_BOM: &[u8; 3] = b"\xEF\xBB\xBF";

/// Check if a method is a standard MCP method (request, response, or notification).
/// This includes both requests and notifications defined in the MCP specification.
///
/// Based on MCP specification 2026-07-28: https://modelcontextprotocol.io/specification/2026-07-28
fn is_standard_method(method: &str) -> bool {
    matches!(
        method,
        "initialize"
            | "ping"
            | "server/discover"
            | "prompts/get"
            | "prompts/list"
            | "resources/list"
            | "resources/read"
            | "resources/subscribe"
            | "resources/unsubscribe"
            | "resources/templates/list"
            | "subscriptions/listen"
            | "tools/call"
            | "tools/list"
            | "completion/complete"
            | "elicitation/create"
            | "logging/setLevel"
            | "roots/list"
            | "sampling/createMessage"
    ) || is_standard_notification(method)
}

fn is_standard_notification(method: &str) -> bool {
    matches!(
        method,
        "notifications/cancelled"
            | "notifications/initialized"
            | "notifications/message"
            | "notifications/progress"
            | "notifications/prompts/list_changed"
            | "notifications/resources/list_changed"
            | "notifications/resources/updated"
            | "notifications/roots/list_changed"
            | "notifications/subscriptions/acknowledged"
            | "notifications/tools/list_changed"
    )
}

/// Determines if a notification should be ignored for compatibility.
fn should_ignore_notification(json_value: &serde_json::Value, method: &str) -> bool {
    let is_notification = json_value.get("id").is_none();

    // Ignore non-MCP notifications (like LSP messages) for compatibility
    if is_notification && !is_standard_method(method) {
        tracing::trace!(
            "Ignoring non-MCP notification '{}' for compatibility",
            method
        );
        return true;
    }

    // Ignore non-standard MCP notifications
    matches!(
        (
            method.starts_with("notifications/"),
            is_standard_notification(method)
        ),
        (true, false)
    )
}

/// Try to parse a message with compatibility handling for non-standard notifications
fn try_parse_with_compatibility<T: serde::de::DeserializeOwned>(
    line: &[u8],
    context: &str,
) -> Result<Option<T>, JsonRpcMessageCodecError> {
    let line = line.strip_prefix(UTF8_BOM.as_slice()).unwrap_or(line);
    if let Ok(line_str) = std::str::from_utf8(line) {
        match serde_json::from_slice(line) {
            Ok(item) => Ok(Some(item)),
            Err(e) => {
                // Check if this is a notification that should be ignored for compatibility
                if let Ok(json_value) = serde_json::from_str::<serde_json::Value>(line_str)
                    && let Some(method) =
                        json_value.get("method").and_then(serde_json::Value::as_str)
                    && should_ignore_notification(&json_value, method)
                {
                    return Ok(None);
                }

                tracing::debug!(
                    "Failed to parse message {}: {} | Error: {}",
                    context,
                    line_str,
                    e
                );
                Err(JsonRpcMessageCodecError::Serde(e))
            }
        }
    } else {
        serde_json::from_slice(line)
            .map(Some)
            .map_err(JsonRpcMessageCodecError::Serde)
    }
}

#[derive(Debug, Error)]
#[non_exhaustive]
pub enum JsonRpcMessageCodecError {
    #[error("max line length exceeded")]
    MaxLineLengthExceeded,
    #[error("serde error {0}")]
    Serde(#[from] serde_json::Error),
    #[error("io error {0}")]
    Io(#[from] std::io::Error),
}

impl From<JsonRpcMessageCodecError> for std::io::Error {
    fn from(value: JsonRpcMessageCodecError) -> Self {
        match value {
            JsonRpcMessageCodecError::MaxLineLengthExceeded => {
                std::io::Error::new(std::io::ErrorKind::InvalidData, value)
            }
            JsonRpcMessageCodecError::Serde(e) => e.into(),
            JsonRpcMessageCodecError::Io(e) => e,
        }
    }
}

impl<T: DeserializeOwned> Decoder for JsonRpcMessageCodec<T> {
    type Item = T;

    type Error = JsonRpcMessageCodecError;

    fn decode(
        &mut self,
        buf: &mut BytesMut,
    ) -> Result<Option<Self::Item>, JsonRpcMessageCodecError> {
        loop {
            // Determine how far into the buffer we'll search for a newline. If
            // there's no max_length set, we'll read to the end of the buffer.
            let read_to = std::cmp::min(self.max_length.saturating_add(1), buf.len());

            let newline_offset = buf[self.next_index..read_to]
                .iter()
                .position(|b| *b == b'\n');

            match (self.is_discarding, newline_offset) {
                (true, Some(offset)) => {
                    // If we found a newline, discard up to that offset and
                    // then stop discarding. On the next iteration, we'll try
                    // to read a line normally.
                    buf.advance(offset + self.next_index + 1);
                    self.is_discarding = false;
                    self.next_index = 0;
                }
                (true, None) => {
                    // Otherwise, we didn't find a newline, so we'll discard
                    // everything we read. On the next iteration, we'll continue
                    // discarding up to max_len bytes unless we find a newline.
                    buf.advance(read_to);
                    self.next_index = 0;
                    if buf.is_empty() {
                        return Ok(None);
                    }
                }
                (false, Some(offset)) => {
                    // Found a line!
                    let newline_index = offset + self.next_index;
                    self.next_index = 0;
                    let line = buf.split_to(newline_index + 1);
                    let line = &line[..line.len() - 1];
                    let line = without_carriage_return(line);

                    // Use compatibility handling function
                    let item = match try_parse_with_compatibility(line, "decode")? {
                        Some(item) => item,
                        None => return Ok(None), // Skip non-standard message
                    };
                    return Ok(Some(item));
                }
                (false, None) if buf.len() > self.max_length => {
                    // Reached the maximum length without finding a
                    // newline, return an error and start discarding on the
                    // next call.
                    self.is_discarding = true;
                    return Err(JsonRpcMessageCodecError::MaxLineLengthExceeded);
                }
                (false, None) => {
                    // We didn't find a line or reach the length limit, so the next
                    // call will resume searching at the current offset.
                    self.next_index = read_to;
                    return Ok(None);
                }
            }
        }
    }

    fn decode_eof(&mut self, buf: &mut BytesMut) -> Result<Option<T>, JsonRpcMessageCodecError> {
        Ok(match self.decode(buf)? {
            Some(frame) => Some(frame),
            None => {
                self.next_index = 0;
                // No terminating newline - return remaining data, if any
                if buf.is_empty() || buf == &b"\r"[..] {
                    None
                } else {
                    let line = buf.split_to(buf.len());
                    let line = without_carriage_return(&line);

                    // Use compatibility handling function
                    let item = match try_parse_with_compatibility(line, "decode_eof")? {
                        Some(item) => item,
                        None => return Ok(None), // Skip non-standard message
                    };
                    Some(item)
                }
            }
        })
    }
}

impl<T: Serialize> Encoder<T> for JsonRpcMessageCodec<T> {
    type Error = JsonRpcMessageCodecError;

    fn encode(&mut self, item: T, buf: &mut BytesMut) -> Result<(), JsonRpcMessageCodecError> {
        serde_json::to_writer(buf.writer(), &item)?;
        buf.put_u8(b'\n');
        Ok(())
    }
}

#[cfg(all(test, feature = "server"))]
mod bounded_input_tests {
    use super::*;
    use crate::RoleServer;
    use tokio::io::AsyncWriteExt;

    fn message() -> Vec<u8> {
        br#"{"jsonrpc":"2.0","id":1,"method":"tools/list","params":{}}"#.to_vec()
    }

    #[tokio::test]
    async fn exact_boundaries_include_cr_and_allow_unterminated_eof() {
        for suffix in [b"\n".as_slice(), b"\r\n", b""] {
            let payload = message();
            let limit = payload.len() + usize::from(suffix == b"\r\n");
            let (mut writer, reader) = tokio::io::duplex(1024);
            writer.write_all(&payload).await.unwrap();
            writer.write_all(suffix).await.unwrap();
            writer.shutdown().await.unwrap();
            let mut transport = AsyncRwTransport::<RoleServer, _, _>::new_with_max_message_size(
                reader,
                tokio::io::sink(),
                limit,
            );
            assert!(transport.receive().await.is_some());
            assert!(transport.receive().await.is_none());
        }
    }

    #[tokio::test]
    async fn oversized_line_ends_reception_without_accepting_later_messages() {
        let payload = message();
        for suffix in [b"\n".as_slice(), b"\r\n", b""] {
            let (mut writer, reader) = tokio::io::duplex(1024);
            writer.write_all(&payload).await.unwrap();
            writer.write_all(suffix).await.unwrap();
            writer.write_all(b"\n").await.unwrap();
            writer.write_all(&payload).await.unwrap();
            writer.write_all(b"\n").await.unwrap();
            let mut transport = AsyncRwTransport::<RoleServer, _, _>::new_with_max_message_size(
                reader,
                tokio::io::sink(),
                payload.len() + usize::from(suffix == b"\r\n") - 1,
            );
            assert!(transport.receive().await.is_none());
            assert!(transport.read_closed);
            assert!(transport.line_buf.is_empty());
            assert!(transport.receive().await.is_none());
        }
    }

    #[tokio::test]
    async fn endless_input_is_rejected_without_waiting_for_newline_or_eof() {
        let (mut writer, reader) = tokio::io::duplex(1024);
        writer.write_all(&[b' '; 33]).await.unwrap();
        let mut transport = AsyncRwTransport::<RoleServer, _, _>::new_with_max_message_size(
            reader,
            tokio::io::sink(),
            32,
        );
        assert!(
            tokio::time::timeout(std::time::Duration::from_secs(1), transport.receive())
                .await
                .unwrap()
                .is_none()
        );
        assert!(transport.line_buf.is_empty());
        assert!(transport.line_buf.capacity() <= 32);
        drop(writer);
    }

    #[tokio::test]
    async fn cancellation_preserves_fragments_and_limits_across_receive_calls() {
        let payload = message();
        let (mut writer, reader) = tokio::io::duplex(1024);
        let mut transport = AsyncRwTransport::<RoleServer, _, _>::new_with_max_message_size(
            reader,
            tokio::io::sink(),
            payload.len(),
        );
        for byte in &payload {
            writer.write_all(&[*byte]).await.unwrap();
            let mut pending = Box::pin(transport.receive());
            assert!(futures::poll!(pending.as_mut()).is_pending());
            drop(pending);
        }
        assert_eq!(transport.line_buf, payload);
        writer.write_all(b"\n").await.unwrap();
        assert!(transport.receive().await.is_some());
        writer.write_all(&payload).await.unwrap();
        let mut pending = Box::pin(transport.receive());
        assert!(futures::poll!(pending.as_mut()).is_pending());
        drop(pending);
        writer.write_all(b" ").await.unwrap();
        assert!(transport.receive().await.is_none());
        assert!(transport.line_buf.is_empty());
    }

    #[tokio::test]
    async fn per_message_budget_resets_and_malformed_lines_do_not_poison_it() {
        let payload = message();
        let (mut writer, reader) = tokio::io::duplex(1024);
        writer.write_all(b"\n{broken\n\r\n").await.unwrap();
        for _ in 0..2 {
            writer.write_all(&payload).await.unwrap();
            writer.write_all(b"\n").await.unwrap();
        }
        let mut transport = AsyncRwTransport::<RoleServer, _, _>::new_with_max_message_size(
            reader,
            tokio::io::sink(),
            payload.len(),
        );
        assert!(transport.receive().await.is_some());
        assert!(transport.receive().await.is_some());
        transport.close().await.unwrap();
        assert!(transport.receive().await.is_none());
        let mut transport = AsyncRwTransport::<RoleServer, _, _>::new_with_max_message_size(
            b"\n\nX".as_slice(),
            tokio::io::sink(),
            0,
        );
        assert!(transport.receive().await.is_none());
        assert!(transport.line_buf.is_empty());
    }

    #[tokio::test]
    async fn default_limit_is_finite_without_eager_payload_allocation() {
        let transport =
            AsyncRwTransport::<RoleServer, _, _>::new(tokio::io::empty(), tokio::io::sink());
        assert_eq!(transport.max_message_size, 16 * 1024 * 1024);
        assert_eq!(transport.line_buf.capacity(), 0);
    }
}
