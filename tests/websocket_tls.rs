//! Rust-only WebSocket TLS/mTLS trust, policy and lifecycle checks.
#![cfg(all(feature = "websocket-tls", feature = "client"))]
#![allow(clippy::unwrap_used)]
use mcp_rs::{
    Client, Server,
    transport::websocket::{
        WebSocketError, WebSocketServerConfig, serve_websocket_tls,
        tokio_rustls::rustls::{
            self,
            pki_types::{CertificateDer, PrivateKeyDer, pem::PemObject},
        },
        tokio_tungstenite::tungstenite::{self, client::IntoClientRequest},
    },
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;
use std::{net::SocketAddr, num::NonZeroUsize, sync::Arc, time::Duration};
use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
    task::JoinHandle,
    time::timeout,
};
use tokio_util::sync::CancellationToken;

const CA: &[u8] = include_bytes!("fixtures/tls/ca.pem");
const OTHER_CA: &[u8] = include_bytes!("fixtures/tls/untrusted-ca.pem");
fn certs(pem: &[u8]) -> Vec<CertificateDer<'static>> {
    CertificateDer::pem_slice_iter(pem)
        .collect::<Result<Vec<_>, _>>()
        .unwrap()
}
fn roots(pem: &[u8]) -> rustls::RootCertStore {
    let mut roots = rustls::RootCertStore::empty();
    for cert in certs(pem) {
        roots.add(cert).unwrap();
    }
    roots
}
fn server_tls(mtls: bool) -> Arc<rustls::ServerConfig> {
    let builder = rustls::ServerConfig::builder();
    let builder = if mtls {
        builder.with_client_cert_verifier(
            rustls::server::WebPkiClientVerifier::builder(Arc::new(roots(CA)))
                .build()
                .unwrap(),
        )
    } else {
        builder.with_no_client_auth()
    };
    Arc::new(
        builder
            .with_single_cert(
                certs(include_bytes!("fixtures/tls/server.pem")),
                PrivateKeyDer::from_pem_slice(include_bytes!("fixtures/tls/server-key.pem"))
                    .unwrap(),
            )
            .unwrap(),
    )
}
fn client_tls(ca: &[u8], identity: Option<bool>) -> Arc<rustls::ClientConfig> {
    let builder = rustls::ClientConfig::builder().with_root_certificates(roots(ca));
    Arc::new(match identity {
        None => builder.with_no_client_auth(),
        Some(trusted) => {
            let (cert, key): (&[u8], &[u8]) = if trusted {
                (
                    include_bytes!("fixtures/tls/client.pem"),
                    include_bytes!("fixtures/tls/client-key.pem"),
                )
            } else {
                (
                    include_bytes!("fixtures/tls/untrusted-client.pem"),
                    include_bytes!("fixtures/tls/untrusted-client-key.pem"),
                )
            };
            builder
                .with_client_auth_cert(certs(cert), PrivateKeyDer::from_pem_slice(key).unwrap())
                .unwrap()
        }
    })
}
#[derive(Deserialize, JsonSchema)]
struct Input {}
fn application() -> Server {
    let mut server = Server::new("websocket-tls", "1");
    server
        .tool("hello")
        .handler(|_, _: Input| async { Ok("hello") })
        .unwrap();
    server
}
struct Fixture {
    addr: SocketAddr,
    stop: CancellationToken,
    task: JoinHandle<()>,
}
impl Fixture {
    async fn start(mtls: bool, config: WebSocketServerConfig) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let stop = CancellationToken::new();
        let token = stop.clone();
        let tls = server_tls(mtls);
        let task = tokio::spawn(async move {
            serve_websocket_tls(application(), listener, token, config, tls)
                .await
                .unwrap();
        });
        Self { addr, stop, task }
    }
    fn url(&self) -> String {
        format!("wss://localhost:{}/mcp", self.addr.port())
    }
    async fn close(self) {
        self.stop.cancel();
        timeout(Duration::from_secs(3), self.task)
            .await
            .unwrap()
            .unwrap();
    }
    async fn client(&self, tls: Arc<rustls::ClientConfig>) -> Result<Client, mcp_rs::ClientError> {
        timeout(
            Duration::from_secs(3),
            Client::websocket_tls(self.url(), tls),
        )
        .await
        .unwrap()
    }
}
async fn hello(client: &Client) {
    assert_eq!(
        timeout(
            Duration::from_secs(3),
            client.call::<_, String>("hello", &json!({}))
        )
        .await
        .unwrap()
        .unwrap(),
        "hello"
    );
}
#[tokio::test]
async fn tls_trust_hostname_origin_and_active_session_shutdown() {
    let f = Fixture::start(
        false,
        WebSocketServerConfig {
            allowed_origins: vec!["https://trusted.test".into()],
            ..Default::default()
        },
    )
    .await;
    assert!(f.client(client_tls(OTHER_CA, None)).await.is_err());
    assert!(
        timeout(
            Duration::from_secs(3),
            Client::websocket_tls(format!("wss://{}/mcp", f.addr), client_tls(CA, None))
        )
        .await
        .unwrap()
        .is_err()
    );
    // Plaintext cannot negotiate with the TLS listener.
    assert!(
        timeout(
            Duration::from_secs(3),
            Client::websocket(format!("ws://{}/mcp", f.addr))
        )
        .await
        .unwrap()
        .is_err()
    );
    let mut request = f.url().into_client_request().unwrap();
    request
        .headers_mut()
        .insert("origin", "https://evil.test".parse().unwrap());
    let error = timeout(
        Duration::from_secs(3),
        Client::websocket_tls(request, client_tls(CA, None)),
    )
    .await
    .unwrap()
    .unwrap_err();
    assert!(matches!(error, mcp_rs::ClientError::WebSocket(error)
        if matches!(*error, WebSocketError::Socket(ref error) if matches!(**error, tungstenite::Error::Http(ref response) if response.status() == 403))));
    let mut request = f.url().into_client_request().unwrap();
    request
        .headers_mut()
        .insert("origin", "https://trusted.test".parse().unwrap());
    let client = timeout(
        Duration::from_secs(3),
        Client::websocket_tls(request, client_tls(CA, None)),
    )
    .await
    .unwrap()
    .unwrap();
    hello(&client).await;
    f.close().await;
    timeout(Duration::from_secs(3), client.close())
        .await
        .unwrap()
        .unwrap();
}
#[tokio::test]
async fn mutual_tls_requires_trusted_client_and_keeps_serving() {
    let f = Fixture::start(true, Default::default()).await;
    assert!(f.client(client_tls(CA, None)).await.is_err());
    assert!(f.client(client_tls(CA, Some(false))).await.is_err());
    let client = f.client(client_tls(CA, Some(true))).await.unwrap();
    hello(&client).await;
    timeout(Duration::from_secs(3), client.close())
        .await
        .unwrap()
        .unwrap();
    f.close().await;
}
#[tokio::test]
async fn tls_handshake_deadline_connection_limit_and_stalled_shutdown() {
    let f = Fixture::start(
        false,
        WebSocketServerConfig {
            max_connections: NonZeroUsize::new(1).unwrap(),
            handshake_timeout: Duration::from_millis(300),
            ..Default::default()
        },
    )
    .await;
    let mut stalled = TcpStream::connect(f.addr).await.unwrap();
    let mut byte = [0];
    assert_eq!(
        timeout(Duration::from_secs(2), stalled.read(&mut byte))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    let client = f.client(client_tls(CA, None)).await.unwrap();
    hello(&client).await;
    timeout(Duration::from_secs(3), client.close())
        .await
        .unwrap()
        .unwrap();
    f.close().await;
    // Cancellation also interrupts a TLS handshake with a long configured deadline.
    let f = Fixture::start(
        false,
        WebSocketServerConfig {
            max_connections: NonZeroUsize::new(1).unwrap(),
            handshake_timeout: Duration::from_secs(60),
            ..Default::default()
        },
    )
    .await;
    let _stalled = TcpStream::connect(f.addr).await.unwrap();
    let mut excess = TcpStream::connect(f.addr).await.unwrap();
    // The first TLS handshake occupies the only permit, with a 60-second deadline.
    // The second socket must close promptly through admission, not that deadline.
    assert_eq!(
        timeout(Duration::from_secs(3), excess.read(&mut byte))
            .await
            .unwrap()
            .unwrap(),
        0
    );
    f.close().await;
}
#[tokio::test]
async fn tls_client_requires_wss_before_connecting() {
    for url in ["ws://127.0.0.1:1", "http://127.0.0.1:1"] {
        let error = Client::websocket_tls(url, client_tls(CA, None))
            .await
            .unwrap_err();
        assert!(
            matches!(error, mcp_rs::ClientError::WebSocket(error) if matches!(*error, WebSocketError::TlsRequired))
        );
    }
}

#[tokio::test]
async fn tls_keeps_inbound_message_limits_and_listener_health() {
    use futures_util::{SinkExt, StreamExt};
    use mcp_rs::transport::websocket::tokio_tungstenite::{self, Connector};
    let f = Fixture::start(
        false,
        WebSocketServerConfig {
            max_message_bytes: NonZeroUsize::new(1024).unwrap(),
            ..Default::default()
        },
    )
    .await;
    let (mut socket, _) = timeout(
        Duration::from_secs(3),
        tokio_tungstenite::connect_async_tls_with_config(
            f.url(),
            None,
            false,
            Some(Connector::Rustls(client_tls(CA, None))),
        ),
    )
    .await
    .unwrap()
    .unwrap();
    socket
        .send(tungstenite::Message::Text("x".repeat(2048).into()))
        .await
        .unwrap();
    let response = timeout(Duration::from_secs(3), socket.next())
        .await
        .unwrap();
    assert!(
        response.is_none()
            || response
                .is_some_and(|r| r.is_err() || matches!(r, Ok(tungstenite::Message::Close(_))))
    );
    let client = f.client(client_tls(CA, None)).await.unwrap();
    hello(&client).await;
    timeout(Duration::from_secs(3), client.close())
        .await
        .unwrap()
        .unwrap();
    f.close().await;
}
