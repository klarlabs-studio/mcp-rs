//! Native Rust TLS/mTLS round trips and certificate admission checks.
#![cfg(all(feature = "grpc-tls", feature = "client"))]
#![allow(clippy::unwrap_used)]
use mcp_rs::{
    Client, Server,
    transport::grpc::{proto, serve_grpc_tls, tonic},
};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;
use std::{net::SocketAddr, time::Duration};
use tokio::{net::TcpListener, task::JoinHandle, time::timeout};
use tokio_util::sync::CancellationToken;
use tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity, ServerTlsConfig};

const CA: &[u8] = include_bytes!("fixtures/tls/ca.pem");
const OTHER_CA: &[u8] = include_bytes!("fixtures/tls/untrusted-ca.pem");
fn server_identity() -> Identity {
    Identity::from_pem(
        include_bytes!("fixtures/tls/server.pem"),
        include_bytes!("fixtures/tls/server-key.pem"),
    )
}
fn client_identity(trusted: bool) -> Identity {
    if trusted {
        Identity::from_pem(
            include_bytes!("fixtures/tls/client.pem"),
            include_bytes!("fixtures/tls/client-key.pem"),
        )
    } else {
        Identity::from_pem(
            include_bytes!("fixtures/tls/untrusted-client.pem"),
            include_bytes!("fixtures/tls/untrusted-client-key.pem"),
        )
    }
}
#[derive(Deserialize, JsonSchema)]
struct Input {}
fn application() -> Server {
    let mut server = Server::new("native-tls", "1");
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
    async fn start(mut tls: ServerTlsConfig) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let stop = CancellationToken::new();
        let token = stop.clone();
        tls = tls
            .identity(server_identity())
            .timeout(Duration::from_secs(2));
        let task = tokio::spawn(async move {
            serve_grpc_tls(application(), listener, token, server_identity(), tls)
                .await
                .unwrap();
        });
        Self { addr, stop, task }
    }
    async fn close(self) {
        self.stop.cancel();
        timeout(Duration::from_secs(3), self.task)
            .await
            .unwrap()
            .unwrap();
    }
    async fn client(&self, tls: ClientTlsConfig) -> Result<Client, Box<dyn std::error::Error>> {
        let channel = Endpoint::from_shared(format!("https://{}", self.addr))?
            .connect_timeout(Duration::from_secs(2))
            .tls_config(tls)?
            .connect()
            .await?;
        Ok(Client::grpc(
            proto::mcp_client::McpClient::new(channel),
            Default::default(),
        )
        .await?)
    }
    async fn rejected(&self, tls: ClientTlsConfig) {
        // Some TLS 1.3 client-auth failures surface on the first RPC rather than connect.
        let result = timeout(Duration::from_secs(3), self.client(tls))
            .await
            .unwrap();
        assert!(
            result.is_err(),
            "untrusted peer must not establish an MCP client"
        );
    }
}
fn trusted() -> ClientTlsConfig {
    ClientTlsConfig::new()
        .domain_name("localhost")
        .ca_certificate(Certificate::from_pem(CA))
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
async fn tls_roundtrip_rejects_unknown_ca_wrong_hostname_and_plaintext() {
    let f = Fixture::start(ServerTlsConfig::new()).await;
    f.rejected(
        ClientTlsConfig::new()
            .domain_name("localhost")
            .ca_certificate(Certificate::from_pem(OTHER_CA)),
    )
    .await;
    f.rejected(trusted().domain_name("other.test")).await;
    // An HTTP/2 plaintext channel must fail before it can discover the MCP server.
    let plain = timeout(Duration::from_secs(3), async {
        let channel = Endpoint::from_shared(format!("http://{}", f.addr))
            .unwrap()
            .connect_timeout(Duration::from_secs(2))
            .connect()
            .await?;
        Client::grpc(
            proto::mcp_client::McpClient::new(channel),
            Default::default(),
        )
        .await
        .map_err(|e| -> Box<dyn std::error::Error> { Box::new(e) })
    })
    .await
    .unwrap();
    assert!(plain.is_err());
    // Invalid connections must not stop the listener.
    let client = timeout(Duration::from_secs(3), f.client(trusted()))
        .await
        .unwrap()
        .unwrap();
    hello(&client).await;
    // Server shutdown also closes an active encrypted MCP session.
    f.close().await;
    timeout(Duration::from_secs(3), client.close())
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn mtls_requires_trusted_client_certificate_and_keeps_serving() {
    let f = Fixture::start(ServerTlsConfig::new().client_ca_root(Certificate::from_pem(CA))).await;
    f.rejected(trusted()).await;
    f.rejected(trusted().identity(client_identity(false))).await;
    let client = timeout(
        Duration::from_secs(3),
        f.client(trusted().identity(client_identity(true))),
    )
    .await
    .unwrap()
    .unwrap();
    hello(&client).await;
    timeout(Duration::from_secs(3), client.close())
        .await
        .unwrap()
        .unwrap();
    f.close().await;
}

#[tokio::test]
async fn invalid_tls_identity_fails_before_serving() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    assert!(
        serve_grpc_tls(
            application(),
            listener,
            CancellationToken::new(),
            Identity::from_pem("invalid certificate", "invalid private key"),
            ServerTlsConfig::new()
        )
        .await
        .is_err()
    );
}
