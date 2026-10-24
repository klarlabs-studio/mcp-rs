//! Cross-language TLS fixture. Checked-in identities are for loopback tests only.
#[path = "support/tls.rs"]
mod tls;
use mcp_rs::{Client, Server, transport};
use schemars::JsonSchema;
use serde::Deserialize;
use serde_json::json;
use std::time::Duration;
use tokio_util::sync::CancellationToken;
use transport::grpc::{
    proto,
    tonic::transport::{Certificate, ClientTlsConfig, Endpoint, Identity, ServerTlsConfig},
};

type Error = Box<dyn std::error::Error + Send + Sync>;
#[derive(Deserialize, JsonSchema)]
struct Input {
    name: String,
}
fn application() -> Result<Server, Error> {
    let mut server = Server::new("tls-fixture", "1");
    server
        .tool("hello")
        .handler(|_, input: Input| async move { Ok(format!("Hello {}", input.name)) })?;
    server
        .resource("users://{id}")
        .template_handler(|_, _, params| async move { Ok(format!("User {}", params["id"])) })?;
    Ok(server)
}
async fn check(client: Client) -> Result<(), Error> {
    let result: String = client.call("hello", &json!({"name":"Felix"})).await?;
    if result != "Hello Felix" {
        return Err("tool result differs".into());
    }
    if client.call_raw("does-not-exist", &json!({})).await.is_ok() {
        return Err("missing protocol error".into());
    }
    let resource = client
        .peer()
        .read_resource(mcp_rs::model::ReadResourceRequestParams::new(
            "users://Felix",
        ))
        .await?;
    if serde_json::to_value(resource)?.pointer("/contents/0/text") != Some(&json!("User Felix")) {
        return Err("resource differs".into());
    }
    client.close().await?;
    println!("TLS probe: discovery, tool, protocol error, resource");
    Ok(())
}
#[tokio::main]
async fn main() -> Result<(), Error> {
    // This example opts into one provider without affecting the library's global policy.
    let _ = transport::websocket::tokio_rustls::rustls::crypto::aws_lc_rs::default_provider()
        .install_default();
    let args: Vec<_> = std::env::args().skip(1).collect();
    let [operation, mode, address, identity] = args.as_slice() else {
        return Err("expected serve|probe websocket|grpc address trusted|missing|untrusted|wrong-ca|wrong-host (serve: tls|mtls)".into());
    };
    if operation == "serve" {
        let mtls = match identity.as_str() {
            "tls" => false,
            "mtls" => true,
            _ => return Err("expected tls or mtls".into()),
        };
        let stop = CancellationToken::new();
        let listener = tokio::net::TcpListener::bind(address).await?;
        let server = application()?;
        let serving = async {
            match mode.as_str() {
                "websocket" => transport::websocket::serve_websocket_tls(
                    server,
                    listener,
                    stop.clone(),
                    Default::default(),
                    tls::server(mtls)?,
                )
                .await
                .map_err(|e| -> Error { Box::new(e) }),
                "grpc" => {
                    let config = if mtls {
                        ServerTlsConfig::new().client_ca_root(Certificate::from_pem(tls::CA))
                    } else {
                        ServerTlsConfig::new()
                    };
                    transport::grpc::serve_grpc_tls(
                        server,
                        listener,
                        stop.clone(),
                        Identity::from_pem(tls::SERVER, tls::SERVER_KEY),
                        config,
                    )
                    .await
                    .map_err(|e| -> Error { Box::new(e) })
                }
                _ => Err("unknown transport".into()),
            }
        };
        tokio::pin!(serving);
        tokio::select! {
            result = &mut serving => result?,
            signal = tokio::signal::ctrl_c() => {
                signal?;
                stop.cancel();
                tokio::time::timeout(Duration::from_secs(4), &mut serving).await??;
            }
        }
        return Ok(());
    }
    if operation != "probe" {
        return Err("unknown operation".into());
    }
    tokio::time::timeout(Duration::from_secs(8), async {
        let client = match mode.as_str() {
            "websocket" => {
                if identity == "wrong-host" {
                    // Keep the socket target local while presenting a different TLS server name.
                    use transport::websocket::{
                        tokio_rustls::TlsConnector, tokio_rustls::rustls::pki_types::ServerName,
                    };
                    let stream = tokio::net::TcpStream::connect(address).await?;
                    let _ = TlsConnector::from(tls::client(identity)?)
                        .connect(ServerName::try_from("wrong.test")?, stream)
                        .await?;
                    println!("wrong hostname accepted");
                    return Ok(());
                }
                let port = address.rsplit_once(':').ok_or("missing port")?.1;
                Client::websocket_tls(
                    format!("wss://localhost:{port}/mcp"),
                    tls::client(identity)?,
                )
                .await?
            }
            "grpc" => {
                let mut config = ClientTlsConfig::new()
                    .domain_name(if identity == "wrong-host" {
                        "wrong.test"
                    } else {
                        "localhost"
                    })
                    .ca_certificate(Certificate::from_pem(if identity == "wrong-ca" {
                        tls::OTHER_CA
                    } else {
                        tls::CA
                    }));
                match identity.as_str() {
                    "trusted" => {
                        config = config.identity(Identity::from_pem(tls::CLIENT, tls::CLIENT_KEY))
                    }
                    "untrusted" => {
                        config = config
                            .identity(Identity::from_pem(tls::OTHER_CLIENT, tls::OTHER_CLIENT_KEY))
                    }
                    "missing" | "wrong-ca" | "wrong-host" => {}
                    _ => return Err("unknown test identity".into()),
                }
                let channel = Endpoint::from_shared(format!("https://{address}"))?
                    .connect_timeout(Duration::from_secs(3))
                    .tls_config(config)?
                    .connect()
                    .await?;
                Client::grpc(
                    proto::mcp_client::McpClient::new(channel),
                    Default::default(),
                )
                .await?
            }
            _ => return Err("unknown transport".into()),
        };
        check(client).await
    })
    .await?
}
