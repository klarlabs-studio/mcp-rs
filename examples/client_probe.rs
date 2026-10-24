//! Typed client interoperability probe for the reference and example servers.
use mcp_rs::{
    Client, protocol::transport::streamable_http_client::StreamableHttpClientTransportConfig,
};
use serde::Serialize;
#[derive(Serialize)]
struct Input {
    name: String,
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut args = std::env::args().skip(1);
    let mode = args.next().ok_or("expected --stdio or --http")?;
    let target = args.next().ok_or("expected a program or endpoint")?;
    let client = match mode.as_str() {
        "--metadata-stdio" => {
            let mut command = tokio::process::Command::new(target);
            command.arg("--metadata-stdio");
            Client::stdio(command).await?
        }
        "--stdio" => Client::stdio(tokio::process::Command::new(target)).await?,
        "--http" => {
            Client::http(
                reqwest::Client::builder()
                    .timeout(std::time::Duration::from_secs(5))
                    .build()?,
                StreamableHttpClientTransportConfig::with_uri(target),
            )
            .await?
        }
        #[cfg(feature = "websocket")]
        "--websocket" => Client::websocket(target).await?,
        #[cfg(feature = "grpc")]
        "--grpc" => {
            use mcp_rs::transport::grpc::{proto, tonic};
            let channel = tonic::transport::Endpoint::from_shared(target)?
                .connect_timeout(std::time::Duration::from_secs(5))
                .connect()
                .await?;
            Client::grpc(
                proto::mcp_client::McpClient::new(channel),
                Default::default(),
            )
            .await?
        }
        _ => return Err("expected --stdio or --http".into()),
    };
    if mode == "--metadata-stdio" {
        let metadata = client.server_metadata()?.ok_or("missing server identity")?;
        let discovery = client
            .peer()
            .discover(mcp_rs::model::RequestMetaObject::new())
            .await?;
        if mcp_rs::ServerMetadata::from_discovery(&discovery)?.as_ref() != Some(&metadata) {
            return Err("discovery and handshake metadata differ".into());
        }
        // Complete a post-discovery request before tearing down stdio.
        client.peer().list_tools(None).await?;
        client.close().await?;
        println!(
            "{}",
            serde_json::json!({"identity":metadata.identity,"buildInfo":metadata.build_info})
        );
        return Ok(());
    }
    let output: String = client
        .tool::<Input, String>("hello")
        .call(&Input {
            name: "Felix".into(),
        })
        .await?;
    if output != "Hello Felix" {
        return Err("unexpected tool result".into());
    }
    #[cfg(any(feature = "websocket", feature = "grpc"))]
    if mode == "--websocket" || mode == "--grpc" {
        let structured: serde_json::Value = client
            .call(
                "structured",
                &Input {
                    name: "Felix".into(),
                },
            )
            .await?;
        if structured != serde_json::json!({"message":"Hello Felix"}) {
            return Err("structured result differs".into());
        }
        if client
            .call_raw(
                "does-not-exist",
                &Input {
                    name: "Felix".into(),
                },
            )
            .await
            .is_ok()
        {
            return Err("missing protocol error".into());
        }
        let resource = client
            .peer()
            .read_resource(mcp_rs::model::ReadResourceRequestParams::new(
                "users://Felix",
            ))
            .await?;
        if serde_json::to_value(resource)?.pointer("/contents/0/text")
            != Some(&serde_json::json!("User Felix"))
        {
            return Err("resource differs".into());
        }
    }
    client.close().await?;
    println!("typed client: {output}");
    Ok(())
}
