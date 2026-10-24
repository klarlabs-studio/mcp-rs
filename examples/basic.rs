//! Example application using either stdio or Streamable HTTP.
use mcp_rs::{Server, ToolError, model::*, schemars::JsonSchema, transport};
use serde::Deserialize;
use tokio_util::sync::CancellationToken;

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct Hello {
    /// Name of the person to greet.
    name: String,
}

fn application() -> Result<Server, mcp_rs::RegistrationError> {
    let mut server = Server::new("mcp-rs-example", "0.1.0");
    server
        .tool("hello")
        .description("Say hello")
        .handler(|_ctx, input: Hello| async move {
            if input.name.trim().is_empty() {
                return Err(ToolError::new("Name must not be empty"));
            }
            Ok(format!("Hello {}", input.name))
        })?;
    server
        .resource("about://server")
        .name("About")
        .handler(|_ctx, _uri| async { Ok("A typed Rust MCP framework".into()) })?;
    server
        .prompt("greet")
        .description("Compose a greeting")
        .argument("name", "Person to greet", true)
        .handler(|_ctx, args| async move {
            Ok(GetPromptResult::new(vec![PromptMessage::new_text(
                Role::User,
                format!("Greet {}", args["name"]),
            )]))
        })?;
    Ok(server)
}

type ExampleError = Box<dyn std::error::Error + Send + Sync>;

fn main() -> Result<(), ExampleError> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let result = runtime.block_on(run());
    // Tokio stdin uses an uninterruptible blocking read. Await async service
    // cleanup first, then bound runtime teardown while the client holds stdin open.
    runtime.shutdown_timeout(std::time::Duration::from_secs(1));
    result
}

async fn run() -> Result<(), ExampleError> {
    let shutdown = CancellationToken::new();
    let service = async {
        let server = application()?;
        if std::env::args().any(|arg| arg == "--http") {
            let bind = std::env::var("MCP_RS_BIND").unwrap_or_else(|_| "127.0.0.1:8080".into());
            let listener = tokio::net::TcpListener::bind(&bind).await?;
            eprintln!("MCP endpoint: http://{}/mcp", listener.local_addr()?);
            transport::serve_http(server, listener, shutdown.clone()).await?;
        } else {
            transport::serve_stdio(server, shutdown.clone()).await?;
        }
        Ok(())
    };
    tokio::pin!(service);
    tokio::select! {
        result = &mut service => result,
        signal = tokio::signal::ctrl_c() => {
            signal?;
            shutdown.cancel();
            service.await
        }
    }
}
