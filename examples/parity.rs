//! Server matching tests/go-reference/main.go for cross-language wire fixtures.
use mcp_rs::{Server, model::*, transport};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use std::time::Duration;
#[cfg(feature = "http")]
use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};
use tokio_util::sync::CancellationToken;

#[cfg(feature = "http")]
struct SkillFixtureDirectory(PathBuf);

#[cfg(feature = "http")]
impl SkillFixtureDirectory {
    fn new() -> std::io::Result<Self> {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let root = std::env::temp_dir();
        for _ in 0..100 {
            let path = root.join(format!(
                "mcp-rs-skills-{}-{}",
                std::process::id(),
                NEXT.fetch_add(1, Ordering::Relaxed)
            ));
            match fs::create_dir(&path) {
                Ok(()) => return Ok(Self(path)),
                Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => continue,
                Err(error) => return Err(error),
            }
        }
        Err(std::io::Error::new(
            std::io::ErrorKind::AlreadyExists,
            "could not allocate a unique skills fixture directory",
        ))
    }

    fn root(&self) -> &std::path::Path {
        &self.0
    }
}

#[cfg(feature = "http")]
impl Drop for SkillFixtureDirectory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[derive(Deserialize, JsonSchema)]
struct Input {
    name: String,
}
#[derive(Deserialize, JsonSchema)]
struct CountInput {
    #[schemars(range(min = 1, max = 5))]
    count: i64,
}
#[derive(Serialize, JsonSchema)]
struct Output {
    message: String,
}

#[derive(Deserialize, Serialize, JsonSchema)]
struct DiscoveryInput {
    q: String,
}

fn discovery_application() -> Result<Server, mcp_rs::RegistrationError> {
    let mut server = Server::new("discovery-fixture", "1");
    for (name, description, tags, deferred) in [
        (
            "billing.create",
            "Create invoice",
            vec!["write", "finance"],
            true,
        ),
        (
            "billing.list",
            "List invoices",
            vec!["read", "finance"],
            false,
        ),
    ] {
        let tool = server
            .tool(name)
            .description(description)
            .group("billing")
            .tags(tags)
            .output_schema::<DiscoveryInput>()?;
        let tool = if deferred { tool.defer_schema() } else { tool };
        tool.handler(|_, input: DiscoveryInput| async move { Ok(input) })?;
    }
    server
        .tool("search")
        .description("Search catalog")
        .tags(["read"])
        .handler(|_, input: DiscoveryInput| async move { Ok(input.q) })?;
    for i in 0..102 {
        server
            .tool(format!("page_{i:03}"))
            .description("Catalog page")
            .group("catalog")
            .tags(["read", "finance"])
            .handler(|_, input: DiscoveryInput| async move { Ok(input.q) })?;
    }
    Ok(server.with_visibility_policy(
        |_, target| !matches!(target, mcp_rs::VisibilityTarget::Tool { name } if name == "page_000"),
    ))
}

fn application() -> Result<Server, mcp_rs::RegistrationError> {
    let mut server = Server::new("parity-fixture", "1");
    server
        .tool("hello")
        .description("Say hello")
        .handler(|ctx, input: Input| async move {
            if input.name == "Push" {
                ctx.peer
                    .notify_tool_list_changed()
                    .await
                    .map_err(|error| mcp_rs::ToolError::new(error.to_string()))?;
            }
            Ok(format!("Hello {}", input.name))
        })?;
    server
        .tool("count")
        .description("Constrained input")
        .handler(|_, input: CountInput| async move { Ok(input.count) })?;
    server
        .tool("structured")
        .description("Structured output")
        .output_schema::<Output>()?
        .handler(|_, input: Input| async move {
            Ok(Output {
                message: format!("Hello {}", input.name),
            })
        })?;
    server
        .tool("rich")
        .rich_handler(|_, input: Input| async move {
            let mut result = CallToolResult::success(vec![ContentBlock::text("display")]);
            result.structured_content = Some(serde_json::json!({"name":input.name}));
            Ok(result)
        })?;
    server.tool("empty").rich_handler(|_, _: Input| async {
        let mut result = CallToolResult::success(vec![]);
        result.structured_content = Some(serde_json::json!({}));
        Ok(result)
    })?;
    server.tool("failure").rich_handler(|_, _: Input| async {
        Ok(CallToolResult::error(vec![ContentBlock::text(
            "Expected failure",
        )]))
    })?;
    for i in 0..101 {
        server
            .tool(format!("page_{i:03}"))
            .handler(|_, _: Input| async { Ok("page") })?;
    }
    server
        .resource("about://server")
        .name("About")
        .mime_type("text/plain")
        .handler(|_, _| async { Ok("About the fixture".into()) })?;
    server
        .resource("users://{id}")
        .name("User")
        .mime_type("text/plain")
        .template_handler(|_, _, params| async move { Ok(format!("User {}", params["id"])) })?;
    server
        .resource("blob://logo")
        .name("Logo")
        .mime_type("application/octet-stream")
        .content_handler(|_, uri, _| async move {
            Ok(vec![
                ResourceContents::blob("AAEC", uri).with_mime_type("application/octet-stream"),
            ])
        })?;
    server
        .prompt("greet")
        .description("Greeting prompt")
        .argument("name", "Person", true)
        .handler(|_, args| async move {
            Ok(GetPromptResult::new(vec![PromptMessage::new_text(
                Role::User,
                format!("Greet {}", args["name"]),
            )]))
        })?;
    Ok(server)
}
fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()?;
    let result = runtime.block_on(async {
        let shutdown = CancellationToken::new();
        let signal = shutdown.clone();
        let watcher = tokio::spawn(async move {
            if tokio::signal::ctrl_c().await.is_ok() {
                signal.cancel();
            }
        });
        let args: Vec<_> = std::env::args().skip(1).collect();
        if args
            .first()
            .is_some_and(|arg| arg == "--subscription-stdio")
        {
            transport::serve_stdio(
                application()?.with_subscription_hub(mcp_rs::SubscriptionHub::default()),
                shutdown,
            )
            .await?;
        } else if args.first().is_some_and(|arg| arg == "--discovery-stdio") {
            transport::serve_stdio(discovery_application()?, shutdown).await?;
        } else if args.first().is_some_and(|arg| arg == "--visibility-stdio") {
            let server = application()?.with_visibility_policy(|_, target| match target {
                mcp_rs::VisibilityTarget::Tool { name } => name != "page_000",
                mcp_rs::VisibilityTarget::Resource { registered_uri, .. } => {
                    registered_uri != "users://{id}"
                }
                mcp_rs::VisibilityTarget::Prompt { name } => name != "greet",
                _ => false,
            });
            transport::serve_stdio(server, shutdown).await?;
        } else if args.first().is_some_and(|arg| arg == "--metadata-stdio") {
            let server = application()?
                .with_build_info(mcp_rs::BuildInfo::new("abc123", "2026-10-17T10:00:00Z"))
                .with_title("Research 🦀")
                .with_description("Track papers, code, and videos")
                .with_website_url("https://example.test/research")
                .with_icons([Icon::new("https://example.test/icon.svg")
                    .with_mime_type("image/svg+xml")
                    .with_sizes(vec!["any".into()])
                    .with_theme(IconTheme::Dark)])
                .with_instructions("Use search before reading details.\nCite the original source.");
            transport::serve_stdio(server, shutdown).await?;
        } else if let Some(mode @ ("--cache-private-stdio" | "--cache-public-stdio")) =
            args.first().map(String::as_str)
        {
            let (ttl_ms, scope) = if mode == "--cache-private-stdio" {
                (12_345, CacheScope::Private)
            } else {
                (0, CacheScope::Public)
            };
            transport::serve_stdio(application()?.with_result_cache(ttl_ms, scope), shutdown)
                .await?;
        } else if args.is_empty() {
            transport::serve_stdio(application()?, shutdown).await?;
        } else {
            let address = args.get(1).ok_or("expected a listen address")?;
            match args[0].as_str() {
                #[cfg(feature = "http")]
                "--skills-http" => {
                    use base64::Engine as _;
                    const SKILL_MD: &str = "---\nname: git-workflow\ndescription: Follow team Git conventions\n---\n\n# Git\n\nUse feature branches.\n";
                    const RELEASE_MD: &str = "---\nname: release-checklist\ndescription: Prepare a safe release\n---\n\n# Release\n\nReview the changelog.\n";
                    let fixtures = SkillFixtureDirectory::new()?;
                    let git = fixtures.root().join("git-workflow");
                    let release = fixtures.root().join("release-checklist");
                    fs::create_dir_all(git.join("references"))?;
                    fs::create_dir_all(release.join("templates"))?;
                    fs::write(git.join("SKILL.md"), SKILL_MD)?;
                    fs::write(git.join("notes.md"), "# Notes\n")?;
                    fs::write(git.join("references/workflow.svg"), "<svg>workflow</svg>\n")?;
                    fs::write(git.join("references/meta.json"), "{\"kind\":\"workflow\"}\n")?;
                    fs::write(git.join("icon.unknown"), [0, 1, 2, 255])?;
                    fs::write(release.join("SKILL.md"), RELEASE_MD)?;
                    fs::write(release.join("templates/checklist.md"), "# Checklist\n")?;
                    // These fixed archive payloads are shared with the Go and Python fixtures.
                    // Keep them in their own directory so no sibling SKILL.md can supply metadata.
                    let archives = fixtures.root().join("archives");
                    fs::create_dir_all(&archives)?;
                    for (name, encoded) in [
                        ("archive-zip.zip", "UEsDBBQAAAAIAAAAIVADubX0VgAAAGoAAAAIAAAAU0tJTEwubWQ1y80NgCAMQOF7p2jiuQ7AzSEcoIEqDYgE0Bin1/hzft8jIki8iEEu1usudGoGJ9UWzU3XZHB4g8O74KRH24pgDRoj0H1D9wuAsQo2L5jZBp6/5aE9XFBLAQIUAxQAAAAIAAAAIVADubX0VgAAAGoAAAAIAAAAAAAAAAAAAACkAQAAAABTS0lMTC5tZFBLBQYAAAAAAQABADYAAAB8AAAAAAA="),
                        ("archive-targz.tar.gz", "H4sIAAAAAAAC/+3UuwrCMBiG4cy5ioBzS1uqQjdBB1EXxQsIbdRQT7RVxKu3VrTQxcmi+D7LH/LnAIEvi8l4OnV3ifggr9QLw6qWmtXze/X4Md/3w0AoT7TglBc6K68U/8lxHLnXOxMpncUbezZO+Rzrq0xMHmf2WNjDPlKDRytRVU+t7KU4ZUblqd1u5f0E2XmukXKZG1VsjDrqONXr16ZqsSsFvsp8NBjORp/9AN7l3wv8Zv4Dv0v+29C5x9OtQ01AAQAAAAAAAAAAAAAAfssNeaEIQgAoAAA="),
                        ("archive-tgz.tgz", "H4sIAAAAAAAC/+3TywrCMBCF4azzFAHXLa1WC+5cit2JDxDaqEFbJU1FfHrrFXHvDf9vMyEzw6zOdDLOsrAsxAtFrUGSnGvruUZxv3t/X/7TOOkJFYk3aGqvXXtS/KcgCGSlSzNU2uVLuzOBXxxkYerc2a23m2qoRpdGodqOmtu9b5xR9cqu1/K0LTu3CSlntVF+adRW5yu9uK6cR0Mp8IXmG1dqH/q9/1z+ozh5zn+axuT/HR4yTUIBAAAAAAAAAAAAAAB+0BHJLb/jACgAAA=="),
                    ] {
                        fs::write(archives.join(name), base64::engine::general_purpose::STANDARD.decode(encoded)?)?;
                    }
                    let mut server = Server::new("skills-fixture", "1");
                    server.register_skills_from_directory(fixtures.root())?;
                    for (skill_path, file) in [
                        ("archive-zip", "archive-zip.zip"),
                        ("archive-targz", "archive-targz.tar.gz"),
                        ("archive-tgz", "archive-tgz.tgz"),
                    ] {
                        server.register_skill_archive(mcp_rs::skills::SkillArchive::from_file(
                            skill_path,
                            archives.join(file),
                        )?)?;
                    }
                    server
                        .skill_template("skill://docs/{product}/SKILL.md", "Product documentation")?
                        .handler(|_, _, params| async move {
                            Ok(format!("# Product\n\n{}\n", params["product"]))
                        })?;
                    let listener = tokio::net::TcpListener::bind(address).await?;
                    // Keep the source tree alive while the server is serving its snapshot.
                    let _fixtures = fixtures;
                    transport::serve_http(server, listener, shutdown).await?;
                }
                #[cfg(feature = "http")]
                "--server-card-http" => {
                    use mcp_rs::server_card::{Remote, ServerCard};
                    let identity = Implementation::new("card-fixture", "1")
                        .with_description("Discover research tools")
                        .with_title("Research")
                        .with_website_url("https://example.test");
                    let card = ServerCard::from_identity(&identity)
                        .with_remote(Remote::streamable_http("https://example.test/mcp"));
                    let router =
                        transport::http_router(Server::new("card-fixture", "1"), shutdown.clone())
                            .merge(transport::server_card::router(
                                &card,
                                "/mcp/server-card",
                                "urn:air:example.test:mcp:card-fixture",
                            )?);
                    let listener = tokio::net::TcpListener::bind(address).await?;
                    axum::serve(listener, router)
                        .with_graceful_shutdown(shutdown.cancelled_owned())
                        .await?;
                }
                #[cfg(feature = "websocket")]
                "--websocket" => {
                    let listener = tokio::net::TcpListener::bind(address).await?;
                    transport::websocket::serve_websocket(
                        application()?,
                        listener,
                        shutdown,
                        Default::default(),
                    )
                    .await?;
                }
                #[cfg(feature = "grpc")]
                "--grpc" => {
                    let listener = tokio::net::TcpListener::bind(address).await?;
                    transport::grpc::serve_grpc(application()?, listener, shutdown).await?;
                }
                _ => return Err("unknown or disabled transport".into()),
            }
        }
        watcher.abort();
        let _ = watcher.await;
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    });
    runtime.shutdown_timeout(Duration::from_secs(1));
    result
}
