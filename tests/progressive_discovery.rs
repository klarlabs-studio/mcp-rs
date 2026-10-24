//! Shared host catalog behavior and typed discovery models.
#![allow(clippy::unwrap_used)]
use mcp_rs::{Server, discovery::ToolDiscoveryParams, protocol::ServerHandler};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Deserialize, Serialize, JsonSchema)]
struct Input {
    q: String,
}

fn app() -> Server {
    let mut server = Server::new("discovery", "1");
    for (name, group, tags, deferred) in [
        ("billing.create", "billing", vec!["write", "finance"], true),
        ("billing.list", "billing", vec!["read", "finance"], false),
        ("search", "", vec!["read"], false),
    ] {
        let tool = server
            .tool(name)
            .group(group)
            .tags(tags)
            .description(name)
            .title(format!("Title {name}"))
            .meta(mcp_rs::model::MetaObject(
                serde_json::from_value(json!({"custom":true})).unwrap(),
            ))
            .output_schema::<Input>()
            .unwrap();
        let tool = if deferred { tool.defer_schema() } else { tool };
        tool.handler(|_, input: Input| async move { Ok(input) })
            .unwrap();
    }
    server
}

#[test]
fn wire_parameters_and_registration_validation_are_explicit() {
    let params: mcp_rs::model::ListToolsRequestParams =
        serde_json::from_value(json!({"group":"billing","tags":["read"],"detail":"names"}))
            .unwrap();
    assert_eq!(
        serde_json::to_value(params).unwrap(),
        json!({"group":"billing","tags":["read"],"detail":"names"})
    );
    let tool = app().get_tool("billing.create").unwrap();
    let value = serde_json::to_value(tool).unwrap();
    assert!(value.get("inputSchema").is_some());
    assert!(value.get("outputSchema").is_some());
    for invalid in [
        json!({"detail":"unknown"}),
        json!({"tags":"read"}),
        json!({"typo":true}),
    ] {
        assert!(serde_json::from_value::<ToolDiscoveryParams>(invalid).is_err());
    }
}

#[test]
fn typed_catalog_decodes_omitted_fields_and_preserves_extensions() {
    use mcp_rs::discovery::ToolDiscoveryResult;
    let minimal = json!({"tools":[{"name":"search"}]});
    let decoded: ToolDiscoveryResult = serde_json::from_value(minimal.clone()).unwrap();
    assert_eq!(decoded.tools[0].name, "search");
    assert!(decoded.tools[0].group.is_empty());
    assert!(decoded.tools[0].tags.is_empty());
    assert!(decoded.tools[0].input_schema.is_none());
    assert!(decoded.next_cursor.is_none());
    assert_eq!(serde_json::to_value(decoded).unwrap(), minimal);
    let rich = json!({"tools":[{
        "name":"search", "title":"Search", "description":"Find papers",
        "inputSchema":{"type":"object"}, "outputSchema":{"type":"string"},
        "annotations":{"readOnlyHint":true},
        "icons":[{"src":"https://example.com/icon.png"}],
        "_meta":{"custom":true}, "group":"research", "tags":["read"]
    }], "nextCursor":"search"});
    let decoded: ToolDiscoveryResult = serde_json::from_value(rich.clone()).unwrap();
    assert_eq!(serde_json::to_value(decoded).unwrap(), rich);
    for invalid in [
        json!({}),
        json!({"tools":[{}]}),
        json!({"tools":[{"name":"x","tags":"read"}]}),
    ] {
        assert!(serde_json::from_value::<ToolDiscoveryResult>(invalid).is_err());
    }
}

#[cfg(feature = "client")]
mod integration {
    use super::*;
    use mcp_rs::{Client, Context, ToolError, VisibilityTarget, protocol::ServiceExt};
    use serde_json::Value;
    use std::time::Duration;

    async fn connect(mut server: Server) -> (Client, tokio::task::JoinHandle<()>) {
        // A host can opt in to a catalog tool; no extra protocol method is installed.
        let catalog = server.clone();
        server
            .tool("catalog")
            .handler(move |ctx: Context, query: ToolDiscoveryParams| {
                let result = catalog.discover_tools(&ctx, &query);
                async move { result.map_err(|error| ToolError::new(error.to_string())) }
            })
            .unwrap();
        let (client_io, server_io) = tokio::io::duplex(65536);
        let task = tokio::spawn(async move {
            server
                .serve(server_io)
                .await
                .unwrap()
                .waiting()
                .await
                .unwrap();
        });
        (Client::connect(client_io).await.unwrap(), task)
    }
    async fn close(client: Client, task: tokio::task::JoinHandle<()>) {
        client.close().await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), task)
            .await
            .unwrap()
            .unwrap();
    }
    async fn query(client: &Client, params: Value) -> Value {
        client.call("catalog", &params).await.unwrap()
    }
    fn names(value: &Value) -> Vec<&str> {
        value["tools"]
            .as_array()
            .unwrap()
            .iter()
            .map(|tool| tool["name"].as_str().unwrap())
            .collect()
    }

    #[tokio::test]
    async fn typed_live_catalog_tracks_replacement_and_visibility() {
        use mcp_rs::{LiveServer, discovery::ToolDiscoveryResult};
        // Separate catalog and serving registries avoid a self-owning handler cycle.
        let live = LiveServer::new(app());
        let retained = live.snapshot();
        let mut host = Server::new("catalog-host", "1");
        let current = live.clone();
        let frozen = retained.clone();
        host.tool("current")
            .handler(move |ctx, params: ToolDiscoveryParams| {
                let result = current.discover_tools(&ctx, &params);
                async move { result.map_err(|error| ToolError::new(error.to_string())) }
            })
            .unwrap();
        host.tool("frozen")
            .handler(move |ctx, params: ToolDiscoveryParams| {
                let result = frozen.discover_tools(&ctx, &params);
                async move { result.map_err(|error| ToolError::new(error.to_string())) }
            })
            .unwrap();
        let (client, task) = connect(host).await;
        let params = ToolDiscoveryParams {
            group: "billing".into(),
            ..Default::default()
        };
        let before: ToolDiscoveryResult = client.call("current", &params).await.unwrap();
        assert_eq!(before.tools.len(), 2);
        assert!(before.tools[0].input_schema.is_none());
        assert!(before.tools[1].input_schema.is_some());
        assert!(before.tools[1].output_schema.is_some());
        assert_eq!(before.tools[1].meta.as_ref().unwrap().0["custom"], true);
        let mut replacement = retained.clone().with_visibility_policy(|_, target| {
            !matches!(target, VisibilityTarget::Tool {name} if name == "billing.create")
        });
        assert!(replacement.remove_tool("billing.list"));
        replacement
            .tool("billing.new")
            .group("billing")
            .handler(|_, _: Input| async { Ok("new") })
            .unwrap();
        live.replace(replacement);
        let after: ToolDiscoveryResult = client.call("current", &params).await.unwrap();
        assert_eq!(
            after
                .tools
                .iter()
                .map(|t| t.name.as_ref())
                .collect::<Vec<_>>(),
            ["billing.new"]
        );
        assert!(after.tools[0].tags.is_empty());
        let frozen: ToolDiscoveryResult = client.call("frozen", &params).await.unwrap();
        assert_eq!(frozen.tools.len(), 2);
        for cursor in ["billing.list", "billing.create"] {
            let query = ToolDiscoveryParams {
                cursor: Some(cursor.into()),
                ..params.clone()
            };
            assert!(
                client
                    .call::<_, ToolDiscoveryResult>("current", &query)
                    .await
                    .is_err()
            );
        }
        close(client, task).await;
    }

    #[tokio::test]
    async fn matches_go_group_and_tags_and_schema_detail_semantics() {
        let (client, task) = connect(app()).await;
        let group = query(&client, json!({"group":"billing"})).await;
        assert_eq!(names(&group), ["billing.create", "billing.list"]);
        assert!(group["tools"][0].get("inputSchema").is_none());
        assert!(group["tools"][0].get("outputSchema").is_none());
        assert!(group["tools"][1].get("inputSchema").is_some());
        assert_eq!(group["tools"][0]["group"], "billing");
        assert_eq!(group["tools"][0]["tags"], json!(["write", "finance"]));
        assert_eq!(group["tools"][0]["_meta"], json!({"custom":true}));
        assert_eq!(group["tools"][0]["title"], "Title billing.create");
        assert_eq!(
            names(&query(&client, json!({"tags":["read","finance","read"]})).await),
            ["billing.list"]
        );
        assert!(
            names(&query(&client, json!({"group":"billing","tags":["Read"]})).await).is_empty()
        );
        assert!(names(&query(&client, json!({"group":"missing"})).await).is_empty());
        for detail in ["names", "full"] {
            let page = query(&client, json!({"detail":detail})).await;
            assert_eq!(names(&page), ["billing.create", "billing.list", "search"]);
            for tool in page["tools"].as_array().unwrap() {
                assert_eq!(tool.get("inputSchema").is_some(), detail == "full");
                assert_eq!(tool.get("outputSchema").is_some(), detail == "full");
            }
        }
        // Standard discovery defers schemas while execution still validates the full definition.
        let tools = client.peer().list_tools(None).await.unwrap().tools;
        assert!(
            tools
                .iter()
                .any(|t| t.name == "billing.create" && t.input_schema.is_none())
        );
        assert!(
            client
                .call::<_, Value>("billing.create", &json!({}))
                .await
                .is_err()
        );
        let result: Value = client
            .call("billing.create", &json!({"q":"ok"}))
            .await
            .unwrap();
        assert_eq!(result, json!({"q":"ok"}));
        close(client, task).await;
    }

    #[tokio::test]
    async fn filtered_pagination_validates_cursors_and_preserves_snapshots() {
        let mut server = Server::new("pages", "1");
        for n in 0..205 {
            server
                .tool(format!("t{n:03}"))
                .group(if n % 2 == 0 { "even" } else { "odd" })
                .handler(|_, _: Input| async { Ok("ok") })
                .unwrap();
        }
        let snapshot = server.clone();
        assert!(server.remove_tool("t000"));
        let (client, task) = connect(snapshot).await;
        let page = query(&client, json!({"group":"even"})).await;
        assert_eq!(names(&page).len(), 100);
        assert_eq!(names(&page)[0], "t000");
        assert_eq!(page["nextCursor"], "t198");
        let last = query(&client, json!({"group":"even","cursor":"t198"})).await;
        assert_eq!(names(&last), ["t200", "t202", "t204"]);
        assert!(last.get("nextCursor").is_none());
        let empty = query(&client, json!({"group":"even","cursor":"t204"})).await;
        assert!(names(&empty).is_empty());
        for params in [
            json!({"group":"odd","cursor":"t198"}),
            json!({"cursor":"unknown"}),
        ] {
            assert!(client.call::<_, Value>("catalog", &params).await.is_err());
        }
        close(client, task).await;
    }

    #[tokio::test]
    async fn visibility_applies_before_pagination_and_denials_do_not_leak() {
        let server = app().with_visibility_policy(|_, target| {
            !matches!(target, VisibilityTarget::Tool {name} if name == "billing.create")
        });
        let (client, task) = connect(server).await;
        assert_eq!(
            names(&query(&client, json!({"group":"billing","detail":"full"})).await),
            ["billing.list"]
        );
        assert!(
            client
                .call::<_, Value>("catalog", &json!({"cursor":"billing.create"}))
                .await
                .is_err()
        );
        close(client, task).await;
        let server = app().with_visibility_policy(|_, target| {
            if matches!(target, VisibilityTarget::Tool {name} if name != "catalog") {
                panic!("policy failure")
            }
            true
        });
        let (client, task) = connect(server).await;
        assert!(
            client
                .call::<_, Value>("catalog", &json!({}))
                .await
                .is_err()
        );
        close(client, task).await;
    }
}
