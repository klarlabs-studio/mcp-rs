//! Skill templates share resource routing, admission, and ownership.
#![allow(clippy::unwrap_used)]

use mcp_rs::{
    Server,
    skills::{SkillBundle, SkillError},
};
const URI: &str = "skill://docs/{product}/SKILL.md";

fn register(server: &mut Server) {
    server
        .skill_template(URI, "Product documentation")
        .unwrap()
        .handler(|_, _, params| async move { Ok(format!("# {}\n", params["product"])) })
        .unwrap();
}

#[test]
fn rejects_unsupported_templates_and_blank_descriptions_without_mutation() {
    let mut server = Server::new("skills", "1");
    for uri in [
        "",
        "docs/{product}/SKILL.md",
        "https://docs/{product}/SKILL.md",
        "skill://docs/static/SKILL.md",
        "skill://docs/{product}/other.md",
        "skill://docs/{}/SKILL.md",
        "skill://docs/{product}/{product}/SKILL.md",
        "skill://docs/{+product}/SKILL.md",
        "skill://docs/{1product}/SKILL.md",
        "skill://docs/prefix-{product}/SKILL.md",
        "skill://../{product}/SKILL.md",
        "skill://docs//{product}/SKILL.md",
        "skill://docs/{product}/SKILL.md?q=x",
        "skill://docs/{product}/SKILL.md#x",
        "skill://docs/{product}/SKILL.md/",
        "skill://docs/{product/SKILL.md",
        "skill://docs/product}/SKILL.md",
        "skill://docs/%20/{product}/SKILL.md",
    ] {
        assert!(
            matches!(
                server.skill_template(uri, "Description"),
                Err(SkillError::InvalidTemplate(_))
            ),
            "{uri}"
        );
    }
    assert!(matches!(
        server.skill_template(URI, " \n "),
        Err(SkillError::InvalidTemplateDescription)
    ));
    assert!(!server.remove_resource("skill://index.json"));
    assert!(!server.remove_resource(URI));
    // Acquiring then dropping a valid builder must not advertise a phantom entry.
    drop(server.skill_template(URI, "Description").unwrap());
    assert!(!server.remove_resource("skill://index.json"));
    register(&mut server);
}

#[test]
fn conflict_registration_rolls_back_and_removal_is_owned_and_copy_on_write() {
    let mut server = Server::new("skills", "1");
    server
        .resource(URI)
        .handler(|_, _| async { Ok("owned".to_owned()) })
        .unwrap();
    assert!(
        server
            .skill_template(URI, "Description")
            .unwrap()
            .handler(|_, _, _| async { Ok("new".to_owned()) })
            .is_err()
    );
    assert!(!server.remove_resource("skill://index.json"));
    assert!(server.remove_resource(URI));
    server
        .resource("skill://index.json")
        .handler(|_, _| async { Ok("owned".to_owned()) })
        .unwrap();
    assert!(
        server
            .skill_template(URI, "Description")
            .unwrap()
            .handler(|_, _, _| async { Ok("new".to_owned()) })
            .is_err()
    );
    assert!(!server.remove_resource(URI));
    server.remove_resource("skill://index.json");
    register(&mut server);
    assert!(matches!(
        server
            .skill_template(URI, "Replacement")
            .unwrap()
            .handler(|_, _, _| async { Ok("new".to_owned()) }),
        Err(SkillError::DuplicateSkill(_))
    ));
    let mut snapshot = server.clone();
    assert!(server.remove_skill_template(URI));
    assert!(!server.remove_resource("skill://index.json"));
    assert!(snapshot.remove_resource(URI));
    assert!(!snapshot.remove_skill_template(URI));
    register(&mut server);
    server
        .register_skill(
            SkillBundle::new("docs/demo", "---\nname: demo\ndescription: Demo\n---\n").unwrap(),
        )
        .unwrap();
    assert!(!server.remove_skill_template("docs/demo"));
    assert!(server.remove_skill_template(URI));
    assert!(server.remove_skill("docs/demo"));
    register(&mut server);
    assert!(server.remove_resource("skill://index.json"));
    assert!(!server.remove_skill_template(URI));
}

#[cfg(feature = "client")]
mod wire {
    use super::*;
    use mcp_rs::{
        Client, Middleware, VisibilityTarget,
        model::{ErrorData, RequestMetaObject, ResourceContents},
        protocol::ServiceExt as _,
        skills::{SKILLS_EXTENSION_ID, SkillIndexType},
    };
    use serde_json::{Value, json};
    use std::sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    };
    async fn serve(server: Server) -> (Client, tokio::task::JoinHandle<()>) {
        let (client_io, server_io) = tokio::io::duplex(65_536);
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
    async fn stop(client: Client, task: tokio::task::JoinHandle<()>) {
        client.close().await.unwrap();
        task.await.unwrap();
    }
    async fn read(client: &Client, uri: &str) -> Value {
        serde_json::to_value(client.read_skill_uri(uri).await.unwrap()).unwrap()
    }

    #[tokio::test]
    async fn discovers_templates_and_routes_raw_parameters_with_exact_priority() {
        let mut server = Server::new("skills", "1");
        server
            .skill_template(format!("  {URI}  "), "  Product documentation  ")
            .unwrap()
            .handler(|_, _, params| async move { Ok(format!("# {}\n", params["product"])) })
            .unwrap();
        server
            .resource("skill://docs/fixed/SKILL.md")
            .handler(|_, _| async { Ok("Exact".to_owned()) })
            .unwrap();
        let (client, task) = serve(server).await;
        let discovery = serde_json::to_value(
            client
                .peer()
                .discover(RequestMetaObject::new())
                .await
                .unwrap(),
        )
        .unwrap();
        assert!(discovery["capabilities"]["extensions"][SKILLS_EXTENSION_ID].is_object());
        let entries = client.list_skills().await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].entry_type, SkillIndexType::Template);
        assert_eq!(
            serde_json::to_value(&entries[0]).unwrap(),
            json!({"type":"mcp-resource-template","description":"Product documentation","url":URI})
        );
        let templates =
            serde_json::to_value(client.peer().list_resource_templates(None).await.unwrap())
                .unwrap();
        assert_eq!(templates["resourceTemplates"][0]["uriTemplate"], URI);
        assert_eq!(
            templates["resourceTemplates"][0]["mimeType"],
            "text/markdown"
        );
        let resources =
            serde_json::to_value(client.peer().list_resources(None).await.unwrap()).unwrap();
        assert!(
            !resources["resources"]
                .as_array()
                .unwrap()
                .iter()
                .any(|resource| resource["uri"] == URI)
        );
        for (parameter, expected) in [
            ("widget", "# widget\n"),
            ("a%2Fb", "# a%2Fb\n"),
            ("fixed", "Exact"),
        ] {
            let uri = format!("skill://docs/{parameter}/SKILL.md");
            let contents = read(&client, &uri).await;
            assert_eq!(contents["contents"][0]["uri"], uri);
            assert_eq!(contents["contents"][0]["text"], expected);
            if parameter != "fixed" {
                assert_eq!(contents["contents"][0]["mimeType"], "text/markdown");
            }
        }
        assert!(
            client
                .read_skill_uri("skill://docs/a/b/SKILL.md")
                .await
                .is_err()
        );
        assert!(
            client
                .read_skill_uri("skill://docs//SKILL.md")
                .await
                .is_err()
        );
        stop(client, task).await;
    }

    #[tokio::test]
    async fn template_index_visibility_uses_its_own_registration_despite_overlap() {
        const OTHER: &str = "skill://docs/{a}/SKILL.md";
        let calls = Arc::new(AtomicUsize::new(0));
        let mut server = Server::new("skills", "1");
        server
            .skill_template(OTHER, "Hidden first matcher")
            .unwrap()
            .handler(|_, _, _| async { Ok("hidden".to_owned()) })
            .unwrap();
        let counted = calls.clone();
        server
            .skill_template(URI, "Visible catalog entry")
            .unwrap()
            .handler(move |_, _, _| {
                counted.fetch_add(1, Ordering::SeqCst);
                async { Ok("visible".to_owned()) }
            })
            .unwrap();
        let server = server.with_visibility_policy(|_, target| match target {
            VisibilityTarget::Resource { registered_uri, .. } => registered_uri != OTHER,
            _ => true,
        });
        let (client, task) = serve(server).await;
        let entries = client.list_skills().await.unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].url, URI);
        let listed = client.peer().list_resource_templates(None).await.unwrap();
        assert_eq!(listed.resource_templates.len(), 1);
        // Existing deterministic routing selects the first matcher, then applies
        // admission. A hidden match must never fall through to the visible one.
        assert!(
            client
                .read_skill_uri("skill://docs/widget/SKILL.md")
                .await
                .is_err()
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        stop(client, task).await;
    }

    #[tokio::test]
    async fn visibility_and_middleware_apply_to_concrete_template_reads() {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let captured = observed.clone();
        let middleware: Middleware = Arc::new(move |ctx, args, next| {
            let captured = captured.clone();
            Box::pin(async move {
                captured
                    .lock()
                    .unwrap()
                    .push(args["uri"].as_str().unwrap().to_owned());
                next(ctx, args).await
            })
        });
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = calls.clone();
        let mut server = Server::new("skills", "1").with_middleware(middleware);
        server
            .skill_template(URI, "Product documentation")
            .unwrap()
            .handler(move |_, _, params| {
                counted.fetch_add(1, Ordering::SeqCst);
                async move { Ok(params["product"].clone()) }
            })
            .unwrap();
        let server = server.with_visibility_policy(|_, target| match target {
            VisibilityTarget::Resource { uri, .. } => !uri.contains("secret"),
            _ => true,
        });
        let (client, task) = serve(server).await;
        assert_eq!(client.list_skills().await.unwrap().len(), 1);
        assert!(
            client
                .read_skill_uri("skill://docs/secret/SKILL.md")
                .await
                .is_err()
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(
            read(&client, "skill://docs/public/SKILL.md").await["contents"][0]["text"],
            "public"
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
        assert!(
            observed
                .lock()
                .unwrap()
                .contains(&"skill://docs/public/SKILL.md".to_owned())
        );
        stop(client, task).await;
    }

    #[tokio::test]
    async fn native_blocks_errors_and_live_replacement_keep_resource_semantics() {
        let mut server = Server::new("skills", "1");
        server
            .skill_template(URI, "Dynamic content")
            .unwrap()
            .content_handler(|_, uri, params| async move {
                if params["product"] == "error" {
                    return Err(ErrorData::invalid_params("Unknown product", None));
                }
                Ok(vec![
                    ResourceContents::text("Guide", &uri).with_mime_type("text/plain"),
                    ResourceContents::blob("AP8=", uri).with_mime_type("application/octet-stream"),
                ])
            })
            .unwrap();
        let live = server.into_live();
        let (client_io, server_io) = tokio::io::duplex(65_536);
        let copy = live.clone();
        let task = tokio::spawn(async move {
            copy.serve(server_io)
                .await
                .unwrap()
                .waiting()
                .await
                .unwrap();
        });
        let client = Client::connect(client_io).await.unwrap();
        let contents = read(&client, "skill://docs/widget/SKILL.md").await;
        assert_eq!(contents["contents"].as_array().unwrap().len(), 2);
        assert_eq!(contents["contents"][0]["mimeType"], "text/plain");
        assert_eq!(contents["contents"][1]["blob"], "AP8=");
        assert_eq!(
            contents["contents"][1]["mimeType"],
            "application/octet-stream"
        );
        assert!(
            client
                .read_skill_uri("skill://docs/error/SKILL.md")
                .await
                .is_err()
        );
        let mut replacement = Server::new("skills", "1");
        register(&mut replacement);
        live.replace(replacement);
        assert_eq!(
            read(&client, "skill://docs/widget/SKILL.md").await["contents"][0]["text"],
            "# widget\n"
        );
        live.replace(Server::new("skills", "1"));
        assert!(client.list_skills().await.is_err());
        assert!(
            client
                .read_skill_uri("skill://docs/widget/SKILL.md")
                .await
                .is_err()
        );
        stop(client, task).await;
    }
}
