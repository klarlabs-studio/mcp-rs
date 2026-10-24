//! End-to-end Agent Skills registration and discovery.
#![cfg(feature = "client")]
#![allow(clippy::unwrap_used, clippy::expect_used)]

use mcp_rs::{
    Client, Middleware, Server,
    model::RequestMetaObject,
    protocol::ServiceExt as _,
    skills::{SKILLS_EXTENSION_ID, SkillBundle},
};
use serde_json::Value;
use std::sync::{Arc, Mutex};

const DEMO_MD: &str = "---\nname: demo-skill\ndescription: A demo skill.\n---\n# Demo skill\n";
const PRIVATE_MD: &str =
    "---\nname: private-skill\ndescription: A private skill.\n---\n# Private skill\n";
const REPLACEMENT_MD: &str =
    "---\nname: replacement-skill\ndescription: Replaced live skill.\n---\n# Replacement\n";
const CHILD_MD: &str = "---\nname: child-skill\ndescription: A nested skill.\n---\n# Child skill\n";

fn demo_bundle() -> SkillBundle {
    SkillBundle::new("demo-skill", DEMO_MD)
        .unwrap()
        .with_text_file("references/guide.md", "text/markdown", "Read me 🦀")
        .unwrap()
        .with_binary_file(
            "assets/icon.bin",
            "application/octet-stream",
            vec![0, 1, 2, 255],
        )
        .unwrap()
}

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
    let client = Client::connect(client_io).await.unwrap();
    (client, task)
}

async fn stop(client: Client, task: tokio::task::JoinHandle<()>) {
    client.close().await.unwrap();
    task.await.unwrap();
}

fn text_at(value: &Value, index: usize) -> &str {
    value["contents"][index]["text"].as_str().unwrap()
}

#[tokio::test]
async fn skills_are_discovered_read_as_text_and_binary_and_advertise_the_extension() {
    let mut server = Server::new("skills", "1");
    server.register_skill(demo_bundle()).unwrap();
    let middleware_uris = Arc::new(Mutex::new(Vec::new()));
    let observed = middleware_uris.clone();
    let middleware: Middleware = Arc::new(move |ctx, args, next| {
        let observed = observed.clone();
        Box::pin(async move {
            observed
                .lock()
                .unwrap()
                .push(args["uri"].as_str().unwrap().to_owned());
            next(ctx, args).await
        })
    });
    let server = server.with_middleware(middleware);
    let (client, task) = serve(server).await;

    let discovery = client
        .peer()
        .discover(RequestMetaObject::new())
        .await
        .unwrap();
    let discovery = serde_json::to_value(discovery).unwrap();
    assert!(discovery["capabilities"]["extensions"][SKILLS_EXTENSION_ID].is_object());

    let skills = client.list_skills().await.unwrap();
    assert!(
        middleware_uris
            .lock()
            .unwrap()
            .contains(&"skill://index.json".to_owned())
    );
    assert_eq!(skills.len(), 1);
    assert_eq!(skills[0].name, "demo-skill");
    assert_eq!(
        skills[0].entry_type,
        mcp_rs::skills::SkillIndexType::SkillMd
    );
    assert_eq!(skills[0].description, "A demo skill.");
    assert_eq!(skills[0].url, "skill://demo-skill/SKILL.md");

    let entry = serde_json::to_value(client.read_skill_uri(&skills[0].url).await.unwrap()).unwrap();
    assert_eq!(text_at(&entry, 0), DEMO_MD);
    assert_eq!(entry["contents"][0]["mimeType"], "text/markdown");

    let guide = serde_json::to_value(
        client
            .read_skill_uri("skill://demo-skill/references/guide.md")
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(text_at(&guide, 0), "Read me 🦀");
    assert_eq!(guide["contents"][0]["mimeType"], "text/markdown");

    let binary = serde_json::to_value(
        client
            .read_skill_uri("skill://demo-skill/assets/icon.bin")
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(binary["contents"][0]["blob"], "AAEC/w==");
    assert_eq!(
        binary["contents"][0]["mimeType"],
        "application/octet-stream"
    );

    stop(client, task).await;
}

#[tokio::test]
async fn visibility_hides_skills_from_index_and_reads() {
    let mut server = Server::new("skills", "1");
    server.register_skill(demo_bundle()).unwrap();
    server
        .register_skill(SkillBundle::new("private-skill", PRIVATE_MD).unwrap())
        .unwrap();
    let server = server.with_visibility_policy(|_, target| match target {
        mcp_rs::VisibilityTarget::Resource { uri, .. } => !uri.contains("private-skill"),
        _ => true,
    });
    let (client, task) = serve(server).await;

    let skills = client.list_skills().await.unwrap();
    assert_eq!(skills.len(), 1);
    assert_eq!(skills[0].name, "demo-skill");
    assert!(
        client
            .read_skill_uri("skill://private-skill/SKILL.md")
            .await
            .is_err()
    );

    stop(client, task).await;

    let mut server = Server::new("skills", "1");
    server.register_skill(demo_bundle()).unwrap();
    let server = server.with_visibility_policy(|_, target| match target {
        mcp_rs::VisibilityTarget::Resource { uri, .. } => uri != "skill://index.json",
        _ => true,
    });
    let (client, task) = serve(server).await;
    assert!(client.list_skills().await.is_err());
    stop(client, task).await;
}

#[test]
fn registration_is_atomic_and_server_clones_have_independent_bundle_removal() {
    let mut reserved_index = Server::new("skills", "1");
    reserved_index
        .resource("skill://index.json")
        .mime_type("application/json")
        .handler(|_, _| async { Ok("user index".to_owned()) })
        .unwrap();
    assert!(reserved_index.register_skill(demo_bundle()).is_err());
    assert!(!reserved_index.remove_skill("demo-skill"));
    assert!(reserved_index.remove_resource("skill://index.json"));

    let mut original = Server::new("skills", "1");
    original.register_skill(demo_bundle()).unwrap();
    let mut clone = original.clone();

    original
        .resource("skill://other-skill/assets/icon.bin")
        .mime_type("application/octet-stream")
        .handler(|_, _| async { Ok("raw asset".to_owned()) })
        .unwrap();
    let collision = SkillBundle::new(
        "other-skill",
        "---\nname: other-skill\ndescription: Other.\n---\n",
    )
    .unwrap()
    .with_text_file("references/guide.md", "text/markdown", "unique first file")
    .unwrap()
    .with_binary_file("assets/icon.bin", "application/octet-stream", vec![9])
    .unwrap();
    assert!(original.register_skill(collision).is_err());
    assert!(!original.remove_resource("skill://other-skill/SKILL.md"));
    assert!(!original.remove_resource("skill://other-skill/references/guide.md"));
    assert!(clone.remove_resource("skill://demo-skill/references/guide.md"));
    assert!(!clone.remove_skill("demo-skill"));
    assert!(original.remove_skill("demo-skill"));
    assert!(!original.remove_resource("skill://index.json"));
    assert!(!clone.remove_resource("skill://index.json"));

    let mut all_skills = Server::new("skills", "1");
    all_skills.register_skill(demo_bundle()).unwrap();
    all_skills
        .register_skill(SkillBundle::new("private-skill", PRIVATE_MD).unwrap())
        .unwrap();
    assert!(all_skills.remove_resource("skill://index.json"));
    assert!(!all_skills.remove_skill("demo-skill"));
    assert!(!all_skills.remove_skill("private-skill"));
}

#[tokio::test]
async fn removing_one_bundle_preserves_nested_bundles_and_unowned_resources() {
    let mut server = Server::new("skills", "1");
    server
        .resource("skill://demo-skill/notes.txt")
        .mime_type("text/plain")
        .handler(|_, _| async { Ok("unowned note".to_owned()) })
        .unwrap();
    server.register_skill(demo_bundle()).unwrap();
    server
        .register_skill(SkillBundle::new("demo-skill/child-skill", CHILD_MD).unwrap())
        .unwrap();
    assert!(server.remove_skill("demo-skill"));
    let (client, task) = serve(server).await;

    let skills = client.list_skills().await.unwrap();
    assert_eq!(skills.len(), 1);
    assert_eq!(skills[0].name, "child-skill");
    assert_eq!(
        text_at(
            &serde_json::to_value(
                client
                    .read_skill_uri("skill://demo-skill/child-skill/SKILL.md")
                    .await
                    .unwrap()
            )
            .unwrap(),
            0
        ),
        CHILD_MD
    );
    assert_eq!(
        text_at(
            &serde_json::to_value(
                client
                    .read_skill_uri("skill://demo-skill/notes.txt")
                    .await
                    .unwrap()
            )
            .unwrap(),
            0
        ),
        "unowned note"
    );
    stop(client, task).await;
}

#[tokio::test]
async fn list_skills_rejects_malformed_and_wrong_schema_indexes_and_missing_index() {
    for payload in [
        "not JSON".to_owned(),
        serde_json::json!({"$schema":"wrong","skills":[]}).to_string(),
    ] {
        let mut server = Server::new("skills", "1");
        let payload = payload.clone();
        server
            .resource("skill://index.json")
            .mime_type("application/json")
            .handler(move |_, _| {
                let payload = payload.clone();
                async move { Ok(payload) }
            })
            .unwrap();
        let (client, task) = serve(server).await;
        assert!(client.list_skills().await.is_err());
        stop(client, task).await;
    }

    let (client, task) = serve(Server::new("skills", "1")).await;
    assert!(client.list_skills().await.is_err());
    stop(client, task).await;

    let malformed_shapes = [
        vec![mcp_rs::model::ResourceContents::blob(
            "AA==",
            "skill://index.json",
        )],
        vec![
            mcp_rs::model::ResourceContents::text("{}", "skill://index.json"),
            mcp_rs::model::ResourceContents::text("{}", "skill://index.json"),
        ],
        vec![mcp_rs::model::ResourceContents::text(
            r#"{"$schema":"https://schemas.agentskills.io/discovery/0.2.0/schema.json","skills":[]}"#,
            "skill://elsewhere.json",
        )],
    ];
    for contents in malformed_shapes {
        let mut server = Server::new("skills", "1");
        server
            .resource("skill://index.json")
            .mime_type("application/json")
            .content_handler(move |_, _, _| {
                let contents = contents.clone();
                async move { Ok(contents) }
            })
            .unwrap();
        let (client, task) = serve(server).await;
        assert!(client.list_skills().await.is_err());
        stop(client, task).await;
    }
}

#[tokio::test]
async fn live_server_replacement_updates_skill_discovery_for_an_existing_client() {
    let mut initial = Server::new("skills", "1");
    initial.register_skill(demo_bundle()).unwrap();
    let live = initial.into_live();
    let (client_io, server_io) = tokio::io::duplex(65_536);
    let live_server = live.clone();
    let task = tokio::spawn(async move {
        live_server
            .serve(server_io)
            .await
            .unwrap()
            .waiting()
            .await
            .unwrap();
    });
    let client = Client::connect(client_io).await.unwrap();
    assert_eq!(client.list_skills().await.unwrap()[0].name, "demo-skill");

    let mut replacement = Server::new("skills", "1");
    replacement
        .register_skill(SkillBundle::new("replacement-skill", REPLACEMENT_MD).unwrap())
        .unwrap();
    live.replace(replacement);

    let skills = client.list_skills().await.unwrap();
    assert_eq!(skills.len(), 1);
    assert_eq!(skills[0].name, "replacement-skill");
    assert!(
        client
            .read_skill_uri("skill://demo-skill/SKILL.md")
            .await
            .is_err()
    );
    assert_eq!(
        text_at(
            &serde_json::to_value(
                client
                    .read_skill_uri("skill://replacement-skill/SKILL.md")
                    .await
                    .unwrap()
            )
            .unwrap(),
            0
        ),
        REPLACEMENT_MD
    );

    stop(client, task).await;
}
