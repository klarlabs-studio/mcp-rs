//! Packed skill snapshot limits, file handling, registration, and wire behavior.
#![allow(clippy::unwrap_used)]

use mcp_rs::{
    Server,
    skills::{MAX_SKILL_ARCHIVE_BYTES, SkillArchive, SkillArchiveFormat, SkillBundle, SkillError},
};
use std::{
    fs,
    path::PathBuf,
    sync::atomic::{AtomicUsize, Ordering},
};

struct Temp(PathBuf);
impl Temp {
    fn new() -> Self {
        static NEXT: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "mcp-a-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn file(&self, name: &str, bytes: &[u8]) -> PathBuf {
        let path = self.0.join(name);
        fs::write(&path, bytes).unwrap();
        path
    }
}
impl Drop for Temp {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}
fn archive(path: &str) -> SkillArchive {
    SkillArchive::new(path, SkillArchiveFormat::Zip, vec![0, 255, 1]).unwrap()
}
fn bundle() -> SkillBundle {
    SkillBundle::new("demo", "---\nname: demo\ndescription: Demo.\n---\n").unwrap()
}

#[test]
fn validates_namespace_description_and_packed_size_without_unpacking() {
    assert!(matches!(
        SkillArchive::new("../demo", SkillArchiveFormat::Zip, vec![]),
        Err(SkillError::InvalidPath(_))
    ));
    for description in ["", "  ", "bad\nheader", "bad\0"] {
        assert!(matches!(
            archive("demo").with_description(description),
            Err(SkillError::InvalidArchiveDescription)
        ));
    }
    assert!(archive("/team/demo/").with_description("Useful 🦀").is_ok());
    assert!(
        SkillArchive::new(
            "demo",
            SkillArchiveFormat::TarGzip,
            vec![0; MAX_SKILL_ARCHIVE_BYTES]
        )
        .is_ok()
    );
    assert!(matches!(
        SkillArchive::new(
            "demo",
            SkillArchiveFormat::Zip,
            vec![0; MAX_SKILL_ARCHIVE_BYTES + 1]
        ),
        Err(SkillError::ArchiveTooLarge)
    ));
    // Opaque bytes, including malformed archive headers, are intentionally accepted.
    assert!(SkillArchive::new("demo", SkillArchiveFormat::Zip, vec![]).is_ok());
}

#[test]
fn file_loader_rejects_suffix_missing_directory_and_oversize() {
    let temp = Temp::new();
    let invalid = temp.file("demo.tar", &[1]);
    assert!(matches!(
        SkillArchive::from_file("demo", invalid),
        Err(SkillError::UnsupportedArchiveFormat(_))
    ));
    assert!(matches!(
        SkillArchive::from_file("demo", temp.0.join("missing.zip")),
        Err(SkillError::Io { .. })
    ));
    let directory = temp.0.join("directory.zip");
    fs::create_dir(&directory).unwrap();
    assert!(matches!(
        SkillArchive::from_file("demo", directory),
        Err(SkillError::UnsupportedFile(_))
    ));
    let large = temp.file("large.zip", &[]);
    fs::File::options()
        .write(true)
        .open(&large)
        .unwrap()
        .set_len(MAX_SKILL_ARCHIVE_BYTES as u64 + 1)
        .unwrap();
    assert!(matches!(
        SkillArchive::from_file("demo", large),
        Err(SkillError::ArchiveTooLarge)
    ));
}

#[cfg(unix)]
#[test]
fn file_loader_rejects_symlinks_and_special_files() {
    let temp = Temp::new();
    let file = temp.file("demo.zip", &[1]);
    let link = temp.0.join("link.zip");
    std::os::unix::fs::symlink(file, &link).unwrap();
    assert!(matches!(
        SkillArchive::from_file("demo", link),
        Err(SkillError::UnsupportedFile(_))
    ));
    let special = temp.0.join("socket.zip");
    let _socket = std::os::unix::net::UnixListener::bind(&special).unwrap();
    assert!(matches!(
        SkillArchive::from_file("demo", special),
        Err(SkillError::UnsupportedFile(_))
    ));
}

#[test]
fn conflicts_roll_back_index_resources_and_cross_kind_namespaces() {
    let mut server = Server::new("skills", "1");
    server
        .resource("skill://demo.zip")
        .handler(|_, _| async { Ok("owned".to_owned()) })
        .unwrap();
    assert!(server.register_skill_archive(archive("demo")).is_err());
    assert!(!server.remove_resource("skill://index.json"));
    assert!(!server.remove_skill("demo"));
    assert!(server.remove_resource("skill://demo.zip"));

    server
        .resource("skill://index.json")
        .handler(|_, _| async { Ok("owned".to_owned()) })
        .unwrap();
    assert!(server.register_skill_archive(archive("demo")).is_err());
    assert!(!server.remove_resource("skill://demo.zip"));
    server.remove_resource("skill://index.json");

    server.register_skill_archive(archive("demo")).unwrap();
    assert!(matches!(
        server.register_skill(bundle()),
        Err(SkillError::DuplicateSkill(_))
    ));
    assert!(matches!(
        server.register_skill_archive(
            SkillArchive::new("demo", SkillArchiveFormat::TarGzip, vec![1]).unwrap()
        ),
        Err(SkillError::DuplicateSkill(_))
    ));
    let mut snapshot = server.clone();
    assert!(server.remove_resource("skill://demo.zip"));
    assert!(!server.remove_resource("skill://index.json"));
    assert!(snapshot.remove_skill("demo"));
    assert!(!snapshot.remove_resource("skill://demo.zip"));
    server.register_skill(bundle()).unwrap();
    assert!(matches!(
        server.register_skill_archive(archive("demo")),
        Err(SkillError::DuplicateSkill(_))
    ));
    assert!(server.remove_resource("skill://index.json"));
    assert!(!server.remove_skill("demo"));
}

#[cfg(feature = "client")]
mod wire {
    use super::*;
    use mcp_rs::protocol::ServiceExt as _;
    use mcp_rs::{
        Client, Middleware,
        skills::{SKILLS_EXTENSION_ID, SkillIndexType},
    };
    use std::sync::{Arc, Mutex};

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

    #[tokio::test]
    async fn suffixes_mime_binary_snapshot_and_explicit_metadata_reach_client() {
        let temp = Temp::new();
        // Sibling metadata must never be loaded implicitly.
        temp.file(
            "SKILL.md",
            b"---\nname: wrong\ndescription: Implicit metadata.\n---\n",
        );
        let mut server = Server::new("skills", "1");
        for (name, filename) in [
            ("zip", "data.ZIP"),
            ("gzip", "data.TAR.GZ"),
            ("alias", "data.TgZ"),
        ] {
            let file = temp.file(filename, &[0, 255, 1]);
            let mut snapshot = SkillArchive::from_file(format!("team/{name}"), &file).unwrap();
            if name == "zip" {
                snapshot = snapshot.with_description("Explicit ZIP.").unwrap();
            }
            server.register_skill_archive(snapshot).unwrap();
            fs::write(file, b"changed").unwrap();
        }
        let (client, task) = serve(server).await;
        assert!(
            serde_json::to_value(
                client
                    .peer()
                    .discover(mcp_rs::model::RequestMetaObject::new())
                    .await
                    .unwrap()
            )
            .unwrap()
            .to_string()
            .contains(SKILLS_EXTENSION_ID)
        );
        let skills = client.list_skills().await.unwrap();
        assert_eq!(skills.len(), 3);
        for entry in skills {
            assert_eq!(entry.entry_type, SkillIndexType::Archive);
            assert_eq!(
                entry.description,
                if entry.name == "zip" {
                    "Explicit ZIP.".to_owned()
                } else {
                    format!("Archived skill {}", entry.name)
                }
            );
            let suffix = if entry.name == "zip" {
                ".zip"
            } else {
                ".tar.gz"
            };
            assert_eq!(entry.url, format!("skill://team/{}{suffix}", entry.name));
            let result =
                serde_json::to_value(client.read_skill_uri(&entry.url).await.unwrap()).unwrap();
            let content = &result["contents"][0];
            assert_eq!(content["uri"], entry.url);
            assert_eq!(content["blob"], "AP8B");
            assert_eq!(
                content["mimeType"],
                if entry.name == "zip" {
                    "application/zip"
                } else {
                    "application/gzip"
                }
            );
            assert!(content.get("text").is_none());
        }
        stop(client, task).await;
    }

    #[tokio::test]
    async fn archives_use_ordinary_visibility_middleware_and_live_replacement() {
        let observed = Arc::new(Mutex::new(Vec::new()));
        let captured = observed.clone();
        let middleware: Middleware = Arc::new(move |ctx, args, next| {
            let captured = captured.clone();
            Box::pin(async move {
                if let Some(uri) = args.get("uri").and_then(|value| value.as_str()) {
                    captured.lock().unwrap().push(uri.to_owned());
                }
                next(ctx, args).await
            })
        });
        let mut server = Server::new("skills", "1").with_middleware(middleware);
        server.register_skill_archive(archive("visible")).unwrap();
        server.register_skill_archive(archive("hidden")).unwrap();
        let server = server.with_visibility_policy(|_, target| match target {
            mcp_rs::VisibilityTarget::Resource { uri, .. } => !uri.contains("hidden"),
            _ => true,
        });
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
        let skills = client.list_skills().await.unwrap();
        assert_eq!(skills.len(), 1);
        assert_eq!(skills[0].name, "visible");
        client.read_skill_uri(&skills[0].url).await.unwrap();
        assert!(client.read_skill_uri("skill://hidden.zip").await.is_err());
        assert!(
            observed
                .lock()
                .unwrap()
                .contains(&"skill://visible.zip".to_owned())
        );
        assert!(
            observed
                .lock()
                .unwrap()
                .contains(&"skill://index.json".to_owned())
        );
        let mut replacement = Server::new("skills", "1");
        replacement
            .register_skill_archive(archive("replacement"))
            .unwrap();
        live.replace(replacement);
        assert_eq!(client.list_skills().await.unwrap()[0].name, "replacement");
        assert!(client.read_skill_uri("skill://visible.zip").await.is_err());
        stop(client, task).await;
    }
}
