//! Directory-backed Agent Skills loading and registration.
#![allow(clippy::unwrap_used, clippy::expect_used)]

use mcp_rs::{
    Server,
    skills::{MAX_SKILLS_PER_DIRECTORY, SkillBundle, SkillError},
};
use std::{
    fs,
    path::{Path, PathBuf},
    sync::atomic::{AtomicUsize, Ordering},
};

const MD: &str = "---\nname: demo-skill\ndescription: Demo skill.\n---\n# Demo\n";
static NEXT: AtomicUsize = AtomicUsize::new(0);

struct TempDir(PathBuf);
impl TempDir {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "mcp-s-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
    fn path(&self) -> &Path {
        &self.0
    }
    fn skill(&self, name: &str, md: &str) -> PathBuf {
        let path = self.0.join(name);
        fs::create_dir_all(&path).unwrap();
        fs::write(path.join("SKILL.md"), md).unwrap();
        path
    }
}
impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
#[cfg(feature = "client")]
fn loads_text_and_binary_files_with_mime_types_as_an_immutable_snapshot() {
    let root = TempDir::new();
    let skill = root.skill("demo-skill", MD);
    fs::create_dir(skill.join("references")).unwrap();
    fs::create_dir(skill.join("assets")).unwrap();
    fs::write(skill.join("references/guide.md"), "Guide 🦀").unwrap();
    fs::write(skill.join("assets/icon.bin"), [0, 1, 2, 255]).unwrap();
    let bundle = SkillBundle::from_directory("demo-skill", &skill).unwrap();
    fs::write(skill.join("references/guide.md"), "changed on disk").unwrap();
    let mut server = Server::new("directory", "1");
    server.register_skill(bundle).unwrap();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        use mcp_rs::{Client, protocol::ServiceExt as _};
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
        let text = serde_json::to_value(
            client
                .read_skill_uri("skill://demo-skill/references/guide.md")
                .await
                .unwrap(),
        )
        .unwrap();
        assert_eq!(text["contents"][0]["text"], "Guide 🦀");
        assert_eq!(text["contents"][0]["mimeType"], "text/markdown");
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
        client.close().await.unwrap();
        task.await.unwrap();
    });
}

#[test]
fn directory_registration_ignores_root_files_and_non_skill_directories() {
    let root = TempDir::new();
    fs::write(root.path().join("README.md"), "ordinary root file").unwrap();
    fs::create_dir(root.path().join("not-a-skill")).unwrap();
    root.skill("first", "---\nname: first\ndescription: First.\n---\n");
    root.skill("second", "---\nname: second\ndescription: Second.\n---\n");
    let mut server = Server::new("directory", "1");
    assert_eq!(
        server.register_skills_from_directory(root.path()).unwrap(),
        2
    );
    let empty = TempDir::new();
    assert_eq!(
        server.register_skills_from_directory(empty.path()).unwrap(),
        0
    );
}

#[test]
fn registration_from_directory_is_atomic_on_bad_bundle_or_resource_collision() {
    let root = TempDir::new();
    root.skill(
        "good-skill",
        "---\nname: good-skill\ndescription: Good.\n---\n",
    );
    root.skill("z-bad-skill", "not valid frontmatter");
    let mut server = Server::new("directory", "1");
    assert!(matches!(
        server.register_skills_from_directory(root.path()),
        Err(SkillError::InvalidFrontmatter(_))
    ));
    assert!(!server.remove_skill("good-skill"));

    fs::remove_dir_all(root.path().join("z-bad-skill")).unwrap();
    root.skill(
        "second-skill",
        "---\nname: second-skill\ndescription: Second.\n---\n",
    );
    server
        .resource("skill://second-skill/SKILL.md")
        .mime_type("text/markdown")
        .handler(|_, _| async { Ok("reserved".to_owned()) })
        .unwrap();
    assert!(server.register_skills_from_directory(root.path()).is_err());
    assert!(!server.remove_skill("good-skill"));
}

#[test]
fn rejects_missing_root_and_invalid_skill_paths() {
    let root = TempDir::new();
    assert!(SkillBundle::from_directory("../demo-skill", root.path()).is_err());
    assert!(matches!(
        SkillBundle::from_directory("demo-skill", root.path().join("absent")),
        Err(SkillError::Io { .. })
    ));
    assert!(SkillBundle::from_directory("demo-skill", root.path()).is_err());
}

#[test]
fn rejects_unsupported_and_restricted_paths_with_specific_errors() {
    let root = TempDir::new();
    let wrong_type = root.path().join("file-root");
    fs::write(&wrong_type, "not a directory").unwrap();
    assert!(matches!(
        SkillBundle::from_directory("demo-skill", &wrong_type),
        Err(SkillError::UnsupportedFile(_))
    ));

    for bad_name in ["bad name.txt", "bad%name.txt", "café.txt"] {
        let skill = root.skill("demo-skill", MD);
        fs::write(skill.join(bad_name), "invalid relative path").unwrap();
        assert!(matches!(
            SkillBundle::from_directory("demo-skill", &skill),
            Err(SkillError::InvalidPath(_))
        ));
        fs::remove_file(skill.join(bad_name)).unwrap();
    }
}

#[test]
fn rejects_invalid_utf8_text_but_accepts_the_same_bytes_as_binary() {
    let root = TempDir::new();
    let skill = root.skill("demo-skill", MD);
    fs::write(skill.join("invalid.txt"), [0xff]).unwrap();
    assert!(matches!(
        SkillBundle::from_directory("demo-skill", &skill),
        Err(SkillError::InvalidUtf8(_))
    ));
    fs::remove_file(skill.join("invalid.txt")).unwrap();
    fs::write(skill.join("opaque.bin"), [0, 255]).unwrap();
    assert!(SkillBundle::from_directory("demo-skill", &skill).is_ok());
}

#[cfg(target_os = "linux")]
#[test]
fn rejects_non_utf8_filesystem_paths() {
    use std::os::unix::ffi::OsStringExt;
    let root = TempDir::new();
    let skill = root.skill("demo-skill", MD);
    let non_utf8_name = std::ffi::OsString::from_vec(vec![b'x', 0xff, b'.', b'b', b'i', b'n']);
    let non_utf8_path = skill.join(&non_utf8_name);
    fs::write(&non_utf8_path, [0, 255]).unwrap();
    assert!(matches!(
        SkillBundle::from_directory("demo-skill", &skill),
        Err(SkillError::InvalidFilesystemPath(_))
    ));
    fs::remove_file(non_utf8_path).unwrap();
}

#[cfg(unix)]
#[test]
fn rejects_symlinked_roots_and_files() {
    use std::os::unix::fs::symlink;
    let root = TempDir::new();
    let real = root.path().join("real");
    fs::create_dir(&real).unwrap();
    fs::write(real.join("SKILL.md"), MD).unwrap();
    let root_link = root.path().join("root-link");
    symlink(&real, &root_link).unwrap();
    assert!(matches!(
        SkillBundle::from_directory("demo-skill", &root_link),
        Err(SkillError::UnsupportedFile(_))
    ));
    let skill = root.skill("demo-skill", MD);
    let target = root.path().join("outside.md");
    fs::write(&target, "outside").unwrap();
    symlink(target, skill.join("guide.md")).unwrap();
    assert!(matches!(
        SkillBundle::from_directory("demo-skill", &skill),
        Err(SkillError::UnsupportedFile(_))
    ));

    let skill_with_dir_link = root.skill(
        "another-skill",
        "---\nname: another-skill\ndescription: Another.\n---\n",
    );
    let linked_dir = skill_with_dir_link.join("linked-dir");
    symlink(&real, &linked_dir).unwrap();
    assert!(matches!(
        SkillBundle::from_directory("another-skill", &skill_with_dir_link),
        Err(SkillError::UnsupportedFile(_))
    ));
}

#[cfg(unix)]
#[test]
fn rejects_unix_socket_special_files() {
    use std::os::unix::net::UnixListener;
    let root = TempDir::new();
    let skill = root.skill("demo-skill", MD);
    let socket = skill.join("service.sock");
    let _listener = UnixListener::bind(&socket).unwrap();
    assert!(matches!(
        SkillBundle::from_directory("demo-skill", &skill),
        Err(SkillError::UnsupportedFile(_))
    ));
}

#[test]
fn enforces_single_file_limit() {
    let root = TempDir::new();
    let skill = root.skill("demo-skill", MD);
    fs::write(skill.join("large.bin"), vec![0; 1024 * 1024 + 1]).unwrap();
    assert!(matches!(
        SkillBundle::from_directory("demo-skill", skill),
        Err(SkillError::FileTooLarge { .. })
    ));
}

#[test]
fn enforces_combined_bundle_size_limit() {
    let root = TempDir::new();
    let skill = root.skill("demo-skill", MD);
    for index in 0..9 {
        fs::write(
            skill.join(format!("large-{index}.bin")),
            vec![index as u8; 1024 * 1024],
        )
        .unwrap();
    }
    assert!(matches!(
        SkillBundle::from_directory("demo-skill", &skill),
        Err(SkillError::BundleTooLarge)
    ));
}

#[test]
fn enforces_skill_collection_count_and_byte_limits_atomically() {
    let count_root = TempDir::new();
    for index in 0..=MAX_SKILLS_PER_DIRECTORY {
        let name = format!("skill-{index:02}");
        count_root.skill(
            &name,
            &format!("---\nname: {name}\ndescription: Count.\n---\n"),
        );
    }
    let mut count_server = Server::new("directory", "1");
    assert!(matches!(
        count_server.register_skills_from_directory(count_root.path()),
        Err(SkillError::DirectoryLimit)
    ));
    assert!(!count_server.remove_skill("skill-00"));

    let bytes_root = TempDir::new();
    for index in 0..5 {
        let name = format!("bundle-{index}");
        let skill = bytes_root.skill(
            &name,
            &format!("---\nname: {name}\ndescription: Bytes.\n---\n"),
        );
        for file_index in 0..7 {
            fs::write(
                skill.join(format!("part-{file_index}.bin")),
                vec![file_index as u8; 1024 * 1024],
            )
            .unwrap();
        }
    }
    let mut bytes_server = Server::new("directory", "1");
    assert!(matches!(
        bytes_server.register_skills_from_directory(bytes_root.path()),
        Err(SkillError::CollectionTooLarge)
    ));
    assert!(!bytes_server.remove_skill("bundle-0"));
}

#[test]
fn enforces_directory_entry_and_depth_limits() {
    let root = TempDir::new();
    let skill = root.skill("demo-skill", MD);
    for index in 0..1024 {
        fs::create_dir(skill.join(format!("empty-{index}"))).unwrap();
    }
    assert!(matches!(
        SkillBundle::from_directory("demo-skill", &skill),
        Err(SkillError::DirectoryLimit)
    ));

    let deep_root = TempDir::new();
    let mut current = deep_root.skill("demo-skill", MD);
    for _ in 0..33 {
        current.push("nested");
        fs::create_dir(&current).unwrap();
    }
    fs::write(current.join("leaf.txt"), "too deep").unwrap();
    assert!(matches!(
        SkillBundle::from_directory("demo-skill", deep_root.path().join("demo-skill")),
        Err(SkillError::DirectoryLimit)
    ));
}
