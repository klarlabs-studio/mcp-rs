//! Integration tests for bounded Agent Skills bundles and discovery index models.
#![allow(clippy::unwrap_used)]

use mcp_rs::skills::{
    MAX_SKILL_FILE_BYTES, MAX_SKILL_FILES, SkillBundle, SkillError, SkillIndex, SkillIndexEntry,
    SkillIndexType,
};

const MD: &str =
    "---\nname: demo-skill\ndescription: |\n  A useful skill.\n  Second line.\n---\n# Demo\n";

#[test]
fn parses_metadata_and_keeps_entrypoint_text() {
    let bundle = SkillBundle::new("/team/demo-skill/", MD).unwrap();
    assert!(
        bundle
            .with_text_file("refs/ok.txt", "text/plain", "ok")
            .is_ok()
    );
}

#[test]
fn adds_utf8_and_binary_files_and_rejects_duplicates() {
    let bundle = SkillBundle::new("demo-skill", MD)
        .unwrap()
        .with_text_file("refs/guide.md", "text/markdown", "café")
        .unwrap()
        .with_binary_file("assets/icon.bin", "application/octet-stream", vec![0, 255])
        .unwrap();
    assert!(
        bundle
            .clone()
            .with_text_file("next.txt", "text/plain", "café")
            .is_ok()
    );
    assert!(matches!(
        bundle.with_text_file("SKILL.md", "text/plain", "duplicate"),
        Err(SkillError::DuplicateFile(_))
    ));
}

#[test]
fn validates_paths_frontmatter_and_mime() {
    for path in ["", "a..b", "team//demo-skill", "team/demo.skill"] {
        assert!(matches!(
            SkillBundle::new(path, MD),
            Err(SkillError::InvalidPath(_))
        ));
    }
    assert!(SkillBundle::new("team/other", MD).is_err());
    assert!(SkillBundle::new("demo-skill", "# no frontmatter").is_err());
    assert!(SkillBundle::new("demo-skill", "---name: demo-skill\ndescription: x\n---\n").is_err());
    assert!(
        SkillBundle::new(
            "demo-skill",
            "---\nname: demo-skill\ndescription: x\n---extra\n"
        )
        .is_err()
    );
    assert!(
        SkillBundle::new(
            "demo-skill",
            "---\nname: demo-skill\nname: demo-skill\ndescription: x\n---\n"
        )
        .is_err()
    );
    let bundle = SkillBundle::new("demo-skill", MD).unwrap();
    assert!(matches!(
        bundle.with_text_file("../secret", "text/plain", "x"),
        Err(SkillError::InvalidPath(_))
    ));
    let bundle = SkillBundle::new("demo-skill", MD).unwrap();
    assert!(matches!(
        bundle.with_text_file("x", "text/plain\nX: y", "x"),
        Err(SkillError::InvalidMimeType(_))
    ));
}

#[test]
fn enforces_file_and_count_limits() {
    let bundle = SkillBundle::new("demo-skill", MD).unwrap();
    assert!(matches!(
        bundle.with_binary_file(
            "large",
            "application/octet-stream",
            vec![0; MAX_SKILL_FILE_BYTES + 1]
        ),
        Err(SkillError::FileTooLarge { .. })
    ));
    let mut bundle = SkillBundle::new("demo-skill", MD).unwrap();
    for i in 0..MAX_SKILL_FILES - 1 {
        bundle = bundle
            .with_text_file(format!("f{i}"), "text/plain", "")
            .unwrap();
    }
    assert!(matches!(
        bundle.with_text_file("overflow", "text/plain", ""),
        Err(SkillError::TooManyFiles)
    ));

    let mut bundle = SkillBundle::new("demo-skill", MD).unwrap();
    for i in 0..7 {
        bundle = bundle
            .with_binary_file(
                format!("large{i}"),
                "application/octet-stream",
                vec![0; MAX_SKILL_FILE_BYTES],
            )
            .unwrap();
    }
    assert!(matches!(
        bundle.with_binary_file(
            "last",
            "application/octet-stream",
            vec![0; MAX_SKILL_FILE_BYTES]
        ),
        Err(SkillError::BundleTooLarge)
    ));
}

#[test]
fn supports_crlf_and_ignores_nested_unknown_keys() {
    let markdown = "---\r\nname: demo-skill\r\nmetadata:\r\n  name: wrong\r\ndescription: 'Quoted description'\r\n---\r\n";
    assert!(SkillBundle::new("org/demo-skill", markdown).is_ok());
}

#[test]
fn index_json_roundtrips_all_entry_kinds_and_template_omits_name() {
    let json = r#"{"$schema":"schema","skills":[{"name":"demo-skill","type":"skill-md","description":"d","url":"skill://demo-skill/SKILL.md"},{"name":"archive","type":"archive","description":"d","url":"skill://archive.zip"},{"type":"mcp-resource-template","description":"d","url":"skill://docs/{id}/SKILL.md"}]}"#;
    let index: SkillIndex = serde_json::from_str(json).unwrap();
    assert_eq!(index.skills[0].entry_type, SkillIndexType::SkillMd);
    assert_eq!(index.skills[2].entry_type, SkillIndexType::Template);
    assert_eq!(
        serde_json::to_value(&index).unwrap()["skills"][2].get("name"),
        None
    );
    assert!(
        serde_json::from_str::<SkillIndexEntry>(r#"{"type":"future","description":"d","url":"u"}"#)
            .is_err()
    );
}
