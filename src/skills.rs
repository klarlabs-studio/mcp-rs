//! Agent Skills bundles, archive snapshots and dynamic templates (SEP-2640).
//!
//! Frontmatter support intentionally covers only the small subset used by the
//! Go implementation: top-level `name` and `description`, simple quotes, and
//! `|`/`>` indented blocks. This is not a general YAML parser.

mod template;
pub use template::SkillTemplateBuilder;

mod archive;
pub use archive::{MAX_SKILL_ARCHIVE_BYTES, SkillArchive, SkillArchiveFormat};

mod directory;
pub use directory::{
    MAX_SKILL_COLLECTION_BYTES, MAX_SKILL_DIRECTORY_DEPTH, MAX_SKILL_DIRECTORY_ENTRIES,
    MAX_SKILLS_PER_DIRECTORY,
};

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

/// Well-known resource URI for the Agent Skills discovery catalog.
pub const SKILL_INDEX_URI: &str = "skill://index.json";
/// Schema URI declared by the Agent Skills discovery catalog.
pub const SKILL_INDEX_SCHEMA: &str = "https://schemas.agentskills.io/discovery/0.2.0/schema.json";
/// MCP extension identifier advertised when skills are registered.
pub const SKILLS_EXTENSION_ID: &str = "io.modelcontextprotocol/skills";
/// Required entry document at each skill root.
pub const SKILL_ENTRY_POINT: &str = "SKILL.md";
/// Recommended MIME type for a skill entry document.
pub const SKILL_MARKDOWN_MIME: &str = "text/markdown";
/// Maximum number of files in a bundle, including its entry document.
pub const MAX_SKILL_FILES: usize = 256;
/// Maximum byte length of an individual file.
pub const MAX_SKILL_FILE_BYTES: usize = 1024 * 1024;
/// Maximum combined byte length of all bundle files.
pub const MAX_SKILL_TOTAL_BYTES: usize = 8 * 1024 * 1024;

/// Errors encountered while loading or registering a skill.
#[derive(Debug, thiserror::Error)]
pub enum SkillError {
    /// A skill namespace or relative file path is invalid.
    #[error("invalid skill path: {0}")]
    InvalidPath(String),
    /// The entry document metadata is absent or malformed.
    #[error("invalid SKILL.md frontmatter: {0}")]
    InvalidFrontmatter(String),
    /// A file path has already been added to the bundle.
    #[error("duplicate skill file path: {0}")]
    DuplicateFile(String),
    /// The bundle contains too many files.
    #[error("skill file limit exceeded ({MAX_SKILL_FILES})")]
    TooManyFiles,
    /// A single file exceeds its byte limit.
    #[error("skill file {path:?} exceeds the {MAX_SKILL_FILE_BYTES} byte limit")]
    FileTooLarge {
        /// Relative path of the oversized file.
        path: String,
    },
    /// The combined file contents exceed the bundle byte limit.
    #[error("skill bundle exceeds the {MAX_SKILL_TOTAL_BYTES} byte total limit")]
    BundleTooLarge,
    /// A MIME type is empty or contains control characters.
    #[error("invalid MIME type: {0}")]
    InvalidMimeType(String),
    /// A filesystem operation failed at the given host path.
    #[error("skill filesystem operation at {path:?}: {source}")]
    Io {
        /// Host path involved in the operation.
        path: std::path::PathBuf,
        /// Original filesystem error.
        #[source]
        source: std::io::Error,
    },
    /// A symlink, special file, or unexpected root type was encountered.
    #[error("unsupported skill filesystem entry: {0:?}")]
    UnsupportedFile(std::path::PathBuf),
    /// A path cannot be represented as UTF-8.
    #[error("skill filesystem path is not UTF-8: {0:?}")]
    InvalidFilesystemPath(std::path::PathBuf),
    /// A text file is not valid UTF-8.
    #[error("skill text file is not UTF-8: {0:?}")]
    InvalidUtf8(std::path::PathBuf),
    /// Directory traversal exceeded an entry, depth, or skill-count limit.
    #[error("skill directory traversal limit exceeded")]
    DirectoryLimit,
    /// A multi-skill collection exceeded its combined byte budget.
    #[error("skill directory collection exceeds its byte budget")]
    CollectionTooLarge,
    /// A namespace is already owned by another skill or archive.
    #[error("skill namespace is already registered: {0}")]
    DuplicateSkill(String),
    /// The archive filename has an unsupported suffix.
    #[error("skill archive must be .zip, .tar.gz, or .tgz: {0:?}")]
    UnsupportedArchiveFormat(std::path::PathBuf),
    /// Packed bytes exceed the archive snapshot limit.
    #[error("skill archive exceeds the {MAX_SKILL_ARCHIVE_BYTES} byte limit")]
    ArchiveTooLarge,
    /// An explicit archive description is empty or contains control characters.
    #[error("invalid skill archive description")]
    InvalidArchiveDescription,
    /// A skill template is outside the supported named-segment URI grammar.
    #[error("invalid skill URI template: {0}")]
    InvalidTemplate(String),
    /// A skill template description is blank.
    #[error("skill template description must not be blank")]
    InvalidTemplateDescription,
    /// Resource registration failed after skill validation.
    #[error(transparent)]
    Registration(#[from] crate::RegistrationError),
}

/// A validated skill and its bounded, in-memory files.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillBundle {
    pub(crate) path: String,
    pub(crate) name: String,
    pub(crate) description: String,
    pub(crate) files: BTreeMap<String, SkillFile>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SkillFile {
    pub(crate) mime_type: String,
    pub(crate) data: SkillData,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum SkillData {
    Text(String),
    Binary(Vec<u8>),
}

impl SkillBundle {
    /// Creates a bundle from a path and its required `SKILL.md` contents.
    /// The final path segment must match frontmatter `name`.
    pub fn new(
        skill_path: impl AsRef<str>,
        markdown: impl Into<String>,
    ) -> Result<Self, SkillError> {
        let path = normalize_skill_path(skill_path.as_ref())?;
        let markdown = markdown.into();
        if markdown.len() > MAX_SKILL_FILE_BYTES {
            return Err(SkillError::FileTooLarge {
                path: SKILL_ENTRY_POINT.to_owned(),
            });
        }
        let (name, description) = parse_frontmatter(&markdown)?;
        let final_segment = path.rsplit('/').next().unwrap_or_default();
        if name != final_segment {
            return Err(SkillError::InvalidFrontmatter(format!(
                "name {name:?} must equal final path segment {final_segment:?}"
            )));
        }
        let mut files = BTreeMap::new();
        files.insert(
            SKILL_ENTRY_POINT.to_owned(),
            SkillFile {
                mime_type: SKILL_MARKDOWN_MIME.to_owned(),
                data: SkillData::Text(markdown),
            },
        );
        Ok(Self {
            path,
            name,
            description,
            files,
        })
    }

    /// Adds a UTF-8 text file with an explicit MIME type.
    pub fn with_text_file(
        mut self,
        relative_path: impl AsRef<str>,
        mime_type: impl Into<String>,
        text: impl Into<String>,
    ) -> Result<Self, SkillError> {
        self.insert_file(
            relative_path.as_ref(),
            mime_type.into(),
            SkillData::Text(text.into()),
        )?;
        Ok(self)
    }

    /// Adds a binary file with an explicit MIME type.
    pub fn with_binary_file(
        mut self,
        relative_path: impl AsRef<str>,
        mime_type: impl Into<String>,
        data: Vec<u8>,
    ) -> Result<Self, SkillError> {
        self.insert_file(
            relative_path.as_ref(),
            mime_type.into(),
            SkillData::Binary(data),
        )?;
        Ok(self)
    }

    fn insert_file(
        &mut self,
        relative_path: &str,
        mime_type: String,
        data: SkillData,
    ) -> Result<(), SkillError> {
        let path = validate_file_path(relative_path)?;
        validate_mime(&mime_type)?;
        if self.files.contains_key(&path) {
            return Err(SkillError::DuplicateFile(path));
        }
        if self.files.len() >= MAX_SKILL_FILES {
            return Err(SkillError::TooManyFiles);
        }
        let size = match &data {
            SkillData::Text(s) => s.len(),
            SkillData::Binary(b) => b.len(),
        };
        if size > MAX_SKILL_FILE_BYTES {
            return Err(SkillError::FileTooLarge { path });
        }
        let total: usize = self
            .files
            .values()
            .map(|f| match &f.data {
                SkillData::Text(s) => s.len(),
                SkillData::Binary(b) => b.len(),
            })
            .sum();
        if total.saturating_add(size) > MAX_SKILL_TOTAL_BYTES {
            return Err(SkillError::BundleTooLarge);
        }
        self.files.insert(path, SkillFile { mime_type, data });
        Ok(())
    }
}

/// JSON body of the `skill://index.json` discovery resource.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillIndex {
    #[serde(rename = "$schema")]
    /// Discovery schema URI.
    pub schema: String,
    /// Skills and templates advertised by the server.
    pub skills: Vec<SkillIndexEntry>,
}

/// One skill or resource template in a discovery index.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkillIndexEntry {
    #[serde(default, skip_serializing_if = "String::is_empty")]
    /// Skill name; omitted for resource templates.
    pub name: String,
    #[serde(rename = "type")]
    /// Kind of resource represented by this entry.
    pub entry_type: SkillIndexType,
    /// Human-readable summary.
    pub description: String,
    /// Resource URI or URI template.
    pub url: String,
}

/// Supported wire values for discovery index entry types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SkillIndexType {
    #[serde(rename = "skill-md")]
    /// A skill with an entry document resource.
    SkillMd,
    #[serde(rename = "archive")]
    /// A skill distributed as an archive resource.
    Archive,
    #[serde(rename = "mcp-resource-template")]
    /// A parameterized skill resource template.
    Template,
}

fn normalize_skill_path(path: &str) -> Result<String, SkillError> {
    let normalized = path.trim_matches('/');
    let parts: Vec<_> = normalized.split('/').collect();
    if normalized.is_empty()
        || parts.iter().any(|p| {
            p.is_empty()
                || !p
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
        })
    {
        return Err(SkillError::InvalidPath(path.to_owned()));
    }
    let last = parts.last().copied().unwrap_or_default();
    if last.is_empty()
        || !last
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    {
        return Err(SkillError::InvalidPath(path.to_owned()));
    }
    Ok(parts.join("/"))
}

fn validate_file_path(path: &str) -> Result<String, SkillError> {
    let parts: Vec<_> = path.split('/').collect();
    if path.is_empty()
        || parts.iter().any(|p| {
            p.is_empty()
                || *p == "."
                || *p == ".."
                || !p
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'_' | b'-' | b'.'))
        })
    {
        return Err(SkillError::InvalidPath(path.to_owned()));
    }
    Ok(parts.join("/"))
}

fn validate_mime(mime: &str) -> Result<(), SkillError> {
    if mime.trim().is_empty() || mime.chars().any(char::is_control) {
        return Err(SkillError::InvalidMimeType(mime.to_owned()));
    }
    Ok(())
}

fn parse_frontmatter(content: &str) -> Result<(String, String), SkillError> {
    let body = content.replace("\r\n", "\n");
    if !body.starts_with("---") {
        return Err(SkillError::InvalidFrontmatter(
            "missing opening --- fence".into(),
        ));
    }
    let rest = body.strip_prefix("---\n").ok_or_else(|| {
        SkillError::InvalidFrontmatter("opening --- fence must be a full line".into())
    })?;
    let end = rest
        .find("\n---\n")
        .or_else(|| rest.strip_suffix("\n---").map(|_| rest.len() - 4))
        .ok_or_else(|| {
            SkillError::InvalidFrontmatter("frontmatter not closed with an exact --- fence".into())
        })?;
    let lines: Vec<_> = rest[..end].split('\n').collect();
    let mut name: Option<String> = None;
    let mut description: Option<String> = None;
    let mut i = 0;
    while i < lines.len() {
        let line = lines[i];
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            i += 1;
            continue;
        }
        let Some((key, value)) = line.split_once(':') else {
            i += 1;
            continue;
        };
        if line.as_bytes().first().is_some_and(u8::is_ascii_whitespace) {
            i += 1;
            continue;
        }
        let value = value.trim();
        match key.trim() {
            "name" => {
                if name.is_some() {
                    return Err(SkillError::InvalidFrontmatter(
                        "duplicate top-level name".into(),
                    ));
                }
                name = Some(unquote(value));
            }
            "description" => {
                if description.is_some() {
                    return Err(SkillError::InvalidFrontmatter(
                        "duplicate top-level description".into(),
                    ));
                }
                let (v, end_i) = read_value(value, &lines, i);
                description = Some(v);
                i = end_i;
            }
            _ => {}
        }
        i += 1;
    }
    let name = name
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| SkillError::InvalidFrontmatter("missing name".into()))?;
    let description = description
        .filter(|s| !s.trim().is_empty())
        .ok_or_else(|| SkillError::InvalidFrontmatter("missing description".into()))?;
    if !name
        .bytes()
        .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-')
    {
        return Err(SkillError::InvalidFrontmatter(
            "name must use lowercase ASCII letters, digits, and hyphens".into(),
        ));
    }
    Ok((name, description))
}

fn unquote(value: &str) -> String {
    let bytes = value.as_bytes();
    if bytes.len() >= 2
        && ((bytes[0] == b'"' && bytes[bytes.len() - 1] == b'"')
            || (bytes[0] == b'\'' && bytes[bytes.len() - 1] == b'\''))
    {
        value[1..value.len() - 1].to_owned()
    } else {
        value.to_owned()
    }
}

fn read_value(value: &str, lines: &[&str], start: usize) -> (String, usize) {
    if value != "|" && value != ">" {
        return (unquote(value), start);
    }
    let mut result = String::new();
    let mut i = start;
    while i + 1 < lines.len() {
        let next = lines[i + 1];
        if !next.is_empty() && !next.starts_with(' ') && !next.starts_with('\t') {
            break;
        }
        i += 1;
        if !result.is_empty() {
            result.push('\n');
        }
        result.push_str(next.trim());
    }
    (result, i)
}
