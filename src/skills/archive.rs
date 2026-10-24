//! Packed skills served as bounded opaque binary resources.
use std::{fs, io::Read, path::Path};

use super::{SkillError, normalize_skill_path};

/// Maximum packed byte length of one archive snapshot (8 MiB).
pub const MAX_SKILL_ARCHIVE_BYTES: usize = 8 * 1024 * 1024;

/// Packed format and corresponding canonical resource suffix.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SkillArchiveFormat {
    /// ZIP archive, served with `application/zip` and `.zip`.
    Zip,
    /// Gzip-compressed tar archive, served with `application/gzip` and `.tar.gz`.
    TarGzip,
}

impl SkillArchiveFormat {
    /// Canonical suffix appended to the skill namespace.
    pub const fn suffix(self) -> &'static str {
        match self {
            Self::Zip => ".zip",
            Self::TarGzip => ".tar.gz",
        }
    }

    /// MIME type advertised and returned by the binary resource.
    pub const fn mime_type(self) -> &'static str {
        match self {
            Self::Zip => "application/zip",
            Self::TarGzip => "application/gzip",
        }
    }
}

/// Immutable packed skill snapshot, never unpacked or inspected by the server.
///
/// Callers must provide trusted archives containing `SKILL.md` at the root, with
/// a frontmatter name matching the final namespace segment. Size checks apply
/// to packed bytes only, not expanded size; extraction is the host's concern.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SkillArchive {
    pub(crate) path: String,
    pub(crate) description: String,
    pub(crate) format: SkillArchiveFormat,
    pub(crate) bytes: Vec<u8>,
}

impl SkillArchive {
    /// Creates an opaque snapshot with description `Archived skill {name}`.
    ///
    /// This does not parse archive headers, contents, or embedded frontmatter.
    /// # Errors
    /// Returns an error for an invalid namespace or oversized packed bytes.
    pub fn new(
        skill_path: impl AsRef<str>,
        format: SkillArchiveFormat,
        bytes: Vec<u8>,
    ) -> Result<Self, SkillError> {
        let path = normalize_skill_path(skill_path.as_ref())?;
        if bytes.len() > MAX_SKILL_ARCHIVE_BYTES {
            return Err(SkillError::ArchiveTooLarge);
        }
        let name = path.rsplit('/').next().unwrap_or_default();
        Ok(Self {
            description: format!("Archived skill {name}"),
            path,
            format,
            bytes,
        })
    }

    /// Snapshots a host-owned `.zip`, `.tar.gz`, or `.tgz` regular file.
    ///
    /// Suffixes are case-insensitive; `.tgz` is served as `.tar.gz`. Symlinks
    /// and special files are rejected. No sibling `SKILL.md` is read: use
    /// [`Self::with_description`] for explicit metadata. Like the directory
    /// loader, this blocking call requires a stable host-owned filesystem and
    /// is not a sandbox against concurrent replacement. Run before serving or
    /// in `spawn_blocking`. Later disk changes do not affect the snapshot.
    /// # Errors
    /// Returns path, suffix, filesystem, file-type, or packed-size errors.
    pub fn from_file(
        skill_path: impl AsRef<str>,
        file_path: impl AsRef<Path>,
    ) -> Result<Self, SkillError> {
        let skill_path = normalize_skill_path(skill_path.as_ref())?;
        let path = file_path.as_ref();
        let filename = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| SkillError::InvalidFilesystemPath(path.to_path_buf()))?
            .to_ascii_lowercase();
        let format = if filename.ends_with(".zip") {
            SkillArchiveFormat::Zip
        } else if filename.ends_with(".tar.gz") || filename.ends_with(".tgz") {
            SkillArchiveFormat::TarGzip
        } else {
            return Err(SkillError::UnsupportedArchiveFormat(path.to_path_buf()));
        };
        let io_error = |source| SkillError::Io {
            path: path.to_path_buf(),
            source,
        };
        let metadata = fs::symlink_metadata(path).map_err(io_error)?;
        if !metadata.is_file() {
            return Err(SkillError::UnsupportedFile(path.to_path_buf()));
        }
        if metadata.len() > MAX_SKILL_ARCHIVE_BYTES as u64 {
            return Err(SkillError::ArchiveTooLarge);
        }
        let file = fs::File::open(path).map_err(io_error)?;
        if !file.metadata().map_err(io_error)?.is_file() {
            return Err(SkillError::UnsupportedFile(path.to_path_buf()));
        }
        let mut bytes = Vec::new();
        file.take(MAX_SKILL_ARCHIVE_BYTES as u64 + 1)
            .read_to_end(&mut bytes)
            .map_err(io_error)?;
        Self::new(skill_path, format, bytes)
    }

    /// Sets explicit metadata without inspecting the packed archive.
    /// # Errors
    /// Returns an error for an empty description or control characters.
    pub fn with_description(mut self, description: impl Into<String>) -> Result<Self, SkillError> {
        let description = description.into();
        if description.trim().is_empty() || description.chars().any(char::is_control) {
            return Err(SkillError::InvalidArchiveDescription);
        }
        self.description = description;
        Ok(self)
    }
}
