//! Bounded, synchronous snapshots of host-owned skill directories.
use super::{
    MAX_SKILL_FILE_BYTES, MAX_SKILL_FILES, SKILL_ENTRY_POINT, SKILL_MARKDOWN_MIME, SkillBundle,
    SkillData, SkillError, normalize_skill_path, validate_file_path,
};
use std::{
    fs::{self, File},
    io::Read,
    path::{Path, PathBuf},
};

/// Maximum entries visited per bundle or collection root, including directories.
pub const MAX_SKILL_DIRECTORY_ENTRIES: usize = 1024;
/// Maximum nested directory depth below a bundle root.
pub const MAX_SKILL_DIRECTORY_DEPTH: usize = 32;
/// Maximum number of skills loaded by one collection operation.
pub const MAX_SKILLS_PER_DIRECTORY: usize = 64;
/// Maximum combined file bytes loaded by one collection operation.
pub const MAX_SKILL_COLLECTION_BYTES: usize = 32 * 1024 * 1024;

impl SkillBundle {
    /// Snapshot a directory containing `SKILL.md` and optional supporting files.
    ///
    /// Blocking filesystem work runs only during this call. Call before serving,
    /// or use `spawn_blocking` from async code. Text MIME types require UTF-8;
    /// unknown extensions become binary `application/octet-stream`. Symlinks and
    /// special files are rejected, including a symlink at the root. This loader
    /// expects a stable host-owned tree; it is not a sandbox against concurrent
    /// filesystem changes. Later disk changes do not affect the returned bundle.
    /// # Errors
    /// Returns validation, filesystem, UTF-8, traversal, or size-limit errors.
    pub fn from_directory(
        skill_path: impl AsRef<str>,
        directory: impl AsRef<Path>,
    ) -> Result<Self, SkillError> {
        let path = normalize_skill_path(skill_path.as_ref())?;
        let root = directory.as_ref();
        require_directory(root)?;
        let mut files = Vec::new();
        let mut visited = 0;
        collect_files(root, "", 0, &mut visited, &mut files)?;
        files.sort();
        let entry = root.join(SKILL_ENTRY_POINT);
        if !files.iter().any(|file| file == SKILL_ENTRY_POINT) {
            return Err(SkillError::InvalidFrontmatter("missing SKILL.md".into()));
        }
        let markdown = text(read_file(&entry, SKILL_ENTRY_POINT)?, &entry)?;
        let mut bundle = Self::new(path, markdown)?;
        for relative in files {
            if relative == SKILL_ENTRY_POINT {
                continue;
            }
            let absolute = root.join(&relative);
            let data = read_file(&absolute, &relative)?;
            let mime = file_mime(&relative);
            bundle = if is_text(mime) {
                bundle.with_text_file(relative, mime, text(data, &absolute)?)?
            } else {
                bundle.with_binary_file(relative, mime, data)?
            };
        }
        Ok(bundle)
    }
}

impl crate::Server {
    /// Atomically snapshot and register skills from immediate subdirectories.
    ///
    /// Directory names become skill paths. Ordinary root files and directories
    /// without `SKILL.md` are ignored; symlinks and special entries are errors.
    /// All loading and registration must succeed before this server changes.
    /// Uses the same blocking/stable-tree contract as [`SkillBundle::from_directory`].
    /// Returns the number of registered skills; an empty collection is a no-op.
    /// # Errors
    /// Returns filesystem, bundle validation, collection-limit, or URI conflicts.
    pub fn register_skills_from_directory(
        &mut self,
        root: impl AsRef<Path>,
    ) -> Result<usize, SkillError> {
        let root = root.as_ref();
        require_directory(root)?;
        let mut candidate = self.clone();
        let mut count = 0;
        let mut bytes = 0usize;
        for entry in entries(root)? {
            let metadata = metadata(&entry)?;
            if metadata.is_file() {
                continue;
            }
            if !metadata.is_dir() {
                return Err(SkillError::UnsupportedFile(entry));
            }
            match fs::symlink_metadata(entry.join(SKILL_ENTRY_POINT)) {
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
                Err(source) => return Err(io_error(&entry.join(SKILL_ENTRY_POINT), source)),
                Ok(_) => {}
            }
            if count >= MAX_SKILLS_PER_DIRECTORY {
                return Err(SkillError::DirectoryLimit);
            }
            let name = entry
                .file_name()
                .and_then(|name| name.to_str())
                .ok_or_else(|| SkillError::InvalidFilesystemPath(entry.clone()))?;
            let bundle = SkillBundle::from_directory(name, &entry)?;
            bytes += bundle
                .files
                .values()
                .map(|file| match &file.data {
                    SkillData::Text(text) => text.len(),
                    SkillData::Binary(data) => data.len(),
                })
                .sum::<usize>();
            if bytes > MAX_SKILL_COLLECTION_BYTES {
                return Err(SkillError::CollectionTooLarge);
            }
            candidate.register_skill(bundle)?;
            count += 1;
        }
        *self = candidate;
        Ok(count)
    }
}

fn io_error(path: &Path, source: std::io::Error) -> SkillError {
    SkillError::Io {
        path: path.to_path_buf(),
        source,
    }
}
fn metadata(path: &Path) -> Result<fs::Metadata, SkillError> {
    let metadata = fs::symlink_metadata(path).map_err(|error| io_error(path, error))?;
    if metadata.file_type().is_symlink() {
        return Err(SkillError::UnsupportedFile(path.to_path_buf()));
    }
    Ok(metadata)
}
fn require_directory(path: &Path) -> Result<(), SkillError> {
    if !metadata(path)?.is_dir() {
        return Err(SkillError::UnsupportedFile(path.to_path_buf()));
    }
    Ok(())
}
fn entries(directory: &Path) -> Result<Vec<PathBuf>, SkillError> {
    let mut entries = Vec::new();
    for entry in fs::read_dir(directory).map_err(|error| io_error(directory, error))? {
        let entry = entry.map_err(|error| io_error(directory, error))?;
        if entries.len() >= MAX_SKILL_DIRECTORY_ENTRIES {
            return Err(SkillError::DirectoryLimit);
        }
        entries.push(entry.path());
    }
    entries.sort();
    Ok(entries)
}
fn collect_files(
    root: &Path,
    prefix: &str,
    depth: usize,
    visited: &mut usize,
    files: &mut Vec<String>,
) -> Result<(), SkillError> {
    for path in entries(root)? {
        *visited += 1;
        if *visited > MAX_SKILL_DIRECTORY_ENTRIES {
            return Err(SkillError::DirectoryLimit);
        }
        let name = path
            .file_name()
            .and_then(|name| name.to_str())
            .ok_or_else(|| SkillError::InvalidFilesystemPath(path.clone()))?;
        let relative = if prefix.is_empty() {
            name.to_owned()
        } else {
            format!("{prefix}/{name}")
        };
        validate_file_path(&relative)?;
        let metadata = metadata(&path)?;
        if metadata.is_dir() {
            if depth >= MAX_SKILL_DIRECTORY_DEPTH {
                return Err(SkillError::DirectoryLimit);
            }
            collect_files(&path, &relative, depth + 1, visited, files)?;
        } else if metadata.is_file() {
            if files.len() >= MAX_SKILL_FILES {
                return Err(SkillError::TooManyFiles);
            }
            files.push(relative);
        } else {
            return Err(SkillError::UnsupportedFile(path));
        }
    }
    Ok(())
}
fn read_file(path: &Path, relative: &str) -> Result<Vec<u8>, SkillError> {
    let metadata = metadata(path)?;
    if !metadata.is_file() {
        return Err(SkillError::UnsupportedFile(path.to_path_buf()));
    }
    if metadata.len() > MAX_SKILL_FILE_BYTES as u64 {
        return Err(SkillError::FileTooLarge {
            path: relative.to_owned(),
        });
    }
    let file = File::open(path).map_err(|error| io_error(path, error))?;
    if !file
        .metadata()
        .map_err(|error| io_error(path, error))?
        .is_file()
    {
        return Err(SkillError::UnsupportedFile(path.to_path_buf()));
    }
    let mut data = Vec::new();
    file.take(MAX_SKILL_FILE_BYTES as u64 + 1)
        .read_to_end(&mut data)
        .map_err(|error| io_error(path, error))?;
    if data.len() > MAX_SKILL_FILE_BYTES {
        return Err(SkillError::FileTooLarge {
            path: relative.to_owned(),
        });
    }
    Ok(data)
}
fn text(data: Vec<u8>, path: &Path) -> Result<String, SkillError> {
    String::from_utf8(data).map_err(|_| SkillError::InvalidUtf8(path.to_path_buf()))
}
fn file_mime(path: &str) -> &'static str {
    match Path::new(path)
        .extension()
        .and_then(|value| value.to_str())
        .unwrap_or_default()
        .to_ascii_lowercase()
        .as_str()
    {
        "md" | "markdown" => SKILL_MARKDOWN_MIME,
        "json" => "application/json",
        "txt" => "text/plain",
        "py" => "text/x-python",
        "js" => "text/javascript",
        "ts" => "text/typescript",
        "yaml" | "yml" => "application/yaml",
        "html" | "htm" => "text/html",
        "css" => "text/css",
        "png" => "image/png",
        "jpg" | "jpeg" => "image/jpeg",
        "gif" => "image/gif",
        "svg" => "image/svg+xml",
        "pdf" => "application/pdf",
        "zip" => "application/zip",
        "gz" => "application/gzip",
        _ => "application/octet-stream",
    }
}
fn is_text(mime: &str) -> bool {
    mime.starts_with("text/")
        || matches!(
            mime,
            "application/json" | "application/yaml" | "image/svg+xml"
        )
}
