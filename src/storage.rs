use std::path::{Component, Path, PathBuf};

use crate::error::AppError;

/// Validates stored relative paths loaded from internal state; invalid values
/// are treated as storage corruption rather than client input errors.
pub fn resolve_relative_path(input: &str) -> Result<PathBuf, AppError> {
    if input.is_empty() {
        return Err(AppError::Storage(
            "empty relative paths are not allowed".to_string(),
        ));
    }

    let path = Path::new(input);
    if path.is_absolute() {
        return Err(AppError::Storage(
            "absolute paths are not allowed".to_string(),
        ));
    }

    for component in path.components() {
        match component {
            Component::Normal(_) => {}
            _ => {
                return Err(AppError::Storage(
                    "path traversal is not allowed".to_string(),
                ))
            }
        }
    }

    Ok(path.to_path_buf())
}

pub fn build_file_path(storage_dir: &Path, relative_path: &str) -> Result<PathBuf, AppError> {
    Ok(storage_dir.join(resolve_relative_path(relative_path)?))
}

pub async fn resolve_stored_download_dir(
    storage_root: &Path,
    storage_dir: &Path,
) -> Result<PathBuf, AppError> {
    let storage_root = canonicalize_existing_or_normalize_missing_path(storage_root).await?;
    let storage_dir = canonicalize_existing_or_normalize_missing_path(storage_dir).await?;

    if !storage_dir.starts_with(&storage_root) {
        return Err(AppError::Storage(
            "download storage directory escaped storage root".to_string(),
        ));
    }

    Ok(storage_dir)
}

pub async fn resolve_stored_file_path(
    storage_root: &Path,
    storage_dir: &Path,
    relative_path: &str,
) -> Result<PathBuf, AppError> {
    let relative_path = resolve_relative_path(relative_path)?;
    let storage_dir = resolve_stored_download_dir(storage_root, storage_dir).await?;

    let file_path = canonicalize_path(&storage_dir.join(relative_path)).await?;
    if !file_path.starts_with(&storage_dir) {
        return Err(AppError::Storage(
            "resolved file path escaped download directory".to_string(),
        ));
    }

    Ok(file_path)
}

pub async fn delete_dir_if_exists(path: &Path) -> Result<(), AppError> {
    match tokio::fs::remove_dir_all(path).await {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(AppError::Storage(error.to_string())),
    }
}

/// Takes an exclusive advisory lock on `<storage_root>/.lock`, failing if
/// another instance holds it. The lock lives as long as the returned file.
pub fn acquire_instance_lock(storage_root: &Path) -> Result<std::fs::File, AppError> {
    std::fs::create_dir_all(storage_root).map_err(|error| AppError::Storage(error.to_string()))?;
    let file = std::fs::OpenOptions::new()
        .create(true)
        .truncate(false)
        .write(true)
        .open(storage_root.join(".lock"))
        .map_err(|error| AppError::Storage(error.to_string()))?;

    file.try_lock().map_err(|error| match error {
        std::fs::TryLockError::WouldBlock => {
            AppError::Storage("another instance is already using this storage root".to_string())
        }
        std::fs::TryLockError::Error(error) => AppError::Storage(error.to_string()),
    })?;

    Ok(file)
}

async fn canonicalize_path(path: &Path) -> Result<PathBuf, AppError> {
    tokio::fs::canonicalize(path)
        .await
        .map_err(|error| AppError::Storage(error.to_string()))
}

async fn canonicalize_existing_or_normalize_missing_path(path: &Path) -> Result<PathBuf, AppError> {
    let absolute_path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|error| AppError::Storage(error.to_string()))?
            .join(path)
    };
    let mut existing_ancestor = absolute_path.as_path();

    loop {
        match tokio::fs::canonicalize(existing_ancestor).await {
            Ok(canonical_ancestor) => {
                let suffix = absolute_path
                    .strip_prefix(existing_ancestor)
                    .map_err(|error| AppError::Storage(error.to_string()))?;
                return normalize_absolute_path(&canonical_ancestor.join(suffix));
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
                existing_ancestor = existing_ancestor
                    .parent()
                    .ok_or_else(|| AppError::Storage(error.to_string()))?;
            }
            Err(error) => return Err(AppError::Storage(error.to_string())),
        }
    }
}

fn normalize_absolute_path(path: &Path) -> Result<PathBuf, AppError> {
    let path = if path.is_absolute() {
        path.to_path_buf()
    } else {
        std::env::current_dir()
            .map_err(|error| AppError::Storage(error.to_string()))?
            .join(path)
    };

    let mut normalized = PathBuf::new();

    for component in path.components() {
        match component {
            Component::Prefix(prefix) => normalized.push(prefix.as_os_str()),
            Component::RootDir => normalized.push(component.as_os_str()),
            Component::CurDir => {}
            Component::Normal(part) => normalized.push(part),
            Component::ParentDir => {
                if !normalized.pop() {
                    return Err(AppError::Storage(
                        "download storage directory escaped storage root".to_string(),
                    ));
                }
            }
        }
    }

    Ok(normalized)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;
    use tempfile::tempdir;

    #[test]
    fn accepts_nested_relative_paths() {
        assert_eq!(
            resolve_relative_path("Book/01.mp3").unwrap(),
            Path::new("Book/01.mp3")
        );
    }

    #[test]
    fn rejects_parent_segments() {
        assert!(resolve_relative_path("../secret.txt").is_err());
    }

    #[test]
    fn rejects_absolute_paths() {
        assert!(resolve_relative_path("/tmp/file.mp3").is_err());
    }

    #[test]
    fn rejects_empty_paths() {
        assert!(resolve_relative_path("").is_err());
    }

    #[test]
    fn instance_lock_is_exclusive() {
        let root = tempdir().unwrap();
        let first = acquire_instance_lock(root.path()).unwrap();

        assert!(acquire_instance_lock(root.path()).is_err());

        drop(first);
        assert!(acquire_instance_lock(root.path()).is_ok());
    }

    #[tokio::test]
    async fn resolves_existing_storage_dirs_under_root() {
        let root = tempdir().unwrap();
        let storage_dir = root.path().join("download-1");

        tokio::fs::create_dir_all(&storage_dir).await.unwrap();

        assert_eq!(
            resolve_stored_download_dir(root.path(), &storage_dir)
                .await
                .unwrap(),
            storage_dir.canonicalize().unwrap()
        );
    }

    #[tokio::test]
    async fn resolves_missing_storage_dirs_under_root() {
        let root = tempdir().unwrap();
        let storage_dir = root.path().join("download-1");

        assert_eq!(
            resolve_stored_download_dir(root.path(), &storage_dir)
                .await
                .unwrap(),
            root.path().canonicalize().unwrap().join("download-1")
        );
    }

    #[tokio::test]
    async fn rejects_storage_dirs_outside_root() {
        let root = tempdir().unwrap();
        let outside = tempdir().unwrap();

        assert!(resolve_stored_download_dir(root.path(), outside.path())
            .await
            .is_err());
    }
}
