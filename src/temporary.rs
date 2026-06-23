//! Temporary workspace logic.
use anyhow::Context;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

pub type SharedWorkspace = Arc<Workspace>;

pub struct Workspace {
    dir: Option<tempfile::TempDir>,
    keep: bool,
}

impl Workspace {
    pub fn new(parent: Option<PathBuf>, keep: bool) -> anyhow::Result<Self> {
        let parent =
            parent.unwrap_or_else(|| std::env::current_dir().expect("current working directory"));
        let dir = tempfile::Builder::new()
            .prefix(".ab-av1-")
            .tempdir_in(&parent)
            .with_context(|| {
                format!(
                    "failed to create temporary workspace in {}",
                    parent.display()
                )
            })?;

        Ok(Self {
            dir: Some(dir),
            keep,
        })
    }

    pub fn path(&self) -> &Path {
        self.dir.as_ref().expect("temporary workspace").path()
    }

    pub fn join(&self, path: impl AsRef<Path>) -> PathBuf {
        self.path().join(path)
    }
}

impl Drop for Workspace {
    fn drop(&mut self) {
        if self.keep
            && let Some(dir) = self.dir.take()
        {
            _ = dir.keep();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workspace_cleans_up_by_default() {
        let parent = tempfile::tempdir().expect("parent temp dir");
        let workspace = Workspace::new(Some(parent.path().to_owned()), false).expect("workspace");
        let path = workspace.path().to_owned();
        std::fs::write(workspace.join("sample.mkv"), b"sample").expect("write sample");

        drop(workspace);

        assert!(!path.exists(), "workspace should be deleted on drop");
    }

    #[test]
    fn workspace_can_be_kept() {
        let parent = tempfile::tempdir().expect("parent temp dir");
        let workspace = Workspace::new(Some(parent.path().to_owned()), true).expect("workspace");
        let path = workspace.path().to_owned();
        std::fs::write(workspace.join("sample.mkv"), b"sample").expect("write sample");

        drop(workspace);

        assert!(path.exists(), "workspace should remain on drop");
        std::fs::remove_dir_all(path).expect("cleanup kept workspace");
    }

    #[test]
    fn workspace_uses_configured_parent_and_hidden_prefix() {
        let parent = tempfile::tempdir().expect("parent temp dir");
        let workspace = Workspace::new(Some(parent.path().to_owned()), false).expect("workspace");
        let path = workspace.path();

        assert_eq!(path.parent(), Some(parent.path()));
        assert!(
            path.file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.starts_with(".ab-av1-"))
        );
    }

    #[test]
    fn workspaces_do_not_collide() {
        let parent = tempfile::tempdir().expect("parent temp dir");
        let first = Workspace::new(Some(parent.path().to_owned()), false).expect("first");
        let second = Workspace::new(Some(parent.path().to_owned()), false).expect("second");

        assert_ne!(first.path(), second.path());
    }
}
