use crate::temporary::CleanupGuard;
use std::path::{Path, PathBuf};

/// User-selected or defaulted output path before ffmpeg may write it.
#[must_use]
#[derive(Debug)]
pub struct PlannedOutput {
    path: PathBuf,
}

/// Output registered for cleanup while ffmpeg may be writing it.
#[must_use]
pub struct PartialOutput {
    guard: CleanupGuard,
}

/// Successful encode output, no longer subject to failure cleanup.
#[must_use]
pub struct CompletedOutput {
    path: PathBuf,
}

impl PlannedOutput {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn begin(self) -> PartialOutput {
        PartialOutput {
            guard: CleanupGuard::arm(self.path),
        }
    }
}

impl PartialOutput {
    pub fn path(&self) -> &Path {
        self.guard.path()
    }

    pub fn commit(self) -> CompletedOutput {
        CompletedOutput {
            path: self.guard.disarm(),
        }
    }
}

impl crate::ffmpeg::EncodeDestination for PartialOutput {
    fn encode_destination(&self) -> &Path {
        self.path()
    }
}

impl CompletedOutput {
    pub fn path(&self) -> &Path {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::temporary;
    use serial_test::serial;
    use std::{env, fs};

    fn temp_path(label: &str) -> PathBuf {
        env::temp_dir().join(format!(
            "ab-av1-encode-lifecycle-{}-{}",
            label,
            std::process::id()
        ))
    }

    #[serial]
    #[test]
    fn planned_output_is_not_registered_for_cleanup() {
        let path = temp_path("planned");
        fs::write(&path, b"stay").expect("write file");
        let _planned = PlannedOutput::new(path.clone());
        assert!(
            !temporary::unadd(&path),
            "planned output must not register cleanup"
        );
        let _ = fs::remove_file(path);
    }

    #[serial]
    #[tokio::test]
    async fn partial_output_cleans_up_when_not_committed() {
        let path = temp_path("partial-drop");
        fs::write(&path, b"temp").expect("write file");
        {
            let _partial = PlannedOutput::new(path.clone()).begin();
        }
        temporary::clean_all().await;
        assert!(!path.exists(), "uncommitted partial output must be deleted");
    }

    #[serial]
    #[tokio::test]
    async fn completed_output_survives_cleanup() {
        let path = temp_path("completed");
        fs::write(&path, b"stay").expect("write file");
        let completed = PlannedOutput::new(path.clone()).begin().commit();
        temporary::clean_all().await;
        assert!(
            completed.path().exists(),
            "completed output must survive cleanup"
        );
        let _ = fs::remove_file(path);
    }
}
