//! temp file logic
use std::{
    collections::HashMap,
    env, iter,
    path::{Path, PathBuf},
    sync::{LazyLock, Mutex},
};

static TEMPS: LazyLock<Mutex<HashMap<PathBuf, TempKind>>> = LazyLock::new(<_>::default);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TempKind {
    /// Should always be deleted at the end of the program.
    NotKeepable,
    /// Usually deleted but may be kept, e.g. with --keep.
    Keepable,
}

/// Add a file as temporary so it can be deleted later.
pub fn add(file: impl Into<PathBuf>, kind: TempKind) {
    TEMPS.lock().unwrap().insert(file.into(), kind);
}

/// Remove a previously added file so that it won't be deleted later,
/// if it hasn't already.
pub fn unadd(file: &Path) -> bool {
    TEMPS.lock().unwrap().remove(file).is_some()
}

/// Delete all added temporary files.
/// If `keep_keepables` true don't delete [`TempKind::Keepable`] temporary files.
pub async fn clean(keep_keepables: bool) {
    match keep_keepables {
        true => clean_non_keepables().await,
        false => clean_all().await,
    }
}

/// Delete all added temporary files.
pub async fn clean_all() {
    let mut files: Vec<_> = std::mem::take(&mut *TEMPS.lock().unwrap())
        .into_keys()
        .collect();
    files.sort_by_key(|f| f.is_dir()); // rm dir at the end

    for file in files {
        match file.is_dir() {
            true => _ = tokio::fs::remove_dir_all(&file).await,
            false => _ = tokio::fs::remove_file(&file).await,
        }
    }
}

async fn clean_non_keepables() {
    let mut matching: Vec<_> = TEMPS
        .lock()
        .unwrap()
        .iter()
        .filter(|(_, k)| **k == TempKind::NotKeepable)
        .map(|(f, _)| f.clone())
        .collect();
    matching.sort_by_key(|f| f.is_dir()); // rm dir at the end

    for file in matching {
        match file.is_dir() {
            true => _ = tokio::fs::remove_dir_all(&file).await,
            false => _ = tokio::fs::remove_file(&file).await,
        }
        TEMPS.lock().unwrap().remove(&file);
    }
}

/// Return a temporary directory that is distinct per process/run.
///
/// Configured `--temp-dir` is used as a parent. When unset, `default_parent`
/// (typically the input file's directory) is used. Only when both are unavailable
/// does this fall back to the current working directory.
pub fn process_dir(conf_parent: Option<PathBuf>, default_parent: Option<PathBuf>) -> PathBuf {
    static SUBDIR: LazyLock<String> = LazyLock::new(|| {
        let mut subdir = String::from(".ab-av1-");
        subdir.extend(iter::repeat_with(fastrand::alphanumeric).take(12));
        subdir
    });

    let mut temp_dir = conf_parent
        .or(default_parent)
        .unwrap_or_else(|| env::current_dir().expect("current working directory"));
    temp_dir.push(&*SUBDIR);

    if !temp_dir.exists() {
        add(&temp_dir, TempKind::Keepable);
        std::fs::create_dir_all(&temp_dir).expect("failed to create temp-dir");
    } else {
        add(&temp_dir, TempKind::Keepable);
    }

    temp_dir
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{env, fs};

    fn temp_path(label: &str) -> PathBuf {
        env::temp_dir().join(format!("ab-av1-temp-test-{}-{}", label, std::process::id()))
    }

    #[tokio::test]
    async fn add_and_clean_all_removes_not_keepable() {
        // setup
        let path = temp_path("not-keepable");
        fs::write(&path, b"temp").expect("write temp file");
        add(&path, TempKind::NotKeepable);

        // execute
        clean_all().await;

        // assert
        assert!(!path.exists(), "NotKeepable file should be deleted");
    }

    #[tokio::test]
    async fn clean_with_keep_keepables_preserves_keepable_files() {
        // setup
        let keep = temp_path("keepable");
        let drop = temp_path("drop");
        fs::write(&keep, b"keep").expect("write keepable");
        fs::write(&drop, b"drop").expect("write not-keepable");
        add(&keep, TempKind::Keepable);
        add(&drop, TempKind::NotKeepable);

        // execute
        clean(true).await;

        // assert
        assert!(keep.exists(), "Keepable file should survive clean(true)");
        assert!(!drop.exists(), "NotKeepable file should be deleted");

        // cleanup
        clean_all().await;
    }

    #[tokio::test]
    async fn unadd_prevents_later_deletion() {
        // setup
        let path = temp_path("unadded");
        fs::write(&path, b"stay").expect("write file");
        add(&path, TempKind::NotKeepable);

        // execute
        let removed = unadd(&path);
        clean_all().await;

        // assert
        assert!(removed);
        assert!(path.exists(), "unadded file must not be deleted");
        let _ = fs::remove_file(path);
    }

    #[test]
    fn default_temp_dir_uses_input_directory_ab_kgc_11() {
        // setup
        let input_dir = env::temp_dir().join("ab-av1-temp-test-input");
        fs::create_dir_all(&input_dir).expect("create input dir");
        let input = input_dir.join("clip.mkv");
        let cwd = env::current_dir().expect("cwd");

        // execute
        let temp_dir = process_dir(None, input.parent().map(Path::to_path_buf));

        // assert
        assert!(
            temp_dir.starts_with(&input_dir),
            "default temp dir should be under input directory, got {}",
            temp_dir.display()
        );
        assert!(
            !temp_dir.starts_with(&cwd) || input_dir.starts_with(&cwd),
            "default temp dir must not prefer cwd over input directory"
        );
        assert!(
            temp_dir
                .file_name()
                .and_then(|n| n.to_str())
                .is_some_and(|n| n.starts_with(".ab-av1-")),
            "temp dir should use the per-run subdirectory"
        );
    }

    // ab-kgc.65: leftover process dirs from interrupted runs must be tracked for cleanup
    #[tokio::test]
    async fn preexisting_process_dir_is_tracked_for_cleanup() {
        // setup — simulate a previous run leaving the per-process temp dir on disk
        let parent = temp_path("preexist-parent");
        fs::create_dir_all(&parent).expect("create parent");
        let run_dir = process_dir(Some(parent.clone()), None);
        clean_all().await;
        fs::create_dir_all(&run_dir).expect("recreate leftover process dir");

        // execute — new run reuses the same process dir path
        let run_dir_again = process_dir(Some(parent.clone()), None);
        assert_eq!(run_dir, run_dir_again);

        // assert — full cleanup must remove the pre-existing directory
        clean(false).await;
        assert!(
            !run_dir.exists(),
            "process_dir must register pre-existing run directories for cleanup"
        );
    }

    // ab-kgc.66: nested temp dirs must not leave orphaned files when only the parent is registered
    #[tokio::test]
    async fn clean_all_removes_nested_temp_directory_contents() {
        // setup
        let parent = temp_path("nested-parent");
        fs::create_dir_all(&parent).expect("create parent");
        let nested = parent.join("inner.mkv");
        fs::write(&nested, b"nested").expect("write nested file");
        add(&parent, TempKind::NotKeepable);

        // execute
        clean_all().await;

        // assert
        assert!(
            !nested.exists(),
            "nested files must be removed with their temp dir"
        );
        assert!(!parent.exists(), "registered temp dir must be removed");
    }

    #[test]
    fn explicit_temp_dir_overrides_input_directory_ab_kgc_11() {
        // setup
        let explicit = env::temp_dir().join("ab-av1-explicit-temp");
        fs::create_dir_all(&explicit).expect("create explicit temp dir");
        let input = env::temp_dir().join("ab-av1-temp-test-input/clip.mkv");

        // execute
        let temp_dir = process_dir(
            Some(explicit.clone()),
            input.parent().map(Path::to_path_buf),
        );

        // assert
        assert!(
            temp_dir.starts_with(&explicit),
            "explicit --temp-dir should win, got {}",
            temp_dir.display()
        );
    }
}
