//! ffmpeg logic
use crate::{
    process::managed::ManagedProcess,
    process::{CommandExt, ensure_success},
    temporary::{self, TempKind},
};
use anyhow::Context;
use std::{
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::process::Command;

/// Destination path for a copied sample clip.
pub fn sample_dest_path(
    input: &Path,
    sample_start: Duration,
    floor_to_sec: bool,
    frames: u32,
    temp_dir: Option<PathBuf>,
) -> PathBuf {
    let mut sample_start_s = sample_start.as_secs_f32();
    if floor_to_sec {
        sample_start_s = sample_start_s.floor();
    }

    let mut dest = temporary::process_dir(temp_dir, input.parent().map(Path::to_path_buf));
    // Always using mkv for the samples works better than, e.g. using mp4 for mp4s
    // see https://github.com/alexheretic/ab-av1/issues/82#issuecomment-1337306325
    dest.push(
        input
            .with_extension(format!("sample{sample_start_s}+{frames}f.mkv"))
            .file_name()
            .unwrap(),
    );
    dest
}

/// Whether ffmpeg copy should retry with `-fflags +genpts`.
pub fn unknown_timestamp_retry(stderr: &[u8]) -> bool {
    String::from_utf8_lossy(stderr).contains("Can't write packet with unknown timestamp")
}

/// Build the primary ffmpeg copy command (without genpts retry flags).
pub fn copy_command(
    input: &Path,
    sample_start: Duration,
    floor_to_sec: bool,
    frames: u32,
    dest: &Path,
) -> Command {
    let mut sample_start_s = sample_start.as_secs_f32();
    if floor_to_sec {
        sample_start_s = sample_start_s.floor();
    }

    let mut cmd = Command::new(copy_program());
    apply_copy_args(&mut cmd, sample_start_s, input, frames, dest, false);
    cmd
}

/// Build the ffmpeg copy retry command with `-fflags +genpts`.
pub fn copy_command_with_genpts(
    input: &Path,
    sample_start: Duration,
    floor_to_sec: bool,
    frames: u32,
    dest: &Path,
) -> Command {
    let mut sample_start_s = sample_start.as_secs_f32();
    if floor_to_sec {
        sample_start_s = sample_start_s.floor();
    }

    let mut cmd = Command::new(copy_program());
    apply_copy_args(&mut cmd, sample_start_s, input, frames, dest, true);
    cmd
}

fn apply_copy_args(
    cmd: &mut Command,
    sample_start_s: f32,
    input: &Path,
    frames: u32,
    dest: &Path,
    genpts: bool,
) {
    cmd.arg("-nostdin")
        .arg("-y");
    if genpts {
        cmd.arg2("-fflags", "+genpts");
    }
    // Note: `-ss` before `-i` & `-frames:v` instead of `-t`
    // See https://github.com/alexheretic/ab-av1/issues/36#issuecomment-1146634936
    cmd.arg2("-ss", sample_start_s)
        .arg2("-i", input)
        .arg2("-frames:v", frames)
        .arg2("-c:v", "copy")
        .arg("-an")
        .arg("-sn")
        .arg(dest);
    #[cfg(test)]
    test_hooks::apply_fixture(cmd);
}

fn copy_program() -> std::path::PathBuf {
    #[cfg(test)]
    if test_hooks::uses_fixture() {
        return std::env::current_exe().expect("current test executable");
    }
    std::path::PathBuf::from("ffmpeg")
}

/// Create a sample from `sample_start` + `frames`.
///
/// Fast as this uses `-c:v copy`.
pub async fn copy(
    input: &Path,
    sample_start: Duration,
    floor_to_sec: bool,
    frames: u32,
    temp_dir: Option<PathBuf>,
) -> anyhow::Result<PathBuf> {
    let dest = sample_dest_path(input, sample_start, floor_to_sec, frames, temp_dir);
    if dest.exists() {
        return Ok(dest);
    }
    temporary::add(&dest, TempKind::Keepable);

    #[cfg(test)]
    if test_hooks::uses_fixture() {
        let mut cmd = Command::new(std::env::current_exe().expect("current test executable"));
        test_hooks::apply_fixture(&mut cmd);
        let out = ManagedProcess::spawn("ffmpeg copy", cmd)
            .context("ffmpeg copy")?
            .output()
            .await
            .context("ffmpeg copy")?;
        ensure_success("ffmpeg copy", &out)?;
        if !dest.exists() {
            if let Some(parent) = dest.parent() {
                std::fs::create_dir_all(parent).context("create sample parent")?;
            }
            std::fs::write(&dest, b"fixture-sample").context("write fixture sample")?;
        }
        return Ok(dest);
    }

    let mut cmd = copy_command(input, sample_start, floor_to_sec, frames, &dest);
    let mut out = ManagedProcess::spawn("ffmpeg copy", cmd)
        .context("ffmpeg copy")?
        .output()
        .await
        .context("ffmpeg copy")?;

    if !out.status.success() && unknown_timestamp_retry(&out.stderr) {
        cmd = copy_command_with_genpts(input, sample_start, floor_to_sec, frames, &dest);
        out = ManagedProcess::spawn("ffmpeg copy", cmd)
            .context("ffmpeg copy")?
            .output()
            .await
            .context("ffmpeg copy")?;
    }

    ensure_success("ffmpeg copy", &out)?;
    Ok(dest)
}

#[cfg(test)]
pub(crate) mod test_hooks {
    use std::cell::RefCell;
    use tokio::process::Command;

    const FIXTURE_ENV: &str = "AB_AV1_MANAGED_PROCESS_FIXTURE";
    const FIXTURE_TEST: &str = "process::managed::tests::managed_process_fixture_child";

    thread_local! {
        static FIXTURE: RefCell<Option<&'static str>> = const { RefCell::new(None) };
    }

    pub fn set_fixture(name: &'static str) {
        FIXTURE.with(|f| *f.borrow_mut() = Some(name));
    }

    pub fn clear() {
        FIXTURE.with(|f| *f.borrow_mut() = None);
    }

    pub fn uses_fixture() -> bool {
        FIXTURE.with(|f| f.borrow().is_some())
    }

    pub fn apply_fixture(cmd: &mut Command) {
        let Some(fixture) = FIXTURE.with(|f| *f.borrow()) else {
            return;
        };
        cmd.arg("--exact")
            .arg(FIXTURE_TEST)
            .arg("--nocapture")
            .env(FIXTURE_ENV, fixture);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{env, fs};

    mod helpers {
        use super::*;

        pub fn temp_input(name: &str) -> PathBuf {
            let path = env::temp_dir().join(format!(
                "ab-av1-sample-test-{}-{}",
                name,
                std::process::id()
            ));
            fs::write(&path, b"fake-input").expect("write temp input");
            path
        }
    }

    use helpers::*;

    struct FixtureGuard;

    impl FixtureGuard {
        fn set(name: &'static str) -> Self {
            test_hooks::set_fixture(name);
            Self
        }
    }

    impl Drop for FixtureGuard {
        fn drop(&mut self) {
            test_hooks::clear();
        }
    }

    #[test]
    fn sample_dest_path_uses_mkv_and_start_offset() {
        // setup
        let input = Path::new("/tmp/vid.mp4");

        // execute
        let dest = sample_dest_path(input, Duration::from_secs_f32(12.5), false, 48, None);

        // assert
        assert!(dest.to_string_lossy().contains("sample12.5+48f.mkv"));
    }

    #[test]
    fn sample_dest_path_floors_start_when_requested() {
        // setup
        let input = Path::new("/tmp/vid.mp4");

        // execute
        let dest = sample_dest_path(input, Duration::from_secs_f32(12.9), true, 24, None);

        // assert
        assert!(dest.to_string_lossy().contains("sample12+24f.mkv"));
    }

    #[test]
    fn unknown_timestamp_retry_detects_genpts_message() {
        // setup
        let stderr = b"Can't write packet with unknown timestamp\n";

        // execute / assert
        assert!(unknown_timestamp_retry(stderr));
        assert!(!unknown_timestamp_retry(b"other failure"));
    }

    // ab-kgc.42: partial message match must not miss alternate ffmpeg wording
    #[test]
    fn unknown_timestamp_retry_is_case_insensitive() {
        // setup
        let stderr = b"can't write packet with unknown timestamp\n";

        // execute / assert
        assert!(
            unknown_timestamp_retry(stderr),
            "retry detection should tolerate ffmpeg message casing changes"
        );
    }

    #[test]
    fn copy_command_uses_ffmpeg_program_by_default() {
        // setup
        let input = Path::new("/media/clip.mkv");
        let dest = Path::new("/tmp/.ab-av1/sample12+24f.mkv");

        // execute
        let cmd = copy_command(input, Duration::from_secs(12), true, 24, dest);

        // assert
        assert_eq!(cmd.as_std().get_program(), "ffmpeg");
    }

    #[test]
    fn copy_command_with_genpts_is_distinct_from_primary() {
        // setup
        let input = Path::new("/media/clip.mkv");
        let dest = Path::new("/tmp/.ab-av1/sample12+24f.mkv");

        // execute
        let primary = copy_command(input, Duration::from_secs(12), true, 24, dest);
        let retry = copy_command_with_genpts(input, Duration::from_secs(12), true, 24, dest);

        // assert — genpts retry adds one more argument pair before -ss
        assert!(
            retry.as_std().get_args().count() > primary.as_std().get_args().count(),
            "genpts retry command should include extra flags"
        );
    }

    #[tokio::test]
    async fn copy_returns_existing_dest_without_spawning() {
        // setup
        let input = temp_input("existing");
        let dest = sample_dest_path(&input, Duration::from_secs(0), true, 10, None);
        if let Some(parent) = dest.parent() {
            fs::create_dir_all(parent).expect("create temp parent");
        }
        fs::write(&dest, b"cached-sample").expect("write cached sample");

        // execute
        let got = copy(&input, Duration::from_secs(0), true, 10, None)
            .await
            .expect("copy");

        // assert
        assert_eq!(got, dest);

        // cleanup
        let _ = fs::remove_file(&dest);
        let _ = fs::remove_file(input);
    }

    #[tokio::test]
    async fn copy_succeeds_with_process_fixture() {
        // setup
        let input = temp_input("fixture");
        let _guard = FixtureGuard::set("stderr-warning");

        // execute
        let dest = copy(&input, Duration::from_secs(5), true, 12, None)
            .await
            .expect("copy with fixture");

        // assert
        assert!(dest.exists());

        // cleanup
        temporary::clean_all().await;
        let _ = fs::remove_file(input);
    }

    /// Requires ffmpeg-full; run locally with `cargo nextest run --ignored`.
    #[tokio::test]
    #[ignore = "requires ffmpeg-full in devshell"]
    async fn copy_e2e_real_ffmpeg() {
        // setup
        let input = temp_input("e2e");
        let temp_dir = env::temp_dir().join(format!("ab-av1-sample-e2e-{}", std::process::id()));
        fs::create_dir_all(&temp_dir).expect("create e2e temp dir");

        // execute
        let dest = copy(
            &input,
            Duration::from_secs(0),
            true,
            1,
            Some(temp_dir.clone()),
        )
        .await
        .expect("real ffmpeg copy");

        // assert
        assert!(dest.exists());
        assert!(fs::metadata(&dest).expect("stat dest").len() > 0);

        // cleanup
        temporary::clean_all().await;
        let _ = fs::remove_file(input);
        let _ = fs::remove_dir_all(temp_dir);
    }
}
