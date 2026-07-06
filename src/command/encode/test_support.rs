use super::Args;
use crate::{
    command::args::{Encode, EncodeToOutput},
    ffprobe::Ffprobe,
};
use std::{env, fs, path::PathBuf, sync::Arc, time::Duration};

pub(crate) mod test_hooks {
    use std::cell::RefCell;

    thread_local! {
        static FIXTURE: RefCell<Option<&'static str>> = const { RefCell::new(None) };
    }

    pub fn set_fixture(name: &'static str) {
        FIXTURE.with(|f| *f.borrow_mut() = Some(name));
    }

    pub fn clear() {
        FIXTURE.with(|f| *f.borrow_mut() = None);
    }

    pub fn fixture() -> Option<&'static str> {
        FIXTURE.with(|f| *f.borrow())
    }
}

pub(crate) fn test_ffmpeg_stream(
    fixture: &'static str,
) -> anyhow::Result<crate::process::FfmpegOutStream> {
    use crate::process::managed::ManagedProcess;
    use tokio::process::Command;

    const FIXTURE_ENV: &str = "AB_AV1_MANAGED_PROCESS_FIXTURE";
    const FIXTURE_TEST: &str = "process::managed::tests::managed_process_fixture_child";

    let mut cmd = Command::new(env::current_exe().expect("current test executable"));
    cmd.arg("--exact")
        .arg(FIXTURE_TEST)
        .arg("--nocapture")
        .env(FIXTURE_ENV, fixture);
    let enc = ManagedProcess::spawn("ffmpeg encode fixture", cmd)?;
    Ok(crate::process::FfmpegOut::stream(
        enc,
        "ffmpeg encode fixture",
        fixture.into(),
    ))
}

pub struct FixtureGuard;

impl FixtureGuard {
    pub fn set(name: &'static str) -> Self {
        test_hooks::set_fixture(name);
        Self
    }
}

impl Drop for FixtureGuard {
    fn drop(&mut self) {
        test_hooks::clear();
    }
}

pub fn test_probe(max_audio_channels: Option<i64>) -> Ffprobe {
    Ffprobe {
        duration: Ok(Duration::from_secs(120)),
        has_audio: max_audio_channels.is_some(),
        max_audio_channels,
        fps: Ok(24.0),
        resolution: Some((1920, 1080)),
        is_image: false,
        pix_fmt: Some("yuv420p".into()),
    }
}

pub fn arc_probe(max_audio_channels: Option<i64>) -> Arc<Ffprobe> {
    Arc::new(test_probe(max_audio_channels))
}

pub fn temp_input(scope: &str, label: &str) -> PathBuf {
    let path = env::temp_dir().join(format!(
        "ab-av1-encode-{scope}-test-{label}-{}",
        std::process::id()
    ));
    fs::write(&path, b"input-bytes").expect("write temp input");
    path
}

pub fn encode_args(input: PathBuf, output: Option<PathBuf>) -> Args {
    Args {
        args: Encode {
            encoder: "libsvtav1".parse().unwrap(),
            input,
            vfilter: None,
            pix_format: None,
            preset: None,
            keyint: None,
            scd: None,
            svt_args: vec![],
            enc_args: vec![],
            enc_input_args: vec![],
        },
        crf: 32.0,
        encode: EncodeToOutput {
            output,
            audio_codec: None,
            downmix_to_stereo: false,
            video_only: false,
            overwrite_input: false,
        },
    }
}
