use crate::{
    command::{
        PROGRESS_CHARS, SmallDuration,
        args::{self, Encoder},
    },
    console_ext::style,
    ffmpeg,
    ffprobe::{self, Ffprobe},
};
use clap::Parser;
use console::style;
use indicatif::{ProgressBar, ProgressStyle};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::Duration,
};
use tokio_stream::StreamExt;

mod error;
mod lifecycle;
mod plan;
mod preflight;
mod progress;
mod report;
mod running;
mod sink;
mod spawner;

#[cfg(test)]
mod test_support;

#[cfg(test)]
pub(crate) use test_support::test_hooks;

pub use error::EncodePlanError;
pub use lifecycle::{CompletedOutput, PartialOutput, PlannedOutput};
pub use plan::EncodePlan;
pub use preflight::{audio_config, resolve_output};
pub use progress::StreamSizes;
pub use report::{EncodeMetrics, FinishedEncode};
pub use sink::ProgressSink;
pub use spawner::{EncodeSpawner, FfmpegSpawner};

/// Invoke ffmpeg to encode a video or image.
#[derive(Parser)]
#[group(skip)]
pub struct Args {
    #[clap(flatten)]
    pub args: args::Encode,

    /// Encoder constant rate factor (1-63). Lower means better quality.
    #[arg(long)]
    pub crf: f32,

    #[clap(flatten)]
    pub encode: args::EncodeToOutput,
}

pub async fn encode(args: Args) -> anyhow::Result<()> {
    let bar = ProgressBar::new(1).with_style(
        ProgressStyle::default_bar()
            .template("{spinner:.cyan.bold} {elapsed_precise:.bold} {wide_bar:.cyan/blue} ({msg}eta {eta})")?
            .progress_chars(PROGRESS_CHARS)
    );
    bar.enable_steady_tick(Duration::from_millis(100));

    let probe = ffprobe::probe(&args.args.input);
    run(args, probe.into(), &bar).await
}

pub async fn run(args: Args, probe: Arc<Ffprobe>, bar: &ProgressBar) -> anyhow::Result<()> {
    #[cfg(test)]
    {
        run_with_spawner(args, probe, bar, &spawner::ThreadLocalFixtureSpawner).await
    }
    #[cfg(not(test))]
    {
        run_with_spawner(args, probe, bar, &FfmpegSpawner).await
    }
}

pub(crate) async fn run_with_spawner(
    args: Args,
    probe: Arc<Ffprobe>,
    bar: &ProgressBar,
    spawner: &impl EncodeSpawner,
) -> anyhow::Result<()> {
    let plan = EncodePlan::build(args, probe).map_err(EncodePlanError::into_anyhow)?;

    if plan.defaulting_output() {
        let out = shell_escape::escape(plan.output_path().display().to_string().into());
        bar.println(style!("Encoding {out}").dim().to_string());
    }
    bar.set_message("encoding, ");
    if let Ok(d) = &plan.probe().duration {
        bar.set_length(d.as_micros_u64().max(1));
    }

    let run = running::run_encode(plan, bar, spawner).await?;
    let finished = FinishedEncode::load(run.input, run.output, run.stream_sizes).await?;
    finished.render_summary(&mut std::io::stderr())?;
    Ok(())
}

/// * vid.mp4 -> "mp4"
/// * vid.??? -> "mkv"
/// * image.??? -> "avif"
pub fn default_output_ext(input: &Path, encoder: &Encoder, is_image: bool) -> &'static str {
    if is_image {
        return encoder.default_image_ext();
    }
    match input.extension().and_then(|e| e.to_str()) {
        Some("mp4") => "mp4",
        Some("webm") => "webm",
        Some("mov") => "mov",
        _ => "mkv",
    }
}

/// E.g. vid.mkv -> "vid.av1.mkv"
pub fn default_output_name(input: &Path, encoder: &Encoder, is_image: bool) -> PathBuf {
    let pre = ffmpeg::pre_extension_name(encoder.as_str());
    let ext = default_output_ext(input, encoder, is_image);
    input.with_extension(format!("{pre}.{ext}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::temporary;
    use std::{env, fs};
    use test_case::test_case;
    use test_support::{arc_probe, encode_args, temp_input};
    use serial_test::serial;
    use spawner::FixtureSpawner;

    // ab-kgc.89: default output extension must preserve input container for webm/mov
    #[test_case("clip.mp4", false, "mp4"; "video mp4 keeps mp4")]
    #[test_case("clip.mkv", false, "mkv"; "video mkv keeps mkv")]
    #[test_case("clip.webm", false, "webm"; "video webm keeps webm")]
    #[test_case("clip.mov", false, "mov"; "video mov keeps mov")]
    #[test_case("still.png", true, "avif"; "image uses encoder default")]
    fn default_output_ext_cases(input_name: &str, is_image: bool, expected: &str) {
        // setup
        let input = Path::new(input_name);
        let encoder: Encoder = "libsvtav1".parse().unwrap();

        // execute
        let ext = default_output_ext(input, &encoder, is_image);

        // assert
        assert_eq!(ext, expected);
    }

    #[test]
    fn default_output_name_adds_encoder_prefix() {
        // setup
        let input = Path::new("movie.mkv");
        let encoder: Encoder = "libsvtav1".parse().unwrap();

        // execute
        let out = default_output_name(input, &encoder, false);

        // assert
        assert_eq!(out, Path::new("movie.av1.mkv"));
    }

    #[serial]
    #[tokio::test]
    async fn run_cleans_temp_output_after_encode_failure() {
        // setup
        let input = temp_input("run", "encode-fail");
        let output =
            env::temp_dir().join(format!("ab-av1-encode-fail-out-{}.mkv", std::process::id()));
        let args = encode_args(input.clone(), Some(output.clone()));
        let bar = ProgressBar::new(1);
        let spawner = FixtureSpawner::new("stderr-badness-exit-7");

        // execute
        let err = run_with_spawner(args, arc_probe(Some(6)), &bar, &spawner)
            .await
            .expect_err("expected encode failure");

        // assert
        assert!(!err.to_string().is_empty());
        assert!(
            !output.exists(),
            "failed encode must remove temporary output file"
        );

        // cleanup
        temporary::clean_all().await;
        let _ = fs::remove_file(input);
    }

    #[tokio::test]
    async fn run_rejects_same_input_and_output_without_overwrite() {
        // setup
        let input = temp_input("run", "same-io");
        let args = encode_args(input.clone(), Some(input.clone()));
        let bar = ProgressBar::new(1);

        // execute
        let err = run(args, arc_probe(Some(6)), &bar)
            .await
            .expect_err("expected same-file error");

        // assert
        assert!(err.to_string().contains("same file"));

        // cleanup
        let _ = fs::remove_file(input);
    }

    #[serial]
    #[tokio::test]
    async fn run_rejects_stereo_downmix_with_copy_codec() {
        // setup
        let input = temp_input("run", "downmix-copy");
        let output = env::temp_dir().join(format!("ab-av1-encode-out-{}", std::process::id()));
        let mut args = encode_args(input.clone(), Some(output.clone()));
        args.encode.downmix_to_stereo = true;
        args.encode.audio_codec = Some("copy".into());
        let bar = ProgressBar::new(1);

        // execute
        let err = run(args, arc_probe(Some(6)), &bar)
            .await
            .expect_err("expected downmix/copy error");

        // assert
        assert!(err.to_string().contains("--stereo-downmix"));
        assert!(
            !temporary::unadd(&output),
            "validation failure must not register output for cleanup"
        );

        // cleanup
        let _ = fs::remove_file(input);
    }

    #[serial]
    #[tokio::test]
    async fn run_completes_with_process_fixture() {
        // setup
        let input = temp_input("run", "fixture-run");
        let output = env::temp_dir().join(format!(
            "ab-av1-encode-fixture-out-{}.mkv",
            std::process::id()
        ));
        let args = encode_args(input.clone(), Some(output.clone()));
        let bar = ProgressBar::new(120);
        let spawner = FixtureSpawner::new("stderr-ffmpeg-progress");

        // execute
        run_with_spawner(args, arc_probe(Some(6)), &bar, &spawner).await.expect("encode run");

        // assert
        assert!(output.exists());

        // cleanup
        temporary::clean_all().await;
        let _ = fs::remove_file(input);
        let _ = fs::remove_file(output);
    }

    #[serial]
    #[tokio::test]
    async fn run_completes_with_video_only_and_downmix() {
        // setup
        let input = temp_input("run", "video-only-downmix");
        let output =
            env::temp_dir().join(format!("ab-av1-encode-vo-out-{}.mkv", std::process::id()));
        let mut args = encode_args(input.clone(), Some(output.clone()));
        args.encode.video_only = true;
        args.encode.downmix_to_stereo = true;
        let bar = ProgressBar::new(120);
        let spawner = FixtureSpawner::new("stderr-ffmpeg-progress");

        // execute
        run_with_spawner(args, arc_probe(Some(6)), &bar, &spawner).await.expect("encode run");

        // assert
        assert!(output.exists());

        // cleanup
        temporary::clean_all().await;
        let _ = fs::remove_file(input);
        let _ = fs::remove_file(output);
    }
}
