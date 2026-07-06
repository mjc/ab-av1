use crate::{
    command::{
        PROGRESS_CHARS, SmallDuration,
        args::{self, Encoder},
    },
    console_ext::style,
    ffmpeg,
    ffprobe::{self, Ffprobe},
    log::ProgressLogger,
    process::FfmpegOut,
    temporary::{self, TempKind},
};
use clap::Parser;
use console::style;
use indicatif::{HumanBytes, ProgressBar, ProgressStyle};
use log::info;
use same_file::is_same_file;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::fs;
use tokio_stream::StreamExt;

#[cfg(test)]
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

#[cfg(test)]
fn test_ffmpeg_stream(fixture: &'static str) -> anyhow::Result<crate::process::FfmpegOutStream> {
    use crate::process::managed::ManagedProcess;
    use std::env;
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

pub async fn run(
    Args {
        args,
        crf,
        encode:
            args::EncodeToOutput {
                output,
                audio_codec,
                downmix_to_stereo,
                video_only,
                overwrite_input,
            },
    }: Args,
    probe: Arc<Ffprobe>,
    bar: &ProgressBar,
) -> anyhow::Result<()> {
    let defaulting_output = output.is_none();
    let output =
        output.unwrap_or_else(|| default_output_name(&args.input, &args.encoder, probe.is_image));

    anyhow::ensure!(
        overwrite_input || !is_same_file(&output, &args.input).unwrap_or(false),
        "Input and Output are specified as the same file. Not proceeding. \
         Pass in `--overwrite-input` to allow this."
    );

    // output is temporary until encoding has completed successfully
    temporary::add(&output, TempKind::NotKeepable);

    if defaulting_output {
        let out = shell_escape::escape(output.display().to_string().into());
        bar.println(style!("Encoding {out}").dim().to_string());
    }
    bar.set_message("encoding, ");

    let mut enc_args = args.to_ffmpeg_args(crf, &probe)?;
    enc_args.video_only = video_only;
    let has_audio = probe.has_audio;
    if let Ok(d) = &probe.duration {
        bar.set_length(d.as_micros_u64().max(1));
    }

    // only downmix if achannels > 3
    let stereo_downmix = downmix_to_stereo && probe.max_audio_channels.is_some_and(|c| c > 3);
    let audio_codec = audio_codec.as_deref();
    if stereo_downmix && audio_codec == Some("copy") {
        anyhow::bail!("--stereo-downmix cannot be used with --acodec copy");
    }

    info!(
        "encoding {}",
        output.file_name().and_then(|n| n.to_str()).unwrap_or("")
    );

    let mut enc = {
        #[cfg(test)]
        if let Some(fixture) = test_hooks::fixture() {
            test_ffmpeg_stream(fixture)?
        } else {
            ffmpeg::encode(enc_args, &output, has_audio, audio_codec, stereo_downmix)?
        }
        #[cfg(not(test))]
        ffmpeg::encode(enc_args, &output, has_audio, audio_codec, stereo_downmix)?
    };
    let mut logger = ProgressLogger::new(module_path!(), Instant::now());
    let mut stream_sizes = None;
    while let Some(progress) = enc.next().await {
        match progress? {
            FfmpegOut::Progress { fps, time, .. } => {
                if fps > 0.0 {
                    bar.set_message(format!("{fps} fps, "));
                }
                if let Ok(d) = &probe.duration {
                    bar.set_position(time.as_micros_u64());
                    logger.update(*d, time, fps);
                }
            }
            FfmpegOut::StreamSizes {
                video,
                audio,
                subtitle,
                other,
            } => stream_sizes = Some((video, audio, subtitle, other)),
        }
    }
    enc.wait().await?; // ensure process has exited
    bar.finish();

    #[cfg(test)]
    if test_hooks::fixture().is_some() && !output.exists() {
        fs::write(&output, b"fixture-encoded").await?;
    }

    // successful encode, so don't delete it!
    temporary::unadd(&output);

    // print output info
    let output_size = fs::metadata(&output).await?.len();
    let output_percent = 100.0 * output_size as f64 / fs::metadata(&args.input).await?.len() as f64;
    let output_size = style(HumanBytes(output_size)).dim().bold();
    let output_percent = style!("{}%", output_percent.round()).dim().bold();
    eprint!(
        "{} {output_size} {}{output_percent}",
        style("Encoded").dim(),
        style("(").dim(),
    );
    if let Some((video, audio, subtitle, other)) = stream_sizes
        && (audio > 0 || subtitle > 0 || other > 0)
    {
        for (label, size) in [
            ("video:", video),
            ("audio:", audio),
            ("subs:", subtitle),
            ("other:", other),
        ] {
            if size > 0 {
                let size = style(HumanBytes(size)).dim();
                eprint!("{} {}{size}", style(",").dim(), style(label).dim(),);
            }
        }
    }
    eprintln!("{}", style(")").dim());

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
    use crate::{
        command::args::{Encode, EncodeToOutput},
        ffprobe::Ffprobe,
    };
    use std::{env, fs, sync::Arc, time::Duration};
    use test_case::test_case;

    mod helpers {
        use super::*;

        pub fn test_probe() -> Arc<Ffprobe> {
            Arc::new(Ffprobe {
                duration: Ok(Duration::from_secs(120)),
                has_audio: true,
                max_audio_channels: Some(6),
                fps: Ok(24.0),
                resolution: Some((1920, 1080)),
                is_image: false,
                pix_fmt: Some("yuv420p".into()),
            })
        }

        pub fn temp_input(label: &str) -> PathBuf {
            let path = env::temp_dir().join(format!(
                "ab-av1-encode-test-{}-{}",
                label,
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

    #[tokio::test]
    async fn run_cleans_temp_output_after_encode_failure() {
        // setup
        let input = temp_input("encode-fail");
        let output =
            env::temp_dir().join(format!("ab-av1-encode-fail-out-{}.mkv", std::process::id()));
        let args = encode_args(input.clone(), Some(output.clone()));
        let bar = ProgressBar::new(1);
        let _guard = FixtureGuard::set("stderr-badness-exit-7");

        // execute
        let err = run(args, test_probe(), &bar)
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
        let input = temp_input("same-io");
        let args = encode_args(input.clone(), Some(input.clone()));
        let bar = ProgressBar::new(1);

        // execute
        let err = run(args, test_probe(), &bar)
            .await
            .expect_err("expected same-file error");

        // assert
        assert!(err.to_string().contains("same file"));

        // cleanup
        let _ = fs::remove_file(input);
    }

    #[tokio::test]
    async fn run_rejects_stereo_downmix_with_copy_codec() {
        // setup
        let input = temp_input("downmix-copy");
        let output = env::temp_dir().join(format!("ab-av1-encode-out-{}", std::process::id()));
        let mut args = encode_args(input.clone(), Some(output));
        args.encode.downmix_to_stereo = true;
        args.encode.audio_codec = Some("copy".into());
        let bar = ProgressBar::new(1);

        // execute
        let err = run(args, test_probe(), &bar)
            .await
            .expect_err("expected downmix/copy error");

        // assert
        assert!(err.to_string().contains("--stereo-downmix"));

        // cleanup
        let _ = fs::remove_file(input);
    }

    #[tokio::test]
    async fn run_completes_with_process_fixture() {
        // setup
        let input = temp_input("fixture-run");
        let output = env::temp_dir().join(format!(
            "ab-av1-encode-fixture-out-{}.mkv",
            std::process::id()
        ));
        let args = encode_args(input.clone(), Some(output.clone()));
        let bar = ProgressBar::new(120);
        let _guard = FixtureGuard::set("stderr-ffmpeg-progress");

        // execute
        run(args, test_probe(), &bar).await.expect("encode run");

        // assert
        assert!(output.exists());

        // cleanup
        temporary::clean_all().await;
        let _ = fs::remove_file(input);
        let _ = fs::remove_file(output);
    }

    #[tokio::test]
    async fn run_completes_with_video_only_and_downmix() {
        // setup
        let input = temp_input("video-only-downmix");
        let output =
            env::temp_dir().join(format!("ab-av1-encode-vo-out-{}.mkv", std::process::id()));
        let mut args = encode_args(input.clone(), Some(output.clone()));
        args.encode.video_only = true;
        args.encode.downmix_to_stereo = true;
        let bar = ProgressBar::new(120);
        let _guard = FixtureGuard::set("stderr-ffmpeg-progress");

        // execute
        run(args, test_probe(), &bar).await.expect("encode run");

        // assert
        assert!(output.exists());

        // cleanup
        temporary::clean_all().await;
        let _ = fs::remove_file(input);
        let _ = fs::remove_file(output);
    }
}
