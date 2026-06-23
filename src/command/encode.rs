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

    let output_cleanup = OutputCleanup::arm(&output);
    let mut enc = ffmpeg::encode(enc_args, &output, has_audio, audio_codec, stereo_downmix)?;
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

    output_cleanup.commit();

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
        _ => "mkv",
    }
}

/// E.g. vid.mkv -> "vid.av1.mkv"
pub fn default_output_name(input: &Path, encoder: &Encoder, is_image: bool) -> PathBuf {
    let pre = ffmpeg::pre_extension_name(encoder.as_str());
    let ext = default_output_ext(input, encoder, is_image);
    input.with_extension(format!("{pre}.{ext}"))
}

struct OutputCleanup {
    path: PathBuf,
}

impl OutputCleanup {
    fn arm(path: impl AsRef<Path>) -> Self {
        let path = path.as_ref().to_owned();
        // From this point on ffmpeg may create or partially write the output,
        // so clean it up on failure.
        temporary::add(&path, TempKind::NotKeepable);
        Self { path }
    }

    fn commit(self) {
        // Successful encode: the output is user-owned, so don't delete it.
        temporary::unadd(&self.path);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{str::FromStr, sync::LazyLock};

    static TEST_MUTEX: LazyLock<std::sync::Mutex<()>> = LazyLock::new(<_>::default);

    fn test_guard() -> std::sync::MutexGuard<'static, ()> {
        TEST_MUTEX
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn test_path(name: &str) -> PathBuf {
        let mut path = std::env::temp_dir();
        path.push(format!("ab-av1-{}-{name}", std::process::id(),));
        path
    }

    fn test_probe() -> Ffprobe {
        Ffprobe {
            duration: Ok(Duration::from_secs(1)),
            has_audio: true,
            max_audio_channels: Some(6),
            fps: Ok(24.0),
            resolution: Some((1920, 1080)),
            is_image: false,
            pix_fmt: None,
        }
    }

    fn encode_args(input: PathBuf, output: PathBuf) -> Args {
        Args {
            args: args::Encode {
                encoder: Encoder::from_str("libsvtav1").expect("encoder"),
                input,
                vfilter: None,
                pix_format: None,
                preset: None,
                keyint: None,
                scd: None,
                svt_args: Vec::new(),
                enc_args: Vec::new(),
                enc_input_args: Vec::new(),
            },
            crf: 32.0,
            encode: args::EncodeToOutput {
                output: Some(output),
                audio_codec: Some("copy".into()),
                downmix_to_stereo: true,
                video_only: false,
                overwrite_input: false,
            },
        }
    }

    #[tokio::test(flavor = "current_thread")]
    async fn committed_output_cleanup_does_not_delete_output() {
        let _guard = test_guard();
        temporary::clean_all().await;

        let output = test_path("committed-output.mkv");
        fs::write(&output, b"encoded output")
            .await
            .expect("write output");

        OutputCleanup::arm(&output).commit();
        temporary::clean_all().await;

        assert_eq!(
            fs::read(&output).await.expect("committed output survives"),
            b"encoded output"
        );

        _ = fs::remove_file(output).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn uncommitted_output_cleanup_deletes_partial_output() {
        let _guard = test_guard();
        temporary::clean_all().await;

        let output = test_path("partial-output.mkv");
        fs::write(&output, b"partial output")
            .await
            .expect("write partial output");

        _ = OutputCleanup::arm(&output);
        temporary::clean_all().await;

        assert!(
            !fs::try_exists(&output).await.expect("check partial output"),
            "uncommitted partial output should be deleted"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn same_file_validation_failure_does_not_delete_input() {
        let _guard = test_guard();
        temporary::clean_all().await;

        let input = test_path("same-file-input.mkv");
        fs::write(&input, b"input").await.expect("write input");

        let bar = ProgressBar::hidden();
        let mut args = encode_args(input.clone(), input.clone());
        args.encode.audio_codec = None;
        args.encode.downmix_to_stereo = false;

        let result = run(args, Arc::new(test_probe()), &bar).await;
        temporary::clean_all().await;

        assert!(
            result
                .expect_err("same file validation should fail")
                .to_string()
                .contains("Input and Output are specified as the same file")
        );
        assert_eq!(fs::read(&input).await.expect("input survives"), b"input");

        _ = fs::remove_file(input).await;
    }

    #[tokio::test(flavor = "current_thread")]
    async fn validation_failure_does_not_delete_existing_output() {
        let _guard = test_guard();
        temporary::clean_all().await;

        let input = test_path("validation-input.mkv");
        let output = test_path("validation-output.mkv");
        fs::write(&input, b"input").await.expect("write input");
        fs::write(&output, b"existing output")
            .await
            .expect("write existing output");

        let bar = ProgressBar::hidden();
        let result = run(
            encode_args(input.clone(), output.clone()),
            Arc::new(test_probe()),
            &bar,
        )
        .await;
        temporary::clean_all().await;

        assert!(
            result
                .expect_err("validation should fail")
                .to_string()
                .contains("--stereo-downmix cannot be used with --acodec copy")
        );
        assert_eq!(
            fs::read(&output).await.expect("existing output survives"),
            b"existing output"
        );

        _ = fs::remove_file(input).await;
        _ = fs::remove_file(output).await;
    }
}
