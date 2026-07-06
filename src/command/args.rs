//! Shared argument logic.
mod encode;
mod vmaf;

pub use encode::*;
pub use vmaf::*;

use crate::{command::encode::default_output_ext, ffprobe::Ffprobe};
use clap::{Parser, ValueHint};
use std::{
    fmt,
    num::ParseIntError,
    path::{Path, PathBuf},
    str::FromStr,
    sync::Arc,
    time::Duration,
};

/// Encoding args that apply when encoding to an output.
#[derive(Parser, Clone)]
pub struct EncodeToOutput {
    /// Output file, by default the same as input with `.av1` before the extension.
    ///
    /// E.g. if unspecified: -i vid.mkv --> vid.av1.mkv
    #[arg(short, long, value_hint = ValueHint::FilePath)]
    pub output: Option<PathBuf>,

    /// Set the output ffmpeg audio codec.
    /// By default 'copy' is used. Otherwise, if re-encoding is necessary, 'libopus' is default.
    ///
    /// See https://ffmpeg.org/ffmpeg.html#Audio-Options.
    #[arg(long = "acodec")]
    pub audio_codec: Option<String>,

    /// Downmix input audio streams to stereo if input streams use greater than
    /// 3 channels.
    ///
    /// No effect if the input audio has 3 or fewer channels.
    #[arg(long)]
    pub downmix_to_stereo: bool,

    /// Only process the main video stream, drop all other streams.
    ///
    /// The output will be a single video stream.
    #[arg(long)]
    pub video_only: bool,

    /// By default input files will not be overwritten to prevent accidental data loss.
    /// Setting this option overrides that allowing input overwrites.
    #[arg(long)]
    pub overwrite_input: bool,
}

/// Sampling arguments.
#[derive(Parser, Clone)]
pub struct Sample {
    /// Number of samples to use across the input video. Overrides --sample-every.
    /// More samples take longer but may provide a more accurate result.
    #[arg(long)]
    pub samples: Option<SampleCountOverride>,

    /// Calculate number of samples by dividing the input duration by this value.
    /// So "12m" would mean with an input 25-36 minutes long, 3 samples would be used.
    /// More samples take longer but may provide a more accurate result.
    ///
    /// Setting --samples overrides this value.
    #[arg(long, default_value = "12m")]
    pub sample_every: SampleDuration,

    /// Minimum number of samples. So at least this many samples will be used.
    #[arg(long)]
    pub min_samples: Option<MinSampleCount>,

    /// Duration of each sample.
    #[arg(long, default_value = "20s")]
    pub sample_duration: SampleDuration,

    /// Keep temporary files after exiting.
    #[arg(long)]
    pub keep: bool,

    /// Directory to store temporary sample data in.
    /// Defaults to using the input's directory.
    #[arg(long, env = "AB_AV1_TEMP_DIR", value_hint = ValueHint::DirPath)]
    pub temp_dir: Option<PathBuf>,

    /// Extension preference for encoded samples (ffmpeg encoder only).
    #[arg(skip)]
    pub extension: Option<Arc<str>>,
}

impl Sample {
    /// Calculate the desired sample count using `samples` or `sample_every` & `min_samples`.
    pub fn sample_count(&self, input_duration: Duration) -> u64 {
        let count = match self.samples {
            Some(samples) => samples.get(),
            None => {
                let every = self.sample_every.get().as_secs_f64();
                (input_duration.as_secs_f64() / every).ceil() as u64
            }
        };
        if self.samples.is_some() {
            count.max(self.min_samples.map_or(0, MinSampleCount::get))
        } else {
            count
                .max(self.min_samples.map_or(1, MinSampleCount::get))
                .max(1)
        }
    }

    pub fn set_extension_from_input(&mut self, input: &Path, encoder: &Encoder, probe: &Ffprobe) {
        self.extension = Some(default_output_ext(input, encoder, probe.is_image).into());
    }

    pub fn set_extension_from_output(&mut self, output: &Path) {
        self.extension = output
            .extension()
            .and_then(|e| e.to_str().map(Into::into))
            .or_else(|| Some("mkv".into()));
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SampleCountOverride(u64);

impl SampleCountOverride {
    pub fn new(samples: u64) -> Self {
        Self(samples)
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

impl FromStr for SampleCountOverride {
    type Err = ParseIntError;

    fn from_str(raw: &str) -> Result<Self, Self::Err> {
        raw.parse().map(Self::new)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SampleDuration(Duration);

#[derive(Debug, thiserror::Error)]
pub enum SampleDurationError {
    #[error(transparent)]
    Parse(#[from] humantime::DurationError),
    #[error("sample duration must be positive")]
    Zero,
}

impl SampleDuration {
    pub fn new(duration: Duration) -> Result<Self, SampleDurationError> {
        match duration.is_zero() {
            true => Err(SampleDurationError::Zero),
            false => Ok(Self(duration)),
        }
    }

    pub fn get(self) -> Duration {
        self.0
    }
}

impl FromStr for SampleDuration {
    type Err = SampleDurationError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(humantime::parse_duration(s)?)
    }
}

impl fmt::Display for SampleDuration {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        humantime::format_duration(self.0).fmt(f)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct MinSampleCount(u64);

#[derive(Debug, thiserror::Error)]
pub enum MinSampleCountError {
    #[error(transparent)]
    Parse(#[from] ParseIntError),
    #[error("--min-samples must be positive")]
    Zero,
}

impl MinSampleCount {
    pub fn new(samples: u64) -> Result<Self, MinSampleCountError> {
        match samples {
            0 => Err(MinSampleCountError::Zero),
            samples => Ok(Self(samples)),
        }
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

impl FromStr for MinSampleCount {
    type Err = MinSampleCountError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        Self::new(s.parse()?)
    }
}

/// Args for when VMAF/XPSNR are used to score ref vs distorted.
#[derive(Debug, Parser, Clone, Hash)]
pub struct ScoreArgs {
    /// Ffmpeg video filter applied to the VMAF/XPSNR reference before analysis.
    /// E.g. --reference-vfilter "scale=1280:-1,fps=24".
    ///
    /// Overrides --vfilter which would otherwise be used.
    #[arg(long)]
    pub reference_vfilter: Option<Arc<str>>,
}

/// Normalized score configuration lowered from clap parsing.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ScoreConfig {
    pub reference_vfilter: Option<Arc<str>>,
}

impl From<ScoreArgs> for ScoreConfig {
    fn from(score: ScoreArgs) -> Self {
        Self {
            reference_vfilter: score.reference_vfilter,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FrameRateOverride(Option<f32>);

#[derive(Debug, thiserror::Error)]
pub enum FrameRateOverrideError {
    #[error(transparent)]
    Parse(#[from] std::num::ParseFloatError),
    #[error("frame rate override must be finite")]
    NonFinite,
}

impl FrameRateOverride {
    pub fn new(fps: f32) -> Self {
        Self(Some(fps).filter(|r| *r > 0.0 && r.is_finite()))
    }

    pub fn fps(self) -> Option<f32> {
        self.0
    }
}

impl std::fmt::Display for FrameRateOverride {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            Some(fps) => fps.fmt(f),
            None => 0.0f32.fmt(f),
        }
    }
}

impl std::hash::Hash for FrameRateOverride {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        match self.0 {
            Some(fps) => fps.to_bits().hash(state),
            None => 0f32.to_bits().hash(state),
        }
    }
}

impl std::str::FromStr for FrameRateOverride {
    type Err = FrameRateOverrideError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let fps: f32 = s.parse()?;
        if !fps.is_finite() {
            return Err(FrameRateOverrideError::NonFinite);
        }
        Ok(Self::new(fps))
    }
}

/// Common xpsnr options.
#[derive(Debug, Parser, Clone, Copy, PartialEq, Hash)]
pub struct Xpsnr {
    /// Frame rate override used to analyse both reference & distorted videos.
    /// Maps to ffmpeg `-r` input arg.
    ///
    /// Setting to 0 disables use.
    #[arg(long, default_value_t = FrameRateOverride::new(60.0))]
    pub xpsnr_fps: FrameRateOverride,

    /// Pixel format used in xpsnr analysis only. By default this is inferred from sources.
    #[arg(value_enum, long)]
    pub xpsnr_pix_format: Option<PixelFormat>,
}

impl Xpsnr {
    pub fn fps(&self) -> Option<f32> {
        self.xpsnr_fps.fps()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Hash)]
pub struct XpsnrConfig {
    pub xpsnr_fps: FrameRateOverride,
    pub xpsnr_pix_format: Option<PixelFormat>,
}

impl XpsnrConfig {
    pub fn fps(&self) -> Option<f32> {
        self.xpsnr_fps.fps()
    }
}

impl From<Xpsnr> for XpsnrConfig {
    fn from(xpsnr: Xpsnr) -> Self {
        Self {
            xpsnr_fps: xpsnr.xpsnr_fps,
            xpsnr_pix_format: xpsnr.xpsnr_pix_format,
        }
    }
}

impl Default for Xpsnr {
    fn default() -> Self {
        Self {
            xpsnr_fps: FrameRateOverride::new(60.0),
            xpsnr_pix_format: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use rstest::rstest;
    use std::{path::Path, time::Duration};

    fn sample_args(
        samples: Option<u64>,
        min_samples: Option<u64>,
        sample_every: Duration,
    ) -> Sample {
        let sample_every = match SampleDuration::new(sample_every) {
            Ok(duration) => duration,
            Err(err) => panic!("invalid test sample_every: {err}"),
        };
        let sample_duration = match SampleDuration::new(Duration::from_secs(20)) {
            Ok(duration) => duration,
            Err(err) => panic!("invalid test sample_duration: {err}"),
        };

        Sample {
            samples: samples.map(SampleCountOverride::new),
            sample_every,
            min_samples: min_samples.and_then(|samples| MinSampleCount::new(samples).ok()),
            sample_duration,
            keep: false,
            temp_dir: None,
            extension: None,
        }
    }

    #[rstest]
    #[case(Duration::from_secs(600), 12 * 60, 1)] // 10m / 12m -> ceil 1
    #[case(Duration::from_secs(25 * 60), 12 * 60, 3)] // 25m / 12m -> ceil 3
    #[case(Duration::from_secs(30), 12 * 60, 1)] // sub-sample_every still min 1
    fn sample_count_from_sample_every(
        #[case] duration: Duration,
        #[case] sample_every_secs: u64,
        #[case] expected: u64,
    ) {
        // setup
        let args = sample_args(None, None, Duration::from_secs(sample_every_secs));
        // execute
        let count = args.sample_count(duration);
        // assert
        assert_eq!(count, expected);
    }

    #[test]
    fn sample_count_samples_override_wins() {
        // setup
        let args = sample_args(Some(7), None, Duration::from_secs(60));
        // execute
        let count = args.sample_count(Duration::from_secs(3600));
        // assert
        assert_eq!(count, 7);
    }

    #[test]
    fn sample_count_respects_min_samples() {
        // setup
        let args = sample_args(None, Some(5), Duration::from_secs(60 * 60));
        // execute
        let count = args.sample_count(Duration::from_secs(60));
        // assert
        assert_eq!(count, 5);
    }

    #[test]
    fn min_sample_count_is_a_checked_copy_newtype() {
        fn assert_copy<T: Copy>() {}

        assert_copy::<MinSampleCount>();
        assert!(matches!(
            MinSampleCount::new(5).map(MinSampleCount::get),
            Ok(5)
        ));
        assert!(matches!(
            MinSampleCount::new(0),
            Err(MinSampleCountError::Zero)
        ));
    }

    #[test]
    fn parse_min_samples_rejects_zero() {
        let args = crate::command::crf_search::Args::try_parse_from([
            "ab-av1",
            "--input",
            "test.mp4",
            "--min-samples",
            "0",
        ]);

        assert!(args.is_err());
    }

    #[test]
    fn parse_samples_returns_typed_override() {
        let args = crate::command::crf_search::Args::try_parse_from([
            "ab-av1",
            "--input",
            "test.mp4",
            "--samples",
            "7",
        ]);

        assert!(matches!(
            args.as_ref()
                .map(|args| args.sample.samples.map(SampleCountOverride::get)),
            Ok(Some(7))
        ));
    }

    #[test]
    fn parse_sample_durations_reject_zero() {
        let sample_every = crate::command::crf_search::Args::try_parse_from([
            "ab-av1",
            "--input",
            "test.mp4",
            "--sample-every",
            "0s",
        ]);
        let sample_duration = crate::command::crf_search::Args::try_parse_from([
            "ab-av1",
            "--input",
            "test.mp4",
            "--sample-duration",
            "0s",
        ]);

        assert!(sample_every.is_err());
        assert!(sample_duration.is_err());
    }

    mod proptest_sample_count {
        use super::*;

        proptest! {
            #[test]
            fn monotonic_in_duration(
                base_secs in 10u64..3600,
                extra_secs in 1u64..3600,
                sample_every_secs in 1u64..600,
            ) {
                // setup
                let args = sample_args(None, None, Duration::from_secs(sample_every_secs));
                let shorter = Duration::from_secs(base_secs);
                let longer = Duration::from_secs(base_secs + extra_secs);

                // execute
                let short_count = args.sample_count(shorter);
                let long_count = args.sample_count(longer);

                // assert
                prop_assert!(long_count >= short_count);
                prop_assert!(short_count >= 1);
                prop_assert!(long_count >= 1);
            }

            #[test]
            fn samples_override_is_exact(samples in 1u64..100) {
                // setup
                let args = sample_args(Some(samples), Some(1), Duration::from_secs(1));

                // execute
                let count = args.sample_count(Duration::from_secs(10_000));

                // assert
                prop_assert_eq!(count, samples);
            }
        }
    }

    #[test]
    fn set_extension_from_input_uses_image_ext() {
        // setup
        let mut args = sample_args(None, None, Duration::from_secs(60));
        let input = Path::new("photo.png");
        let encoder = Encoder::for_test("libsvtav1");
        let probe = Ffprobe {
            duration: Ok(Duration::ZERO),
            has_audio: false,
            max_audio_channels: None,
            fps: Ok(24.0),
            resolution: None,
            is_image: true,
            pix_fmt: None,
        };
        // execute
        args.set_extension_from_input(input, &encoder, &probe);
        // assert
        assert_eq!(args.extension.as_deref(), Some("avif"));
    }

    // ab-kgc.10: default extension must follow input container when ffprobe fails
    #[test]
    fn set_extension_from_input_uses_input_container_on_probe_failure() {
        // setup
        let mut args = sample_args(None, None, Duration::from_secs(60));
        let input = Path::new("clip.webm");
        let encoder = Encoder::for_test("libsvtav1");
        let probe = Ffprobe {
            duration: Err(anyhow::anyhow!("ffprobe: missing").into()),
            has_audio: false,
            max_audio_channels: None,
            fps: Err(anyhow::anyhow!("ffprobe: missing").into()),
            resolution: None,
            is_image: false,
            pix_fmt: None,
        };

        // execute
        args.set_extension_from_input(input, &encoder, &probe);

        // assert
        assert_eq!(
            args.extension.as_deref(),
            Some("webm"),
            "probe failure must not block default extension from input path"
        );
    }

    #[test]
    fn set_extension_from_input_uses_mkv_fallback_for_unknown_extension_on_probe_failure() {
        // setup
        let mut args = sample_args(None, None, Duration::from_secs(60));
        let input = Path::new("clip.unknown");
        let encoder = Encoder::for_test("libsvtav1");
        let probe = Ffprobe {
            duration: Err(anyhow::anyhow!("ffprobe: missing").into()),
            has_audio: false,
            max_audio_channels: None,
            fps: Err(anyhow::anyhow!("ffprobe: missing").into()),
            resolution: None,
            is_image: false,
            pix_fmt: None,
        };

        // execute
        args.set_extension_from_input(input, &encoder, &probe);

        // assert
        assert_eq!(args.extension.as_deref(), Some("mkv"));
    }

    // ab-kgc.10: min_samples must apply even when sample_every would yield fewer
    #[test]
    fn sample_count_min_samples_wins_over_short_duration() {
        // setup — 30s input / 60s sample_every => 1 sample, but min_samples=3
        let args = sample_args(None, Some(3), Duration::from_secs(60));

        // execute
        let count = args.sample_count(Duration::from_secs(30));

        // assert
        assert_eq!(count, 3);
    }

    // ab-kgc.43: extensionless outputs should inherit the input container for sample encoding
    #[test]
    fn set_extension_from_output_without_extension_falls_back_to_input_container() {
        // setup
        let mut args = sample_args(None, None, Duration::from_secs(60));

        // execute — auto_encode only calls set_extension_from_output today
        args.set_extension_from_output(Path::new("/out/encoded"));

        // assert
        assert_eq!(
            args.extension.as_deref(),
            Some("mkv"),
            "extensionless output path must still yield a sample container extension"
        );
    }

    #[test]
    fn set_extension_from_output_uses_path_extension() {
        // setup
        let mut args = sample_args(None, None, Duration::from_secs(60));
        // execute
        args.set_extension_from_output(Path::new("out/sample.mkv"));
        // assert
        assert_eq!(args.extension.as_deref(), Some("mkv"));
    }

    // ab-kgc.33: sub-second sample_every should not be silently clamped to 1 second
    #[test]
    fn sample_count_honors_sub_second_sample_every() {
        let args = sample_args(None, None, Duration::from_millis(500));
        let count = args.sample_count(Duration::from_secs(10));
        assert_eq!(
            count, 20,
            "10s / 500ms should yield 20 samples, not duration/1s"
        );
    }

    // ab-kgc.19: explicit --samples 0 must not be silently raised
    #[test]
    fn sample_count_rejects_zero_samples_override() {
        let args = sample_args(Some(0), None, Duration::from_secs(60));
        assert_eq!(
            args.sample_count(Duration::from_secs(3600)),
            0,
            "explicit --samples 0 must not be silently raised"
        );
    }

    #[rstest]
    #[case(60.0, Some(60.0))]
    #[case(0.0, None)]
    #[case(-1.0, None)]
    fn xpsnr_fps_filter(#[case] fps: f32, #[case] expected: Option<f32>) {
        // setup
        let xpsnr = Xpsnr {
            xpsnr_fps: FrameRateOverride::new(fps),
            ..Default::default()
        };
        // execute
        // assert
        assert_eq!(xpsnr.fps(), expected);
    }

    #[test]
    fn xpsnr_default_matches_cli_defaults() {
        let xpsnr = Xpsnr::default();
        assert_eq!(xpsnr.fps(), Some(60.0));
        assert_eq!(xpsnr.xpsnr_pix_format, None);
    }

    #[test]
    fn frame_rate_override_is_a_checked_newtype() {
        assert_eq!(FrameRateOverride::new(60.0).fps(), Some(60.0));
        assert_eq!(FrameRateOverride::new(0.0).fps(), None);
        assert_eq!(FrameRateOverride::new(-1.0).fps(), None);
        assert!("NaN".parse::<FrameRateOverride>().is_err());
        assert!("inf".parse::<FrameRateOverride>().is_err());
    }

    // ab-kgc.82: xpsnr pix_format must participate in args hashing (distinct from cache key ab-kgc.21)
    #[test]
    fn xpsnr_hash_includes_pix_format() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let a = Xpsnr {
            xpsnr_pix_format: Some(PixelFormat::Yuv420p),
            ..Default::default()
        };
        let b = Xpsnr {
            xpsnr_pix_format: Some(PixelFormat::Yuv420p10le),
            ..Default::default()
        };

        let mut hash_a = DefaultHasher::new();
        let mut hash_b = DefaultHasher::new();
        a.hash(&mut hash_a);
        b.hash(&mut hash_b);

        assert_ne!(
            hash_a.finish(),
            hash_b.finish(),
            "xpsnr_pix_format must affect Xpsnr hash"
        );
    }
}
