//! Shared argument logic.
mod encode;
mod vmaf;

pub use encode::*;
pub use vmaf::*;

use crate::{command::encode::default_output_ext, ffprobe::Ffprobe};
use clap::{Parser, ValueHint};
use std::{
    path::{Path, PathBuf},
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
    pub samples: Option<u64>,

    /// Calculate number of samples by dividing the input duration by this value.
    /// So "12m" would mean with an input 25-36 minutes long, 3 samples would be used.
    /// More samples take longer but may provide a more accurate result.
    ///
    /// Setting --samples overrides this value.
    #[arg(long, default_value = "12m", value_parser = humantime::parse_duration)]
    pub sample_every: Duration,

    /// Minimum number of samples. So at least this many samples will be used.
    #[arg(long)]
    pub min_samples: Option<u64>,

    /// Duration of each sample.
    #[arg(long, default_value = "20s", value_parser = humantime::parse_duration)]
    pub sample_duration: Duration,

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
            Some(s) => s,
            None => {
                let every = self.sample_every.as_secs_f64();
                if every <= 0.0 {
                    1
                } else {
                    (input_duration.as_secs_f64() / every).ceil() as u64
                }
            }
        };
        if self.samples.is_some() {
            count.max(self.min_samples.unwrap_or(0))
        } else {
            count.max(self.min_samples.unwrap_or(1)).max(1)
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

/// Common xpsnr options.
#[derive(Debug, Parser, Clone, Copy)]
pub struct Xpsnr {
    /// Frame rate override used to analyse both reference & distorted videos.
    /// Maps to ffmpeg `-r` input arg.
    ///
    /// Setting to 0 disables use.
    #[arg(long, default_value_t = 60.0)]
    pub xpsnr_fps: f32,

    /// Pixel format used in xpsnr analysis only. By default this is inferred from sources.
    #[arg(value_enum, long)]
    pub xpsnr_pix_format: Option<PixelFormat>,
}

impl Xpsnr {
    pub fn fps(&self) -> Option<f32> {
        Some(self.xpsnr_fps).filter(|r| *r > 0.0)
    }
}

impl std::hash::Hash for Xpsnr {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.xpsnr_fps.to_ne_bytes().hash(state);
        self.xpsnr_pix_format.hash(state);
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
        Sample {
            samples,
            sample_every,
            min_samples,
            sample_duration: Duration::from_secs(20),
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
            xpsnr_fps: fps,
            xpsnr_pix_format: None,
        };
        // execute
        // assert
        assert_eq!(xpsnr.fps(), expected);
    }

    // ab-kgc.82: xpsnr pix_format must participate in args hashing (distinct from cache key ab-kgc.21)
    #[test]
    fn xpsnr_hash_includes_pix_format() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::{Hash, Hasher};

        let a = Xpsnr {
            xpsnr_fps: 60.0,
            xpsnr_pix_format: Some(PixelFormat::Yuv420p),
        };
        let b = Xpsnr {
            xpsnr_fps: 60.0,
            xpsnr_pix_format: Some(PixelFormat::Yuv420p10le),
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
