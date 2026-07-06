//! ffprobe logic
use crate::command::args::PixelFormat;
use anyhow::{Context, anyhow};
use std::{fmt, fs::File, io::Read, path::Path, time::Duration};

pub struct Ffprobe {
    /// Duration of video.
    pub duration: Result<Duration, ProbeError>,
    /// The video has audio stream(s).
    pub has_audio: bool,
    /// Audio number of channels (if multiple channel the highest).
    pub max_audio_channels: Option<i64>,
    /// Video frame rate.
    pub fps: Result<f64, ProbeError>,
    pub resolution: Option<(u32, u32)>,
    pub is_image: bool,
    pub pix_fmt: Option<String>,
}

impl Ffprobe {
    pub fn pixel_format(&self) -> Option<PixelFormat> {
        let pf = self.pix_fmt.as_deref()?;
        PixelFormat::try_from(pf).ok()
    }

    pub fn nframes(&self) -> Result<u64, ProbeError> {
        match (&self.fps, &self.duration) {
            (Ok(fps), Ok(duration)) => {
                let frames = (fps * duration.as_secs_f64()).round();
                if frames.is_normal() && frames.is_sign_positive() {
                    Ok(frames as _)
                } else {
                    Err(ProbeError(format!("Invalid nframes {frames}")))
                }
            }
            (Err(e), _) | (_, Err(e)) => Err(e.clone()),
        }
    }
}

/// Try to ffprobe the given input.
pub fn probe(input: &Path) -> Ffprobe {
    let is_image = is_image(input).unwrap_or(false);

    #[cfg(test)]
    if test_hooks::force_ffprobe_error() {
        return ffprobe_error_fallback(is_image, "forced ffprobe failure (test fixture)");
    }

    let probe = match ffprobe::ffprobe(input) {
        Ok(p) => p,
        Err(err) => return ffprobe_error_fallback(is_image, err),
    };

    let fps = read_fps(&probe);
    let duration = read_duration(&probe);
    let has_audio = probe
        .streams
        .iter()
        .any(|s| s.codec_type.as_deref() == Some("audio"));
    let max_audio_channels = probe
        .streams
        .iter()
        .filter(|s| s.codec_type.as_deref() == Some("audio"))
        .filter_map(|a| a.channels)
        .max();

    let resolution = probe
        .streams
        .iter()
        .filter(|s| s.codec_type.as_deref() == Some("video"))
        .find_map(|s| {
            let w = s.width.and_then(|w| u32::try_from(w).ok())?;
            let h = s.height.and_then(|w| u32::try_from(w).ok())?;
            Some((w, h))
        });

    let pix_fmt = probe
        .streams
        .into_iter()
        .filter(|s| s.codec_type.as_deref() == Some("video"))
        .find_map(|s| s.pix_fmt);

    Ffprobe {
        duration: duration.map_err(ProbeError::from),
        fps: fps.map_err(ProbeError::from),
        has_audio,
        max_audio_channels,
        resolution,
        is_image,
        pix_fmt,
    }
}

fn ffprobe_error_fallback(is_image: bool, err: impl fmt::Display) -> Ffprobe {
    Ffprobe {
        duration: Err(ProbeError(format!("ffprobe: {err}"))),
        fps: Err(ProbeError(format!("ffprobe: {err}"))),
        has_audio: false,
        max_audio_channels: None,
        resolution: None,
        is_image,
        pix_fmt: None,
    }
}

fn is_image(path: &Path) -> anyhow::Result<bool> {
    let file = File::open(path)?;
    let mut file_header = Vec::with_capacity(8192);
    file.take(8192).read_to_end(&mut file_header)?;

    Ok(infer::is_image(&file_header))
}

fn read_duration(probe: &ffprobe::FfProbe) -> anyhow::Result<Duration> {
    match probe.format.duration.as_deref() {
        Some(duration_s) => {
            let duration_f = duration_s
                .parse::<f64>()
                .with_context(|| format!("invalid ffprobe video duration: {duration_s:?}"))?;
            Duration::try_from_secs_f64(duration_f)
                .map_err(|e| anyhow!("{e}: ffprobe video duration: {duration_s:?}"))
        }
        None => Ok(Duration::ZERO),
    }
}

fn read_fps(probe: &ffprobe::FfProbe) -> anyhow::Result<f64> {
    let vstream = probe
        .streams
        .iter()
        .find(|s| s.codec_type.as_deref() == Some("video"))
        .context("no video stream found")?;

    parse_frame_rate(&vstream.avg_frame_rate)
        .or_else(|| parse_frame_rate(&vstream.r_frame_rate))
        .context("invalid ffprobe video frame rate")
}

/// parse "x/y" or float strings.
pub fn parse_frame_rate(rate: &str) -> Option<f64> {
    if let Some((x, y)) = rate.split_once('/') {
        let x: f64 = x.parse().ok()?;
        let y: f64 = y.parse().ok()?;
        if x <= 0.0 || y <= 0.0 {
            return None;
        }
        Some(x / y)
    } else {
        rate.parse()
            .ok()
            .filter(|f: &f64| f.is_finite() && *f > 0.0)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbeError(String);

impl fmt::Display for ProbeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl From<anyhow::Error> for ProbeError {
    fn from(err: anyhow::Error) -> Self {
        Self(format!("{err}"))
    }
}

impl std::error::Error for ProbeError {}

#[cfg(test)]
pub(crate) mod test_hooks {
    use std::cell::RefCell;

    thread_local! {
        static FORCE_FFPROBE_ERROR: RefCell<bool> = const { RefCell::new(false) };
    }

    pub fn set_force_ffprobe_error(force: bool) {
        FORCE_FFPROBE_ERROR.with(|f| *f.borrow_mut() = force);
    }

    pub fn clear() {
        FORCE_FFPROBE_ERROR.with(|f| *f.borrow_mut() = false);
    }

    pub fn force_ffprobe_error() -> bool {
        FORCE_FFPROBE_ERROR.with(|f| *f.borrow())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use rstest::rstest;
    use test_case::test_case;

    mod helpers {
        use super::*;

        pub fn probe_with(
            fps: Result<f64, ProbeError>,
            duration: Result<Duration, ProbeError>,
        ) -> Ffprobe {
            Ffprobe {
                duration,
                fps,
                has_audio: false,
                max_audio_channels: None,
                resolution: None,
                is_image: false,
                pix_fmt: None,
            }
        }

        pub struct ForceFfprobeError(bool);

        impl ForceFfprobeError {
            pub fn set() -> Self {
                test_hooks::set_force_ffprobe_error(true);
                Self(true)
            }
        }

        impl Drop for ForceFfprobeError {
            fn drop(&mut self) {
                if self.0 {
                    test_hooks::clear();
                }
            }
        }
    }

    use helpers::{ForceFfprobeError, probe_with};

    #[rstest]
    #[case("30000/1001", 30000.0 / 1001.0)]
    #[case("24/1", 24.0)]
    #[case("24", 24.0)]
    #[case("29.97", 29.97)]
    #[case("24 / 1", 24.0)] // ab-kgc.91: spaced fraction strings
    fn parse_frame_rate_valid(#[case] input: &str, #[case] expected: f64) {
        // setup
        // execute
        let fps = parse_frame_rate(input).expect("valid frame rate");
        // assert
        assert!(
            (fps - expected).abs() < 1e-9,
            "got {fps}, expected {expected}"
        );
    }

    #[test_case("0/1", None; "zero numerator")]
    #[test_case("1/0", None; "zero denominator")]
    #[test_case("-1/24", None; "negative numerator")]
    #[test_case("", None; "empty")]
    #[test_case("not-a-rate", None; "garbage")]
    #[test_case("NaN", None; "nan")]
    fn parse_frame_rate_invalid(input: &str, expected: Option<f64>) {
        // setup
        // execute
        let fps = parse_frame_rate(input);
        // assert
        assert_eq!(fps, expected);
    }

    mod proptest_parse_frame_rate {
        use super::*;

        proptest! {
            #[test]
            fn valid_fraction_is_positive(num in 1u32..10_000, den in 1u32..10_000) {
                // setup
                let rate = format!("{num}/{den}");

                // execute
                let fps = parse_frame_rate(&rate).expect("valid fraction");

                // assert
                let expected = num as f64 / den as f64;
                prop_assert!(fps.is_finite() && fps > 0.0);
                prop_assert!((fps - expected).abs() < 1e-9);
            }

            #[test]
            fn invalid_fraction_rejects_non_positive(num in 0u32..100, den in 0u32..100) {
                // setup
                let rate = format!("{num}/{den}");

                // execute
                let fps = parse_frame_rate(&rate);

                // assert
                if num == 0 || den == 0 {
                    prop_assert!(fps.is_none());
                }
            }
        }
    }

    #[test]
    fn nframes_computes_from_fps_and_duration() {
        // setup
        let probe = probe_with(Ok(30.0), Ok(Duration::from_secs_f64(10.0)));
        // execute
        let frames = probe.nframes().expect("nframes");
        // assert
        assert_eq!(frames, 300);
    }

    #[test]
    fn nframes_propagates_fps_error() {
        // setup
        let err = ProbeError("bad fps".into());
        let probe = probe_with(Err(err.clone()), Ok(Duration::from_secs(10)));
        // execute
        let result = probe.nframes();
        // assert
        assert_eq!(result, Err(err));
    }

    #[test]
    fn nframes_rejects_non_positive_frame_count() {
        // setup
        let probe = probe_with(Ok(0.001), Ok(Duration::from_millis(1)));
        // execute
        let result = probe.nframes();
        // assert
        assert!(result.is_err());
    }

    #[test]
    fn pixel_format_parsing() {
        // setup
        let mut probe = probe_with(Ok(24.0), Ok(Duration::from_secs(1)));

        // execute / assert — known pix_fmt
        probe.pix_fmt = Some("yuv420p10le".into());
        assert_eq!(probe.pixel_format(), Some(PixelFormat::Yuv420p10le));

        // execute / assert — unknown pix_fmt
        probe.pix_fmt = Some("rgb24".into());
        assert_eq!(probe.pixel_format(), None);

        // execute / assert — ab-kgc.92: mixed-case pix_fmt strings
        probe.pix_fmt = Some("YUV420P".into());
        assert_eq!(probe.pixel_format(), Some(PixelFormat::Yuv420p));
    }

    #[test]
    fn ffprobe_error_fallback_behavior() {
        // setup (none)
        // execute
        let image_probe = ffprobe_error_fallback(true, "probe failed");
        let video_probe = ffprobe_error_fallback(false, "missing ffprobe");

        // assert — image detection preserved on failure
        assert!(image_probe.is_image);
        assert!(!image_probe.has_audio);
        assert!(image_probe.duration.is_err());
        assert!(image_probe.fps.is_err());

        // assert — conservative audio metadata when not an image
        assert!(!video_probe.has_audio);
        assert_eq!(video_probe.max_audio_channels, None);
    }

    #[test]
    fn probe_preserves_image_detection_when_ffprobe_fails() {
        // setup: image header on disk + test seam forces ffprobe failure.
        let path =
            std::env::temp_dir().join(format!("ab-av1-probe-fallback-{}.jpg", std::process::id()));
        std::fs::write(&path, [0xFF, 0xD8, 0xFF, 0xD9]).expect("write jpeg stub");
        let _guard = ForceFfprobeError::set();
        // execute
        let probe = probe(&path);
        // assert
        assert!(
            probe.is_image,
            "header-based image detection must survive ffprobe failure"
        );
        assert!(!probe.has_audio);
        assert!(
            probe.duration.is_err(),
            "ffprobe failure must propagate to duration"
        );
        assert!(probe.fps.is_err(), "ffprobe failure must propagate to fps");
        assert!(
            probe.duration.unwrap_err().to_string().contains("ffprobe"),
            "error should mention ffprobe"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn probe_error_display_and_from_anyhow() {
        // setup
        let direct = ProbeError("bad probe".into());
        // execute / assert
        assert_eq!(direct.to_string(), "bad probe");
        let from_err: ProbeError = anyhow!("oops").into();
        assert!(from_err.to_string().contains("oops"));
    }

    #[test]
    fn nframes_error_and_edge_cases() {
        // setup / execute / assert — duration error propagates
        let duration_err = ProbeError("bad duration".into());
        let probe = probe_with(Ok(24.0), Err(duration_err.clone()));
        assert_eq!(probe.nframes(), Err(duration_err));

        // setup / execute / assert — zero duration rejected
        let probe = probe_with(Ok(24.0), Ok(Duration::ZERO));
        assert!(probe.nframes().is_err());

        // setup / execute / assert — infinite fps rejected rather than wrapping
        let probe = probe_with(Ok(f64::INFINITY), Ok(Duration::from_secs(10)));
        assert!(probe.nframes().is_err());
    }

    #[test]
    fn probe_webp_header_detects_image_on_ffprobe_failure() {
        let path =
            std::env::temp_dir().join(format!("ab-av1-probe-webp-{}.webp", std::process::id()));
        // Minimal RIFF WEBP header stub
        std::fs::write(
            &path,
            b"RIFF\x24\x00\x00\x00WEBPVP8 \x18\x00\x00\x00\x2f\x01\x00\x9d\x01\x2a\x01\x00\x01\x00\x02\x00\x34\x25\xa4\x00\x03\x70\x00\xfe\xfb\xfd\xc0\x00",
        )
        .expect("write webp stub");
        let _guard = ForceFfprobeError::set();
        let probe = probe(&path);
        assert!(
            probe.is_image,
            "webp header must be detected when ffprobe fails"
        );
        let _ = std::fs::remove_file(path);
    }

    #[test]
    fn probe_reads_zero_duration_from_minimal_gif() {
        // setup: 1×1 GIF — ffprobe succeeds, format duration is zero.
        let gif = b"GIF89a\x01\x00\x01\x00\x80\x00\x00\xff\xff\xff\x00\x00\x00,\
\x00\x00\x00\x00\x01\x00\x01\x00\x00\x02\x02D\x01\x00;";
        let path =
            std::env::temp_dir().join(format!("ab-av1-probe-gif-{}.gif", std::process::id()));
        std::fs::write(&path, gif).expect("write gif stub");
        // execute
        let probe = probe(&path);
        // assert
        assert!(probe.is_image);
        assert_eq!(probe.duration.ok(), Some(Duration::ZERO));
        let _ = std::fs::remove_file(path);
    }
}
