use crate::{
    command::rules::{validate_enc_arg, validate_encoder_passthrough, validate_svt_arg},
    ffmpeg::FfmpegEncodeArgs,
    ffprobe::{Ffprobe, ProbeError},
    float::TerseF32,
};
use clap::{Parser, ValueHint};
#[cfg(test)]
use rstest::rstest;
use std::{
    borrow::Cow,
    fmt::{self, Write},
    path::PathBuf,
    str::FromStr,
    sync::Arc,
    time::Duration,
};

/// Common svt-av1/ffmpeg input encoding arguments.
#[derive(Parser, Clone)]
pub struct Encode {
    /// Encoder override. See https://ffmpeg.org/ffmpeg-all.html#toc-Video-Encoders.
    ///
    /// [possible values: libsvtav1, libx264, libx265, libvpx-vp9, ...]
    #[arg(value_enum, short, long, default_value = "libsvtav1")]
    pub encoder: Encoder,

    /// Input video file.
    #[arg(short, long, value_hint = ValueHint::FilePath)]
    pub input: PathBuf,

    /// Ffmpeg video filter applied to the input before encoding.
    /// E.g. --vfilter "scale=1280:-1,fps=24".
    ///
    /// See https://ffmpeg.org/ffmpeg-filters.html#Video-Filters
    ///
    /// For VMAF calculations this is also applied to the reference video meaning VMAF
    /// scores represent the quality of input stream *after* applying filters compared
    /// to the encoded result.
    /// This allows filters like cropping to work with VMAF, as it would be the
    /// cropped stream that is VMAF compared to a cropped-then-encoded stream. Such filters
    /// would not otherwise generally be comparable.
    ///
    /// A consequence is the VMAF score will not reflect any quality lost
    /// by the vfilter itself, only the encode.
    /// To override the VMAF vfilter set --reference-vfilter.
    #[arg(long)]
    pub vfilter: Option<String>,

    /// Pixel format. libsvtav1, libaom-av1 & librav1e default to yuv420p10le.
    #[arg(value_enum, long)]
    pub pix_format: Option<PixelFormat>,

    /// Encoder preset (0-13).
    /// Higher presets means faster encodes, but with a quality tradeoff.
    ///
    /// For some ffmpeg encoders a word may be used, e.g. "fast".
    /// libaom-av1 preset is mapped to equivalent -cpu-used argument.
    ///
    /// [svt-av1 default: 8]
    #[arg(long, allow_hyphen_values = true)]
    pub preset: Option<Arc<str>>,

    /// Interval between keyframes. Can be specified as a number of frames, or a duration.
    /// E.g. "300" or "10s". Defaults to 10s if the input duration is over 3m.
    ///
    /// Longer intervals can give better compression but make seeking more coarse.
    /// Durations will be converted to frames using the input fps.
    ///
    /// Works on svt-av1 & most ffmpeg encoders set with --encoder.
    #[arg(long)]
    pub keyint: Option<KeyInterval>,

    /// Svt-av1 scene change detection, inserts keyframes at scene changes.
    /// Defaults on if using default keyint & the input duration is over 3m. Otherwise off.
    #[arg(long)]
    pub scd: Option<bool>,

    /// Additional svt-av1 arg(s). E.g. --svt mbr=2000 --svt film-grain=8
    ///
    /// See https://gitlab.com/AOMediaCodec/SVT-AV1/-/blob/master/Docs/svt-av1_encoder_user_guide.md#options
    #[arg(long = "svt", value_parser = parse_svt_arg)]
    pub svt_args: Vec<SvtArg>,

    /// Additional ffmpeg encoder arg(s). E.g. `--enc x265-params=lossless=1`
    /// These are added as ffmpeg output file options.
    ///
    /// The first '=' symbol will be used to infer that this is an option with a value.
    /// Passed to ffmpeg like "x265-params=lossless=1" -> ['-x265-params', 'lossless=1']
    #[arg(long = "enc", allow_hyphen_values = true, value_parser = parse_enc_arg)]
    pub enc_args: Vec<EncoderArg>,

    /// Additional ffmpeg input encoder arg(s). E.g. `--enc-input r=1`
    /// These are added as ffmpeg input file options.
    ///
    /// See --enc docs.
    ///
    /// *_vaapi (e.g. h264_vaapi) encoder default:
    /// `--enc-input hwaccel=vaapi --enc-input hwaccel_output_format=vaapi`.
    ///
    /// *_vulkan encoder default: `--enc-input hwaccel=vulkan --enc-input hwaccel_output_format=vulkan`.
    ///
    /// Disable defaults by setting them to "none"
    /// e.g. `-enc-input hwaccel=none --enc-input hwaccel_output_format=none`
    #[arg(long = "enc-input", allow_hyphen_values = true, value_parser = parse_enc_input_arg)]
    pub enc_input_args: Vec<EncoderInputArg>,
}

fn parse_svt_arg(arg: &str) -> anyhow::Result<SvtArg> {
    let arg = arg.trim_start_matches('-').to_owned();

    validate_svt_arg(arg.as_str()).map_err(anyhow::Error::new)?;

    Ok(arg.into())
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct SvtArg(Arc<str>);

impl SvtArg {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SvtArg {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.as_str().fmt(f)
    }
}

impl From<String> for SvtArg {
    fn from(arg: String) -> Self {
        Self(arg.into())
    }
}

impl From<&str> for SvtArg {
    fn from(arg: &str) -> Self {
        Self(Arc::from(arg))
    }
}

fn parse_enc_arg(arg: &str) -> anyhow::Result<EncoderArg> {
    let mut arg = arg.to_owned();
    if !arg.starts_with('-') {
        arg.insert(0, '-');
    }

    validate_enc_arg(arg.as_str()).map_err(anyhow::Error::new)?;

    Ok(arg.into())
}

fn parse_enc_input_arg(arg: &str) -> anyhow::Result<EncoderInputArg> {
    let arg = parse_enc_arg(arg)?;
    Ok(arg.as_str().into())
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EncoderArg(Arc<str>);

impl EncoderArg {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<String> for EncoderArg {
    fn from(arg: String) -> Self {
        Self(arg.into())
    }
}

impl From<&str> for EncoderArg {
    fn from(arg: &str) -> Self {
        Self(Arc::from(arg))
    }
}

impl FromStr for EncoderArg {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_enc_arg(s)
    }
}

impl AsRef<str> for EncoderArg {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct EncoderInputArg(Arc<str>);

impl EncoderInputArg {
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl From<&str> for EncoderInputArg {
    fn from(arg: &str) -> Self {
        Self(Arc::from(arg))
    }
}

impl FromStr for EncoderInputArg {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        parse_enc_input_arg(s)
    }
}

impl AsRef<str> for EncoderInputArg {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct PassthroughArg<'a> {
    option: &'a str,
    value: Option<&'a str>,
}

impl<'a> PassthroughArg<'a> {
    fn parse(raw: &'a str) -> Self {
        match raw.split_once('=') {
            Some((option, value)) => Self {
                option,
                value: Some(value),
            },
            None => Self {
                option: raw,
                value: None,
            },
        }
    }

    fn values(self) -> impl Iterator<Item = &'a str> {
        [Some(self.option), self.value].into_iter().flatten()
    }

    fn hint_value(self) -> Cow<'a, str> {
        match self.value {
            Some(value) => Cow::Owned(format!("{}={value}", self.option.trim_start_matches('-'))),
            None => Cow::Borrowed(self.option.trim_start_matches('-')),
        }
    }

    fn is_svtav1_params(self) -> bool {
        self.option.trim_start_matches('-') == "svtav1-params"
    }
}

fn owned_arg(arg: &str) -> Arc<String> {
    arg.to_owned().into()
}

struct CollectedPassthroughArgs {
    args: Vec<Arc<String>>,
    omitted_defaults: Vec<&'static str>,
}

fn collect_passthrough_args<'a, T>(
    args: impl IntoIterator<Item = &'a T>,
    omit_default_value_for: &'static [&'static str],
) -> CollectedPassthroughArgs
where
    T: AsRef<str> + 'a,
{
    let mut omitted_defaults = Vec::new();

    let args = args
        .into_iter()
        .map(|arg| PassthroughArg::parse(arg.as_ref()))
        .filter(|arg| {
            if arg.value == Some("none")
                && let Some(name) = omit_default_value_for
                    .iter()
                    .copied()
                    .find(|name| *name == arg.option)
            {
                omitted_defaults.push(name);
                return false;
            }

            true
        })
        .flat_map(PassthroughArg::values)
        .map(owned_arg)
        .collect();

    CollectedPassthroughArgs {
        args,
        omitted_defaults,
    }
}

impl Encode {
    fn default_preset(&self, svtav1: bool) -> Option<Arc<str>> {
        match &self.preset {
            Some(preset) => Some(preset.clone()),
            None if svtav1 => Some("8".into()),
            None => None,
        }
    }

    fn inferred_pix_fmt(&self, vcodec: &str) -> Option<PixelFormat> {
        self.pix_format.or(match vcodec {
            "libsvtav1" | "libaom-av1" | "librav1e" => Some(PixelFormat::Yuv420p10le),
            _ => None,
        })
    }

    fn default_input_args(&self) -> CollectedPassthroughArgs {
        collect_passthrough_args(&self.enc_input_args, &["-hwaccel", "-hwaccel_output_format"])
    }

    pub fn encode_hint(&self, crf: f32) -> String {
        let Self {
            encoder,
            input,
            vfilter,
            preset,
            pix_format,
            keyint,
            scd,
            svt_args,
            enc_args,
            enc_input_args,
        } = self;

        let input = shell_escape::escape(input.display().to_string().into());

        let mut hint = "ab-av1 encode".to_owned();

        let vcodec = encoder.as_str();
        if vcodec != "libsvtav1" {
            write!(hint, " -e {vcodec}").unwrap();
        }
        write!(hint, " -i {input} --crf {}", TerseF32(crf)).unwrap();

        if let Some(preset) = preset {
            write!(hint, " --preset {preset}").unwrap();
        }
        if let Some(keyint) = keyint {
            write!(hint, " --keyint {keyint}").unwrap();
        }
        if let Some(scd) = scd {
            write!(hint, " --scd {scd}").unwrap();
        }
        if let Some(pix_fmt) = pix_format {
            write!(hint, " --pix-format {pix_fmt}").unwrap();
        }
        if let Some(filter) = vfilter {
            write!(hint, " --vfilter {filter:?}").unwrap();
        }
        for arg in svt_args {
            write!(hint, " --svt {arg}").unwrap();
        }
        for arg in enc_input_args {
            write!(
                hint,
                " --enc-input {}",
                PassthroughArg::parse(arg.as_str()).hint_value()
            )
            .unwrap();
        }
        for arg in enc_args {
            write!(hint, " --enc {}", PassthroughArg::parse(arg.as_str()).hint_value()).unwrap();
        }

        hint
    }

    pub fn to_ffmpeg_args(
        &self,
        crf: f32,
        probe: &Ffprobe,
    ) -> anyhow::Result<FfmpegEncodeArgs<'_>> {
        let vcodec = &self.encoder.0;
        let svtav1 = vcodec.as_ref() == "libsvtav1";
        validate_encoder_passthrough(
            svtav1,
            !self.svt_args.is_empty(),
            std::iter::empty::<&str>(),
            std::iter::empty::<&str>(),
        )
        .map_err(anyhow::Error::new)?;

        let preset = self.default_preset(svtav1);

        let keyint = self.keyint(probe)?;

        let mut svtav1_params = vec![];
        if svtav1 {
            let scd = match (self.scd, self.keyint, keyint) {
                (Some(true), ..) | (_, None, Some(_)) => 1,
                _ => 0,
            };
            svtav1_params.push(format!("scd={scd}"));
            // include crf in svtav1-params to support quarter-steps
            svtav1_params.push(format!("crf={crf}"));
            // add all --svt args
            svtav1_params.extend(self.svt_args.iter().map(|a| a.to_string()));
        }

        let mut args: Vec<Arc<String>> = self
            .enc_args
            .iter()
            .filter_map(|arg| {
                let parsed = PassthroughArg::parse(arg.as_str());
                if parsed.is_svtav1_params() {
                    svtav1_params.push(arg.as_str().to_owned());
                    None
                } else {
                    Some(parsed)
                }
            })
            .flat_map(PassthroughArg::values)
            .map(owned_arg)
            .collect();

        if !svtav1_params.is_empty() {
            args.push(owned_arg("-svtav1-params"));
            args.push(svtav1_params.join(":").into());
        }

        // Set keyint/-g for all vcodecs
        if let Some(keyint) = keyint
            && !args.iter().any(|a| &**a == "-g")
        {
            args.push(owned_arg("-g"));
            args.push(keyint.to_string().into());
        }

        for (name, val) in self.encoder.default_ffmpeg_args() {
            if !args.iter().any(|arg| &**arg == name) {
                args.push(name.to_string().into());
                args.push(val.to_string().into());
            }
        }

        let pix_fmt = self.inferred_pix_fmt(vcodec);

        let mut input_args = self.default_input_args();

        for (name, val) in self.encoder.default_ffmpeg_input_args() {
            if !input_args.omitted_defaults.contains(name)
                && !input_args.args.iter().any(|arg| &**arg == name)
            {
                input_args.args.push(name.to_string().into());
                input_args.args.push(val.to_string().into());
            }
        }

        validate_encoder_passthrough(
            true,
            false,
            input_args.args.iter().map(|arg| arg.as_str()),
            args.iter().map(|arg| arg.as_str()),
        )
        .map_err(anyhow::Error::new)?;

        Ok(FfmpegEncodeArgs {
            input: &self.input,
            vcodec: Arc::clone(vcodec),
            pix_fmt,
            vfilter: self.vfilter.as_deref(),
            crf,
            preset,
            output_args: args,
            input_args: input_args.args,
            video_only: false,
        })
    }

    fn keyint(&self, probe: &Ffprobe) -> anyhow::Result<Option<i32>> {
        const KEYINT_DEFAULT_INPUT_MIN: Duration = Duration::from_secs(60 * 3);
        const KEYINT_DEFAULT: Duration = Duration::from_secs(10);

        let filter_fps = self.vfilter.as_deref().and_then(try_parse_fps_vfilter);
        Ok(
            match (self.keyint, &probe.duration, &probe.fps, filter_fps) {
                // use the filter-fps if used, otherwise the input fps
                (Some(ki), .., Some(fps)) => Some(ki.keyint_number(Ok(fps))?),
                (Some(ki), _, fps, None) => Some(ki.keyint_number(fps.clone())?),
                (None, Ok(duration), _, Some(fps)) if *duration >= KEYINT_DEFAULT_INPUT_MIN => {
                    Some(KeyInterval::Duration(KEYINT_DEFAULT).keyint_number(Ok(fps))?)
                }
                (None, Ok(duration), Ok(fps), None) if *duration >= KEYINT_DEFAULT_INPUT_MIN => {
                    Some(KeyInterval::Duration(KEYINT_DEFAULT).keyint_number(Ok(*fps))?)
                }
                _ => None,
            },
        )
    }
}

/// Video codec for encoding.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct Encoder(Arc<str>);

#[cfg(test)]
impl Encoder {
    pub fn for_test(name: &str) -> Self {
        Self(name.into())
    }
}

impl Encoder {
    /// vcodec name that would work if you used it as the -e argument.
    pub fn as_str(&self) -> &str {
        &self.0
    }

    /// Returns:
    /// * `true`: Higher crf values mean higher quality.
    /// * `false`: Higher crf values mean lower quality.
    pub fn high_crf_means_hq(&self) -> bool {
        self.as_str() == "hevc_videotoolbox"
    }

    /// Returns default crf-increment.
    ///
    /// Generally 0.1 if codec supports decimal crf.
    pub fn default_crf_increment(&self) -> f32 {
        match self.as_str() {
            "libx264" | "libx265" | "libvpx-vp9" => 0.1,
            "libsvtav1" => 0.25,
            _ => 1.0,
        }
    }

    pub fn default_min_crf(&self) -> f32 {
        match self.as_str() {
            "mpeg2video" => 2.0,
            "libsvtav1" => 5.0,
            _ => 10.0,
        }
    }

    pub fn default_max_crf(&self) -> f32 {
        match self.as_str() {
            "librav1e" | "av1_vaapi" => 255.0,
            "libx264" | "libx265" => 46.0,
            "mpeg2video" => 30.0,
            "hevc_videotoolbox" => 100.0,
            "libsvtav1" => 70.0,
            _ => 55.0,
        }
    }

    pub fn default_image_ext(&self) -> &'static str {
        match self.as_str() {
            // ffmpeg doesn't currently have good heif support,
            // these raw formats allow crf-search to work
            "libx264" => "264",
            "libx265" => "265",
            // otherwise assume av1
            _ => "avif",
        }
    }

    /// Additional encoder specific ffmpeg arg defaults.
    fn default_ffmpeg_args(&self) -> &[(&'static str, &'static str)] {
        match self.as_str() {
            // add `-b:v 0` for aom & vp9 to use "constant quality" mode
            "libaom-av1" | "libvpx-vp9" => &[("-b:v", "0")],
            // enable lookahead mode for qsv encoders
            "av1_qsv" | "hevc_qsv" | "h264_qsv" => &[
                ("-look_ahead", "1"),
                ("-extbrc", "1"),
                ("-look_ahead_depth", "40"),
            ],
            _ => &[],
        }
    }

    /// Additional encoder specific ffmpeg input arg defaults.
    fn default_ffmpeg_input_args(&self) -> &[(&'static str, &'static str)] {
        match self.as_str() {
            e if e.ends_with("_vaapi") => {
                &[("-hwaccel", "vaapi"), ("-hwaccel_output_format", "vaapi")]
            }
            e if e.ends_with("_vulkan") => {
                &[("-hwaccel", "vulkan"), ("-hwaccel_output_format", "vulkan")]
            }
            _ => <_>::default(),
        }
    }
}

impl std::str::FromStr for Encoder {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        Ok(match s {
            // Support "svt-av1" alias for back compat
            "svt-av1" => Self("libsvtav1".into()),
            vcodec => Self(vcodec.into()),
        })
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum KeyInterval {
    Frames(i32),
    Duration(Duration),
}

impl KeyInterval {
    pub fn keyint_number(&self, fps: Result<f64, ProbeError>) -> Result<i32, ProbeError> {
        Ok(match self {
            Self::Frames(keyint) => *keyint,
            Self::Duration(duration) => (duration.as_secs_f64() * fps?).round() as i32,
        })
    }
}

impl fmt::Display for KeyInterval {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Frames(frames) => write!(f, "{frames}"),
            Self::Duration(d) => write!(f, "{}", humantime::format_duration(*d)),
        }
    }
}

/// Parse as integer frames or a duration.
impl std::str::FromStr for KeyInterval {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> anyhow::Result<Self> {
        let frame_err = match s.parse::<i32>() {
            Ok(f) => return Ok(Self::Frames(f)),
            Err(err) => err,
        };
        match humantime::parse_duration(s) {
            Ok(d) => Ok(Self::Duration(d)),
            Err(e) => Err(anyhow::anyhow!("frames: {frame_err}, duration: {e}")),
        }
    }
}

/// Ordered by ascending quality.
#[derive(clap::ValueEnum, Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
#[clap(rename_all = "lower")]
pub enum PixelFormat {
    Yuv420p,
    Yuv420p10le,
    Yuv422p10le,
    Yuv444p10le,
}

impl PixelFormat {
    /// Returns the max quality pixel format, or None if both are None.
    pub fn opt_max(a: Option<Self>, b: Option<Self>) -> Option<Self> {
        match (a, b) {
            (Some(a), Some(b)) => Some(a.max(b)),
            (a, b) => a.or(b),
        }
    }
}

#[test]
fn pixel_format_order() {
    use PixelFormat::*;
    assert!(Yuv420p < Yuv420p10le);
    assert!(Yuv420p10le < Yuv422p10le);
    assert!(Yuv422p10le < Yuv444p10le);
}

impl PixelFormat {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Yuv420p10le => "yuv420p10le",
            Self::Yuv422p10le => "yuv422p10le",
            Self::Yuv444p10le => "yuv444p10le",
            Self::Yuv420p => "yuv420p",
        }
    }
}

impl fmt::Display for PixelFormat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl TryFrom<&str> for PixelFormat {
    type Error = ();

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        match value.to_ascii_lowercase().as_str() {
            "yuv420p10le" => Ok(Self::Yuv420p10le),
            "yuv422p10le" => Ok(Self::Yuv422p10le),
            "yuv444p10le" => Ok(Self::Yuv444p10le),
            "yuv420p" => Ok(Self::Yuv420p),
            _ => Err(()),
        }
    }
}

fn try_parse_fps_vfilter(vfilter: &str) -> Option<f64> {
    let fps_filter = vfilter
        .split(',')
        .find_map(|vf| vf.trim().strip_prefix("fps="))?
        .trim();

    match fps_filter {
        "ntsc" => Some(30000.0 / 1001.0),
        "pal" => Some(25.0),
        "film" => Some(24.0),
        "ntsc_film" => Some(24000.0 / 1001.0),
        _ => crate::ffprobe::parse_frame_rate(fps_filter),
    }
}

#[test]
fn test_try_parse_fps_vfilter() {
    let fps = try_parse_fps_vfilter("scale=1280:-1, fps=24, transpose=1").unwrap();
    assert!((fps - 24.0).abs() < f64::EPSILON, "{fps:?}");

    let fps = try_parse_fps_vfilter("scale=1280:-1, fps=ntsc, transpose=1").unwrap();
    assert!((fps - 30000.0 / 1001.0).abs() < f64::EPSILON, "{fps:?}");
}

#[test]
fn keyinterval_from_str_parses_frames_and_duration() {
    // setup / execute / assert
    use std::str::FromStr;
    assert_eq!(
        KeyInterval::from_str("300").unwrap(),
        KeyInterval::Frames(300)
    );
    assert_eq!(
        KeyInterval::from_str("10s").unwrap(),
        KeyInterval::Duration(Duration::from_secs(10))
    );
}

/// Should use keyint & scd defaults for >3m inputs.
#[test]
fn svtav1_to_ffmpeg_args_default_over_3m() {
    let enc = Encode {
        encoder: Encoder("libsvtav1".into()),
        input: "vid.mp4".into(),
        vfilter: Some("scale=320:-1,fps=film".into()),
        preset: None,
        pix_format: None,
        keyint: None,
        scd: None,
        svt_args: vec!["film-grain=30".into()],
        enc_args: <_>::default(),
        enc_input_args: <_>::default(),
    };

    let probe = Ffprobe {
        duration: Ok(Duration::from_secs(300)),
        has_audio: true,
        max_audio_channels: None,
        fps: Ok(30.0),
        resolution: Some((1280, 720)),
        is_image: false,
        pix_fmt: None,
    };

    let FfmpegEncodeArgs {
        input,
        vcodec,
        vfilter,
        pix_fmt,
        crf,
        preset,
        output_args,
        input_args,
        video_only,
    } = enc.to_ffmpeg_args(32.0, &probe).expect("to_ffmpeg_args");

    assert_eq!(&*vcodec, "libsvtav1");
    assert_eq!(input, enc.input);
    assert_eq!(vfilter, Some("scale=320:-1,fps=film"));
    assert_eq!(crf, 32.0);
    assert_eq!(preset, Some("8".into()));
    assert_eq!(pix_fmt, Some(PixelFormat::Yuv420p10le));
    assert!(!video_only);

    assert!(
        output_args
            .windows(2)
            .any(|w| w[0].as_str() == "-g" && w[1].as_str() == "240"),
        "expected -g in {output_args:?}"
    );
    let svtargs_idx = output_args
        .iter()
        .position(|a| a.as_str() == "-svtav1-params")
        .expect("missing -svtav1-params");
    let svtargs = output_args
        .get(svtargs_idx + 1)
        .expect("missing -svtav1-params value")
        .as_str();
    assert_eq!(svtargs, "scd=1:crf=32:film-grain=30");
    assert!(input_args.is_empty());
}

#[test]
fn svtav1_to_ffmpeg_args_default_under_3m() {
    let enc = Encode {
        encoder: Encoder("libsvtav1".into()),
        input: "vid.mp4".into(),
        vfilter: None,
        preset: Some("7".into()),
        pix_format: Some(PixelFormat::Yuv420p),
        keyint: None,
        scd: None,
        svt_args: vec![],
        enc_args: <_>::default(),
        enc_input_args: <_>::default(),
    };

    let probe = Ffprobe {
        duration: Ok(Duration::from_secs(179)),
        has_audio: true,
        max_audio_channels: None,
        fps: Ok(24.0),
        resolution: Some((1280, 720)),
        is_image: false,
        pix_fmt: None,
    };

    let FfmpegEncodeArgs {
        input,
        vcodec,
        vfilter,
        pix_fmt,
        crf,
        preset,
        output_args,
        input_args,
        video_only,
    } = enc.to_ffmpeg_args(32.0, &probe).expect("to_ffmpeg_args");

    assert_eq!(&*vcodec, "libsvtav1");
    assert_eq!(input, enc.input);
    assert_eq!(vfilter, None);
    assert_eq!(crf, 32.0);
    assert_eq!(preset, Some("7".into()));
    assert_eq!(pix_fmt, Some(PixelFormat::Yuv420p));
    assert!(!video_only);

    assert!(
        !output_args.iter().any(|a| a.as_str() == "-g"),
        "unexpected -g in {output_args:?}"
    );
    let svtargs_idx = output_args
        .iter()
        .position(|a| a.as_str() == "-svtav1-params")
        .expect("missing -svtav1-params");
    let svtargs = output_args
        .get(svtargs_idx + 1)
        .expect("missing -svtav1-params value")
        .as_str();
    assert_eq!(svtargs, "scd=0:crf=32");
    assert!(input_args.is_empty());
}

#[test]
fn pixel_format_opt_max() {
    // setup
    use PixelFormat::*;
    // execute / assert
    assert_eq!(
        PixelFormat::opt_max(Some(Yuv420p), Some(Yuv444p10le)),
        Some(Yuv444p10le)
    );
    assert_eq!(
        PixelFormat::opt_max(None, Some(Yuv420p10le)),
        Some(Yuv420p10le)
    );
    assert_eq!(
        PixelFormat::opt_max(Some(Yuv422p10le), None),
        Some(Yuv422p10le)
    );
    assert_eq!(PixelFormat::opt_max(None, None), None);
}

#[test]
fn parse_svt_arg_rejects_reserved_keys() {
    // setup
    // execute
    let err = parse_svt_arg("crf=32").expect_err("crf reserved");
    // assert
    assert!(err.to_string().contains("crf"));
}

#[test]
fn parse_svt_arg_returns_typed_svt_arg() {
    // setup
    // execute
    let arg = parse_svt_arg("film-grain=8");
    // assert
    assert!(matches!(
        arg.as_ref().map(SvtArg::as_str),
        Ok("film-grain=8")
    ));
}

#[test]
fn parse_enc_arg_adds_leading_dash() {
    // setup
    // execute
    let arg = parse_enc_arg("x265-params=lossless=1");
    // assert
    assert!(matches!(
        arg.as_ref().map(EncoderArg::as_str),
        Ok("-x265-params=lossless=1")
    ));
}

#[test]
fn parse_enc_input_arg_returns_typed_input_arg() {
    // setup
    // execute
    let arg = parse_enc_input_arg("hwaccel=none");
    // assert
    assert!(matches!(
        arg.as_ref().map(EncoderInputArg::as_str),
        Ok("-hwaccel=none")
    ));
}

#[test]
fn passthrough_arg_values_split_once_and_keep_hyphen_values() {
    assert_eq!(
        PassthroughArg::parse("-metadata=title=a=b")
            .values()
            .collect::<Vec<_>>(),
        ["-metadata", "title=a=b"]
    );
    assert_eq!(
        PassthroughArg::parse("-profile=-1")
            .values()
            .collect::<Vec<_>>(),
        ["-profile", "-1"]
    );
    assert_eq!(
        PassthroughArg::parse("-dn").values().collect::<Vec<_>>(),
        ["-dn"]
    );
}

#[test]
fn collect_passthrough_args_omits_none_for_default_inputs() {
    let args = [
        EncoderInputArg::from("-hwaccel=none"),
        EncoderInputArg::from("-hwaccel_output_format=none"),
    ];

    assert!(collect_passthrough_args(
        &args,
        &["-hwaccel", "-hwaccel_output_format"]
    )
    .args
    .is_empty());
}

// ab-kgc.30: CLI docs promise 0.1 increment for libvpx-vp9
#[test]
fn libvpx_vp9_default_crf_increment_matches_docs() {
    let enc = Encoder::for_test("libvpx-vp9");
    assert_eq!(
        enc.default_crf_increment(),
        0.1,
        "libvpx-vp9 should use 0.1 crf increment per Args docs"
    );
}

#[test]
fn hevc_videotoolbox_default_max_crf_matches_encoder() {
    let enc = Encoder::for_test("hevc_videotoolbox");
    assert_eq!(enc.default_max_crf(), 100.0);
    assert_eq!(enc.default_min_crf(), 10.0);
}

#[test]
fn to_ffmpeg_args_libvpx_vp9_constant_quality_default() {
    let enc = Encode {
        encoder: Encoder("libvpx-vp9".into()),
        input: "vid.webm".into(),
        vfilter: None,
        preset: None,
        pix_format: None,
        keyint: None,
        scd: None,
        svt_args: vec![],
        enc_args: vec![],
        enc_input_args: vec![],
    };
    let args = enc
        .to_ffmpeg_args(32.0, &test_probe(120, 24.0))
        .expect("to_ffmpeg_args");
    assert!(
        args.output_args
            .windows(2)
            .any(|w| w[0].as_str() == "-b:v" && w[1].as_str() == "0"),
        "libvpx-vp9 needs -b:v 0 for CRF mode: {:?}",
        args.output_args
    );
}

#[cfg(test)]
#[rstest]
#[case::libx264("libx264", 0.1, 10.0, 46.0)]
#[case::libx265("libx265", 0.1, 10.0, 46.0)]
#[case::libsvtav1("libsvtav1", 0.25, 5.0, 70.0)]
#[case::libvpx_vp9("libvpx-vp9", 0.1, 10.0, 55.0)]
#[case::mpeg2("mpeg2video", 1.0, 2.0, 30.0)]
#[case::librav1e("librav1e", 1.0, 10.0, 255.0)]
fn encoder_defaults_matrix(
    #[case] name: &str,
    #[case] increment: f32,
    #[case] min_crf: f32,
    #[case] max_crf: f32,
) {
    // setup
    let enc = Encoder::for_test(name);

    // execute / assert
    assert_eq!(enc.default_crf_increment(), increment);
    assert_eq!(enc.default_min_crf(), min_crf);
    assert_eq!(enc.default_max_crf(), max_crf);
}

#[cfg(test)]
#[rstest]
#[case::svt_alias("svt-av1", "libsvtav1")]
#[case::passthrough("libaom-av1", "libaom-av1")]
fn encoder_from_str_matrix(#[case] input: &str, #[case] expected: &str) {
    // setup
    use std::str::FromStr;

    // execute
    let enc = Encoder::from_str(input).expect("parse encoder");

    // assert
    assert_eq!(enc.as_str(), expected);
}

#[cfg(test)]
fn test_probe(duration_secs: u64, fps: f64) -> Ffprobe {
    Ffprobe {
        duration: Ok(Duration::from_secs(duration_secs)),
        has_audio: true,
        max_audio_channels: Some(2),
        fps: Ok(fps),
        resolution: Some((1280, 720)),
        is_image: false,
        pix_fmt: None,
    }
}

#[test]
fn encode_hint_includes_non_default_options() {
    // setup
    let enc = Encode {
        encoder: Encoder("libx264".into()),
        input: "clip mkv".into(),
        vfilter: Some("scale=1280:-1".into()),
        preset: Some("fast".into()),
        pix_format: Some(PixelFormat::Yuv420p),
        keyint: Some(KeyInterval::Frames(300)),
        scd: Some(true),
        svt_args: vec![],
        enc_args: vec!["x265-params=lossless=1".into()],
        enc_input_args: vec!["hwaccel=vaapi".into()],
    };
    // execute
    let hint = enc.encode_hint(32.5);
    // assert
    assert!(hint.contains("-e libx264"));
    assert!(hint.contains("--preset fast"));
    assert!(hint.contains("--keyint 300"));
    assert!(hint.contains("--scd true"));
    assert!(hint.contains("--pix-format yuv420p"));
    assert!(hint.contains("--vfilter"));
    assert!(hint.contains("--enc x265-params=lossless=1"));
    assert!(hint.contains("--enc-input hwaccel=vaapi"));
}

#[test]
fn encode_hint_omits_default_svtav1_encoder_flag() {
    // setup
    let enc = Encode {
        encoder: Encoder("libsvtav1".into()),
        input: "vid.mp4".into(),
        vfilter: None,
        preset: None,
        pix_format: None,
        keyint: None,
        scd: None,
        svt_args: vec!["film-grain=8".into()],
        enc_args: vec![],
        enc_input_args: vec![],
    };
    // execute
    let hint = enc.encode_hint(32.0);
    // assert
    assert!(!hint.contains("-e libsvtav1"));
    assert!(hint.contains("--svt film-grain=8"));
}

#[test]
fn keyinterval_display_formats_frames_and_duration() {
    // setup / execute / assert
    assert_eq!(KeyInterval::Frames(300).to_string(), "300");
    assert_eq!(
        KeyInterval::Duration(Duration::from_secs(10)).to_string(),
        "10s"
    );
}

#[test]
fn keyinterval_parse_error_reports_both_failures() {
    // setup / execute
    use std::str::FromStr;
    let err = KeyInterval::from_str("not-valid").expect_err("invalid keyint");
    // assert
    let msg = err.to_string();
    assert!(msg.contains("frames:"));
    assert!(msg.contains("duration:"));
}

#[test]
fn keyinterval_frames_variant_passthrough() {
    // setup / execute / assert
    assert_eq!(
        KeyInterval::Frames(240)
            .keyint_number(Ok(30.0))
            .expect("frames"),
        240
    );
}

#[test]
fn to_ffmpeg_args_rejects_svt_on_non_svtav1() {
    // setup
    let enc = Encode {
        encoder: Encoder("libx264".into()),
        input: "vid.mp4".into(),
        vfilter: None,
        preset: None,
        pix_format: None,
        keyint: None,
        scd: None,
        svt_args: vec!["film-grain=8".into()],
        enc_args: vec![],
        enc_input_args: vec![],
    };
    // execute
    let err = enc
        .to_ffmpeg_args(32.0, &test_probe(120, 24.0))
        .expect_err("svt on x264");
    // assert
    assert!(
        err.to_string()
            .contains("--svt may only be used with svt-av1")
    );
}

#[test]
fn to_ffmpeg_args_libaom_defaults() {
    // setup
    let enc = Encode {
        encoder: Encoder("libaom-av1".into()),
        input: "vid.mkv".into(),
        vfilter: None,
        preset: None,
        pix_format: None,
        keyint: None,
        scd: None,
        svt_args: vec![],
        enc_args: vec![],
        enc_input_args: vec![],
    };
    // execute
    let args = enc
        .to_ffmpeg_args(30.0, &test_probe(120, 24.0))
        .expect("to_ffmpeg_args");
    // assert
    assert_eq!(args.pix_fmt, Some(PixelFormat::Yuv420p10le));
    assert!(
        args.output_args
            .windows(2)
            .any(|w| w[0].as_str() == "-b:v" && w[1].as_str() == "0")
    );
    assert!(
        args.output_args
            .iter()
            .all(|a| a.as_str() != "-svtav1-params")
    );
}

#[test]
fn to_ffmpeg_args_qsv_defaults() {
    // setup
    let enc = Encode {
        encoder: Encoder("av1_qsv".into()),
        input: "vid.mp4".into(),
        vfilter: None,
        preset: None,
        pix_format: None,
        keyint: None,
        scd: None,
        svt_args: vec![],
        enc_args: vec![],
        enc_input_args: vec![],
    };
    // execute
    let args = enc
        .to_ffmpeg_args(28.0, &test_probe(120, 24.0))
        .expect("to_ffmpeg_args");
    // assert
    for (flag, val) in [
        ("-look_ahead", "1"),
        ("-extbrc", "1"),
        ("-look_ahead_depth", "40"),
    ] {
        assert!(
            args.output_args
                .windows(2)
                .any(|w| w[0].as_str() == flag && w[1].as_str() == val),
            "missing {flag} in {:?}",
            args.output_args
        );
    }
}

#[test]
fn to_ffmpeg_args_vaapi_input_defaults() {
    // setup
    let enc = Encode {
        encoder: Encoder("h264_vaapi".into()),
        input: "vid.mp4".into(),
        vfilter: None,
        preset: None,
        pix_format: None,
        keyint: None,
        scd: None,
        svt_args: vec![],
        enc_args: vec![],
        enc_input_args: vec![],
    };
    // execute
    let args = enc
        .to_ffmpeg_args(28.0, &test_probe(120, 24.0))
        .expect("to_ffmpeg_args");
    // assert
    assert!(
        args.input_args
            .windows(2)
            .any(|w| w[0].as_str() == "-hwaccel" && w[1].as_str() == "vaapi")
    );
    assert!(
        args.input_args
            .windows(2)
            .any(|w| { w[0].as_str() == "-hwaccel_output_format" && w[1].as_str() == "vaapi" })
    );
}

#[test]
fn to_ffmpeg_args_vulkan_input_defaults() {
    // setup
    let enc = Encode {
        encoder: Encoder("av1_vulkan".into()),
        input: "vid.mp4".into(),
        vfilter: None,
        preset: None,
        pix_format: None,
        keyint: None,
        scd: None,
        svt_args: vec![],
        enc_args: vec![],
        enc_input_args: vec![],
    };
    // execute
    let args = enc
        .to_ffmpeg_args(28.0, &test_probe(120, 24.0))
        .expect("to_ffmpeg_args");
    // assert
    assert!(
        args.input_args
            .windows(2)
            .any(|w| w[0].as_str() == "-hwaccel" && w[1].as_str() == "vulkan")
    );
}

#[test]
fn to_ffmpeg_args_merges_enc_svtav1_params() {
    // setup
    let enc = Encode {
        encoder: Encoder("libsvtav1".into()),
        input: "vid.mp4".into(),
        vfilter: None,
        preset: None,
        pix_format: None,
        keyint: None,
        scd: None,
        svt_args: vec!["film-grain=8".into()],
        enc_args: vec!["svtav1-params=tune=0".into()],
        enc_input_args: vec![],
    };
    // execute
    let args = enc
        .to_ffmpeg_args(32.0, &test_probe(60, 24.0))
        .expect("to_ffmpeg_args");
    // assert
    let svt = args
        .output_args
        .windows(2)
        .find(|w| w[0].as_str() == "-svtav1-params")
        .map(|w| w[1].as_str())
        .expect("svtav1-params");
    assert!(svt.contains("tune=0"));
    assert!(svt.contains("film-grain=8"));
    assert!(svt.contains("crf=32"));
}

#[test]
fn to_ffmpeg_args_enc_input_none_disables_vaapi_defaults() {
    // setup
    let enc = Encode {
        encoder: Encoder("h264_vaapi".into()),
        input: "vid.mp4".into(),
        vfilter: None,
        preset: None,
        pix_format: None,
        keyint: None,
        scd: None,
        svt_args: vec![],
        enc_args: vec![],
        enc_input_args: vec!["-hwaccel=none".into(), "-hwaccel_output_format=none".into()],
    };
    // execute
    let args = enc
        .to_ffmpeg_args(28.0, &test_probe(120, 24.0))
        .expect("to_ffmpeg_args");
    // assert
    assert!(
        !args.input_args.iter().any(|a| a.as_str() == "-hwaccel"),
        "hwaccel defaults should be omitted: {:?}",
        args.input_args
    );
}

#[test]
fn to_ffmpeg_args_rejects_reserved_output_and_input_args() {
    // setup
    let enc_output = Encode {
        encoder: Encoder("libx264".into()),
        input: "vid.mp4".into(),
        vfilter: None,
        preset: None,
        pix_format: None,
        keyint: None,
        scd: None,
        svt_args: vec![],
        enc_args: vec!["-crf=32".into()],
        enc_input_args: vec![],
    };
    let mut enc_input = enc_output.clone();
    enc_input.enc_args = vec![];
    enc_input.enc_input_args = vec!["-preset=fast".into()];

    // execute
    let output_err = enc_output
        .to_ffmpeg_args(32.0, &test_probe(120, 24.0))
        .expect_err("reserved -crf");
    let input_err = enc_input
        .to_ffmpeg_args(32.0, &test_probe(120, 24.0))
        .expect_err("reserved -preset");

    // assert
    assert!(output_err.to_string().contains("`-crf` not allowed"));
    assert!(input_err.to_string().contains("`-preset` not allowed"));
}

#[test]
fn to_ffmpeg_args_explicit_keyint_with_vfilter_fps() {
    // setup
    let enc = Encode {
        encoder: Encoder("libsvtav1".into()),
        input: "vid.mp4".into(),
        vfilter: Some("fps=24".into()),
        preset: None,
        pix_format: None,
        keyint: Some(KeyInterval::Duration(Duration::from_secs(5))),
        scd: None,
        svt_args: vec![],
        enc_args: vec![],
        enc_input_args: vec![],
    };
    // execute
    let args = enc
        .to_ffmpeg_args(32.0, &test_probe(60, 30.0))
        .expect("to_ffmpeg_args");
    // assert: vfilter fps=24 wins over probe fps=30 → 5s * 24 = 120
    assert!(
        args.output_args
            .windows(2)
            .any(|w| w[0].as_str() == "-g" && w[1].as_str() == "120")
    );
}

#[test]
fn to_ffmpeg_args_scd_explicit_true_without_default_keyint() {
    // setup
    let enc = Encode {
        encoder: Encoder("libsvtav1".into()),
        input: "vid.mp4".into(),
        vfilter: None,
        preset: None,
        pix_format: None,
        keyint: None,
        scd: Some(true),
        svt_args: vec![],
        enc_args: vec![],
        enc_input_args: vec![],
    };
    // execute
    let args = enc
        .to_ffmpeg_args(32.0, &test_probe(60, 24.0))
        .expect("to_ffmpeg_args");
    // assert
    let svt = args
        .output_args
        .windows(2)
        .find(|w| w[0].as_str() == "-svtav1-params")
        .map(|w| w[1].as_str())
        .expect("svtav1-params");
    assert!(svt.starts_with("scd=1:"));
}

#[test]
fn encoder_high_crf_means_hq_videotoolbox() {
    // setup / execute / assert
    assert!(Encoder::for_test("hevc_videotoolbox").high_crf_means_hq());
    assert!(!Encoder::for_test("libx264").high_crf_means_hq());
}

#[test]
fn encoder_default_image_ext_x264() {
    // setup / execute / assert
    assert_eq!(Encoder::for_test("libx264").default_image_ext(), "264");
}

#[test]
fn pixel_format_as_str_yuv420p() {
    // setup / execute / assert
    assert_eq!(PixelFormat::Yuv420p.as_str(), "yuv420p");
}
