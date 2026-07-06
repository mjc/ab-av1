//! ffmpeg encoding logic
use crate::{
    command::args::PixelFormat,
    float::TerseF32,
    process::managed::ManagedProcess,
    process::{CommandExt, FfmpegOut, FfmpegOutStream},
    temporary::{self, TempKind},
};
use anyhow::Context;
use log::debug;
use std::{
    collections::HashSet,
    fmt::Write,
    hash::{Hash, Hasher},
    path::{Path, PathBuf},
    sync::{Arc, LazyLock},
};
use tokio::process::Command;

/// Exposed ffmpeg encoding args.
#[derive(Debug, Clone)]
pub struct FfmpegEncodeArgs<'a> {
    pub input: &'a Path,
    pub vcodec: Arc<str>,
    pub vfilter: Option<&'a str>,
    pub pix_fmt: Option<PixelFormat>,
    pub crf: f32,
    pub preset: Option<Arc<str>>,
    pub output_args: Vec<Arc<String>>,
    pub input_args: Vec<Arc<String>>,
    pub video_only: bool,
}

impl FfmpegEncodeArgs<'_> {
    pub fn sample_encode_hash(&self, state: &mut impl Hasher) {
        static SVT_AV1_V: LazyLock<Vec<u8>> = LazyLock::new(|| {
            std::process::Command::new("SvtAv1EncApp")
                .arg("--version")
                .output()
                .map(|o| o.stdout)
                .unwrap_or_default()
        });

        // hashing svt-av1 version means new encoder releases will avoid old cache data
        if &*self.vcodec == "libsvtav1" {
            SVT_AV1_V.hash(state);
        }

        // input not relevant to sample encoding
        self.vcodec.hash(state);
        self.vfilter.hash(state);
        self.pix_fmt.hash(state);
        self.crf.to_bits().hash(state);
        self.preset.hash(state);
        self.output_args.hash(state);
        self.input_args.hash(state);
    }
}

/// Encode a sample.
pub fn encode_sample(
    FfmpegEncodeArgs {
        input,
        vcodec,
        vfilter,
        pix_fmt,
        crf,
        preset,
        output_args,
        input_args,
        video_only: _,
    }: FfmpegEncodeArgs,
    temp_dir: Option<PathBuf>,
    dest_ext: &str,
) -> anyhow::Result<(PathBuf, FfmpegOutStream)> {
    let pre = pre_extension_name(&vcodec);
    let crf_str = format!("{}", TerseF32(crf)).replace('.', "_");
    let dest_file_name = match &preset {
        Some(p) => input.with_extension(format!("{pre}.crf{crf_str}.{p}.{dest_ext}")),
        None => input.with_extension(format!("{pre}.crf{crf_str}.{dest_ext}")),
    };
    let dest_file_name = dest_file_name.file_name().unwrap();
    let mut dest = temporary::process_dir(temp_dir, input.parent().map(Path::to_path_buf));
    dest.push(dest_file_name);

    temporary::add(&dest, TempKind::Keepable);

    let mut cmd = Command::new("ffmpeg");
    cmd.arg("-nostdin")
        .arg("-y")
        .args(input_args.iter().map(|a| &**a))
        .arg2("-i", input)
        .arg2("-c:v", &*vcodec)
        .args(output_args.iter().map(|a| &**a))
        // Avoid dropping or duplicating frames as this may negatively affect input/output analysis
        .arg2("-fps_mode", "passthrough")
        .arg2(vcodec.crf_arg(), vcodec.crf(crf))
        .arg2_opt("-pix_fmt", pix_fmt.map(|v| v.as_str()))
        .arg2_opt(vcodec.preset_arg(), preset)
        .arg2_opt("-vf", vfilter)
        .arg("-an")
        .arg(&dest);
    let cmd_str = cmd.to_cmd_str();
    debug!("cmd `{cmd_str}`");

    let enc = ManagedProcess::spawn("ffmpeg encode_sample", cmd).context("ffmpeg encode_sample")?;

    let stream = FfmpegOut::stream(enc, "ffmpeg encode_sample", cmd_str);
    Ok((dest, stream))
}

/// Encode to output.
pub fn encode(
    FfmpegEncodeArgs {
        input,
        vcodec,
        vfilter,
        pix_fmt,
        crf,
        preset,
        output_args,
        input_args,
        video_only,
    }: FfmpegEncodeArgs,
    output: &Path,
    has_audio: bool,
    audio_codec: Option<&str>,
    downmix_to_stereo: bool,
) -> anyhow::Result<FfmpegOutStream> {
    let oargs: HashSet<_> = output_args.iter().map(|a| a.as_str()).collect();
    let output_ext = output.extension().and_then(|e| e.to_str());

    let add_faststart = output_ext == Some("mp4") && !oargs.contains("-movflags");
    let matroska = matches!(output_ext, Some("mkv") | Some("webm"));
    let add_cues_to_front = matroska && !oargs.contains("-cues_to_front");

    let audio_codec = audio_codec.unwrap_or(if downmix_to_stereo && has_audio {
        "libopus"
    } else {
        "copy"
    });

    let set_ba_128k = audio_codec == "libopus" && !oargs.contains("-b:a");
    let downmix_to_stereo = downmix_to_stereo && !oargs.contains("-ac");
    let map = match video_only {
        true => "0:v:0",
        false => "0",
    };
    // This doesn't seem to work on .mp4 files
    let mut metadata = format!(
        "AB_AV1_FFMPEG_ARGS=-c:v {vcodec} {} {crf}",
        vcodec.crf_arg()
    );
    if let Some(preset) = &preset {
        write!(&mut metadata, " {} {preset}", vcodec.preset_arg()).unwrap();
    }

    let mut cmd = Command::new("ffmpeg");
    cmd.arg("-nostdin")
        .args(input_args.iter().map(|a| &**a))
        .arg("-y")
        .arg2("-i", input)
        .arg2("-map", map)
        .arg2("-c:v", "copy")
        .arg2("-c:v:0", &*vcodec)
        .arg2("-metadata", metadata)
        .arg2("-c:a", audio_codec)
        .arg2("-c:s", "copy")
        .args(output_args.iter().map(|a| &**a))
        .arg2(vcodec.crf_arg(), vcodec.crf(crf))
        .arg2_opt("-pix_fmt", pix_fmt.map(|v| v.as_str()))
        .arg2_opt(vcodec.preset_arg(), preset)
        .arg2_opt("-vf", vfilter)
        .arg_if(matroska, "-dn") // "Only audio, video, and subtitles are supported for Matroska"
        .arg2_if(downmix_to_stereo, "-ac", 2)
        .arg2_if(set_ba_128k, "-b:a", "128k")
        .arg2_if(add_faststart, "-movflags", "+faststart")
        .arg2_if(add_cues_to_front, "-cues_to_front", "y")
        .arg(output);
    let cmd_str = cmd.to_cmd_str();
    debug!("cmd `{cmd_str}`");

    let enc = ManagedProcess::spawn("ffmpeg encode", cmd).context("ffmpeg encode")?;

    Ok(FfmpegOut::stream(enc, "ffmpeg encode", cmd_str))
}

pub fn pre_extension_name(vcodec: &str) -> &str {
    match vcodec.strip_prefix("lib").filter(|s| !s.is_empty()) {
        Some("svtav1") => "av1",
        Some("vpx-vp9") => "vp9",
        Some(suffix) => suffix,
        _ => vcodec,
    }
}

trait VCodecSpecific {
    /// Arg to use preset values with, normally `-preset`.
    fn preset_arg(&self) -> &str;
    /// Arg to use crf values with, normally `-crf`.
    fn crf_arg(&self) -> &str;
    /// crf value to pass to ffmpeg.
    fn crf(&self, crf: f32) -> f32;
}
impl VCodecSpecific for Arc<str> {
    fn preset_arg(&self) -> &str {
        match &**self {
            "libaom-av1" | "libvpx-vp9" => "-cpu-used",
            "librav1e" => "-speed",
            _ => "-preset",
        }
    }

    fn crf_arg(&self) -> &str {
        // use crf-like args to support encoders that don't have crf
        match &**self {
            // https://ffmpeg.org//ffmpeg-codecs.html#librav1e
            // https://github.com/fraunhoferhhi/vvenc/wiki/FFmpeg-Integration#fix-qp-mode-constant-quality-mode
            "librav1e" | "libvvenc" => "-qp",
            "mpeg2video" => "-q",
            "hevc_videotoolbox" => "-q:v",
            // https://ffmpeg.org//ffmpeg-codecs.html#VAAPI-encoders
            e if e.ends_with("_vaapi") => "-q",
            e if e.ends_with("_vulkan") => "-qp",
            e if e.ends_with("_nvenc") => "-cq",
            // https://ffmpeg.org//ffmpeg-codecs.html#QSV-Encoders
            e if e.ends_with("_qsv") => "-global_quality",
            _ => "-crf",
        }
    }

    fn crf(&self, crf: f32) -> f32 {
        match &**self {
            // ffmpeg svt-av1 crf above 63 don't work, but up to 70 does work in -svtav1-params
            "libsvtav1" => crf.min(63.0),
            _ => crf,
        }
    }
}

pub fn remove_arg(args: &mut Vec<Arc<String>>, arg: &'static str) {
    let mut retain_next = true;
    args.retain(|a| {
        if **a == arg {
            retain_next = false;
            false
        } else if !retain_next {
            retain_next = true;
            false
        } else {
            true
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use rstest::rstest;
    use std::sync::Arc;

    #[test]
    fn pre_extension_name_maps_codecs() {
        // setup / execute / assert
        assert_eq!(pre_extension_name("libsvtav1"), "av1");
        assert_eq!(pre_extension_name("libvpx-vp9"), "vp9");
        assert_eq!(pre_extension_name("libx264"), "x264");
    }

    #[rstest]
    #[case::libsvtav1("libsvtav1", "-crf", "-preset", 63.0)]
    #[case::librav1e("librav1e", "-qp", "-speed", 40.0)]
    #[case::libx264("libx264", "-crf", "-preset", 32.0)]
    #[case::hevc_vt("hevc_videotoolbox", "-q:v", "-preset", 50.0)]
    fn vcodec_arg_matrix(
        #[case] codec: &str,
        #[case] crf_arg: &str,
        #[case] preset_arg: &str,
        #[case] crf_in: f32,
    ) {
        // setup
        let vcodec: Arc<str> = Arc::from(codec);

        // execute / assert
        assert_eq!(VCodecSpecific::crf_arg(&vcodec), crf_arg);
        assert_eq!(VCodecSpecific::preset_arg(&vcodec), preset_arg);
        assert_eq!(VCodecSpecific::crf(&vcodec, crf_in), crf_in.min(if codec == "libsvtav1" { 63.0 } else { crf_in }));
    }

    #[test]
    fn sample_encode_hash_stable_for_identical_args() {
        // setup
        use std::collections::hash_map::DefaultHasher;
        use std::hash::Hasher;

        let input = std::path::Path::new("/tmp/vid.mkv");
        let args = FfmpegEncodeArgs {
            input,
            vcodec: Arc::from("libsvtav1"),
            vfilter: Some("scale=1280:-1"),
            pix_fmt: None,
            crf: 30.0,
            preset: Some(Arc::from("8")),
            output_args: vec![],
            input_args: vec![],
            video_only: false,
        };

        // execute
        let mut hasher_a = DefaultHasher::new();
        let mut hasher_b = DefaultHasher::new();
        args.sample_encode_hash(&mut hasher_a);
        args.sample_encode_hash(&mut hasher_b);

        // assert
        assert_eq!(hasher_a.finish(), hasher_b.finish());
    }

    #[test]
    fn sample_encode_hash_differs_when_crf_changes() {
        // setup
        use std::collections::hash_map::DefaultHasher;
        use std::hash::Hasher;

        let input = std::path::Path::new("/tmp/vid.mkv");
        let base = FfmpegEncodeArgs {
            input,
            vcodec: Arc::from("libsvtav1"),
            vfilter: None,
            pix_fmt: None,
            crf: 30.0,
            preset: None,
            output_args: vec![],
            input_args: vec![],
            video_only: false,
        };
        let mut other = base.clone();
        other.crf = 31.0;

        // execute
        let mut hash_a = DefaultHasher::new();
        let mut hash_b = DefaultHasher::new();
        base.sample_encode_hash(&mut hash_a);
        other.sample_encode_hash(&mut hash_b);

        // assert
        assert_ne!(hash_a.finish(), hash_b.finish());
    }

    // ab-kgc.24: duplicate ffmpeg flags should survive single removal
    #[test]
    fn remove_arg_strips_only_first_matching_pair() {
        // setup — duplicate flags should require multiple removals or retain extras
        let mut args = vec![
            Arc::new("-preset".to_string()),
            Arc::new("8".to_string()),
            Arc::new("-preset".to_string()),
            Arc::new("6".to_string()),
            Arc::new("-crf".to_string()),
            Arc::new("30".to_string()),
        ];

        // execute
        remove_arg(&mut args, "-preset");

        // assert — second -preset pair must survive one removal
        assert_eq!(
            args.iter().map(|a| a.as_str()).collect::<Vec<_>>(),
            vec!["-preset", "6", "-crf", "30"]
        );
    }

    #[test]
    fn remove_arg_strips_flag_and_value() {
        // setup
        let mut args = vec![
            Arc::new("-preset".to_string()),
            Arc::new("8".to_string()),
            Arc::new("-crf".to_string()),
            Arc::new("30".to_string()),
        ];

        // execute
        remove_arg(&mut args, "-preset");

        // assert
        assert_eq!(
            args.iter().map(|a| a.as_str()).collect::<Vec<_>>(),
            vec!["-crf", "30"]
        );
    }

    // ab-kgc.71: libaom-av1 sample filenames should use av1 suffix
    #[test]
    fn pre_extension_name_libaom_av1_uses_av1_suffix() {
        assert_eq!(pre_extension_name("libaom-av1"), "av1");
    }

    // ab-kgc.72: libdav1d codec names should produce av1 extension
    #[test]
    fn pre_extension_name_libdav1d_uses_av1_suffix() {
        assert_eq!(pre_extension_name("libdav1d"), "av1");
    }

    // ab-kgc.73: bare svtav1 encoder names should normalize like libsvtav1
    #[test]
    fn pre_extension_name_bare_svtav1_uses_av1_suffix() {
        assert_eq!(pre_extension_name("svtav1"), "av1");
    }

    // ab-kgc.76: remove_arg must not consume a value token that looks like another flag
    #[test]
    fn remove_arg_preserves_value_when_it_matches_a_flag_name() {
        let mut args = vec![
            Arc::new("-preset".to_string()),
            Arc::new("-crf".to_string()),
            Arc::new("30".to_string()),
        ];

        remove_arg(&mut args, "-preset");

        assert_eq!(
            args.iter().map(|a| a.as_str()).collect::<Vec<_>>(),
            vec!["-crf", "30"]
        );
    }

    #[test]
    fn sample_encode_hash_differs_when_output_args_change() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::Hasher;

        let input = std::path::Path::new("/tmp/vid.mkv");
        let base = FfmpegEncodeArgs {
            input,
            vcodec: Arc::from("libx264"),
            vfilter: None,
            pix_fmt: None,
            crf: 30.0,
            preset: None,
            output_args: vec![],
            input_args: vec![],
            video_only: false,
        };
        let mut other = base.clone();
        other.output_args = vec![Arc::from("-g".to_string()), Arc::from("240".to_string())];

        let mut hash_a = DefaultHasher::new();
        let mut hash_b = DefaultHasher::new();
        base.sample_encode_hash(&mut hash_a);
        other.sample_encode_hash(&mut hash_b);

        assert_ne!(hash_a.finish(), hash_b.finish());
    }

    #[test]
    fn sample_encode_hash_differs_when_input_args_change() {
        use std::collections::hash_map::DefaultHasher;
        use std::hash::Hasher;

        let input = std::path::Path::new("/tmp/vid.mkv");
        let base = FfmpegEncodeArgs {
            input,
            vcodec: Arc::from("libx264"),
            vfilter: None,
            pix_fmt: None,
            crf: 30.0,
            preset: None,
            output_args: vec![],
            input_args: vec![],
            video_only: false,
        };
        let mut other = base.clone();
        other.input_args = vec![Arc::from("-ss".to_string()), Arc::from("12".to_string())];

        let mut hash_a = DefaultHasher::new();
        let mut hash_b = DefaultHasher::new();
        base.sample_encode_hash(&mut hash_a);
        other.sample_encode_hash(&mut hash_b);

        assert_ne!(hash_a.finish(), hash_b.finish());
    }

    #[rstest]
    #[case::libaom_av1("libaom-av1", "-cpu-used", "-crf")]
    #[case::mpeg2video("mpeg2video", "-preset", "-q")]
    #[case::h264_nvenc("h264_nvenc", "-preset", "-cq")]
    #[case::h264_vaapi("h264_vaapi", "-preset", "-q")]
    #[case::h264_qsv("h264_qsv", "-preset", "-global_quality")]
    fn vcodec_arg_matrix_extended(
        #[case] codec: &str,
        #[case] preset_arg: &str,
        #[case] crf_arg: &str,
    ) {
        let vcodec: Arc<str> = Arc::from(codec);
        assert_eq!(VCodecSpecific::preset_arg(&vcodec), preset_arg);
        assert_eq!(VCodecSpecific::crf_arg(&vcodec), crf_arg);
    }

    // ab-kgc.74: bare libvpx should normalize like libvpx-vp9
    #[test]
    fn pre_extension_name_libvpx_is_vp9() {
        assert_eq!(pre_extension_name("libvpx"), "vp9");
    }

    // ab-kgc.75: sample cache filenames must distinguish libaom from libsvtav1
    #[test]
    fn pre_extension_name_libaom_and_svtav1_share_av1_suffix() {
        assert_eq!(pre_extension_name("libaom-av1"), pre_extension_name("libsvtav1"));
    }

    #[test]
    fn libsvtav1_crf_caps_above_ffmpeg_limit() {
        let vcodec: Arc<str> = Arc::from("libsvtav1");
        assert_eq!(VCodecSpecific::crf(&vcodec, 70.0), 63.0);
        assert_eq!(VCodecSpecific::crf(&vcodec, 63.0), 63.0);
    }
}
