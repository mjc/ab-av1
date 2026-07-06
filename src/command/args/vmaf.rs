use crate::command::args::PixelFormat;
use anyhow::Context;
use clap::Parser;
use std::{borrow::Cow, fmt::Display, sync::Arc, thread};

const DEFAULT_VMAF_FPS: f32 = 25.0;

/// Common vmaf options.
#[derive(Debug, Parser, Clone)]
pub struct Vmaf {
    /// Set to calculate vmaf when it would otherwise not be, e.g. when calculating xpsnr.
    /// So using this allows both vmaf & xpsnr to be calculated at the same time.
    // TODO: nicer if named "--vmaf"
    #[arg(long, num_args=0..=1, default_missing_value = "true")]
    pub and_vmaf: Option<bool>,

    /// Additional vmaf arg(s). E.g. --vmaf n_threads=8 --vmaf n_subsample=4
    ///
    /// By default `n_threads` is set to available system threads.
    ///
    /// Also see https://ffmpeg.org/ffmpeg-filters.html#libvmaf.
    #[arg(long = "vmaf", value_parser = parse_vmaf_arg)]
    pub vmaf_args: Vec<Arc<str>>,

    /// Video resolution scale to use in VMAF analysis. If set, video streams will be bicubic
    /// scaled to this during VMAF analysis. `auto` (default) automatically sets
    /// based on the model and input video resolution. `none` disables any scaling.
    /// `WxH` format may be used to specify custom scaling, e.g. `1920x1080`.
    ///
    /// auto behaviour:
    /// * 1k model (default for resolutions <= 2560x1440) if width and height
    ///   are less than 1728 & 972 respectively upscale to 1080p. Otherwise no scaling.
    /// * 4k model (default for resolutions > 2560x1440) if width and height
    ///   are less than 3456 & 1944 respectively upscale to 4k. Otherwise no scaling.
    ///
    /// The auto behaviour is based on the distorted video dimensions, equivalent
    /// to post input/reference vfilter dimensions.
    ///
    /// Scaling happens after any input/reference vfilters.
    #[arg(long, default_value_t, value_parser = parse_vmaf_scale)]
    pub vmaf_scale: VmafScale,

    /// Frame rate override used to analyse both reference & distorted videos.
    /// Maps to ffmpeg `-r` input arg.
    ///
    /// Setting to 0 disables use.
    #[arg(long, default_value_t = DEFAULT_VMAF_FPS)]
    pub vmaf_fps: f32,
}

impl Default for Vmaf {
    fn default() -> Self {
        Self {
            and_vmaf: None,
            vmaf_args: <_>::default(),
            vmaf_scale: <_>::default(),
            vmaf_fps: DEFAULT_VMAF_FPS,
        }
    }
}

impl std::hash::Hash for Vmaf {
    fn hash<H: std::hash::Hasher>(&self, state: &mut H) {
        self.and_vmaf.hash(state);
        self.vmaf_args.hash(state);
        self.vmaf_scale.hash(state);
        self.vmaf_fps.to_ne_bytes().hash(state);
    }
}

fn parse_vmaf_arg(arg: &str) -> anyhow::Result<Arc<str>> {
    Ok(arg.to_owned().into())
}

impl Vmaf {
    pub fn fps(&self) -> Option<f32> {
        Some(self.vmaf_fps).filter(|r| *r > 0.0)
    }

    /// Returns ffmpeg `filter_complex`/`lavfi` value for calculating vmaf.
    pub fn ffmpeg_lavfi(
        &self,
        distorted_res: Option<(u32, u32)>,
        pix_fmt: Option<PixelFormat>,
        ref_vfilter: Option<&str>,
    ) -> String {
        let mut args = self.vmaf_args.clone();
        if !args.iter().any(|a| a.contains("n_threads")) {
            // default n_threads to all cores
            args.push(
                format!(
                    "n_threads={}",
                    thread::available_parallelism().map_or(1, |p| p.get())
                )
                .into(),
            );
        }
        let mut lavfi = args.join(":");
        lavfi.insert_str(0, "libvmaf=shortest=true:ts_sync_mode=nearest:");

        let mut model = VmafModel::from_args(&args);
        if let (None, Some((w, h))) = (model, distorted_res)
            && w > 2560
            && h > 1440
        {
            // for >2k resolutions use 4k model
            lavfi.push_str(":model=version=vmaf_4k_v0.6.1");
            model = Some(VmafModel::Vmaf4K);
        }

        let ref_vf: Cow<_> = match ref_vfilter {
            None => "".into(),
            Some(vf) if vf.ends_with(',') => vf.into(),
            Some(vf) => format!("{vf},").into(),
        };
        let format = pix_fmt.map(|v| format!("format={v},")).unwrap_or_default();
        let scale = self
            .vf_scale(model.unwrap_or_default(), distorted_res)
            .map(|(w, h)| format!("scale={w}:{h}:flags=bicubic,"))
            .unwrap_or_default();

        // prefix:
        // * Add reference-vfilter if any
        // * convert both streams to common pixel format
        // * scale to vmaf width if necessary
        // * sync presentation timestamp
        let prefix = format!(
            "[0:v]{format}{scale}setpts=PTS-STARTPTS,settb=AVTB[dis];\
             [1:v]{format}{ref_vf}{scale}setpts=PTS-STARTPTS,settb=AVTB[ref];\
             [dis][ref]"
        );

        lavfi.insert_str(0, &prefix);
        lavfi
    }

    fn vf_scale(&self, model: VmafModel, distorted_res: Option<(u32, u32)>) -> Option<(i32, i32)> {
        match (self.vmaf_scale, distorted_res) {
            (VmafScale::Auto, Some((w, h))) => match model {
                // upscale small resolutions to 1k for use with the 1k model
                VmafModel::Vmaf1K if w < 1728 && h < 972 => {
                    Some(minimally_scale((w, h), (1920, 1080)))
                }
                // upscale small resolutions to 4k for use with the 4k model
                VmafModel::Vmaf4K if w < 3456 && h < 1944 => {
                    Some(minimally_scale((w, h), (3840, 2160)))
                }
                _ => None,
            },
            (VmafScale::Custom { width, height }, Some((w, h))) => {
                Some(minimally_scale((w, h), (width, height)))
            }
            (VmafScale::Custom { width, height }, None) => Some((width as _, height as _)),
            _ => None,
        }
    }
}

/// Return the smallest ffmpeg vf `(w, h)` scale values so that at least one of the
/// `target_w` or `target_h` bounds are met.
fn minimally_scale((from_w, from_h): (u32, u32), (target_w, target_h): (u32, u32)) -> (i32, i32) {
    let w_factor = from_w as f64 / target_w as f64;
    let h_factor = from_h as f64 / target_h as f64;
    if h_factor > w_factor {
        (-1, target_h as _) // scale vertically
    } else {
        (target_w as _, -1) // scale horizontally
    }
}

#[derive(Default, Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum VmafScale {
    None,
    #[default]
    Auto,
    Custom {
        width: u32,
        height: u32,
    },
}

fn parse_vmaf_scale(vs: &str) -> anyhow::Result<VmafScale> {
    const ERR: &str = "vmaf-scale must be 'none', 'auto' or WxH format e.g. '1920x1080'";
    match vs {
        "none" => Ok(VmafScale::None),
        "auto" => Ok(VmafScale::Auto),
        _ => {
            let (w, h) = vs.split_once('x').context(ERR)?;
            let (width, height) = (w.parse().context(ERR)?, h.parse().context(ERR)?);
            Ok(VmafScale::Custom { width, height })
        }
    }
}

impl Display for VmafScale {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::None => "none".fmt(f),
            Self::Auto => "auto".fmt(f),
            Self::Custom { width, height } => write!(f, "{width}x{height}"),
        }
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Hash)]
enum VmafModel {
    /// Default 1080p model.
    #[default]
    Vmaf1K,
    /// 4k model.
    Vmaf4K,
    /// Some other user specified model.
    Custom,
}

impl VmafModel {
    fn from_args(args: &[Arc<str>]) -> Option<Self> {
        let mut using_custom_model: Vec<_> = args
            .iter()
            .filter(|v| is_vmaf_model_override(v))
            .collect();

        match using_custom_model.len() {
            0 => None,
            1 => Some(match using_custom_model.remove(0) {
                v if v.ends_with("version=vmaf_v0.6.1") => Self::Vmaf1K,
                v if v.ends_with("version=vmaf_4k_v0.6.1") => Self::Vmaf4K,
                _ => Self::Custom,
            }),
            _ => Some(Self::Custom),
        }
    }
}

/// True when a libvmaf arg explicitly selects a model (not e.g. `phone_model=1`).
fn is_vmaf_model_override(arg: &str) -> bool {
    arg.split(':').any(|part| part.starts_with("model="))
}

#[test]
fn vmaf_lavfi() {
    let vmaf = Vmaf {
        vmaf_args: vec!["n_threads=5".into(), "n_subsample=4".into()],
        ..<_>::default()
    };
    assert_eq!(
        vmaf.ffmpeg_lavfi(
            None,
            Some(PixelFormat::Yuv420p),
            Some("scale=1280:-1,fps=24")
        ),
        "[0:v]format=yuv420p,setpts=PTS-STARTPTS,settb=AVTB[dis];\
         [1:v]format=yuv420p,scale=1280:-1,fps=24,setpts=PTS-STARTPTS,settb=AVTB[ref];\
         [dis][ref]libvmaf=shortest=true:ts_sync_mode=nearest:n_threads=5:n_subsample=4"
    );
}

#[test]
fn vmaf_lavfi_default() {
    let vmaf = Vmaf::default();
    let expected = format!(
        "[0:v]setpts=PTS-STARTPTS,settb=AVTB[dis];\
         [1:v]setpts=PTS-STARTPTS,settb=AVTB[ref];\
         [dis][ref]libvmaf=shortest=true:ts_sync_mode=nearest:n_threads={}",
        thread::available_parallelism().map_or(1, |p| p.get())
    );
    assert_eq!(vmaf.ffmpeg_lavfi(None, None, None), expected);
}

#[test]
fn vmaf_lavfi_default_pix_fmt() {
    let vmaf = Vmaf::default();
    let expected = format!(
        "[0:v]format=yuv420p10le,setpts=PTS-STARTPTS,settb=AVTB[dis];\
         [1:v]format=yuv420p10le,setpts=PTS-STARTPTS,settb=AVTB[ref];\
         [dis][ref]libvmaf=shortest=true:ts_sync_mode=nearest:n_threads={}",
        thread::available_parallelism().map_or(1, |p| p.get())
    );
    assert_eq!(
        vmaf.ffmpeg_lavfi(None, Some(PixelFormat::Yuv420p10le), None),
        expected
    );
}

#[test]
fn vmaf_lavfi_include_n_threads() {
    let vmaf = Vmaf {
        vmaf_args: vec!["log_path=output.xml".into()],
        ..<_>::default()
    };
    let expected = format!(
        "[0:v]format=yuv420p,setpts=PTS-STARTPTS,settb=AVTB[dis];\
         [1:v]format=yuv420p,setpts=PTS-STARTPTS,settb=AVTB[ref];\
         [dis][ref]libvmaf=shortest=true:ts_sync_mode=nearest:log_path=output.xml:n_threads={}",
        thread::available_parallelism().map_or(1, |p| p.get())
    );
    assert_eq!(
        vmaf.ffmpeg_lavfi(None, Some(PixelFormat::Yuv420p), None),
        expected
    );
}

/// Low resolution videos should be upscaled to 1080p
#[test]
fn vmaf_lavfi_small_width() {
    let vmaf = Vmaf {
        vmaf_args: vec!["n_threads=5".into(), "n_subsample=4".into()],
        ..<_>::default()
    };
    assert_eq!(
        vmaf.ffmpeg_lavfi(Some((1280, 720)), Some(PixelFormat::Yuv420p), None),
        "[0:v]format=yuv420p,scale=1920:-1:flags=bicubic,setpts=PTS-STARTPTS,settb=AVTB[dis];\
         [1:v]format=yuv420p,scale=1920:-1:flags=bicubic,setpts=PTS-STARTPTS,settb=AVTB[ref];\
         [dis][ref]libvmaf=shortest=true:ts_sync_mode=nearest:n_threads=5:n_subsample=4"
    );
}

/// 4k videos should use 4k model
#[test]
fn vmaf_lavfi_4k() {
    let vmaf = Vmaf {
        vmaf_args: vec!["n_threads=5".into(), "n_subsample=4".into()],
        ..<_>::default()
    };
    assert_eq!(
        vmaf.ffmpeg_lavfi(Some((3840, 2160)), Some(PixelFormat::Yuv420p), None),
        "[0:v]format=yuv420p,setpts=PTS-STARTPTS,settb=AVTB[dis];\
         [1:v]format=yuv420p,setpts=PTS-STARTPTS,settb=AVTB[ref];\
         [dis][ref]libvmaf=shortest=true:ts_sync_mode=nearest:n_threads=5:n_subsample=4:model=version=vmaf_4k_v0.6.1"
    );
}

/// >2k videos should be upscaled to 4k & use 4k model
#[test]
fn vmaf_lavfi_3k_upscale_to_4k() {
    let vmaf = Vmaf {
        vmaf_args: vec!["n_threads=5".into()],
        ..<_>::default()
    };
    assert_eq!(
        vmaf.ffmpeg_lavfi(Some((3008, 1692)), Some(PixelFormat::Yuv420p), None),
        "[0:v]format=yuv420p,scale=3840:-1:flags=bicubic,setpts=PTS-STARTPTS,settb=AVTB[dis];\
         [1:v]format=yuv420p,scale=3840:-1:flags=bicubic,setpts=PTS-STARTPTS,settb=AVTB[ref];\
         [dis][ref]libvmaf=shortest=true:ts_sync_mode=nearest:n_threads=5:model=version=vmaf_4k_v0.6.1"
    );
}

/// If user has overridden the model, don't default a vmaf width
#[test]
fn vmaf_lavfi_small_width_custom_model() {
    let vmaf = Vmaf {
        vmaf_args: vec![
            "model=version=foo".into(),
            "n_threads=5".into(),
            "n_subsample=4".into(),
        ],
        ..<_>::default()
    };
    assert_eq!(
        vmaf.ffmpeg_lavfi(Some((1280, 720)), Some(PixelFormat::Yuv420p), None),
        "[0:v]format=yuv420p,setpts=PTS-STARTPTS,settb=AVTB[dis];\
         [1:v]format=yuv420p,setpts=PTS-STARTPTS,settb=AVTB[ref];\
         [dis][ref]libvmaf=shortest=true:ts_sync_mode=nearest:model=version=foo:n_threads=5:n_subsample=4"
    );
}

#[test]
fn vmaf_lavfi_custom_model_and_width() {
    let vmaf = Vmaf {
        vmaf_args: vec![
            "model=version=foo".into(),
            "n_threads=5".into(),
            "n_subsample=4".into(),
        ],
        // if specified just do it
        vmaf_scale: VmafScale::Custom {
            width: 123,
            height: 720,
        },
        ..<_>::default()
    };
    assert_eq!(
        vmaf.ffmpeg_lavfi(Some((1280, 720)), Some(PixelFormat::Yuv420p), None),
        "[0:v]format=yuv420p,scale=123:-1:flags=bicubic,setpts=PTS-STARTPTS,settb=AVTB[dis];\
         [1:v]format=yuv420p,scale=123:-1:flags=bicubic,setpts=PTS-STARTPTS,settb=AVTB[ref];\
         [dis][ref]libvmaf=shortest=true:ts_sync_mode=nearest:model=version=foo:n_threads=5:n_subsample=4"
    );
}

#[test]
fn vmaf_lavfi_1080p() {
    let vmaf = Vmaf {
        vmaf_args: vec!["n_threads=5".into(), "n_subsample=4".into()],
        ..<_>::default()
    };
    assert_eq!(
        vmaf.ffmpeg_lavfi(Some((1920, 1080)), Some(PixelFormat::Yuv420p), None),
        "[0:v]format=yuv420p,setpts=PTS-STARTPTS,settb=AVTB[dis];\
         [1:v]format=yuv420p,setpts=PTS-STARTPTS,settb=AVTB[ref];\
         [dis][ref]libvmaf=shortest=true:ts_sync_mode=nearest:n_threads=5:n_subsample=4"
    );
}

// bug-hunt-red → ab-kgc.79: width >2560 must select 4k VMAF model
#[test]
fn vmaf_lavfi_wide_2561x1440_selects_4k_model() {
    let vmaf = Vmaf::default();
    let lavfi = vmaf.ffmpeg_lavfi(Some((2561, 1440)), Some(PixelFormat::Yuv420p), None);
    assert!(
        lavfi.contains("model=version=vmaf_4k_v0.6.1"),
        "2561x1440 should use the 4k model: {lavfi}"
    );
}

// ab-kgc.80: ultrawide 4k-width sources must not stay on the 1k model
#[test]
fn vmaf_lavfi_ultrawide_3840x1440_selects_4k_model() {
    let vmaf = Vmaf::default();
    let lavfi = vmaf.ffmpeg_lavfi(Some((3840, 1440)), Some(PixelFormat::Yuv420p), None);
    assert!(
        lavfi.contains("model=version=vmaf_4k_v0.6.1"),
        "3840x1440 should use the 4k model: {lavfi}"
    );
}

#[test]
fn vmaf_fps_zero_disables_override() {
    let vmaf = Vmaf {
        vmaf_fps: 0.0,
        ..Default::default()
    };
    assert_eq!(vmaf.fps(), None);
}

#[test]
fn vmaf_lavfi_scale_none_skips_auto_upscale() {
    let vmaf = Vmaf {
        vmaf_scale: VmafScale::None,
        ..Default::default()
    };
    let lavfi = vmaf.ffmpeg_lavfi(Some((1280, 720)), Some(PixelFormat::Yuv420p), None);
    assert!(
        !lavfi.contains("scale="),
        "vmaf-scale=none must disable auto upscale: {lavfi}"
    );
}

#[test]
fn vmaf_lavfi_exact_2k_boundary_uses_1k_model() {
    let vmaf = Vmaf::default();
    let lavfi = vmaf.ffmpeg_lavfi(Some((2560, 1440)), Some(PixelFormat::Yuv420p), None);
    assert!(
        !lavfi.contains("model=version=vmaf_4k_v0.6.1"),
        "exact 2560x1440 should remain on the 1k model: {lavfi}"
    );
}

// ab-kgc.81: portrait 720p must upscale height for the 1k model
#[test]
fn vmaf_lavfi_portrait_720x1280_auto_upscales_height() {
    let vmaf = Vmaf::default();
    let lavfi = vmaf.ffmpeg_lavfi(Some((720, 1280)), Some(PixelFormat::Yuv420p), None);
    assert!(
        lavfi.contains("scale=-1:1080:flags=bicubic"),
        "portrait sources should upscale to 1080p height: {lavfi}"
    );
}

#[test]
fn vmaf_fps_negative_disables_use() {
    let vmaf = Vmaf {
        vmaf_fps: -1.0,
        ..Default::default()
    };
    assert_eq!(vmaf.fps(), None);
}

#[test]
fn vmaf_hash_distinguishes_and_vmaf_flag() {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    let mut off = Vmaf::default();
    off.and_vmaf = None;
    let mut on = Vmaf::default();
    on.and_vmaf = Some(true);

    let hash = |v: &Vmaf| {
        let mut h = DefaultHasher::new();
        v.hash(&mut h);
        h.finish()
    };
    assert_ne!(hash(&off), hash(&on));
}

// ab-kgc.78: parse_vmaf_scale must reject zero WxH dimensions
#[test]
fn parse_vmaf_scale_rejects_zero_dimensions() {
    let err = parse_vmaf_scale("0x1080").expect_err("zero width");
    assert!(err.to_string().contains("vmaf-scale"));
    let err = parse_vmaf_scale("1920x0").expect_err("zero height");
    assert!(err.to_string().contains("vmaf-scale"));
}

#[test]
fn vmaf_phone_model_does_not_count_as_model_override() {
    let vmaf = Vmaf {
        vmaf_args: vec!["phone_model=1".into(), "n_threads=5".into()],
        ..Default::default()
    };
    let lavfi = vmaf.ffmpeg_lavfi(Some((3840, 2160)), Some(PixelFormat::Yuv420p), None);
    assert!(
        lavfi.contains("model=version=vmaf_4k_v0.6.1"),
        "phone_model=1 must not suppress automatic 4k model selection: {lavfi}"
    );
}
