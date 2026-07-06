use crate::{
    command::{
        PROGRESS_CHARS,
        args::{self, PixelFormat, ScoreConfig},
    },
    ffprobe,
    log::ProgressLogger,
    process::FfmpegOut,
    xpsnr::{self, XpsnrOut},
};
use anyhow::Context;
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use std::{
    fmt::Write,
    path::PathBuf,
    pin::pin,
    time::{Duration, Instant},
};
use tokio_stream::StreamExt;

/// Full XPSNR score calculation, distorted file vs reference file.
/// Works with videos and images.
#[derive(Parser)]
#[clap(verbatim_doc_comment)]
#[group(skip)]
pub struct Args {
    /// Reference video file.
    #[arg(long)]
    pub reference: PathBuf,

    /// Re-encoded/distorted video file.
    #[arg(long)]
    pub distorted: PathBuf,

    #[clap(flatten)]
    pub score: args::ScoreArgs,

    #[clap(flatten)]
    pub xpsnr: args::Xpsnr,
}

#[derive(Debug, Clone)]
pub struct XpsnrConfig {
    reference: PathBuf,
    distorted: PathBuf,
    score: ScoreConfig,
    xpsnr: args::XpsnrConfig,
}

impl From<Args> for XpsnrConfig {
    fn from(
        Args {
            reference,
            distorted,
            score,
            xpsnr,
        }: Args,
    ) -> Self {
        Self {
            reference,
            distorted,
            score: score.into(),
            xpsnr: xpsnr.into(),
        }
    }
}

pub async fn xpsnr(config: XpsnrConfig) -> anyhow::Result<()> {
    let XpsnrConfig {
        reference,
        distorted,
        score,
        xpsnr,
    } = config;

    let bar = ProgressBar::new(1).with_style(
        ProgressStyle::default_bar()
            .template("{spinner:.cyan.bold} {elapsed_precise:.bold} {wide_bar:.cyan/blue} ({msg}eta {eta})")?
            .progress_chars(PROGRESS_CHARS)
    );
    bar.enable_steady_tick(Duration::from_millis(100));
    bar.set_message("xpsnr running, ");

    let dprobe = ffprobe::probe(&distorted);
    let rprobe = ffprobe::probe(&reference);
    let nframes = dprobe.nframes().or_else(|_| rprobe.nframes());
    let duration = dprobe.duration.as_ref().or(rprobe.duration.as_ref());
    let source_fps = dprobe.fps.as_ref().or(rprobe.fps.as_ref()).ok().copied();
    if let Ok(nframes) = nframes {
        bar.set_length(nframes);
    }

    let mut xpsnr_out = pin!(xpsnr::run(
        &reference,
        &distorted,
        &lavfi(
            score.reference_vfilter.as_deref(),
            xpsnr
                .xpsnr_pix_format
                .or_else(|| PixelFormat::opt_max(dprobe.pixel_format(), rprobe.pixel_format())),
        ),
        xpsnr.fps(),
    )?);
    let mut logger = ProgressLogger::new(module_path!(), Instant::now());
    let mut score = None;
    while let Some(next) = xpsnr_out.next().await {
        match next {
            XpsnrOut::Done(s) => {
                score = Some(s);
            }
            XpsnrOut::Progress(FfmpegOut::Progress {
                frame, fps, time, ..
            }) => {
                let time = source_fps.map_or(time, |source_fps| {
                    xpsnr::progress_time(time, source_fps, xpsnr.fps())
                });
                if fps > 0.0 {
                    bar.set_message(format!("xpsnr {fps} fps, "));
                }
                if nframes.is_ok() {
                    bar.set_position(frame);
                }
                if let Ok(total) = duration {
                    logger.update(*total, time, fps);
                }
            }
            XpsnrOut::Progress(FfmpegOut::StreamSizes { .. }) => {}
            XpsnrOut::Err(e) => return Err(e),
        }
    }
    bar.finish();

    println!("{}", score.context("no xpsnr score")?);
    Ok(())
}

pub fn lavfi(ref_vfilter: Option<&str>, pix_fmt: Option<PixelFormat>) -> String {
    /// Add filter to `lavfi`, if necessary. If no filter added return `old_name`.
    /// Otherwise return `new_name`.
    fn add_filter(
        lavfi: &mut String,
        old_name: &'static str,
        new_name: &'static str,
        vfilter: Option<&str>,
        pix_fmt: Option<PixelFormat>,
    ) -> &'static str {
        if vfilter.is_none() && pix_fmt.is_none() {
            return old_name;
        }

        lavfi.push_str(old_name);
        let mut has_filter = false;
        if let Some(pix_fmt) = pix_fmt {
            _ = write!(lavfi, "format={pix_fmt}");
            has_filter = true;
        }
        if let Some(vf) = vfilter {
            if has_filter {
                lavfi.push(',');
            }
            lavfi.push_str(vf);
            has_filter = true;
        }
        if has_filter {
            lavfi.push_str(",setpts=PTS-STARTPTS,settb=AVTB");
        }
        lavfi.push_str(new_name);
        lavfi.push(';');
        new_name
    }

    let mut lavfi = String::new();

    let ref_stream = add_filter(&mut lavfi, "[1:v]", "[ref]", ref_vfilter, pix_fmt);
    let dis_stream = add_filter(&mut lavfi, "[0:v]", "[dis]", None, pix_fmt);
    lavfi.push_str(ref_stream);
    lavfi.push_str(dis_stream);
    lavfi.push_str("xpsnr");
    lavfi
}

#[test]
fn test_lavfi_default() {
    assert_eq!(lavfi(None, None), "[1:v][0:v]xpsnr");
}

#[test]
fn test_lavfi_ref_vfilter() {
    assert_eq!(
        lavfi(Some("scale=1280:-1"), None),
        "[1:v]scale=1280:-1,setpts=PTS-STARTPTS,settb=AVTB[ref];\
         [ref][0:v]xpsnr"
    );
}

#[test]
fn test_lavfi_pixel_format() {
    assert_eq!(
        lavfi(None, Some(PixelFormat::Yuv420p10le)),
        "[1:v]format=yuv420p10le,setpts=PTS-STARTPTS,settb=AVTB[ref];\
         [0:v]format=yuv420p10le,setpts=PTS-STARTPTS,settb=AVTB[dis];\
         [ref][dis]xpsnr"
    );
}

#[test]
fn test_lavfi_all() {
    assert_eq!(
        lavfi(Some("scale=640:-1"), Some(PixelFormat::Yuv420p10le)),
        "[1:v]format=yuv420p10le,scale=640:-1,setpts=PTS-STARTPTS,settb=AVTB[ref];\
         [0:v]format=yuv420p10le,setpts=PTS-STARTPTS,settb=AVTB[dis];\
         [ref][dis]xpsnr"
    );
}

// ab-kgc.96: XPSNR lavfi should sync timestamps like VMAF analysis graphs
#[test]
fn xpsnr_lavfi_includes_timestamp_sync_filters() {
    let filter = lavfi(None, Some(PixelFormat::Yuv420p));
    assert!(
        filter.contains("setpts=PTS-STARTPTS") && filter.contains("settb=AVTB"),
        "xpsnr lavfi must sync presentation timestamps: {filter}"
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn xpsnr_config_lowers_score_args() {
        let config = XpsnrConfig::from(Args {
            reference: PathBuf::from("ref.mkv"),
            distorted: PathBuf::from("dist.mkv"),
            score: args::ScoreArgs {
                reference_vfilter: Some("scale=1280:-1".into()),
            },
            xpsnr: args::Xpsnr::default(),
        });

        assert_eq!(
            config.score.reference_vfilter.as_deref(),
            Some("scale=1280:-1")
        );
    }

    #[test]
    fn xpsnr_config_from_args_does_not_allocate() {
        let args = Args {
            reference: PathBuf::from("ref.mkv"),
            distorted: PathBuf::from("dist.mkv"),
            score: args::ScoreArgs {
                reference_vfilter: Some("scale=1280:-1".into()),
            },
            xpsnr: args::Xpsnr {
                xpsnr_fps: args::FrameRateOverride::new(0.0),
                xpsnr_pix_format: Some(PixelFormat::Yuv420p10le),
            },
        };

        crate::test_support::assert_no_allocations(|| {
            std::hint::black_box(XpsnrConfig::from(args));
        });
    }
}
