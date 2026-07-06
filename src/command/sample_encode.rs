mod cache;

use crate::{
    command::{
        PROGRESS_CHARS, SmallDuration,
        args::{self, PixelFormat},
    },
    console_ext::style,
    ffmpeg::{self, FfmpegEncodeArgs, remove_arg},
    ffprobe::{self, Ffprobe},
    log::ProgressLogger,
    process::FfmpegOut,
    sample, temporary,
    vmaf::{self, VmafOut},
    xpsnr::{self, XpsnrOut},
};
use anyhow::ensure;
use clap::{ArgAction, Parser};
use console::style;
use futures_util::Stream;
use indicatif::{HumanBytes, HumanDuration, ProgressBar, ProgressStyle};
use log::info;
use std::{
    fmt::Display,
    io::{self, IsTerminal},
    path::{Path, PathBuf},
    pin::pin,
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::fs;
use tokio_stream::StreamExt;

/// Encode & analyse input samples to predict how a full encode would go.
/// This is much quicker than a full encode/vmaf run.
///
/// Outputs:
/// * Mean sample score
/// * Predicted full encode size
/// * Predicted full encode time
#[derive(Parser, Clone)]
#[clap(verbatim_doc_comment)]
#[group(skip)]
pub struct Args {
    #[clap(flatten)]
    pub args: args::Encode,

    /// Encoder constant rate factor (1-63). Lower means better quality.
    #[arg(long)]
    pub crf: f32,

    #[clap(flatten)]
    pub sample: args::Sample,

    /// Enable sample-encode caching.
    #[arg(
        long,
        default_value_t = true,
        env = "AB_AV1_CACHE",
        action(ArgAction::Set)
    )]
    pub cache: bool,

    /// Stdout message format `human` or `json`.
    #[arg(long, value_enum, default_value_t = StdoutFormat::Human)]
    pub stdout_format: StdoutFormat,

    #[clap(flatten)]
    pub vmaf: args::Vmaf,

    #[clap(flatten)]
    pub score: args::ScoreArgs,

    #[clap(flatten)]
    pub xpsnr_opts: args::Xpsnr,

    /// Calculate a XPSNR score instead of VMAF.
    #[arg(long)]
    pub xpsnr: bool,
}

pub async fn sample_encode(mut args: Args) -> anyhow::Result<()> {
    const BAR_LEN: u64 = 1024 * 1024 * 1024;
    const BAR_LEN_F: f32 = BAR_LEN as _;

    let bar = ProgressBar::new(BAR_LEN).with_style(
        ProgressStyle::default_bar()
            .template("{spinner:.cyan.bold} {elapsed_precise:.bold} {prefix} {wide_bar:.cyan/blue} ({msg}eta {eta})")?
            .progress_chars(PROGRESS_CHARS)
    );
    bar.enable_steady_tick(Duration::from_millis(100));

    let probe = ffprobe::probe(&args.args.input);
    args.sample
        .set_extension_from_input(&args.args.input, &args.args.encoder, &probe);

    let enc_args = args.args.clone();
    let crf = args.crf;
    let stdout_fmt = args.stdout_format;
    let input_is_image = probe.is_image;

    let mut run = pin!(run(args, probe.into()));
    while let Some(update) = run.next().await {
        match update? {
            Update::Status(Status {
                work,
                fps,
                progress,
                sample,
                samples,
                full_pass,
            }) => {
                match full_pass {
                    true => bar.set_prefix("Full pass"),
                    false => bar.set_prefix(format!("Sample {sample}/{samples}")),
                }
                let label = work.fps_label();
                match work {
                    Work::Encode if fps <= 0.0 => bar.set_message("encoding,  "),
                    _ if fps <= 0.0 => bar.set_message(format!("{label},       ")),
                    _ => bar.set_message(format!("{label} {fps} fps, ")),
                }
                bar.set_position((progress * BAR_LEN_F).round() as _);
            }
            Update::SampleResult { sample, result } => result.print_attempt(&bar, sample, None),
            Update::Done(output) => {
                bar.finish();
                if io::stderr().is_terminal() {
                    eprintln!(
                        "\n{} {}\n",
                        style("Encode with:").dim(),
                        style(enc_args.encode_hint(crf)).dim().italic(),
                    );
                }
                stdout_fmt.print_result(&output, input_is_image);
            }
        }
    }
    Ok(())
}

pub fn run(
    Args {
        args,
        crf,
        sample: sample_args,
        cache,
        stdout_format: _,
        vmaf,
        score,
        xpsnr,
        xpsnr_opts,
    }: Args,
    input_probe: Arc<Ffprobe>,
) -> impl Stream<Item = anyhow::Result<Update>> {
    async_stream::try_stream! {
        let input = Arc::new(args.input.clone());
        let input_pix_fmt = input_probe.pixel_format();
        let input_is_image = input_probe.is_image;
        let input_len = fs::metadata(&*input).await?.len();
        let mut enc_args = args.to_ffmpeg_args(crf, &input_probe)?;
        // ignore user -fps_mode for sample encoding, as we always use passthrough
        remove_arg(&mut enc_args.output_args, "-fps_mode");
        remove_arg(&mut enc_args.output_args, "-vsync");

        let duration = input_probe.duration.clone()?;
        let input_fps = input_probe.fps.clone()?;
        let samples = sample_args.sample_count(duration).max(1);
        let keep = sample_args.keep;
        let temp_dir = sample_args.temp_dir;
        // let scoring = ScoringInfo {
        //     args: &score,
        //     vmaf: &vmaf,
        //     xpsnr: &xpsnr,
        // };

        let (samples, sample_duration, full_pass) = {
            if input_is_image {
                (1, duration.max(Duration::from_secs(1)), true)
            } else if sample_args.sample_duration.is_zero()
                || sample_args.sample_duration * samples as _ >= duration.mul_f64(0.85)
            {
                // if the sample time is most of the full input time just encode the whole thing
                (1, duration, true)
            } else {
                let sample_duration = if input_fps > 0.0 {
                    // if sample-length is lower than a single frame use the frame time
                    let one_frame_duration = Duration::from_secs_f64(1.0 / input_fps);
                    sample_args.sample_duration.max(one_frame_duration)
                } else {
                    sample_args.sample_duration
                };
                (samples, sample_duration, false)
            }
        };
        let sample_duration_us = sample_duration.as_micros_u64();

        // Start creating copy samples async, this is IO bound & not cpu intensive
        let (tx, mut sample_tasks) = tokio::sync::mpsc::unbounded_channel();
        let sample_temp = temp_dir.clone();
        let sample_in = input.clone();
        tokio::task::spawn_local(async move {
            if full_pass {
                // Use the entire video as a single sample
                let _ = tx.send((0, Ok((sample_in.clone(), input_len))));
            } else {
                for sample_idx in 0..samples {
                    let sample = sample(
                        sample_in.clone(),
                        sample_idx,
                        samples,
                        sample_duration,
                        duration,
                        input_fps,
                        sample_temp.clone(),
                    )
                    .await;
                    if tx.send((sample_idx, sample)).is_err() {
                        break;
                    }
                }
            }
        });

        let mut results = Vec::new();
        loop {
            let (sample_idx, sample) = match sample_tasks.recv().await {
                Some(s) => s,
                None => break,
            };
            let sample_n = sample_idx + 1;
            let (sample, sample_size) = sample?;

            info!("encoding sample {sample_n}/{samples} crf {crf}");
            yield Update::Status(Status {
                work: Work::Encode,
                fps: 0.0,
                progress: sample_idx as f32 / samples as f32,
                full_pass,
                sample: sample_n,
                samples,
            });

            // encode sample
            let result = match cache::cached_encode(
                cache,
                &sample,
                &input,
                duration,
                input.extension(),
                input_len,
                full_pass,
                &enc_args,
                (&score, &vmaf, &xpsnr),
            )
            .await
            {
                (Some(result), _) => {
                    if samples > 1 {
                        result.log_attempt(sample_n, samples, crf);
                    }
                    result
                }
                (None, key) => {
                    let b = Instant::now();
                    let mut logger = ProgressLogger::new(module_path!(), b);
                    let (encoded_sample, mut output) = ffmpeg::encode_sample(
                        FfmpegEncodeArgs {
                            input: &sample,
                            ..enc_args.clone()
                        },
                        temp_dir.clone(),
                        sample_args.extension.as_deref().unwrap_or("mkv"),
                    )?;
                    while let Some(enc_progress) = output.next().await {
                        if let FfmpegOut::Progress { time, fps, .. } = enc_progress? {
                            yield Update::Status(Status {
                                work: Work::Encode,
                                fps,
                                progress: (time.as_micros_u64() + sample_idx * sample_duration_us * 2) as f32
                                    / (sample_duration_us * samples * 2) as f32,
                                full_pass,
                                sample: sample_n,
                                samples,
                            });
                            logger.update(sample_duration, time, fps);
                        }
                    }
                    output.wait().await?; // ensure process has exited

                    let encode_time = b.elapsed();
                    let encoded_size = fs::metadata(&encoded_sample).await?.len();
                    let encoded_probe = ffprobe::probe(&encoded_sample);

                    let mut result = EncodeResult {
                        vmaf_score: None,
                        xpsnr_score: None,
                        sample_size,
                        encoded_size,
                        encode_time,
                        sample_duration: encoded_probe
                            .duration
                            .ok()
                            .filter(|d| !d.is_zero())
                            .unwrap_or(sample_duration),
                        from_cache: false,
                    };

                    let do_vmaf = vmaf.and_vmaf.unwrap_or(!xpsnr);
                    if xpsnr {
                        yield Update::Status(Status {
                            work: Work::Score(ScoreKind::Xpsnr),
                            fps: 0.0,
                            progress: (sample_idx as f32 + 0.5) / samples as f32,
                            full_pass,
                            sample: sample_n,
                            samples,
                        });

                        let lavfi = super::xpsnr::lavfi(
                            score.reference_vfilter.as_deref().or(args.vfilter.as_deref()),
                            xpsnr_opts.xpsnr_pix_format
                                .or_else(|| PixelFormat::opt_max(enc_args.pix_fmt, input_pix_fmt)),
                        );
                        let xpsnr_out = xpsnr::run(&sample, &encoded_sample, &lavfi, xpsnr_opts.fps())?;
                        let mut xpsnr_out = pin!(xpsnr_out);
                        let mut logger = ProgressLogger::new("ab_av1::xpsnr", Instant::now());
                        while let Some(next) = xpsnr_out.next().await {
                            match next {
                                XpsnrOut::Done(s) => {
                                    result.xpsnr_score = Some(s);
                                }
                                XpsnrOut::Progress(FfmpegOut::Progress { time, fps, .. }) => {
                                    let progress = match do_vmaf {
                                        false => (sample_duration_us +
                                            time.as_micros_u64() +
                                            sample_idx * sample_duration_us * 2) as f32
                                            / (sample_duration_us * samples * 2) as f32,
                                        true => (sample_duration_us +
                                            time.as_micros_u64() / 2 +
                                            sample_idx * sample_duration_us * 2) as f32
                                            / (sample_duration_us * samples * 2) as f32
                                    };
                                    yield Update::Status(Status {
                                        work: Work::Score(ScoreKind::Xpsnr),
                                        fps,
                                        progress,
                                        full_pass,
                                        sample: sample_n,
                                        samples,
                                    });
                                    logger.update(sample_duration, time, fps);
                                }
                                XpsnrOut::Progress(_) => {}
                                XpsnrOut::Err(e) => Err(e)?,
                            }
                        }
                    }
                    if do_vmaf {
                        let init_progress = match xpsnr {
                            false => (sample_idx as f32 + 0.5) / samples as f32,
                            true => (sample_idx as f32 + 0.75) / samples as f32,
                        };
                        yield Update::Status(Status {
                            work: Work::Score(ScoreKind::Vmaf),
                            fps: 0.0,
                            progress: init_progress,
                            full_pass,
                            sample: sample_n,
                            samples,
                        });
                        let vmaf = vmaf::run(
                            &sample,
                            &encoded_sample,
                            &vmaf.ffmpeg_lavfi(
                                encoded_probe.resolution,
                                PixelFormat::opt_max(enc_args.pix_fmt, input_pix_fmt),
                                score.reference_vfilter.as_deref().or(args.vfilter.as_deref()),
                            ),
                            vmaf.fps(),
                        )?;
                        let mut vmaf = pin!(vmaf);
                        let mut logger = ProgressLogger::new("ab_av1::vmaf", Instant::now());
                        while let Some(vmaf) = vmaf.next().await {
                            match vmaf {
                                VmafOut::Done(score) => {
                                    result.vmaf_score = Some(score);
                                }
                                VmafOut::Progress(FfmpegOut::Progress { time, fps, .. }) => {
                                    let progress = match xpsnr {
                                        false => (sample_duration_us +
                                            time.as_micros_u64() +
                                            sample_idx * sample_duration_us * 2) as f32
                                            / (sample_duration_us * samples * 2) as f32,
                                        true => (sample_duration_us + sample_duration_us / 2 +
                                            time.as_micros_u64() / 2 +
                                            sample_idx * sample_duration_us * 2) as f32
                                            / (sample_duration_us * samples * 2) as f32,
                                    };
                                    yield Update::Status(Status {
                                        work: Work::Score(ScoreKind::Vmaf),
                                        fps,
                                        progress,
                                        full_pass,
                                        sample: sample_n,
                                        samples,
                                    });
                                    logger.update(sample_duration, time, fps);
                                }
                                VmafOut::Progress(_) => {}
                                VmafOut::Err(e) => Err(e)?,
                            }
                        }
                    }

                    if samples > 1 {
                        result.log_attempt(sample_n, samples, crf);
                    }

                    if let Some(k) = key {
                        cache::cache_result(k, &result).await?;
                    }

                    // Early clean. Note: Avoid cleaning copy samples
                    temporary::clean(true).await;
                    if !keep {
                        let _ = tokio::fs::remove_file(encoded_sample).await;
                    }

                    result
                }
            };

            results.push(result.clone());
            yield Update::SampleResult { sample: sample_n, result };
        }

        let output = Output {
            vmaf_score: results.mean_vmaf_score(),
            xpsnr_score: results.mean_xpsnr_score(),
            // Using file size * encode_percent can over-estimate. However, if it ends up less
            // than the duration estimation it may turn out to be more accurate.
            predicted_encode_size: results
                .estimate_encode_size_by_duration(duration, full_pass)
                .min(estimate_encode_size_by_file_percent(&results, &input, full_pass).await?),
            encode_percent: results.encoded_percent_size(),
            predicted_encode_time: results.estimate_encode_time(duration, full_pass),
            from_cache: results.iter().all(|r| r.from_cache),
        };
        info!(
            "crf {crf}{}{} predicted video stream size {} ({:.0}%) taking {}{}",
            output.vmaf_score
                .map(|s| format!(" VMAF {s:.2}"))
                .unwrap_or_default(),
            output.xpsnr_score
                .map(|s| format!(" XPSNR {s:.2}"))
                .unwrap_or_default(),
            HumanBytes(output.predicted_encode_size),
            output.encode_percent,
            HumanDuration(output.predicted_encode_time),
            if output.from_cache { " (cache)" } else { "" }
        );

        yield Update::Done(output);
    }
}

/// Copy a sample from the input to the temp_dir (or input dir).
async fn sample(
    input: Arc<PathBuf>,
    sample_idx: u64,
    samples: u64,
    sample_duration: Duration,
    duration: Duration,
    fps: f64,
    temp_dir: Option<PathBuf>,
) -> anyhow::Result<(Arc<PathBuf>, u64)> {
    let sample_n = sample_idx + 1;

    let sample_start = (duration.saturating_sub(sample_duration * samples as _)
        / (samples as u32 + 1))
        * sample_n as _
        + sample_duration * sample_idx as _;

    let sample_frames = ((sample_duration.as_secs_f64() * fps).round() as u32).max(1);
    let floor_to_sec = sample_duration >= Duration::from_secs(2);

    let sample = sample::copy(&input, sample_start, floor_to_sec, sample_frames, temp_dir).await?;
    let sample_size = fs::metadata(&sample).await?.len();
    ensure!(
        // ffmpeg copy may fail successfully and give us a small/empty output
        sample_size > 1024,
        "ffmpeg copy failed: encoded sample too small"
    );
    Ok((sample.into(), sample_size))
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EncodeResult {
    pub sample_size: u64,
    pub encoded_size: u64,
    pub vmaf_score: Option<f32>,
    pub xpsnr_score: Option<f32>,
    pub encode_time: Duration,
    /// Duration of the sample.
    ///
    /// This should be close to `SAMPLE_SIZE` but may deviate due to how samples are cut.
    pub sample_duration: Duration,
    /// Result read from cache.
    pub from_cache: bool,
}

impl EncodeResult {
    pub fn print_attempt(&self, bar: &ProgressBar, sample_n: u64, crf: Option<f32>) {
        let Self {
            sample_size,
            encoded_size,
            vmaf_score,
            xpsnr_score,
            from_cache,
            ..
        } = self;
        bar.println(
            style!(
                "- {}Sample {sample_n} ({:.0}%){}{}{}",
                crf.map(|crf| format!("crf {crf}: ")).unwrap_or_default(),
                100.0 * *encoded_size as f32 / *sample_size as f32,
                vmaf_score
                    .map(|s| format!(" VMAF {s:.2}"))
                    .unwrap_or_default(),
                xpsnr_score
                    .map(|s| format!(" XPSNR {s:.2}"))
                    .unwrap_or_default(),
                if *from_cache { " (cache)" } else { "" },
            )
            .dim()
            .to_string(),
        );
    }

    pub fn log_attempt(&self, sample_n: u64, samples: u64, crf: f32) {
        let Self {
            sample_size,
            encoded_size,
            vmaf_score,
            xpsnr_score,
            from_cache,
            ..
        } = self;
        info!(
            "sample {sample_n}/{samples} crf {crf}{}{} ({:.0}%){}",
            vmaf_score
                .map(|s| format!(" VMAF {s:.2}"))
                .unwrap_or_default(),
            xpsnr_score
                .map(|s| format!(" XPSNR {s:.2}"))
                .unwrap_or_default(),
            100.0 * *encoded_size as f32 / *sample_size as f32,
            if *from_cache { " (cache)" } else { "" }
        );
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum ScoreKind {
    Vmaf,
    Xpsnr,
}

impl ScoreKind {
    /// Display label for fps in progress bar.
    pub fn fps_label(&self) -> &'static str {
        match self {
            Self::Vmaf => "vmaf",
            Self::Xpsnr => "xpsnr",
        }
    }

    /// General display name.
    pub fn display_str(&self) -> &'static str {
        match self {
            Self::Vmaf => "VMAF",
            Self::Xpsnr => "XPSNR",
        }
    }
}

impl Display for ScoreKind {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.display_str())
    }
}

trait EncodeResults {
    fn encoded_percent_size(&self) -> f64;

    fn mean_vmaf_score(&self) -> Option<f32>;

    fn mean_xpsnr_score(&self) -> Option<f32>;

    /// Return estimated encoded **video stream** size by multiplying sample size by duration.
    fn estimate_encode_size_by_duration(
        &self,
        input_duration: Duration,
        single_full_pass: bool,
    ) -> u64;

    fn estimate_encode_time(&self, input_duration: Duration, single_full_pass: bool) -> Duration;
}

impl EncodeResults for Vec<EncodeResult> {
    fn encoded_percent_size(&self) -> f64 {
        if self.is_empty() {
            return 100.0;
        }
        let encoded = self.iter().map(|r| r.encoded_size).sum::<u64>() as f64;
        let sample = self.iter().map(|r| r.sample_size).sum::<u64>() as f64;
        encoded * 100.0 / sample
    }

    fn mean_vmaf_score(&self) -> Option<f32> {
        let mut scores = self.iter().filter_map(|r| r.vmaf_score).peekable();
        scores.peek()?;
        Some(scores.sum::<f32>() / self.len() as f32)
    }

    fn mean_xpsnr_score(&self) -> Option<f32> {
        let mut scores = self.iter().filter_map(|r| r.xpsnr_score).peekable();
        scores.peek()?;
        Some(scores.sum::<f32>() / self.len() as f32)
    }

    fn estimate_encode_size_by_duration(
        &self,
        input_duration: Duration,
        single_full_pass: bool,
    ) -> u64 {
        if self.is_empty() {
            return 0;
        }
        if single_full_pass {
            return self[0].encoded_size;
        }

        let sample_duration: Duration = self.iter().map(|s| s.sample_duration).sum();
        let sample_factor = input_duration.as_secs_f64() / sample_duration.as_secs_f64();
        let sample_encode_size: f64 = self.iter().map(|r| r.encoded_size as f64).sum();

        (sample_encode_size * sample_factor).round() as _
    }

    fn estimate_encode_time(&self, input_duration: Duration, single_full_pass: bool) -> Duration {
        if self.is_empty() {
            return Duration::ZERO;
        }
        if single_full_pass {
            return self[0].encode_time;
        }

        let sample_duration: Duration = self.iter().map(|s| s.sample_duration).sum();
        let sample_factor = input_duration.as_secs_f64() / sample_duration.as_secs_f64();
        let sample_encode_time: Duration = self.iter().map(|r| r.encode_time).sum();

        let estimate = sample_encode_time.mul_f64(sample_factor);
        if estimate < Duration::from_secs(1) {
            estimate
        } else {
            Duration::from_secs(estimate.as_secs())
        }
    }
}

/// Return estimated encoded **video stream** size by applying the sample percentage
/// change to the input file size.
///
/// This can over-estimate the larger the non-video proportion of the input.
async fn estimate_encode_size_by_file_percent(
    results: &Vec<EncodeResult>,
    input: &Path,
    single_full_pass: bool,
) -> anyhow::Result<u64> {
    if results.is_empty() {
        return Ok(0);
    }
    if single_full_pass {
        return Ok(results[0].encoded_size);
    }
    let encode_proportion = results.encoded_percent_size() / 100.0;

    Ok((fs::metadata(input).await?.len() as f64 * encode_proportion).round() as _)
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum StdoutFormat {
    Human,
    Json,
}

impl StdoutFormat {
    fn print_result(
        self,
        Output {
            vmaf_score,
            xpsnr_score,
            predicted_encode_size,
            encode_percent,
            predicted_encode_time,
            from_cache: _,
        }: &Output,
        image: bool,
    ) {
        match self {
            Self::Human => {
                let vmaf_fmt = match *vmaf_score {
                    None => format_args!(""),
                    Some(s) => match s {
                        _ if s >= 95.0 => format_args!("VMAF {} ", style(s).bold().green()),
                        _ if s < 80.0 => format_args!("VMAF {} ", style(s).bold().red()),
                        _ => format_args!("VMAF {} ", style(s).bold()),
                    },
                };
                let xpsnr_fmt = match *xpsnr_score {
                    None => format_args!(""),
                    Some(s) => format_args!("XPSNR {} ", style(s).bold()),
                };
                let percent = encode_percent.round();
                let size = match *predicted_encode_size {
                    v if percent < 80.0 => style(HumanBytes(v)).bold().green(),
                    v if percent >= 100.0 => style(HumanBytes(v)).bold().red(),
                    v => style(HumanBytes(v)).bold(),
                };
                let percent = match percent {
                    v if v < 80.0 => style!("{}%", v).bold().green(),
                    v if v >= 100.0 => style!("{}%", v).bold().red(),
                    v => style!("{}%", v).bold(),
                };
                let time = style(HumanDuration(*predicted_encode_time)).bold();
                let enc_description = match image {
                    true => "image",
                    false => "video stream",
                };
                println!(
                    "{vmaf_fmt}{xpsnr_fmt}predicted {enc_description} size {size} ({percent}) taking {time}"
                );
            }
            Self::Json => {
                let mut json = serde_json::json!({
                    "predicted_encode_size": predicted_encode_size,
                    "predicted_encode_percent": encode_percent,
                    "predicted_encode_seconds": predicted_encode_time.as_secs_f64(),
                });
                if let Some(score) = *vmaf_score {
                    json["vmaf"] = score.into();
                }
                if let Some(score) = *xpsnr_score {
                    json["xpsnr"] = score.into();
                }
                println!("{json}");
            }
        }
    }
}

/// Sample encode result.
#[derive(Debug, Clone)]
pub struct Output {
    /// Sample mean VMAF score.
    pub vmaf_score: Option<f32>,
    /// Sample mean XPSNR score.
    pub xpsnr_score: Option<f32>,
    /// Estimated full encoded **video stream** size.
    ///
    /// Encoded sample size multiplied by duration.
    pub predicted_encode_size: u64,
    /// Sample mean encoded percentage.
    pub encode_percent: f64,
    /// Estimated full encode time.
    ///
    /// Sample encode time multiplied by duration.
    pub predicted_encode_time: Duration,
    /// All sample results were read from the cache.
    pub from_cache: bool,
}

impl Output {
    /// Extract vmaf or xpsnr score. Use when it is expected to have only 1 of these.
    pub fn single_score(&self) -> f32 {
        self.vmaf_score.or(self.xpsnr_score).unwrap_or_default()
    }

    /// Extract vmaf or xpsnr kind. Use when it is expected to have only 1 of these.
    pub fn single_score_kind(&self) -> ScoreKind {
        match self.vmaf_score {
            Some(_) => ScoreKind::Vmaf,
            _ => ScoreKind::Xpsnr,
        }
    }
}

/// Kinds of sample-encode work.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub enum Work {
    #[default]
    Encode,
    Score(ScoreKind),
}

impl Work {
    /// Display label for fps in progress bar.
    pub fn fps_label(&self) -> &'static str {
        match self {
            Self::Encode => "enc",
            Self::Score(kind) => kind.fps_label(),
        }
    }
}

#[derive(Debug)]
pub struct Status {
    /// Kind of work being performed
    pub work: Work,
    /// fps, `0.0` may be interpreted as "unknown"
    pub fps: f32,
    /// sample progress `[0, 1]`
    pub progress: f32,
    /// Sample number `1,....,n`
    pub sample: u64,
    /// Total samples
    pub samples: u64,
    /// Encoding the entire input video
    pub full_pass: bool,
}

#[derive(Debug)]
pub enum Update {
    Status(Status),
    SampleResult {
        /// Sample number `1,....,n`
        sample: u64,
        result: EncodeResult,
    },
    Done(Output),
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use rstest::rstest;
    use std::time::Duration;

    mod helpers {
        use super::*;

        pub fn encode_result(
            sample_size: u64,
            encoded_size: u64,
            sample_duration_secs: u64,
            vmaf: Option<f32>,
            xpsnr: Option<f32>,
        ) -> EncodeResult {
            EncodeResult {
                sample_size,
                encoded_size,
                vmaf_score: vmaf,
                xpsnr_score: xpsnr,
                encode_time: Duration::from_secs(sample_duration_secs),
                sample_duration: Duration::from_secs(sample_duration_secs),
                from_cache: false,
            }
        }
    }

    use helpers::*;

    /// Mirror `sample_encode::sample` grid divisor: `(samples as u32 + 1)`.
    fn sample_grid_divisor(samples: u64) -> u32 {
        samples as u32 + 1
    }

    /// Mirror progress denominator: `sample_duration_us * samples * 2`.
    fn encode_progress_ratio(
        time_us: u64,
        sample_idx: u64,
        sample_duration_us: u64,
        samples: u64,
    ) -> f32 {
        (time_us + sample_idx * sample_duration_us * 2) as f32
            / (sample_duration_us * samples * 2) as f32
    }

    // ab-kgc.23: mirrors sample_encode::sample frame math
    #[test]
    fn sample_frame_calculation_does_not_truncate_large_products() {
        // setup — mirrors sample_encode::sample frame math (ab-kgc.19)
        let sample_duration = Duration::from_secs(100_000);
        let fps = 50_000.0;

        // execute
        let product = sample_duration.as_secs_f64() * fps;
        let frames = (product.round() as u32).max(1);

        // assert — wrapping would request far too few frames
        assert!(
            f64::from(frames) >= product * 0.99,
            "duration*fps product {product} must not truncate via u32 cast (got {frames})"
        );
    }

    // ab-kgc.37–38: zero total sample duration must not yield infinite predictions
    #[test]
    fn estimate_encode_predictions_finite_for_zero_sample_duration() {
        // setup
        let size_results = vec![encode_result(1000, 500, 0, Some(90.0), None)];
        let time_results = vec![encode_result(1000, 500, 0, None, Some(90.0))];
        let input_duration = Duration::from_secs(100);

        // execute
        let size = size_results.estimate_encode_size_by_duration(input_duration, false);
        let estimate = time_results.estimate_encode_time(input_duration, false);

        // assert
        assert!(
            size < u64::MAX,
            "size prediction must be finite for zero sample duration"
        );
        assert!(
            estimate < Duration::from_secs(u64::MAX / 2),
            "time prediction must be finite for zero sample duration"
        );
    }

    // ab-kgc.39: empty encode results should not assume 100% size ratio
    #[test]
    fn encoded_percent_size_empty_results_returns_zero() {
        // setup
        let results: Vec<EncodeResult> = vec![];

        // execute
        let percent = results.encoded_percent_size();

        // assert
        assert_eq!(
            percent, 0.0,
            "empty results should not report 100% encoded size"
        );
    }

    // ab-kgc.47: sample grid divisor must not wrap for sample counts above u32::MAX
    #[test]
    fn sample_grid_divisor_non_zero_for_huge_sample_counts() {
        // setup — when samples > u32::MAX, `(samples as u32 + 1)` truncates to 1
        let samples = u32::MAX as u64 + 1;

        // execute
        let divisor = sample_grid_divisor(samples);

        // assert
        assert_ne!(
            divisor, 1,
            "sample grid divisor must not truncate when samples exceed u32::MAX"
        );
    }

    // ab-kgc.60: zero sample_duration_us must not yield infinite encode progress
    #[test]
    fn encode_progress_finite_when_sample_duration_zero() {
        // setup — mirrors encode progress denominator in sample_encode::run
        let progress = encode_progress_ratio(1_000, 0, 0, 4);

        // execute / assert
        assert!(
            progress.is_finite(),
            "encode progress must be finite when sample_duration is zero, got {progress}"
        );
    }

    // ab-kgc.48–49: NaN scores must not produce NaN means
    #[test]
    fn mean_scores_ignore_nan_values() {
        // setup
        let vmaf_results = vec![
            encode_result(1000, 500, 10, Some(f32::NAN), None),
            encode_result(1000, 500, 10, Some(90.0), None),
        ];
        let xpsnr_results = vec![
            encode_result(1000, 500, 10, None, Some(f32::NAN)),
            encode_result(1000, 500, 10, None, Some(88.0)),
        ];

        // execute
        let vmaf_mean = vmaf_results.mean_vmaf_score();
        let xpsnr_mean = xpsnr_results.mean_xpsnr_score();

        // assert
        assert_eq!(
            vmaf_mean,
            Some(90.0),
            "NaN vmaf scores must not poison the reported mean"
        );
        assert_eq!(
            xpsnr_mean,
            Some(88.0),
            "NaN xpsnr scores must not poison the reported mean"
        );
    }

    // ab-kgc.55: all-NaN vmaf scores should report None not Some(NaN)
    #[test]
    fn mean_vmaf_score_all_nan_returns_none() {
        // setup
        let results = vec![
            encode_result(1000, 500, 10, Some(f32::NAN), None),
            encode_result(1000, 500, 10, Some(f32::NAN), None),
        ];

        // execute
        let mean = results.mean_vmaf_score();

        // assert
        assert_eq!(mean, None, "all-NaN vmaf scores should yield no mean");
    }

    // ab-kgc.50–63: zero sample_size must not yield infinite attempt percentages
    #[test]
    fn attempt_percentages_finite_when_sample_size_zero() {
        // setup — mirrors EncodeResult::print_attempt and log_attempt percentage math
        let sample_size = 0_u64;
        let encoded_size = 500_u64;

        // execute
        let percent = 100.0 * encoded_size as f32 / sample_size as f32;

        // assert — ab-kgc.50/63: print_attempt and log_attempt share this math
        assert!(
            percent.is_finite(),
            "attempt/log attempt percent must be finite when sample_size is zero, got {percent}"
        );
    }
    // ab-kgc.51–52: absent scores must not guess defaults
    #[test]
    fn output_single_score_absent_has_no_silent_defaults() {
        // setup
        let output = Output {
            vmaf_score: None,
            xpsnr_score: None,
            predicted_encode_size: 0,
            encode_percent: 0.0,
            predicted_encode_time: Duration::ZERO,
            from_cache: false,
        };

        // execute
        let score = output.single_score();
        let kind = output.single_score_kind();

        // assert
        assert!(
            score.is_nan(),
            "single_score must not default to 0.0 when both scores are absent"
        );
        assert_eq!(
            kind,
            ScoreKind::Vmaf,
            "single_score_kind must not default to Xpsnr when both scores are absent"
        );
    }

    // ab-kgc.56–64: JSON cache roundtrip must preserve NaN scores
    #[test]
    fn encode_result_json_roundtrip_preserves_nan_scores() {
        // setup
        let vmaf_original = encode_result(1_000, 500, 10, Some(f32::NAN), None);
        let xpsnr_original = encode_result(1_000, 500, 10, None, Some(f32::NAN));

        // execute
        let vmaf_decoded: EncodeResult =
            serde_json::from_slice(&serde_json::to_vec(&vmaf_original).expect("serialize vmaf"))
                .expect("deserialize vmaf");
        let xpsnr_decoded: EncodeResult =
            serde_json::from_slice(&serde_json::to_vec(&xpsnr_original).expect("serialize xpsnr"))
                .expect("deserialize xpsnr");

        // assert
        assert!(vmaf_decoded.vmaf_score.unwrap().is_nan());
        assert!(xpsnr_decoded.xpsnr_score.unwrap().is_nan());
    }

    // ab-kgc.62: estimate_encode_time must not truncate subseconds when scaled total >= 1s
    #[test]
    fn estimate_encode_time_preserves_subseconds_when_scaled_above_one_second() {
        // setup — 600ms sample encode scaled 2× → 1.2s predicted; must not truncate to 1s
        let mut result = encode_result(1000, 500, 0, None, Some(90.0));
        result.encode_time = Duration::from_millis(600);
        result.sample_duration = Duration::from_millis(600);
        let results = vec![result];
        let input_duration = Duration::from_millis(1200);

        // execute
        let estimate = results.estimate_encode_time(input_duration, false);

        // assert
        assert_eq!(
            estimate,
            Duration::from_millis(1200),
            "scaled encode time must retain subsecond precision"
        );
    }

    // ab-kgc.28: zero sample_size must not yield infinite encoded percent
    #[test]
    fn encoded_percent_size_with_zero_sample_size_is_finite() {
        // setup
        let results = vec![encode_result(0, 500, 10, Some(90.0), None)];

        // execute
        let percent = results.encoded_percent_size();

        // assert
        assert!(
            percent.is_finite(),
            "encoded percent must be finite when sample_size is zero, got {percent}"
        );
    }

    #[test]
    fn encoded_percent_size_averages_multiple_samples() {
        // setup
        let results = vec![
            encode_result(1000, 500, 10, Some(90.0), None),
            encode_result(1000, 600, 10, Some(92.0), None),
        ];

        // execute / assert
        assert!((results.encoded_percent_size() - 55.0).abs() < f64::EPSILON);
    }

    #[test]
    fn mean_scores_average_present_values() {
        // setup
        let results = vec![
            encode_result(1000, 500, 10, Some(90.0), Some(88.0)),
            encode_result(1000, 500, 10, Some(94.0), Some(92.0)),
        ];

        // execute / assert
        assert_eq!(results.mean_vmaf_score(), Some(92.0));
        assert_eq!(results.mean_xpsnr_score(), Some(90.0));
    }

    // ab-kgc.22: sparse per-sample scores must not dilute the mean
    #[test]
    fn mean_scores_average_only_samples_with_scores() {
        // setup — mixed cache/legacy rows where only some samples have scores
        let vmaf_results = vec![
            encode_result(1000, 500, 10, Some(90.0), None),
            encode_result(1000, 500, 10, None, None),
        ];
        let xpsnr_results = vec![
            encode_result(1000, 500, 10, None, Some(88.0)),
            encode_result(1000, 500, 10, None, Some(92.0)),
            encode_result(1000, 500, 10, None, None),
        ];

        // execute
        let vmaf_mean = vmaf_results.mean_vmaf_score();
        let xpsnr_mean = xpsnr_results.mean_xpsnr_score();

        // assert — must not divide by total sample count when scores are sparse
        assert_eq!(
            vmaf_mean,
            Some(90.0),
            "expected vmaf mean of present scores only"
        );
        assert_eq!(
            xpsnr_mean,
            Some(90.0),
            "expected xpsnr mean of present scores only"
        );
    }

    #[test]
    fn estimate_encode_size_scales_by_duration_ratio() {
        // setup
        let results = vec![encode_result(1000, 500, 10, Some(90.0), None)];
        let input_duration = Duration::from_secs(100);

        // execute
        let size = results.estimate_encode_size_by_duration(input_duration, false);

        // assert — 500 bytes per 10s sample → 5000 for 100s input
        assert_eq!(size, 5000);
    }

    #[test]
    fn estimate_encode_time_scales_and_truncates_to_seconds() {
        // setup
        let results = vec![
            encode_result(1000, 500, 10, None, Some(90.0)),
            encode_result(1000, 500, 10, None, Some(91.0)),
        ];
        let input_duration = Duration::from_secs(100);

        // execute
        let estimate = results.estimate_encode_time(input_duration, false);

        // assert — 20s total sample encode time scaled 5× → 100s
        assert_eq!(estimate, Duration::from_secs(100));
    }

    #[test]
    fn single_full_pass_prediction_uses_first_sample_only() {
        // setup
        let results = vec![
            encode_result(1000, 400, 10, Some(90.0), None),
            encode_result(1000, 900, 10, Some(95.0), None),
        ];
        let input_duration = Duration::from_secs(3600);

        // execute / assert
        assert_eq!(
            results.estimate_encode_size_by_duration(input_duration, true),
            400
        );
        assert_eq!(
            results.estimate_encode_time(input_duration, true),
            Duration::from_secs(10)
        );
    }

    #[test]
    fn encode_result_json_roundtrip_for_cache() {
        // setup
        let original = encode_result(1_000_000, 500_000, 20, Some(95.5), Some(92.0));

        // execute
        let bytes = serde_json::to_vec(&original).expect("serialize");
        let decoded: EncodeResult = serde_json::from_slice(&bytes).expect("deserialize");

        // assert
        assert_eq!(decoded.sample_size, original.sample_size);
        assert_eq!(decoded.encoded_size, original.encoded_size);
        assert_eq!(decoded.vmaf_score, original.vmaf_score);
        assert_eq!(decoded.xpsnr_score, original.xpsnr_score);
        assert!(!decoded.from_cache);
    }

    #[rstest]
    #[case::vmaf_only(false, Some(70.0), None, ScoreKind::Vmaf)]
    #[case::xpsnr_only(true, None, Some(92.0), ScoreKind::Xpsnr)]
    #[case::vmaf_when_both_present(false, Some(70.0), Some(92.0), ScoreKind::Vmaf)]
    fn output_single_score_kind_matrix(
        #[case] use_xpsnr: bool,
        #[case] vmaf: Option<f32>,
        #[case] xpsnr: Option<f32>,
        #[case] expected: ScoreKind,
    ) {
        // setup
        let output = Output {
            vmaf_score: vmaf,
            xpsnr_score: xpsnr,
            predicted_encode_size: 0,
            encode_percent: 0.0,
            predicted_encode_time: Duration::ZERO,
            from_cache: false,
        };

        // execute / assert
        let _ = use_xpsnr;
        assert_eq!(output.single_score_kind(), expected);
    }

    mod proptest_predictions {
        use super::*;

        proptest! {
            #[test]
            fn encoded_percent_monotonic_with_encoded_size(
                sample_size in 1000u64..10_000u64,
                encoded_a in 100u64..5000u64,
                encoded_b in 100u64..5000u64,
            ) {
                let results_a = vec![encode_result(sample_size, encoded_a, 10, None, None)];
                let results_b = vec![encode_result(sample_size, encoded_b, 10, None, None)];
                let pct_a = results_a.encoded_percent_size();
                let pct_b = results_b.encoded_percent_size();
                prop_assert_eq!(pct_a <= pct_b, encoded_a <= encoded_b);
            }
        }
    }
}
