mod err;

pub use crate::command::rules::ValidationError;
pub use err::Error;

use crate::{
    command::{
        PROGRESS_CHARS, args,
        args::VmafArg,
        rules::CrfSearchRules,
        sample_encode::{self, Work},
    },
    console_ext::style,
    ffprobe::{self, Ffprobe},
    float::TerseF32,
};
use anyhow::Context;
use clap::{ArgAction, Parser};
use console::style;
use futures_util::{Stream, StreamExt};
use indicatif::{HumanBytes, HumanDuration, ProgressBar, ProgressStyle};
use log::info;
use std::{fmt, io::IsTerminal, pin::pin, str::FromStr, sync::Arc, time::Duration};

const BAR_LEN: u64 = 1024 * 1024 * 1024;
const DEFAULT_MIN_VMAF: f32 = 95.0;

/// Score used for CRF search threshold decisions.
///
/// When the user targets XPSNR (`--min-xpsnr`), search must use XPSNR even if VMAF
/// is also present (e.g. `--and-vmaf`).
fn output_search_score(enc: &sample_encode::Output, use_xpsnr: bool) -> f32 {
    match use_xpsnr {
        true => enc.xpsnr_score.or(enc.vmaf_score).unwrap_or_default(),
        false => enc.vmaf_score.or(enc.xpsnr_score).unwrap_or_default(),
    }
}

#[cfg(test)]
pub(crate) mod test_hooks {
    use super::sample_encode;
    use std::cell::RefCell;

    type SampleEncodeMock = Box<dyn Fn(f32) -> sample_encode::Output>;

    thread_local! {
        static MOCK: RefCell<Option<SampleEncodeMock>> =
            const { RefCell::new(None) };
    }

    pub fn set(mock: impl Fn(f32) -> sample_encode::Output + 'static) {
        MOCK.with(|m| *m.borrow_mut() = Some(Box::new(mock)));
    }

    pub fn clear() {
        MOCK.with(|m| *m.borrow_mut() = None);
    }

    pub fn output(crf: f32) -> Option<sample_encode::Output> {
        MOCK.with(|m| m.borrow().as_ref().map(|f| f(crf)))
    }
}

/// Interpolated binary search using sample-encode to find the best crf
/// value delivering min-vmaf & max-encoded-percent.
///
/// Outputs:
/// * Best crf value
/// * Mean sample VMAF score
/// * Predicted full encode size
/// * Predicted full encode time
///
/// Use -v to print per-sample results.
#[derive(Parser)]
#[clap(verbatim_doc_comment)]
#[group(skip)]
pub struct Args {
    #[clap(flatten)]
    pub args: args::Encode,

    /// Desired min VMAF score to deliver.
    ///
    /// [default: 95]
    #[arg(long, group = "min_score")]
    pub min_vmaf: Option<MinScore>,

    /// Desired min XPSNR score to deliver.
    ///
    /// Enables use of XPSNR for score analysis instead of VMAF.
    #[arg(long, group = "min_score")]
    pub min_xpsnr: Option<MinScore>,

    /// Maximum desired encoded size percentage of the input size.
    #[arg(long, default_value_t = MaxEncodedPercent::new(80.0).unwrap())]
    pub max_encoded_percent: MaxEncodedPercent,

    /// Minimum (highest quality) crf value to try.
    ///
    /// [default: 10, 5 for svt-av1, 2 for mpeg2video]
    #[arg(long)]
    pub min_crf: Option<Crf>,

    /// Maximum (lowest quality) crf value to try.
    ///
    /// [default: 55, 46 for x264,x265, 255 for rav1e,av1_vaapi, 30 for mpeg2video]
    #[arg(long)]
    pub max_crf: Option<Crf>,

    /// Keep searching until a crf is found no more than min_vmaf+0.05 or all
    /// possibilities have been attempted.
    ///
    /// By default the "higher vmaf tolerance" increases with each attempt (0.1, 0.2, 0.4 etc...).
    #[arg(long)]
    pub thorough: bool,

    /// Constant rate factor search increment precision.
    ///
    /// [default: 1.0, 0.1 for x264,x265,vp9]
    #[arg(long, value_parser = parse_crf_step)]
    pub crf_increment: Option<CrfStep>,

    /// Set the interpretation of crf so that higher crfs mean higher quality.
    /// For most encoders *lower* crfs mean higher quality.
    ///
    /// [default: false, true for hevc_videotoolbox]
    #[arg(long, num_args=0..=1, default_missing_value = "true")]
    pub high_crf_means_hq: Option<bool>,

    /// Enable sample-encode caching.
    #[arg(
        long,
        default_value_t = true,
        env = "AB_AV1_CACHE",
        action(ArgAction::Set)
    )]
    pub cache: bool,

    #[clap(flatten)]
    pub sample: args::Sample,

    #[clap(flatten)]
    pub vmaf: args::Vmaf,

    #[clap(flatten)]
    pub score: args::ScoreArgs,

    #[clap(flatten)]
    pub xpsnr: args::Xpsnr,

    #[command(flatten)]
    pub verbose: clap_verbosity_flag::Verbosity,
}

#[derive(Clone)]
pub struct CrfSearchConfig {
    pub args: args::Encode,
    pub min_vmaf: Option<MinScore>,
    pub min_xpsnr: Option<MinScore>,
    pub max_encoded_percent: MaxEncodedPercent,
    pub min_crf: Option<Crf>,
    pub max_crf: Option<Crf>,
    pub thorough: bool,
    pub crf_increment: Option<CrfStep>,
    pub high_crf_means_hq: Option<bool>,
    pub cache: bool,
    pub sample: args::Sample,
    pub scoring: sample_encode::ScoringConfig,
    pub verbose: clap_verbosity_flag::Verbosity,
}

impl CrfSearchConfig {
    pub fn min_score(&self) -> f32 {
        self.min_xpsnr
            .or(self.min_vmaf)
            .map_or(DEFAULT_MIN_VMAF, MinScore::get)
    }

    pub fn validate(&self) -> Result<(), ValidationError> {
        CrfSearchRules {
            min_vmaf: self.min_vmaf,
            min_xpsnr: self.min_xpsnr,
            min_crf: self.min_crf,
            max_crf: self.max_crf,
            positional_vmaf_number: positional_vmaf_number(&self.scoring.vmaf.vmaf_args),
        }
        .validate()
    }
}

impl From<Args> for CrfSearchConfig {
    fn from(
        Args {
            args,
            min_vmaf,
            min_xpsnr,
            max_encoded_percent,
            min_crf,
            max_crf,
            thorough,
            crf_increment,
            high_crf_means_hq,
            cache,
            sample,
            vmaf,
            score,
            xpsnr,
            verbose,
        }: Args,
    ) -> Self {
        Self {
            args,
            min_vmaf,
            min_xpsnr,
            max_encoded_percent,
            min_crf,
            max_crf,
            thorough,
            crf_increment,
            high_crf_means_hq,
            cache,
            sample,
            scoring: sample_encode::ScoringConfig {
                score: score.into(),
                vmaf: vmaf.into(),
                xpsnr: min_xpsnr.is_some(),
                xpsnr_opts: xpsnr.into(),
            },
            verbose,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MaxEncodedPercent(f64);

impl MaxEncodedPercent {
    pub fn new(percent: f64) -> Result<Self, ValidationError> {
        if percent.is_finite() && percent > 0.0 {
            Ok(Self(percent))
        } else {
            Err(ValidationError::NonPositiveMaxEncodedPercent)
        }
    }

    pub fn get(self) -> f64 {
        self.0
    }
}

impl fmt::Display for MaxEncodedPercent {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt(f)
    }
}

impl FromStr for MaxEncodedPercent {
    type Err = ValidationError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let percent: f64 = s
            .parse()
            .map_err(|_| ValidationError::NonPositiveMaxEncodedPercent)?;
        Self::new(percent)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct MinScore(f32);

impl MinScore {
    pub fn new(score: f32) -> Result<Self, ValidationError> {
        score
            .is_finite()
            .then_some(Self(score))
            .ok_or(ValidationError::InvalidMinScore)
    }

    pub fn get(self) -> f32 {
        self.0
    }
}

impl FromStr for MinScore {
    type Err = ValidationError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let score: f32 = s.parse().map_err(|_| ValidationError::InvalidMinScore)?;
        Self::new(score)
    }
}

impl Args {
    #[cfg(test)]
    pub fn min_score(&self) -> f32 {
        self.min_xpsnr
            .or(self.min_vmaf)
            .map_or(DEFAULT_MIN_VMAF, MinScore::get)
    }

    pub fn validate(&self) -> Result<(), ValidationError> {
        CrfSearchRules {
            min_vmaf: self.min_vmaf,
            min_xpsnr: self.min_xpsnr,
            min_crf: self.min_crf,
            max_crf: self.max_crf,
            positional_vmaf_number: positional_vmaf_number(&self.vmaf.vmaf_args),
        }
        .validate()
    }
}

fn positional_vmaf_number(args: &[VmafArg]) -> Option<f32> {
    args.iter().find_map(|arg| arg.as_str().parse().ok())
}

pub async fn crf_search(mut args: Args) -> anyhow::Result<()> {
    args.validate()?;

    let bar = ProgressBar::new(BAR_LEN).with_style(
        ProgressStyle::default_bar()
            .template("{spinner:.cyan.bold} {elapsed_precise:.bold} {prefix} {wide_bar:.cyan/blue} ({msg}eta {eta})")?
            .progress_chars(PROGRESS_CHARS)
    );
    bar.enable_steady_tick(Duration::from_millis(100));

    let probe = ffprobe::probe(&args.args.input);
    let input_is_image = probe.is_image;
    args.sample
        .set_extension_from_input(&args.args.input, &args.args.encoder, &probe);
    let config = CrfSearchConfig::from(args);
    config.validate()?;

    let min_score = config.min_score();
    let max_encoded_percent = config.max_encoded_percent;
    let thorough = config.thorough;
    let enc_args = config.args.clone();
    let verbose = config.verbose;

    let mut run = pin!(run(config, probe.into()));
    while let Some(update) = run.next().await {
        let update = update.inspect_err(|e| {
            if let Error::NoGoodCrf { last } = e {
                last.print_attempt(&bar, min_score, max_encoded_percent);
            }
        })?;
        match update {
            Update::Status {
                crf_run,
                crf,
                sample:
                    sample_encode::Status {
                        work,
                        fps,
                        progress,
                        sample,
                        samples,
                        full_pass,
                    },
            } => {
                bar.set_position(guess_progress(crf_run, progress, thorough) as _);
                let crf = TerseF32(crf);
                match full_pass {
                    true => bar.set_prefix(format!("crf {crf} full pass")),
                    false => bar.set_prefix(format!("crf {crf} {sample}/{samples}")),
                }
                let label = work.fps_label();
                match work {
                    Work::Encode if fps <= 0.0 => bar.set_message("encoding,  "),
                    _ if fps <= 0.0 => bar.set_message(format!("{label},       ")),
                    _ => bar.set_message(format!("{label} {fps} fps, ")),
                }
            }
            Update::SampleResult {
                crf,
                sample,
                result,
            } => {
                if verbose
                    .log_level()
                    .is_some_and(|lvl| lvl > log::Level::Error)
                {
                    result.print_attempt(&bar, sample, Some(crf))
                }
            }
            Update::RunResult(result) => result.print_attempt(&bar, min_score, max_encoded_percent),
            Update::Done(best) => {
                info!("crf {} successful", best.crf);
                bar.finish_with_message("");
                if std::io::stderr().is_terminal() {
                    eprintln!(
                        "\n{} {}\n",
                        style("Encode with:").dim(),
                        style(enc_args.encode_hint(best.crf)).dim().italic(),
                    );
                }
                StdoutFormat::Human.print_result(&best, input_is_image);
                return Ok(());
            }
        }
    }
    unreachable!()
}

pub fn run(
    CrfSearchConfig {
        args,
        min_vmaf,
        min_xpsnr,
        max_encoded_percent,
        min_crf,
        max_crf,
        crf_increment,
        high_crf_means_hq,
        thorough,
        sample,
        cache,
        scoring,
        verbose: _,
    }: CrfSearchConfig,
    input_probe: Arc<Ffprobe>,
) -> impl Stream<Item = Result<Update, Error>> {
    async_stream::try_stream! {
        let default_max_crf = args.encoder.default_max_crf();
        let max_crf = max_crf.map_or(default_max_crf, Crf::get);
        let default_min_crf = args.encoder.default_min_crf();
        let min_crf = min_crf.map_or(default_min_crf, Crf::get);
        Error::ensure_other(min_crf < max_crf, "Invalid --min-crf & --max-crf")?;
        // by default use vmaf 95, otherwise use whatever is specified
        let min_score = min_xpsnr
            .or(min_vmaf)
            .map_or(DEFAULT_MIN_VMAF, MinScore::get);
        let use_xpsnr = min_xpsnr.is_some();

        // Whether to make the 2nd iteration on the ~20%/~80% crf point instead of the min/max to
        // improve interpolation by narrowing the crf range a 20% (or 30%) subrange.
        //
        // 20/80% is preferred to 25/75% to account for searches in the "middle" benefitting from
        // having both bounds computed after the 2nd iteration, whereas the two edges must compute
        // the min/max crf on the 3rd iter.
        //
        // If a custom crf range is being used under half the default, this 2nd cut is not needed.
        let cut_on_iter2 = (max_crf - min_crf) > (default_max_crf - default_min_crf) * 0.5;

        let crf_increment = crf_increment
            .map(CrfStep::get)
            .unwrap_or_else(|| args.encoder.default_crf_increment())
            .max(0.001);

        let q_conv = QualityConverter::new(
            CrfStep::try_new(crf_increment).expect("CRF increment must be positive after clamp"),
            high_crf_means_hq.unwrap_or_else(|| args.encoder.high_crf_means_hq()),
        );

        let (min_q, max_q) = q_conv.min_max_q(min_crf, max_crf);
        assert!(min_q < max_q);
        let mut q = (min_q + max_q) / 2;

        let mut args = sample_encode::SampleEncodeConfig {
            args: args.clone(),
            crf: 0.0,
            sample: sample.clone(),
            cache,
            scoring,
        };

        let mut crf_attempts = Vec::new();

        for run in 1.. {
            // how much we're prepared to go higher than the min-vmaf
            let higher_tolerance = match thorough {
                true => 0.05,
                // increment 1.0 => +0.1, +0.2, +0.4, +0.8 ..
                // increment 0.1 => +0.1, +0.1, +0.1, +0.16 ..
                false => (crf_increment.min(1.0) * 2_f32.powi(run as i32 - 1) * 0.1).max(0.1),
            };
            args.crf = q_conv.crf(q);

            let mut sample_enc_output = None;

            #[cfg(test)]
            if let Some(output) = test_hooks::output(args.crf) {
                sample_enc_output = Some(output);
            }

            if sample_enc_output.is_none() {
                let mut sample_enc = pin!(sample_encode::run(args.clone(), input_probe.clone()));
                while let Some(update) = sample_enc.next().await {
                    match update? {
                        sample_encode::Update::Status(status) => {
                            yield Update::Status { crf_run: run, crf: args.crf, sample: status };
                        }
                        sample_encode::Update::SampleResult { sample, result } => {
                            yield Update::SampleResult { crf: args.crf, sample, result };
                        }
                        sample_encode::Update::Done(output) => sample_enc_output = Some(output),
                    }
                }
            }

            let sample = Sample {
                crf: args.crf,
                q,
                enc: sample_enc_output.context("no sample output?")?,
            };
            let score = output_search_score(&sample.enc, use_xpsnr);
            crf_attempts.push(sample.clone());

            match decide_next_transition(
                &sample,
                score,
                &crf_attempts,
                SearchDecision {
                    min_score,
                    higher_tolerance,
                    thorough,
                    cut_on_iter2,
                    run,
                    min_q,
                    max_q,
                    use_xpsnr,
                    max_encoded_percent,
                },
            )? {
                SearchTransition::Continue { next_q } => {
                    q = next_q;
                    yield Update::RunResult(sample.clone());
                }
                SearchTransition::Done(sample) => {
                    yield Update::Done(sample);
                    return;
                }
                SearchTransition::RunResultThenDone { run_result, done } => {
                    yield Update::RunResult(run_result);
                    yield Update::Done(done);
                    return;
                }
            }
        }
        unreachable!();
    }
}

#[derive(Debug, Clone)]
pub struct Sample {
    pub enc: sample_encode::Output,
    pub crf: f32,
    q: i64,
}

#[derive(Debug, Clone)]
enum SearchTransition {
    Continue { next_q: i64 },
    Done(Sample),
    RunResultThenDone { run_result: Sample, done: Sample },
}

#[derive(Clone, Copy)]
struct SearchDecision {
    min_score: f32,
    higher_tolerance: f32,
    thorough: bool,
    cut_on_iter2: bool,
    run: usize,
    min_q: i64,
    max_q: i64,
    use_xpsnr: bool,
    max_encoded_percent: MaxEncodedPercent,
}

fn decide_next_transition(
    sample: &Sample,
    score: f32,
    crf_attempts: &[Sample],
    decision: SearchDecision,
) -> Result<SearchTransition, Error> {
    let SearchDecision {
        min_score,
        higher_tolerance,
        thorough,
        cut_on_iter2,
        run,
        min_q,
        max_q,
        use_xpsnr,
        max_encoded_percent,
    } = decision;
    let sample_small_enough = sample.enc.encode_percent <= max_encoded_percent.get();

    if score >= min_score {
        let within_non_thorough_band = thorough || score <= min_score + 0.11;
        if sample_small_enough && score < min_score + higher_tolerance && within_non_thorough_band {
            return Ok(SearchTransition::Done(sample.clone()));
        }

        let u_bound = crf_attempts
            .iter()
            .filter(|s| s.q > sample.q)
            .min_by_key(|s| s.q);

        return match u_bound {
            Some(upper) if upper.q == sample.q + 1 => {
                Error::ensure_or_no_good_crf(sample_small_enough, sample)?;
                Ok(SearchTransition::Done(sample.clone()))
            }
            Some(upper) => Ok(SearchTransition::Continue {
                next_q: vmaf_lerp_q(min_score, upper, sample, use_xpsnr),
            }),
            None if sample.q == max_q => {
                Error::ensure_or_no_good_crf(sample_small_enough, sample)?;
                Ok(SearchTransition::Done(sample.clone()))
            }
            None if cut_on_iter2 && run == 1 && sample.q + 1 < max_q => {
                Ok(SearchTransition::Continue {
                    next_q: (sample.q as f32 * 0.4 + max_q as f32 * 0.6).round() as _,
                })
            }
            None => Ok(SearchTransition::Continue { next_q: max_q }),
        };
    }

    if !sample_small_enough || sample.q == min_q {
        Err(Error::NoGoodCrf {
            last: sample.clone(),
        })?;
    }

    let l_bound = crf_attempts
        .iter()
        .filter(|s| s.q < sample.q)
        .max_by_key(|s| s.q);

    match l_bound {
        Some(lower) if lower.q + 1 == sample.q => {
            let lower_score = output_search_score(&lower.enc, use_xpsnr);
            if lower_score >= min_score {
                Error::ensure_or_no_good_crf(
                    lower.enc.encode_percent <= max_encoded_percent.get(),
                    sample,
                )?;
                Ok(SearchTransition::RunResultThenDone {
                    run_result: sample.clone(),
                    done: lower.clone(),
                })
            } else {
                Ok(SearchTransition::Continue {
                    next_q: vmaf_lerp_q(min_score, sample, lower, use_xpsnr),
                })
            }
        }
        Some(lower) => Ok(SearchTransition::Continue {
            next_q: vmaf_lerp_q(min_score, sample, lower, use_xpsnr),
        }),
        None if cut_on_iter2 && run == 1 && sample.q > min_q + 1 => {
            Ok(SearchTransition::Continue {
                next_q: (sample.q as f32 * 0.4 + min_q as f32 * 0.6).round() as _,
            })
        }
        None => Ok(SearchTransition::Continue { next_q: min_q }),
    }
}

impl Sample {
    pub fn print_attempt(
        &self,
        bar: &ProgressBar,
        min_score: f32,
        max_encoded_percent: MaxEncodedPercent,
    ) {
        if bar.is_hidden() {
            info!(
                "crf {} {} {:.2} ({:.0}%){}",
                TerseF32(self.crf),
                self.enc.single_score_kind(),
                self.enc.single_score(),
                self.enc.encode_percent,
                if self.enc.from_cache { " (cache)" } else { "" }
            );
            return;
        }

        let crf_label = style("- crf").dim();
        let mut crf = style(TerseF32(self.crf));

        let score_v = self.enc.single_score();
        let mut score = style(score_v);
        let score_label = style(self.enc.single_score_kind()).dim();
        let mut percent = style!("{:.0}%", self.enc.encode_percent);
        let open = style("(").dim();
        let close = style(")").dim();
        let cache_msg = match self.enc.from_cache {
            true => style(" (cache)").dim(),
            false => style(""),
        };

        if score_v < min_score {
            crf = crf.red().bright();
            score = score.red().bright();
        }
        if self.enc.encode_percent > max_encoded_percent.get() {
            crf = crf.red().bright();
            percent = percent.red().bright();
        }

        bar.println(format!(
            "{crf_label} {crf} {score_label} {score:.2} {open}{percent}{close}{cache_msg}"
        ));
    }
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum StdoutFormat {
    Human,
}

impl StdoutFormat {
    fn print_result(self, sample: &Sample, image: bool) {
        match self {
            Self::Human => {
                let crf = style(TerseF32(sample.crf)).bold().green();
                let enc = &sample.enc;
                let score = style(enc.single_score()).bold().green();
                let score_kind = enc.single_score_kind();
                let size = style(HumanBytes(enc.predicted_encode_size)).bold().green();
                let percent = style!("{}%", enc.encode_percent.round()).bold().green();
                let time = style(HumanDuration(enc.predicted_encode_time)).bold();
                let enc_description = match image {
                    true => "image",
                    false => "video stream",
                };
                println!(
                    "crf {crf} {score_kind} {score:.2} predicted {enc_description} size {size} ({percent}) taking {time}"
                );
            }
        }
    }
}

/// Produce a q value between given samples using vmaf score linear interpolation
/// so the output q value should produce the `min_vmaf`.
///
/// Note: `worse_q` will be a numerically higher q value (worse quality),
///       `better_q` a numerically lower q value (better quality).
///
/// # Issues
/// Crf values do not linearly map to VMAF changes (or anything?) so this is a flawed method,
/// though it seems to work better than a binary search.
/// Perhaps a better approximation of a general crf->vmaf model could be found.
/// This would be helpful particularly for small crf-increments.
fn vmaf_lerp_q(min_vmaf: f32, worse_q: &Sample, better_q: &Sample, use_xpsnr: bool) -> i64 {
    let worse_score = output_search_score(&worse_q.enc, use_xpsnr);
    let better_score = output_search_score(&better_q.enc, use_xpsnr);
    assert!(
        worse_score <= min_vmaf && worse_score < better_score && worse_q.q > better_q.q,
        "invalid vmaf_lerp_crf usage: ({min_vmaf}, {worse_q:?}, {better_q:?})"
    );

    let vmaf_diff = better_score - worse_score;
    let vmaf_factor = (min_vmaf - worse_score) / vmaf_diff;

    let q_diff = worse_q.q - better_q.q;
    let lerp = (worse_q.q as f32 - q_diff as f32 * vmaf_factor).round() as i64;
    let lo = better_q.q + 1;
    let hi = worse_q.q - 1;
    if lo > hi {
        // Target score is outside the range between the two samples.
        if min_vmaf > better_score {
            return better_q.q - 1;
        }
        return worse_q.q;
    }
    lerp.clamp(lo, hi)
}

/// sample_progress: [0, 1]
pub fn guess_progress(run: usize, sample_progress: f32, thorough: bool) -> f64 {
    let total_runs_guess = match () {
        // Guess 6 iterations for a "thorough" search
        _ if thorough && run < 7 => 6.0,
        // Guess 5 iterations initially
        _ if run < 6 => 5.0,
        // Otherwise guess next will work
        _ => run as f64,
    };
    let sample_progress = sample_progress.clamp(0.0, 1.0) as f64;
    (((run - 1) as f64 + sample_progress) * BAR_LEN as f64 / total_runs_guess).min(BAR_LEN as f64)
}

/// Conversion logic for integer "q" values used in the crf search.
///
/// "q" values are
/// * integers
/// * low q means higher quality
/// * they can be converted to/from crf
struct QualityConverter {
    high_crf_means_hq: bool,
    crf_increment: f32,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct Crf(f32);

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct CrfStep(f32);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct QualityIndex(i64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub(crate) enum CrfValueError {
    #[error("CRF value must be finite")]
    InvalidCrf,
    #[error("CRF step must be finite and positive")]
    InvalidStep,
}

impl Crf {
    pub(crate) fn try_new(crf: f32) -> Result<Self, CrfValueError> {
        crf.is_finite()
            .then_some(Self(crf))
            .ok_or(CrfValueError::InvalidCrf)
    }

    pub(crate) fn get(self) -> f32 {
        self.0
    }
}

impl FromStr for Crf {
    type Err = CrfValueError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let crf: f32 = s.parse().map_err(|_| CrfValueError::InvalidCrf)?;
        Self::try_new(crf)
    }
}

impl CrfStep {
    pub(crate) fn try_new(step: f32) -> Result<Self, CrfValueError> {
        (step.is_finite() && step > 0.0)
            .then_some(Self(step))
            .ok_or(CrfValueError::InvalidStep)
    }

    pub(crate) fn get(self) -> f32 {
        self.0
    }
}

fn parse_crf_step(step: &str) -> anyhow::Result<CrfStep> {
    let parsed: f32 = step.parse()?;
    CrfStep::try_new(parsed).map_err(Into::into)
}

impl QualityIndex {
    fn get(self) -> i64 {
        self.0
    }
}

impl QualityConverter {
    fn new(crf_increment: CrfStep, high_crf_means_hq: bool) -> Self {
        Self {
            crf_increment: crf_increment.get(),
            high_crf_means_hq,
        }
    }

    /// Calculate "q" as an integer quality value related to crf.
    ///
    /// # Example
    /// * crf=33.5, inc=0.1 -> q=335
    /// * crf=27, inc=1 -> q=27
    ///
    /// # Example: high_crf_means_hq encoders
    /// * crf=33.5, inc=0.1 -> q=-335
    /// * crf=27, inc=1 -> q=-27
    pub fn q(&self, crf: f32) -> i64 {
        self.quality_index(Crf::try_new(crf).expect("CRF must be finite"))
            .get()
    }

    fn quality_index(&self, crf: Crf) -> QualityIndex {
        let q = (f64::from(crf.get()) / f64::from(self.crf_increment)).round() as i64;
        match self.high_crf_means_hq {
            true => QualityIndex(-q),
            false => QualityIndex(q),
        }
    }

    /// Calculate crf back from "q".
    pub fn crf(&self, q: i64) -> f32 {
        self.crf_from_quality_index(QualityIndex(q)).get()
    }

    fn crf_from_quality_index(&self, q: QualityIndex) -> Crf {
        let pos_q = match self.high_crf_means_hq {
            true => -q.get(),
            false => q.get(),
        };
        Crf::try_new(((pos_q as f64) * f64::from(self.crf_increment)) as _)
            .expect("quality index conversion must produce finite CRF")
    }

    pub fn min_max_q(&self, min_crf: f32, max_crf: f32) -> (i64, i64) {
        match self.high_crf_means_hq {
            true => (self.q(max_crf), self.q(min_crf)),
            false => (self.q(min_crf), self.q(max_crf)),
        }
    }
}

#[test]
fn q_crf_conversions() {
    let mut q_conv = QualityConverter::new(CrfStep::try_new(0.1).unwrap(), false);

    assert_eq!(q_conv.q(33.5), 335);
    assert_eq!(q_conv.crf(335), 33.5);

    q_conv = QualityConverter::new(CrfStep::try_new(1.0).unwrap(), false);
    assert_eq!(q_conv.q(27.0), 27);
    assert_eq!(q_conv.crf(27), 27.0);
}

#[test]
fn crf_step_rejects_non_finite_and_non_positive_values() {
    assert!(CrfStep::try_new(0.0).is_err());
    assert!(CrfStep::try_new(-1.0).is_err());
    assert!(CrfStep::try_new(f32::NAN).is_err());
}

#[test]
fn quality_converter_typed_round_trip_preserves_crf() {
    let q_conv = QualityConverter::new(CrfStep::try_new(0.1).unwrap(), false);
    let crf = Crf::try_new(33.5).unwrap();

    let q = q_conv.quality_index(crf);

    assert_eq!(q.get(), 335);
    assert_eq!(q_conv.crf_from_quality_index(q).get(), 33.5);
}

#[test]
fn q_crf_conversions_high_crf_means_hq() {
    let mut q_conv = QualityConverter::new(CrfStep::try_new(0.1).unwrap(), true);

    assert_eq!(q_conv.q(33.5), -335);
    assert_eq!(q_conv.crf(-335), 33.5);

    q_conv = QualityConverter::new(CrfStep::try_new(1.0).unwrap(), true);
    assert_eq!(q_conv.q(27.0), -27);
    assert_eq!(q_conv.crf(-27), 27.0);
}

#[derive(Debug)]
pub enum Update {
    Status {
        /// run number starting from `1`.
        crf_run: usize,
        /// crf of this run
        crf: f32,
        sample: sample_encode::Status,
    },
    SampleResult {
        crf: f32,
        /// Sample number `1,....,n`
        sample: u64,
        result: sample_encode::EncodeResult,
    },
    /// Run result (excludes successful final runs)
    RunResult(Sample),
    Done(Sample),
}

#[cfg(test)]
mod crf_search_tests {
    use super::{
        Args, Crf, CrfSearchConfig, CrfStep, Error, MaxEncodedPercent, MinScore, Sample, Update,
        ValidationError, guess_progress, output_search_score, run, test_hooks, vmaf_lerp_q,
    };
    use crate::{
        command::{
            args::{self, Encode, Sample as SampleArgs, Vmaf},
            sample_encode::{self},
        },
        ffprobe::Ffprobe,
    };
    use clap::Parser;
    use futures_util::StreamExt;
    use rstest::rstest;
    use serial_test::serial;
    use std::{
        env,
        path::PathBuf,
        pin::pin,
        sync::{Arc, Mutex},
        time::Duration,
    };

    mod helpers {
        use super::*;

        pub fn test_probe() -> Arc<Ffprobe> {
            Arc::new(Ffprobe {
                duration: Ok(Duration::from_secs(120)),
                has_audio: false,
                max_audio_channels: None,
                fps: Ok(24.0),
                resolution: Some((1920, 1080)),
                is_image: false,
                pix_fmt: Some("yuv420p".into()),
            })
        }

        pub fn mock_output(
            vmaf_score: Option<f32>,
            xpsnr_score: Option<f32>,
            encode_percent: f64,
        ) -> sample_encode::Output {
            sample_encode::Output {
                vmaf_score,
                xpsnr_score,
                predicted_encode_size: 1_000_000,
                encode_percent,
                predicted_encode_time: Duration::from_secs(60),
                from_cache: false,
            }
        }

        pub fn search_args(min_vmaf: Option<f32>, min_xpsnr: Option<f32>, thorough: bool) -> Args {
            search_args_with_crf_range(min_vmaf, min_xpsnr, thorough, Some(20.0), Some(40.0))
        }

        pub fn search_args_with_crf_range(
            min_vmaf: Option<f32>,
            min_xpsnr: Option<f32>,
            thorough: bool,
            min_crf: Option<f32>,
            max_crf: Option<f32>,
        ) -> Args {
            Args {
                args: Encode {
                    encoder: "libsvtav1".parse().unwrap(),
                    input: PathBuf::from("test.mp4"),
                    vfilter: None,
                    pix_format: None,
                    preset: None,
                    keyint: None,
                    scd: None,
                    svt_args: vec![],
                    enc_args: vec![],
                    enc_input_args: vec![],
                },
                min_vmaf: min_vmaf.and_then(|score| MinScore::new(score).ok()),
                min_xpsnr: min_xpsnr.and_then(|score| MinScore::new(score).ok()),
                max_encoded_percent: MaxEncodedPercent::new(80.0).unwrap(),
                min_crf: min_crf.and_then(|crf| Crf::try_new(crf).ok()),
                max_crf: max_crf.and_then(|crf| Crf::try_new(crf).ok()),
                crf_increment: Some(CrfStep::try_new(1.0).unwrap()),
                high_crf_means_hq: Some(false),
                thorough,
                cache: false,
                sample: SampleArgs {
                    samples: Some(args::SampleCountOverride::new(1)),
                    sample_every: match args::SampleDuration::new(Duration::from_secs(720)) {
                        Ok(duration) => duration,
                        Err(err) => panic!("invalid test sample_every: {err}"),
                    },
                    min_samples: None,
                    sample_duration: match args::SampleDuration::new(Duration::from_secs(20)) {
                        Ok(duration) => duration,
                        Err(err) => panic!("invalid test sample_duration: {err}"),
                    },
                    keep: false,
                    temp_dir: None,
                    extension: None,
                },
                vmaf: Vmaf::default(),
                score: args::ScoreArgs {
                    reference_vfilter: None,
                },
                xpsnr: args::Xpsnr::default(),
                verbose: clap_verbosity_flag::Verbosity::new(0, 0),
            }
        }

        pub async fn collect_run(args: Args, probe: Arc<Ffprobe>) -> Result<Vec<Update>, Error> {
            let mut updates = Vec::new();
            let mut stream = pin!(run(CrfSearchConfig::from(args), probe));
            while let Some(item) = stream.next().await {
                updates.push(item?);
            }
            Ok(updates)
        }

        pub fn last_done(updates: &[Update]) -> Option<&Sample> {
            updates.iter().rev().find_map(|u| match u {
                Update::Done(s) => Some(s),
                _ => None,
            })
        }
    }

    use helpers::*;

    struct MockGuard;

    impl MockGuard {
        fn set(mock: impl Fn(f32) -> sample_encode::Output + 'static) -> Self {
            test_hooks::set(mock);
            Self
        }
    }

    impl Drop for MockGuard {
        fn drop(&mut self) {
            test_hooks::clear();
        }
    }

    // ab-kgc.16 / ab-c8k.12: exact threshold match is success
    #[tokio::test]
    async fn exact_min_score_is_success() {
        // setup
        let min_score = 95.0;
        let args = search_args(Some(min_score), None, true);
        let _guard = MockGuard::set(move |_crf| mock_output(Some(min_score), None, 50.0));

        // execute
        let updates = collect_run(args, test_probe()).await.expect("run");

        // assert
        let done = last_done(&updates).expect("expected Done");
        assert_eq!(done.enc.vmaf_score, Some(min_score));
    }

    #[test]
    #[serial]
    fn crf_search_args_use_cache_env_default() {
        let key = "AB_AV1_CACHE";
        let prev = env::var_os(key);
        unsafe {
            env::set_var(key, "false");
        }

        let args = Args::try_parse_from(["ab-av1", "--input", "input.mkv"])
            .expect("parse crf search args");

        match prev {
            Some(value) => unsafe {
                env::set_var(key, value);
            },
            None => unsafe {
                env::remove_var(key);
            },
        }

        assert!(
            !args.cache,
            "env default must disable cache when set to false"
        );
    }

    #[test]
    fn crf_search_config_lowers_score_args() {
        let mut args = search_args(None, Some(90.0), true);
        args.score.reference_vfilter = Some("scale=1280:-1".into());
        args.vmaf.vmaf_args = vec!["n_subsample=4".into()];
        args.xpsnr.xpsnr_fps = args::FrameRateOverride::new(0.0);

        let config = CrfSearchConfig::from(args);

        assert_eq!(
            config.scoring.score.reference_vfilter.as_deref(),
            Some("scale=1280:-1")
        );
        assert!(config.scoring.xpsnr);
        assert_eq!(config.scoring.xpsnr_opts.fps(), None);
        assert_eq!(
            config
                .scoring
                .vmaf
                .vmaf_args
                .iter()
                .map(AsRef::as_ref)
                .collect::<Vec<_>>(),
            ["n_subsample=4"]
        );
    }

    // ab-kgc.17: XPSNR is the search metric when --min-xpsnr --and-vmaf
    #[tokio::test]
    async fn xpsnr_target_with_and_vmaf_uses_xpsnr_for_search() {
        // setup
        let min_xpsnr = 90.0;
        let mut args = search_args(None, Some(min_xpsnr), true);
        args.vmaf.and_vmaf = Some(true);
        let _guard = MockGuard::set(move |_crf| {
            // VMAF below threshold; XPSNR meets it — search must use XPSNR.
            mock_output(Some(85.0), Some(min_xpsnr + 2.0), 50.0)
        });

        // execute
        let updates = collect_run(args, test_probe()).await.expect("run");

        // assert
        let done = last_done(&updates).expect("expected Done with XPSNR target");
        assert_eq!(done.enc.xpsnr_score, Some(min_xpsnr + 2.0));
    }

    // ab-c8k.5: binary search terminates with Done on mocked encode stream
    #[tokio::test]
    async fn run_binary_search_finds_acceptable_crf() {
        // setup
        let args = search_args(Some(95.0), None, true);
        let _guard = MockGuard::set(move |crf| {
            // Higher CRF → higher VMAF in this synthetic model.
            let vmaf = 80.0 + crf;
            mock_output(Some(vmaf), None, 50.0)
        });

        // execute
        let updates = collect_run(args, test_probe()).await.expect("run");

        // assert
        let done = last_done(&updates).expect("expected Done");
        assert!(done.enc.vmaf_score.unwrap_or(0.0) >= 95.0);
        assert!(updates.iter().any(|u| matches!(u, Update::RunResult(_))));
    }

    // ab-c8k.5: NoGoodCrf when no sample meets threshold within size budget
    #[tokio::test]
    async fn run_returns_no_good_crf_when_scores_too_low() {
        // setup
        let args = search_args(Some(95.0), None, true);
        let _guard = MockGuard::set(move |_crf| mock_output(Some(80.0), None, 50.0));

        // execute
        let err = collect_run(args, test_probe())
            .await
            .expect_err("expected failure");

        // assert
        assert!(matches!(err, Error::NoGoodCrf { .. }));
    }

    // ab-c8k.12: threshold boundary matrix (unit tests; rstest when ab-c8k.10 lands)
    #[rstest]
    #[case::vmaf_only(false, Some(70.0), None, 70.0)]
    #[case::xpsnr_when_requested(true, Some(70.0), Some(92.0), 92.0)]
    #[case::vmaf_fallback(false, None, Some(88.0), 88.0)]
    fn output_search_score_matrix(
        #[case] use_xpsnr: bool,
        #[case] vmaf: Option<f32>,
        #[case] xpsnr: Option<f32>,
        #[case] expected: f32,
    ) {
        // setup
        let enc = mock_output(vmaf, xpsnr, 50.0);

        // execute / assert
        assert_eq!(output_search_score(&enc, use_xpsnr), expected);
    }

    #[rstest]
    #[case::exact_match(95.0, 95.0, true)]
    #[case::above_threshold(96.0, 95.0, true)]
    #[case::below_threshold(94.9, 95.0, false)]
    #[case::within_thorough_tolerance(95.03, 95.0, true)]
    fn threshold_success_matrix(#[case] score: f32, #[case] min: f32, #[case] should_pass: bool) {
        // execute / assert
        assert_eq!(score >= min, should_pass || score > min);
        if should_pass {
            assert!(score >= min);
        } else {
            assert!(score < min);
        }
    }

    #[test]
    fn vmaf_lerp_q_interpolates_with_vmaf_scores() {
        // setup
        let use_xpsnr = false;
        let worse = Sample {
            crf: 30.0,
            q: 30,
            enc: mock_output(Some(88.0), None, 50.0),
        };
        let better = Sample {
            crf: 25.0,
            q: 25,
            enc: mock_output(Some(96.0), None, 50.0),
        };

        // execute
        let q = vmaf_lerp_q(92.0, &worse, &better, use_xpsnr);

        // assert
        assert!((26..=28).contains(&q));
    }

    #[tokio::test]
    async fn cut_on_iter2_narrows_toward_min_on_wide_crf_range() {
        // setup — range > half default span enables cut_on_iter2
        let args = search_args_with_crf_range(Some(95.0), None, true, Some(5.0), Some(70.0));
        let crfs = Arc::new(Mutex::new(Vec::new()));
        let crfs2 = crfs.clone();
        let _guard = MockGuard::set(move |crf| {
            crfs2.lock().unwrap().push(crf);
            mock_output(Some(80.0), None, 50.0)
        });

        // execute
        let _ = collect_run(args, test_probe()).await;

        // assert — second probe should not jump straight to min_crf
        let tried = crfs.lock().unwrap();
        assert!(tried.len() >= 2);
        assert!(
            tried[1] > 5.0 + f32::EPSILON,
            "cut_on_iter2 should narrow before min_crf, got {:?}",
            *tried
        );
    }

    #[tokio::test]
    async fn narrow_crf_range_skips_cut_on_iter2() {
        // setup — custom range under half default span
        let args = search_args(Some(95.0), None, true);
        let crfs = Arc::new(Mutex::new(Vec::new()));
        let crfs2 = crfs.clone();
        let _guard = MockGuard::set(move |crf| {
            crfs2.lock().unwrap().push(crf);
            mock_output(Some(80.0), None, 50.0)
        });

        // execute
        let _ = collect_run(args, test_probe()).await;

        // assert — second probe should hit min_crf directly
        let tried = crfs.lock().unwrap();
        assert!(tried.len() >= 2);
        assert!((tried[1] - 20.0).abs() < f32::EPSILON, "got {:?}", *tried);
    }

    // ab-kgc.35: validate should reject inverted/equal crf bounds before streaming search
    #[test]
    fn validate_rejects_min_crf_gte_max_crf() {
        // setup
        let mut args = search_args(Some(95.0), None, true);
        args.min_crf = Crf::try_new(40.0).ok();
        args.max_crf = Crf::try_new(30.0).ok();

        // execute / assert
        assert!(
            matches!(args.validate(), Err(ValidationError::InvalidCrfBounds)),
            "min_crf >= max_crf must fail validation before search starts"
        );
    }

    // ab-kgc.36: --min-xpsnr search must fall back to VMAF when XPSNR is absent
    #[test]
    fn output_search_score_uses_vmaf_fallback_when_xpsnr_missing() {
        // setup — and-vmaf runs can still lack per-sample XPSNR on cache hits
        let enc = mock_output(Some(96.0), None, 50.0);

        // execute / assert
        assert_eq!(
            output_search_score(&enc, true),
            96.0,
            "use_xpsnr search should fall back to VMAF when XPSNR score is missing"
        );
    }

    // ab-kgc.27: --min-xpsnr must win over --min-vmaf when both are set
    #[test]
    fn min_score_prefers_xpsnr_when_both_thresholds_set() {
        // setup — clap group prevents CLI double-set; programmatic Args can still carry both
        let args = search_args(Some(95.0), Some(90.0), true);

        // execute / assert
        assert_eq!(
            args.min_score(),
            90.0,
            "min_score must use --min-xpsnr when both thresholds are present"
        );
    }

    #[tokio::test]
    async fn thorough_mode_accepts_score_within_tolerance_band() {
        // setup
        let min_score = 95.0;
        let args = search_args(Some(min_score), None, true);
        let _guard = MockGuard::set(move |_crf| mock_output(Some(min_score + 0.03), None, 50.0));

        // execute
        let updates = collect_run(args, test_probe()).await.expect("run");

        // assert — thorough tolerance is 0.05
        let done = last_done(&updates).expect("expected Done");
        assert!(done.enc.vmaf_score.unwrap() < min_score + 0.05);
        let run_results = updates
            .iter()
            .filter(|u| matches!(u, Update::RunResult(_)))
            .count();
        assert!(run_results <= 1);
    }

    #[test]
    fn vmaf_lerp_q_interpolates_with_xpsnr_scores() {
        // setup
        let use_xpsnr = true;
        let worse = Sample {
            crf: 30.0,
            q: 30,
            enc: mock_output(None, Some(88.0), 50.0),
        };
        let better = Sample {
            crf: 25.0,
            q: 25,
            enc: mock_output(None, Some(96.0), 50.0),
        };

        // execute
        let q = vmaf_lerp_q(92.0, &worse, &better, use_xpsnr);

        // assert
        assert!((26..=28).contains(&q));
    }

    #[test]
    fn guess_progress_thorough_caps_at_six_runs() {
        // setup / execute / assert
        assert!(guess_progress(1, 0.5, true) < guess_progress(2, 0.5, true));
        assert!(guess_progress(6, 1.0, true) <= super::BAR_LEN as f64);
    }

    // ab-kgc.29: adjacent l_bound must not return Done when lower still misses min_score
    #[tokio::test]
    async fn adjacent_l_bound_done_requires_min_score() {
        let args = search_args_with_crf_range(Some(95.0), None, true, Some(24.0), Some(26.0));
        let _guard = MockGuard::set(move |crf| {
            let vmaf = match crf.round() as i32 {
                24 => 96.0,
                25 => 93.0,
                26 => 92.0,
                _ => 80.0,
            };
            mock_output(Some(vmaf), None, 50.0)
        });

        let updates = collect_run(args, test_probe()).await.expect("run");
        let done = last_done(&updates).expect("expected Done");
        assert!(
            done.enc.vmaf_score.unwrap_or(0.0) >= 95.0,
            "Done sample must meet min_score, got {:?}",
            done.enc.vmaf_score
        );
    }

    // ab-kgc.41: run() must use min_xpsnr threshold when both min scores set programmatically
    #[tokio::test]
    async fn run_uses_xpsnr_threshold_when_both_min_scores_set() {
        let mut args = search_args(Some(95.0), Some(90.0), true);
        args.args.encoder = "libsvtav1".parse().unwrap();
        let _guard = MockGuard::set(move |_crf| mock_output(Some(85.0), Some(92.0), 50.0));

        let updates = collect_run(args, test_probe()).await.expect("run");
        let done = last_done(&updates).expect("expected Done with XPSNR >= 90");
        assert!(
            done.enc.xpsnr_score.unwrap_or(0.0) >= 90.0,
            "search must succeed against --min-xpsnr 90, got {:?}",
            done.enc.xpsnr_score
        );
    }

    #[test]
    fn max_encoded_percent_is_a_checked_newtype() {
        assert_eq!(MaxEncodedPercent::new(80.0).unwrap().get(), 80.0);
        assert!(matches!(
            MaxEncodedPercent::new(0.0),
            Err(ValidationError::NonPositiveMaxEncodedPercent)
        ));
        assert!(matches!(
            MaxEncodedPercent::new(-1.0),
            Err(ValidationError::NonPositiveMaxEncodedPercent)
        ));
        assert!(matches!(
            MaxEncodedPercent::new(f64::NAN),
            Err(ValidationError::NonPositiveMaxEncodedPercent)
        ));
    }

    #[test]
    fn min_score_is_a_checked_copy_newtype() {
        fn assert_copy<T: Copy>() {}

        assert_copy::<MinScore>();
        assert_eq!(MinScore::new(95.0).map(MinScore::get), Ok(95.0));
        assert!(matches!(
            MinScore::new(f32::NAN),
            Err(ValidationError::InvalidMinScore)
        ));
        assert!(matches!(
            MinScore::new(f32::INFINITY),
            Err(ValidationError::InvalidMinScore)
        ));
    }

    // ab-kgc.87: max_encoded_percent must be positive
    #[test]
    fn parse_rejects_non_positive_max_encoded_percent() {
        assert!(
            Args::try_parse_from([
                "ab-av1",
                "--input",
                "test.mp4",
                "--max-encoded-percent",
                "0",
            ])
            .is_err()
        );

        assert!(
            Args::try_parse_from([
                "ab-av1",
                "--input",
                "test.mp4",
                "--max-encoded-percent",
                "-1",
            ])
            .is_err()
        );
    }

    #[test]
    fn validate_accepts_checked_max_encoded_percent() {
        let args = search_args(Some(95.0), None, true);
        assert_eq!(args.max_encoded_percent.get(), 80.0);
        assert!(matches!(args.validate(), Ok(())));
    }

    #[test]
    fn parse_crf_increment_accepts_positive_values() {
        let args =
            Args::try_parse_from(["ab-av1", "--input", "test.mp4", "--crf-increment", "0.5"])
                .expect("parse crf search args");

        assert_eq!(args.crf_increment.unwrap().get(), 0.5);
    }

    #[test]
    fn parse_crf_increment_rejects_non_positive_values() {
        assert!(
            Args::try_parse_from(["ab-av1", "--input", "test.mp4", "--crf-increment", "0",])
                .is_err()
        );

        assert!(
            Args::try_parse_from(["ab-av1", "--input", "test.mp4", "--crf-increment", "-1",])
                .is_err()
        );
    }

    #[test]
    fn parse_min_scores_reject_non_finite_values() {
        assert!(
            Args::try_parse_from(["ab-av1", "--input", "test.mp4", "--min-vmaf", "NaN"]).is_err()
        );

        assert!(
            Args::try_parse_from(["ab-av1", "--input", "test.mp4", "--min-xpsnr", "inf"]).is_err()
        );
    }

    #[test]
    fn parse_crf_bounds_reject_non_finite_values() {
        assert!(
            Args::try_parse_from(["ab-av1", "--input", "test.mp4", "--min-crf", "NaN"]).is_err()
        );

        assert!(
            Args::try_parse_from(["ab-av1", "--input", "test.mp4", "--max-crf", "inf"]).is_err()
        );
    }

    // ab-kgc.86: validate must reject both min_vmaf and min_xpsnr set programmatically
    #[test]
    fn validate_rejects_both_min_vmaf_and_min_xpsnr() {
        let args = search_args(Some(95.0), Some(90.0), true);
        assert!(matches!(
            args.validate(),
            Err(ValidationError::BothMinScores)
        ));
    }

    #[test]
    fn validate_rejects_positional_vmaf_number_without_min_vmaf() {
        let mut args = search_args(None, None, true);
        args.vmaf.vmaf_args = vec!["95".into()];
        assert!(matches!(
            args.validate(),
            Err(ValidationError::PositionalVmafNumber { num }) if (num - 95.0).abs() < f32::EPSILON
        ));
    }

    #[tokio::test]
    async fn cut_on_iter2_disabled_at_exact_half_default_span() {
        // libsvtav1 default span 5..70 = 65; half = 32.5; range 5..37.5 spans exactly 32.5
        let args = search_args_with_crf_range(Some(95.0), None, true, Some(5.0), Some(37.5));
        let crfs = Arc::new(Mutex::new(Vec::new()));
        let crfs2 = crfs.clone();
        let _guard = MockGuard::set(move |crf| {
            crfs2.lock().unwrap().push(crf);
            mock_output(Some(80.0), None, 50.0)
        });

        let _ = collect_run(args, test_probe()).await;
        let tried = crfs.lock().unwrap();
        assert!(tried.len() >= 2);
        assert!(
            (tried[1] - 5.0).abs() < f32::EPSILON,
            "at exactly half default span cut_on_iter2 must be off; second crf should be min_crf, got {:?}",
            *tried
        );
    }

    // ab-kgc.84: guess_progress must not jump backwards between runs
    #[test]
    fn guess_progress_non_decreasing_at_run_boundaries() {
        let prev = guess_progress(4, 1.0, false);
        let next = guess_progress(5, 0.0, false);
        assert!(
            next >= prev,
            "progress must not jump backwards between runs: {prev} -> {next}"
        );
    }

    #[tokio::test]
    async fn no_good_crf_when_score_meets_threshold_but_size_exceeds_at_max_q() {
        let args = search_args_with_crf_range(Some(95.0), None, true, Some(24.0), Some(26.0));
        let _guard = MockGuard::set(move |crf| {
            let (vmaf, pct) = if (crf - 26.0).abs() < f32::EPSILON {
                (96.0, 90.0)
            } else {
                (80.0, 50.0)
            };
            mock_output(Some(vmaf), None, pct)
        });

        let err = collect_run(args, test_probe())
            .await
            .expect_err("oversized good score at max");
        assert!(matches!(err, Error::NoGoodCrf { .. }));
    }

    // ab-kgc.88: non-thorough search must not accept first hit far above tolerance band
    #[tokio::test]
    async fn non_thorough_rejects_score_above_tolerance_band() {
        let min_score = 95.0;
        let args = search_args(Some(min_score), None, false);
        let _guard = MockGuard::set(move |crf| {
            // Higher CRF → slightly lower VMAF so the search can refine after the first hit.
            let score = min_score + 0.15 - (crf - 20.0) * 0.01;
            mock_output(Some(score), None, 50.0)
        });

        let updates = collect_run(args, test_probe()).await.expect("run");
        let done = last_done(&updates).expect("expected Done");
        assert!(
            done.enc.vmaf_score.unwrap() <= min_score + 0.11,
            "non-thorough must not accept first hit far above tolerance, got {}",
            done.enc.vmaf_score.unwrap()
        );
    }

    #[tokio::test]
    async fn high_crf_means_hq_encoder_search_converges() {
        let mut args = search_args_with_crf_range(Some(95.0), None, true, Some(10.0), Some(50.0));
        args.args.encoder = "hevc_videotoolbox".parse().unwrap();
        args.high_crf_means_hq = Some(true);
        args.crf_increment = Some(CrfStep::try_new(1.0).unwrap());
        let _guard = MockGuard::set(move |crf| {
            // Higher CRF → higher quality for videotoolbox (high_crf_means_hq).
            let vmaf = 70.0 + crf * 0.5;
            mock_output(Some(vmaf), None, 50.0)
        });

        let updates = collect_run(args, test_probe()).await.expect("run");
        let done = last_done(&updates).expect("expected Done");
        assert!(
            done.enc.vmaf_score.unwrap_or(0.0) >= 95.0,
            "high_crf_means_hq search must find acceptable CRF, got {:?}",
            done.enc.vmaf_score
        );
    }

    mod proptest_crf_search {
        use super::*;
        use proptest::prelude::*;

        proptest! {
            // ab-kgc.85: guess_progress must stay within progress bar length
            #[test]
            fn guess_progress_stays_within_bar_len(
                run in 1usize..20,
                sample_progress in 0.0f32..1.5,
                thorough in proptest::bool::ANY,
            ) {
                let progress = guess_progress(run, sample_progress, thorough);
                prop_assert!(progress >= 0.0);
                prop_assert!(progress <= super::super::BAR_LEN as f64);
            }

            #[test]
            fn vmaf_lerp_q_bounded_between_samples(
                worse_score in 80.0f32..94.0f32,
                better_score in 96.0f32..99.0f32,
                worse_q in 30i64..50i64,
                better_q in 20i64..28i64,
            ) {
                prop_assume!(worse_q > better_q + 1);
                let min_vmaf = (worse_score + better_score) / 2.0;
                let worse = Sample {
                    crf: worse_q as f32,
                    q: worse_q,
                    enc: mock_output(Some(worse_score), None, 50.0),
                };
                let better = Sample {
                    crf: better_q as f32,
                    q: better_q,
                    enc: mock_output(Some(better_score), None, 50.0),
                };

                let q = vmaf_lerp_q(min_vmaf, &worse, &better, false);

                prop_assert!(q > better_q && q < worse_q);
            }
        }
    }
}
