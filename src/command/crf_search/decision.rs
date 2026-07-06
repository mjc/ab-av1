use super::{BAR_LEN, Error, MaxEncodedPercent, Sample, output_search_score};

#[derive(Debug, Clone)]
pub enum SearchTransition {
    Continue { next_q: i64 },
    Done(Sample),
    RunResultThenDone { run_result: Sample, done: Sample },
}

#[derive(Clone, Copy)]
pub struct SearchDecision {
    pub min_score: f32,
    pub higher_tolerance: f32,
    pub thorough: bool,
    pub cut_on_iter2: bool,
    pub run: usize,
    pub min_q: i64,
    pub max_q: i64,
    pub use_xpsnr: bool,
    pub max_encoded_percent: MaxEncodedPercent,
}

pub fn decide_next_transition(
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

/// Produce a q value between given samples using vmaf score linear interpolation.
pub fn vmaf_lerp_q(min_vmaf: f32, worse_q: &Sample, better_q: &Sample, use_xpsnr: bool) -> i64 {
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
        _ if thorough && run < 7 => 6.0,
        _ if run < 6 => 5.0,
        _ => run as f64,
    };
    let sample_progress = sample_progress.clamp(0.0, 1.0) as f64;
    (((run - 1) as f64 + sample_progress) * BAR_LEN as f64 / total_runs_guess).min(BAR_LEN as f64)
}
