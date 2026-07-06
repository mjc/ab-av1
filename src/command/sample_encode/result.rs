use crate::console_ext::style;

use console::style;
use indicatif::{HumanBytes, HumanDuration, ProgressBar};
use log::info;
use std::{fmt::Display, time::Duration};

mod score_json {
    use serde::{Deserialize, Deserializer, Serializer};

    pub fn serialize<S>(score: &Option<f32>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match score {
            Some(v) if v.is_nan() => serializer.serialize_str("NaN"),
            Some(v) => serializer.serialize_some(v),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<f32>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = Option::<serde_json::Value>::deserialize(deserializer)?;
        match value {
            None | Some(serde_json::Value::Null) => Ok(None),
            Some(serde_json::Value::String(s)) if s.eq_ignore_ascii_case("nan") => {
                Ok(Some(f32::NAN))
            }
            Some(serde_json::Value::Number(n)) => n
                .as_f64()
                .map(|v| Some(v as f32))
                .ok_or_else(|| serde::de::Error::custom("invalid score number")),
            Some(other) => Err(serde::de::Error::custom(format!(
                "invalid score value: {other}"
            ))),
        }
    }
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct EncodeResult {
    pub sample_size: u64,
    pub encoded_size: u64,
    #[serde(
        serialize_with = "score_json::serialize",
        deserialize_with = "score_json::deserialize"
    )]
    pub vmaf_score: Option<f32>,
    #[serde(
        serialize_with = "score_json::serialize",
        deserialize_with = "score_json::deserialize"
    )]
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
        let percent = if *sample_size == 0 {
            0.0
        } else {
            100.0 * *encoded_size as f32 / *sample_size as f32
        };
        bar.println(
            style!(
                "- {}Sample {sample_n} ({percent:.0}%){}{}{}",
                crf.map(|crf| format!("crf {crf}: ")).unwrap_or_default(),
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
        let percent = if *sample_size == 0 {
            0.0
        } else {
            100.0 * *encoded_size as f32 / *sample_size as f32
        };
        info!(
            "sample {sample_n}/{samples} crf {crf}{}{} ({percent:.0}%){}",
            vmaf_score
                .map(|s| format!(" VMAF {s:.2}"))
                .unwrap_or_default(),
            xpsnr_score
                .map(|s| format!(" XPSNR {s:.2}"))
                .unwrap_or_default(),
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

pub(crate) trait EncodeResults {
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

fn mean_finite_scores(scores: impl Iterator<Item = Option<f32>>) -> Option<f32> {
    let (sum, count) = scores
        .flatten()
        .filter(|score| score.is_finite())
        .fold((0.0, 0usize), |(sum, count), score| {
            (sum + score, count + 1)
        });
    (count > 0).then_some(sum / count as f32)
}

impl EncodeResults for Vec<EncodeResult> {
    fn encoded_percent_size(&self) -> f64 {
        if self.is_empty() {
            return 0.0;
        }
        let encoded = self.iter().map(|r| r.encoded_size).sum::<u64>() as f64;
        let sample = self.iter().map(|r| r.sample_size).sum::<u64>() as f64;
        if sample <= 0.0 {
            return 0.0;
        }
        encoded * 100.0 / sample
    }

    fn mean_vmaf_score(&self) -> Option<f32> {
        mean_finite_scores(self.iter().map(|r| r.vmaf_score))
    }

    fn mean_xpsnr_score(&self) -> Option<f32> {
        mean_finite_scores(self.iter().map(|r| r.xpsnr_score))
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
        let sample_secs = sample_duration.as_secs_f64();
        if sample_secs <= 0.0 {
            return 0;
        }
        let sample_factor = input_duration.as_secs_f64() / sample_secs;
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
        let sample_secs = sample_duration.as_secs_f64();
        if sample_secs <= 0.0 {
            return Duration::ZERO;
        }
        let sample_factor = input_duration.as_secs_f64() / sample_secs;
        let sample_encode_time: Duration = self.iter().map(|r| r.encode_time).sum();

        sample_encode_time.mul_f64(sample_factor)
    }
}

#[derive(Debug, Clone, Copy, clap::ValueEnum)]
pub enum StdoutFormat {
    Human,
    Json,
}

impl StdoutFormat {
    pub(crate) fn print_result(
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
        self.vmaf_score.or(self.xpsnr_score).unwrap_or(f32::NAN)
    }

    /// Extract vmaf or xpsnr kind. Use when it is expected to have only 1 of these.
    pub fn single_score_kind(&self) -> ScoreKind {
        match (self.vmaf_score, self.xpsnr_score) {
            (Some(_), _) => ScoreKind::Vmaf,
            (None, Some(_)) => ScoreKind::Xpsnr,
            (None, None) => ScoreKind::Vmaf,
        }
    }
}
