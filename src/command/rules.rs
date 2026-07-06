use crate::command::crf_search::{Crf, MinScore};
use std::path::Path;

#[derive(Debug, Clone, Copy, PartialEq, thiserror::Error)]
pub enum ValidationError {
    #[error("Only one of --min-vmaf and --min-xpsnr may be set")]
    BothMinScores,
    #[error("Invalid --min-crf & --max-crf")]
    InvalidCrfBounds,
    #[error("Minimum score must be finite")]
    InvalidMinScore,
    #[error("--max-encoded-percent must be positive")]
    NonPositiveMaxEncodedPercent,
    #[error("Invalid use of --vmaf NUMBER, did you mean: --min-vmaf {num}")]
    PositionalVmafNumber { num: f32 },
    #[error(
        "Input and Output are specified as the same file. Not proceeding. \
         Pass in `--overwrite-input` to allow this."
    )]
    SameInputOutput,
    #[error("--stereo-downmix cannot be used with --acodec copy")]
    StereoDownmixWithCopy,
    #[error("--svt may only be used with svt-av1")]
    SvtArgsOnNonSvtAv1,
    #[error("Encoder argument `{arg}` not allowed{hint}")]
    ReservedEncoderArg {
        arg: &'static str,
        hint: &'static str,
    },
    #[error("'{key}' cannot be used here")]
    ReservedSvtArg { key: &'static str },
    #[error("'svtav1-params' cannot be set here, use `--svt`")]
    Svtav1ParamsInEncoderArg,
}

pub(crate) struct CrfSearchRules {
    pub min_vmaf: Option<MinScore>,
    pub min_xpsnr: Option<MinScore>,
    pub min_crf: Option<Crf>,
    pub max_crf: Option<Crf>,
    pub positional_vmaf_number: Option<f32>,
}

impl CrfSearchRules {
    pub fn validate(&self) -> Result<(), ValidationError> {
        [
            (self.min_vmaf.is_some() && self.min_xpsnr.is_some())
                .then_some(ValidationError::BothMinScores),
            matches!(
                (self.min_crf, self.max_crf),
                (Some(min_crf), Some(max_crf)) if min_crf.get() >= max_crf.get()
            )
            .then_some(ValidationError::InvalidCrfBounds),
            (self.min_vmaf.is_none())
                .then_some(self.positional_vmaf_number)
                .flatten()
                .map(|num| ValidationError::PositionalVmafNumber { num }),
        ]
        .into_iter()
        .flatten()
        .next()
        .map_or(Ok(()), Err)
    }
}

pub(crate) struct EncodeRules<'a> {
    pub overwrite_input: bool,
    pub same_input_output: bool,
    pub downmix_to_stereo: bool,
    pub audio_codec: Option<&'a str>,
}

impl EncodeRules<'_> {
    pub fn validate(&self) -> Result<(), ValidationError> {
        [
            (!self.overwrite_input && self.same_input_output)
                .then_some(ValidationError::SameInputOutput),
            (self.downmix_to_stereo && self.audio_codec == Some("copy"))
                .then_some(ValidationError::StereoDownmixWithCopy),
        ]
        .into_iter()
        .flatten()
        .next()
        .map_or(Ok(()), Err)
    }
}

pub(crate) fn validate_encoder_passthrough<I, O>(
    svtav1: bool,
    has_svt_args: bool,
    input_args: I,
    output_args: O,
) -> Result<(), ValidationError>
where
    I: IntoIterator,
    I::Item: AsRef<str>,
    O: IntoIterator,
    O::Item: AsRef<str>,
{
    (!svtav1 && has_svt_args)
        .then_some(ValidationError::SvtArgsOnNonSvtAv1)
        .into_iter()
        .chain(input_args.into_iter().filter_map(|arg| {
            reserved_input_arg(arg.as_ref())
                .map(|(arg, hint)| ValidationError::ReservedEncoderArg { arg, hint })
        }))
        .chain(output_args.into_iter().filter_map(|arg| {
            reserved_output_arg(arg.as_ref())
                .map(|(arg, hint)| ValidationError::ReservedEncoderArg { arg, hint })
        }))
        .next()
        .map_or(Ok(()), Err)
}

pub(crate) struct SampleCountRules {
    pub samples: Option<u64>,
    pub computed_samples: u64,
    pub min_samples: Option<u64>,
}

impl SampleCountRules {
    pub fn sample_count(self) -> u64 {
        match self.samples {
            Some(samples) => samples.max(self.min_samples.unwrap_or(0)),
            None => self
                .computed_samples
                .max(self.min_samples.unwrap_or(1))
                .max(1),
        }
    }
}

pub(crate) fn choose_temp_parent<'a>(
    configured: Option<&'a Path>,
    default_parent: Option<&'a Path>,
) -> Option<&'a Path> {
    configured.or(default_parent)
}

pub(crate) fn validate_svt_arg(arg: &str) -> Result<(), ValidationError> {
    ["crf", "preset", "keyint", "scd", "input-depth"]
        .into_iter()
        .find(|deny| arg.starts_with(deny))
        .map(|key| ValidationError::ReservedSvtArg { key })
        .map_or(Ok(()), Err)
}

pub(crate) fn validate_enc_arg(arg: &str) -> Result<(), ValidationError> {
    arg.starts_with("-svtav1-params")
        .then_some(ValidationError::Svtav1ParamsInEncoderArg)
        .map_or(Ok(()), Err)
}

fn reserved_input_arg(arg: &str) -> Option<(&'static str, &'static str)> {
    match arg {
        "-i" => Some(("-i", "")),
        "-y" => Some(("-y", "")),
        "-n" => Some(("-n", "")),
        "-pix_fmt" => Some(("-pix_fmt", " use --pix-format")),
        "-crf" => Some(("-crf", "")),
        "-preset" => Some(("-preset", " use --preset")),
        "-vf" => Some(("-vf", " use --vfilter")),
        "-filter:v" => Some(("-filter:v", " use --vfilter")),
        _ => None,
    }
}

fn reserved_output_arg(arg: &str) -> Option<(&'static str, &'static str)> {
    reserved_input_arg(arg).or(match arg {
        "-c:a" => Some(("-c:a", " use --acodec")),
        "-codec:a" => Some(("-codec:a", " use --acodec")),
        "-acodec" => Some(("-acodec", " use --acodec")),
        "-c:v" => Some(("-c:v", " use --encoder")),
        "-c:v:0" => Some(("-c:v:0", " use --encoder")),
        "-codec:v" => Some(("-codec:v", " use --encoder")),
        "-codec:v:0" => Some(("-codec:v:0", " use --encoder")),
        "-vcodec" => Some(("-vcodec", " use --encoder")),
        _ => None,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::crf_search::{Crf, MinScore};

    fn crf(value: f32) -> Crf {
        match Crf::try_new(value) {
            Ok(crf) => crf,
            Err(err) => panic!("invalid test CRF: {err}"),
        }
    }

    fn min_score(value: f32) -> MinScore {
        match MinScore::new(value) {
            Ok(score) => score,
            Err(err) => panic!("invalid test min score: {err}"),
        }
    }

    fn assert_source_excludes(context: &str, source: &str, forbidden: &str) {
        assert!(
            !source.contains(forbidden),
            "{context} contains forbidden `{forbidden}`"
        );
    }

    #[test]
    fn crf_search_rules_report_first_error_without_clap_or_processes() {
        let cases = [
            (
                CrfSearchRules {
                    min_vmaf: Some(min_score(95.0)),
                    min_xpsnr: Some(min_score(55.0)),
                    min_crf: Some(crf(20.0)),
                    max_crf: Some(crf(10.0)),
                    positional_vmaf_number: Some(95.0),
                },
                ValidationError::BothMinScores,
            ),
            (
                CrfSearchRules {
                    min_vmaf: None,
                    min_xpsnr: None,
                    min_crf: Some(crf(20.0)),
                    max_crf: Some(crf(10.0)),
                    positional_vmaf_number: Some(95.0),
                },
                ValidationError::InvalidCrfBounds,
            ),
            (
                CrfSearchRules {
                    min_vmaf: None,
                    min_xpsnr: None,
                    min_crf: Some(crf(10.0)),
                    max_crf: Some(crf(20.0)),
                    positional_vmaf_number: Some(95.0),
                },
                ValidationError::PositionalVmafNumber { num: 95.0 },
            ),
        ];

        cases.into_iter().for_each(|(rules, expected)| {
            assert_eq!(rules.validate(), Err(expected));
        });
    }

    #[test]
    fn crf_search_rules_accept_valid_combinations() {
        assert_eq!(
            CrfSearchRules {
                min_vmaf: Some(min_score(95.0)),
                min_xpsnr: None,
                min_crf: Some(crf(10.0)),
                max_crf: Some(crf(20.0)),
                positional_vmaf_number: None,
            }
            .validate(),
            Ok(())
        );
    }

    #[test]
    fn encode_rules_report_first_error_without_probe_or_filesystem() {
        let cases = [
            (
                EncodeRules {
                    overwrite_input: false,
                    same_input_output: true,
                    downmix_to_stereo: true,
                    audio_codec: Some("copy"),
                },
                ValidationError::SameInputOutput,
            ),
            (
                EncodeRules {
                    overwrite_input: true,
                    same_input_output: true,
                    downmix_to_stereo: true,
                    audio_codec: Some("copy"),
                },
                ValidationError::StereoDownmixWithCopy,
            ),
        ];

        cases.into_iter().for_each(|(rules, expected)| {
            assert_eq!(rules.validate(), Err(expected));
        });
    }

    #[test]
    fn encode_rules_accept_valid_combinations() {
        assert_eq!(
            EncodeRules {
                overwrite_input: false,
                same_input_output: false,
                downmix_to_stereo: true,
                audio_codec: Some("libopus"),
            }
            .validate(),
            Ok(())
        );
    }

    #[test]
    fn encoder_passthrough_rules_report_first_reserved_arg() {
        let cases = [
            (
                false,
                true,
                [].as_slice(),
                [].as_slice(),
                ValidationError::SvtArgsOnNonSvtAv1,
            ),
            (
                true,
                false,
                ["-vf"].as_slice(),
                ["-vcodec"].as_slice(),
                ValidationError::ReservedEncoderArg {
                    arg: "-vf",
                    hint: " use --vfilter",
                },
            ),
            (
                true,
                false,
                [].as_slice(),
                ["-vcodec"].as_slice(),
                ValidationError::ReservedEncoderArg {
                    arg: "-vcodec",
                    hint: " use --encoder",
                },
            ),
        ];

        cases.into_iter().for_each(
            |(svtav1, has_svt_args, input_args, output_args, expected)| {
                assert_eq!(
                    validate_encoder_passthrough(svtav1, has_svt_args, input_args, output_args),
                    Err(expected)
                );
            },
        );
    }

    #[test]
    fn encoder_passthrough_rules_accept_valid_args() {
        assert_eq!(
            validate_encoder_passthrough(
                true,
                true,
                ["-hwaccel", "none"].as_slice(),
                ["-b:v", "0"].as_slice()
            ),
            Ok(())
        );
    }

    #[test]
    fn rule_validation_success_paths_do_not_allocate() {
        let input_args = ["-hwaccel", "none"];
        let output_args = ["-b:v", "0"];

        crate::test_support::assert_no_allocations(|| {
            assert_eq!(
                CrfSearchRules {
                    min_vmaf: Some(min_score(95.0)),
                    min_xpsnr: None,
                    min_crf: Some(crf(10.0)),
                    max_crf: Some(crf(20.0)),
                    positional_vmaf_number: None,
                }
                .validate(),
                Ok(())
            );
            assert_eq!(
                EncodeRules {
                    overwrite_input: false,
                    same_input_output: false,
                    downmix_to_stereo: true,
                    audio_codec: Some("libopus"),
                }
                .validate(),
                Ok(())
            );
            assert_eq!(
                validate_encoder_passthrough(true, true, input_args, output_args),
                Ok(())
            );
        });
    }

    #[test]
    fn rule_validation_failure_paths_do_not_allocate() {
        crate::test_support::assert_no_allocations(|| {
            assert_eq!(
                CrfSearchRules {
                    min_vmaf: Some(min_score(95.0)),
                    min_xpsnr: Some(min_score(55.0)),
                    min_crf: Some(crf(10.0)),
                    max_crf: Some(crf(20.0)),
                    positional_vmaf_number: Some(95.0),
                }
                .validate(),
                Err(ValidationError::BothMinScores)
            );
            assert_eq!(
                EncodeRules {
                    overwrite_input: false,
                    same_input_output: true,
                    downmix_to_stereo: false,
                    audio_codec: None,
                }
                .validate(),
                Err(ValidationError::SameInputOutput)
            );
            assert_eq!(
                validate_encoder_passthrough(false, true, ["-hwaccel"], ["-b:v"]),
                Err(ValidationError::SvtArgsOnNonSvtAv1)
            );
            assert_eq!(
                validate_svt_arg("preset=8"),
                Err(ValidationError::ReservedSvtArg { key: "preset" })
            );
            assert_eq!(
                validate_enc_arg("-svtav1-params=scd=1"),
                Err(ValidationError::Svtav1ParamsInEncoderArg)
            );
            assert_eq!(
                SampleCountRules {
                    samples: None,
                    computed_samples: 0,
                    min_samples: Some(3),
                }
                .sample_count(),
                3
            );
            assert_eq!(choose_temp_parent(None, None), None);
        });
    }

    #[test]
    fn command_config_validation_hotspots_do_not_bypass_rules() {
        [
            ("command args", include_str!("args.rs")),
            ("encode args", include_str!("args/encode.rs")),
            ("crf search command", include_str!("crf_search.rs")),
            ("encode preflight", include_str!("encode/preflight.rs")),
        ]
        .into_iter()
        .flat_map(|(name, source)| ["ensure!(", "bail!("].map(move |needle| (name, source, needle)))
        .for_each(|(name, source, needle)| {
            assert_source_excludes(name, source, needle);
        });
    }

    #[test]
    fn rule_engine_does_not_depend_on_cli_arg_types_or_boundary_crates() {
        let production_source = include_str!("rules.rs")
            .split("#[cfg(test)]")
            .next()
            .map_or("", |source| source);

        [
            "VmafArg",
            "command::args",
            "clap",
            "tokio",
            "ffmpeg",
            "ffprobe",
        ]
        .into_iter()
        .for_each(|forbidden| {
            assert_source_excludes(
                "command::rules production code",
                production_source,
                forbidden,
            );
        });
    }

    #[test]
    fn sample_count_rule_preserves_explicit_zero_and_minimum_defaults() {
        let cases = [
            (
                SampleCountRules {
                    samples: Some(0),
                    computed_samples: 60,
                    min_samples: Some(5),
                },
                5,
            ),
            (
                SampleCountRules {
                    samples: None,
                    computed_samples: 0,
                    min_samples: None,
                },
                1,
            ),
            (
                SampleCountRules {
                    samples: None,
                    computed_samples: 2,
                    min_samples: Some(5),
                },
                5,
            ),
        ];

        cases.into_iter().for_each(|(rules, expected)| {
            assert_eq!(rules.sample_count(), expected);
        });
    }

    #[test]
    fn temp_parent_rule_prefers_explicit_then_input_parent() {
        let explicit = std::path::Path::new("/explicit");
        let input_parent = std::path::Path::new("/input");

        assert_eq!(
            choose_temp_parent(Some(explicit), Some(input_parent)),
            Some(explicit)
        );
        assert_eq!(
            choose_temp_parent(None, Some(input_parent)),
            Some(input_parent)
        );
        assert_eq!(choose_temp_parent(None, None), None);
    }

    #[test]
    fn encoder_parser_rules_reject_reserved_svt_and_enc_args() {
        let cases = [
            (
                validate_svt_arg("crf=32"),
                ValidationError::ReservedSvtArg { key: "crf" },
            ),
            (
                validate_svt_arg("input-depth=10"),
                ValidationError::ReservedSvtArg { key: "input-depth" },
            ),
            (
                validate_enc_arg("-svtav1-params=scd=1"),
                ValidationError::Svtav1ParamsInEncoderArg,
            ),
        ];

        cases.into_iter().for_each(|(actual, expected)| {
            assert_eq!(actual, Err(expected));
        });
        assert_eq!(validate_svt_arg("film-grain=8"), Ok(()));
        assert_eq!(validate_enc_arg("-x265-params=lossless=1"), Ok(()));
    }
}
