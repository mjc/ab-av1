use crate::{
    command::{
        PROGRESS_CHARS, args, crf_search, encode,
        sample_encode::{self, Work},
    },
    console_ext::style,
    ffprobe,
    float::TerseF32,
    temporary,
};
use anyhow::Context;
use clap::Parser;
use console::style;
use futures_util::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use std::{pin::pin, sync::Arc, time::Duration};

const BAR_LEN: u64 = 1024 * 1024 * 1024;

/// Automatically determine the best crf to deliver the min-vmaf and use it to encode a video or image.
///
/// Two phases:
/// * crf-search to determine the best --crf value
/// * ffmpeg & SvtAv1EncApp to encode using the settings
///
/// Use -v to print per-crf results.
/// Use -vv to print per-sample results.
#[derive(Parser)]
#[clap(verbatim_doc_comment)]
#[group(skip)]
pub struct Args {
    #[clap(flatten)]
    pub search: crf_search::Args,

    #[clap(flatten)]
    pub encode: args::EncodeToOutput,
}

pub async fn auto_encode(Args { mut search, encode }: Args) -> anyhow::Result<()> {
    const SPINNER_RUNNING: &str = "{spinner:.cyan.bold} {elapsed_precise:.bold} {prefix} {wide_bar:.cyan/blue} ({msg}eta {eta})";
    const SPINNER_FINISHED: &str =
        "{spinner:.cyan.bold} {elapsed_precise:.bold} {prefix} {wide_bar:.cyan/blue} ({msg})";

    let defaulting_output = encode.output.is_none();
    let input_probe = Arc::new(ffprobe::probe(&search.args.input));

    let resolved = encode::resolve_output(
        &search.args.input,
        &search.args.encoder,
        &encode,
        &input_probe,
    )
    .map_err(encode::EncodePlanError::into_anyhow)?;
    encode::audio_config(&encode, &input_probe).map_err(encode::EncodePlanError::into_anyhow)?;
    let output = resolved.planned.path().to_path_buf();

    search.sample.set_extension_from_output(&output);
    search.validate()?;
    let search = crf_search::CrfSearchConfig::from(search);
    search.validate()?;

    let bar = ProgressBar::new(BAR_LEN).with_style(
        ProgressStyle::default_bar()
            .template(SPINNER_RUNNING)?
            .progress_chars(PROGRESS_CHARS),
    );
    bar.enable_steady_tick(Duration::from_millis(100));

    if defaulting_output {
        let out = shell_escape::escape(output.display().to_string().into());
        bar.println(style!("Encoding {out}").dim().to_string());
    }

    let min_score = search.min_score();
    let max_encoded_percent = search.max_encoded_percent;
    let enc_args = search.args.clone();
    let thorough = search.thorough;
    let verbose = search.verbose;
    let keep_temp_files = search.sample.keep;

    let mut crf_search = pin!(crf_search::run(search, input_probe.clone()));
    let mut best = None;
    while let Some(update) = crf_search.next().await {
        match update {
            Err(err) => {
                if let crf_search::Error::NoGoodCrf { last } = &err {
                    // show last sample attempt in progress bar
                    bar.set_style(
                        ProgressStyle::default_bar()
                            .template(SPINNER_FINISHED)?
                            .progress_chars(PROGRESS_CHARS),
                    );
                    let mut vmaf = style(last.enc.single_score());
                    if last.enc.single_score() < min_score {
                        vmaf = vmaf.red();
                    }
                    let mut percent = style!("{:.0}%", last.enc.encode_percent);
                    if last.enc.encode_percent > max_encoded_percent.get() {
                        percent = percent.red();
                    }
                    let score_kind = last.enc.single_score_kind();
                    bar.finish_with_message(format!("{score_kind} {vmaf:.2}, size {percent}"));
                }
                bar.finish();
                return Err(err.into());
            }
            Ok(crf_search::Update::Status {
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
            }) => {
                bar.set_position(crf_search::guess_progress(crf_run, progress, thorough) as _);
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
            Ok(crf_search::Update::SampleResult {
                crf,
                sample,
                result,
            }) => {
                if verbose
                    .log_level()
                    .is_some_and(|lvl| lvl > log::Level::Warn)
                {
                    result.print_attempt(&bar, sample, Some(crf))
                }
            }
            Ok(crf_search::Update::RunResult(result)) => {
                if verbose
                    .log_level()
                    .is_some_and(|lvl| lvl > log::Level::Error)
                {
                    result.print_attempt(&bar, min_score, max_encoded_percent)
                }
            }
            Ok(crf_search::Update::Done(result)) => best = Some(result),
        }
    }
    let best = best.context("no crf-search best?")?;

    bar.set_style(
        ProgressStyle::default_bar()
            .template(SPINNER_FINISHED)?
            .progress_chars(PROGRESS_CHARS),
    );
    bar.finish_with_message(format!(
        "{} {:.2}, size {}",
        best.enc.single_score_kind(),
        style(best.enc.single_score()).green(),
        style(format!("{:.0}%", best.enc.encode_percent)).green(),
    ));
    temporary::clean(keep_temp_files).await;

    let bar = ProgressBar::new(12).with_style(
        ProgressStyle::default_bar()
            .template(SPINNER_RUNNING)?
            .progress_chars(PROGRESS_CHARS),
    );
    bar.set_prefix("Encoding");
    bar.enable_steady_tick(Duration::from_millis(100));

    encode::run(
        encode::Args {
            args: enc_args,
            crf: crf_search::Crf::try_new(best.crf).context("crf-search returned invalid CRF")?,
            encode: args::EncodeToOutput {
                output: Some(output),
                ..encode
            },
        },
        input_probe,
        &bar,
    )
    .await
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        command::{
            args::{self, Encode, Sample as SampleArgs, Vmaf},
            crf_search::{self, test_hooks as crf_test_hooks},
            encode::test_hooks as encode_test_hooks,
            sample_encode,
        },
        temporary::{self, TempKind},
    };
    use std::{env, fs, path::PathBuf, time::Duration};
    use tokio::sync::Mutex;

    static AUTO_ENCODE_TEST_LOCK: Mutex<()> = Mutex::const_new(());

    mod helpers {
        use super::*;

        pub fn unique_suffix() -> u128 {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        }

        pub fn temp_input(label: &str) -> PathBuf {
            let path = env::temp_dir().join(format!(
                "ab-av1-auto-encode-{}-{}-{}",
                label,
                std::process::id(),
                unique_suffix()
            ));
            fs::write(&path, b"input").expect("write temp input");
            path
        }

        pub fn auto_args(input: PathBuf, output: Option<PathBuf>, keep: bool) -> Args {
            Args {
                search: crf_search::Args {
                    args: Encode {
                        encoder: "libsvtav1".parse().unwrap(),
                        input,
                        vfilter: None,
                        pix_format: None,
                        preset: None,
                        keyint: None,
                        scd: None,
                        svt_args: vec![],
                        enc_args: vec![],
                        enc_input_args: vec![],
                    },
                    min_vmaf: crate::command::crf_search::MinScore::new(95.0).ok(),
                    min_xpsnr: None,
                    max_encoded_percent: crate::command::crf_search::MaxEncodedPercent::new(80.0)
                        .unwrap(),
                    min_crf: crate::command::crf_search::Crf::try_new(20.0).ok(),
                    max_crf: crate::command::crf_search::Crf::try_new(40.0).ok(),
                    crf_increment: Some(crate::command::crf_search::CrfStep::try_new(1.0).unwrap()),
                    high_crf_means_hq: Some(false),
                    thorough: true,
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
                        keep,
                        temp_dir: None,
                        extension: None,
                    },
                    vmaf: Vmaf::default(),
                    score: args::ScoreArgs {
                        reference_vfilter: None,
                    },
                    xpsnr: args::Xpsnr::default(),
                    verbose: clap_verbosity_flag::Verbosity::new(0, 0),
                },
                encode: args::EncodeToOutput {
                    output,
                    audio_codec: None,
                    downmix_to_stereo: false,
                    video_only: false,
                    overwrite_input: false,
                },
            }
        }

        pub fn mock_output(vmaf: f32) -> sample_encode::Output {
            sample_encode::Output {
                vmaf_score: Some(vmaf),
                xpsnr_score: None,
                predicted_encode_size: 1_000_000,
                encode_percent: 50.0,
                predicted_encode_time: Duration::from_secs(60),
                from_cache: false,
            }
        }
    }

    use helpers::*;

    struct MockGuard {
        crf: bool,
        encode: bool,
    }

    impl MockGuard {
        fn crf(mock: impl Fn(f32) -> sample_encode::Output + 'static) -> Self {
            crf_test_hooks::set(mock);
            Self {
                crf: true,
                encode: false,
            }
        }

        fn both(
            crf_mock: impl Fn(f32) -> sample_encode::Output + 'static,
            encode_fixture: &'static str,
        ) -> Self {
            crf_test_hooks::set(crf_mock);
            encode_test_hooks::set_fixture(encode_fixture);
            Self {
                crf: true,
                encode: true,
            }
        }
    }

    impl Drop for MockGuard {
        fn drop(&mut self) {
            if self.crf {
                crf_test_hooks::clear();
            }
            if self.encode {
                encode_test_hooks::clear();
            }
        }
    }

    // ab-kgc.90: auto-encode must reject --stereo-downmix with --acodec copy before search
    #[serial_test::serial]
    #[tokio::test]
    async fn rejects_stereo_downmix_with_copy_codec_before_search() {
        let _lock = AUTO_ENCODE_TEST_LOCK.lock().await;
        // setup
        let input = temp_input("downmix-copy");
        let output = env::temp_dir().join(format!(
            "ab-av1-auto-downmix-out-{}-{}.mkv",
            std::process::id(),
            unique_suffix()
        ));
        let mut args = auto_args(input.clone(), Some(output), false);
        args.encode.downmix_to_stereo = true;
        args.encode.audio_codec = Some("copy".into());
        let _guard = MockGuard::crf(|_crf| mock_output(96.0));

        // execute
        let err = auto_encode(args)
            .await
            .expect_err("expected downmix/copy rejection before crf search");

        // assert
        assert!(
            err.to_string().contains("--stereo-downmix"),
            "auto-encode must reject --stereo-downmix with --acodec copy before search"
        );

        // cleanup
        let _ = fs::remove_file(input);
    }

    #[serial_test::serial]
    #[tokio::test]
    async fn rejects_same_input_and_output_without_overwrite() {
        let _lock = AUTO_ENCODE_TEST_LOCK.lock().await;
        // setup
        let input = temp_input("same-io");
        let args = auto_args(input.clone(), Some(input.clone()), false);

        // execute
        let err = auto_encode(args)
            .await
            .expect_err("expected same-file error");

        // assert
        assert!(err.to_string().contains("same file"));

        // cleanup
        let _ = fs::remove_file(input);
    }

    #[serial_test::serial]
    #[tokio::test]
    async fn propagates_no_good_crf_from_search() {
        let _lock = AUTO_ENCODE_TEST_LOCK.lock().await;
        // setup
        let input = temp_input("no-good-crf");
        let output = env::temp_dir().join(format!(
            "ab-av1-auto-out-{}-{}.mkv",
            std::process::id(),
            unique_suffix()
        ));
        let args = auto_args(input.clone(), Some(output), false);
        let _guard = MockGuard::crf(|_crf| mock_output(80.0));

        // execute
        let err = auto_encode(args).await.expect_err("expected NoGoodCrf");

        // assert
        assert!(err.to_string().contains("Failed to find a suitable crf"));

        // cleanup
        let _ = fs::remove_file(input);
    }

    #[serial_test::serial]
    #[tokio::test]
    async fn successful_run_preserves_keepable_temps_with_keep_ab_kgc_15() {
        let _lock = AUTO_ENCODE_TEST_LOCK.lock().await;
        // setup
        let input = temp_input("keep");
        let output = env::temp_dir().join(format!(
            "ab-av1-auto-keep-out-{}-{}.mkv",
            std::process::id(),
            unique_suffix()
        ));
        let keepable = env::temp_dir().join(format!(
            "ab-av1-auto-keepable-{}-{}",
            std::process::id(),
            unique_suffix()
        ));
        fs::write(&keepable, b"keep-me").expect("write keepable");
        temporary::add(&keepable, TempKind::Keepable);
        let args = auto_args(input.clone(), Some(output.clone()), true);
        let _guard = MockGuard::both(|_crf| mock_output(96.0), "stderr-ffmpeg-progress");

        // execute
        auto_encode(args).await.expect("auto encode");

        // assert
        assert!(
            keepable.exists(),
            "auto-encode --keep must preserve Keepable temp files after search"
        );
        assert!(output.exists(), "encode output should exist");

        // cleanup
        let _ = fs::remove_file(input);
        let _ = fs::remove_file(output);
        let _ = fs::remove_file(keepable);
    }

    #[serial_test::serial]
    #[tokio::test]
    async fn successful_run_cleans_keepable_temps_without_keep() {
        let _lock = AUTO_ENCODE_TEST_LOCK.lock().await;
        // setup
        let input = temp_input("no-keep");
        let output = env::temp_dir().join(format!(
            "ab-av1-auto-no-keep-out-{}-{}.mkv",
            std::process::id(),
            unique_suffix()
        ));
        let keepable = env::temp_dir().join(format!(
            "ab-av1-auto-drop-{}-{}",
            std::process::id(),
            unique_suffix()
        ));
        fs::write(&keepable, b"drop-me").expect("write keepable");
        temporary::add(&keepable, TempKind::Keepable);
        let args = auto_args(input.clone(), Some(output.clone()), false);
        let _guard = MockGuard::both(|_crf| mock_output(96.0), "stderr-ffmpeg-progress");

        // execute
        auto_encode(args).await.expect("auto encode");

        // assert
        assert!(
            !keepable.exists(),
            "without --keep, search temp files should be cleaned before encode"
        );

        // cleanup
        let _ = fs::remove_file(input);
        let _ = fs::remove_file(output);
    }
}
