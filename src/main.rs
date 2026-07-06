mod command;
mod console_ext;
mod ffmpeg;
mod ffprobe;
mod float;
mod log;
mod process;
mod sample;
mod score_stream;
mod temporary;
#[cfg(test)]
mod test_support;
mod vmaf;
mod xpsnr;

use ::log::LevelFilter;
use anyhow::anyhow;
use clap::Parser;
use futures_util::FutureExt;
use std::io::IsTerminal;
use tokio::signal;

#[derive(Parser)]
#[command(version, about)]
enum Command {
    SampleEncode(command::sample_encode::Args),
    Vmaf(command::vmaf::Args),
    Xpsnr(command::xpsnr::Args),
    Encode(command::encode::Args),
    CrfSearch(command::crf_search::Args),
    AutoEncode(command::auto_encode::Args),
    PrintCompletions(command::print_completions::Args),
}

#[tokio::main(flavor = "current_thread")]
async fn main() {
    env_logger::builder()
        .filter_module(
            "ab_av1",
            match std::io::stderr().is_terminal() {
                true => LevelFilter::Off,
                false => LevelFilter::Info,
            },
        )
        .parse_default_env()
        .init();

    let action = Command::parse();
    let keep = action.keep_temp_files();

    let local = tokio::task::LocalSet::new();
    let command = local.run_until(match action {
        Command::SampleEncode(args) => command::sample_encode(args).boxed_local(),
        Command::Vmaf(args) => command::vmaf(args.into()).boxed_local(),
        Command::Xpsnr(args) => command::xpsnr(args.into()).boxed_local(),
        Command::Encode(args) => command::encode(args).boxed_local(),
        Command::CrfSearch(args) => command::crf_search(args).boxed_local(),
        Command::AutoEncode(args) => command::auto_encode(args).boxed_local(),
        Command::PrintCompletions(args) => return command::print_completions(args),
    });

    let out = tokio::select! {
        r = command => r,
        _ = signal::ctrl_c() => Err(anyhow!("ctrl_c")),
    };
    drop(local);

    // Final cleanup. Samples are already deleted (if wished by the user) during `command::sample_encode::run`.
    temporary::clean(keep).await;

    if let Err(err) = out {
        eprintln!("Error: {err}");
        std::process::exit(1);
    }
}

impl Command {
    /// This decides what commands will keep temp files.
    ///
    /// # Important
    ///
    /// Add commands using the sample sub-args here referencing the `keep` flag,
    /// or the temp files will be removed anyways.
    fn keep_temp_files(&self) -> bool {
        match self {
            Self::SampleEncode(args) => args.sample.keep,
            Self::CrfSearch(args) => args.sample.keep,
            Self::AutoEncode(args) => args.search.sample.keep,
            _ => false,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn command_parse_routes_sample_encode_subcommand() {
        let command = Command::try_parse_from([
            "ab-av1",
            "sample-encode",
            "--input",
            "input.mkv",
            "--crf",
            "30",
        ])
        .expect("parse sample-encode command");

        match command {
            Command::SampleEncode(args) => {
                assert!(!args.sample.keep);
                assert_eq!(args.args.input, std::path::Path::new("input.mkv"));
            }
            _ => panic!("expected sample-encode command"),
        }
    }

    #[test]
    fn command_parse_routes_print_completions_subcommand() {
        let command = Command::try_parse_from(["ab-av1", "print-completions", "bash"])
            .expect("parse print-completions command");

        match command {
            Command::PrintCompletions(_) => {}
            _ => panic!("expected print-completions command"),
        }
    }
}
