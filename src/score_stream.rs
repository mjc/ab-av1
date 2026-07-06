use crate::process::{
    Chunks, CommandExt, FfmpegOut, cmd_err, exit_ok_stderr,
    managed::{ManagedEvent, ManagedProcess},
};
use std::path::Path;
use tokio::process::Command;
use tokio_stream::{Stream, StreamExt};

#[derive(Debug, PartialEq)]
pub enum ScoreStreamParse {
    Progress(FfmpegOut),
    LogicalDone(Score),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Score(f32);

impl Score {
    pub fn new(score: f32) -> Self {
        Self(score)
    }

    pub fn get(self) -> f32 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum LogicalScoreCompletion {
    Pending,
    Done(Score),
}

impl LogicalScoreCompletion {
    fn record(&mut self, event: &ScoreStreamParse) {
        if let ScoreStreamParse::LogicalDone(score) = event
            && !self.is_done()
        {
            *self = Self::Done(*score);
        }
    }

    fn is_done(self) -> bool {
        matches!(self, Self::Done(_))
    }
}

/// Build ffmpeg command for VMAF/XPSNR scoring (testable without spawning).
pub(crate) fn build_score_ffmpeg_command(
    reference: &Path,
    distorted: &Path,
    filter_complex: &str,
    fps: Option<f32>,
) -> Command {
    let mut cmd = Command::new("ffmpeg");
    cmd.arg("-nostdin")
        .arg2_opt("-r", fps)
        .arg2("-i", distorted)
        .arg2_opt("-r", fps)
        .arg2("-i", reference)
        .arg2("-filter_complex", filter_complex)
        // Workaround unused streams causing ffmpeg memory leaks
        // See https://github.com/alexheretic/ab-av1/issues/189
        .suppress_non_video_streams()
        .arg2("-f", "null")
        .arg("-");
    cmd
}

pub fn run_score_stream<Out>(
    process: ManagedProcess,
    name: &'static str,
    cmd_str: String,
    parse_chunk: fn(&[u8], &mut Chunks) -> Option<ScoreStreamParse>,
    into_out: fn(ScoreStreamParse) -> Out,
    into_err: fn(anyhow::Error) -> Out,
) -> impl Stream<Item = Out> {
    let events = process.terminate_on_drop().stderr_events();

    // Score streams intentionally separate logical completion from process
    // completion: callers may see `LogicalDone` and drop the stream, which
    // terminates/reaps ffmpeg through the terminate-on-drop policy. If ffmpeg
    // exits first or never prints a score, the final bounded stderr buffer is
    // used to report child failure or parse failure with command context.
    async_stream::stream! {
        let mut chunks = Chunks::default();
        let mut logical_score = LogicalScoreCompletion::Pending;
        tokio::pin!(events);
        while let Some(next) = events.next().await {
            match next {
                Ok(ManagedEvent::RawStderr(chunk)) => {
                    if let Some(event) = parse_chunk(chunk.as_bytes(), &mut chunks) {
                        logical_score.record(&event);
                        yield into_out(event);
                    }
                }
                Ok(ManagedEvent::ReplayGap(_)) => {}
                Ok(ManagedEvent::ProcessDone(done)) => {
                    let status = done.status();
                    if let Err(err) = exit_ok_stderr(name, Ok(status), &cmd_str, &chunks) {
                        yield into_err(err);
                    }
                }
                Err(err) => yield into_err(err),
            }
        }
        if !logical_score.is_done() {
            yield into_err(cmd_err(
                format!("could not parse {name} score"),
                &cmd_str,
                &chunks,
            ));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::process::CommandExt;
    use std::path::Path;

    #[test]
    fn build_score_ffmpeg_command_distorted_input_is_stream_zero() {
        let cmd = build_score_ffmpeg_command(
            Path::new("ref.mkv"),
            Path::new("dist.mkv"),
            "[0:v][1:v]score",
            Some(25.0),
        );
        let cmd_str = cmd.to_cmd_str();
        let dist_pos = cmd_str.find("dist.mkv").expect("distorted path");
        let ref_pos = cmd_str.find("ref.mkv").expect("reference path");
        assert!(
            dist_pos < ref_pos,
            "distorted input must precede reference: `{cmd_str}`"
        );
        assert!(cmd_str.contains("-r 25"));
        for flag in ["-an", "-sn", "-dn"] {
            assert!(
                cmd_str.split_whitespace().any(|arg| arg == flag),
                "expected {flag} in `{cmd_str}`"
            );
        }
    }

    #[test]
    fn logical_score_completion_tracks_score_as_a_state() {
        let mut completion = LogicalScoreCompletion::Pending;
        assert!(!completion.is_done());

        completion.record(&ScoreStreamParse::Progress(FfmpegOut::StreamSizes {
            video: 1,
            audio: 0,
            subtitle: 0,
            other: 0,
        }));
        assert!(!completion.is_done());

        completion.record(&ScoreStreamParse::LogicalDone(Score::new(97.5)));
        assert_eq!(completion, LogicalScoreCompletion::Done(Score::new(97.5)));
        assert!(completion.is_done());
    }

    // ab-kgc.44: first parsed score must win when ffmpeg prints duplicate score lines
    #[test]
    fn logical_score_completion_keeps_first_score_when_duplicated() {
        // setup
        let mut completion = LogicalScoreCompletion::Pending;

        // execute
        completion.record(&ScoreStreamParse::LogicalDone(Score::new(97.5)));
        completion.record(&ScoreStreamParse::LogicalDone(Score::new(88.0)));

        // assert
        assert_eq!(
            completion,
            LogicalScoreCompletion::Done(Score::new(97.5)),
            "duplicate score lines must not overwrite the first logical score"
        );
    }
}
