use crate::process::{
    ChunkLineError, Chunks, CommandExt, FfmpegOut, cmd_err, exit_ok_stderr,
    managed::{ManagedEvent, ManagedProcess},
};
use std::path::Path;
use thiserror::Error;
use tokio::process::Command;
use tokio_stream::{Stream, StreamExt};

#[derive(Debug, PartialEq)]
pub enum ScoreStreamParse {
    Progress(FfmpegOut),
    LogicalDone(Score),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Score(f32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Error)]
pub enum ScoreError {
    #[error("score must not be NaN")]
    Nan,
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub enum ParsedScore {
    Miss,
    Score(Score),
    Invalid(ScoreError),
}

impl ParsedScore {
    pub fn from_score(score: Result<Score, ScoreError>) -> Self {
        match score {
            Ok(score) => Self::Score(score),
            Err(err) => Self::Invalid(err),
        }
    }

    pub fn hit(self) -> Option<Self> {
        match self {
            Self::Miss => None,
            hit => Some(hit),
        }
    }
}

impl Score {
    pub fn try_new(score: f32) -> Result<Self, ScoreError> {
        if score.is_nan() {
            Err(ScoreError::Nan)
        } else {
            Ok(Self(score))
        }
    }

    pub fn new(score: f32) -> Self {
        Self::try_new(score).expect("score must not be NaN")
    }

    pub fn get(self) -> f32 {
        self.0
    }
}

pub(crate) fn parse_score_chunk(
    chunk: &[u8],
    chunks: &mut Chunks,
    parse_score_line: impl Fn(&str) -> ParsedScore,
) -> Result<Option<ScoreStreamParse>, ChunkLineError> {
    chunks.push(chunk);

    if let Some(score) = parse_buffered_score(chunks, parse_score_line)? {
        return Ok(Some(ScoreStreamParse::LogicalDone(score)));
    }
    Ok(FfmpegOut::try_parse(chunks.last_line()).map(ScoreStreamParse::Progress))
}

pub(crate) fn parse_buffered_score(
    chunks: &Chunks,
    parse_score_line: impl Fn(&str) -> ParsedScore,
) -> Result<Option<Score>, ChunkLineError> {
    Ok(
        match chunks.rfind_line_map_checked(|line| parse_score_line(line).hit())? {
            Some(ParsedScore::Score(score)) => Some(score),
            Some(ParsedScore::Invalid(_) | ParsedScore::Miss) | None => None,
        },
    )
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
    let spec = ScoreFfmpegCommand::new(reference, distorted, filter_complex, fps);
    let mut cmd = Command::new("ffmpeg");
    spec.apply(&mut cmd);
    cmd
}

#[derive(Debug, Clone, Copy)]
pub(crate) struct ScoreFfmpegCommand<'a> {
    reference: &'a Path,
    distorted: &'a Path,
    filter_complex: &'a str,
    fps: Option<f32>,
}

impl<'a> ScoreFfmpegCommand<'a> {
    pub(crate) fn new(
        reference: &'a Path,
        distorted: &'a Path,
        filter_complex: &'a str,
        fps: Option<f32>,
    ) -> Self {
        Self {
            reference,
            distorted,
            filter_complex,
            fps,
        }
    }

    pub(crate) fn apply(self, cmd: &mut Command) {
        cmd.arg("-nostdin")
            .arg2_opt("-r", self.fps)
            .arg2("-i", self.distorted)
            .arg2_opt("-r", self.fps)
            .arg2("-i", self.reference)
            .arg2("-filter_complex", self.filter_complex)
            // Workaround unused streams causing ffmpeg memory leaks
            // See https://github.com/alexheretic/ab-av1/issues/189
            .suppress_non_video_streams()
            .arg2("-f", "null")
            .arg("-");
    }
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
    fn score_ffmpeg_command_spec_applies_borrowed_args() {
        let spec = ScoreFfmpegCommand::new(
            Path::new("ref.mkv"),
            Path::new("dist.mkv"),
            "[0:v][1:v]score",
            Some(25.0),
        );
        let mut cmd = Command::new("ffmpeg");

        spec.apply(&mut cmd);

        let cmd_str = cmd.to_cmd_str();
        assert!(cmd_str.contains("ref.mkv"));
        assert!(cmd_str.contains("dist.mkv"));
        assert!(cmd_str.contains("[0:v][1:v]score"));
    }

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

    #[test]
    fn parse_score_chunk_handles_split_score_lines() {
        let mut chunks = Chunks::default();
        let parse_score_line = |line: &str| {
            line.strip_prefix("metric score: ")
                .and_then(|score| score.parse().ok())
                .map(Score::new)
                .map(ParsedScore::Score)
                .unwrap_or(ParsedScore::Miss)
        };

        assert_eq!(
            parse_score_chunk(b"metric ", &mut chunks, parse_score_line),
            Ok(None)
        );
        assert_eq!(
            parse_score_chunk(b"score: 91.25\n", &mut chunks, parse_score_line),
            Ok(Some(ScoreStreamParse::LogicalDone(Score::new(91.25))))
        );
    }

    #[test]
    fn score_rejects_nan() {
        assert_eq!(Score::try_new(f32::NAN), Err(ScoreError::Nan));
    }

    #[test]
    fn score_accepts_infinity_for_xpsnr() {
        assert_eq!(
            Score::try_new(f32::INFINITY).map(Score::get),
            Ok(f32::INFINITY)
        );
    }
}
