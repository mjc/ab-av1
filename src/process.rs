pub mod managed;

use crate::process::managed::{ManagedEvent, ManagedProcess};
use anyhow::{anyhow, ensure};
use std::{
    borrow::Cow,
    ffi::OsStr,
    fmt::Display,
    io,
    pin::Pin,
    process::{ExitStatus, Output},
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};
use tokio_stream::{Stream, StreamExt};

pub fn ensure_success(name: &'static str, out: &Output) -> anyhow::Result<()> {
    ensure!(
        out.status.success(),
        "{name} exit code {}\n---stderr---\n{}\n------------",
        out.status
            .code()
            .map(|c| c.to_string())
            .unwrap_or_else(|| "None".into()),
        String::from_utf8_lossy(&out.stderr).trim(),
    );
    Ok(())
}

/// Convert exit code result into simple result.
pub fn exit_ok(name: &'static str, done: io::Result<ExitStatus>) -> anyhow::Result<()> {
    let code = done?;
    ensure!(
        code.success(),
        "{name} exit code {}",
        code.code()
            .map(|c| c.to_string())
            .unwrap_or_else(|| "None".into())
    );
    Ok(())
}

/// Convert exit code result into simple result adding stderr to error messages.
pub fn exit_ok_stderr(
    name: &'static str,
    done: io::Result<ExitStatus>,
    cmd_str: &str,
    stderr: &Chunks,
) -> anyhow::Result<()> {
    exit_ok(name, done).map_err(|e| cmd_err(e, cmd_str, stderr))
}

pub fn cmd_err(err: impl Display, cmd_str: &str, stderr: &Chunks) -> anyhow::Error {
    anyhow!(
        "{err}\n----cmd-----\n{cmd_str}\n---stderr---\n{}\n------------",
        String::from_utf8_lossy(&stderr.out).trim()
    )
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FfmpegProgress {
    frame: u64,
    fps: f32,
    time: Duration,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FfmpegStreamSizes {
    video: u64,
    audio: u64,
    subtitle: u64,
    other: u64,
}

impl From<FfmpegProgress> for FfmpegOut {
    fn from(progress: FfmpegProgress) -> Self {
        Self::Progress {
            frame: progress.frame,
            fps: progress.fps,
            time: progress.time,
        }
    }
}

impl From<FfmpegStreamSizes> for FfmpegOut {
    fn from(sizes: FfmpegStreamSizes) -> Self {
        Self::StreamSizes {
            video: sizes.video,
            audio: sizes.audio,
            subtitle: sizes.subtitle,
            other: sizes.other,
        }
    }
}

#[derive(Debug, PartialEq)]
pub enum FfmpegOut {
    Progress {
        frame: u64,
        fps: f32,
        time: Duration,
    },
    StreamSizes {
        video: u64,
        audio: u64,
        subtitle: u64,
        other: u64,
    },
}

impl FfmpegOut {
    pub fn try_parse(line: &str) -> Option<Self> {
        if let Some(progress) = parse_ffmpeg_progress(line) {
            return Some(progress.into());
        }
        if let Some(sizes) = parse_ffmpeg_stream_sizes(line) {
            return Some(sizes.into());
        }
        None
    }

    pub fn stream(process: ManagedProcess, name: &'static str, cmd_str: String) -> FfmpegOutStream {
        FfmpegOutStream {
            events: Box::pin(process.must_complete().stderr_events()),
            chunks: <_>::default(),
            name,
            cmd_str,
            completion: FfmpegProcessCompletion::Pending,
        }
    }
}

pub(crate) fn parse_ffmpeg_progress(line: &str) -> Option<FfmpegProgress> {
    if !line.starts_with("frame=") {
        return None;
    }

    let frame = parse_label_substr("frame=", line)?.parse().ok()?;
    let fps = parse_label_substr("fps=", line)?.parse().ok()?;
    let progress_time = parse_label_substr("time=", line)?;
    if progress_time == "N/A" {
        return Some(FfmpegProgress {
            frame,
            fps,
            time: Duration::ZERO,
        });
    }
    let (h, rest) = progress_time.split_once(':')?;
    let (m, s) = rest.split_once(':')?;
    let h = h.parse::<u64>().ok()?;
    let m = m.parse::<u64>().ok()?;
    let s = s.parse::<f64>().ok()?;

    Some(FfmpegProgress {
        frame,
        fps,
        time: Duration::from_secs(h * 60 * 60 + m * 60).checked_add(Duration::from_secs_f64(s))?,
    })
}

pub(crate) fn parse_ffmpeg_stream_sizes(line: &str) -> Option<FfmpegStreamSizes> {
    if !(line.starts_with("video:") && line.contains("muxing overhead")) {
        return None;
    }

    Some(FfmpegStreamSizes {
        video: parse_label_size("video:", line)?,
        audio: parse_label_size("audio:", line)?,
        subtitle: parse_label_size("subtitle:", line)?,
        other: parse_label_size("other streams:", line)?,
    })
}

/// Parse a ffmpeg `label=  value ` type substring.
fn parse_label_substr<'a>(label: &str, line: &'a str) -> Option<&'a str> {
    let line = &line[line.find(label)? + label.len()..];
    let val_start = line.char_indices().find(|(_, c)| !c.is_whitespace())?.0;
    let val_end = val_start
        + line[val_start..]
            .char_indices()
            .find(|(_, c)| c.is_whitespace())
            .map(|(idx, _)| idx)
            .unwrap_or_else(|| line[val_start..].len());

    Some(&line[val_start..val_end])
}

fn parse_label_size(label: &str, line: &str) -> Option<u64> {
    let size = parse_label_substr(label, line)?;
    let kbs: u64 = size.strip_suffix("kB")?.parse().ok()?;
    Some(kbs * 1024)
}

/// Output chunk storage.
///
/// Stores up to ~32k chunk data on the heap.
#[derive(Default)]
pub struct Chunks {
    out: Vec<u8>,
    /// Truncate to this index before the next Self::push
    trunc_next_push: Option<usize>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ChunkLineError {
    #[error("chunk line is not valid UTF-8")]
    Utf8,
}

impl Chunks {
    /// Append a chunk.
    ///
    /// If the chunk **ends** in a '\r' carriage returns this will trigger
    /// appropriate overwriting on the next call to `push`.
    ///
    /// Removes oldest lines if storage exceeds maximum.
    pub fn push(&mut self, chunk: &[u8]) {
        const MAX_LEN: usize = 32_000;

        if let Some(idx) = self.trunc_next_push.take() {
            self.out.truncate(idx);
        }

        self.out.extend(chunk);

        // if too long remove lines until small
        while self.out.len() > MAX_LEN {
            self.rm_oldest_line();
        }

        // Setup `trunc_next_push` driven by '\r'
        // Typically progress updates, e.g. ffmpeg:
        // ```text
        // frame=  495 fps= 25 q=40.0 size=     768KiB time=00:00:16.47 bitrate= 381.8kbits/s speed=0.844x    \r
        // ```
        if chunk.ends_with(b"\r") {
            self.trunc_next_push = Some(self.after_last_line_feed());
        }
    }

    /// Returns index after the latest '\n' or 0 if there are none.
    fn after_last_line_feed(&self) -> usize {
        self.out
            .iter()
            .rposition(|b| *b == b'\n')
            .map(|n| n + 1)
            .unwrap_or(0)
    }

    fn rm_oldest_line(&mut self) {
        let mut next_eol = self
            .out
            .iter()
            .position(|b| *b == b'\n')
            .unwrap_or(self.out.len() - 1);
        if self.out.get(next_eol + 1) == Some(&b'\r') {
            next_eol += 1;
        }

        self.out.splice(..next_eol + 1, []);
    }

    pub fn rfind_line(&self, predicate: impl Fn(&str) -> bool) -> Option<&str> {
        self.rfind_line_map(|line| predicate(line).then_some(line))
    }

    pub fn rfind_line_map<'a, T>(&'a self, f: impl Fn(&'a str) -> Option<T>) -> Option<T> {
        self.rfind_line_map_checked(f).ok().flatten()
    }

    pub fn rfind_line_map_checked<'a, T>(
        &'a self,
        f: impl Fn(&'a str) -> Option<T>,
    ) -> Result<Option<T>, ChunkLineError> {
        let lines = self
            .out
            .rsplit(|b| *b == b'\n')
            .flat_map(|l| l.rsplit(|b| *b == b'\r'));
        for line in lines {
            let line = std::str::from_utf8(line).map_err(|_| ChunkLineError::Utf8)?;
            if let Some(out) = f(line) {
                return Ok(Some(out));
            }
        }
        Ok(None)
    }

    /// Returns last non-empty line, if any.
    pub fn last_line(&self) -> &str {
        self.rfind_line(|l| !l.is_empty()).unwrap_or_default()
    }
}

pin_project_lite::pin_project! {
    /// Streaming ffmpeg stderr parser for encode/sample progress.
    ///
    /// This stream uses the must-complete process policy. Progress events are
    /// opportunistic: ffmpeg may produce none, one, or many progress lines. Success
    /// is only established by the consuming `wait` transition reaching process
    /// completion; EOF before `ProcessDone` is reported as `UnexpectedEof`.
    #[must_use = "streams do nothing unless polled"]
    pub struct FfmpegOutStream {
        #[pin]
        events: Pin<Box<dyn Stream<Item = anyhow::Result<ManagedEvent>>>>,
        name: &'static str,
        cmd_str: String,
        chunks: Chunks,
        completion: FfmpegProcessCompletion,
    }
}

#[derive(Debug, Clone, Copy)]
enum FfmpegProcessCompletion {
    Pending,
    Done(FfmpegProcessDone),
}

impl FfmpegProcessCompletion {
    fn status(self) -> Option<ExitStatus> {
        match self {
            Self::Pending => None,
            Self::Done(done) => Some(done.status()),
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct FfmpegProcessDone(ExitStatus);

impl FfmpegProcessDone {
    fn new(status: ExitStatus) -> Self {
        Self(status)
    }

    fn status(self) -> ExitStatus {
        self.0
    }
}

impl FfmpegOutStream {
    /// Consume progress events until ffmpeg reaches a terminal process status.
    ///
    /// Child failure is returned with bounded stderr and command context.
    /// Parser misses are not errors for encode progress: a successful child may
    /// complete without emitting a parseable progress line.
    pub async fn wait(mut self) -> io::Result<ExitStatus> {
        while self.completion.status().is_none() {
            match self.next().await {
                Some(Ok(_)) => {}
                Some(Err(err)) => return Err(io::Error::other(err)),
                None => break,
            }
        }
        self.completion.status().ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "ffmpeg event stream ended before process completion",
            )
        })
    }
}

impl Stream for FfmpegOutStream {
    type Item = anyhow::Result<FfmpegOut>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            let this = self.as_mut().project();
            match this.events.poll_next(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(item)) => match item {
                    Ok(ManagedEvent::RawStderr(chunk)) => {
                        self.chunks.push(chunk.as_bytes());
                        if let Some(out) = FfmpegOut::try_parse(self.chunks.last_line()) {
                            return Poll::Ready(Some(Ok(out)));
                        }
                    }
                    Ok(ManagedEvent::ReplayGap(_)) => {}
                    Ok(ManagedEvent::ProcessDone(done)) => {
                        let status = done.status();
                        self.completion =
                            FfmpegProcessCompletion::Done(FfmpegProcessDone::new(status));
                        if let Err(err) =
                            exit_ok_stderr(self.name, Ok(status), &self.cmd_str, &self.chunks)
                        {
                            return Poll::Ready(Some(Err(err)));
                        }
                    }
                    Err(err) => return Poll::Ready(Some(Err(err))),
                },
                Poll::Ready(None) => return Poll::Ready(None),
            }
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, self.events.size_hint().1)
    }
}

#[test]
fn parse_ffmpeg_progress_returns_typed_progress() {
    let out = "frame=  161 fps= 73 q=-0.0 size=  978076kB time=00:00:06.71 bitrate=1193201.6kbits/s dup=13 drop=0 speed=3.03x    ";
    assert_eq!(
        parse_ffmpeg_progress(out),
        Some(FfmpegProgress {
            frame: 161,
            fps: 73.0,
            time: Duration::new(6, 710_000_000),
        })
    );
}

#[test]
fn parse_ffmpeg_progress_chunk() {
    let out = "frame=  288 fps= 94 q=-0.0 size=N/A time=01:23:12.34 bitrate=N/A speed=3.94x    \r";
    assert_eq!(
        FfmpegOut::try_parse(out),
        Some(FfmpegOut::Progress {
            frame: 288,
            fps: 94.0,
            time: Duration::new(60 * 60 + 23 * 60 + 12, 340_000_000),
        })
    );
}

#[test]
fn parse_ffmpeg_progress_accepts_elapsed_hours_over_23() {
    let out = "frame=  288 fps= 94 q=-0.0 size=N/A time=25:00:00.00 bitrate=N/A speed=3.94x    \r";
    assert_eq!(
        FfmpegOut::try_parse(out),
        Some(FfmpegOut::Progress {
            frame: 288,
            fps: 94.0,
            time: Duration::from_secs(25 * 60 * 60),
        })
    );
}

#[test]
fn parse_ffmpeg_progress_split_chunk() {
    let mut chunks = Chunks::default();
    let first = b"frame=  288 fps= 94 q=-0.0 size=N/A time=01:23:";
    let second = b"12.34 bitrate=N/A speed=3.94x    \r";

    assert_eq!(
        FfmpegOut::try_parse(std::str::from_utf8(first).unwrap()),
        None
    );
    chunks.push(first);
    assert_eq!(FfmpegOut::try_parse(chunks.last_line()), None);

    chunks.push(second);
    assert_eq!(
        FfmpegOut::try_parse(chunks.last_line()),
        Some(FfmpegOut::Progress {
            frame: 288,
            fps: 94.0,
            time: Duration::new(60 * 60 + 23 * 60 + 12, 340_000_000),
        })
    );
}

#[test]
fn parse_ffmpeg_progress_line() {
    let out = "frame=  161 fps= 73 q=-0.0 size=  978076kB time=00:00:06.71 bitrate=1193201.6kbits/s dup=13 drop=0 speed=3.03x    ";
    assert_eq!(
        FfmpegOut::try_parse(out),
        Some(FfmpegOut::Progress {
            frame: 161,
            fps: 73.0,
            time: Duration::new(6, 710_000_000),
        })
    );
}

#[test]
fn parse_ffmpeg_progress_na_time() {
    let out = "frame=  288 fps= 94 q=-0.0 size=N/A time=N/A bitrate=N/A speed=3.94x    ";
    assert_eq!(
        FfmpegOut::try_parse(out),
        Some(FfmpegOut::Progress {
            frame: 288,
            fps: 94.0,
            time: Duration::ZERO,
        })
    );
}

#[test]
fn parse_ffmpeg_stream_sizes_returns_typed_sizes() {
    let out = "video:2897022kB audio:537162kB subtitle:0kB other streams:0kB global headers:0kB muxing overhead: 0.289700%\n";
    assert_eq!(
        parse_ffmpeg_stream_sizes(out),
        Some(FfmpegStreamSizes {
            video: 2897022 * 1024,
            audio: 537162 * 1024,
            subtitle: 0,
            other: 0,
        })
    );
}

#[test]
fn parse_ffmpeg_out_stream_sizes() {
    let out = "video:2897022kB audio:537162kB subtitle:0kB other streams:0kB global headers:0kB muxing overhead: 0.289700%\n";
    assert_eq!(
        FfmpegOut::try_parse(out),
        Some(FfmpegOut::StreamSizes {
            video: 2897022 * 1024,
            audio: 537162 * 1024,
            subtitle: 0,
            other: 0,
        })
    );
}

#[test]
fn chunks_checked_line_map_reports_malformed_utf8() {
    let mut chunks = Chunks::default();
    chunks.push(b"good line\nbad \xff line\n");

    assert_eq!(
        chunks.rfind_line_map_checked(|line| line.contains("missing").then_some(line)),
        Err(ChunkLineError::Utf8)
    );
}

#[cfg(test)]
mod stream_tests {
    use super::*;
    use std::env;
    use tokio::process::Command;
    use tokio_stream::StreamExt;

    const FIXTURE_ENV: &str = "AB_AV1_MANAGED_PROCESS_FIXTURE";
    const FIXTURE_TEST: &str = "process::managed::tests::managed_process_fixture_child";

    fn fixture_command(fixture: &str) -> Command {
        let mut cmd = Command::new(env::current_exe().expect("current test executable"));
        cmd.arg("--exact")
            .arg(FIXTURE_TEST)
            .arg("--nocapture")
            .env(FIXTURE_ENV, fixture);
        cmd
    }

    #[tokio::test]
    async fn ffmpeg_out_stream_parses_stderr_progress_and_waits() {
        let child = ManagedProcess::spawn(
            "progress fixture",
            fixture_command("stderr-ffmpeg-progress"),
        )
        .expect("spawn progress fixture");
        let mut stream = FfmpegOut::stream(child, "progress fixture", "progress fixture".into());

        assert_eq!(
            stream
                .next()
                .await
                .expect("progress item")
                .expect("progress parse"),
            FfmpegOut::Progress {
                frame: 12,
                fps: 24.0,
                time: Duration::new(1, 500_000_000),
            }
        );

        assert!(
            stream
                .wait()
                .await
                .expect("wait progress fixture")
                .success(),
            "success-path wait should reap the child"
        );
    }

    #[tokio::test]
    async fn ffmpeg_out_stream_supports_long_running_periodic_progress() {
        let child = ManagedProcess::spawn(
            "periodic progress fixture",
            fixture_command("stderr-ffmpeg-progress-twice"),
        )
        .expect("spawn periodic progress fixture");
        let mut stream = FfmpegOut::stream(
            child,
            "periodic progress fixture",
            "periodic progress fixture".into(),
        );

        assert_eq!(
            stream
                .next()
                .await
                .expect("first progress item")
                .expect("first progress parse"),
            FfmpegOut::Progress {
                frame: 12,
                fps: 24.0,
                time: Duration::new(1, 500_000_000),
            }
        );
        assert_eq!(
            stream
                .next()
                .await
                .expect("second progress item")
                .expect("second progress parse"),
            FfmpegOut::Progress {
                frame: 24,
                fps: 24.0,
                time: Duration::new(3, 0),
            }
        );
        assert!(
            stream
                .wait()
                .await
                .expect("wait periodic progress fixture")
                .success()
        );
    }

    #[tokio::test]
    async fn ffmpeg_out_stream_wait_succeeds_for_process_done_without_progress() {
        let child = ManagedProcess::spawn("no-progress fixture", fixture_command("stderr-warning"))
            .expect("spawn no-progress fixture");
        let stream = FfmpegOut::stream(child, "no-progress fixture", "no-progress fixture".into());

        assert!(
            stream
                .wait()
                .await
                .expect("wait no-progress fixture")
                .success()
        );
    }

    #[tokio::test]
    async fn ffmpeg_out_stream_reports_failure_with_stderr_context() {
        let child =
            ManagedProcess::spawn("failure fixture", fixture_command("stderr-badness-exit-7"))
                .expect("spawn failure fixture");
        let mut stream = FfmpegOut::stream(child, "failure fixture", "failure fixture".into());

        let err = stream
            .next()
            .await
            .expect("failure item")
            .expect_err("non-zero exit should surface as stream error")
            .to_string();

        assert!(err.contains("failure fixture exit code 7"));
        assert!(err.contains("----cmd-----\nfailure fixture"));
        assert!(err.contains("---stderr---\nbadness"));
    }

    #[tokio::test]
    async fn ffmpeg_out_stream_failure_context_keeps_recent_bounded_stderr() {
        let child = ManagedProcess::spawn(
            "bounded failure fixture",
            fixture_command("stderr-many-lines-exit-7"),
        )
        .expect("spawn bounded failure fixture");
        let mut stream = FfmpegOut::stream(
            child,
            "bounded failure fixture",
            "bounded failure fixture".into(),
        );

        let err = stream
            .next()
            .await
            .expect("failure item")
            .expect_err("non-zero exit should surface as stream error")
            .to_string();

        assert!(err.contains("bounded failure fixture exit code 7"));
        assert!(err.contains("line-4999"));
        assert!(!err.contains("line-0000"));
        assert!(err.len() < 34_000, "stderr context should stay bounded");
    }

    #[tokio::test]
    async fn ffmpeg_out_stream_ignores_stdout_while_parsing_stderr_progress() {
        let child = ManagedProcess::spawn(
            "mixed-output fixture",
            fixture_command("stdout-noise-stderr-ffmpeg-progress"),
        )
        .expect("spawn mixed-output fixture");
        let mut stream =
            FfmpegOut::stream(child, "mixed-output fixture", "mixed-output fixture".into());

        assert_eq!(
            stream
                .next()
                .await
                .expect("progress item")
                .expect("progress parse"),
            FfmpegOut::Progress {
                frame: 3,
                fps: 30.0,
                time: Duration::new(0, 250_000_000),
            }
        );

        assert!(
            stream
                .wait()
                .await
                .expect("wait mixed-output fixture")
                .success()
        );
    }
}

pub trait CommandExt {
    /// Adds two arguments.
    fn arg2(&mut self, a: impl ArgString, b: impl ArgString) -> &mut Self;

    /// Adds two arguments, the 2nd an option. `None` mean noop.
    fn arg2_opt(&mut self, a: impl ArgString, b: Option<impl ArgString>) -> &mut Self;

    /// Adds two arguments if `condition` otherwise noop.
    fn arg2_if(&mut self, condition: bool, a: impl ArgString, b: impl ArgString) -> &mut Self;

    /// Adds an argument if `condition` otherwise noop.
    fn arg_if(&mut self, condition: bool, a: impl ArgString) -> &mut Self;

    /// Disable audio, subtitle, and data streams (score/null output runs).
    fn suppress_non_video_streams(&mut self) -> &mut Self;

    /// Convert to readable shell-like string.
    fn to_cmd_str(&self) -> String;
}
impl CommandExt for tokio::process::Command {
    fn arg2(&mut self, a: impl ArgString, b: impl ArgString) -> &mut Self {
        self.arg(a.arg_string()).arg(b.arg_string())
    }

    fn arg2_opt(&mut self, a: impl ArgString, b: Option<impl ArgString>) -> &mut Self {
        match b {
            Some(b) => self.arg2(a, b),
            None => self,
        }
    }

    fn arg2_if(&mut self, c: bool, a: impl ArgString, b: impl ArgString) -> &mut Self {
        match c {
            true => self.arg2(a, b),
            false => self,
        }
    }

    fn arg_if(&mut self, condition: bool, a: impl ArgString) -> &mut Self {
        match condition {
            true => self.arg(a.arg_string()),
            false => self,
        }
    }

    fn suppress_non_video_streams(&mut self) -> &mut Self {
        self.arg("-an").arg("-sn").arg("-dn")
    }

    fn to_cmd_str(&self) -> String {
        let cmd = self.as_std();
        cmd.get_args().map(|a| a.to_string_lossy()).fold(
            cmd.get_program().to_string_lossy().to_string(),
            |mut all, next| {
                all.push(' ');
                all += &next;
                all
            },
        )
    }
}

pub trait ArgString {
    fn arg_string(&self) -> Cow<'_, OsStr>;
}

macro_rules! impl_arg_string_as_ref {
    ($t:ty) => {
        impl ArgString for $t {
            fn arg_string(&self) -> Cow<'_, OsStr> {
                Cow::Borrowed(self.as_ref())
            }
        }
    };
}
impl_arg_string_as_ref!(String);
impl_arg_string_as_ref!(&'_ String);
impl_arg_string_as_ref!(&'_ str);
impl_arg_string_as_ref!(&'_ &'_ str);
impl_arg_string_as_ref!(&'_ std::path::Path);
impl_arg_string_as_ref!(&'_ std::path::PathBuf);

macro_rules! impl_arg_string_display {
    ($t:ty) => {
        impl ArgString for $t {
            fn arg_string(&self) -> Cow<'_, OsStr> {
                Cow::Owned(self.to_string().into())
            }
        }
    };
}
impl_arg_string_display!(u8);
impl_arg_string_display!(u16);
impl_arg_string_display!(u32);
impl_arg_string_display!(i32);
impl_arg_string_display!(f32);

impl ArgString for Arc<str> {
    fn arg_string(&self) -> Cow<'_, OsStr> {
        Cow::Borrowed((**self).as_ref())
    }
}
