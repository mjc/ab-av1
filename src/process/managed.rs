#![allow(
    dead_code,
    reason = "managed process wrapper is introduced before callers migrate onto it"
)]

use anyhow::bail;
#[cfg(unix)]
use nix::{
    sys::signal::{Signal, killpg},
    unistd::Pid,
};
#[cfg(unix)]
use std::collections::{HashMap, HashSet};
use std::future::Future;
#[cfg(unix)]
use std::io;
use std::process::{ExitStatus, Output};
use std::sync::Arc;
#[cfg(unix)]
use std::sync::{Mutex, OnceLock};
use std::time::Duration;
use tokio::process::Command;
use tokio_process_tools::{Chunk, visitors::inspect::InspectChunks};
use tokio_process_tools::{
    CollectionOverflowBehavior, Consumable, DEFAULT_MAX_BUFFERED_CHUNKS,
    DEFAULT_OUTPUT_EOF_TIMEOUT, DEFAULT_READ_CHUNK_SIZE, GracefulShutdown,
    LossyWithoutBackpressure, Next, Process, RawCollectionOptions, RawOutputOptions, ReplayEnabled,
    StreamEvent, Subscribable, Subscription,
};
use tokio_process_tools::{NumBytesExt, ProcessHandle, SingleSubscriberOutputStream};
use tokio_stream::Stream;

const DEFAULT_WAIT_TIMEOUT: Duration = Duration::from_secs(30 * 24 * 60 * 60);
const DEFAULT_TERMINATION_GRACE: Duration = Duration::from_millis(25);
const DEFAULT_STDERR_LIMIT: usize = 32_768;

#[cfg(unix)]
static ACTIVE_PROCESS_GROUPS: OnceLock<Mutex<HashSet<i32>>> = OnceLock::new();

#[cfg(unix)]
static SCOPED_PROCESS_GROUPS: OnceLock<Mutex<HashMap<ProcessScope, HashSet<i32>>>> =
    OnceLock::new();

tokio::task_local! {
    static PROCESS_SCOPE: ProcessScope;
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct ProcessScope(Arc<str>);

impl ProcessScope {
    #[must_use]
    pub fn new(id: impl Into<Arc<str>>) -> Self {
        Self(id.into())
    }

    pub async fn run<F>(&self, future: F) -> F::Output
    where
        F: Future,
    {
        PROCESS_SCOPE.scope(self.clone(), future).await
    }

    #[cfg(unix)]
    pub fn pause(&self) -> anyhow::Result<()> {
        self.signal(Signal::SIGSTOP)
    }

    #[cfg(unix)]
    pub fn resume(&self) -> anyhow::Result<()> {
        self.signal(Signal::SIGCONT)
    }

    #[cfg(unix)]
    pub fn stop(&self) -> anyhow::Result<()> {
        self.resume()?;
        self.signal(Signal::SIGTERM)
    }

    #[cfg(unix)]
    fn signal(&self, signal: Signal) -> anyhow::Result<()> {
        let groups = scoped_process_groups()
            .lock()
            .expect("scoped process groups lock")
            .get(self)
            .cloned()
            .unwrap_or_default();
        signal_process_groups(groups, signal)
    }
}

#[cfg(unix)]
fn active_process_groups() -> &'static Mutex<HashSet<i32>> {
    ACTIVE_PROCESS_GROUPS.get_or_init(|| Mutex::new(HashSet::new()))
}

#[cfg(unix)]
fn scoped_process_groups() -> &'static Mutex<HashMap<ProcessScope, HashSet<i32>>> {
    SCOPED_PROCESS_GROUPS.get_or_init(|| Mutex::new(HashMap::new()))
}

#[derive(Debug, Clone, Copy)]
pub struct ManagedProcessOptions {
    wait_timeout: Duration,
    termination_grace: Duration,
    stderr_limit: usize,
}

impl Default for ManagedProcessOptions {
    fn default() -> Self {
        Self {
            wait_timeout: DEFAULT_WAIT_TIMEOUT,
            termination_grace: DEFAULT_TERMINATION_GRACE,
            stderr_limit: DEFAULT_STDERR_LIMIT,
        }
    }
}

impl ManagedProcessOptions {
    pub fn with_wait_timeout(mut self, wait_timeout: Duration) -> Self {
        self.wait_timeout = wait_timeout;
        self
    }

    pub fn with_stderr_limit(mut self, stderr_limit: usize) -> Self {
        self.stderr_limit = stderr_limit;
        self
    }
}

pub struct ManagedProcess {
    handle: ProcessHandle<
        SingleSubscriberOutputStream<LossyWithoutBackpressure, ReplayEnabled>,
        SingleSubscriberOutputStream<LossyWithoutBackpressure, ReplayEnabled>,
    >,
    options: ManagedProcessOptions,
    #[cfg(unix)]
    process_group: Option<i32>,
    #[cfg(unix)]
    process_scope: Option<ProcessScope>,
}

impl Drop for ManagedProcess {
    fn drop(&mut self) {
        #[cfg(unix)]
        if let Some(process_group) = self.process_group {
            active_process_groups()
                .lock()
                .expect("active process groups lock")
                .remove(&process_group);
            if let Some(scope) = &self.process_scope {
                let mut groups = scoped_process_groups()
                    .lock()
                    .expect("scoped process groups lock");
                if let Some(scoped) = groups.get_mut(scope) {
                    scoped.remove(&process_group);
                    if scoped.is_empty() {
                        groups.remove(scope);
                    }
                }
            }
        }
    }
}

/// Process policy for streams that must run through process completion.
///
/// Use this for encode/progress streams where dropping before `ProcessDone` is
/// a programming error. Dropping the wrapper while the child is live preserves
/// the underlying process drop guard, so misuse is loud in tests.
pub struct MustCompleteProcess(ManagedProcess);

/// Process policy for streams where the caller may stop after a logical result.
///
/// Use this for score streams: VMAF/XPSNR can produce a logical score before
/// ffmpeg exits. Dropping this wrapper or a stream built from it terminates the
/// child instead of detaching it.
pub struct TerminateOnDropProcess(Option<ManagedProcess>);

impl Drop for TerminateOnDropProcess {
    fn drop(&mut self) {
        let Some(process) = self.0.take() else {
            return;
        };

        let Ok(handle) = tokio::runtime::Handle::try_current() else {
            return;
        };

        handle.spawn(async move {
            let _ = process.terminate_after(Duration::ZERO).await;
        });
    }
}

/// Bounded terminal output collected after a process has exited.
///
/// Stderr keeps the newest bytes up to the configured limit; when older bytes
/// were dropped, `stderr_truncation` is `Truncated`.
#[derive(Debug)]
pub struct ManagedOutput {
    pub status: ExitStatus,
    pub stderr: Vec<u8>,
    pub stderr_truncation: OutputTruncation,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputTruncation {
    Complete,
    Truncated,
}

impl OutputTruncation {
    fn from_truncated(truncated: bool) -> Self {
        if truncated {
            Self::Truncated
        } else {
            Self::Complete
        }
    }

    pub fn is_truncated(self) -> bool {
        matches!(self, Self::Truncated)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RawOutputChunk(Vec<u8>);

impl RawOutputChunk {
    fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ProcessDone(ExitStatus);

impl ProcessDone {
    fn new(status: ExitStatus) -> Self {
        Self(status)
    }

    pub fn status(self) -> ExitStatus {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct OutputReplayGap;

/// Typed process stderr stream event.
///
/// The underlying tokio-process-tools stream replays recent stderr bytes for
/// delayed subscribers and may report `ReplayGap` when lossy buffering skipped
/// data under pressure. `ProcessDone` means the child reached a terminal status;
/// score streams may yield their own logical completion before this event.
#[derive(Debug)]
pub enum ManagedEvent {
    RawStderr(RawOutputChunk),
    ReplayGap(OutputReplayGap),
    ProcessDone(ProcessDone),
}

impl ManagedProcess {
    pub fn spawn(name: &'static str, cmd: Command) -> anyhow::Result<Self> {
        Self::spawn_with_options(name, cmd, ManagedProcessOptions::default())
    }

    pub fn spawn_with_options(
        name: &'static str,
        cmd: Command,
        options: ManagedProcessOptions,
    ) -> anyhow::Result<Self> {
        let handle = Process::new(cmd)
            .name(name)
            .stdout_and_stderr(|stream| {
                stream
                    .single_subscriber()
                    .lossy_without_backpressure()
                    .replay_last_bytes(DEFAULT_STDERR_LIMIT.bytes())
                    .read_chunk_size(DEFAULT_READ_CHUNK_SIZE)
                    .max_buffered_chunks(DEFAULT_MAX_BUFFERED_CHUNKS)
            })
            .spawn()?;
        #[cfg(unix)]
        let process_group = handle.id().map(|pid| pid as i32);
        #[cfg(unix)]
        let process_scope = PROCESS_SCOPE.try_with(Clone::clone).ok();
        #[cfg(unix)]
        if let Some(process_group) = process_group {
            active_process_groups()
                .lock()
                .expect("active process groups lock")
                .insert(process_group);
            if let Some(scope) = &process_scope {
                scoped_process_groups()
                    .lock()
                    .expect("scoped process groups lock")
                    .entry(scope.clone())
                    .or_default()
                    .insert(process_group);
            }
        }
        Ok(Self {
            handle,
            options,
            #[cfg(unix)]
            process_group,
            #[cfg(unix)]
            process_scope,
        })
    }

    fn graceful_shutdown_for(options: ManagedProcessOptions) -> GracefulShutdown {
        GracefulShutdown::builder()
            .unix_sigterm(options.termination_grace)
            .windows_ctrl_break(options.termination_grace)
            .build()
    }

    pub async fn stderr_chunks(self) -> anyhow::Result<(ExitStatus, Vec<u8>)> {
        let output = self.stderr_output().await?;
        Ok((output.status, output.stderr))
    }

    pub async fn output(mut self) -> anyhow::Result<Output> {
        let options = self.options;
        let shutdown = Self::graceful_shutdown_for(options);
        let output = self
            .handle
            .wait_for_completion(options.wait_timeout)
            .with_raw_output(
                DEFAULT_OUTPUT_EOF_TIMEOUT,
                RawOutputOptions::symmetric(RawCollectionOptions::Bounded {
                    max_bytes: options.stderr_limit.bytes(),
                    overflow_behavior: CollectionOverflowBehavior::DropOldestData,
                }),
            )
            .or_terminate(shutdown)
            .await?;
        let Some(output) = output.into_completed() else {
            bail!(
                "process exceeded {:?} and was terminated",
                options.wait_timeout
            );
        };

        Ok(Output {
            status: output.status,
            stdout: output.stdout.bytes,
            stderr: output.stderr.bytes,
        })
    }

    pub async fn stderr_output(mut self) -> anyhow::Result<ManagedOutput> {
        let options = self.options;
        let shutdown = Self::graceful_shutdown_for(options);
        let output = self
            .handle
            .wait_for_completion(options.wait_timeout)
            .with_raw_output(
                DEFAULT_OUTPUT_EOF_TIMEOUT,
                RawOutputOptions::symmetric(RawCollectionOptions::Bounded {
                    max_bytes: options.stderr_limit.bytes(),
                    overflow_behavior: CollectionOverflowBehavior::DropOldestData,
                }),
            )
            .or_terminate(shutdown)
            .await?;
        let Some(output) = output.into_completed() else {
            bail!(
                "process exceeded {:?} and was terminated",
                options.wait_timeout
            );
        };

        Ok(ManagedOutput {
            status: output.status,
            stderr: output.stderr.bytes,
            stderr_truncation: OutputTruncation::from_truncated(output.stderr.truncated),
        })
    }

    pub async fn observe_stderr_chunks(
        mut self,
        on_chunk: impl FnMut(Chunk) -> Next + Send + 'static,
    ) -> anyhow::Result<ExitStatus> {
        let options = self.options;
        let shutdown = Self::graceful_shutdown_for(options);
        let consumer = self
            .handle
            .stderr()
            .consume(InspectChunks::builder().f(on_chunk).build())?;
        let status = self
            .handle
            .wait_for_completion(options.wait_timeout)
            .or_terminate(shutdown)
            .await?;
        let Some(status) = status.into_completed() else {
            bail!(
                "process exceeded {:?} and was terminated",
                options.wait_timeout
            );
        };
        consumer.wait().await?;
        Ok(status)
    }

    /// Select the must-complete streaming policy.
    ///
    /// This is the policy used by encode progress streams: consumers should
    /// drive the stream to `ProcessDone` or call the stream's terminal `wait`.
    pub fn must_complete(self) -> MustCompleteProcess {
        MustCompleteProcess(self)
    }

    /// Select the terminate-on-drop streaming policy.
    ///
    /// This is the policy used by score streams that can stop after a logical
    /// score. Dropping the wrapper or stream schedules child termination.
    pub fn terminate_on_drop(self) -> TerminateOnDropProcess {
        TerminateOnDropProcess(Some(self))
    }

    pub async fn observe_stdout_chunks(
        mut self,
        on_chunk: impl FnMut(Chunk) -> Next + Send + 'static,
    ) -> anyhow::Result<ExitStatus> {
        let options = self.options;
        let shutdown = Self::graceful_shutdown_for(options);
        let consumer = self
            .handle
            .stdout()
            .consume(InspectChunks::builder().f(on_chunk).build())?;
        let status = self
            .handle
            .wait_for_completion(options.wait_timeout)
            .or_terminate(shutdown)
            .await?;
        let Some(status) = status.into_completed() else {
            bail!(
                "process exceeded {:?} and was terminated",
                options.wait_timeout
            );
        };
        consumer.wait().await?;
        Ok(status)
    }

    pub fn id(&self) -> Option<u32> {
        self.handle.id()
    }

    #[cfg(unix)]
    pub fn pause(&mut self) -> anyhow::Result<()> {
        self.send_process_group_signal("SIGSTOP", Signal::SIGSTOP)
    }

    #[cfg(unix)]
    pub fn resume(&mut self) -> anyhow::Result<()> {
        self.send_process_group_signal("SIGCONT", Signal::SIGCONT)
    }

    #[cfg(unix)]
    fn send_process_group_signal(
        &mut self,
        signal_name: &'static str,
        signal: Signal,
    ) -> anyhow::Result<()> {
        self.handle
            .send_signal_with_reaper(
                signal_name,
                |handle| {
                    let pid = handle.id().ok_or_else(|| {
                        io::Error::new(io::ErrorKind::NotFound, "managed process already exited")
                    })?;
                    killpg(Pid::from_raw(pid as i32), signal)
                        .map_err(|error| io::Error::from_raw_os_error(error as i32))
                },
                |_| Ok(None),
            )
            .map_err(Into::into)
    }

    pub async fn terminate_after(mut self, timeout: Duration) -> anyhow::Result<ExitStatus> {
        Ok(self
            .handle
            .wait_for_completion(timeout)
            .or_terminate(
                GracefulShutdown::builder()
                    .unix_sigterm(self.options.termination_grace)
                    .windows_ctrl_break(self.options.termination_grace)
                    .build(),
            )
            .await?
            .into_result())
    }
}

#[cfg(unix)]
pub fn pause_active_processes() -> anyhow::Result<()> {
    signal_active_processes(Signal::SIGSTOP)
}

#[cfg(unix)]
pub fn resume_active_processes() -> anyhow::Result<()> {
    signal_active_processes(Signal::SIGCONT)
}

#[cfg(unix)]
fn signal_active_processes(signal: Signal) -> anyhow::Result<()> {
    let groups = active_process_groups()
        .lock()
        .expect("active process groups lock")
        .iter()
        .copied()
        .collect::<Vec<_>>();
    signal_process_groups(groups, signal)
}

#[cfg(unix)]
fn signal_process_groups(
    groups: impl IntoIterator<Item = i32>,
    signal: Signal,
) -> anyhow::Result<()> {
    for group in groups {
        match killpg(Pid::from_raw(group), signal) {
            Ok(()) | Err(nix::errno::Errno::ESRCH) => {}
            Err(error) => return Err(error.into()),
        }
    }
    Ok(())
}

fn managed_event_from_stream_event(event: StreamEvent) -> anyhow::Result<Option<ManagedEvent>> {
    Ok(match event {
        StreamEvent::Chunk(chunk) => Some(ManagedEvent::RawStderr(RawOutputChunk::new(
            chunk.as_ref().to_vec(),
        ))),
        StreamEvent::Gap => Some(ManagedEvent::ReplayGap(OutputReplayGap)),
        StreamEvent::Eof => None,
        StreamEvent::ReadError(err) => Err(err)?,
    })
}

async fn wait_for_process_done(process: &mut ManagedProcess) -> anyhow::Result<ProcessDone> {
    let options = process.options;
    let shutdown = ManagedProcess::graceful_shutdown_for(options);
    let status = process
        .handle
        .wait_for_completion(options.wait_timeout)
        .or_terminate(shutdown)
        .await?;
    let status = match status.into_completed() {
        Some(status) => status,
        None => Err(anyhow::anyhow!(
            "process exceeded {:?} and was terminated",
            options.wait_timeout,
        ))?,
    };
    Ok(ProcessDone::new(status))
}

impl MustCompleteProcess {
    /// Stream stderr chunks and then the terminal process status.
    ///
    /// Read errors and timeout termination are yielded as errors. EOF from
    /// stderr is not success by itself; the stream waits for process completion
    /// and only then yields `ProcessDone`.
    pub fn stderr_events(self) -> impl Stream<Item = anyhow::Result<ManagedEvent>> {
        async_stream::try_stream! {
            let mut process = TerminateOnDropProcess(Some(self.0));
            let Some(inner) = process.0.as_mut() else {
                return;
            };
            let mut stderr = inner.handle.stderr().try_subscribe()?;
            while let Some(event) = stderr.next_event().await {
                match managed_event_from_stream_event(event)? {
                    Some(ManagedEvent::RawStderr(chunk)) => yield ManagedEvent::RawStderr(chunk),
                    Some(ManagedEvent::ReplayGap(gap)) => yield ManagedEvent::ReplayGap(gap),
                    Some(ManagedEvent::ProcessDone(done)) => yield ManagedEvent::ProcessDone(done),
                    None => break,
                }
            }

            drop(stderr);
            let Some(inner) = process.0.as_mut() else {
                return;
            };
            let done = wait_for_process_done(inner).await?;
            drop(process.0.take());
            yield ManagedEvent::ProcessDone(done);
        }
    }
}

impl TerminateOnDropProcess {
    /// Stream stderr chunks with cancellation-on-drop semantics.
    ///
    /// If the stream is dropped during stderr streaming or final process wait,
    /// the owned child is terminated. If it reaches `ProcessDone`, the child has
    /// already completed and no cancellation is performed.
    pub fn stderr_events(mut self) -> impl Stream<Item = anyhow::Result<ManagedEvent>> {
        async_stream::try_stream! {
            let mut process = TerminateOnDropProcess(self.0.take());
            let Some(inner) = process.0.as_mut() else {
                return;
            };
            let mut stderr = inner.handle.stderr().try_subscribe()?;
            // Replay is delivered without an explicit gap marker; score streams
            // may subscribe after ffmpeg has already emitted progress lines.
            let first = tokio::time::timeout(Duration::ZERO, stderr.next_event()).await;
            if matches!(first, Ok(Some(_))) {
                yield ManagedEvent::ReplayGap(OutputReplayGap);
            }
            if let Ok(Some(event)) = first {
                match managed_event_from_stream_event(event)? {
                    Some(ManagedEvent::RawStderr(chunk)) => yield ManagedEvent::RawStderr(chunk),
                    Some(ManagedEvent::ReplayGap(gap)) => yield ManagedEvent::ReplayGap(gap),
                    Some(ManagedEvent::ProcessDone(done)) => yield ManagedEvent::ProcessDone(done),
                    None => {}
                }
            }
            while let Some(event) = stderr.next_event().await {
                match managed_event_from_stream_event(event)? {
                    Some(ManagedEvent::RawStderr(chunk)) => yield ManagedEvent::RawStderr(chunk),
                    Some(ManagedEvent::ReplayGap(gap)) => yield ManagedEvent::ReplayGap(gap),
                    Some(ManagedEvent::ProcessDone(done)) => yield ManagedEvent::ProcessDone(done),
                    None => break,
                }
            }

            drop(stderr);
            let Some(inner) = process.0.as_mut() else {
                return;
            };
            let done = wait_for_process_done(inner).await?;
            drop(process.0.take());
            yield ManagedEvent::ProcessDone(done);
        }
    }
}

#[cfg(test)]
mod test_support;

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Context;
    use std::{
        env,
        sync::{Arc, Mutex},
    };
    use test_support::{FIXTURE_ENV, ManagedProcessFixture, fixture_command};
    use tokio_stream::StreamExt;

    #[test]
    fn raw_output_chunk_exposes_borrowed_bytes_for_parsers() {
        let chunk = RawOutputChunk::new(b"progress".to_vec());
        assert_eq!(chunk.as_bytes(), b"progress");
        assert_eq!(chunk.into_bytes(), b"progress");
    }

    #[test]
    fn output_truncation_is_an_explicit_terminal_collection_state() {
        assert_eq!(
            OutputTruncation::from_truncated(false),
            OutputTruncation::Complete
        );
        assert_eq!(
            OutputTruncation::from_truncated(true),
            OutputTruncation::Truncated
        );
    }

    #[test]
    fn managed_process_fixture_child() {
        let Ok(fixture) = env::var(FIXTURE_ENV) else {
            return;
        };

        ManagedProcessFixture::from_name(&fixture)
            .unwrap_or_else(|| panic!("unknown process fixture {fixture}"))
            .run();
    }

    #[test]
    fn streaming_fixture_catalog_covers_required_scenarios() {
        let fixtures = ManagedProcessFixture::ALL;

        for fixture in fixtures {
            assert_eq!(
                ManagedProcessFixture::from_name(fixture.name()),
                Some(*fixture)
            );
            assert!(
                !fixture.expected_sequence().is_empty(),
                "{} should describe its expected stream sequence",
                fixture.name()
            );
        }

        assert!(
            fixtures
                .iter()
                .any(|fixture| fixture.has_periodic_progress())
        );
        assert!(
            fixtures
                .iter()
                .any(|fixture| fixture.has_score_before_continued_runtime())
        );
        assert!(fixtures.iter().any(|fixture| fixture.has_noisy_stderr()));
        assert!(fixtures.iter().any(|fixture| fixture.has_noisy_stdout()));
        assert!(fixtures.iter().any(|fixture| fixture.has_non_zero_exit()));
        assert!(fixtures.iter().any(|fixture| fixture.has_delayed_eof()));
        assert!(fixtures.iter().any(|fixture| fixture.has_timeout_cleanup()));
        assert!(
            fixtures
                .iter()
                .any(|fixture| fixture.has_truncation_volume())
        );
        assert!(
            fixtures
                .iter()
                .any(|fixture| fixture.supports_delayed_subscription_replay())
        );
    }

    async fn assert_score_like_stream_terminates_when_dropped_after_logical_done(
        fixture: &str,
        done_marker: &str,
    ) {
        let cmd = fixture_command(fixture);
        let process =
            ManagedProcess::spawn("score-like stderr fixture", cmd).expect("spawn shell fixture");
        assert!(process.id().is_some(), "process id");
        let mut events = Box::pin(process.terminate_on_drop().stderr_events());
        let mut parsed_logical_done = false;

        while let Some(event) = events.next().await {
            match event.expect("managed event") {
                ManagedEvent::RawStderr(chunk) => {
                    if String::from_utf8_lossy(chunk.as_bytes()).contains(done_marker) {
                        parsed_logical_done = true;
                        break;
                    }
                }
                ManagedEvent::ReplayGap(_) => {}
                ManagedEvent::ProcessDone(_) => {
                    panic!("test must stop polling before ManagedEvent::ProcessDone")
                }
            }
        }

        assert!(parsed_logical_done, "fixture should emit a parseable score");
        drop(events);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    async fn assert_terminate_on_drop_stream_terminates_when_dropped_during_stderr(
        fixture: &str,
        chunk_marker: &str,
    ) {
        let cmd = fixture_command(fixture);
        let process =
            ManagedProcess::spawn("terminate-on-drop fixture", cmd).expect("spawn fixture");
        assert!(process.id().is_some(), "process id");
        let mut events = Box::pin(process.terminate_on_drop().stderr_events());
        let mut saw_chunk = false;

        while let Some(event) = events.next().await {
            match event.expect("managed event") {
                ManagedEvent::RawStderr(chunk) => {
                    if String::from_utf8_lossy(chunk.as_bytes()).contains(chunk_marker) {
                        saw_chunk = true;
                        break;
                    }
                }
                ManagedEvent::ReplayGap(_) => {}
                ManagedEvent::ProcessDone(_) => {
                    panic!("test must stop polling before ManagedEvent::ProcessDone")
                }
            }
        }

        assert!(saw_chunk, "fixture should emit stderr before sleeping");
        drop(events);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    // ab-kgc.45: terminate-on-drop must surface replay gaps for delayed subscribers
    #[tokio::test]
    async fn terminate_on_drop_stderr_stream_yields_replay_gap_for_delayed_subscriber() {
        // setup
        let cmd = fixture_command("vmaf-progress-score");
        let process = ManagedProcess::spawn("replay gap fixture", cmd).expect("spawn fixture");
        tokio::time::sleep(Duration::from_millis(50)).await;
        let mut events = Box::pin(process.terminate_on_drop().stderr_events());

        // execute
        let mut saw_gap = false;
        let mut saw_score = false;
        while let Some(event) = events.next().await {
            match event.expect("managed event") {
                ManagedEvent::RawStderr(chunk) => {
                    if String::from_utf8_lossy(chunk.as_bytes()).contains("VMAF score:") {
                        saw_score = true;
                        break;
                    }
                }
                ManagedEvent::ReplayGap(_) => saw_gap = true,
                ManagedEvent::ProcessDone(_) => break,
            }
        }

        // assert
        assert!(
            saw_gap,
            "delayed subscriber must be notified when replay buffer skipped data"
        );
        assert!(saw_score, "fixture should still yield a parseable score");
    }

    #[tokio::test]
    async fn managed_process_collects_stderr_and_waits() {
        let cmd = fixture_command("stderr-progress");

        let (status, stderr) = ManagedProcess::spawn("stderr fixture", cmd)
            .expect("spawn shell fixture")
            .stderr_chunks()
            .await
            .expect("collect stderr");

        assert!(status.success());
        assert_eq!(stderr, b"progress");
    }

    #[tokio::test]
    async fn managed_process_output_returns_status_stdout_and_stderr() {
        let cmd = fixture_command("stdout-noise-stderr-ffmpeg-progress");

        let output = ManagedProcess::spawn("output fixture", cmd)
            .expect("spawn shell fixture")
            .output()
            .await
            .expect("collect output");

        assert!(output.status.success());
        assert!(String::from_utf8_lossy(&output.stdout).contains("stdout-noise"));
        assert!(String::from_utf8_lossy(&output.stderr).contains("frame="));
    }

    #[tokio::test]
    async fn managed_process_terminates_after_timeout() {
        let cmd = fixture_command("sleep-long");

        let process = ManagedProcess::spawn("sleep fixture", cmd).expect("spawn shell fixture");
        assert!(
            process.id().is_some(),
            "child should be running before termination"
        );

        let status = process
            .terminate_after(Duration::from_millis(25))
            .await
            .expect("terminate child after timeout");

        assert!(
            !status.success(),
            "terminated process should not exit successfully"
        );
    }

    #[tokio::test]
    #[should_panic]
    async fn dropping_must_complete_process_is_loud() {
        let cmd = fixture_command("sleep-long");

        let process = ManagedProcess::spawn("must-complete fixture", cmd)
            .expect("spawn must-complete fixture")
            .must_complete();

        drop(process);
    }

    #[tokio::test]
    async fn dropping_terminate_on_drop_process_terminates_instead_of_panicking() {
        let cmd = fixture_command("sleep-long");

        let process = ManagedProcess::spawn("terminate-on-drop fixture", cmd)
            .expect("spawn terminate-on-drop fixture")
            .terminate_on_drop();

        drop(process);
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    #[tokio::test]
    async fn managed_process_output_timeout_returns_error() {
        let cmd = fixture_command("sleep-long");
        let options = ManagedProcessOptions::default().with_wait_timeout(Duration::from_millis(1));

        let err = ManagedProcess::spawn_with_options("timeout fixture", cmd, options)
            .expect("spawn fixture")
            .stderr_output()
            .await
            .expect_err("timeout should be reported as an error");

        assert!(err.to_string().contains("process exceeded"));
    }

    #[tokio::test]
    async fn managed_process_reports_bounded_stderr_truncation() {
        let cmd = fixture_command("stderr-digits");

        let output = ManagedProcess::spawn("noisy stderr fixture", cmd)
            .expect("spawn shell fixture")
            .stderr_output()
            .await
            .expect("collect bounded stderr");

        assert!(output.status.success());
        assert_eq!(output.stderr, b"1234567890");
        assert_eq!(output.stderr_truncation, OutputTruncation::Complete);
        assert!(!output.stderr_truncation.is_truncated());
    }

    #[tokio::test]
    async fn managed_process_reports_custom_bounded_stderr_truncation() {
        let cmd = fixture_command("stderr-digits");
        let options = ManagedProcessOptions::default().with_stderr_limit(4);

        let output = ManagedProcess::spawn_with_options("noisy stderr fixture", cmd, options)
            .expect("spawn shell fixture")
            .stderr_output()
            .await
            .expect("collect bounded stderr");

        assert!(output.status.success());
        assert_eq!(output.stderr, b"7890");
        assert_eq!(output.stderr_truncation, OutputTruncation::Truncated);
        assert!(output.stderr_truncation.is_truncated());
    }

    #[tokio::test]
    async fn managed_process_observes_stderr_chunks_while_waiting() {
        let cmd = fixture_command("stderr-one-sleep-two");

        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_in_consumer = Arc::clone(&seen);
        let status = ManagedProcess::spawn("streaming stderr fixture", cmd)
            .expect("spawn shell fixture")
            .observe_stderr_chunks(move |chunk| {
                seen_in_consumer
                    .lock()
                    .expect("seen chunks lock")
                    .extend_from_slice(chunk.as_ref());
                Next::Continue
            })
            .await
            .expect("observe stderr");

        assert!(status.success());
        assert!(
            seen.lock()
                .expect("seen chunks lock")
                .windows(b"onetwo".len())
                .any(|window| window == b"onetwo"),
            "stderr observer should include fixture output"
        );
    }

    #[tokio::test]
    async fn managed_process_streams_stderr_events_then_done() {
        let cmd = fixture_command("stderr-onetwo");
        let events = ManagedProcess::spawn("stderr events fixture", cmd)
            .expect("spawn shell fixture")
            .must_complete()
            .stderr_events();
        tokio::pin!(events);

        let mut stderr = Vec::new();
        let mut status = None;
        while let Some(event) = events.next().await {
            match event.expect("managed event") {
                ManagedEvent::RawStderr(chunk) => stderr.extend(chunk.into_bytes()),
                ManagedEvent::ReplayGap(_) => {}
                ManagedEvent::ProcessDone(done) => status = Some(done.status()),
            }
        }

        assert_eq!(stderr, b"onetwo");
        assert!(status.expect("done status").success());
    }

    #[tokio::test]
    async fn managed_process_replays_stderr_emitted_before_subscription() {
        let cmd = fixture_command("stderr-onetwo");
        let process =
            ManagedProcess::spawn("stderr replay fixture", cmd).expect("spawn shell fixture");

        tokio::time::sleep(Duration::from_millis(50)).await;
        let events = process.must_complete().stderr_events();
        tokio::pin!(events);

        let mut stderr = Vec::new();
        while let Some(event) = events.next().await {
            match event.expect("managed event") {
                ManagedEvent::RawStderr(chunk) => stderr.extend(chunk.into_bytes()),
                ManagedEvent::ReplayGap(_) => {}
                ManagedEvent::ProcessDone(_) => break,
            }
        }

        assert_eq!(stderr, b"onetwo");
    }

    #[tokio::test]
    async fn score_like_stderr_event_stream_terminates_when_dropped_after_logical_done() {
        assert_score_like_stream_terminates_when_dropped_after_logical_done(
            "vmaf-score-then-sleep",
            "VMAF score:",
        )
        .await;
        assert_score_like_stream_terminates_when_dropped_after_logical_done(
            "xpsnr-score-then-sleep",
            "XPSNR",
        )
        .await;
    }

    #[tokio::test]
    async fn terminate_on_drop_stderr_event_stream_terminates_when_dropped_during_stderr() {
        assert_terminate_on_drop_stream_terminates_when_dropped_during_stderr(
            "stderr-one-sleep-two",
            "one",
        )
        .await;
    }

    #[tokio::test]
    async fn managed_process_observes_stdout_chunks_while_waiting() {
        let cmd = fixture_command("stdout-one-sleep-two");

        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_in_consumer = Arc::clone(&seen);
        let status = ManagedProcess::spawn("streaming stdout fixture", cmd)
            .expect("spawn shell fixture")
            .observe_stdout_chunks(move |chunk| {
                seen_in_consumer
                    .lock()
                    .expect("seen chunks lock")
                    .extend_from_slice(chunk.as_ref());
                Next::Continue
            })
            .await
            .expect("observe stdout");

        assert!(status.success());
        assert!(
            seen.lock()
                .expect("seen chunks lock")
                .windows(b"onetwo".len())
                .any(|window| window == b"onetwo"),
            "stdout observer should include fixture output"
        );
    }

    #[tokio::test]
    #[should_panic]
    async fn dropping_live_managed_process_panics_instead_of_silently_detaching() {
        let cmd = fixture_command("sleep-long");

        let process = ManagedProcess::spawn("drop guard fixture", cmd).expect("spawn fixture");

        drop(process);
    }

    #[tokio::test]
    async fn explicit_termination_is_the_supported_active_process_cleanup_path() {
        let cmd = fixture_command("sleep-long");

        let process =
            ManagedProcess::spawn("explicit termination fixture", cmd).expect("spawn fixture");
        assert!(
            process.id().is_some(),
            "managed process should expose liveness before terminal transition"
        );

        let status = process
            .terminate_after(Duration::from_millis(25))
            .await
            .expect("terminate managed process");

        assert!(
            !status.success(),
            "timeout termination should return the child terminal status"
        );
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn managed_process_pauses_and_resumes_process_group() -> anyhow::Result<()> {
        let mut command = Command::new("sleep");
        command.arg("30");
        let process = ManagedProcess::spawn("pause-resume-fixture", command)?;
        let pid = process.id().expect("fixture process id");

        pause_active_processes()?;
        assert_eq!(linux_process_state(pid)?, 'T');

        resume_active_processes()?;
        assert_ne!(linux_process_state(pid)?, 'T');

        process.terminate_after(Duration::ZERO).await?;
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn process_scope_controls_only_its_process_group() -> anyhow::Result<()> {
        let first_scope = ProcessScope::new("first-job");
        let second_scope = ProcessScope::new("second-job");
        let first = first_scope
            .run(async {
                let mut command = Command::new("sleep");
                command.arg("30");
                ManagedProcess::spawn("first scoped fixture", command)
            })
            .await?;
        let second = second_scope
            .run(async {
                let mut command = Command::new("sleep");
                command.arg("30");
                ManagedProcess::spawn("second scoped fixture", command)
            })
            .await?;
        let first_pid = first.id().expect("first fixture process id");
        let second_pid = second.id().expect("second fixture process id");

        first_scope.pause()?;
        wait_for_linux_process_state(first_pid, 'T').await?;
        assert_ne!(linux_process_state(second_pid)?, 'T');

        first_scope.resume()?;
        wait_for_linux_process_not_state(first_pid, 'T').await?;
        first.terminate_after(Duration::ZERO).await?;
        second.terminate_after(Duration::ZERO).await?;
        Ok(())
    }

    #[cfg(target_os = "linux")]
    #[tokio::test]
    async fn process_scope_stops_only_its_process_group() -> anyhow::Result<()> {
        let first_scope = ProcessScope::new("stopped-job");
        let second_scope = ProcessScope::new("running-job");
        let first = first_scope
            .run(async {
                let mut command = Command::new("sleep");
                command.arg("30");
                ManagedProcess::spawn("stopped scoped fixture", command)
            })
            .await?;
        let second = second_scope
            .run(async {
                let mut command = Command::new("sleep");
                command.arg("30");
                ManagedProcess::spawn("running scoped fixture", command)
            })
            .await?;
        let second_pid = second.id().expect("running fixture process id");

        first_scope.stop()?;
        let status = first.terminate_after(Duration::from_secs(1)).await?;

        assert!(!status.success());
        assert_ne!(linux_process_state(second_pid)?, 'Z');
        second.terminate_after(Duration::ZERO).await?;
        Ok(())
    }

    #[cfg(target_os = "linux")]
    fn linux_process_state(pid: u32) -> anyhow::Result<char> {
        let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"))?;
        stat.rsplit_once(") ")
            .and_then(|(_, fields)| fields.chars().next())
            .context("read Linux process state")
    }

    #[cfg(target_os = "linux")]
    async fn wait_for_linux_process_state(pid: u32, expected: char) -> anyhow::Result<()> {
        for _ in 0..50 {
            if linux_process_state(pid)? == expected {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        anyhow::bail!("process {pid} did not enter state {expected}")
    }

    #[cfg(target_os = "linux")]
    async fn wait_for_linux_process_not_state(pid: u32, unexpected: char) -> anyhow::Result<()> {
        for _ in 0..50 {
            if linux_process_state(pid)? != unexpected {
                return Ok(());
            }
            tokio::time::sleep(Duration::from_millis(1)).await;
        }
        anyhow::bail!("process {pid} remained in state {unexpected}")
    }
}
