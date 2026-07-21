use crate::command::worker_protocol::{
    AnnouncePayload, CRF_SEARCH_TOPIC, CancelPayload, Capabilities, ClientEvent, ClientFrame,
    ControlAction, ControlPayload, ControlState, ControlStatePayload, CrfSearchCompletedPayload,
    CrfSearchProgressPayload, CrfSearchResultPayload, EncodeCompletedPayload,
    EncodeProgressPayload, ErrorReplyPayload, FailureReportPayload, HeartbeatPayload, JobKind,
    PullWorkPayload, ReplyBody, ServerPushFrame, ServerReply, TransferFailurePayload,
    TransferProgressPayload, TransferStage, TransferStartedPayload, WorkStatus,
};
use crate::command::worker_transfer::{Chunk, ChunkReceiver};
use crate::command::{crf_search, encode, sample_encode};
use crate::ffprobe::Ffprobe;
use crate::process::managed::ProcessScope;
use anyhow::{Context, Result, anyhow, bail};
use clap::{Parser, ValueEnum};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::HashMap,
    fs, io,
    io::Read,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use sysinfo::{Disks, Pid, System};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream,
    tungstenite::{Error as WsError, Message},
    tungstenite::{client::IntoClientRequest, http::header::ORIGIN, protocol::WebSocketConfig},
};
use tracing::{debug, trace};

const PHOENIX_VSN: &str = "2.0.0";
const SUPPORTED_PROTOCOL_VERSION: u64 = 1;
const TRANSFER_CHUNK_MAGIC: &[u8; 4] = b"RAV1";
const TRANSFER_CHUNK_VERSION: u8 = 1;
const TRANSFER_CHUNK_TYPE: u8 = 1;
const TRANSFER_CHUNK_HEADER_LEN: usize = 52;
const MAX_TRANSFER_FRAME_BYTES: usize = 640 * 1024 * 1024;
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
const HTTP_TRANSFER_PROGRESS_INTERVAL: Duration = Duration::from_millis(500);
static HEARTBEAT_SYSTEM: OnceLock<Mutex<System>> = OnceLock::new();

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[value(rename_all = "kebab-case")]
pub(crate) enum WorkerMode {
    #[default]
    CrfSearch,
    Encode,
    Both,
}

impl WorkerMode {
    #[must_use]
    const fn as_str(self) -> &'static str {
        match self {
            Self::CrfSearch => "crf-search",
            Self::Encode => "encode",
            Self::Both => "both",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WorkerCapacity {
    logical_cpus: usize,
    mode: WorkerMode,
}

impl WorkerCapacity {
    #[must_use]
    const fn new(logical_cpus: usize, mode: WorkerMode) -> Self {
        Self { logical_cpus, mode }
    }

    #[must_use]
    fn detect(mode: WorkerMode) -> Self {
        let logical_cpus =
            std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
        Self::new(logical_cpus, mode)
    }

    #[must_use]
    const fn max_active_jobs(self) -> usize {
        match self.mode {
            WorkerMode::CrfSearch | WorkerMode::Encode => 1,
            WorkerMode::Both => {
                if self.logical_cpus > 8 {
                    2
                } else {
                    1
                }
            }
        }
    }
}

/// Connect to a Reencodarr websocket worker endpoint and request one job.
#[derive(Parser, Debug, Clone)]
pub struct Args {
    /// Reencodarr base URL, e.g. http://127.0.0.1:4000
    #[arg(long)]
    connect: String,

    /// Worker authentication token.
    #[arg(long, env = "REENCODARR_WORKER_TOKEN")]
    token: String,

    /// Client worker id announced to Reencodarr.
    #[arg(long)]
    worker_id: String,

    /// Worker version announced to Reencodarr.
    #[arg(long, default_value = env!("CARGO_PKG_VERSION"))]
    version: String,

    /// Protocol version announced to Reencodarr.
    #[arg(long, default_value_t = SUPPORTED_PROTOCOL_VERSION)]
    protocol_version: u64,

    /// Exit after the first work poll instead of running as a long-lived worker.
    #[arg(long)]
    once: bool,

    /// Use a local file instead of waiting for the server to transfer one over the socket.
    #[arg(long)]
    local_path: Option<PathBuf>,

    /// Job kinds this worker accepts.
    #[arg(long = "worker-mode", alias = "mode", default_value = "both")]
    worker_mode: WorkerMode,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerConfig {
    connect: String,
    token: String,
    worker_id: String,
    version: String,
    protocol_version: u64,
    once: bool,
    local_path: Option<PathBuf>,
    worker_mode: WorkerMode,
}

impl From<Args> for WorkerConfig {
    fn from(
        Args {
            connect,
            token,
            worker_id,
            version,
            protocol_version,
            once,
            local_path,
            worker_mode,
        }: Args,
    ) -> Self {
        Self {
            connect,
            token,
            worker_id,
            version,
            protocol_version,
            once,
            local_path,
            worker_mode,
        }
    }
}

#[must_use]
fn initial_pull_work_payload(mode: WorkerMode) -> PullWorkPayload {
    PullWorkPayload {
        input_missing: false,
        job_type: (mode != WorkerMode::CrfSearch).then_some(match mode {
            WorkerMode::CrfSearch | WorkerMode::Both => JobKind::CrfSearch,
            WorkerMode::Encode => JobKind::Encode,
        }),
    }
}

#[must_use]
fn next_job_type_after_no_work(mode: WorkerMode, requested: JobKind) -> Option<JobKind> {
    match (mode, requested) {
        (WorkerMode::Both, JobKind::CrfSearch) => Some(JobKind::Encode),
        (WorkerMode::Encode, JobKind::Encode) => Some(JobKind::Encode),
        (WorkerMode::CrfSearch, JobKind::CrfSearch) => None,
        (WorkerMode::Both, JobKind::Encode) => Some(JobKind::CrfSearch),
        (WorkerMode::CrfSearch, JobKind::Encode) => None,
        (WorkerMode::Encode, JobKind::CrfSearch) => Some(JobKind::Encode),
    }
}

#[derive(Debug, PartialEq)]
pub struct WorkerSession {
    pub assigned_worker_id: String,
    pub negotiated_protocol_version: u64,
    pub work_status: String,
    pub assigned_job: Option<crate::command::worker_protocol::JobAssignedPayload>,
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone)]
struct WorkerJob {
    assignment: crate::command::worker_protocol::JobAssignedPayload,
    input_dir: PathBuf,
    input_path: PathBuf,
}

#[cfg_attr(not(test), allow(dead_code))]
impl WorkerJob {
    fn new(
        assignment: crate::command::worker_protocol::JobAssignedPayload,
        input_dir: PathBuf,
        input_path: PathBuf,
    ) -> Self {
        Self {
            assignment,
            input_dir,
            input_path,
        }
    }

    fn input_path(&self) -> &Path {
        &self.input_path
    }

    fn crf_search_config(&self) -> Result<crf_search::CrfSearchConfig> {
        let mut argv = self.assignment.crf_search_args.clone();
        if argv.is_empty() {
            bail!(
                "job {} missing crf_search_args; server must provide CRF search arguments",
                self.assignment.job_id
            );
        }

        if argv.first().is_some_and(|arg| arg == "ab-av1") {
            argv.remove(0);
        }
        if argv.first().is_some_and(|arg| arg == "crf-search") {
            argv.remove(0);
        }
        argv.insert(0, "crf-search".into());

        let mut config = crf_search::CrfSearchConfig::from(crf_search::Args::try_parse_from(argv)?);
        config.args.input = self.input_path().to_path_buf();
        config.sample.temp_dir = Some(self.input_dir.clone());
        Ok(config)
    }

    fn encode_config(&self) -> Result<encode::EncodeConfig> {
        let mut argv = self.assignment.encode_args.clone();
        if argv.is_empty() {
            bail!(
                "job {} missing encode_args; server must provide encode arguments",
                self.assignment.job_id
            );
        }
        if argv.first().is_some_and(|arg| arg == "ab-av1") {
            argv.remove(0);
        }
        if argv.first().is_none_or(|arg| arg != "encode") {
            argv.insert(0, "encode".into());
        }

        let input = self.input_path().display().to_string();
        if let Some(index) = argv.iter().position(|arg| arg == "--input" || arg == "-i") {
            let value = argv
                .get_mut(index + 1)
                .context("encode_args input flag has no value")?;
            *value = input;
        } else {
            argv.extend(["--input".into(), input]);
        }
        let shared_output = self.assignment.output_shared_path.clone();
        if let Some(index) = argv.iter().position(|arg| arg == "--output" || arg == "-o") {
            let value = argv
                .get_mut(index + 1)
                .context("encode_args output flag has no value")?;
            let output_path = if let Some(shared_output) = shared_output.as_deref() {
                PathBuf::from(shared_output)
            } else {
                let output_name = Path::new(value)
                    .file_name()
                    .context("encode_args output path has no filename")?;
                self.input_dir.join(output_name)
            };
            *value = output_path.display().to_string();
        } else if let Some(shared_output) = shared_output {
            argv.extend(["--output".into(), shared_output]);
        }

        Ok(encode::EncodeConfig::from(encode::Args::try_parse_from(
            argv,
        )?))
    }

    fn progress_payload(
        &self,
        crf: f32,
        status: &sample_encode::Status,
    ) -> CrfSearchProgressPayload {
        CrfSearchProgressPayload {
            job_id: self.assignment.job_id.clone(),
            video_id: self.assignment.video_id,
            percent: (status.progress.clamp(0.0, 1.0) * 100.0),
            filename: self.assignment.source_name.clone(),
            eta: None,
            fps: status.fps,
            crf,
            sample_num: status.sample,
            total_samples: status.samples,
        }
    }

    fn crf_result_payload(
        &self,
        sample: &crf_search::Sample,
        chosen: bool,
    ) -> CrfSearchResultPayload {
        CrfSearchResultPayload {
            job_id: self.assignment.job_id.clone(),
            video_id: self.assignment.video_id,
            source_name: self.assignment.source_name.clone(),
            crf: sample.crf,
            vmaf_score: sample.enc.vmaf_score,
            xpsnr_score: sample.enc.xpsnr_score,
            predicted_encode_size: sample.enc.predicted_encode_size,
            encode_percent: sample.enc.encode_percent,
            predicted_encode_time_secs: sample.enc.predicted_encode_time.as_secs_f64(),
            from_cache: sample.enc.from_cache,
            score: sample.enc.single_score(),
            percent: sample.enc.encode_percent,
            size: sample.enc.predicted_encode_size,
            time: sample.enc.predicted_encode_time.as_secs_f64(),
            params: json!({ "encoder": "libsvtav1", "preset": 8 }),
            target: self.assignment.target_vmaf,
            chosen,
        }
    }

    fn failure_payload(&self, error: &anyhow::Error) -> FailureReportPayload {
        FailureReportPayload {
            job_id: self.assignment.job_id.clone(),
            video_id: self.assignment.video_id,
            stage: match self.assignment.job_type {
                JobKind::CrfSearch => "crf_search",
                JobKind::Encode => "encoding",
            }
            .into(),
            category: "process_failure".into(),
            message: error.to_string(),
            code: match self.assignment.job_type {
                JobKind::CrfSearch => "worker_crf_search_failed",
                JobKind::Encode => "worker_encode_failed",
            }
            .into(),
            context: json!({
                "job_id": self.assignment.job_id,
                "source_name": self.assignment.source_name,
                "error_chain": format!("{error:#}"),
            }),
            retriable: false,
            stderr_excerpt: Some(format!("{error:#}")),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct WorkerJobReportState {
    #[serde(skip_serializing_if = "Option::is_none")]
    heartbeat: Option<HeartbeatPayload>,
    #[serde(skip_serializing_if = "Option::is_none")]
    transfer_progress: Option<TransferProgressPayload>,
    #[serde(skip_serializing_if = "Option::is_none")]
    crf_progress: Option<CrfSearchProgressPayload>,
    #[serde(skip_serializing_if = "Option::is_none")]
    encode_progress: Option<crate::command::worker_protocol::EncodeProgressPayload>,
    #[serde(skip_serializing_if = "Option::is_none")]
    encode_completed: Option<EncodeCompletedPayload>,
    #[serde(skip_serializing_if = "Option::is_none")]
    failure: Option<FailureReportPayload>,
    #[serde(skip_serializing_if = "Option::is_none")]
    crf_completed: Option<CrfSearchCompletedPayload>,
    crf_results: Vec<CrfSearchResultPayload>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkerJobOutcome {
    Completed,
    Stopped,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkerControlState {
    Running,
    Paused,
    Stopped,
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug)]
struct PendingJob {
    job: WorkerJob,
    receiver: Option<ChunkReceiver>,
    transfer_started_at: Instant,
}

#[cfg_attr(not(test), allow(dead_code))]
impl PendingJob {
    fn waiting(job: WorkerJob) -> Self {
        Self {
            job,
            receiver: None,
            transfer_started_at: Instant::now(),
        }
    }

    fn job(&self) -> &WorkerJob {
        &self.job
    }

    fn input_path(&self) -> &Path {
        self.job.input_path()
    }

    fn ensure_receiver(&mut self, chunk_size_bytes: u64) -> Result<()> {
        if self.receiver.is_some() {
            return Ok(());
        }

        let input_path = self.job.input_path().to_path_buf();
        let input_dir = self.job.input_dir.clone();
        let size_bytes = self.job.assignment.size_bytes;
        self.receiver = Some(
            ChunkReceiver::new(input_path, &input_dir, Some(size_bytes), chunk_size_bytes)
                .context("prepare worker input transfer")?,
        );
        self.transfer_started_at = Instant::now();
        Ok(())
    }

    fn apply_raw_chunk(&mut self, chunk: TransferChunk) -> Result<()> {
        if chunk.transfer_id != self.job.assignment.job_id {
            bail!(
                "chunk transfer job mismatch: expected {}, got {}",
                self.job.assignment.job_id,
                chunk.transfer_id
            );
        }
        if chunk.video_id != self.job.assignment.video_id {
            bail!(
                "chunk transfer video mismatch: expected {}, got {}",
                self.job.assignment.video_id,
                chunk.video_id
            );
        }
        if chunk.total_bytes != self.job.assignment.size_bytes {
            bail!(
                "chunk transfer size mismatch: expected {}, got {}",
                self.job.assignment.size_bytes,
                chunk.total_bytes
            );
        }
        if self.receiver.is_none() {
            self.ensure_receiver(chunk.bytes.len() as u64)
                .context("prepare worker input transfer from first chunk")?;
        }
        let offset = self
            .receiver
            .as_ref()
            .expect("pending receiver")
            .received_bytes();
        if offset.saturating_add(chunk.bytes.len() as u64) != chunk.bytes_sent {
            bail!(
                "chunk {} size mismatch: expected cumulative {}, got {}",
                chunk.chunk_index,
                chunk.bytes_sent,
                offset.saturating_add(chunk.bytes.len() as u64)
            );
        }

        let receiver = self.receiver.as_mut().expect("pending receiver");
        receiver.push(Chunk {
            index: chunk.chunk_index,
            offset,
            bytes: chunk.bytes,
            checksum: chunk.crc32,
        })?;
        Ok(())
    }

    fn transfer_progress_payload(
        &self,
        chunk_index: u64,
        total_chunks: u64,
    ) -> TransferProgressPayload {
        let received_bytes = self
            .receiver
            .as_ref()
            .map(ChunkReceiver::received_bytes)
            .unwrap_or(self.job.assignment.size_bytes);
        let expected_bytes = Some(self.job.assignment.size_bytes);
        let elapsed = self.transfer_started_at.elapsed().as_secs().max(1);
        let bytes_per_second = received_bytes / elapsed;
        let remaining_bytes = self
            .job
            .assignment
            .size_bytes
            .saturating_sub(received_bytes);
        let eta = (bytes_per_second > 0).then_some(remaining_bytes / bytes_per_second);
        let percent = if self.job.assignment.size_bytes == 0 {
            100.0
        } else {
            100.0 * received_bytes as f64 / self.job.assignment.size_bytes as f64
        };

        TransferProgressPayload {
            job_id: self.job.assignment.job_id.clone(),
            transfer_id: self.job.assignment.job_id.clone(),
            video_id: self.job.assignment.video_id,
            filename: self.job.assignment.source_name.clone(),
            received_bytes,
            expected_bytes,
            percent,
            bytes_per_second,
            eta,
            chunk_index,
            total_chunks,
        }
    }

    fn finish(&mut self) -> Result<()> {
        let final_path = self.input_path().to_path_buf();
        let receiver = self.receiver.take().context("missing chunk receiver")?;
        let written = receiver
            .finish(Some(self.job.assignment.size_bytes), None)
            .context("finalize worker input transfer")?;
        if written != final_path {
            bail!(
                "transfer finished at unexpected path: expected {}, got {}",
                final_path.display(),
                written.display()
            );
        }
        Ok(())
    }
}

#[cfg_attr(not(test), allow(dead_code))]
async fn run_worker_job(job: WorkerJob, probe: Arc<Ffprobe>) -> Result<crf_search::Sample> {
    run_worker_job_until(job, probe, std::future::pending::<()>()).await
}

#[cfg_attr(not(test), allow(dead_code))]
async fn run_worker_job_until<S>(
    job: WorkerJob,
    probe: Arc<Ffprobe>,
    shutdown: S,
) -> Result<crf_search::Sample>
where
    S: std::future::Future<Output = ()>,
{
    let config = job.crf_search_config()?;
    let mut run = std::pin::pin!(crf_search::run(config, probe));
    tokio::pin!(shutdown);

    loop {
        tokio::select! {
            _ = &mut shutdown => bail!("worker shutdown"),
            update = run.next() => match update {
                Some(Ok(crf_search::Update::Done(best))) => return Ok(best),
                Some(Ok(crf_search::Update::Status { .. }))
                | Some(Ok(crf_search::Update::SampleResult { .. }))
                | Some(Ok(crf_search::Update::RunResult(_))) => {}
                Some(Err(error)) => return Err(error.into()),
                None => break,
            },
        }
    }

    unreachable!("crf-search stream should finish with Done")
}

async fn run_worker_job_with_reporting(
    config: &WorkerConfig,
    job: WorkerJob,
    probe: Arc<Ffprobe>,
    worker: &mut Option<ConnectedWorker>,
) -> Result<WorkerJobOutcome> {
    let crf_config = job.crf_search_config()?;
    let mut run = std::pin::pin!(crf_search::run(crf_config, probe));
    let mut heartbeat = tokio::time::interval(HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut state = WorkerJobReportState::default();
    let mut reconnect = tokio::time::interval(Duration::from_secs(5));
    reconnect.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut completed_best = None;
    let mut paused = false;

    loop {
        match worker.as_mut() {
            Some(current_worker) => {
                tokio::select! {
                    _ = heartbeat.tick() => {
                        let heartbeat = heartbeat_payload(&job.input_dir, Some(job.assignment.video_id));
                        state.heartbeat = Some(heartbeat.clone());
                        if let Some(current_worker) = worker.as_mut() {
                            debug!(
                                job_id = %job.assignment.job_id,
                                active_video_id = job.assignment.video_id,
                                "sending worker heartbeat"
                            );
                            if let Err(error) = current_worker
                                .send_event(ClientEvent::Heartbeat(heartbeat))
                                .await
                            {
                                debug!(
                                    job_id = %job.assignment.job_id,
                                    error = %error,
                                    "worker heartbeat failed; reconnecting while job continues"
                                );
                                *worker = None;
                            }
                        }
                    }
                    frame = current_worker.socket.next() => {
                        match frame {
                            Some(Ok(Message::Ping(payload))) => {
                                current_worker
                                    .socket
                                    .send(Message::Pong(payload))
                                    .await
                                    .context("send websocket pong")?;
                            }
                            Some(Ok(Message::Pong(_))) => {}
                            Some(Ok(Message::Text(text))) => {
                                match decode_worker_push(&text)? {
                                    Some(WorkerPush::Cancel(cancel))
                                        if cancel.job_id == job.assignment.job_id =>
                                    {
                                        eprintln!(
                                            "worker job {} canceled: {}",
                                            cancel.job_id, cancel.reason
                                        );
                                        return Err(anyhow!(
                                            "worker job {} canceled: {}",
                                            cancel.job_id, cancel.reason
                                        ));
                                    }
                                    Some(WorkerPush::Control(control))
                                        if control.video_id.is_none()
                                            || control.video_id == Some(job.assignment.video_id) =>
                                    {
                                        match control.action {
                                            ControlAction::Pause => {
                                                crate::process::managed::pause_active_processes()?;
                                                paused = true;
                                                current_worker.send_control_state(
                                                    ControlState::Paused,
                                                    Some(job.assignment.video_id),
                                                ).await?;
                                            }
                                            ControlAction::Resume | ControlAction::Start => {
                                                crate::process::managed::resume_active_processes()?;
                                                paused = false;
                                                current_worker.send_control_state(
                                                    ControlState::Running,
                                                    Some(job.assignment.video_id),
                                                ).await?;
                                            }
                                            ControlAction::Stop => {
                                                crate::process::managed::resume_active_processes()?;
                                                current_worker.send_control_state(
                                                    ControlState::Stopped,
                                                    None,
                                                ).await?;
                                                return Ok(WorkerJobOutcome::Stopped);
                                            }
                                        }
                                    }
                                    _ => {}
                                }
                            }
                            Some(Ok(Message::Binary(_))) | Some(Ok(Message::Frame(_))) => {}
                            Some(Ok(Message::Close(frame))) => {
                                debug!(job_id = %job.assignment.job_id, ?frame, "worker socket closed during job");
                                *worker = None;
                            }
                            Some(Err(error)) => {
                                debug!(job_id = %job.assignment.job_id, error = %error, "worker socket lost during job");
                                *worker = None;
                            }
                            None => {
                                debug!(job_id = %job.assignment.job_id, "worker websocket ended during job");
                                *worker = None;
                            }
                        }
                    }
                    update = run.next(), if !paused => {
                        let (best, disconnected) = handle_crf_update(
                            &job,
                            &mut state,
                            Some(current_worker),
                            update,
                        ).await?;
                        if disconnected {
                            *worker = None;
                        }
                        if let Some(best) = best {
                            if disconnected || worker.is_none() {
                                completed_best = Some(best);
                            } else {
                                return Ok(WorkerJobOutcome::Completed);
                            }
                        }
                    }
                }
            }
            None => {
                tokio::select! {
                    _ = heartbeat.tick() => {
                        state.heartbeat = Some(heartbeat_payload(&job.input_dir, Some(job.assignment.video_id)));
                    }
                    _ = reconnect.tick() => {
                        match ConnectedWorker::connect(config).await {
                            Ok(mut reconnected) => {
                                if replay_worker_state(&mut reconnected, &state).await {
                                    if paused {
                                        reconnected.send_control_state(
                                            ControlState::Paused,
                                            Some(job.assignment.video_id),
                                        ).await?;
                                    }
                                    *worker = Some(reconnected);
                                    if completed_best.take().is_some() {
                                        return Ok(WorkerJobOutcome::Completed);
                                    }
                                }
                            }
                            Err(error) => {
                                trace!(job_id = %job.assignment.job_id, error = %error, "worker reconnect attempt failed");
                            }
                        }
                    }
                    update = run.next(), if !paused => {
                        let (best, _) = handle_crf_update(
                            &job,
                            &mut state,
                            None,
                            update,
                        ).await?;
                        if let Some(best) = best {
                            completed_best = Some(best);
                        }
                    }
                }
            }
        }
    }
}

async fn handle_crf_update(
    job: &WorkerJob,
    state: &mut WorkerJobReportState,
    worker: Option<&mut ConnectedWorker>,
    update: Option<Result<crf_search::Update, crf_search::Error>>,
) -> Result<(Option<crf_search::Sample>, bool)> {
    let Some(update) = update else {
        return Ok((None, false));
    };

    match update {
        Ok(crf_search::Update::Done(best)) => {
            let payload = job.crf_result_payload(&best, true);
            state.crf_results.push(payload.clone());
            let completed = CrfSearchCompletedPayload {
                job_id: job.assignment.job_id.clone(),
                video_id: job.assignment.video_id,
                result: "ok".into(),
                chosen_crf: best.crf,
                results: Vec::new(),
            };
            state.crf_completed = Some(completed.clone());
            let mut disconnected = false;
            if let Some(worker) = worker {
                disconnected = !send_worker_event(
                    worker,
                    ClientEvent::CrfSearchResult(payload),
                    &job.assignment.job_id,
                    "crf_result",
                )
                .await;
                if !disconnected {
                    disconnected = !send_worker_event(
                        worker,
                        ClientEvent::CrfSearchCompleted(completed),
                        &job.assignment.job_id,
                        "crf_search_completed",
                    )
                    .await;
                }
            }
            Ok((Some(best), disconnected))
        }
        Ok(crf_search::Update::Status { crf, sample, .. }) => {
            let payload = job.progress_payload(crf, &sample);
            state.crf_progress = Some(payload.clone());
            let mut disconnected = false;
            if let Some(worker) = worker {
                disconnected = !send_worker_event(
                    worker,
                    ClientEvent::CrfSearchProgress(payload),
                    &job.assignment.job_id,
                    "crf_progress",
                )
                .await;
            }
            Ok((None, disconnected))
        }
        Ok(crf_search::Update::SampleResult {
            crf,
            sample,
            result,
        }) => {
            debug!(
                job_id = %job.assignment.job_id,
                crf,
                sample,
                vmaf = ?result.vmaf_score,
                "recorded sample result"
            );
            Ok((None, false))
        }
        Ok(crf_search::Update::RunResult(sample)) => {
            let payload = job.crf_result_payload(&sample, false);
            state.crf_results.push(payload.clone());
            let mut disconnected = false;
            if let Some(worker) = worker {
                disconnected = !send_worker_event(
                    worker,
                    ClientEvent::CrfSearchResult(payload),
                    &job.assignment.job_id,
                    "crf_run_result",
                )
                .await;
            }
            Ok((None, disconnected))
        }
        Err(error) => Err(error.into()),
    }
}

async fn replay_worker_state(worker: &mut ConnectedWorker, state: &WorkerJobReportState) -> bool {
    let mut delivered = true;

    if let Some(heartbeat) = &state.heartbeat {
        delivered &= send_worker_event(
            worker,
            ClientEvent::Heartbeat(heartbeat.clone()),
            "state",
            "heartbeat",
        )
        .await;
    }
    if let Some(progress) = &state.transfer_progress {
        delivered &= send_worker_event(
            worker,
            ClientEvent::TransferProgress(progress.clone()),
            "state",
            "transfer_progress",
        )
        .await;
    }
    if let Some(progress) = &state.crf_progress {
        delivered &= send_worker_event(
            worker,
            ClientEvent::CrfSearchProgress(progress.clone()),
            "state",
            "crf_progress",
        )
        .await;
    }
    if let Some(progress) = &state.encode_progress {
        delivered &= send_worker_event(
            worker,
            ClientEvent::EncodeProgress(progress.clone()),
            "state",
            "encode_progress",
        )
        .await;
    }
    if let Some(completed) = &state.encode_completed {
        delivered &= send_worker_event(
            worker,
            ClientEvent::EncodeCompleted(completed.clone()),
            "state",
            "encode_completed",
        )
        .await;
    }
    if let Some(failure) = &state.failure {
        delivered &= send_worker_event(
            worker,
            ClientEvent::VideoFailed(failure.clone()),
            "state",
            "video_failed",
        )
        .await;
    }
    for result in &state.crf_results {
        delivered &= send_worker_event(
            worker,
            ClientEvent::CrfSearchResult(result.clone()),
            "state",
            "crf_result",
        )
        .await;
    }
    if let Some(completed) = &state.crf_completed {
        delivered &= send_worker_event(
            worker,
            ClientEvent::CrfSearchCompleted(completed.clone()),
            "state",
            "crf_search_completed",
        )
        .await;
    }

    delivered
}

async fn send_worker_event(
    worker: &mut ConnectedWorker,
    event: ClientEvent,
    job_id: &str,
    event_name: &'static str,
) -> bool {
    if let Err(error) = worker.send_event(event).await {
        debug!(
            job_id = %job_id,
            event = event_name,
            error = %error,
            "worker event send failed; keeping job and waiting to reconnect"
        );
        false
    } else {
        true
    }
}

fn heartbeat_payload(path: &Path, active_video_id: Option<u64>) -> HeartbeatPayload {
    let system = HEARTBEAT_SYSTEM.get_or_init(|| {
        let mut system = System::new_all();
        system.refresh_cpu();
        Mutex::new(system)
    });
    let mut system = system.lock().expect("heartbeat system lock");
    system.refresh_cpu();
    system.refresh_memory();

    let pid = Pid::from_u32(std::process::id());
    system.refresh_process(pid);
    let memory_rss_bytes = system.process(pid).map(|process| process.memory());

    let disks = Disks::new_with_refreshed_list();
    let disk = disks
        .iter()
        .filter(|disk| path.starts_with(disk.mount_point()))
        .max_by_key(|disk| disk.mount_point().as_os_str().len());

    HeartbeatPayload {
        cpu_percent: Some(system.global_cpu_info().cpu_usage()),
        memory_rss_bytes,
        memory_total_bytes: Some(system.total_memory()),
        disk_free_bytes: disk.map(|disk| disk.available_space()),
        disk_total_bytes: disk.map(|disk| disk.total_space()),
        active_video_id,
    }
}

fn worker_job_input_dir(job_id: &str) -> PathBuf {
    std::env::current_dir()
        .expect("current working directory")
        .join(format!("ab-av1-worker-{}", job_id))
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
struct JoinResponse {
    worker_id: String,
}

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
struct AnnounceResponse {
    accepted: bool,
    protocol_version: u64,
}

#[derive(Debug, Clone, Copy)]
struct WorkerRuntime {
    idle_delay: Duration,
    reconnect_base_delay: Duration,
    reconnect_max_delay: Duration,
    max_pulls: Option<usize>,
}

impl Default for WorkerRuntime {
    fn default() -> Self {
        Self {
            idle_delay: Duration::from_secs(5),
            reconnect_base_delay: Duration::from_secs(1),
            reconnect_max_delay: Duration::from_secs(30),
            max_pulls: None,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct ReconnectBackoff {
    current: Duration,
    base: Duration,
    max: Duration,
}

impl ReconnectBackoff {
    fn new(base: Duration, max: Duration) -> Self {
        Self {
            current: base,
            base,
            max,
        }
    }

    fn next_delay(&mut self) -> Duration {
        let delay = self.current;
        self.current = self.current.saturating_mul(2).min(self.max);
        delay
    }

    fn reset(&mut self) {
        self.current = self.base;
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum WorkerJobPhase {
    AwaitingInput(InputDelivery),
    InputReady,
    CrfSearching,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum InputDelivery {
    Http,
    Resend,
    Websocket,
}

fn worker_job_phase(job: &WorkerJob, local_path: Option<&Path>) -> Result<WorkerJobPhase> {
    if job.input_path().exists() {
        return match job.assignment.status {
            WorkStatus::JobAssigned => Ok(WorkerJobPhase::InputReady),
            WorkStatus::JobInProgress => Ok(WorkerJobPhase::CrfSearching),
            WorkStatus::NoWork => bail!(
                "assigned job {} has invalid no_work status",
                job.assignment.job_id
            ),
        };
    }

    if local_path.is_some() {
        bail!(
            "local input path does not exist: {}",
            job.input_path().display()
        );
    }

    if job.assignment.transfer.is_some() {
        return Ok(WorkerJobPhase::AwaitingInput(InputDelivery::Http));
    }

    match job.assignment.status {
        WorkStatus::JobAssigned => Ok(WorkerJobPhase::AwaitingInput(InputDelivery::Websocket)),
        WorkStatus::JobInProgress => Ok(WorkerJobPhase::AwaitingInput(InputDelivery::Resend)),
        WorkStatus::NoWork => bail!(
            "assigned job {} has invalid no_work status",
            job.assignment.job_id
        ),
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PendingJobOutcome {
    Waiting,
    Ready,
    Canceled,
    Paused,
    Stopped,
}

#[derive(Debug)]
enum WorkerPush {
    Cancel(CancelPayload),
    Control(ControlPayload),
    Started(TransferStartedPayload),
}

#[derive(Debug)]
struct TransferChunk {
    transfer_id: String,
    video_id: u64,
    chunk_index: u64,
    total_chunks: u64,
    bytes_sent: u64,
    total_bytes: u64,
    crc32: u64,
    bytes: Vec<u8>,
}

type WorkerWriter = SplitSink<WorkerSocket, Message>;
type WorkerReader = SplitStream<WorkerSocket>;

#[derive(Debug)]
enum JobCommand {
    TransferStarted(TransferStartedPayload),
    TransferChunk(TransferChunk),
    Control(ControlPayload),
    Cancel(CancelPayload),
}

enum MultiplexOutput {
    Event {
        job_id: String,
        event: ClientEvent,
        name: &'static str,
    },
    Done {
        job_id: String,
        result: std::result::Result<WorkerJobOutcome, String>,
    },
    RequestInputResend {
        job_id: String,
    },
}

struct MultiplexJob {
    job: WorkerJob,
    command: UnboundedSender<JobCommand>,
    state: WorkerJobReportState,
    finished: bool,
}

struct MultiplexedWorker {
    next_ref: u64,
    writer: WorkerWriter,
    reader: WorkerReader,
}

impl MultiplexedWorker {
    fn from_connected(worker: ConnectedWorker) -> Self {
        let ConnectedWorker {
            next_ref,
            socket,
            pending_control: _,
            assigned_worker_id: _,
            negotiated_protocol_version: _,
        } = worker;
        let (writer, reader) = socket.split();
        Self {
            next_ref,
            writer,
            reader,
        }
    }

    async fn send_event(&mut self, event: ClientEvent) -> Result<()> {
        let reference = self.next_ref;
        self.next_ref += 1;
        send_json(&mut self.writer, ClientFrame::new(reference, event)).await
    }

    async fn send_pull(&mut self, payload: PullWorkPayload) -> Result<String> {
        let reference = self.next_ref;
        self.next_ref += 1;
        send_json(
            &mut self.writer,
            ClientFrame::new(reference, ClientEvent::PullWork(payload)),
        )
        .await?;
        Ok(reference.to_string())
    }

    async fn send_pong(&mut self, payload: Vec<u8>) -> Result<()> {
        self.writer
            .send(Message::Pong(payload))
            .await
            .context("send websocket pong")
    }
}

fn multiplex_event(
    output: &UnboundedSender<MultiplexOutput>,
    job_id: &str,
    event: ClientEvent,
    name: &'static str,
) -> bool {
    output
        .send(MultiplexOutput::Event {
            job_id: job_id.to_owned(),
            event,
            name,
        })
        .is_ok()
}

async fn run_multiplex_job(
    job: WorkerJob,
    mut commands: UnboundedReceiver<JobCommand>,
    output: UnboundedSender<MultiplexOutput>,
) {
    let job_id = job.assignment.job_id.clone();
    let process_scope = ProcessScope::new(job_id.clone());
    let result = process_scope
        .run(run_multiplex_job_inner(&job, &mut commands, &output))
        .await;
    if let Err(error) = cleanup_multiplex_worker_input(&job, &result) {
        debug!(
            job_id = %job.assignment.job_id,
            error = %error,
            "failed to remove completed multiplexed worker job directory"
        );
    }
    let outcome = result.map_err(|error| format!("{error:#}"));
    let _ = output.send(MultiplexOutput::Done {
        job_id,
        result: outcome,
    });
}

fn cleanup_multiplex_worker_input(
    job: &WorkerJob,
    outcome: &Result<WorkerJobOutcome>,
) -> Result<()> {
    if matches!(outcome, Ok(WorkerJobOutcome::Completed)) {
        let retain_local_output = job.assignment.job_type == JobKind::Encode
            && job.assignment.output_transfer.is_none()
            && job.assignment.output_shared_path.is_none();
        if retain_local_output && job.input_dir == worker_job_input_dir(&job.assignment.job_id) {
            fs::remove_file(job.input_path()).with_context(|| {
                format!(
                    "remove completed worker input {}",
                    job.input_path().display()
                )
            })?;
        } else {
            remove_worker_input(job)?;
        }
    }
    Ok(())
}

async fn run_multiplex_job_inner(
    job: &WorkerJob,
    commands: &mut UnboundedReceiver<JobCommand>,
    output: &UnboundedSender<MultiplexOutput>,
) -> Result<WorkerJobOutcome> {
    let phase = worker_job_phase(job, None)?;
    if matches!(phase, WorkerJobPhase::AwaitingInput(InputDelivery::Resend)) {
        let _ = output.send(MultiplexOutput::RequestInputResend {
            job_id: job.assignment.job_id.clone(),
        });
    }

    if matches!(phase, WorkerJobPhase::AwaitingInput(InputDelivery::Http)) {
        download_multiplex_input(job, output).await?;
    } else if matches!(phase, WorkerJobPhase::AwaitingInput(_)) {
        receive_multiplex_input(job, commands, output).await?;
    }

    let probe = Arc::new(crate::ffprobe::probe(job.input_path()));
    match job.assignment.job_type {
        JobKind::CrfSearch => run_multiplex_crf(job, probe, commands, output).await,
        JobKind::Encode => run_multiplex_encode(job, probe, commands, output).await,
    }
}

async fn download_multiplex_input(
    job: &WorkerJob,
    output: &UnboundedSender<MultiplexOutput>,
) -> Result<()> {
    let transfer = job
        .assignment
        .transfer
        .clone()
        .context("HTTP input transfer metadata missing")?;
    let parent = job
        .input_path()
        .parent()
        .context("worker input has no parent")?;
    fs::create_dir_all(parent).context("create worker input directory")?;
    let part_path = parent.join(".ab-av1-http.part");
    let input_path = job.input_path().to_path_buf();
    let job_id = job.assignment.job_id.clone();
    let expected_size = job.assignment.size_bytes;
    let received = Arc::new(AtomicU64::new(0));
    let copy_received = Arc::clone(&received);
    let copy_job_id = job_id.clone();
    let copy = tokio::task::spawn_blocking(move || -> Result<u64> {
        let response = ureq::get(&transfer.url)
            .set(&transfer.auth.header, &transfer.auth.value)
            .call()
            .map_err(|error| {
                anyhow!("HTTP input download failed for job {copy_job_id}: {error}")
            })?;
        let reader = CountingReader {
            inner: response.into_reader(),
            received: copy_received,
        };
        let mut output_file =
            fs::File::create(&part_path).context("create HTTP worker input part file")?;
        let bytes = io::copy(&mut reader.take(expected_size), &mut output_file)
            .context("write HTTP worker input")?;
        if expected_size > 0 && bytes != expected_size {
            bail!(
                "HTTP input download for job {copy_job_id} wrote {bytes} bytes, expected {expected_size}"
            );
        }
        fs::rename(&part_path, &input_path).context("move HTTP worker input into place")?;
        Ok(bytes)
    });
    let started_at = Instant::now();
    let mut progress = tokio::time::interval(HTTP_TRANSFER_PROGRESS_INTERVAL);
    progress.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    tokio::pin!(copy);
    loop {
        tokio::select! {
            _ = progress.tick() => {
                let bytes = received.load(Ordering::Relaxed);
                let _ = multiplex_event(
                    output,
                    &job_id,
                    ClientEvent::TransferProgress(http_transfer_progress_payload(
                        &job_id,
                        job.assignment.video_id,
                        &job.assignment.source_name,
                        expected_size,
                        bytes,
                        started_at,
                    )),
                    "transfer_progress",
                );
            }
            result = &mut copy => {
                result.context("join HTTP worker input download task")??;
                return Ok(());
            }
        }
    }
}

async fn receive_multiplex_input(
    job: &WorkerJob,
    commands: &mut UnboundedReceiver<JobCommand>,
    output: &UnboundedSender<MultiplexOutput>,
) -> Result<()> {
    let mut pending = PendingJob::waiting(job.clone());
    let mut heartbeat = tokio::time::interval(HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                let _ = multiplex_event(
                    output,
                    &job.assignment.job_id,
                    ClientEvent::Heartbeat(heartbeat_payload(&job.input_dir, Some(job.assignment.video_id))),
                    "heartbeat",
                );
            }
            command = commands.recv() => match command {
                Some(JobCommand::TransferStarted(started)) => {
                    pending.ensure_receiver(started.chunk_size_bytes)?;
                }
                Some(JobCommand::TransferChunk(chunk)) => {
                    let chunk_index = chunk.chunk_index;
                    let total_chunks = chunk.total_chunks;
                    pending.apply_raw_chunk(chunk)?;
                    let progress = pending.transfer_progress_payload(chunk_index, total_chunks);
                    let _ = multiplex_event(
                        output,
                        &job.assignment.job_id,
                        ClientEvent::TransferProgress(progress),
                        "transfer_progress",
                    );
                    if pending.receiver.as_ref().is_some_and(|receiver| {
                        receiver.received_bytes() == pending.job.assignment.size_bytes
                    }) {
                        pending.finish()?;
                        return Ok(());
                    }
                }
                Some(JobCommand::Cancel(cancel)) => {
                    bail!("worker job {} canceled: {}", cancel.job_id, cancel.reason);
                }
                Some(JobCommand::Control(control)) => {
                    if control.action == ControlAction::Stop {
                        return Ok(());
                    }
                }
                None => bail!("worker command channel closed while receiving input"),
            }
        }
    }
}

async fn run_multiplex_crf(
    job: &WorkerJob,
    probe: Arc<Ffprobe>,
    commands: &mut UnboundedReceiver<JobCommand>,
    output: &UnboundedSender<MultiplexOutput>,
) -> Result<WorkerJobOutcome> {
    let crf_config = job.crf_search_config()?;
    let process_scope = ProcessScope::new(job.assignment.job_id.clone());
    let mut run = std::pin::pin!(crf_search::run(crf_config, probe));
    let mut heartbeat = tokio::time::interval(HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut paused = false;

    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                let _ = multiplex_event(
                    output,
                    &job.assignment.job_id,
                    ClientEvent::Heartbeat(heartbeat_payload(&job.input_dir, Some(job.assignment.video_id))),
                    "heartbeat",
                );
            }
            command = commands.recv() => match command {
                Some(JobCommand::Cancel(cancel)) => {
                    bail!("worker job {} canceled: {}", cancel.job_id, cancel.reason);
                }
                Some(JobCommand::Control(control)) => {
                    match control.action {
                        ControlAction::Pause => {
                            process_scope.pause()?;
                            paused = true;
                            let _ = multiplex_event(
                                output,
                                &job.assignment.job_id,
                                ClientEvent::ControlState(ControlStatePayload {
                                    state: ControlState::Paused,
                                    active_video_id: Some(job.assignment.video_id),
                                    job_id: None,
                                }),
                                "control_state",
                            );
                        }
                        ControlAction::Resume | ControlAction::Start => {
                            process_scope.resume()?;
                            paused = false;
                            let _ = multiplex_event(
                                output,
                                &job.assignment.job_id,
                                ClientEvent::ControlState(ControlStatePayload {
                                    state: ControlState::Running,
                                    active_video_id: Some(job.assignment.video_id),
                                    job_id: None,
                                }),
                                "control_state",
                            );
                        }
                        ControlAction::Stop => {
                            process_scope.stop()?;
                            let _ = multiplex_event(
                                output,
                                &job.assignment.job_id,
                                ClientEvent::ControlState(ControlStatePayload {
                                    state: ControlState::Stopped,
                                    active_video_id: None,
                                    job_id: None,
                                }),
                                "control_state",
                            );
                            return Ok(WorkerJobOutcome::Stopped);
                        }
                    }
                }
                Some(JobCommand::TransferStarted(_) | JobCommand::TransferChunk(_)) => {}
                None => bail!("worker command channel closed while running CRF search"),
            },
            update = run.next(), if !paused => {
                match update {
                    Some(Ok(crf_search::Update::Done(best))) => {
                        let payload = job.crf_result_payload(&best, true);
                        let _ = multiplex_event(
                            output,
                            &job.assignment.job_id,
                            ClientEvent::CrfSearchResult(payload),
                            "crf_result",
                        );
                        let _ = multiplex_event(
                            output,
                            &job.assignment.job_id,
                            ClientEvent::CrfSearchCompleted(CrfSearchCompletedPayload {
                                job_id: job.assignment.job_id.clone(),
                                video_id: job.assignment.video_id,
                                result: "ok".into(),
                                chosen_crf: best.crf,
                                results: Vec::new(),
                            }),
                            "crf_search_completed",
                        );
                        return Ok(WorkerJobOutcome::Completed);
                    }
                    Some(Ok(crf_search::Update::Status { crf, sample, .. })) => {
                        let payload = job.progress_payload(crf, &sample);
                        let _ = multiplex_event(
                            output,
                            &job.assignment.job_id,
                            ClientEvent::CrfSearchProgress(payload),
                            "crf_progress",
                        );
                    }
                    Some(Ok(crf_search::Update::RunResult(sample))) => {
                        let payload = job.crf_result_payload(&sample, false);
                        let _ = multiplex_event(
                            output,
                            &job.assignment.job_id,
                            ClientEvent::CrfSearchResult(payload),
                            "crf_run_result",
                        );
                    }
                    Some(Ok(crf_search::Update::SampleResult { .. })) => {}
                    Some(Err(error)) => return Err(error.into()),
                    None => bail!("CRF search ended without a result"),
                }
            }
        }
    }
}

async fn run_multiplex_encode(
    job: &WorkerJob,
    probe: Arc<Ffprobe>,
    commands: &mut UnboundedReceiver<JobCommand>,
    output: &UnboundedSender<MultiplexOutput>,
) -> Result<WorkerJobOutcome> {
    run_multiplex_encode_with_heartbeat_interval(job, probe, commands, output, HEARTBEAT_INTERVAL)
        .await
}

async fn run_multiplex_encode_with_heartbeat_interval(
    job: &WorkerJob,
    probe: Arc<Ffprobe>,
    commands: &mut UnboundedReceiver<JobCommand>,
    output: &UnboundedSender<MultiplexOutput>,
    heartbeat_interval: Duration,
) -> Result<WorkerJobOutcome> {
    let config = job.encode_config()?;
    let process_scope = ProcessScope::new(job.assignment.job_id.clone());
    let _ = multiplex_event(
        output,
        &job.assignment.job_id,
        ClientEvent::EncodeProgress(EncodeProgressPayload {
            job_id: job.assignment.job_id.clone(),
            video_id: job.assignment.video_id,
            percent: 0.0,
            fps: 0.0,
            eta: None,
            output_bytes: 0,
            output_percent: 0.0,
            throughput: None,
        }),
        "encode_progress",
    );

    let duration = probe.duration.as_ref().ok().copied();
    let input_bytes = fs::metadata(job.input_path())
        .map(|metadata| metadata.len())
        .unwrap_or_default();
    let progress_output = output.clone();
    let progress_job_id = job.assignment.job_id.clone();
    let progress_video_id = job.assignment.video_id;
    let run = encode::run_worker_with_progress(config, probe, move |fps, time, output_path| {
        let output_bytes = fs::metadata(output_path)
            .map(|metadata| metadata.len())
            .unwrap_or_default();
        let percent = duration.map_or(0.0, |duration| {
            (100.0 * time.as_secs_f64() / duration.as_secs_f64().max(0.001)).min(100.0)
        });
        let output_percent = if input_bytes == 0 {
            0.0
        } else {
            100.0 * output_bytes as f64 / input_bytes as f64
        };
        let _ = multiplex_event(
            &progress_output,
            &progress_job_id,
            ClientEvent::EncodeProgress(EncodeProgressPayload {
                job_id: progress_job_id.clone(),
                video_id: progress_video_id,
                percent,
                fps,
                eta: None,
                output_bytes,
                output_percent,
                throughput: Some(format!("{fps:.2} fps")),
            }),
            "encode_progress",
        );
    });
    tokio::pin!(run);
    let mut heartbeat = tokio::time::interval(heartbeat_interval);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    loop {
        tokio::select! {
            _ = heartbeat.tick() => {
                let _ = multiplex_event(
                    output,
                    &job.assignment.job_id,
                    ClientEvent::Heartbeat(heartbeat_payload(&job.input_dir, Some(job.assignment.video_id))),
                    "heartbeat",
                );
            }
            result = &mut run => {
                let (output_path, finished) = result?;
                let output_bytes = finished.metrics.output_bytes;
                let output_percent = finished.metrics.percent;
                let progress = EncodeProgressPayload {
                    job_id: job.assignment.job_id.clone(),
                    video_id: job.assignment.video_id,
                    percent: 100.0,
                    fps: 0.0,
                    eta: Some(0),
                    output_bytes,
                    output_percent,
                    throughput: None,
                };
                if let Some(transfer) = &job.assignment.output_transfer {
                    upload_multiplex_output(&output_path, transfer, output_bytes, &job.assignment.job_id)
                        .await?;
                }
                let _ = multiplex_event(
                    output,
                    &job.assignment.job_id,
                    ClientEvent::EncodeProgress(progress),
                    "encode_progress",
                );
                let _ = multiplex_event(
                    output,
                    &job.assignment.job_id,
                    ClientEvent::EncodeCompleted(EncodeCompletedPayload {
                        job_id: job.assignment.job_id.clone(),
                        video_id: job.assignment.video_id,
                        source_name: job.assignment.source_name.clone(),
                        output_path: output_path.display().to_string(),
                        output_bytes,
                        output_percent,
                    }),
                    "encode_completed",
                );
                return Ok(WorkerJobOutcome::Completed);
            }
            command = commands.recv() => match command {
                Some(JobCommand::Cancel(cancel)) => {
                    bail!("worker job {} canceled: {}", cancel.job_id, cancel.reason);
                }
                Some(JobCommand::Control(control)) => match control.action {
                    ControlAction::Pause => {
                        process_scope.pause()?;
                        let _ = multiplex_event(
                            output,
                            &job.assignment.job_id,
                            ClientEvent::ControlState(ControlStatePayload {
                                state: ControlState::Paused,
                                active_video_id: Some(job.assignment.video_id),
                                job_id: Some(job.assignment.job_id.clone()),
                            }),
                            "control_state",
                        );
                    }
                    ControlAction::Resume | ControlAction::Start => {
                        process_scope.resume()?;
                        let _ = multiplex_event(
                            output,
                            &job.assignment.job_id,
                            ClientEvent::ControlState(ControlStatePayload {
                                state: ControlState::Running,
                                active_video_id: Some(job.assignment.video_id),
                                job_id: Some(job.assignment.job_id.clone()),
                            }),
                            "control_state",
                        );
                    }
                    ControlAction::Stop => {
                        process_scope.stop()?;
                        let _ = multiplex_event(
                            output,
                            &job.assignment.job_id,
                            ClientEvent::ControlState(ControlStatePayload {
                                state: ControlState::Stopped,
                                active_video_id: None,
                                job_id: Some(job.assignment.job_id.clone()),
                            }),
                            "control_state",
                        );
                        return Ok(WorkerJobOutcome::Stopped);
                    }
                },
                Some(JobCommand::TransferStarted(_) | JobCommand::TransferChunk(_)) => {}
                None => bail!("worker command channel closed while running encode"),
            }
        }
    }
}

async fn upload_multiplex_output(
    output_path: &Path,
    transfer: &crate::command::worker_protocol::TransferSpec,
    output_bytes: u64,
    job_id: &str,
) -> Result<()> {
    let output_path = output_path.to_path_buf();
    let transfer = transfer.clone();
    let job_id = job_id.to_owned();
    tokio::task::spawn_blocking(move || -> Result<()> {
        let file = fs::File::open(&output_path).with_context(|| {
            format!(
                "open encoded output for upload for job {job_id}: {}",
                output_path.display()
            )
        })?;
        ureq::put(&transfer.url)
            .set(&transfer.auth.header, &transfer.auth.value)
            .set("Content-Length", &output_bytes.to_string())
            .send(file)
            .map_err(|error| anyhow!("HTTP output upload failed for job {job_id}: {error}"))?;
        Ok(())
    })
    .await
    .context("join HTTP output upload task")??;
    Ok(())
}

type WorkerSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

struct ConnectedWorker {
    assigned_worker_id: String,
    negotiated_protocol_version: u64,
    next_ref: u64,
    socket: WorkerSocket,
    pending_control: Option<ControlAction>,
}

impl ConnectedWorker {
    async fn connect(config: &WorkerConfig) -> Result<Self> {
        let capacity = WorkerCapacity::detect(config.worker_mode);
        let request_url = worker_websocket_url(&config.connect, &config.token)?;
        let mut request = request_url
            .clone()
            .into_client_request()
            .context("build websocket request")?;
        request.headers_mut().insert(
            ORIGIN,
            config
                .connect
                .trim_end_matches('/')
                .parse()
                .context("build websocket origin")?,
        );
        let (mut socket, _) = tokio_tungstenite::connect_async_with_config(
            request,
            Some(worker_websocket_config()),
            false,
        )
        .await
        .map_err(|error| websocket_connect_error(&request_url, error))?;

        send_json(&mut socket, ClientFrame::new(1, ClientEvent::Join)).await?;
        let join: JoinResponse = expect_reply(&mut socket, "1", "phx_join").await?;

        send_json(
            &mut socket,
            ClientFrame::new(
                2,
                ClientEvent::Announce(AnnouncePayload {
                    worker_id: config.worker_id.clone(),
                    hostname: local_hostname(),
                    protocol_version: config.protocol_version,
                    version: config.version.clone(),
                    capabilities: Capabilities {
                        crf_search: matches!(
                            config.worker_mode,
                            WorkerMode::CrfSearch | WorkerMode::Both
                        ),
                        encode: matches!(config.worker_mode, WorkerMode::Encode | WorkerMode::Both),
                        mode: config.worker_mode.as_str().into(),
                        logical_cpus: capacity.logical_cpus,
                        max_active_jobs: capacity.max_active_jobs(),
                    },
                }),
            ),
        )
        .await?;
        let announce: AnnounceResponse = expect_reply(&mut socket, "2", "announce").await?;
        if !announce.accepted {
            bail!("worker announcement was not accepted");
        }

        Ok(Self {
            assigned_worker_id: join.worker_id,
            negotiated_protocol_version: announce.protocol_version,
            next_ref: 3,
            socket,
            pending_control: None,
        })
    }

    async fn request_work(&mut self) -> Result<ServerReply> {
        self.request_work_with(PullWorkPayload::default()).await
    }

    async fn request_work_kind(&mut self, job_type: JobKind) -> Result<ServerReply> {
        self.request_work_with(PullWorkPayload {
            input_missing: false,
            job_type: Some(job_type),
        })
        .await
    }

    async fn request_work_with(&mut self, payload: PullWorkPayload) -> Result<ServerReply> {
        let request_ref = self.next_ref;
        self.next_ref += 1;
        let frame = ClientFrame::new(request_ref, ClientEvent::PullWork(payload));
        debug!(
            request_ref = request_ref,
            frame = %serde_json::to_string(&frame).context("serialize pull_work frame")?,
            "sending pull_work"
        );

        send_json(&mut self.socket, frame).await?;
        let expected_ref = request_ref.to_string();
        while let Some(message) = self.socket.next().await {
            match message.context("read websocket message")? {
                Message::Text(text) => {
                    if let Some(WorkerPush::Control(control)) = decode_worker_push(&text)? {
                        self.pending_control = Some(control.action);
                        continue;
                    }
                    if let Some(reply) = decode_expected_reply(&text, &expected_ref, "pull_work")? {
                        return reply;
                    }
                }
                Message::Ping(payload) => {
                    self.socket
                        .send(Message::Pong(payload))
                        .await
                        .context("send websocket pong")?;
                }
                Message::Close(frame) => {
                    bail!("websocket closed while waiting for work: {frame:?}")
                }
                Message::Pong(_) | Message::Binary(_) | Message::Frame(_) => {}
            }
        }
        bail!("websocket ended while waiting for work")
    }

    fn take_pending_control(&mut self) -> Option<ControlAction> {
        self.pending_control.take()
    }

    async fn send_event(&mut self, event: ClientEvent) -> Result<()> {
        let request_ref = self.next_ref;
        self.next_ref += 1;
        send_json(&mut self.socket, ClientFrame::new(request_ref, event)).await
    }

    async fn send_control_state(
        &mut self,
        state: ControlState,
        active_video_id: Option<u64>,
    ) -> Result<()> {
        self.send_event(ClientEvent::ControlState(ControlStatePayload {
            state,
            active_video_id,
            job_id: None,
        }))
        .await
    }

    async fn send_transfer_progress(&mut self, payload: TransferProgressPayload) -> Result<()> {
        let throughput = format_bytes_per_second(payload.bytes_per_second);
        debug!(
            job_id = %payload.job_id,
            transfer_id = %payload.transfer_id,
            video_id = payload.video_id,
            received_bytes = payload.received_bytes,
            expected_bytes = ?payload.expected_bytes,
            percent = payload.percent,
            bytes_per_second = %throughput,
            chunk_index = payload.chunk_index,
            total_chunks = payload.total_chunks,
            "sending transfer progress"
        );
        self.send_event(ClientEvent::TransferProgress(payload))
            .await
    }

    async fn wait_for_pending_job(
        &mut self,
        pending_job: &mut PendingJob,
        idle_delay: Duration,
    ) -> Result<PendingJobOutcome> {
        tokio::select! {
            frame = self.socket.next() => {
                match frame {
                    Some(Ok(Message::Ping(payload))) => {
                        self.socket
                            .send(Message::Pong(payload))
                            .await
                            .context("send websocket pong")?;
                        Ok(PendingJobOutcome::Waiting)
                    }
                    Some(Ok(Message::Pong(_))) => Ok(PendingJobOutcome::Waiting),
                    Some(Ok(Message::Text(text))) => {
                        match decode_worker_push(&text)? {
                            Some(WorkerPush::Cancel(cancel))
                                if cancel.job_id == pending_job.job().assignment.job_id =>
                            {
                                eprintln!(
                                    "worker job {} canceled: {}",
                                    cancel.job_id, cancel.reason
                                );
                                Ok(PendingJobOutcome::Canceled)
                            }
                            Some(WorkerPush::Started(started))
                                if started.transfer_id == pending_job.job().assignment.job_id =>
                            {
                                pending_job.ensure_receiver(started.chunk_size_bytes)?;
                                debug!(
                                    job_id = %started.transfer_id,
                                    source_name = %started.source_name,
                                    chunk_size_bytes = started.chunk_size_bytes,
                                    size_bytes = started.size_bytes,
                                    total_bytes = started.total_bytes,
                                    total_chunks = started.total_chunks,
                                    received_bytes = pending_job
                                        .receiver
                                        .as_ref()
                                        .map(ChunkReceiver::received_bytes)
                                        .unwrap_or_default(),
                                    "transfer started"
                                );
                                Ok(PendingJobOutcome::Waiting)
                            }
                            Some(WorkerPush::Control(control)) => match control.action {
                                ControlAction::Stop => {
                                    self.send_control_state(ControlState::Stopped, None).await?;
                                    Ok(PendingJobOutcome::Stopped)
                                }
                                ControlAction::Pause => {
                                    self.send_control_state(
                                        ControlState::Paused,
                                        Some(pending_job.job.assignment.video_id),
                                    ).await?;
                                    Ok(PendingJobOutcome::Paused)
                                }
                                ControlAction::Resume | ControlAction::Start => {
                                    self.send_control_state(
                                        ControlState::Running,
                                        Some(pending_job.job.assignment.video_id),
                                    ).await?;
                                    Ok(PendingJobOutcome::Waiting)
                                }
                            },
                            Some(_) => Ok(PendingJobOutcome::Waiting),
                            None => Ok(PendingJobOutcome::Waiting),
                        }
                    }
                    Some(Ok(Message::Binary(bytes))) => {
                        let chunk = decode_binary_worker_push(&bytes)?;
                        if let Some(chunk) = chunk
                            && chunk.transfer_id == pending_job.job().assignment.job_id
                        {
                            if chunk.chunk_index == 0 || chunk.chunk_index % 16 == 0 {
                                debug!(
                                    job_id = %chunk.transfer_id,
                                    chunk_index = chunk.chunk_index,
                                    bytes_sent = chunk.bytes_sent,
                                    total_bytes = chunk.total_bytes,
                                    total_chunks = chunk.total_chunks,
                                    "received binary chunk"
                                );
                            } else {
                                trace!(
                                    job_id = %chunk.transfer_id,
                                    chunk_index = chunk.chunk_index,
                                    bytes_sent = chunk.bytes_sent,
                                    total_bytes = chunk.total_bytes,
                                    total_chunks = chunk.total_chunks,
                                    "received binary chunk"
                                );
                            }
                            let chunk_index = chunk.chunk_index;
                            let total_chunks = chunk.total_chunks;
                            pending_job.apply_raw_chunk(chunk)?;
                            self.send_transfer_progress(
                                pending_job.transfer_progress_payload(chunk_index, total_chunks),
                            )
                            .await?;
                            if pending_job.receiver.as_ref().is_some_and(|receiver| {
                                receiver.received_bytes() == pending_job.job.assignment.size_bytes
                            }) {
                                debug!(
                                    job_id = %pending_job.job().assignment.job_id,
                                    "transfer complete"
                                );
                                pending_job.finish()?;
                                return Ok(PendingJobOutcome::Ready);
                            }
                        }
                        Ok(PendingJobOutcome::Waiting)
                    }
                    Some(Ok(Message::Frame(_))) => {
                        Ok(PendingJobOutcome::Waiting)
                    }
                    Some(Ok(Message::Close(frame))) => {
                        bail!("websocket closed while waiting for worker input: {frame:?}")
                    }
                    Some(Err(error)) => Err(error).context("read websocket message"),
                    None => bail!("websocket ended while waiting for worker input"),
                }
            }
            _ = tokio::time::sleep(idle_delay) => {
                self
                    .send_event(ClientEvent::Heartbeat(heartbeat_payload(
                        &pending_job.job.input_dir,
                        Some(pending_job.job.assignment.video_id),
                    )))
                    .await?;
                debug!(
                    job_id = %pending_job.job().assignment.job_id,
                    received_bytes = pending_job
                        .receiver
                        .as_ref()
                        .map(|receiver| receiver.received_bytes())
                        .unwrap_or_default(),
                    "pending job still waiting"
                );
                Ok(PendingJobOutcome::Waiting)
            }
        }
    }

    async fn wait_until_running(
        &mut self,
        control_state: &mut WorkerControlState,
        idle_delay: Duration,
    ) -> Result<bool> {
        let mut stopped = *control_state == WorkerControlState::Stopped;
        loop {
            tokio::select! {
                frame = self.socket.next() => match frame {
                    Some(Ok(Message::Ping(payload))) => {
                        self.socket.send(Message::Pong(payload)).await.context("send websocket pong")?;
                    }
                    Some(Ok(Message::Pong(_)))
                    | Some(Ok(Message::Binary(_)))
                    | Some(Ok(Message::Frame(_))) => {}
                    Some(Ok(Message::Text(text))) => {
                        if let Some(WorkerPush::Control(control)) = decode_worker_push(&text)? {
                            match control.action {
                                ControlAction::Start | ControlAction::Resume => {
                                    self.send_control_state(ControlState::Running, None).await?;
                                    *control_state = WorkerControlState::Running;
                                    return Ok(stopped);
                                }
                                ControlAction::Pause if !stopped => {
                                    self.send_control_state(ControlState::Paused, None).await?;
                                    *control_state = WorkerControlState::Paused;
                                }
                                ControlAction::Pause | ControlAction::Stop => {
                                    self.send_control_state(ControlState::Stopped, None).await?;
                                    *control_state = WorkerControlState::Stopped;
                                    stopped = true;
                                }
                            }
                        }
                    }
                    Some(Ok(Message::Close(frame))) => bail!("websocket closed while worker stopped: {frame:?}"),
                    Some(Err(error)) => return Err(error).context("read websocket message while worker stopped"),
                    None => bail!("websocket ended while worker stopped"),
                },
                _ = tokio::time::sleep(idle_delay) => {
                    self.send_event(ClientEvent::Heartbeat(heartbeat_payload(Path::new("."), None))).await?;
                }
            }
        }
    }
}

fn local_hostname() -> Option<String> {
    std::env::var("HOSTNAME").ok().or_else(|| {
        std::fs::read_to_string("/proc/sys/kernel/hostname")
            .ok()
            .map(|hostname| hostname.trim().to_owned())
            .filter(|hostname| !hostname.is_empty())
    })
}

fn format_bytes_per_second(bytes_per_second: u64) -> String {
    const MIB: u64 = 1024 * 1024;
    format!("{} MiB/s", bytes_per_second / MIB)
}

async fn run_worker_job_and_publish(
    config: &WorkerConfig,
    worker: &mut Option<ConnectedWorker>,
    job: &WorkerJob,
) -> Result<WorkerJobOutcome> {
    debug!(
        job_id = %job.assignment.job_id,
        input = %job.input_path().display(),
        "starting worker job"
    );
    let probe = Arc::new(crate::ffprobe::probe(job.input_path()));
    debug!(job_id = %job.assignment.job_id, "probe complete, running worker job");
    let result = match job.assignment.job_type {
        JobKind::CrfSearch => {
            run_worker_job_with_reporting(config, job.clone(), probe, worker).await
        }
        JobKind::Encode => run_worker_encode(job, probe, worker).await,
    };
    crate::temporary::clean_all().await;
    remove_worker_input(job)?;
    let outcome = match result {
        Ok(outcome) => outcome,
        Err(error) => {
            publish_worker_failure(worker, job, &error).await;
            return Err(error);
        }
    };
    Ok(outcome)
}

async fn run_worker_encode(
    job: &WorkerJob,
    probe: Arc<Ffprobe>,
    worker: &mut Option<ConnectedWorker>,
) -> Result<WorkerJobOutcome> {
    let config = job.encode_config()?;
    let (output_path, finished) = encode::run_worker(config, probe).await?;
    let output_bytes = finished.metrics.output_bytes;
    if let Some(worker) = worker {
        send_worker_event(
            worker,
            ClientEvent::EncodeCompleted(EncodeCompletedPayload {
                job_id: job.assignment.job_id.clone(),
                video_id: job.assignment.video_id,
                source_name: job.assignment.source_name.clone(),
                output_path: output_path.display().to_string(),
                output_bytes,
                output_percent: finished.metrics.percent,
            }),
            &job.assignment.job_id,
            "encode_completed",
        )
        .await;
    }
    Ok(WorkerJobOutcome::Completed)
}

fn remove_worker_input(job: &WorkerJob) -> Result<()> {
    if job.input_dir != worker_job_input_dir(&job.assignment.job_id) {
        return Ok(());
    }

    debug!(
        job_id = %job.assignment.job_id,
        input = %job.input_path().display(),
        "removing worker input"
    );
    fs::remove_dir_all(&job.input_dir)
        .with_context(|| format!("remove worker input directory {}", job.input_dir.display()))?;
    Ok(())
}

fn remove_pending_worker_input(pending_job: &mut Option<PendingJob>) -> Result<()> {
    pending_job
        .take()
        .map_or(Ok(()), |pending| remove_worker_input(&pending.job))
}

async fn publish_worker_failure(
    worker: &mut Option<ConnectedWorker>,
    job: &WorkerJob,
    error: &anyhow::Error,
) {
    let Some(worker) = worker else {
        return;
    };

    let payload = job.failure_payload(error);
    let _ = send_worker_event(
        worker,
        ClientEvent::VideoFailed(payload),
        &job.assignment.job_id,
        "video_failed",
    )
    .await;
}

fn build_worker_job(
    assignment: crate::command::worker_protocol::JobAssignedPayload,
    local_path: Option<&Path>,
) -> Result<WorkerJob> {
    if local_path.is_none()
        && let Some(input_path) = offered_local_input(&assignment)
    {
        return Ok(WorkerJob::new(
            assignment,
            std::env::current_dir().context("current working directory")?,
            input_path,
        ));
    }

    let input_dir = worker_job_input_dir(&assignment.job_id);
    fs::create_dir_all(&input_dir).context("create worker job dir")?;
    let input_path = local_path
        .map(Path::to_path_buf)
        .map(Ok)
        .unwrap_or_else(|| worker_job_input_path(&input_dir, &assignment))?;
    Ok(WorkerJob::new(assignment, input_dir, input_path))
}

fn offered_local_input(
    assignment: &crate::command::worker_protocol::JobAssignedPayload,
) -> Option<PathBuf> {
    let args = match assignment.job_type {
        JobKind::CrfSearch => &assignment.crf_search_args,
        JobKind::Encode => &assignment.encode_args,
    };
    let path = args
        .windows(2)
        .find_map(|args| (args[0] == "--input").then(|| PathBuf::from(&args[1])))?;
    let metadata = path.metadata().ok()?;
    (metadata.is_file() && metadata.len() == assignment.size_bytes).then_some(path)
}

fn worker_job_input_path(
    input_dir: &Path,
    assignment: &crate::command::worker_protocol::JobAssignedPayload,
) -> Result<PathBuf> {
    let source_name = worker_source_file_name(assignment)?;
    let expected = input_dir.join(&source_name);
    if expected.exists() {
        return Ok(expected);
    }

    let mut candidates = Vec::new();
    for entry in fs::read_dir(input_dir).context("read worker job dir")? {
        let entry = entry.context("read worker job dir entry")?;
        let path = entry.path();
        let metadata = entry
            .metadata()
            .context("read worker job dir entry metadata")?;
        debug!(
            job_id = %assignment.job_id,
            path = %path.display(),
            is_file = metadata.is_file(),
            len = metadata.len(),
            expected_len = assignment.size_bytes,
            "found worker input dir entry"
        );
        if !metadata.is_file() || metadata.len() != assignment.size_bytes {
            continue;
        }
        if path
            .file_name()
            .is_some_and(|name| name == ".ab-av1-worker.part")
        {
            continue;
        }
        candidates.push(path);
    }

    match candidates.as_slice() {
        [path] => {
            debug!(
                job_id = %assignment.job_id,
                input = %path.display(),
                expected = %expected.display(),
                source_name = %assignment.source_name,
                "using existing completed worker input"
            );
            Ok(path.clone())
        }
        [] => {
            debug!(
                job_id = %assignment.job_id,
                expected = %expected.display(),
                source_name = %assignment.source_name,
                "no existing completed worker input found"
            );
            Ok(expected)
        }
        _ => {
            debug!(
                job_id = %assignment.job_id,
                expected = %expected.display(),
                source_name = %assignment.source_name,
                candidates = candidates.len(),
                "multiple existing worker inputs matched expected size"
            );
            Ok(expected)
        }
    }
}

fn worker_source_file_name(
    assignment: &crate::command::worker_protocol::JobAssignedPayload,
) -> Result<PathBuf> {
    let args = match assignment.job_type {
        JobKind::CrfSearch => &assignment.crf_search_args,
        JobKind::Encode => &assignment.encode_args,
    };
    Path::new(&assignment.source_name)
        .file_name()
        .map(PathBuf::from)
        .or_else(|| {
            args.windows(2)
                .find(|args| args[0] == "--input")
                .and_then(|args| Path::new(&args[1]).file_name().map(PathBuf::from))
        })
        .with_context(|| {
            format!(
                "job {} has no source filename in source_name or crf_search_args --input",
                assignment.job_id
            )
        })
}

async fn download_worker_input(worker: &mut ConnectedWorker, job: &WorkerJob) -> Result<bool> {
    let Some(transfer) = job.assignment.transfer.clone() else {
        return Ok(false);
    };

    if job.input_path().exists() {
        return Ok(true);
    }

    let parent = job
        .input_path()
        .parent()
        .with_context(|| format!("worker input has no parent: {}", job.input_path().display()))?;
    fs::create_dir_all(parent).context("create worker input dir")?;

    let part_path = parent.join(".ab-av1-http.part");
    let input_path = job.input_path().to_path_buf();
    let job_id = job.assignment.job_id.clone();
    let expected_size = job.assignment.size_bytes;
    let received = Arc::new(AtomicU64::new(0));
    let copy_received = Arc::clone(&received);

    let mut copy = tokio::task::spawn_blocking(move || -> Result<u64> {
        let response = ureq::get(&transfer.url)
            .set(&transfer.auth.header, &transfer.auth.value)
            .call()
            .map_err(|error| anyhow!("HTTP input download failed for job {job_id}: {error}"))?;

        let reader = CountingReader {
            inner: response.into_reader(),
            received: copy_received,
        };
        let mut output =
            fs::File::create(&part_path).context("create HTTP worker input part file")?;
        let bytes = io::copy(&mut reader.take(expected_size), &mut output)
            .context("write HTTP worker input")?;

        if expected_size > 0 && bytes != expected_size {
            bail!(
                "HTTP input download for job {job_id} wrote {bytes} bytes, expected {expected_size}"
            );
        }

        fs::rename(&part_path, &input_path).context("move HTTP worker input into place")?;
        Ok(bytes)
    });

    let started_at = Instant::now();
    let mut progress = tokio::time::interval(HTTP_TRANSFER_PROGRESS_INTERVAL);
    progress.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            _ = progress.tick() => {
                let bytes = received.load(Ordering::Relaxed);
                if bytes > 0 {
                    worker
                        .send_transfer_progress(http_transfer_progress_payload(
                            &job.assignment.job_id,
                            job.assignment.video_id,
                            &job.assignment.source_name,
                            job.assignment.size_bytes,
                            bytes,
                            started_at,
                        ))
                        .await?;
                }
            }
            result = &mut copy => {
                let bytes = result.context("join HTTP worker input download task")??;
                worker
                    .send_transfer_progress(http_transfer_progress_payload(
                        &job.assignment.job_id,
                        job.assignment.video_id,
                        &job.assignment.source_name,
                        job.assignment.size_bytes,
                        bytes,
                        started_at,
                    ))
                    .await?;
                break;
            }
        }
    }

    Ok(true)
}

struct CountingReader<R> {
    inner: R,
    received: Arc<AtomicU64>,
}

impl<R: Read> Read for CountingReader<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let read = self.inner.read(buf)?;
        self.received.fetch_add(read as u64, Ordering::Relaxed);
        Ok(read)
    }
}

fn http_transfer_progress_payload(
    job_id: &str,
    video_id: u64,
    filename: &str,
    expected_size: u64,
    received_bytes: u64,
    started_at: Instant,
) -> TransferProgressPayload {
    let elapsed = started_at.elapsed().as_secs().max(1);
    let bytes_per_second = received_bytes / elapsed;
    let remaining_bytes = expected_size.saturating_sub(received_bytes);
    let eta = (bytes_per_second > 0).then_some(remaining_bytes / bytes_per_second);
    let percent = if expected_size == 0 {
        100.0
    } else {
        100.0 * received_bytes as f64 / expected_size as f64
    };

    TransferProgressPayload {
        job_id: job_id.to_owned(),
        transfer_id: job_id.to_owned(),
        video_id,
        filename: filename.to_owned(),
        received_bytes,
        expected_bytes: Some(expected_size),
        percent,
        bytes_per_second,
        eta,
        chunk_index: 0,
        total_chunks: 0,
    }
}

pub async fn worker(config: WorkerConfig) -> Result<()> {
    if config.once {
        let session = run_worker_session(&config).await?;
        println!(
            "connected worker {} via {} and received {}",
            session.assigned_worker_id, session.negotiated_protocol_version, session.work_status
        );
        return Ok(());
    }

    run_worker_until(&config, WorkerRuntime::default()).await?;
    Ok(())
}

async fn run_worker_until(config: &WorkerConfig, runtime: WorkerRuntime) -> Result<()> {
    let mut completed_pulls = 0usize;
    let mut control_state = WorkerControlState::Running;
    let mut reconnect_backoff =
        ReconnectBackoff::new(runtime.reconnect_base_delay, runtime.reconnect_max_delay);

    loop {
        match run_connected_worker(config, runtime, &mut completed_pulls, &mut control_state).await
        {
            Ok(()) => {
                reconnect_backoff.reset();
                return Ok(());
            }
            Err(error) => {
                eprintln!("worker connection lost: {error:#}");
                tokio::time::sleep(reconnect_backoff.next_delay()).await;
            }
        }
    }
}

async fn request_input_resend(
    worker: &mut ConnectedWorker,
    job: &WorkerJob,
    local_path: Option<&Path>,
) -> Result<WorkerJob> {
    let reason = format!(
        "worker input is missing at {}; worker cannot resume job_in_progress without local file",
        job.input_path().display()
    );
    debug!(
        job_id = %job.assignment.job_id,
        input = %job.input_path().display(),
        reason = %reason,
        "reporting retriable transfer failure"
    );
    worker
        .send_event(ClientEvent::TransferFailure(TransferFailurePayload {
            job_id: job.assignment.job_id.clone(),
            stage: TransferStage::ReceiveChunk,
            retriable: true,
            reason,
        }))
        .await?;

    debug!(
        job_id = %job.assignment.job_id,
        "requesting input resend for active job"
    );
    let mut pull = PullWorkPayload::input_missing();
    pull.job_type =
        (job.assignment.job_type != JobKind::CrfSearch).then_some(job.assignment.job_type);
    let resend = worker.request_work_with(pull).await?;

    let ServerReply::JobAssigned(assignment) = resend else {
        bail!(
            "server returned no_work after input_missing for job {}",
            job.assignment.job_id
        );
    };

    if assignment.job_id != job.assignment.job_id {
        bail!(
            "server reassigned job {} after input_missing for job {}",
            assignment.job_id,
            job.assignment.job_id
        );
    }
    if assignment.status != WorkStatus::JobAssigned {
        bail!(
            "server kept job {} in {} after input_missing; refusing to wait without transfer",
            assignment.job_id,
            assignment.status.as_str()
        );
    }

    build_worker_job(assignment, local_path)
}

async fn request_pending_input(
    worker: &mut ConnectedWorker,
    job: &WorkerJob,
    local_path: Option<&Path>,
) -> Result<PendingJob> {
    let resend_job = request_input_resend(worker, job, local_path).await?;
    Ok(PendingJob::waiting(resend_job))
}

async fn download_or_wait_for_input(
    worker: &mut ConnectedWorker,
    job: &WorkerJob,
    local_path: Option<&Path>,
) -> Result<Option<PendingJob>> {
    match download_worker_input(worker, job).await {
        Ok(true) => return Ok(None),
        Ok(false) => {}
        Err(error) => debug!(
            job_id = %job.assignment.job_id,
            error = %error,
            "HTTP worker input download failed, falling back to websocket transfer"
        ),
    }

    Ok(Some(request_pending_input(worker, job, local_path).await?))
}

async fn run_connected_worker(
    config: &WorkerConfig,
    runtime: WorkerRuntime,
    completed_pulls: &mut usize,
    control_state: &mut WorkerControlState,
) -> Result<()> {
    if config.worker_mode != WorkerMode::CrfSearch {
        return run_multiplexed_worker(config, runtime, completed_pulls).await;
    }

    debug!(
        connect = %config.connect,
        worker_id = %config.worker_id,
        once = config.once,
        local_path = ?config.local_path,
        "connecting worker"
    );
    let mut worker = Some(ConnectedWorker::connect(config).await?);
    let mut pending_job: Option<PendingJob> = None;
    let mut requested_job_type = initial_pull_work_payload(config.worker_mode).job_type;

    if *control_state != WorkerControlState::Running {
        let reported_state = match control_state {
            WorkerControlState::Paused => ControlState::Paused,
            WorkerControlState::Stopped => ControlState::Stopped,
            WorkerControlState::Running => unreachable!(),
        };
        worker
            .as_mut()
            .expect("connected worker")
            .send_control_state(reported_state, None)
            .await?;
    }

    loop {
        if *control_state != WorkerControlState::Running {
            let stopped = worker
                .as_mut()
                .expect("connected worker")
                .wait_until_running(control_state, runtime.idle_delay)
                .await?;
            if stopped {
                remove_pending_worker_input(&mut pending_job)?;
            }
        }

        if pending_job.is_some() {
            let next = {
                let job = pending_job.as_mut().expect("pending job");
                trace!(
                    job_id = %job.job.assignment.job_id,
                    input = %job.input_path().display(),
                    "waiting for pending job input"
                );
                worker
                    .as_mut()
                    .expect("connected worker")
                    .wait_for_pending_job(job, runtime.idle_delay)
                    .await?
            };

            match next {
                PendingJobOutcome::Waiting => {
                    debug!(
                        job_id = %pending_job.as_ref().expect("pending job").job.assignment.job_id,
                        received_bytes = pending_job
                            .as_ref()
                            .and_then(|job| job.receiver.as_ref().map(ChunkReceiver::received_bytes))
                            .unwrap_or_default(),
                        "pending job still waiting"
                    );
                    continue;
                }
                PendingJobOutcome::Canceled => {
                    if let Some(job) = pending_job.as_ref() {
                        debug!(job_id = %job.job.assignment.job_id, "pending job canceled");
                    }
                    remove_pending_worker_input(&mut pending_job)?;
                    continue;
                }
                PendingJobOutcome::Paused => {
                    *control_state = WorkerControlState::Paused;
                    continue;
                }
                PendingJobOutcome::Stopped => {
                    remove_pending_worker_input(&mut pending_job)?;
                    *control_state = WorkerControlState::Stopped;
                    continue;
                }
                PendingJobOutcome::Ready => {
                    let job = pending_job.take().expect("pending job");
                    debug!(
                        job_id = %job.job.assignment.job_id,
                        input = %job.input_path().display(),
                        "pending job input arrived"
                    );
                    if run_worker_job_and_publish(config, &mut worker, &job.job).await?
                        == WorkerJobOutcome::Stopped
                    {
                        *control_state = WorkerControlState::Stopped;
                    }
                    continue;
                }
            }
        }

        debug!("requesting work");
        let worker_ref = worker.as_mut().expect("connected worker");
        let work_status = match requested_job_type {
            Some(job_type) => worker_ref.request_work_kind(job_type).await?,
            None => worker_ref.request_work().await?,
        };
        if let Some(control) = worker_ref.take_pending_control() {
            match control {
                ControlAction::Stop => {
                    worker_ref
                        .send_control_state(ControlState::Stopped, None)
                        .await?;
                    *control_state = WorkerControlState::Stopped;
                    continue;
                }
                ControlAction::Pause => {
                    worker_ref
                        .send_control_state(ControlState::Paused, None)
                        .await?;
                    *control_state = WorkerControlState::Paused;
                    continue;
                }
                ControlAction::Resume | ControlAction::Start => {
                    worker_ref
                        .send_control_state(ControlState::Running, None)
                        .await?;
                }
            }
        }
        requested_job_type = match &work_status {
            ServerReply::NoWork(_) => requested_job_type
                .and_then(|kind| next_job_type_after_no_work(config.worker_mode, kind)),
            ServerReply::JobAssigned(_) => initial_pull_work_payload(config.worker_mode).job_type,
        };
        *completed_pulls += 1;
        let status = work_status_label(&work_status);
        println!(
            "connected worker {} via {} and received {}",
            worker_ref.assigned_worker_id, worker_ref.negotiated_protocol_version, status
        );

        if let ServerReply::JobAssigned(assignment) = work_status {
            let job = build_worker_job(assignment, config.local_path.as_deref())?;
            debug!(
                job_id = %job.assignment.job_id,
                status = %job.assignment.status.as_str(),
                input = %job.input_path().display(),
                already_present = job.input_path().exists(),
                pending_transfer = job.assignment.status == WorkStatus::JobAssigned
                    && !job.input_path().exists()
                    && config.local_path.is_none(),
                "job assigned"
            );
            if let Some(current_worker) = worker.as_mut() {
                current_worker
                    .send_event(ClientEvent::Heartbeat(heartbeat_payload(
                        &job.input_dir,
                        Some(job.assignment.video_id),
                    )))
                    .await?;
            }
            let phase = worker_job_phase(&job, config.local_path.as_deref())?;
            match phase {
                WorkerJobPhase::InputReady | WorkerJobPhase::CrfSearching => {
                    debug!(
                        job_id = %job.assignment.job_id,
                        input = %job.input_path().display(),
                        phase = ?phase,
                        "input already present, starting job"
                    );
                    if run_worker_job_and_publish(config, &mut worker, &job).await?
                        == WorkerJobOutcome::Stopped
                    {
                        *control_state = WorkerControlState::Stopped;
                    }
                }
                WorkerJobPhase::AwaitingInput(delivery) => {
                    let pending = match delivery {
                        InputDelivery::Http => {
                            match download_or_wait_for_input(
                                worker.as_mut().expect("connected worker"),
                                &job,
                                config.local_path.as_deref(),
                            )
                            .await?
                            {
                                None => {
                                    debug!(
                                        job_id = %job.assignment.job_id,
                                        input = %job.input_path().display(),
                                        "downloaded worker input over HTTP, starting job"
                                    );
                                    if run_worker_job_and_publish(config, &mut worker, &job).await?
                                        == WorkerJobOutcome::Stopped
                                    {
                                        *control_state = WorkerControlState::Stopped;
                                    }
                                    None
                                }
                                Some(pending) => Some(pending),
                            }
                        }
                        InputDelivery::Resend => Some(
                            request_pending_input(
                                worker.as_mut().expect("connected worker"),
                                &job,
                                config.local_path.as_deref(),
                            )
                            .await?,
                        ),
                        InputDelivery::Websocket => Some(PendingJob::waiting(job)),
                    };

                    if let Some(pending) = pending {
                        debug!(
                            job_id = %pending.job.assignment.job_id,
                            input = %pending.input_path().display(),
                            temp_dir = %pending.job.input_dir.display(),
                            phase = ?phase,
                            receiver_ready = false,
                            pending_job = true,
                            "waiting for worker input"
                        );
                        pending_job = Some(pending);
                    }
                }
            }
            continue;
        }

        if runtime.max_pulls == Some(*completed_pulls) {
            return Ok(());
        }

        tokio::time::sleep(runtime.idle_delay).await;
    }
}

async fn run_multiplexed_worker(
    config: &WorkerConfig,
    runtime: WorkerRuntime,
    completed_pulls: &mut usize,
) -> Result<()> {
    let connected = ConnectedWorker::connect(config).await?;
    let mut connection = Some(MultiplexedWorker::from_connected(connected));
    let capacity = WorkerCapacity::detect(config.worker_mode);
    let (output, mut outputs) = mpsc::unbounded_channel();
    let mut jobs: HashMap<String, MultiplexJob> = HashMap::new();
    let mut pending: HashMap<String, (JobKind, Option<String>)> = HashMap::new();
    let mut no_work = HashMap::new();
    let mut reconnect = tokio::time::interval(runtime.reconnect_base_delay);
    reconnect.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        if let Some(worker) = connection.as_mut() {
            schedule_multiplex_pulls(
                worker,
                config.worker_mode,
                capacity.max_active_jobs(),
                &jobs,
                &mut pending,
                &no_work,
                &mut *completed_pulls,
                runtime.max_pulls,
            )
            .await?;
        }

        let mut connection_lost = false;
        if let Some(worker) = connection.as_mut() {
            tokio::select! {
                item = outputs.recv() => {
                    connection_lost = !handle_multiplex_output(
                        worker,
                        item,
                        &mut jobs,
                        &mut pending,
                    ).await?;
                }
                frame = worker.reader.next() => {
                    connection_lost = !handle_multiplex_frame(
                        worker,
                        frame,
                        &mut jobs,
                        &mut pending,
                        &mut no_work,
                        &output,
                        completed_pulls,
                        config.local_path.as_deref(),
                    ).await?;
                }
                _ = reconnect.tick() => {
                    no_work.clear();
                }
            }
        } else {
            tokio::select! {
                item = outputs.recv() => {
                    let Some(item) = item else {
                        bail!("multiplexed worker output channel closed");
                    };
                    handle_multiplex_output_offline(item, &mut jobs, &mut pending)?;
                }
                _ = reconnect.tick() => {
                    match ConnectedWorker::connect(config).await {
                        Ok(candidate) => {
                            let mut candidate = MultiplexedWorker::from_connected(candidate);
                            if replay_multiplex_jobs(&mut candidate, &jobs).await {
                                connection = Some(candidate);
                                jobs.retain(|_, job| !job.finished);
                                reconnect = tokio::time::interval(runtime.reconnect_base_delay);
                                reconnect.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                            }
                        }
                        Err(error) => {
                            trace!(error = %error, "multiplexed worker reconnect attempt failed");
                        }
                    }
                }
            }
        }

        if connection_lost {
            connection = None;
            pending.clear();
        }

        if runtime.max_pulls.is_some_and(|max| *completed_pulls >= max) && jobs.is_empty() {
            return Ok(());
        }
    }
}

fn handle_multiplex_output_offline(
    item: MultiplexOutput,
    jobs: &mut HashMap<String, MultiplexJob>,
    pending: &mut HashMap<String, (JobKind, Option<String>)>,
) -> Result<()> {
    match item {
        MultiplexOutput::Event { job_id, event, .. } => {
            if let Some(job) = jobs.get_mut(&job_id) {
                record_multiplex_event(&mut job.state, &event);
            }
        }
        MultiplexOutput::Done { job_id, result } => {
            if let Some(job) = jobs.get_mut(&job_id) {
                job.finished = true;
                if let Err(error) = result {
                    job.state.failure = Some(job.job.failure_payload(&anyhow!(error)));
                }
            }
        }
        MultiplexOutput::RequestInputResend { .. } => {}
    }
    pending.retain(|_, (_, resend)| {
        resend
            .as_deref()
            .is_none_or(|job_id| jobs.contains_key(job_id))
    });
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn schedule_multiplex_pulls(
    worker: &mut MultiplexedWorker,
    mode: WorkerMode,
    max_active_jobs: usize,
    jobs: &HashMap<String, MultiplexJob>,
    pending: &mut HashMap<String, (JobKind, Option<String>)>,
    no_work: &HashMap<JobKind, bool>,
    completed_pulls: &mut usize,
    max_pulls: Option<usize>,
) -> Result<()> {
    while jobs.len() + pending.len() < max_active_jobs
        && max_pulls.is_none_or(|max| *completed_pulls + pending.len() < max)
    {
        let Some(job_type) = next_multiplex_job_type(mode, jobs, pending, no_work) else {
            break;
        };
        let reference = worker
            .send_pull(PullWorkPayload {
                input_missing: false,
                job_type: Some(job_type),
            })
            .await?;
        pending.insert(reference, (job_type, None));
    }
    Ok(())
}

fn next_multiplex_job_type(
    mode: WorkerMode,
    jobs: &HashMap<String, MultiplexJob>,
    pending: &HashMap<String, (JobKind, Option<String>)>,
    no_work: &HashMap<JobKind, bool>,
) -> Option<JobKind> {
    let occupied = |kind| {
        jobs.values().any(|job| job.job.assignment.job_type == kind)
            || pending.values().any(|(job_type, _)| *job_type == kind)
    };
    let allowed = match mode {
        WorkerMode::CrfSearch => [Some(JobKind::CrfSearch), None],
        WorkerMode::Encode => [Some(JobKind::Encode), None],
        WorkerMode::Both => [Some(JobKind::CrfSearch), Some(JobKind::Encode)],
    };
    allowed
        .into_iter()
        .flatten()
        .find(|kind| !occupied(*kind) && !no_work.get(kind).copied().unwrap_or(false))
}

async fn handle_multiplex_output(
    worker: &mut MultiplexedWorker,
    item: Option<MultiplexOutput>,
    jobs: &mut HashMap<String, MultiplexJob>,
    pending: &mut HashMap<String, (JobKind, Option<String>)>,
) -> Result<bool> {
    let Some(item) = item else {
        bail!("multiplexed worker output channel closed");
    };
    match item {
        MultiplexOutput::Event {
            job_id,
            event,
            name,
        } => {
            if let Some(job) = jobs.get_mut(&job_id) {
                record_multiplex_event(&mut job.state, &event);
            }
            Ok(send_multiplex_event(worker, event, &job_id, name).await)
        }
        MultiplexOutput::Done { job_id, result } => {
            let Some(job) = jobs.get_mut(&job_id) else {
                return Ok(true);
            };
            job.finished = true;
            if let Err(error) = result {
                let failure = job.job.failure_payload(&anyhow!(error));
                job.state.failure = Some(failure.clone());
                if !send_multiplex_event(
                    worker,
                    ClientEvent::VideoFailed(failure),
                    &job_id,
                    "video_failed",
                )
                .await
                {
                    return Ok(false);
                }
            }
            jobs.remove(&job_id);
            pending.retain(|_, (_, resend)| resend.as_deref() != Some(&job_id));
            Ok(true)
        }
        MultiplexOutput::RequestInputResend { job_id } => {
            let Some(job) = jobs.get(&job_id) else {
                return Ok(true);
            };
            send_multiplex_event(
                worker,
                ClientEvent::TransferFailure(TransferFailurePayload {
                    job_id: job_id.clone(),
                    stage: TransferStage::ReceiveChunk,
                    retriable: true,
                    reason: format!(
                        "worker input is missing at {}",
                        job.job.input_path().display()
                    ),
                }),
                &job_id,
                "transfer_failed",
            )
            .await;
            let reference = worker
                .send_pull(PullWorkPayload {
                    input_missing: true,
                    job_type: Some(job.job.assignment.job_type),
                })
                .await?;
            pending.insert(reference, (job.job.assignment.job_type, Some(job_id)));
            Ok(true)
        }
    }
}

async fn send_multiplex_event(
    worker: &mut MultiplexedWorker,
    event: ClientEvent,
    job_id: &str,
    name: &'static str,
) -> bool {
    if let Err(error) = worker.send_event(event).await {
        debug!(job_id = %job_id, event = name, error = %error, "multiplexed worker send failed");
        false
    } else {
        true
    }
}

fn record_multiplex_event(state: &mut WorkerJobReportState, event: &ClientEvent) {
    match event {
        ClientEvent::Heartbeat(payload) => state.heartbeat = Some(payload.clone()),
        ClientEvent::TransferProgress(payload) => state.transfer_progress = Some(payload.clone()),
        ClientEvent::CrfSearchProgress(payload) => state.crf_progress = Some(payload.clone()),
        ClientEvent::CrfSearchResult(payload) => state.crf_results.push(payload.clone()),
        ClientEvent::CrfSearchCompleted(payload) => state.crf_completed = Some(payload.clone()),
        ClientEvent::EncodeProgress(payload) => state.encode_progress = Some(payload.clone()),
        ClientEvent::EncodeCompleted(payload) => state.encode_completed = Some(payload.clone()),
        ClientEvent::VideoFailed(payload) => state.failure = Some(payload.clone()),
        ClientEvent::Join
        | ClientEvent::Announce(_)
        | ClientEvent::PullWork(_)
        | ClientEvent::ControlState(_)
        | ClientEvent::TransferFailure(_) => {}
    }
}

#[allow(clippy::too_many_arguments)]
async fn handle_multiplex_frame(
    worker: &mut MultiplexedWorker,
    frame: Option<std::result::Result<Message, WsError>>,
    jobs: &mut HashMap<String, MultiplexJob>,
    pending: &mut HashMap<String, (JobKind, Option<String>)>,
    no_work: &mut HashMap<JobKind, bool>,
    output: &UnboundedSender<MultiplexOutput>,
    completed_pulls: &mut usize,
    local_path: Option<&Path>,
) -> Result<bool> {
    let Some(frame) = frame else {
        return Ok(false);
    };
    let frame = match frame {
        Ok(frame) => frame,
        Err(error) => {
            debug!(error = %error, "multiplexed worker websocket read failed");
            return Ok(false);
        }
    };
    match frame {
        Message::Ping(payload) => worker.send_pong(payload).await.map(|()| true),
        Message::Pong(_) | Message::Frame(_) => Ok(true),
        Message::Close(frame) => {
            debug!(?frame, "multiplexed worker websocket closed");
            Ok(false)
        }
        Message::Binary(bytes) => {
            if let Some(chunk) = decode_binary_worker_push(&bytes)?
                && let Some(job) = jobs.get(&chunk.transfer_id)
            {
                job.command
                    .send(JobCommand::TransferChunk(chunk))
                    .map_err(|_| anyhow!("worker input command channel closed"))?;
            }
            Ok(true)
        }
        Message::Text(text) => {
            let mut reply = None;
            for reference in pending.keys() {
                if let Some(decoded) =
                    decode_expected_reply::<ServerReply>(&text, reference, "pull_work")?
                {
                    reply = Some((reference.clone(), decoded?));
                    break;
                }
            }
            if let Some((reference, response)) = reply {
                *completed_pulls += 1;
                let (job_type, resend_for) = pending
                    .remove(&reference)
                    .expect("pending pull reference still present");
                *no_work.entry(job_type).or_insert(false) =
                    matches!(response, ServerReply::NoWork(_));
                match response {
                    ServerReply::NoWork(_) => {}
                    ServerReply::JobAssigned(assignment) => {
                        *no_work.entry(job_type).or_insert(false) = false;
                        if let Some(job_id) = resend_for {
                            if let Some(job) = jobs.get(&job_id)
                                && assignment.transfer.is_some()
                            {
                                job.command
                                    .send(JobCommand::TransferStarted(TransferStartedPayload {
                                        chunk_size_bytes: assignment.chunk_size_bytes,
                                        size_bytes: assignment.size_bytes,
                                        source_name: assignment.source_name,
                                        status: assignment.status.as_str().into(),
                                        total_bytes: assignment.size_bytes,
                                        total_chunks: assignment
                                            .size_bytes
                                            .div_ceil(assignment.chunk_size_bytes.max(1)),
                                        transfer_id: job_id,
                                        video_id: assignment.video_id,
                                    }))
                                    .map_err(|_| anyhow!("worker input command channel closed"))?;
                            }
                        } else {
                            let job = build_worker_job(assignment, local_path)?;
                            let job_id = job.assignment.job_id.clone();
                            let (command, commands) = mpsc::unbounded_channel();
                            let _ = multiplex_event(
                                output,
                                &job_id,
                                ClientEvent::Heartbeat(heartbeat_payload(
                                    &job.input_dir,
                                    Some(job.assignment.video_id),
                                )),
                                "heartbeat",
                            );
                            tokio::task::spawn_local(run_multiplex_job(
                                job.clone(),
                                commands,
                                output.clone(),
                            ));
                            jobs.insert(
                                job_id,
                                MultiplexJob {
                                    job,
                                    command,
                                    state: WorkerJobReportState::default(),
                                    finished: false,
                                },
                            );
                        }
                    }
                }
                return Ok(true);
            }

            match decode_worker_push(&text)? {
                Some(WorkerPush::Cancel(cancel)) => {
                    if let Some(job) = jobs.get(&cancel.job_id) {
                        job.command
                            .send(JobCommand::Cancel(cancel))
                            .map_err(|_| anyhow!("worker input command channel closed"))?;
                    }
                }
                Some(WorkerPush::Control(control)) => {
                    let jobs: Vec<_> = jobs
                        .values()
                        .filter(|job| {
                            control_targets_job(
                                &control,
                                &job.job.assignment.job_id,
                                job.job.assignment.video_id,
                                job.finished,
                            )
                        })
                        .collect();

                    for job in jobs {
                        job.command
                            .send(JobCommand::Control(control.clone()))
                            .map_err(|_| anyhow!("worker control channel closed"))?;
                    }
                }
                Some(WorkerPush::Started(started)) => {
                    if let Some(job) = jobs.get(&started.transfer_id) {
                        job.command
                            .send(JobCommand::TransferStarted(started))
                            .map_err(|_| anyhow!("worker input command channel closed"))?;
                    }
                }
                None => {}
            }
            Ok(true)
        }
    }
}

async fn replay_multiplex_jobs(
    worker: &mut MultiplexedWorker,
    jobs: &HashMap<String, MultiplexJob>,
) -> bool {
    for (job_id, job) in jobs {
        if let Some(progress) = encode_reconnect_progress(&job.job, &job.state, job.finished)
            && !send_multiplex_event(
                worker,
                ClientEvent::EncodeProgress(progress),
                job_id,
                "encode_progress",
            )
            .await
        {
            return false;
        }
        if !replay_multiplex_state(worker, &job.state, job_id).await {
            return false;
        }
    }
    true
}

fn encode_reconnect_progress(
    job: &WorkerJob,
    state: &WorkerJobReportState,
    finished: bool,
) -> Option<EncodeProgressPayload> {
    (!finished
        && job.assignment.job_type == JobKind::Encode
        && state.encode_progress.is_none()
        && state.encode_completed.is_none()
        && state.failure.is_none())
    .then(|| EncodeProgressPayload {
        job_id: job.assignment.job_id.clone(),
        video_id: job.assignment.video_id,
        percent: 0.0,
        fps: 0.0,
        eta: None,
        output_bytes: 0,
        output_percent: 0.0,
        throughput: None,
    })
}

async fn replay_multiplex_state(
    worker: &mut MultiplexedWorker,
    state: &WorkerJobReportState,
    job_id: &str,
) -> bool {
    if let Some(payload) = &state.heartbeat
        && !send_multiplex_event(
            worker,
            ClientEvent::Heartbeat(payload.clone()),
            job_id,
            "heartbeat",
        )
        .await
    {
        return false;
    }
    if let Some(payload) = &state.transfer_progress
        && !send_multiplex_event(
            worker,
            ClientEvent::TransferProgress(payload.clone()),
            job_id,
            "transfer_progress",
        )
        .await
    {
        return false;
    }
    if let Some(payload) = &state.crf_progress
        && !send_multiplex_event(
            worker,
            ClientEvent::CrfSearchProgress(payload.clone()),
            job_id,
            "crf_progress",
        )
        .await
    {
        return false;
    }
    for payload in &state.crf_results {
        if !send_multiplex_event(
            worker,
            ClientEvent::CrfSearchResult(payload.clone()),
            job_id,
            "crf_result",
        )
        .await
        {
            return false;
        }
    }
    if let Some(payload) = &state.crf_completed
        && !send_multiplex_event(
            worker,
            ClientEvent::CrfSearchCompleted(payload.clone()),
            job_id,
            "crf_search_completed",
        )
        .await
    {
        return false;
    }
    if let Some(payload) = &state.encode_progress
        && !send_multiplex_event(
            worker,
            ClientEvent::EncodeProgress(payload.clone()),
            job_id,
            "encode_progress",
        )
        .await
    {
        return false;
    }
    if let Some(payload) = &state.encode_completed
        && !send_multiplex_event(
            worker,
            ClientEvent::EncodeCompleted(payload.clone()),
            job_id,
            "encode_completed",
        )
        .await
    {
        return false;
    }
    if let Some(payload) = &state.failure
        && !send_multiplex_event(
            worker,
            ClientEvent::VideoFailed(payload.clone()),
            job_id,
            "video_failed",
        )
        .await
    {
        return false;
    }
    true
}

async fn run_worker_session(config: &WorkerConfig) -> Result<WorkerSession> {
    let mut worker = ConnectedWorker::connect(config).await?;
    let pull_work = match initial_pull_work_payload(config.worker_mode).job_type {
        Some(job_type) => worker.request_work_kind(job_type).await?,
        None => worker.request_work().await?,
    };
    let work_status = work_status_label(&pull_work);
    let assigned_job = match pull_work {
        ServerReply::JobAssigned(payload) => Some(payload),
        ServerReply::NoWork(_) => None,
    };

    Ok(WorkerSession {
        assigned_worker_id: worker.assigned_worker_id,
        negotiated_protocol_version: worker.negotiated_protocol_version,
        work_status,
        assigned_job,
    })
}

fn work_status_label(reply: &ServerReply) -> String {
    match reply {
        ServerReply::NoWork(payload) => payload.status.as_str().into(),
        ServerReply::JobAssigned(payload) => {
            format!("{} (job_id={})", payload.status.as_str(), payload.job_id)
        }
    }
}

fn control_targets_job(
    control: &ControlPayload,
    job_id: &str,
    video_id: u64,
    finished: bool,
) -> bool {
    !finished
        && match (&control.job_id, control.video_id) {
            (None, None) => true,
            (Some(target_job_id), _) => target_job_id == job_id,
            (None, Some(target_video_id)) => target_video_id == video_id,
        }
}

fn decode_worker_push(text: &str) -> Result<Option<WorkerPush>> {
    let frame: ServerPushFrame<Value> = match serde_json::from_str(text) {
        Ok(frame) => frame,
        Err(_) => return Ok(None),
    };
    if frame.2 != CRF_SEARCH_TOPIC {
        return Ok(None);
    }
    if frame.3 == "phx_reply" {
        return Ok(None);
    }

    let payload = frame.4.clone();
    debug!(
        topic = %frame.2,
        event = %frame.3,
        payload_bytes = text.len(),
        "received worker push"
    );
    let push = match frame.3.as_str() {
        "cancel" => WorkerPush::Cancel(
            serde_json::from_value::<CancelPayload>(payload.clone())
                .context("decode cancel push")?,
        ),
        "control" => WorkerPush::Control(
            serde_json::from_value::<ControlPayload>(payload.clone())
                .context("decode control push")?,
        ),
        "transfer_started" => WorkerPush::Started(
            serde_json::from_value::<TransferStartedPayload>(payload.clone())
                .with_context(|| format!("decode transfer started push event={}", frame.3))?,
        ),
        _ => return Ok(None),
    };

    Ok(Some(push))
}

fn decode_binary_transfer_chunk(bytes: &[u8]) -> Result<TransferChunk> {
    if bytes.len() < TRANSFER_CHUNK_HEADER_LEN {
        bail!(
            "binary transfer chunk too short: got {} bytes, need at least {}",
            bytes.len(),
            TRANSFER_CHUNK_HEADER_LEN
        );
    }
    if &bytes[0..4] != TRANSFER_CHUNK_MAGIC {
        bail!(
            "invalid binary transfer chunk magic: len={} prefix={}",
            bytes.len(),
            hex_prefix(bytes, 24)
        );
    }
    if bytes[4] != TRANSFER_CHUNK_VERSION {
        bail!("unsupported binary transfer chunk version {}", bytes[4]);
    }
    if bytes[5] != TRANSFER_CHUNK_TYPE {
        bail!("unsupported binary transfer chunk type {}", bytes[5]);
    }

    let transfer_id_size = u16::from_be_bytes([bytes[6], bytes[7]]) as usize;
    let transfer_id_start = TRANSFER_CHUNK_HEADER_LEN;
    let data_start = transfer_id_start
        .checked_add(transfer_id_size)
        .context("binary transfer chunk transfer_id_size overflow")?;
    if bytes.len() < data_start {
        bail!(
            "binary transfer chunk transfer_id truncated: got {} bytes, need {}",
            bytes.len(),
            data_start
        );
    }

    let transfer_id = std::str::from_utf8(&bytes[transfer_id_start..data_start])
        .context("decode binary transfer chunk transfer_id")?
        .to_owned();

    Ok(TransferChunk {
        transfer_id,
        video_id: read_u64(bytes, 8),
        chunk_index: read_u64(bytes, 16),
        total_chunks: read_u64(bytes, 24),
        bytes_sent: read_u64(bytes, 32),
        total_bytes: read_u64(bytes, 40),
        crc32: read_u32(bytes, 48) as u64,
        bytes: bytes[data_start..].to_vec(),
    })
}

fn decode_binary_worker_push(bytes: &[u8]) -> Result<Option<TransferChunk>> {
    let Some((topic, event, payload)) = decode_phoenix_binary_frame(bytes)? else {
        return Ok(None);
    };
    if topic != CRF_SEARCH_TOPIC || event != "transfer_chunk" {
        return Ok(None);
    }
    decode_binary_transfer_chunk(payload).map(Some)
}

fn decode_phoenix_binary_frame(bytes: &[u8]) -> Result<Option<(&str, &str, &[u8])>> {
    if bytes.len() < 4 {
        return Ok(None);
    }

    let join_ref_size = bytes[0] as usize;
    let ref_size = bytes[1] as usize;
    let topic_size = bytes[2] as usize;
    let event_size = bytes[3] as usize;
    let join_ref_start = 4usize;
    let ref_start = join_ref_start
        .checked_add(join_ref_size)
        .context("phoenix binary join_ref_size overflow")?;
    let topic_start = ref_start
        .checked_add(ref_size)
        .context("phoenix binary ref_size overflow")?;
    let event_start = topic_start
        .checked_add(topic_size)
        .context("phoenix binary topic_size overflow")?;
    let payload_start = event_start
        .checked_add(event_size)
        .context("phoenix binary event_size overflow")?;
    if bytes.len() < payload_start {
        bail!(
            "phoenix binary frame truncated: len={} header={}",
            bytes.len(),
            hex_prefix(bytes, 8)
        );
    }

    let topic = std::str::from_utf8(&bytes[topic_start..event_start])
        .context("decode phoenix binary topic")?;
    let event = std::str::from_utf8(&bytes[event_start..payload_start])
        .context("decode phoenix binary event")?;
    Ok(Some((topic, event, &bytes[payload_start..])))
}

fn read_u64(bytes: &[u8], offset: usize) -> u64 {
    u64::from_be_bytes(
        bytes[offset..offset + 8]
            .try_into()
            .expect("read_u64 offset validated by fixed header length"),
    )
}

fn read_u32(bytes: &[u8], offset: usize) -> u32 {
    u32::from_be_bytes(
        bytes[offset..offset + 4]
            .try_into()
            .expect("read_u32 offset validated by fixed header length"),
    )
}

fn hex_prefix(bytes: &[u8], max_len: usize) -> String {
    bytes
        .iter()
        .take(max_len)
        .map(|byte| format!("{byte:02x}"))
        .collect::<Vec<_>>()
        .join(" ")
}

fn websocket_connect_error(request_url: &str, error: WsError) -> anyhow::Error {
    match error {
        WsError::Http(response) => {
            let status = response.status();
            let body = response
                .body()
                .as_ref()
                .map(|bytes| String::from_utf8_lossy(bytes).into_owned())
                .unwrap_or_default();
            anyhow!(
                "connect websocket {request_url}: HTTP {status} {}",
                body.trim()
            )
        }
        other => anyhow!("connect websocket {request_url}: {other}"),
    }
}

fn worker_websocket_url(base_url: &str, token: &str) -> Result<String> {
    let base_url = base_url.trim_end_matches('/');
    let scheme = match () {
        _ if base_url.starts_with("http://") => "ws://",
        _ if base_url.starts_with("https://") => "wss://",
        _ if base_url.starts_with("ws://") => "ws://",
        _ if base_url.starts_with("wss://") => "wss://",
        _ => bail!("unsupported websocket base URL: {base_url}"),
    };
    let rest = base_url
        .split_once("://")
        .map(|(_, rest)| rest)
        .ok_or_else(|| anyhow!("missing scheme in websocket base URL: {base_url}"))?;
    Ok(format!(
        "{scheme}{rest}/workers/socket/websocket?token={token}&vsn={PHOENIX_VSN}"
    ))
}

fn worker_websocket_config() -> WebSocketConfig {
    WebSocketConfig {
        max_message_size: Some(MAX_TRANSFER_FRAME_BYTES),
        max_frame_size: Some(MAX_TRANSFER_FRAME_BYTES),
        ..WebSocketConfig::default()
    }
}

async fn send_json<W, T>(writer: &mut W, value: T) -> Result<()>
where
    W: SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
    T: serde::Serialize,
{
    writer
        .send(Message::Text(
            serde_json::to_string(&value).context("encode websocket message")?,
        ))
        .await
        .context("send websocket message")
}

async fn expect_reply<T, R>(reader: &mut R, expected_ref: &str, expected_event: &str) -> Result<T>
where
    R: StreamExt<Item = std::result::Result<Message, tokio_tungstenite::tungstenite::Error>>
        + Unpin,
    T: for<'de> Deserialize<'de>,
{
    while let Some(message) = reader.next().await {
        match message.context("read websocket message")? {
            Message::Text(text) => {
                if let Some(reply) = decode_expected_reply(&text, expected_ref, expected_event)? {
                    return reply;
                }
            }
            Message::Close(frame) => {
                bail!("websocket closed before {expected_event} reply: {frame:?}")
            }
            Message::Ping(_) | Message::Pong(_) | Message::Binary(_) | Message::Frame(_) => {
                continue;
            }
        }
    }

    bail!("websocket ended before {expected_event} reply")
}

fn decode_expected_reply<T>(
    text: &str,
    expected_ref: &str,
    expected_event: &str,
) -> Result<Option<Result<T>>>
where
    T: for<'de> Deserialize<'de>,
{
    let ServerPushFrame(_, msg_ref, topic, event, body): ServerPushFrame<Value> =
        serde_json::from_str(text).context("decode phoenix frame")?;
    if topic != CRF_SEARCH_TOPIC || event != "phx_reply" || msg_ref.as_deref() != Some(expected_ref)
    {
        return Ok(None);
    }
    let ReplyBody { status, response }: ReplyBody<Value> =
        serde_json::from_value(body).context("decode phoenix reply body")?;
    Ok(Some(match status.as_str() {
        "ok" => serde_json::from_value(response.clone())
            .map_err(|error| anyhow!("decode phoenix ok reply: {error}; raw_response={response}")),
        "error" => {
            let error: ErrorReplyPayload =
                serde_json::from_value(response).context("decode phoenix error reply")?;
            let supported_versions = if error.supported_protocol_versions.is_empty() {
                String::new()
            } else {
                format!(
                    " (supported_protocol_versions={:?})",
                    error.supported_protocol_versions
                )
            };
            Err(anyhow!(
                "{expected_event} failed: {}{}",
                error.reason,
                supported_versions
            ))
        }
        other => Err(anyhow!("unexpected phoenix status {other}")),
    }))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::worker_protocol::{
        CancelPayload, ErrorReplyPayload, JobAssignedPayload, ReplyBody, ServerFrame,
        ServerPushFrame, TransferAuth, TransferSpec, WorkStatus,
    };
    use crate::{command::crf_search::test_hooks as crf_test_hooks, ffprobe::Ffprobe};
    use anyhow::Result;
    use serde_json::{Value, json};
    use std::{
        io::{Read, Write},
        sync::Arc,
    };
    use tokio::net::TcpListener;
    use tokio_tungstenite::{accept_async, tungstenite::Message};

    #[derive(Clone, Copy)]
    struct WorkerTestConfig {
        once: bool,
        protocol_version: u64,
    }

    impl WorkerTestConfig {
        fn continuous() -> Self {
            Self {
                once: false,
                protocol_version: 1,
            }
        }

        fn with_protocol_version(protocol_version: u64) -> Self {
            Self {
                once: false,
                protocol_version,
            }
        }
    }

    struct FakeCoordinator {
        address: std::net::SocketAddr,
        server: tokio::task::JoinHandle<()>,
    }

    impl FakeCoordinator {
        async fn bind(address: &str) -> Result<(TcpListener, std::net::SocketAddr)> {
            let listener = TcpListener::bind(address).await?;
            let address = listener.local_addr()?;
            Ok((listener, address))
        }

        async fn with_no_work_replies(no_work_replies: usize) -> Result<Self> {
            let (listener, address) = Self::bind("127.0.0.1:0").await?;

            let server = tokio::spawn(async move {
                serve_no_work_session(listener, no_work_replies).await;
            });

            Ok(Self { address, server })
        }

        async fn with_job_assignment() -> Result<Self> {
            let (listener, address) = Self::bind("127.0.0.1:0").await?;

            let server = tokio::spawn(async move {
                let (stream, _) = listener.accept().await.expect("accept connection");
                let socket = accept_async(stream).await.expect("accept websocket");
                let (mut writer, mut reader) = socket.split();

                expect_join(&mut reader).await;
                send_join_reply(&mut writer).await;
                expect_announce(&mut reader, 1).await;
                send_announce_reply(&mut writer).await;
                expect_pull_work(&mut reader, 3).await;
                send_job_assigned_reply(&mut writer).await;
            });

            Ok(Self { address, server })
        }

        fn worker_config(&self, config: WorkerTestConfig) -> WorkerConfig {
            WorkerConfig {
                connect: format!("http://{}", self.address),
                token: "test-worker-token".into(),
                worker_id: "abav1-dev".into(),
                version: "0.11.4".into(),
                protocol_version: config.protocol_version,
                once: config.once,
                local_path: None,
                worker_mode: WorkerMode::CrfSearch,
            }
        }

        async fn finish(self) {
            self.server.await.expect("server task");
        }
    }

    #[test]
    fn args_lowers_to_worker_config() {
        let config = WorkerConfig::from(Args {
            connect: "http://127.0.0.1:4000".into(),
            token: "token".into(),
            worker_id: "abav1-dev".into(),
            version: "0.11.4".into(),
            protocol_version: 1,
            once: false,
            local_path: None,
            worker_mode: WorkerMode::CrfSearch,
        });

        assert_eq!(config.connect, "http://127.0.0.1:4000");
        assert_eq!(config.token, "token");
        assert_eq!(config.worker_id, "abav1-dev");
        assert_eq!(config.version, "0.11.4");
        assert_eq!(config.protocol_version, 1);
        assert!(!config.once);
        assert_eq!(config.worker_mode, WorkerMode::CrfSearch);
    }

    #[test]
    fn args_accepts_both_worker_mode() {
        let args = Args::try_parse_from([
            "ab-av1",
            "--connect",
            "http://127.0.0.1:4000",
            "--token",
            "token",
            "--worker-id",
            "worker",
            "--worker-mode",
            "both",
        ])
        .expect("parse worker mode");

        assert_eq!(args.worker_mode, WorkerMode::Both);
    }

    #[test]
    fn args_defaults_to_both_worker_mode() {
        let args = Args::try_parse_from([
            "ab-av1",
            "--connect",
            "http://127.0.0.1:4000",
            "--token",
            "token",
            "--worker-id",
            "worker",
        ])
        .expect("parse default worker mode");

        assert_eq!(args.worker_mode, WorkerMode::Both);
    }

    #[test]
    fn both_mode_capacity_changes_after_eight_logical_cpus() {
        let small = WorkerCapacity::new(8, WorkerMode::Both);
        let large = WorkerCapacity::new(9, WorkerMode::Both);
        assert_eq!(small.max_active_jobs(), 1);
        assert_eq!(large.max_active_jobs(), 2);
    }

    #[test]
    fn worker_mode_selects_only_its_requested_job_kind() {
        assert_eq!(
            initial_pull_work_payload(WorkerMode::CrfSearch),
            PullWorkPayload::default()
        );
        assert_eq!(
            initial_pull_work_payload(WorkerMode::Encode).job_type,
            Some(JobKind::Encode)
        );
        assert_eq!(
            initial_pull_work_payload(WorkerMode::Both).job_type,
            Some(JobKind::CrfSearch)
        );
        assert_eq!(
            next_job_type_after_no_work(WorkerMode::Both, JobKind::CrfSearch),
            Some(JobKind::Encode)
        );
    }

    #[test]
    fn both_scheduler_prefers_crf_then_uses_encode_slot() {
        let jobs = HashMap::new();
        let pending = HashMap::new();
        let no_work = HashMap::new();
        assert_eq!(
            next_multiplex_job_type(WorkerMode::Both, &jobs, &pending, &no_work),
            Some(JobKind::CrfSearch)
        );

        let mut no_work = HashMap::new();
        no_work.insert(JobKind::CrfSearch, true);
        assert_eq!(
            next_multiplex_job_type(WorkerMode::Both, &jobs, &pending, &no_work),
            Some(JobKind::Encode)
        );

        let mut pending = HashMap::new();
        pending.insert("crf-pull".into(), (JobKind::CrfSearch, None));
        let no_work = HashMap::new();
        assert_eq!(
            next_multiplex_job_type(WorkerMode::Both, &jobs, &pending, &no_work),
            Some(JobKind::Encode)
        );
    }

    #[test]
    fn reconnect_progress_identifies_active_encode_without_prior_progress() {
        let job = WorkerJob::new(
            JobAssignedPayload {
                status: WorkStatus::JobAssigned,
                job_type: JobKind::Encode,
                job_id: "encode-123".into(),
                video_id: 123,
                source_name: "movie.mkv".into(),
                size_bytes: 1_000,
                chunk_size_bytes: 256,
                target_vmaf: 95.0,
                transfer: None,
                output_transfer: None,
                output_shared_path: None,
                encode_args: vec!["encode".into(), "--input".into(), "movie.mkv".into()],
                crf_search_args: Vec::new(),
            },
            PathBuf::from("/tmp/encode-123"),
            PathBuf::from("/tmp/encode-123/movie.mkv"),
        );

        assert!(matches!(
            encode_reconnect_progress(&job, &WorkerJobReportState::default(), false),
            Some(EncodeProgressPayload {
                job_id,
                video_id: 123,
                percent: 0.0,
                ..
            }) if job_id == "encode-123"
        ));
    }

    #[test]
    fn encode_assignment_replaces_server_input_with_worker_path() {
        let input = std::env::temp_dir().join("worker-local-input.mkv");
        let job = WorkerJob::new(
            JobAssignedPayload {
                status: WorkStatus::JobAssigned,
                job_type: JobKind::Encode,
                job_id: "encode-job".into(),
                video_id: 123,
                source_name: "movie.mkv".into(),
                size_bytes: 1,
                chunk_size_bytes: 1,
                target_vmaf: 0.0,
                transfer: None,
                output_transfer: None,
                output_shared_path: None,
                encode_args: vec![
                    "ab-av1".into(),
                    "encode".into(),
                    "--input".into(),
                    "/server/movie.mkv".into(),
                    "--crf".into(),
                    "30".into(),
                    "--output".into(),
                    "encoded.mkv".into(),
                ],
                crf_search_args: Vec::new(),
            },
            std::env::temp_dir(),
            input.clone(),
        );

        let config = job.encode_config().expect("encode config");
        assert_eq!(config.input(), input.as_path());

        let mut shared_assignment = job.assignment.clone();
        shared_assignment.output_shared_path = Some("/shared/movie.av1.mkv".into());
        let shared_job = WorkerJob::new(shared_assignment, std::env::temp_dir(), input);
        let shared_config = shared_job.encode_config().expect("shared encode config");
        assert_eq!(
            shared_config.output(),
            Some(Path::new("/shared/movie.av1.mkv"))
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn encode_uploads_output_before_reporting_completion() -> Result<()> {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        let address = listener.local_addr()?;
        let server = std::thread::spawn(move || -> Result<Vec<u8>> {
            let (mut stream, _) = listener.accept()?;
            let mut request = Vec::new();
            let mut buffer = [0; 4096];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let read = stream.read(&mut buffer)?;
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..read]);
            }
            let header_end = request
                .windows(4)
                .position(|window| window == b"\r\n\r\n")
                .context("missing HTTP request header terminator")?
                + 4;
            let headers = String::from_utf8_lossy(&request[..header_end]);
            let content_length = headers
                .lines()
                .find_map(|line| line.strip_prefix("Content-Length: "))
                .context("missing output content length")?
                .parse::<usize>()?;
            while request.len() < header_end + content_length {
                let read = stream.read(&mut buffer)?;
                if read == 0 {
                    break;
                }
                request.extend_from_slice(&buffer[..read]);
            }
            stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n")?;
            Ok(request[header_end..header_end + content_length].to_vec())
        });

        let input_dir =
            std::env::temp_dir().join(format!("ab-av1-multiplex-encode-{}", std::process::id()));
        fs::create_dir_all(&input_dir)?;
        let input = input_dir.join("movie.mkv");
        fs::write(&input, b"input-bytes")?;
        crate::command::encode::test_hooks::set_fixture("stderr-ffmpeg-progress");
        let job = WorkerJob::new(
            JobAssignedPayload {
                status: WorkStatus::JobAssigned,
                job_type: JobKind::Encode,
                job_id: "upload-job".into(),
                video_id: 123,
                source_name: "movie.mkv".into(),
                size_bytes: 11,
                chunk_size_bytes: 1,
                target_vmaf: 0.0,
                transfer: None,
                output_transfer: Some(TransferSpec {
                    url: format!("http://{address}"),
                    auth: TransferAuth {
                        scheme: "Bearer".into(),
                        header: "Authorization".into(),
                        value: "token".into(),
                    },
                }),
                output_shared_path: None,
                encode_args: vec![
                    "ab-av1".into(),
                    "encode".into(),
                    "--input".into(),
                    "/server/movie.mkv".into(),
                    "--output".into(),
                    "/server/movie.av1.mkv".into(),
                    "--crf".into(),
                    "30".into(),
                ],
                crf_search_args: Vec::new(),
            },
            input_dir.clone(),
            input,
        );
        let (output, mut outputs) = mpsc::unbounded_channel();
        let (_commands, mut commands) = mpsc::unbounded_channel();
        let result = run_multiplex_encode(
            &job,
            Arc::new(Ffprobe {
                duration: Ok(Duration::from_secs(120)),
                has_audio: false,
                max_audio_channels: None,
                fps: Ok(24.0),
                resolution: Some((1920, 1080)),
                is_image: false,
                pix_fmt: Some("yuv420p".into()),
            }),
            &mut commands,
            &output,
        )
        .await;
        crate::command::encode::test_hooks::clear();
        result?;
        let uploaded = server.join().expect("upload server thread")?;
        assert!(!uploaded.is_empty());
        let mut completed = false;
        while let Ok(item) = outputs.try_recv() {
            if matches!(
                item,
                MultiplexOutput::Event {
                    event: ClientEvent::EncodeCompleted(_),
                    ..
                }
            ) {
                completed = true;
            }
        }
        assert!(completed, "completion must follow successful upload");
        let _ = fs::remove_dir_all(input_dir);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn multiplex_encode_sends_heartbeat_without_ffmpeg_progress() -> Result<()> {
        let input_dir =
            std::env::temp_dir().join(format!("ab-av1-multiplex-heartbeat-{}", std::process::id()));
        fs::create_dir_all(&input_dir)?;
        let input = input_dir.join("movie.mkv");
        fs::write(&input, b"input-bytes")?;
        crate::command::encode::test_hooks::set_fixture("sleep-long");
        let job = WorkerJob::new(
            JobAssignedPayload {
                status: WorkStatus::JobAssigned,
                job_type: JobKind::Encode,
                job_id: "heartbeat-job".into(),
                video_id: 123,
                source_name: "movie.mkv".into(),
                size_bytes: 11,
                chunk_size_bytes: 1,
                target_vmaf: 0.0,
                transfer: None,
                output_transfer: None,
                output_shared_path: None,
                encode_args: vec![
                    "encode".into(),
                    "--input".into(),
                    "/server/movie.mkv".into(),
                    "--output".into(),
                    input_dir.join("movie.av1.mkv").display().to_string(),
                    "--crf".into(),
                    "30".into(),
                ],
                crf_search_args: Vec::new(),
            },
            input_dir.clone(),
            input,
        );
        let (output, mut outputs) = mpsc::unbounded_channel();
        let (commands, mut command_receiver) = mpsc::unbounded_channel();
        let mut run = std::pin::pin!(run_multiplex_encode_with_heartbeat_interval(
            &job,
            Arc::new(Ffprobe {
                duration: Ok(Duration::from_secs(120)),
                has_audio: false,
                max_audio_channels: None,
                fps: Ok(24.0),
                resolution: Some((1920, 1080)),
                is_image: false,
                pix_fmt: Some("yuv420p".into()),
            }),
            &mut command_receiver,
            &output,
            Duration::from_millis(1),
        ));

        assert!(matches!(
            tokio::select! {
                item = outputs.recv() => item,
                result = &mut run => panic!("encode ended early: {result:?}"),
            },
            Some(MultiplexOutput::Event {
                event: ClientEvent::EncodeProgress(_),
                ..
            })
        ));
        let heartbeat = tokio::time::timeout(Duration::from_millis(100), async {
            loop {
                tokio::select! {
                    item = outputs.recv() => match item {
                        Some(MultiplexOutput::Event {
                            event: ClientEvent::Heartbeat(_),
                            ..
                        }) => break true,
                        Some(_) => continue,
                        None => break false,
                    },
                    result = &mut run => panic!("encode ended early: {result:?}"),
                }
            }
        })
        .await
        .unwrap_or(false);

        commands.send(JobCommand::Control(ControlPayload {
            action: ControlAction::Stop,
            video_id: Some(123),
            job_id: Some("heartbeat-job".into()),
        }))?;
        assert!(matches!(run.await?, WorkerJobOutcome::Stopped));
        crate::command::encode::test_hooks::clear();
        let _ = fs::remove_dir_all(input_dir);
        assert!(heartbeat);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn worker_accepts_and_completes_encode_assignment() -> Result<()> {
        let (listener, address) = FakeCoordinator::bind("127.0.0.1:0").await?;
        let input = std::env::temp_dir().join(format!(
            "ab-av1-multiplex-client-input-{}.mkv",
            std::process::id()
        ));
        let output = std::env::temp_dir().join(format!(
            "ab-av1-multiplex-client-output-{}.mkv",
            std::process::id()
        ));
        fs::write(&input, b"input-bytes")?;
        let expected_output = output.display().to_string();
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept connection");
            let socket = accept_async(stream).await.expect("accept websocket");
            let (mut writer, mut reader) = socket.split();
            expect_join(&mut reader).await;
            send_join_reply(&mut writer).await;
            expect_announce_for_mode(&mut reader, 1, WorkerMode::Encode).await;
            send_announce_reply(&mut writer).await;
            expect_pull_work_payload(
                &mut reader,
                3,
                PullWorkPayload {
                    input_missing: false,
                    job_type: Some(JobKind::Encode),
                },
            )
            .await;
            let assignment = JobAssignedPayload {
                status: WorkStatus::JobAssigned,
                job_type: JobKind::Encode,
                job_id: "e2e-encode-job".into(),
                video_id: 123,
                source_name: "movie.mkv".into(),
                size_bytes: 11,
                chunk_size_bytes: 1,
                target_vmaf: 0.0,
                transfer: None,
                output_transfer: None,
                output_shared_path: Some(expected_output.clone()),
                encode_args: vec![
                    "ab-av1".into(),
                    "encode".into(),
                    "--input".into(),
                    "/server/movie.mkv".into(),
                    "--output".into(),
                    "/server/movie.av1.mkv".into(),
                    "--crf".into(),
                    "30".into(),
                ],
                crf_search_args: Vec::new(),
            };
            writer
                .send(Message::Text(
                    serde_json::to_string(&ServerFrame::reply(
                        3,
                        ReplyBody::ok(ServerReply::JobAssigned(assignment)),
                    ))
                    .expect("encode assignment json"),
                ))
                .await
                .expect("send encode assignment");
            loop {
                let Some(Ok(Message::Text(text))) = reader.next().await else {
                    break;
                };
                let frame: Value = serde_json::from_str(&text).expect("worker event json");
                if frame[3] == "encode_completed" {
                    assert_eq!(frame[4]["output_path"], expected_output);
                    break;
                }
            }
        });
        crate::command::encode::test_hooks::set_fixture("stderr-ffmpeg-progress");
        let config = WorkerConfig {
            connect: format!("http://{address}"),
            token: "test-worker-token".into(),
            worker_id: "abav1-dev".into(),
            version: "0.11.4".into(),
            protocol_version: 1,
            once: false,
            local_path: Some(input.clone()),
            worker_mode: WorkerMode::Encode,
        };
        let local = tokio::task::LocalSet::new();
        let result = local
            .run_until(run_worker_until(
                &config,
                WorkerRuntime {
                    idle_delay: Duration::from_millis(10),
                    reconnect_base_delay: Duration::from_millis(10),
                    reconnect_max_delay: Duration::from_millis(10),
                    max_pulls: Some(1),
                },
            ))
            .await;
        crate::command::encode::test_hooks::clear();
        result?;
        server.await.expect("encode coordinator");
        let _ = fs::remove_file(input);
        let _ = fs::remove_file(output);
        Ok(())
    }

    #[test]
    fn worker_websocket_url_rewrites_http_scheme() {
        let url = worker_websocket_url("http://127.0.0.1:4000/", "secret").expect("url");

        assert_eq!(
            url,
            "ws://127.0.0.1:4000/workers/socket/websocket?token=secret&vsn=2.0.0"
        );
    }

    #[tokio::test(flavor = "current_thread")]
    async fn worker_session_joins_announces_and_requests_work() -> Result<()> {
        let coordinator = FakeCoordinator::with_no_work_replies(1).await?;

        let session =
            run_worker_session(&coordinator.worker_config(WorkerTestConfig::continuous())).await?;

        assert_eq!(
            session,
            WorkerSession {
                assigned_worker_id: "worker-123".into(),
                negotiated_protocol_version: 1,
                work_status: "no_work".into(),
                assigned_job: None,
            }
        );

        coordinator.finish().await;
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn worker_session_exposes_assigned_job_payload() -> Result<()> {
        let coordinator = FakeCoordinator::with_job_assignment().await?;

        let session =
            run_worker_session(&coordinator.worker_config(WorkerTestConfig::continuous())).await?;

        let job = session.assigned_job.expect("assigned job");
        assert_eq!(session.assigned_worker_id, "worker-123");
        assert_eq!(session.negotiated_protocol_version, 1);
        assert_eq!(session.work_status, "job_assigned (job_id=job-123)");
        assert_eq!(job.job_id, "job-123");
        assert_eq!(job.video_id, 123);
        assert_eq!(job.source_name, "movie.mkv");
        assert_eq!(job.size_bytes, 1024);
        assert_eq!(job.chunk_size_bytes, 256);
        assert_eq!(job.target_vmaf, 96.5);

        coordinator.finish().await;
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn worker_stays_connected_and_pulls_work_again_after_no_work() -> Result<()> {
        let coordinator = FakeCoordinator::with_no_work_replies(2).await?;

        run_worker_until(
            &coordinator.worker_config(WorkerTestConfig::continuous()),
            WorkerRuntime {
                idle_delay: Duration::from_millis(1),
                reconnect_base_delay: Duration::from_millis(1),
                reconnect_max_delay: Duration::from_millis(1),
                max_pulls: Some(2),
            },
        )
        .await?;

        coordinator.finish().await;
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stopped_worker_waits_for_start_before_pulling_again() -> Result<()> {
        let (listener, address) = FakeCoordinator::bind("127.0.0.1:0").await?;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept connection");
            let socket = accept_async(stream).await.expect("accept websocket");
            let (mut writer, mut reader) = socket.split();

            expect_join(&mut reader).await;
            send_join_reply(&mut writer).await;
            expect_announce(&mut reader, 1).await;
            send_announce_reply(&mut writer).await;
            expect_pull_work(&mut reader, 3).await;
            send_control_push(&mut writer, ControlAction::Stop, None).await;
            send_no_work_reply(&mut writer, 3).await;
            assert_eq!(
                expect_client_event(&mut reader, 4, "control_state").await,
                json!({"state": "stopped"})
            );
            send_control_push(&mut writer, ControlAction::Start, None).await;
            assert_eq!(
                expect_client_event(&mut reader, 5, "control_state").await,
                json!({"state": "running"})
            );
            expect_pull_work(&mut reader, 6).await;
            send_no_work_reply(&mut writer, 6).await;
        });

        run_worker_until(
            &FakeCoordinator {
                address,
                server: tokio::spawn(async {}),
            }
            .worker_config(WorkerTestConfig::continuous()),
            WorkerRuntime {
                idle_delay: Duration::from_secs(30),
                reconnect_base_delay: Duration::from_millis(1),
                reconnect_max_delay: Duration::from_millis(1),
                max_pulls: Some(1),
            },
        )
        .await?;
        server.await.expect("server task");
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn paused_worker_waits_for_resume_before_pulling_again() -> Result<()> {
        let (listener, address) = FakeCoordinator::bind("127.0.0.1:0").await?;
        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept connection");
            let socket = accept_async(stream).await.expect("accept websocket");
            let (mut writer, mut reader) = socket.split();

            expect_join(&mut reader).await;
            send_join_reply(&mut writer).await;
            expect_announce(&mut reader, 1).await;
            send_announce_reply(&mut writer).await;
            expect_pull_work(&mut reader, 3).await;
            send_control_push(&mut writer, ControlAction::Pause, None).await;
            send_no_work_reply(&mut writer, 3).await;
            assert_eq!(
                expect_client_event(&mut reader, 4, "control_state").await,
                json!({"state": "paused"})
            );
            send_control_push(&mut writer, ControlAction::Resume, None).await;
            assert_eq!(
                expect_client_event(&mut reader, 5, "control_state").await,
                json!({"state": "running"})
            );
            expect_pull_work(&mut reader, 6).await;
            send_no_work_reply(&mut writer, 6).await;
        });

        run_worker_until(
            &FakeCoordinator {
                address,
                server: tokio::spawn(async {}),
            }
            .worker_config(WorkerTestConfig::continuous()),
            WorkerRuntime {
                idle_delay: Duration::from_secs(30),
                reconnect_base_delay: Duration::from_millis(1),
                reconnect_max_delay: Duration::from_millis(1),
                max_pulls: Some(1),
            },
        )
        .await?;
        server.await.expect("server task");
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn worker_reconnects_after_disconnect_and_continues_pulling_work() -> Result<()> {
        let coordinator = FakeCoordinator::with_no_work_replies(1).await?;
        let address = coordinator.address;

        let worker = async move {
            run_worker_until(
                &coordinator.worker_config(WorkerTestConfig::continuous()),
                WorkerRuntime {
                    idle_delay: Duration::from_millis(1),
                    reconnect_base_delay: Duration::from_millis(1),
                    reconnect_max_delay: Duration::from_millis(2),
                    max_pulls: Some(2),
                },
            )
            .await
        };

        let replacement = async move {
            tokio::time::sleep(Duration::from_millis(5)).await;
            let listener = TcpListener::bind(address)
                .await
                .expect("bind replacement coordinator");
            serve_no_work_session(listener, 1).await;
        };

        let (worker, _replacement) = tokio::join!(worker, replacement);
        worker?;
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn worker_requests_input_resend_when_resumed_job_lacks_local_file() -> Result<()> {
        let job_id = "missing-input-resend";
        let _ = fs::remove_dir_all(worker_job_input_dir(job_id));
        let (listener, address) = FakeCoordinator::bind("127.0.0.1:0").await?;

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept connection");
            let socket = accept_async(stream).await.expect("accept websocket");
            let (mut writer, mut reader) = socket.split();

            expect_join(&mut reader).await;
            send_join_reply(&mut writer).await;
            expect_announce(&mut reader, 1).await;
            send_announce_reply(&mut writer).await;

            expect_pull_work(&mut reader, 3).await;
            send_job_reply_with_job_id(&mut writer, 3, WorkStatus::JobInProgress, job_id).await;

            let heartbeat = expect_client_event(&mut reader, 4, "heartbeat").await;
            assert_eq!(heartbeat["active_video_id"], json!(123));

            let failure = expect_client_event(&mut reader, 5, "transfer_failed").await;
            assert_eq!(failure["job_id"], json!(job_id));
            assert_eq!(failure["stage"], json!("receive_chunk"));
            assert_eq!(failure["retriable"], json!(true));

            expect_pull_work_payload(&mut reader, 6, PullWorkPayload::input_missing()).await;
            send_job_reply_with_job_id(&mut writer, 6, WorkStatus::JobAssigned, job_id).await;
        });

        let config = FakeCoordinator {
            address,
            server: tokio::spawn(async {}),
        }
        .worker_config(WorkerTestConfig::continuous());
        let mut completed_pulls = 0;
        let mut control_state = WorkerControlState::Running;
        let error = run_connected_worker(
            &config,
            WorkerRuntime {
                idle_delay: Duration::from_millis(50),
                reconnect_base_delay: Duration::from_millis(1),
                reconnect_max_delay: Duration::from_millis(1),
                max_pulls: None,
            },
            &mut completed_pulls,
            &mut control_state,
        )
        .await
        .expect_err("server closes after resend assignment");

        assert!(
            error.to_string().contains("websocket"),
            "unexpected error: {error}"
        );
        server.await.expect("server task");
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn worker_reports_supported_versions_on_protocol_mismatch() -> Result<()> {
        let (listener, address) = FakeCoordinator::bind("127.0.0.1:0").await?;

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept connection");
            let socket = accept_async(stream).await.expect("accept websocket");
            let (mut writer, mut reader) = socket.split();

            expect_join(&mut reader).await;
            send_join_reply(&mut writer).await;
            expect_announce(&mut reader, 99).await;
            send_announce_error_reply(
                &mut writer,
                2,
                json!({
                    "reason": "unsupported_protocol_version",
                    "supported_protocol_versions": [1]
                }),
            )
            .await;
        });

        let error = run_worker_session(
            &FakeCoordinator {
                address,
                server: tokio::spawn(async {}),
            }
            .worker_config(WorkerTestConfig::with_protocol_version(99)),
        )
        .await
        .expect_err("protocol mismatch should fail");

        assert!(
            error.to_string().contains("unsupported_protocol_version"),
            "unexpected error: {error}"
        );
        assert!(
            error.to_string().contains("[1]"),
            "unexpected error: {error}"
        );

        server.await.expect("server task");
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn worker_ignores_cancel_push_while_waiting_for_pull_work_reply() -> Result<()> {
        let (listener, address) = FakeCoordinator::bind("127.0.0.1:0").await?;

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept connection");
            let socket = accept_async(stream).await.expect("accept websocket");
            let (mut writer, mut reader) = socket.split();

            expect_join(&mut reader).await;
            send_join_reply(&mut writer).await;
            expect_announce(&mut reader, 1).await;
            send_announce_reply(&mut writer).await;
            expect_pull_work(&mut reader, 3).await;
            send_cancel_push(&mut writer, "job-123", "shutdown").await;
            send_no_work_reply(&mut writer, 3).await;
        });

        let session = run_worker_session(
            &FakeCoordinator {
                address,
                server: tokio::spawn(async {}),
            }
            .worker_config(WorkerTestConfig::continuous()),
        )
        .await?;

        assert_eq!(session.work_status, "no_work");

        server.await.expect("server task");
        Ok(())
    }

    #[test]
    fn worker_decodes_pause_control_push() -> Result<()> {
        let text = serde_json::to_string(&ServerPushFrame::new(
            "control",
            crate::command::worker_protocol::ControlPayload {
                action: crate::command::worker_protocol::ControlAction::Pause,
                video_id: Some(123),
                job_id: None,
            },
        ))?;

        assert!(matches!(
            decode_worker_push(&text)?,
            Some(WorkerPush::Control(
                crate::command::worker_protocol::ControlPayload {
                    action: crate::command::worker_protocol::ControlAction::Pause,
                    video_id: Some(123),
                    job_id: None,
                }
            ))
        ));
        Ok(())
    }

    #[test]
    fn unscoped_control_targets_every_active_job() {
        let control = crate::command::worker_protocol::ControlPayload {
            action: crate::command::worker_protocol::ControlAction::Resume,
            video_id: None,
            job_id: None,
        };

        assert!(control_targets_job(&control, "crf-1", 1, false));
        assert!(control_targets_job(&control, "encode-2", 2, false));
        assert!(!control_targets_job(&control, "finished-3", 3, true));
    }

    #[test]
    fn worker_formats_assigned_job_status_with_job_id() {
        let status = work_status_label(&ServerReply::JobAssigned(JobAssignedPayload {
            status: WorkStatus::JobAssigned,
            job_type: JobKind::CrfSearch,
            job_id: "job-123".into(),
            video_id: 123,
            source_name: "movie.mkv".into(),
            size_bytes: 1024,
            chunk_size_bytes: 256,
            target_vmaf: 96.5,
            transfer: None,
            output_transfer: None,
            output_shared_path: None,
            encode_args: Vec::new(),
            crf_search_args: vec![
                "crf-search".into(),
                "--input".into(),
                "/server/movie.mkv".into(),
                "--min-vmaf".into(),
                "96.5".into(),
            ],
        }));

        assert_eq!(status, "job_assigned (job_id=job-123)");
    }

    #[test]
    fn worker_job_lowering_uses_an_isolated_temp_dir_and_target_vmaf() {
        let job_dir =
            std::env::temp_dir().join(format!("ab-av1-worker-job-{}", std::process::id()));
        let input_path = job_dir.join("movie.mkv");
        let job = WorkerJob::new(
            JobAssignedPayload {
                status: WorkStatus::JobAssigned,
                job_type: JobKind::CrfSearch,
                job_id: "job-123".into(),
                video_id: 123,
                source_name: "movie.mkv".into(),
                size_bytes: 1024,
                chunk_size_bytes: 256,
                target_vmaf: 96.5,
                transfer: None,
                output_transfer: None,
                output_shared_path: None,
                encode_args: Vec::new(),
                crf_search_args: vec![
                    "crf-search".into(),
                    "--input".into(),
                    "/server/movie.mkv".into(),
                    "--min-vmaf".into(),
                    "96.5".into(),
                ],
            },
            job_dir.clone(),
            input_path,
        );

        let config = job.crf_search_config().expect("job config");

        assert_eq!(config.args.input, job_dir.join("movie.mkv"));
        assert_eq!(config.sample.temp_dir.as_deref(), Some(job_dir.as_path()));
        assert_eq!(config.min_vmaf.expect("target vmaf").get(), 96.5);
        assert!(config.cache);
    }

    #[test]
    fn build_worker_job_uses_local_path_only_when_requested() {
        let assignment = JobAssignedPayload {
            status: WorkStatus::JobAssigned,
            job_type: JobKind::CrfSearch,
            job_id: "job-123".into(),
            video_id: 123,
            source_name: "movie.mkv".into(),
            size_bytes: 1024,
            chunk_size_bytes: 256,
            target_vmaf: 96.5,
            transfer: None,
            output_transfer: None,
            output_shared_path: None,
            encode_args: Vec::new(),
            crf_search_args: vec![
                "crf-search".into(),
                "--input".into(),
                "/server/movie.mkv".into(),
                "--min-vmaf".into(),
                "96.5".into(),
            ],
        };
        let local_path = std::env::temp_dir()
            .join(format!("ab-av1-worker-local-{}", std::process::id()))
            .join("movie.mkv");

        let job = build_worker_job(assignment, Some(local_path.as_path())).expect("worker job");

        assert_eq!(job.input_path(), local_path.as_path());
    }

    #[test]
    fn build_worker_job_uses_offered_local_file_when_size_matches() -> Result<()> {
        let worker_dir = worker_job_input_dir("offered-local");
        let _ = fs::remove_dir_all(&worker_dir);
        let root = std::env::temp_dir().join(format!(
            "ab-av1-worker-offered-local-{}",
            std::process::id()
        ));
        let local_path = root.join("movie.mkv");
        fs::create_dir_all(&root)?;
        fs::write(&local_path, b"data")?;
        let assignment = serde_json::from_value(json!({
            "status": "job_assigned",
            "job_id": "offered-local",
            "video_id": 123,
            "source_name": "movie.mkv",
            "local_path": local_path,
            "size_bytes": 4,
            "target_vmaf": 96.5,
            "crf_search_args": ["crf-search", "--input", local_path]
        }))?;

        let job = build_worker_job(assignment, None)?;

        assert_eq!(job.input_path(), local_path.as_path());
        assert!(!worker_dir.exists());
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn build_worker_job_uses_source_basename_for_worker_file_lookup() -> Result<()> {
        let job_dir = worker_job_input_dir("job-path-source");
        fs::create_dir_all(&job_dir)?;
        let input_path = job_dir.join("movie.mkv");
        fs::write(&input_path, [0_u8; 4])?;

        let job = build_worker_job(
            JobAssignedPayload {
                status: WorkStatus::JobInProgress,
                job_type: JobKind::CrfSearch,
                job_id: "job-path-source".into(),
                video_id: 123,
                source_name: "/server/library/movie.mkv".into(),
                size_bytes: 4,
                chunk_size_bytes: 0,
                target_vmaf: 96.5,
                transfer: None,
                output_transfer: None,
                output_shared_path: None,
                encode_args: Vec::new(),
                crf_search_args: vec![
                    "crf-search".into(),
                    "--input".into(),
                    "/server/library/movie.mkv".into(),
                    "--min-vmaf".into(),
                    "96.5".into(),
                ],
            },
            None,
        )?;

        assert_eq!(job.input_path(), input_path.as_path());
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn worker_job_runs_crf_search_from_fake_probe() -> Result<()> {
        crf_test_hooks::set(|_crf| sample_encode::Output {
            vmaf_score: Some(97.0),
            xpsnr_score: None,
            predicted_encode_size: 100,
            encode_percent: 50.0,
            predicted_encode_time: Duration::from_secs(1),
            from_cache: false,
        });

        let job_dir =
            std::env::temp_dir().join(format!("ab-av1-worker-exec-{}", std::process::id()));
        let input_path = job_dir.join("movie.mkv");
        let job = WorkerJob::new(
            JobAssignedPayload {
                status: WorkStatus::JobAssigned,
                job_type: JobKind::CrfSearch,
                job_id: "job-123".into(),
                video_id: 123,
                source_name: "movie.mkv".into(),
                size_bytes: 1024,
                chunk_size_bytes: 256,
                target_vmaf: 96.5,
                transfer: None,
                output_transfer: None,
                output_shared_path: None,
                encode_args: Vec::new(),
                crf_search_args: vec![
                    "crf-search".into(),
                    "--input".into(),
                    "/server/movie.mkv".into(),
                    "--min-vmaf".into(),
                    "96.5".into(),
                ],
            },
            job_dir,
            input_path,
        );

        let probe = Arc::new(Ffprobe {
            duration: Ok(Duration::from_secs(600)),
            has_audio: false,
            max_audio_channels: None,
            fps: Ok(24.0),
            resolution: Some((1280, 720)),
            is_image: false,
            pix_fmt: Some("yuv420p10le".into()),
        });

        let best = run_worker_job(job.clone(), probe).await?;
        crf_test_hooks::clear();

        assert!(best.crf.is_finite());
        assert_eq!(best.enc.vmaf_score, Some(97.0));
        assert_eq!(best.enc.encode_percent, 50.0);
        let result = job.crf_result_payload(&best, true);
        assert_eq!(result.job_id, "job-123");
        assert_eq!(result.video_id, 123);
        assert_eq!(result.source_name, "movie.mkv");
        assert_eq!(result.crf, best.crf);
        assert_eq!(result.vmaf_score, Some(97.0));
        assert_eq!(result.xpsnr_score, None);
        assert_eq!(result.predicted_encode_size, 100);
        assert_eq!(result.encode_percent, 50.0);
        assert_eq!(result.predicted_encode_time_secs, 1.0);
        assert!(!result.from_cache);
        assert!(result.chosen);
        Ok(())
    }

    #[test]
    fn pending_job_finalizes_chunk_to_worker_input_and_reports_progress() -> Result<()> {
        let job_id = format!("worker-flow-chunk-{}", std::process::id());
        let input_dir = worker_job_input_dir(&job_id);
        let input_path = input_dir.join("movie.mkv");
        let _ = fs::remove_dir_all(&input_dir);
        let job = WorkerJob::new(
            JobAssignedPayload {
                status: WorkStatus::JobAssigned,
                job_type: JobKind::CrfSearch,
                job_id: job_id.clone(),
                video_id: 123,
                source_name: "movie.mkv".into(),
                size_bytes: 4,
                chunk_size_bytes: 4,
                target_vmaf: 95.0,
                transfer: None,
                output_transfer: None,
                output_shared_path: None,
                encode_args: Vec::new(),
                crf_search_args: vec![
                    "crf-search".into(),
                    "--input".into(),
                    "/server/movie.mkv".into(),
                    "--min-vmaf".into(),
                    "95".into(),
                ],
            },
            input_dir.clone(),
            input_path.clone(),
        );
        let mut pending = PendingJob::waiting(job);
        pending.apply_raw_chunk(TransferChunk {
            transfer_id: job_id,
            video_id: 123,
            chunk_index: 0,
            total_chunks: 1,
            bytes_sent: 4,
            total_bytes: 4,
            crc32: crc32fast::hash(b"data") as u64,
            bytes: b"data".to_vec(),
        })?;

        let progress = pending.transfer_progress_payload(0, 1);
        assert_eq!(progress.received_bytes, 4);
        assert_eq!(progress.expected_bytes, Some(4));
        assert_eq!(progress.percent, 100.0);
        assert_eq!(progress.chunk_index, 0);
        assert_eq!(progress.total_chunks, 1);

        pending.finish()?;
        assert_eq!(fs::read(&input_path)?, b"data");
        fs::remove_dir_all(input_dir)?;
        Ok(())
    }

    #[test]
    fn worker_input_cleanup_removes_owned_directory_only() -> Result<()> {
        let root = worker_job_input_dir("cleanup-job");
        let _ = fs::remove_dir_all(&root);
        let input_path = root.join("movie.mkv");
        fs::create_dir_all(&root)?;
        fs::write(&input_path, b"data")?;
        let job = WorkerJob::new(
            JobAssignedPayload {
                status: WorkStatus::JobAssigned,
                job_type: JobKind::CrfSearch,
                job_id: "cleanup-job".into(),
                video_id: 123,
                source_name: "movie.mkv".into(),
                size_bytes: 4,
                chunk_size_bytes: 4,
                target_vmaf: 95.0,
                transfer: None,
                output_transfer: None,
                output_shared_path: None,
                encode_args: Vec::new(),
                crf_search_args: vec![
                    "crf-search".into(),
                    "--input".into(),
                    "/server/movie.mkv".into(),
                    "--min-vmaf".into(),
                    "95".into(),
                ],
            },
            root.clone(),
            input_path.clone(),
        );
        remove_worker_input(&job)?;
        assert!(!root.exists());
        Ok(())
    }

    #[test]
    fn multiplex_worker_input_cleanup_is_success_only() -> Result<()> {
        let job_id = format!("cleanup-outcome-{}", std::process::id());
        let root = worker_job_input_dir(&job_id);
        let _ = fs::remove_dir_all(&root);
        let input_path = root.join("movie.mkv");
        fs::create_dir_all(&root)?;
        fs::write(&input_path, b"data")?;
        let job = WorkerJob::new(
            JobAssignedPayload {
                status: WorkStatus::JobAssigned,
                job_type: JobKind::CrfSearch,
                job_id,
                video_id: 123,
                source_name: "movie.mkv".into(),
                size_bytes: 4,
                chunk_size_bytes: 4,
                target_vmaf: 95.0,
                transfer: None,
                output_transfer: None,
                output_shared_path: None,
                encode_args: Vec::new(),
                crf_search_args: vec!["crf-search".into(), "--input".into(), "movie.mkv".into()],
            },
            root.clone(),
            input_path,
        );

        cleanup_multiplex_worker_input(&job, &Ok(WorkerJobOutcome::Stopped))?;
        assert!(root.exists());
        cleanup_multiplex_worker_input(&job, &Ok(WorkerJobOutcome::Completed))?;
        assert!(!root.exists());
        Ok(())
    }

    #[test]
    fn completed_local_encode_cleanup_preserves_output() -> Result<()> {
        let job_id = format!("cleanup-local-output-{}", std::process::id());
        let root = worker_job_input_dir(&job_id);
        let _ = fs::remove_dir_all(&root);
        let input_path = root.join("movie.mkv");
        let output_path = root.join("movie.av1.mkv");
        fs::create_dir_all(&root)?;
        fs::write(&input_path, b"input")?;
        fs::write(&output_path, b"output")?;
        let job = WorkerJob::new(
            JobAssignedPayload {
                status: WorkStatus::JobAssigned,
                job_type: JobKind::Encode,
                job_id,
                video_id: 123,
                source_name: "movie.mkv".into(),
                size_bytes: 5,
                chunk_size_bytes: 5,
                target_vmaf: 0.0,
                transfer: None,
                output_transfer: None,
                output_shared_path: None,
                encode_args: vec![
                    "encode".into(),
                    "--input".into(),
                    "movie.mkv".into(),
                    "--output".into(),
                    "movie.av1.mkv".into(),
                ],
                crf_search_args: Vec::new(),
            },
            root.clone(),
            input_path.clone(),
        );

        cleanup_multiplex_worker_input(&job, &Ok(WorkerJobOutcome::Completed))?;

        assert!(!input_path.exists());
        assert!(output_path.exists());
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[test]
    fn worker_job_phase_selects_input_delivery_and_resume_behavior() -> Result<()> {
        let root = std::env::temp_dir().join(format!("ab-av1-worker-phase-{}", std::process::id()));
        let input_path = root.join("movie.mkv");
        let make_job = |status, transfer| {
            WorkerJob::new(
                JobAssignedPayload {
                    status,
                    job_type: JobKind::CrfSearch,
                    job_id: "phase-job".into(),
                    video_id: 123,
                    source_name: "movie.mkv".into(),
                    size_bytes: 4,
                    chunk_size_bytes: 4,
                    target_vmaf: 95.0,
                    transfer,
                    output_transfer: None,
                    output_shared_path: None,
                    encode_args: Vec::new(),
                    crf_search_args: vec![],
                },
                root.clone(),
                input_path.clone(),
            )
        };

        let _ = fs::remove_dir_all(&root);
        assert_eq!(
            worker_job_phase(&make_job(WorkStatus::JobAssigned, None), None)?,
            WorkerJobPhase::AwaitingInput(InputDelivery::Websocket)
        );
        assert_eq!(
            worker_job_phase(
                &make_job(
                    WorkStatus::JobAssigned,
                    Some(TransferSpec {
                        url: "http://server/input".into(),
                        auth: TransferAuth {
                            scheme: "Bearer".into(),
                            header: "authorization".into(),
                            value: "token".into(),
                        },
                    })
                ),
                None
            )?,
            WorkerJobPhase::AwaitingInput(InputDelivery::Http)
        );
        assert_eq!(
            worker_job_phase(&make_job(WorkStatus::JobInProgress, None), None)?,
            WorkerJobPhase::AwaitingInput(InputDelivery::Resend)
        );

        fs::create_dir_all(&root)?;
        fs::write(&input_path, b"data")?;
        assert_eq!(
            worker_job_phase(&make_job(WorkStatus::JobAssigned, None), None)?,
            WorkerJobPhase::InputReady
        );
        assert_eq!(
            worker_job_phase(&make_job(WorkStatus::JobInProgress, None), None)?,
            WorkerJobPhase::CrfSearching
        );

        fs::remove_file(&input_path)?;
        assert!(
            worker_job_phase(&make_job(WorkStatus::JobAssigned, None), Some(&input_path)).is_err()
        );
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn crf_updates_are_retained_for_reconnect_reporting() -> Result<()> {
        let job = WorkerJob::new(
            JobAssignedPayload {
                status: WorkStatus::JobAssigned,
                job_type: JobKind::CrfSearch,
                job_id: "reporting-job".into(),
                video_id: 123,
                source_name: "movie.mkv".into(),
                size_bytes: 4,
                chunk_size_bytes: 4,
                target_vmaf: 95.0,
                transfer: None,
                output_transfer: None,
                output_shared_path: None,
                encode_args: Vec::new(),
                crf_search_args: vec![
                    "crf-search".into(),
                    "--input".into(),
                    "/server/movie.mkv".into(),
                    "--min-vmaf".into(),
                    "95".into(),
                ],
            },
            std::env::temp_dir(),
            std::env::temp_dir().join("movie.mkv"),
        );
        let mut state = WorkerJobReportState::default();
        let status = sample_encode::Status {
            work: sample_encode::Work::Encode,
            fps: 24.0,
            progress: 0.5,
            sample: 2,
            samples: 4,
            full_pass: false,
        };
        let (_, disconnected) = handle_crf_update(
            &job,
            &mut state,
            None,
            Some(Ok(crf_search::Update::Status {
                crf_run: 1,
                crf: 31.0,
                sample: status,
            })),
        )
        .await?;
        assert!(!disconnected);
        let progress = state.crf_progress.as_ref().expect("stored progress");
        assert_eq!(progress.video_id, 123);
        assert_eq!(progress.percent, 50.0);
        assert_eq!(progress.fps, 24.0);
        assert_eq!(progress.crf, 31.0);
        assert_eq!(progress.sample_num, 2);
        assert_eq!(progress.total_samples, 4);

        crf_test_hooks::set(|_crf| sample_encode::Output {
            vmaf_score: Some(96.0),
            xpsnr_score: None,
            predicted_encode_size: 100,
            encode_percent: 50.0,
            predicted_encode_time: Duration::from_secs(1),
            from_cache: false,
        });
        let sample = run_worker_job(
            job.clone(),
            Arc::new(Ffprobe {
                duration: Ok(Duration::from_secs(600)),
                has_audio: false,
                max_audio_channels: None,
                fps: Ok(24.0),
                resolution: Some((1280, 720)),
                is_image: false,
                pix_fmt: Some("yuv420p10le".into()),
            }),
        )
        .await?;
        crf_test_hooks::clear();
        let (best, disconnected) = handle_crf_update(
            &job,
            &mut state,
            None,
            Some(Ok(crf_search::Update::Done(sample))),
        )
        .await?;
        assert!(!disconnected);
        assert!(best.is_some());
        assert_eq!(state.crf_results.len(), 1);
        assert!(state.crf_results[0].chosen);
        Ok(())
    }

    #[test]
    fn reconnect_backoff_grows_and_caps() {
        let mut backoff =
            ReconnectBackoff::new(Duration::from_millis(100), Duration::from_millis(1_000));

        assert_eq!(backoff.next_delay(), Duration::from_millis(100));
        assert_eq!(backoff.next_delay(), Duration::from_millis(200));
        assert_eq!(backoff.next_delay(), Duration::from_millis(400));
        assert_eq!(backoff.next_delay(), Duration::from_millis(800));
        assert_eq!(backoff.next_delay(), Duration::from_millis(1_000));
        assert_eq!(backoff.next_delay(), Duration::from_millis(1_000));
    }

    #[test]
    fn worker_runtime_defaults_back_off_reconnects() {
        let runtime = WorkerRuntime::default();

        assert_eq!(runtime.reconnect_base_delay, Duration::from_secs(1));
        assert_eq!(runtime.reconnect_max_delay, Duration::from_secs(30));
    }

    #[test]
    fn reconnect_backoff_resets_after_success() {
        let mut backoff =
            ReconnectBackoff::new(Duration::from_millis(100), Duration::from_millis(1_000));

        assert_eq!(backoff.next_delay(), Duration::from_millis(100));
        assert_eq!(backoff.next_delay(), Duration::from_millis(200));

        backoff.reset();

        assert_eq!(backoff.next_delay(), Duration::from_millis(100));
    }

    #[test]
    fn binary_transfer_chunk_decodes_rav1_frame() {
        let data = b"hello worker bytes".to_vec();
        let frame = binary_transfer_chunk_frame(
            "job-123",
            123,
            7,
            10,
            8 * 1024 + data.len() as u64,
            64 * 1024,
            &data,
        );

        let chunk = decode_binary_transfer_chunk(&frame).expect("decode binary transfer chunk");

        assert_eq!(chunk.transfer_id, "job-123");
        assert_eq!(chunk.video_id, 123);
        assert_eq!(chunk.chunk_index, 7);
        assert_eq!(chunk.total_chunks, 10);
        assert_eq!(chunk.bytes_sent, 8 * 1024 + data.len() as u64);
        assert_eq!(chunk.total_bytes, 64 * 1024);
        assert_eq!(chunk.crc32, crc32fast::hash(&data) as u64);
        assert_eq!(chunk.bytes, data);
    }

    #[test]
    fn binary_transfer_chunk_rejects_bad_magic() {
        let mut frame = binary_transfer_chunk_frame("job-123", 123, 0, 1, 5, 5, b"hello");
        frame[0..4].copy_from_slice(b"NOPE");

        assert!(decode_binary_transfer_chunk(&frame).is_err());
    }

    #[test]
    fn binary_worker_push_decodes_phoenix_enveloped_transfer_chunk() {
        let data = b"hello worker bytes".to_vec();
        let chunk = binary_transfer_chunk_frame(
            "job-123",
            123,
            7,
            10,
            8 * 1024 + data.len() as u64,
            64 * 1024,
            &data,
        );
        let frame = phoenix_binary_frame("1", CRF_SEARCH_TOPIC, "transfer_chunk", &chunk);

        let chunk = decode_binary_worker_push(&frame)
            .expect("decode phoenix binary push")
            .expect("transfer chunk");

        assert_eq!(chunk.transfer_id, "job-123");
        assert_eq!(chunk.video_id, 123);
        assert_eq!(chunk.chunk_index, 7);
        assert_eq!(chunk.bytes, data);
    }

    fn binary_transfer_chunk_frame(
        transfer_id: &str,
        video_id: u64,
        chunk_index: u64,
        total_chunks: u64,
        bytes_sent: u64,
        total_bytes: u64,
        data: &[u8],
    ) -> Vec<u8> {
        let mut frame =
            Vec::with_capacity(TRANSFER_CHUNK_HEADER_LEN + transfer_id.len() + data.len());
        frame.extend_from_slice(TRANSFER_CHUNK_MAGIC);
        frame.push(TRANSFER_CHUNK_VERSION);
        frame.push(TRANSFER_CHUNK_TYPE);
        frame.extend_from_slice(&(transfer_id.len() as u16).to_be_bytes());
        frame.extend_from_slice(&video_id.to_be_bytes());
        frame.extend_from_slice(&chunk_index.to_be_bytes());
        frame.extend_from_slice(&total_chunks.to_be_bytes());
        frame.extend_from_slice(&bytes_sent.to_be_bytes());
        frame.extend_from_slice(&total_bytes.to_be_bytes());
        frame.extend_from_slice(&crc32fast::hash(data).to_be_bytes());
        frame.extend_from_slice(transfer_id.as_bytes());
        frame.extend_from_slice(data);
        frame
    }

    fn phoenix_binary_frame(reference: &str, topic: &str, event: &str, payload: &[u8]) -> Vec<u8> {
        let mut frame =
            Vec::with_capacity(4 + reference.len() + topic.len() + event.len() + payload.len());
        frame.push(0);
        frame.push(reference.len() as u8);
        frame.push(topic.len() as u8);
        frame.push(event.len() as u8);
        frame.extend_from_slice(reference.as_bytes());
        frame.extend_from_slice(topic.as_bytes());
        frame.extend_from_slice(event.as_bytes());
        frame.extend_from_slice(payload);
        frame
    }

    async fn expect_join<R>(reader: &mut R)
    where
        R: StreamExt<Item = std::result::Result<Message, tokio_tungstenite::tungstenite::Error>>
            + Unpin,
    {
        assert_text_message(
            reader
                .next()
                .await
                .expect("join frame")
                .expect("join message"),
            serde_json::to_value(ClientFrame::new(1, ClientEvent::Join)).expect("join frame json"),
        );
    }

    async fn expect_announce<R>(reader: &mut R, protocol_version: u64)
    where
        R: StreamExt<Item = std::result::Result<Message, tokio_tungstenite::tungstenite::Error>>
            + Unpin,
    {
        expect_announce_for_mode(reader, protocol_version, WorkerMode::CrfSearch).await;
    }

    async fn expect_announce_for_mode<R>(reader: &mut R, protocol_version: u64, mode: WorkerMode)
    where
        R: StreamExt<Item = std::result::Result<Message, tokio_tungstenite::tungstenite::Error>>
            + Unpin,
    {
        let message = reader
            .next()
            .await
            .expect("announce frame")
            .expect("announce message");
        let Message::Text(text) = message else {
            panic!("expected announce text frame");
        };
        let frame: Value = serde_json::from_str(&text).expect("announce frame json");
        assert_eq!(frame[1], "2");
        assert_eq!(frame[2], CRF_SEARCH_TOPIC);
        assert_eq!(frame[3], "announce");
        assert_eq!(frame[4]["worker_id"], "abav1-dev");
        assert_eq!(frame[4]["protocol_version"], protocol_version);
        assert_eq!(frame[4]["version"], "0.11.4");
        assert_eq!(
            frame[4]["capabilities"]["crf_search"],
            matches!(mode, WorkerMode::CrfSearch | WorkerMode::Both)
        );
        assert_eq!(
            frame[4]["capabilities"]["encode"],
            matches!(mode, WorkerMode::Encode | WorkerMode::Both)
        );
        assert_eq!(frame[4]["capabilities"]["mode"], mode.as_str());
        assert_eq!(
            frame[4]["capabilities"]["logical_cpus"],
            std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get)
        );
        assert_eq!(frame[4]["capabilities"]["max_active_jobs"], 1);
    }

    async fn expect_pull_work<R>(reader: &mut R, request_ref: u64)
    where
        R: StreamExt<Item = std::result::Result<Message, tokio_tungstenite::tungstenite::Error>>
            + Unpin,
    {
        expect_pull_work_payload(reader, request_ref, PullWorkPayload::default()).await;
    }

    async fn expect_pull_work_payload<R>(reader: &mut R, request_ref: u64, payload: PullWorkPayload)
    where
        R: StreamExt<Item = std::result::Result<Message, tokio_tungstenite::tungstenite::Error>>
            + Unpin,
    {
        assert_text_message(
            reader
                .next()
                .await
                .expect("pull_work frame")
                .expect("pull_work message"),
            serde_json::to_value(ClientFrame::new(
                request_ref,
                ClientEvent::PullWork(payload),
            ))
            .expect("pull_work frame json"),
        );
    }

    async fn expect_client_event<R>(reader: &mut R, request_ref: u64, event: &str) -> Value
    where
        R: StreamExt<Item = std::result::Result<Message, tokio_tungstenite::tungstenite::Error>>
            + Unpin,
    {
        let Message::Text(text) = reader
            .next()
            .await
            .expect("client event frame")
            .expect("client event message")
        else {
            panic!("expected text client event");
        };
        let actual: Value = serde_json::from_str(&text).expect("decode client event");
        let frame = actual.as_array().expect("client event frame array");
        assert_eq!(frame[0], json!("1"));
        assert_eq!(frame[1], json!(request_ref.to_string()));
        assert_eq!(frame[2], json!(CRF_SEARCH_TOPIC));
        assert_eq!(frame[3], json!(event));
        frame[4].clone()
    }

    async fn send_join_reply<W>(writer: &mut W)
    where
        W: SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
    {
        writer
            .send(Message::Text(
                json!([null, "1", CRF_SEARCH_TOPIC, "phx_reply", {
                    "status": "ok",
                    "response": {"worker_id": "worker-123"}
                }])
                .to_string(),
            ))
            .await
            .expect("send join reply");
    }

    async fn send_announce_reply<W>(writer: &mut W)
    where
        W: SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
    {
        writer
            .send(Message::Text(
                serde_json::to_string(&ServerFrame::<Value>::reply(
                    2,
                    ReplyBody::ok(json!({"accepted": true, "protocol_version": 1})),
                ))
                .expect("announce reply json"),
            ))
            .await
            .expect("send announce reply");
    }

    async fn send_no_work_reply<W>(writer: &mut W, request_ref: u64)
    where
        W: SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
    {
        writer
            .send(Message::Text(
                serde_json::to_string(&ServerFrame::reply(
                    request_ref,
                    ReplyBody::ok(ServerReply::NoWork(
                        crate::command::worker_protocol::NoWorkPayload {
                            status: WorkStatus::NoWork,
                        },
                    )),
                ))
                .expect("no_work reply json"),
            ))
            .await
            .expect("send pull_work reply");
    }

    async fn send_job_assigned_reply<W>(writer: &mut W)
    where
        W: SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
    {
        send_job_reply(writer, 3, WorkStatus::JobAssigned).await;
    }

    async fn send_job_reply<W>(writer: &mut W, request_ref: u64, status: WorkStatus)
    where
        W: SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
    {
        send_job_reply_with_job_id(writer, request_ref, status, "job-123").await;
    }

    async fn send_job_reply_with_job_id<W>(
        writer: &mut W,
        request_ref: u64,
        status: WorkStatus,
        job_id: &str,
    ) where
        W: SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
    {
        let chunk_size_bytes = if status == WorkStatus::JobAssigned {
            256
        } else {
            0
        };
        writer
            .send(Message::Text(
                serde_json::to_string(&ServerFrame::reply(
                    request_ref,
                    ReplyBody::ok(ServerReply::JobAssigned(JobAssignedPayload {
                        status,
                        job_type: JobKind::CrfSearch,
                        job_id: job_id.into(),
                        video_id: 123,
                        source_name: "movie.mkv".into(),
                        size_bytes: 1024,
                        chunk_size_bytes,
                        target_vmaf: 96.5,
                        transfer: None,
                        output_transfer: None,
                        output_shared_path: None,
                        encode_args: Vec::new(),
                        crf_search_args: vec![
                            "crf-search".into(),
                            "--input".into(),
                            "/server/movie.mkv".into(),
                            "--min-vmaf".into(),
                            "96.5".into(),
                        ],
                    })),
                ))
                .expect("job reply json"),
            ))
            .await
            .expect("send job reply");
    }

    async fn send_announce_error_reply<W>(writer: &mut W, request_ref: u64, response: Value)
    where
        W: SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
    {
        writer
            .send(Message::Text(
                serde_json::to_string(&crate::command::worker_protocol::ServerFrame::reply(
                    request_ref,
                    ReplyBody::error(
                        serde_json::from_value::<ErrorReplyPayload>(response)
                            .expect("error payload"),
                    ),
                ))
                .expect("announce error reply json"),
            ))
            .await
            .expect("send announce error reply");
    }

    async fn send_cancel_push<W>(writer: &mut W, job_id: &str, reason: &str)
    where
        W: SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
    {
        writer
            .send(Message::Text(
                serde_json::to_string(&ServerPushFrame::new(
                    "cancel",
                    CancelPayload {
                        job_id: job_id.into(),
                        reason: reason.into(),
                    },
                ))
                .expect("cancel push json"),
            ))
            .await
            .expect("send cancel push");
    }

    async fn send_control_push<W>(writer: &mut W, action: ControlAction, video_id: Option<u64>)
    where
        W: SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
    {
        writer
            .send(Message::Text(
                serde_json::to_string(&ServerPushFrame::new(
                    "control",
                    ControlPayload {
                        action,
                        video_id,
                        job_id: None,
                    },
                ))
                .expect("control push json"),
            ))
            .await
            .expect("send control push");
    }

    async fn serve_no_work_session(listener: TcpListener, no_work_replies: usize) {
        let (stream, _) = listener.accept().await.expect("accept connection");
        let socket = accept_async(stream).await.expect("accept websocket");
        let (mut writer, mut reader) = socket.split();

        expect_join(&mut reader).await;
        send_join_reply(&mut writer).await;
        expect_announce(&mut reader, 1).await;
        send_announce_reply(&mut writer).await;

        for request_ref in 3..(3 + no_work_replies as u64) {
            expect_pull_work(&mut reader, request_ref).await;
            send_no_work_reply(&mut writer, request_ref).await;
        }
    }

    fn assert_text_message(message: Message, expected: Value) {
        let Message::Text(text) = message else {
            panic!("expected text frame, got {message:?}");
        };
        let actual: Value = serde_json::from_str(&text).expect("decode message");
        assert_eq!(actual, expected);
    }
}
