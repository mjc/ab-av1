use crate::command::worker_protocol::{
    ActiveJobPayload, AnnouncePayload, CRF_SEARCH_TOPIC, CancelPayload, Capabilities, ClientEvent,
    ClientFrame, ControlAction, ControlPayload, ControlState, ControlStatePayload,
    CrfSearchCompletedPayload, CrfSearchProgressPayload, CrfSearchResultPayload,
    EncodeCompletedPayload, EncodeProgressPayload, ErrorReplyPayload, FailureReportPayload,
    HeartbeatPayload, JobKind, PullWorkPayload, ReplyBody, ServerPushFrame, ServerReply,
    TransferFailurePayload, TransferProgressPayload, TransferStage, TransferStartedPayload,
    WorkStatus,
};
use crate::command::worker_transfer::{Chunk, ChunkReceiver};
use crate::command::{crf_search, encode, sample_encode};
use crate::ffprobe::Ffprobe;
use crate::process::{ProcessExitError, managed::ProcessScope};
use anyhow::{Context, Result, anyhow, bail};
use clap::{Parser, ValueEnum};
use futures_util::stream::{SplitSink, SplitStream};
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{HashMap, VecDeque},
    fs, io,
    io::Read,
    num::NonZeroU64,
    path::{Path, PathBuf},
    sync::{
        Arc, Condvar, Mutex, OnceLock,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};
use sysinfo::{Disks, Pid, ProcessesToUpdate, System};
use tokio::net::TcpStream;
use tokio::sync::mpsc::{self, UnboundedReceiver, UnboundedSender};
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream,
    tungstenite::{Error as WsError, Message},
    tungstenite::{client::IntoClientRequest, http::header::ORIGIN, protocol::WebSocketConfig},
};
use tracing::{debug, info, trace, warn};

const PHOENIX_VSN: &str = "2.0.0";
const SUPPORTED_PROTOCOL_VERSION: u64 = 1;
const TRANSFER_CHUNK_MAGIC: &[u8; 4] = b"RAV1";
const TRANSFER_CHUNK_VERSION: u8 = 1;
const TRANSFER_CHUNK_TYPE: u8 = 1;
const TRANSFER_CHUNK_HEADER_LEN: usize = 52;
const MAX_TRANSFER_FRAME_BYTES: usize = 640 * 1024 * 1024;
const HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
const EVENT_ACK_TIMEOUT: Duration = Duration::from_secs(30);
const HTTP_TRANSFER_PROGRESS_INTERVAL: Duration = Duration::from_millis(500);
const HTTP_TRANSFER_IDLE_TIMEOUT: Duration = Duration::from_secs(60);
static HEARTBEAT_SYSTEM: OnceLock<Mutex<System>> = OnceLock::new();

#[derive(Debug)]
struct TransferGate {
    state: Mutex<ControlState>,
    changed: Condvar,
}

impl TransferGate {
    fn running() -> Self {
        Self {
            state: Mutex::new(ControlState::Running),
            changed: Condvar::new(),
        }
    }

    fn set(&self, state: ControlState) {
        *self.state.lock().expect("transfer gate lock") = state;
        self.changed.notify_all();
    }

    fn wait_until_running(&self) -> io::Result<()> {
        let mut state = self.state.lock().expect("transfer gate lock");
        while *state == ControlState::Paused {
            state = self.changed.wait(state).expect("transfer gate wait");
        }
        if *state == ControlState::Stopped {
            return Err(io::Error::new(
                io::ErrorKind::ConnectionAborted,
                "worker transfer stopped",
            ));
        }
        Ok(())
    }
}

struct ControlledReader<R> {
    inner: R,
    gate: Arc<TransferGate>,
}

fn worker_http_agent() -> ureq::Agent {
    ureq::Agent::config_builder()
        .timeout_recv_body(Some(HTTP_TRANSFER_IDLE_TIMEOUT))
        .timeout_send_body(Some(HTTP_TRANSFER_IDLE_TIMEOUT))
        .build()
        .into()
}

impl<R: Read> Read for ControlledReader<R> {
    fn read(&mut self, buffer: &mut [u8]) -> io::Result<usize> {
        self.gate.wait_until_running()?;
        self.inner.read(buffer)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct BytesPerSecond(Option<NonZeroU64>);

impl BytesPerSecond {
    #[must_use]
    fn from_elapsed(received_bytes: u64, elapsed: Duration) -> Self {
        let elapsed_secs = elapsed.as_secs().max(1);
        Self(NonZeroU64::new(received_bytes / elapsed_secs))
    }

    #[must_use]
    fn get(self) -> u64 {
        self.0.map_or(0, NonZeroU64::get)
    }

    #[must_use]
    fn eta(self, remaining_bytes: u64) -> Option<u64> {
        self.0
            .map(|bytes_per_second| remaining_bytes / bytes_per_second.get())
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct TransferProgressMetrics {
    received_bytes: u64,
    expected_bytes: u64,
    percent: f64,
    bytes_per_second: BytesPerSecond,
    eta: Option<u64>,
}

impl TransferProgressMetrics {
    #[must_use]
    fn new(expected_bytes: u64, received_bytes: u64, elapsed: Duration) -> Self {
        let bytes_per_second = BytesPerSecond::from_elapsed(received_bytes, elapsed);
        let remaining_bytes = expected_bytes.saturating_sub(received_bytes);
        let percent = if expected_bytes == 0 {
            100.0
        } else {
            100.0 * received_bytes as f64 / expected_bytes as f64
        };

        Self {
            received_bytes,
            expected_bytes,
            percent,
            bytes_per_second,
            eta: bytes_per_second.eta(remaining_bytes),
        }
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, ValueEnum, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
#[value(rename_all = "kebab-case")]
pub(crate) enum WorkerMode {
    #[default]
    CrfSearch,
    Encode,
    Both,
    Weighted,
}

impl WorkerMode {
    #[must_use]
    const fn as_str(self) -> &'static str {
        match self {
            Self::CrfSearch => "crf-search",
            Self::Encode => "encode",
            Self::Both => "both",
            Self::Weighted => "weighted",
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WorkerCapacity {
    logical_cpus: usize,
    mode: WorkerMode,
    max_active_jobs: Option<std::num::NonZeroUsize>,
}

impl WorkerCapacity {
    #[must_use]
    const fn new(
        logical_cpus: usize,
        mode: WorkerMode,
        max_active_jobs: Option<std::num::NonZeroUsize>,
    ) -> Self {
        Self {
            logical_cpus,
            mode,
            max_active_jobs,
        }
    }

    #[must_use]
    fn detect(mode: WorkerMode, max_active_jobs: Option<std::num::NonZeroUsize>) -> Self {
        let logical_cpus =
            std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
        Self::new(logical_cpus, mode, max_active_jobs)
    }

    #[must_use]
    const fn max_active_jobs(self) -> usize {
        if matches!(self.mode, WorkerMode::Weighted) {
            return 1;
        }
        if let Some(max_active_jobs) = self.max_active_jobs {
            return max_active_jobs.get();
        }

        match self.mode {
            WorkerMode::CrfSearch | WorkerMode::Encode | WorkerMode::Weighted => 1,
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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct WorkScheduler {
    crf_searches_per_encode: std::num::NonZeroUsize,
    crf_searches_since_encode: usize,
}

impl WorkScheduler {
    const fn new(crf_searches_per_encode: std::num::NonZeroUsize) -> Self {
        Self {
            crf_searches_per_encode,
            crf_searches_since_encode: 0,
        }
    }

    const fn preferred(self) -> JobKind {
        if self.crf_searches_since_encode < self.crf_searches_per_encode.get() {
            JobKind::CrfSearch
        } else {
            JobKind::Encode
        }
    }

    const fn record_assignment(&mut self, job_type: JobKind) {
        match job_type {
            JobKind::CrfSearch => {
                self.crf_searches_since_encode = self.crf_searches_since_encode.saturating_add(1);
            }
            JobKind::Encode => self.crf_searches_since_encode = 0,
        }
    }
}

/// Connect to a Reencodarr websocket worker endpoint and request one job.
#[derive(Parser, Debug, Clone)]
pub struct Args {
    /// Reencodarr base URL, e.g. http://127.0.0.1:4000
    #[arg(long, env = "REENCODARR_WORKER_CONNECT_URL")]
    connect: String,

    /// Worker authentication token.
    #[arg(long, env = "REENCODARR_WORKER_TOKEN")]
    token: String,

    /// Client worker id announced to Reencodarr.
    #[arg(long, env = "REENCODARR_WORKER_ID")]
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

    /// Maximum jobs to run concurrently (not supported in weighted mode).
    #[arg(long, env = "AB_AV1_WORKER_MAX_ACTIVE_JOBS")]
    max_active_jobs: Option<std::num::NonZeroUsize>,

    /// CRF search jobs selected for each encode job in weighted mode.
    #[arg(long, env = "AB_AV1_WORKER_CRF_SEARCHES_PER_ENCODE")]
    crf_searches_per_encode: Option<std::num::NonZeroUsize>,
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
    max_active_jobs: Option<std::num::NonZeroUsize>,
    crf_searches_per_encode: Option<std::num::NonZeroUsize>,
}

impl TryFrom<Args> for WorkerConfig {
    type Error = anyhow::Error;

    fn try_from(
        Args {
            connect,
            token,
            worker_id,
            version,
            protocol_version,
            once,
            local_path,
            worker_mode,
            max_active_jobs,
            crf_searches_per_encode,
        }: Args,
    ) -> Result<Self> {
        if worker_mode != WorkerMode::Weighted && crf_searches_per_encode.is_some() {
            bail!("--crf-searches-per-encode requires --worker-mode weighted");
        }
        if worker_mode == WorkerMode::Weighted && crf_searches_per_encode.is_none() {
            bail!("--worker-mode weighted requires --crf-searches-per-encode");
        }
        if worker_mode == WorkerMode::Weighted && max_active_jobs.is_some() {
            bail!(
                "--max-active-jobs is not supported with --worker-mode weighted; \
                 run multiple worker processes for parallel weighted workers"
            );
        }

        Ok(Self {
            connect,
            token,
            worker_id,
            version,
            protocol_version,
            once,
            local_path,
            worker_mode,
            max_active_jobs,
            crf_searches_per_encode,
        })
    }
}

impl WorkerConfig {
    fn crf_searches_per_encode(&self) -> std::num::NonZeroUsize {
        self.crf_searches_per_encode
            .unwrap_or(std::num::NonZeroUsize::MIN)
    }
}

#[must_use]
fn initial_pull_work_payload(mode: WorkerMode) -> PullWorkPayload {
    PullWorkPayload {
        input_missing: false,
        job_type: (mode != WorkerMode::CrfSearch).then_some(match mode {
            WorkerMode::CrfSearch | WorkerMode::Both | WorkerMode::Weighted => JobKind::CrfSearch,
            WorkerMode::Encode => JobKind::Encode,
        }),
    }
}

#[must_use]
fn next_job_type_after_no_work(mode: WorkerMode, requested: JobKind) -> Option<JobKind> {
    match (mode, requested) {
        (WorkerMode::Both, JobKind::CrfSearch) => Some(JobKind::Encode),
        (WorkerMode::Weighted, JobKind::CrfSearch) => Some(JobKind::Encode),
        (WorkerMode::Encode, JobKind::Encode) => Some(JobKind::Encode),
        (WorkerMode::CrfSearch, JobKind::CrfSearch) => None,
        (WorkerMode::Both, JobKind::Encode) => Some(JobKind::CrfSearch),
        (WorkerMode::Weighted, JobKind::Encode) => Some(JobKind::CrfSearch),
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
        let exit_code = error
            .chain()
            .find_map(|cause| cause.downcast_ref::<ProcessExitError>())
            .and_then(ProcessExitError::code)
            .unwrap_or(crate::FAILURE_EXIT_CODE);
        let no_good_crf =
            error
                .chain()
                .find_map(|cause| match cause.downcast_ref::<crf_search::Error>() {
                    Some(crf_search::Error::NoGoodCrf { last }) => Some(last),
                    Some(crf_search::Error::Other(_)) | None => None,
                });
        let max_encoded_percent = self
            .crf_search_config()
            .ok()
            .map(|config| config.max_encoded_percent.get());
        let category = match (no_good_crf, max_encoded_percent) {
            (Some(last), Some(max)) if last.enc.encode_percent > max => "size_limits",
            (Some(_), _) => "crf_optimization",
            (None, _) => "process_failure",
        };
        let code = match category {
            "size_limits" => "SIZE_LIMIT".to_owned(),
            "crf_optimization" => "CRF_NOT_FOUND".to_owned(),
            _ => format!("EXIT_{exit_code}"),
        };
        let argv = match self.assignment.job_type {
            JobKind::CrfSearch => &self.assignment.crf_search_args,
            JobKind::Encode => &self.assignment.encode_args,
        };

        FailureReportPayload {
            job_id: self.assignment.job_id.clone(),
            video_id: self.assignment.video_id,
            stage: match self.assignment.job_type {
                JobKind::CrfSearch => "crf_search",
                JobKind::Encode => "encoding",
            }
            .into(),
            category: category.into(),
            message: error.to_string(),
            code,
            context: json!({
                "job_id": self.assignment.job_id,
                "source_name": self.assignment.source_name,
                "exit_code": exit_code,
                "error_chain": format!("{error:#}"),
                "argv": argv,
                "encode_percent": no_good_crf.map(|last| last.enc.encode_percent),
                "max_encoded_percent": max_encoded_percent,
            }),
            retriable: false,
            stderr_excerpt: Some(format!("{error:#}")),
        }
    }

    fn control_state_payload(
        &self,
        control: &ControlPayload,
        state: ControlState,
    ) -> ControlStatePayload {
        ControlStatePayload {
            state,
            active_video_id: Some(self.assignment.video_id),
            job_id: Some(self.assignment.job_id.clone()),
            command_id: control.command_id.clone(),
        }
    }
}

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct WorkerJobReportState {
    #[serde(skip_serializing_if = "Option::is_none")]
    control_state: Option<ControlStatePayload>,
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
        let progress = TransferProgressMetrics::new(
            self.job.assignment.size_bytes,
            received_bytes,
            self.transfer_started_at.elapsed(),
        );

        TransferProgressPayload {
            job_id: self.job.assignment.job_id.clone(),
            transfer_id: self.job.assignment.job_id.clone(),
            video_id: self.job.assignment.video_id,
            filename: self.job.assignment.source_name.clone(),
            received_bytes: progress.received_bytes,
            expected_bytes: Some(progress.expected_bytes),
            percent: progress.percent,
            bytes_per_second: progress.bytes_per_second.get(),
            eta: progress.eta,
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
                | Some(Ok(crf_search::Update::SampleEncodeDone(_)))
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
                    frame = current_worker.next_frame() => {
                        match frame {
                            Ok(WorkerFrame::Push(WorkerPush::Cancel(cancel)))
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
                            Ok(WorkerFrame::Push(WorkerPush::Control(control)))
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
                            Ok(_) => {}
                            Err(error) => {
                                debug!(job_id = %job.assignment.job_id, %error, "worker connection lost during job");
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
        Ok(crf_search::Update::SampleEncodeDone(_)) => Ok((None, false)),
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
        system.refresh_cpu_all();
        Mutex::new(system)
    });
    let mut system = system.lock().expect("heartbeat system lock");
    system.refresh_cpu_all();
    system.refresh_memory();

    let pid = Pid::from_u32(std::process::id());
    system.refresh_processes(ProcessesToUpdate::Some(&[pid]), true);
    let memory_rss_bytes = system.process(pid).map(|process| process.memory());

    let disks = Disks::new_with_refreshed_list();
    let disk = disks
        .iter()
        .filter(|disk| path.starts_with(disk.mount_point()))
        .max_by_key(|disk| disk.mount_point().as_os_str().len());

    HeartbeatPayload {
        cpu_percent: Some(system.global_cpu_usage()),
        memory_rss_bytes,
        memory_total_bytes: Some(system.total_memory()),
        disk_free_bytes: disk.map(|disk| disk.available_space()),
        disk_total_bytes: disk.map(|disk| disk.total_space()),
        active_video_id,
    }
}

fn worker_job_input_dir(job_id: &str) -> PathBuf {
    std::env::temp_dir().join(format!("ab-av1-worker-{}", job_id))
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
    offline_job_timeout: Duration,
    max_pulls: Option<usize>,
}

impl Default for WorkerRuntime {
    fn default() -> Self {
        Self {
            idle_delay: Duration::from_secs(5),
            reconnect_base_delay: Duration::from_secs(1),
            reconnect_max_delay: Duration::from_secs(30),
            offline_job_timeout: Duration::from_secs(90),
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
enum WorkerFrame {
    Push(WorkerPush),
    Text(String),
    Binary(Vec<u8>),
    Ping(Vec<u8>),
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
        result: anyhow::Result<WorkerJobOutcome>,
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

#[derive(Debug, Clone)]
struct PendingEventAck {
    job_id: String,
    name: &'static str,
    sent_at: Instant,
}

impl PendingEventAck {
    fn new(job_id: impl Into<String>, name: &'static str) -> Self {
        Self {
            job_id: job_id.into(),
            name,
            sent_at: Instant::now(),
        }
    }

    fn deadline(&self, timeout: Duration) -> Instant {
        self.sent_at + timeout
    }
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

    async fn send_event(&mut self, event: ClientEvent) -> Result<String> {
        let reference = self.next_ref;
        self.next_ref += 1;
        send_json(&mut self.writer, ClientFrame::new(reference, event)).await?;
        Ok(reference.to_string())
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
            .send(Message::Pong(payload.into()))
            .await
            .context("send websocket pong")
    }

    async fn next_frame(&mut self) -> Result<WorkerFrame> {
        loop {
            match decode_worker_frame(self.reader.next().await)? {
                Some(WorkerFrame::Ping(payload)) => self.send_pong(payload).await?,
                Some(frame) => return Ok(frame),
                None => {}
            }
        }
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
    let mut cleanup_guard = WorkerInputCleanup::new(&job);
    let process_scope = ProcessScope::new(job_id.clone());
    let result = process_scope
        .run(run_multiplex_job_inner(&job, &mut commands, &output))
        .await;
    match cleanup_multiplex_worker_input(&job, &result) {
        Ok(()) => cleanup_guard.disarm(),
        Err(error) => {
            debug!(
                job_id = %job.assignment.job_id,
                error = %error,
                "failed to remove completed multiplexed worker job directory"
            );
        }
    }
    let _ = output.send(MultiplexOutput::Done { job_id, result });
}

struct WorkerInputCleanup {
    input_dir: Option<PathBuf>,
}

impl WorkerInputCleanup {
    fn new(job: &WorkerJob) -> Self {
        let input_dir = (job.input_dir == worker_job_input_dir(&job.assignment.job_id))
            .then(|| job.input_dir.clone());
        Self { input_dir }
    }

    fn disarm(&mut self) {
        self.input_dir = None;
    }
}

impl Drop for WorkerInputCleanup {
    fn drop(&mut self) {
        if let Some(input_dir) = self.input_dir.take() {
            let _ = fs::remove_dir_all(input_dir);
        }
    }
}

fn cleanup_multiplex_worker_input(
    job: &WorkerJob,
    outcome: &Result<WorkerJobOutcome>,
) -> Result<()> {
    if outcome.is_err() || matches!(outcome, Ok(WorkerJobOutcome::Stopped)) {
        return remove_worker_input(job);
    }

    if matches!(outcome, Ok(WorkerJobOutcome::Completed)) {
        let retain_local_output = job.assignment.job_type == JobKind::Encode
            && job.assignment.output_transfer.is_none()
            && job.assignment.output_shared_path.is_none();
        if retain_local_output
            && job.input_dir == worker_job_input_dir(&job.assignment.job_id)
            && job.input_path().starts_with(&job.input_dir)
        {
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

fn apply_job_control(
    job: &WorkerJob,
    control: &ControlPayload,
    output: &UnboundedSender<MultiplexOutput>,
    process_scope: Option<&ProcessScope>,
    paused: &mut bool,
) -> Result<Option<WorkerJobOutcome>> {
    let (state, outcome) = match control.action {
        ControlAction::Pause => {
            if let Some(scope) = process_scope {
                scope.pause()?;
            }
            *paused = true;
            (ControlState::Paused, None)
        }
        ControlAction::Resume | ControlAction::Start => {
            if let Some(scope) = process_scope {
                scope.resume()?;
            }
            *paused = false;
            (ControlState::Running, None)
        }
        ControlAction::Stop => {
            if let Some(scope) = process_scope {
                scope.stop()?;
            }
            (ControlState::Stopped, Some(WorkerJobOutcome::Stopped))
        }
    };
    let _ = multiplex_event(
        output,
        &job.assignment.job_id,
        ClientEvent::ControlState(job.control_state_payload(control, state)),
        "control_state",
    );
    Ok(outcome)
}

fn apply_transfer_control(
    job: &WorkerJob,
    control: &ControlPayload,
    output: &UnboundedSender<MultiplexOutput>,
    gate: &TransferGate,
    paused: &mut bool,
) -> Result<Option<WorkerJobOutcome>> {
    let outcome = apply_job_control(job, control, output, None, paused)?;
    gate.set(match control.action {
        ControlAction::Pause => ControlState::Paused,
        ControlAction::Resume | ControlAction::Start => ControlState::Running,
        ControlAction::Stop => ControlState::Stopped,
    });
    Ok(outcome)
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
        if let Some(outcome) = download_multiplex_input(job, commands, output).await? {
            return Ok(outcome);
        }
    } else if matches!(phase, WorkerJobPhase::AwaitingInput(_))
        && let Some(outcome) = receive_multiplex_input(job, commands, output).await?
    {
        return Ok(outcome);
    }

    let probe = match probe_multiplex_input(job, commands, output).await? {
        Ok(probe) => probe,
        Err(outcome) => return Ok(outcome),
    };
    match job.assignment.job_type {
        JobKind::CrfSearch => run_multiplex_crf(job, probe, commands, output).await,
        JobKind::Encode => run_multiplex_encode(job, probe, commands, output).await,
    }
}

async fn probe_multiplex_input(
    job: &WorkerJob,
    commands: &mut UnboundedReceiver<JobCommand>,
    output: &UnboundedSender<MultiplexOutput>,
) -> Result<std::result::Result<Arc<Ffprobe>, WorkerJobOutcome>> {
    let input = job.input_path().to_path_buf();
    let probe = async move { Arc::new(crate::ffprobe::probe_managed(&input).await) };
    drive_multiplex_probe(job, commands, output, probe).await
}

async fn drive_multiplex_probe<F>(
    job: &WorkerJob,
    commands: &mut UnboundedReceiver<JobCommand>,
    output: &UnboundedSender<MultiplexOutput>,
    probe: F,
) -> Result<std::result::Result<Arc<Ffprobe>, WorkerJobOutcome>>
where
    F: std::future::Future<Output = Arc<Ffprobe>>,
{
    let process_scope = ProcessScope::new(job.assignment.job_id.clone());
    let probe = process_scope.run(probe);
    tokio::pin!(probe);
    let mut paused = false;
    loop {
        tokio::select! {
            result = &mut probe, if !paused => {
                return Ok(Ok(result));
            }
            command = commands.recv() => match command {
                Some(JobCommand::Control(control)) => {
                    if let Some(outcome) =
                        apply_job_control(job, &control, output, Some(&process_scope), &mut paused)?
                    {
                        return Ok(Err(outcome));
                    }
                }
                Some(JobCommand::Cancel(cancel)) => {
                    process_scope.stop()?;
                    bail!("worker job {} canceled: {}", cancel.job_id, cancel.reason);
                }
                Some(JobCommand::TransferStarted(_) | JobCommand::TransferChunk(_)) => {}
                None => {
                    process_scope.stop()?;
                    bail!("worker command channel closed while probing input");
                }
            }
        }
    }
}

async fn download_multiplex_input(
    job: &WorkerJob,
    commands: &mut UnboundedReceiver<JobCommand>,
    output: &UnboundedSender<MultiplexOutput>,
) -> Result<Option<WorkerJobOutcome>> {
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
    let gate = Arc::new(TransferGate::running());
    let copy_received = Arc::clone(&received);
    let copy_gate = Arc::clone(&gate);
    let copy_job_id = job_id.clone();
    let copy = tokio::task::spawn_blocking(move || -> Result<u64> {
        let response = worker_http_agent()
            .get(&transfer.url)
            .header(&transfer.auth.header, &transfer.auth.value)
            .call()
            .map_err(|error| {
                anyhow!("HTTP input download failed for job {copy_job_id}: {error}")
            })?;
        let reader = ControlledReader {
            inner: CountingReader {
                inner: response.into_body().into_reader(),
                received: copy_received,
            },
            gate: copy_gate,
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
    let mut paused = false;
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
                return Ok(None);
            }
            command = commands.recv() => match command {
                Some(JobCommand::Control(control)) => {
                    if let Some(outcome) =
                        apply_transfer_control(job, &control, output, &gate, &mut paused)?
                    {
                        let _ = (&mut copy).await;
                        return Ok(Some(outcome));
                    }
                }
                Some(JobCommand::Cancel(cancel)) => {
                    gate.set(ControlState::Stopped);
                    let _ = (&mut copy).await;
                    bail!("worker job {} canceled: {}", cancel.job_id, cancel.reason);
                }
                Some(JobCommand::TransferStarted(_) | JobCommand::TransferChunk(_)) => {}
                None => {
                    gate.set(ControlState::Stopped);
                    let _ = (&mut copy).await;
                    bail!("worker command channel closed while downloading input");
                }
            }
        }
    }
}

async fn receive_multiplex_input(
    job: &WorkerJob,
    commands: &mut UnboundedReceiver<JobCommand>,
    output: &UnboundedSender<MultiplexOutput>,
) -> Result<Option<WorkerJobOutcome>> {
    let mut pending = PendingJob::waiting(job.clone());
    let mut heartbeat = tokio::time::interval(HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut paused = false;
    let mut deferred = VecDeque::new();

    loop {
        let deferred_command = (!paused).then(|| deferred.pop_front()).flatten();
        if let Some(command) = deferred_command {
            if apply_input_command(&mut pending, command, output)? {
                return Ok(None);
            }
            continue;
        }

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
                Some(command @ (JobCommand::TransferStarted(_) | JobCommand::TransferChunk(_)))
                    if paused =>
                {
                    deferred.push_back(command);
                }
                Some(command @ (JobCommand::TransferStarted(_) | JobCommand::TransferChunk(_))) => {
                    if apply_input_command(&mut pending, command, output)? {
                        return Ok(None);
                    }
                }
                Some(JobCommand::Cancel(cancel)) => {
                    bail!("worker job {} canceled: {}", cancel.job_id, cancel.reason);
                }
                Some(JobCommand::Control(control)) => {
                    if let Some(outcome) =
                        apply_job_control(job, &control, output, None, &mut paused)?
                    {
                        return Ok(Some(outcome));
                    }
                }
                None => bail!("worker command channel closed while receiving input"),
            }
        }
    }
}

fn apply_input_command(
    pending: &mut PendingJob,
    command: JobCommand,
    output: &UnboundedSender<MultiplexOutput>,
) -> Result<bool> {
    match command {
        JobCommand::TransferStarted(started) => {
            pending.ensure_receiver(started.chunk_size_bytes)?;
        }
        JobCommand::TransferChunk(chunk) => {
            let chunk_index = chunk.chunk_index;
            let total_chunks = chunk.total_chunks;
            pending.apply_raw_chunk(chunk)?;
            let progress = pending.transfer_progress_payload(chunk_index, total_chunks);
            let _ = multiplex_event(
                output,
                &pending.job.assignment.job_id,
                ClientEvent::TransferProgress(progress),
                "transfer_progress",
            );
            if pending.receiver.as_ref().is_some_and(|receiver| {
                receiver.received_bytes() == pending.job.assignment.size_bytes
            }) {
                pending.finish()?;
                return Ok(true);
            }
        }
        JobCommand::Cancel(_) | JobCommand::Control(_) => {
            unreachable!("only input transfer commands are deferred")
        }
    }
    Ok(false)
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
                    if let Some(outcome) =
                        apply_job_control(job, &control, output, Some(&process_scope), &mut paused)?
                    {
                        return Ok(outcome);
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
                    Some(Ok(crf_search::Update::SampleResult { .. }))
                    | Some(Ok(crf_search::Update::SampleEncodeDone(_))) => {}
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
                if let Some(transfer) = &job.assignment.output_transfer
                    && let Some(outcome) = upload_multiplex_output(
                        job,
                        &output_path,
                        transfer,
                        output_bytes,
                        commands,
                        output,
                    )
                    .await?
                    {
                        return Ok(outcome);
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
                Some(JobCommand::Control(control)) => {
                    if let Some(outcome) =
                        apply_job_control(job, &control, output, Some(&process_scope), &mut paused)?
                    {
                        return Ok(outcome);
                    }
                }
                Some(JobCommand::TransferStarted(_) | JobCommand::TransferChunk(_)) => {}
                None => bail!("worker command channel closed while running encode"),
            }
        }
    }
}

async fn upload_multiplex_output(
    job: &WorkerJob,
    output_path: &Path,
    transfer: &crate::command::worker_protocol::TransferSpec,
    output_bytes: u64,
    commands: &mut UnboundedReceiver<JobCommand>,
    output: &UnboundedSender<MultiplexOutput>,
) -> Result<Option<WorkerJobOutcome>> {
    let output_path = output_path.to_path_buf();
    let transfer = transfer.clone();
    let job_id = job.assignment.job_id.clone();
    let gate = Arc::new(TransferGate::running());
    let copy_gate = Arc::clone(&gate);
    let upload = tokio::task::spawn_blocking(move || -> Result<()> {
        let file = fs::File::open(&output_path).with_context(|| {
            format!(
                "open encoded output for upload for job {job_id}: {}",
                output_path.display()
            )
        })?;
        let reader = ControlledReader {
            inner: file,
            gate: copy_gate,
        };
        worker_http_agent()
            .put(&transfer.url)
            .header(&transfer.auth.header, &transfer.auth.value)
            .header("Content-Length", output_bytes.to_string())
            .send(ureq::SendBody::from_owned_reader(reader))
            .map_err(|error| anyhow!("HTTP output upload failed for job {job_id}: {error}"))?;
        Ok(())
    });
    tokio::pin!(upload);
    let mut paused = false;
    loop {
        tokio::select! {
            result = &mut upload => {
                result.context("join HTTP output upload task")??;
                return Ok(None);
            }
            command = commands.recv() => match command {
                Some(JobCommand::Control(control)) => {
                    if let Some(outcome) =
                        apply_transfer_control(job, &control, output, &gate, &mut paused)?
                    {
                        let _ = (&mut upload).await;
                        return Ok(Some(outcome));
                    }
                }
                Some(JobCommand::Cancel(cancel)) => {
                    gate.set(ControlState::Stopped);
                    let _ = (&mut upload).await;
                    bail!("worker job {} canceled: {}", cancel.job_id, cancel.reason);
                }
                Some(JobCommand::TransferStarted(_) | JobCommand::TransferChunk(_)) => {}
                None => {
                    gate.set(ControlState::Stopped);
                    let _ = (&mut upload).await;
                    bail!("worker command channel closed while uploading output");
                }
            }
        }
    }
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
        let capacity = WorkerCapacity::detect(config.worker_mode, config.max_active_jobs);
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
                            WorkerMode::CrfSearch | WorkerMode::Both | WorkerMode::Weighted
                        ),
                        encode: matches!(
                            config.worker_mode,
                            WorkerMode::Encode | WorkerMode::Both | WorkerMode::Weighted
                        ),
                        mode: config.worker_mode.as_str().into(),
                        logical_cpus: capacity.logical_cpus,
                        max_active_jobs: capacity.max_active_jobs(),
                        crf_searches_per_encode: config
                            .crf_searches_per_encode
                            .map(std::num::NonZeroUsize::get),
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
        loop {
            match self.next_frame().await? {
                WorkerFrame::Push(WorkerPush::Control(control)) => {
                    self.pending_control = Some(control.action);
                }
                WorkerFrame::Text(text) => {
                    if let Some(reply) = decode_expected_reply(&text, &expected_ref, "pull_work")? {
                        return reply;
                    }
                }
                WorkerFrame::Push(_) | WorkerFrame::Binary(_) | WorkerFrame::Ping(_) => {}
            }
        }
    }

    fn take_pending_control(&mut self) -> Option<ControlAction> {
        self.pending_control.take()
    }

    async fn send_event(&mut self, event: ClientEvent) -> Result<()> {
        let request_ref = self.next_ref;
        self.next_ref += 1;
        send_json(&mut self.socket, ClientFrame::new(request_ref, event)).await
    }

    async fn next_frame(&mut self) -> Result<WorkerFrame> {
        loop {
            match decode_worker_frame(self.socket.next().await)? {
                Some(WorkerFrame::Ping(payload)) => {
                    self.socket
                        .send(Message::Pong(payload.into()))
                        .await
                        .context("send websocket pong")?;
                }
                Some(frame) => return Ok(frame),
                None => {}
            }
        }
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
            command_id: None,
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
            frame = self.next_frame() => {
                match frame {
                    Ok(WorkerFrame::Push(WorkerPush::Cancel(cancel)))
                        if cancel.job_id == pending_job.job().assignment.job_id =>
                    {
                        eprintln!(
                            "worker job {} canceled: {}",
                            cancel.job_id, cancel.reason
                        );
                        Ok(PendingJobOutcome::Canceled)
                    }
                    Ok(WorkerFrame::Push(WorkerPush::Started(started)))
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
                    Ok(WorkerFrame::Push(WorkerPush::Control(control))) => match control.action {
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
                    Ok(WorkerFrame::Binary(bytes)) => {
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
                    Ok(WorkerFrame::Push(_) | WorkerFrame::Text(_) | WorkerFrame::Ping(_)) => {
                        Ok(PendingJobOutcome::Waiting)
                    }
                    Err(error) => {
                        Err(error).context("read worker websocket frame while waiting for input")
                    }
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
                frame = self.next_frame() => match frame? {
                    WorkerFrame::Push(WorkerPush::Control(control)) => {
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
                    WorkerFrame::Push(_)
                    | WorkerFrame::Text(_)
                    | WorkerFrame::Binary(_)
                    | WorkerFrame::Ping(_) => {}
                },
                _ = tokio::time::sleep(idle_delay) => {
                    self.send_event(ClientEvent::Heartbeat(heartbeat_payload(&std::env::temp_dir(), None))).await?;
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
    if job.input_dir != worker_job_input_dir(&job.assignment.job_id) || !job.input_dir.exists() {
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
    let input_dir = worker_job_input_dir(&assignment.job_id);
    fs::create_dir_all(&input_dir).context("create worker job dir")?;
    let input_path = local_path
        .map(Path::to_path_buf)
        .map(Ok)
        .unwrap_or_else(|| {
            offered_local_input(&assignment)
                .map(Ok)
                .unwrap_or_else(|| worker_job_input_path(&input_dir, &assignment))
        })?;
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
        let response = worker_http_agent()
            .get(&transfer.url)
            .header(&transfer.auth.header, &transfer.auth.value)
            .call()
            .map_err(|error| anyhow!("HTTP input download failed for job {job_id}: {error}"))?;

        let reader = CountingReader {
            inner: response.into_body().into_reader(),
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
    let progress =
        TransferProgressMetrics::new(expected_size, received_bytes, started_at.elapsed());

    TransferProgressPayload {
        job_id: job_id.to_owned(),
        transfer_id: job_id.to_owned(),
        video_id,
        filename: filename.to_owned(),
        received_bytes: progress.received_bytes,
        expected_bytes: Some(progress.expected_bytes),
        percent: progress.percent,
        bytes_per_second: progress.bytes_per_second.get(),
        eta: progress.eta,
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
    let capacity = WorkerCapacity::detect(config.worker_mode, config.max_active_jobs);
    info!(
        worker_id = %connected.assigned_worker_id,
        protocol_version = connected.negotiated_protocol_version,
        mode = %config.worker_mode.as_str(),
        max_active_jobs = capacity.max_active_jobs(),
        "worker connected"
    );
    let mut connection = Some(MultiplexedWorker::from_connected(connected));
    let (output, mut outputs) = mpsc::unbounded_channel();
    let mut jobs: HashMap<String, MultiplexJob> = HashMap::new();
    let mut pending: HashMap<String, (JobKind, Option<String>)> = HashMap::new();
    let mut pending_acks: HashMap<String, PendingEventAck> = HashMap::new();
    let mut no_work = HashMap::new();
    // The ratio survives websocket reconnects but intentionally resets when the worker process does.
    let mut scheduler = WorkScheduler::new(config.crf_searches_per_encode());
    let mut reconnect = tokio::time::interval(runtime.reconnect_base_delay);
    reconnect.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    let mut heartbeat = tokio::time::interval(HEARTBEAT_INTERVAL);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    heartbeat.tick().await;
    let mut offline_deadline = None;

    loop {
        if let Some(worker) = connection.as_mut() {
            schedule_multiplex_pulls(
                worker,
                config.worker_mode,
                capacity.max_active_jobs(),
                &scheduler,
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
            let event_ack_deadline = next_event_ack_deadline(&pending_acks, EVENT_ACK_TIMEOUT);
            tokio::select! {
                item = outputs.recv() => {
                    connection_lost = !handle_multiplex_output(
                        worker,
                        item,
                        &mut jobs,
                        &mut pending,
                        &mut pending_acks,
                    ).await?;
                }
                frame = worker.next_frame() => {
                    connection_lost = !handle_multiplex_frame(
                        frame,
                        &mut jobs,
                        &mut pending,
                        &mut no_work,
                        &mut pending_acks,
                        &mut scheduler,
                        &output,
                        completed_pulls,
                        config.local_path.as_deref(),
                    )?;
                }
                _ = heartbeat.tick() => {
                    connection_lost = !send_multiplex_heartbeat(
                        worker,
                        &jobs,
                        &mut pending_acks,
                    ).await;
                }
                _ = wait_for_deadline(event_ack_deadline) => {
                    warn!("worker channel acknowledgement timed out; reconnecting");
                    connection_lost = true;
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
                            let worker_id = candidate.assigned_worker_id.clone();
                            let mut candidate = MultiplexedWorker::from_connected(candidate);
                            pending_acks.clear();
                            if replay_multiplex_jobs(&mut candidate, &jobs, &mut pending_acks).await {
                                info!(
                                    %worker_id,
                                    active_jobs = jobs.len(),
                                    "worker reconnected"
                                );
                                connection = Some(candidate);
                                offline_deadline = None;
                                reconnect = tokio::time::interval(runtime.reconnect_base_delay);
                                reconnect.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                                heartbeat = tokio::time::interval(HEARTBEAT_INTERVAL);
                                heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                                heartbeat.tick().await;
                            }
                        }
                        Err(error) => {
                            trace!(error = %error, "multiplexed worker reconnect attempt failed");
                        }
                    }
                }
                _ = wait_for_deadline(offline_deadline) => {
                    expire_offline_jobs(&mut jobs, &mut pending, &mut outputs).await?;
                    bail!("coordinator unavailable until worker job lease expired");
                }
            }
        }

        if connection_lost {
            connection = None;
            if !jobs.is_empty() {
                offline_deadline = Some(Instant::now() + runtime.offline_job_timeout);
            }
            pending.clear();
            no_work.clear();
            pending_acks.clear();
        }

        if runtime.max_pulls.is_some_and(|max| *completed_pulls >= max) && jobs.is_empty() {
            return Ok(());
        }
    }
}

async fn wait_for_deadline(deadline: Option<Instant>) {
    match deadline {
        Some(deadline) => {
            tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)).await;
        }
        None => std::future::pending().await,
    }
}

fn next_event_ack_deadline(
    pending_acks: &HashMap<String, PendingEventAck>,
    timeout: Duration,
) -> Option<Instant> {
    pending_acks.values().map(|ack| ack.deadline(timeout)).min()
}

async fn send_multiplex_heartbeat(
    worker: &mut MultiplexedWorker,
    jobs: &HashMap<String, MultiplexJob>,
    pending_acks: &mut HashMap<String, PendingEventAck>,
) -> bool {
    if pending_acks.values().any(|ack| ack.name == "heartbeat") {
        return true;
    }

    let active_job = jobs.values().find(|job| !job.finished);
    let path = worker_heartbeat_path(active_job);
    let active_video_id = active_job.map(|job| job.job.assignment.video_id);

    match worker
        .send_event(ClientEvent::Heartbeat(heartbeat_payload(
            &path,
            active_video_id,
        )))
        .await
    {
        Ok(reference) => {
            pending_acks.insert(reference, PendingEventAck::new("", "heartbeat"));
            true
        }
        Err(error) => {
            debug!(error = %error, "multiplexed worker heartbeat failed");
            false
        }
    }
}

fn worker_heartbeat_path(active_job: Option<&MultiplexJob>) -> PathBuf {
    active_job
        .map(|job| job.job.input_dir.clone())
        .unwrap_or_else(std::env::temp_dir)
}

async fn expire_offline_jobs(
    jobs: &mut HashMap<String, MultiplexJob>,
    pending: &mut HashMap<String, (JobKind, Option<String>)>,
    outputs: &mut UnboundedReceiver<MultiplexOutput>,
) -> Result<()> {
    for (job_id, job) in &*jobs {
        if !job.finished {
            job.command
                .send(JobCommand::Cancel(CancelPayload {
                    job_id: job_id.clone(),
                    reason: "worker job lease expired while coordinator was unavailable".into(),
                }))
                .map_err(|_| anyhow!("worker job {job_id} command channel closed"))?;
        }
    }

    while jobs.values().any(|job| !job.finished) {
        let item = outputs
            .recv()
            .await
            .context("worker output channel closed while expiring offline jobs")?;
        handle_multiplex_output_offline(item, jobs, pending)?;
    }

    for job in jobs.values() {
        remove_worker_input(&job.job)?;
    }
    jobs.clear();
    pending.clear();
    Ok(())
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
                    job.state.failure = Some(job.job.failure_payload(&error));
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
    scheduler: &WorkScheduler,
    jobs: &HashMap<String, MultiplexJob>,
    pending: &mut HashMap<String, (JobKind, Option<String>)>,
    no_work: &HashMap<JobKind, bool>,
    completed_pulls: &mut usize,
    max_pulls: Option<usize>,
) -> Result<()> {
    while jobs.len() + pending.len() < max_active_jobs
        && max_pulls.is_none_or(|max| *completed_pulls + pending.len() < max)
    {
        if mode == WorkerMode::Weighted && !pending.is_empty() {
            break;
        }
        let Some(job_type) = next_multiplex_job_type(mode, scheduler, jobs, pending, no_work)
        else {
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
    scheduler: &WorkScheduler,
    jobs: &HashMap<String, MultiplexJob>,
    pending: &HashMap<String, (JobKind, Option<String>)>,
    no_work: &HashMap<JobKind, bool>,
) -> Option<JobKind> {
    if mode == WorkerMode::Weighted && (!jobs.is_empty() || !pending.is_empty()) {
        return None;
    }

    let occupied = |kind| {
        jobs.values().any(|job| job.job.assignment.job_type == kind)
            || pending.values().any(|(job_type, _)| *job_type == kind)
    };
    let allowed = match mode {
        WorkerMode::CrfSearch => [Some(JobKind::CrfSearch), None],
        WorkerMode::Encode => [Some(JobKind::Encode), None],
        WorkerMode::Both => [Some(JobKind::CrfSearch), Some(JobKind::Encode)],
        WorkerMode::Weighted => [
            Some(scheduler.preferred()),
            Some(match scheduler.preferred() {
                JobKind::CrfSearch => JobKind::Encode,
                JobKind::Encode => JobKind::CrfSearch,
            }),
        ],
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
    pending_acks: &mut HashMap<String, PendingEventAck>,
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
            Ok(send_multiplex_event(worker, event, &job_id, name, pending_acks).await)
        }
        MultiplexOutput::Done { job_id, result } => {
            let Some(job) = jobs.get_mut(&job_id) else {
                return Ok(true);
            };
            job.finished = true;
            match &result {
                Ok(outcome) => info!(%job_id, ?outcome, "worker job finished"),
                Err(error) => warn!(%job_id, %error, "worker job failed"),
            }
            if let Err(error) = result {
                let failure = job.job.failure_payload(&error);
                job.state.failure = Some(failure.clone());
                if !send_multiplex_event(
                    worker,
                    ClientEvent::VideoFailed(failure),
                    &job_id,
                    "video_failed",
                    pending_acks,
                )
                .await
                {
                    return Ok(false);
                }
            }
            remove_finished_job_if_acknowledged(&job_id, jobs, pending, pending_acks);
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
                pending_acks,
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
    pending_acks: &mut HashMap<String, PendingEventAck>,
) -> bool {
    match worker.send_event(event).await {
        Ok(reference) => {
            if event_requires_ack(name) {
                pending_acks.insert(reference, PendingEventAck::new(job_id, name));
            }
            true
        }
        Err(error) => {
            debug!(job_id = %job_id, event = name, error = %error, "multiplexed worker send failed");
            false
        }
    }
}

fn event_requires_ack(name: &str) -> bool {
    matches!(
        name,
        "job_active"
            | "crf_search_completed"
            | "encode_completed"
            | "video_failed"
            | "control_state"
    )
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
        ClientEvent::ControlState(payload) => state.control_state = Some(payload.clone()),
        ClientEvent::Join
        | ClientEvent::Announce(_)
        | ClientEvent::JobActive(_)
        | ClientEvent::PullWork(_)
        | ClientEvent::TransferFailure(_) => {}
    }
}

#[allow(clippy::too_many_arguments)]
fn handle_multiplex_frame(
    frame: Result<WorkerFrame>,
    jobs: &mut HashMap<String, MultiplexJob>,
    pending: &mut HashMap<String, (JobKind, Option<String>)>,
    no_work: &mut HashMap<JobKind, bool>,
    pending_acks: &mut HashMap<String, PendingEventAck>,
    scheduler: &mut WorkScheduler,
    output: &UnboundedSender<MultiplexOutput>,
    completed_pulls: &mut usize,
    local_path: Option<&Path>,
) -> Result<bool> {
    let frame = match frame {
        Ok(frame) => frame,
        Err(error) => {
            debug!(%error, "multiplexed worker connection lost");
            return Ok(false);
        }
    };
    match frame {
        WorkerFrame::Binary(bytes) => {
            if let Some(chunk) = decode_binary_worker_push(&bytes)?
                && let Some(job) = jobs.get(&chunk.transfer_id)
            {
                job.command
                    .send(JobCommand::TransferChunk(chunk))
                    .map_err(|_| anyhow!("worker input command channel closed"))?;
            }
            Ok(true)
        }
        WorkerFrame::Text(text) => {
            let mut event_ack = None;
            for (reference, ack) in pending_acks.iter() {
                if let Some(decoded) = decode_expected_reply::<Value>(&text, reference, ack.name)? {
                    event_ack = Some((reference.clone(), ack.clone(), decoded));
                    break;
                }
            }
            if let Some((reference, ack, decoded)) = event_ack {
                pending_acks.remove(&reference);
                match decoded {
                    Ok(response) => {
                        handle_multiplex_ack(&ack, &response, jobs, pending)?;
                        return Ok(true);
                    }
                    Err(error) => {
                        warn!(
                            job_id = %ack.job_id,
                            event = ack.name,
                            error = %error,
                            "multiplexed worker event was rejected; keeping connection"
                        );
                        return Ok(true);
                    }
                }
            }

            let mut reply = None;
            for reference in pending.keys() {
                if let Some(decoded) =
                    decode_expected_reply::<ServerReply>(&text, reference, "pull_work")?
                {
                    reply = Some((reference.clone(), decoded));
                    break;
                }
            }
            if let Some((reference, response)) = reply {
                let (job_type, resend_for) = pending
                    .remove(&reference)
                    .expect("pending pull reference still present");
                let response = match response {
                    Ok(response) => response,
                    Err(error) => {
                        warn!(error = %error, "pull_work reply failed; reconnecting worker");
                        return Ok(false);
                    }
                };
                *completed_pulls += 1;
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
                            scheduler.record_assignment(job_type);
                            let job = build_worker_job(assignment, local_path)?;
                            let job_id = job.assignment.job_id.clone();
                            info!(
                                %job_id,
                                video_id = job.assignment.video_id,
                                job_type = ?job.assignment.job_type,
                                source_name = %job.assignment.source_name,
                                "worker job assigned"
                            );
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

            Ok(true)
        }
        WorkerFrame::Push(WorkerPush::Cancel(cancel)) => {
            if let Some(job) = jobs.get(&cancel.job_id) {
                job.command
                    .send(JobCommand::Cancel(cancel))
                    .map_err(|_| anyhow!("worker input command channel closed"))?;
            }
            Ok(true)
        }
        WorkerFrame::Push(WorkerPush::Control(control)) => {
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
            Ok(true)
        }
        WorkerFrame::Push(WorkerPush::Started(started)) => {
            if let Some(job) = jobs.get(&started.transfer_id) {
                job.command
                    .send(JobCommand::TransferStarted(started))
                    .map_err(|_| anyhow!("worker input command channel closed"))?;
            }
            Ok(true)
        }
        WorkerFrame::Ping(_) => Ok(true),
    }
}

fn acknowledge_multiplex_event(
    ack: &PendingEventAck,
    jobs: &mut HashMap<String, MultiplexJob>,
    pending: &mut HashMap<String, (JobKind, Option<String>)>,
) {
    if jobs
        .get(&ack.job_id)
        .is_some_and(|job| job.finished && terminal_event_name(job) == Some(ack.name))
    {
        jobs.remove(&ack.job_id);
        pending.retain(|_, (_, resend)| resend.as_deref() != Some(&ack.job_id));
    }
}

fn handle_multiplex_ack(
    ack: &PendingEventAck,
    response: &Value,
    jobs: &mut HashMap<String, MultiplexJob>,
    pending: &mut HashMap<String, (JobKind, Option<String>)>,
) -> Result<()> {
    if ack.name == "job_active"
        && response
            .get("discarded")
            .and_then(Value::as_bool)
            .unwrap_or(false)
    {
        if let Some(job) = jobs.get(&ack.job_id) {
            job.command
                .send(JobCommand::Cancel(CancelPayload {
                    job_id: ack.job_id.clone(),
                    reason: "coordinator discarded stale worker attempt".into(),
                }))
                .map_err(|_| anyhow!("worker job command channel closed"))?;
        }
        jobs.remove(&ack.job_id);
        pending.retain(|_, (_, resend)| resend.as_deref() != Some(&ack.job_id));
        return Ok(());
    }

    acknowledge_multiplex_event(ack, jobs, pending);
    Ok(())
}

fn terminal_event_name(job: &MultiplexJob) -> Option<&'static str> {
    if job.state.failure.is_some() {
        Some("video_failed")
    } else if job.state.encode_completed.is_some() {
        Some("encode_completed")
    } else if job.state.crf_completed.is_some() {
        Some("crf_search_completed")
    } else if job
        .state
        .control_state
        .as_ref()
        .is_some_and(|control| control.state == ControlState::Stopped)
    {
        Some("control_state")
    } else {
        None
    }
}

fn remove_finished_job_if_acknowledged(
    job_id: &str,
    jobs: &mut HashMap<String, MultiplexJob>,
    pending: &mut HashMap<String, (JobKind, Option<String>)>,
    pending_acks: &HashMap<String, PendingEventAck>,
) {
    let Some(job) = jobs.get(job_id) else {
        return;
    };
    if !job.finished {
        return;
    }
    if pending_acks.values().any(|ack| ack.job_id == job_id) {
        return;
    }
    jobs.remove(job_id);
    pending.retain(|_, (_, resend)| resend.as_deref() != Some(job_id));
}

async fn replay_multiplex_jobs(
    worker: &mut MultiplexedWorker,
    jobs: &HashMap<String, MultiplexJob>,
    pending_acks: &mut HashMap<String, PendingEventAck>,
) -> bool {
    for (job_id, job) in jobs {
        if !job.finished
            && !send_multiplex_event(
                worker,
                ClientEvent::JobActive(ActiveJobPayload {
                    job_id: job_id.clone(),
                    video_id: job.job.assignment.video_id,
                    job_type: job.job.assignment.job_type,
                }),
                job_id,
                "job_active",
                pending_acks,
            )
            .await
        {
            return false;
        }
        if let Some(control_state) = &job.state.control_state
            && !send_multiplex_event(
                worker,
                ClientEvent::ControlState(control_state.clone()),
                job_id,
                "control_state",
                pending_acks,
            )
            .await
        {
            return false;
        }
        if let Some(progress) = encode_reconnect_progress(&job.job, &job.state, job.finished)
            && !send_multiplex_event(
                worker,
                ClientEvent::EncodeProgress(progress),
                job_id,
                "encode_progress",
                pending_acks,
            )
            .await
        {
            return false;
        }
        if !replay_multiplex_state(worker, &job.state, job_id, pending_acks).await {
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
    pending_acks: &mut HashMap<String, PendingEventAck>,
) -> bool {
    if let Some(payload) = &state.heartbeat
        && !send_multiplex_event(
            worker,
            ClientEvent::Heartbeat(payload.clone()),
            job_id,
            "heartbeat",
            pending_acks,
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
            pending_acks,
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
            pending_acks,
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
            pending_acks,
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
            pending_acks,
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
            pending_acks,
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
            pending_acks,
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
            pending_acks,
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

fn decode_worker_frame(
    frame: Option<std::result::Result<Message, WsError>>,
) -> Result<Option<WorkerFrame>> {
    let frame = frame
        .ok_or_else(|| anyhow!("worker websocket ended"))?
        .context("read worker websocket frame")?;

    match frame {
        Message::Text(text) => Ok(match decode_worker_push(&text)? {
            Some(push) => Some(WorkerFrame::Push(push)),
            None => Some(WorkerFrame::Text(text.to_string())),
        }),
        Message::Binary(bytes) => Ok(Some(WorkerFrame::Binary(bytes.to_vec()))),
        Message::Ping(payload) => Ok(Some(WorkerFrame::Ping(payload.to_vec()))),
        Message::Pong(_) | Message::Frame(_) => Ok(None),
        Message::Close(frame) => bail!("worker websocket closed: {frame:?}"),
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
        "phx_error" | "phx_close" => {
            bail!("worker Phoenix channel closed: {}", frame.3)
        }
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
    WebSocketConfig::default()
        .max_message_size(Some(MAX_TRANSFER_FRAME_BYTES))
        .max_frame_size(Some(MAX_TRANSFER_FRAME_BYTES))
}

async fn send_json<W, T>(writer: &mut W, value: T) -> Result<()>
where
    W: SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
    T: serde::Serialize,
{
    writer
        .send(Message::Text(
            (serde_json::to_string(&value).context("encode websocket message")?).into(),
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
    loop {
        match decode_worker_frame(reader.next().await)? {
            Some(WorkerFrame::Text(text)) => {
                if let Some(reply) = decode_expected_reply(&text, expected_ref, expected_event)? {
                    return reply;
                }
            }
            Some(WorkerFrame::Push(_) | WorkerFrame::Binary(_) | WorkerFrame::Ping(_)) | None => {}
        }
    }
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
                max_active_jobs: None,
                crf_searches_per_encode: None,
            }
        }

        async fn finish(self) {
            self.server.await.expect("server task");
        }
    }

    #[test]
    fn args_lowers_to_worker_config() {
        let config = WorkerConfig::try_from(Args {
            connect: "http://127.0.0.1:4000".into(),
            token: "token".into(),
            worker_id: "abav1-dev".into(),
            version: "0.11.4".into(),
            protocol_version: 1,
            once: false,
            local_path: None,
            worker_mode: WorkerMode::CrfSearch,
            max_active_jobs: std::num::NonZeroUsize::new(1),
            crf_searches_per_encode: None,
        })
        .expect("lower worker args");

        assert_eq!(config.connect, "http://127.0.0.1:4000");
        assert_eq!(config.token, "token");
        assert_eq!(config.worker_id, "abav1-dev");
        assert_eq!(config.version, "0.11.4");
        assert_eq!(config.protocol_version, 1);
        assert!(!config.once);
        assert_eq!(config.worker_mode, WorkerMode::CrfSearch);
        assert_eq!(
            config.max_active_jobs.map(std::num::NonZeroUsize::get),
            Some(1)
        );
        assert_eq!(config.crf_searches_per_encode, None);
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
    fn args_accepts_weighted_worker_mode() {
        let args = Args::try_parse_from([
            "ab-av1",
            "--connect",
            "http://127.0.0.1:4000",
            "--token",
            "token",
            "--worker-id",
            "worker",
            "--worker-mode",
            "weighted",
            "--crf-searches-per-encode",
            "3",
        ])
        .expect("parse weighted worker mode");

        assert_eq!(args.worker_mode, WorkerMode::Weighted);
        assert_eq!(args.crf_searches_per_encode.unwrap().get(), 3);
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
    fn args_accepts_nonzero_max_active_jobs() {
        let args = Args::try_parse_from([
            "ab-av1",
            "--connect",
            "http://127.0.0.1:4000",
            "--token",
            "token",
            "--worker-id",
            "worker",
            "--max-active-jobs",
            "1",
        ])
        .expect("parse max active jobs");

        assert_eq!(
            args.max_active_jobs.map(std::num::NonZeroUsize::get),
            Some(1)
        );
        assert!(
            Args::try_parse_from([
                "ab-av1",
                "--connect",
                "http://127.0.0.1:4000",
                "--token",
                "token",
                "--worker-id",
                "worker",
                "--max-active-jobs",
                "0",
            ])
            .is_err()
        );
    }

    #[test]
    fn args_accepts_nonzero_crf_searches_per_encode() {
        let args = Args::try_parse_from([
            "ab-av1",
            "--connect",
            "http://127.0.0.1:4000",
            "--token",
            "token",
            "--worker-id",
            "worker",
            "--worker-mode",
            "weighted",
            "--crf-searches-per-encode",
            "3",
        ])
        .expect("parse CRF searches per encode");

        assert_eq!(args.crf_searches_per_encode.unwrap().get(), 3);
        assert!(
            Args::try_parse_from([
                "ab-av1",
                "--connect",
                "http://127.0.0.1:4000",
                "--token",
                "token",
                "--worker-id",
                "worker",
                "--worker-mode",
                "weighted",
                "--crf-searches-per-encode",
                "0",
            ])
            .is_err()
        );
    }

    #[test]
    fn ratio_is_rejected_outside_weighted_mode() {
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
            "--crf-searches-per-encode",
            "3",
        ])
        .expect("parse worker args");

        assert!(WorkerConfig::try_from(args).is_err());
    }

    #[test]
    fn both_mode_capacity_changes_after_eight_logical_cpus() {
        let small = WorkerCapacity::new(8, WorkerMode::Both, None);
        let large = WorkerCapacity::new(9, WorkerMode::Both, None);
        assert_eq!(small.max_active_jobs(), 1);
        assert_eq!(large.max_active_jobs(), 2);
    }

    #[test]
    fn max_active_jobs_overrides_detected_capacity() {
        let capacity = WorkerCapacity::new(16, WorkerMode::Both, std::num::NonZeroUsize::new(1));
        assert_eq!(capacity.max_active_jobs(), 1);
    }

    #[test]
    fn worker_failure_payload_uses_cli_exit_code_shape() {
        let job = WorkerJob::new(
            JobAssignedPayload {
                status: WorkStatus::JobAssigned,
                job_type: JobKind::CrfSearch,
                job_id: "crf-search-123".into(),
                video_id: 123,
                source_name: "movie.mkv".into(),
                size_bytes: 1_000,
                chunk_size_bytes: 256,
                target_vmaf: 95.0,
                transfer: None,
                output_transfer: None,
                output_shared_path: None,
                encode_args: Vec::new(),
                crf_search_args: vec!["crf-search".into(), "movie.mkv".into()],
            },
            PathBuf::from("/tmp/crf-search-123"),
            PathBuf::from("/tmp/crf-search-123/movie.mkv"),
        );

        let payload = job.failure_payload(&anyhow!(crf_search::Error::Other(anyhow!(
            ProcessExitError::new(Some(254), "ffmpeg exited unsuccessfully",)
        ))));

        assert_eq!(payload.stage, "crf_search");
        assert_eq!(payload.category, "process_failure");
        assert_eq!(payload.code, "EXIT_254");
        assert_eq!(payload.context["exit_code"], json!(254));
        assert_eq!(payload.context["argv"], json!(["crf-search", "movie.mkv"]));
    }

    #[test]
    fn worker_crf_failure_reports_why_no_crf_was_acceptable() {
        let job = WorkerJob::new(
            JobAssignedPayload {
                status: WorkStatus::JobAssigned,
                job_type: JobKind::CrfSearch,
                job_id: "crf-search-123".into(),
                video_id: 123,
                source_name: "movie.mkv".into(),
                size_bytes: 1_000,
                chunk_size_bytes: 256,
                target_vmaf: 95.0,
                transfer: None,
                output_transfer: None,
                output_shared_path: None,
                encode_args: Vec::new(),
                crf_search_args: vec![
                    "crf-search".into(),
                    "--max-encoded-percent".into(),
                    "80".into(),
                    "--input".into(),
                    "movie.mkv".into(),
                ],
            },
            PathBuf::from("/tmp/crf-search-123"),
            PathBuf::from("/tmp/crf-search-123/movie.mkv"),
        );
        let sample = crf_search::Sample::new(
            30.0,
            30,
            sample_encode::Output {
                vmaf_score: Some(95.0),
                xpsnr_score: None,
                predicted_encode_size: 1_000,
                encode_percent: 81.0,
                predicted_encode_time: Duration::from_secs(60),
                from_cache: false,
            },
        );

        let error = anyhow!(crf_search::Error::Other(anyhow!(
            crf_search::Error::NoGoodCrf {
                last: sample.clone()
            }
        )))
        .context("worker CRF search failed");
        let payload = job.failure_payload(&error);

        assert_eq!(payload.category, "size_limits");
        assert_eq!(payload.code, "SIZE_LIMIT");
        assert_eq!(payload.context["encode_percent"], json!(81.0));
        assert_eq!(payload.context["max_encoded_percent"], json!(80.0));

        let mut optimization_sample = sample.clone();
        optimization_sample.enc.encode_percent = 70.0;
        let optimization_error = anyhow!(crf_search::Error::Other(anyhow!(
            crf_search::Error::NoGoodCrf {
                last: optimization_sample
            }
        )));
        let optimization_payload = job.failure_payload(&optimization_error);

        assert_eq!(optimization_payload.category, "crf_optimization");
        assert_eq!(optimization_payload.code, "CRF_NOT_FOUND");
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
            initial_pull_work_payload(WorkerMode::Weighted).job_type,
            Some(JobKind::CrfSearch)
        );
        assert_eq!(
            next_job_type_after_no_work(WorkerMode::Both, JobKind::CrfSearch),
            Some(JobKind::Encode)
        );
    }

    #[test]
    fn idle_multiplex_heartbeat_uses_absolute_temp_path() {
        assert!(worker_heartbeat_path(None).is_absolute());
        assert_eq!(worker_heartbeat_path(None), std::env::temp_dir());
    }

    #[test]
    fn both_mode_keeps_one_crf_and_one_encode_slot() {
        let scheduler = WorkScheduler::new(std::num::NonZeroUsize::new(1).unwrap());
        let no_work = HashMap::new();
        assert_eq!(
            next_multiplex_job_type(
                WorkerMode::Both,
                &scheduler,
                &HashMap::new(),
                &HashMap::new(),
                &no_work,
            ),
            Some(JobKind::CrfSearch)
        );

        let pending = HashMap::from([("crf-pull".into(), (JobKind::CrfSearch, None))]);
        assert_eq!(
            next_multiplex_job_type(
                WorkerMode::Both,
                &scheduler,
                &HashMap::new(),
                &pending,
                &no_work,
            ),
            Some(JobKind::Encode)
        );

        let mut no_work = HashMap::new();
        no_work.insert(JobKind::CrfSearch, true);
        assert_eq!(
            next_multiplex_job_type(
                WorkerMode::Both,
                &scheduler,
                &HashMap::new(),
                &HashMap::new(),
                &no_work,
            ),
            Some(JobKind::Encode)
        );
    }

    #[test]
    fn weighted_mode_selects_n_crf_searches_per_encode() {
        let mut scheduler = WorkScheduler::new(std::num::NonZeroUsize::new(3).unwrap());
        let no_work = HashMap::new();
        let expected = [
            JobKind::CrfSearch,
            JobKind::CrfSearch,
            JobKind::CrfSearch,
            JobKind::Encode,
            JobKind::CrfSearch,
            JobKind::CrfSearch,
            JobKind::CrfSearch,
            JobKind::Encode,
        ];

        for job_type in expected {
            assert_eq!(
                next_multiplex_job_type(
                    WorkerMode::Weighted,
                    &scheduler,
                    &HashMap::new(),
                    &HashMap::new(),
                    &no_work,
                ),
                Some(job_type)
            );
            scheduler.record_assignment(job_type);
        }
    }

    #[test]
    fn weighted_mode_uses_encode_when_crf_queue_is_empty() {
        let scheduler = WorkScheduler::new(std::num::NonZeroUsize::new(2).unwrap());
        let no_work = HashMap::from([(JobKind::CrfSearch, true)]);

        assert_eq!(
            next_multiplex_job_type(
                WorkerMode::Weighted,
                &scheduler,
                &HashMap::new(),
                &HashMap::new(),
                &no_work,
            ),
            Some(JobKind::Encode)
        );
    }

    #[test]
    fn weighted_mode_waits_for_the_active_job_to_finish() {
        let scheduler = WorkScheduler::new(std::num::NonZeroUsize::new(5).unwrap());
        let pending = HashMap::from([("crf-pull".into(), (JobKind::CrfSearch, None))]);

        assert_eq!(
            next_multiplex_job_type(
                WorkerMode::Weighted,
                &scheduler,
                &HashMap::new(),
                &pending,
                &HashMap::new(),
            ),
            None
        );
    }

    #[test]
    fn weighted_scheduler_state_survives_connection_replacement() {
        let mut scheduler = WorkScheduler::new(std::num::NonZeroUsize::new(3).unwrap());
        scheduler.record_assignment(JobKind::CrfSearch);
        scheduler.record_assignment(JobKind::CrfSearch);

        // Replacing the websocket connection does not replace the scheduler.
        assert_eq!(scheduler.preferred(), JobKind::CrfSearch);
        scheduler.record_assignment(JobKind::CrfSearch);
        assert_eq!(scheduler.preferred(), JobKind::Encode);
    }

    #[test]
    fn weighted_mode_runs_one_job_at_a_time() {
        let capacity = WorkerCapacity::new(16, WorkerMode::Weighted, None);
        let scheduler = WorkScheduler::new(std::num::NonZeroUsize::new(10).unwrap());

        assert_eq!(capacity.max_active_jobs(), 1);
        assert_eq!(scheduler.preferred(), JobKind::CrfSearch);
    }

    #[test]
    fn weighted_mode_rejects_concurrent_jobs() {
        let args = Args::try_parse_from([
            "ab-av1",
            "--connect",
            "http://127.0.0.1:4000",
            "--token",
            "token",
            "--worker-id",
            "worker",
            "--worker-mode",
            "weighted",
            "--crf-searches-per-encode",
            "3",
            "--max-active-jobs",
            "2",
        ])
        .expect("parse worker args");

        assert!(WorkerConfig::try_from(args).is_err());
    }

    #[test]
    fn finished_encode_waits_for_completion_ack_before_job_removal() {
        let (command, _commands) = mpsc::unbounded_channel();
        let mut jobs = HashMap::from([(
            "encode-123".into(),
            MultiplexJob {
                job: WorkerJob::new(
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
                        output_shared_path: Some("/tmp/movie.av1.mkv".into()),
                        encode_args: vec!["encode".into(), "--input".into(), "movie.mkv".into()],
                        crf_search_args: Vec::new(),
                    },
                    PathBuf::from("/tmp"),
                    PathBuf::from("/tmp/movie.mkv"),
                ),
                command,
                state: WorkerJobReportState {
                    encode_completed: Some(EncodeCompletedPayload {
                        job_id: "encode-123".into(),
                        video_id: 123,
                        source_name: "movie.mkv".into(),
                        output_path: "/tmp/movie.av1.mkv".into(),
                        output_bytes: 500,
                        output_percent: 50.0,
                    }),
                    ..WorkerJobReportState::default()
                },
                finished: true,
            },
        )]);
        let mut pending = HashMap::new();
        let mut pending_acks = HashMap::from([(
            "7".into(),
            PendingEventAck::new("encode-123", "encode_completed"),
        )]);

        remove_finished_job_if_acknowledged("encode-123", &mut jobs, &mut pending, &pending_acks);
        assert!(jobs.contains_key("encode-123"));

        acknowledge_multiplex_event(
            &PendingEventAck::new("encode-123", "control_state"),
            &mut jobs,
            &mut pending,
        );
        assert!(
            jobs.contains_key("encode-123"),
            "an unrelated control acknowledgement must not retire the completion"
        );

        let ack = pending_acks.remove("7").expect("pending ack");
        acknowledge_multiplex_event(&ack, &mut jobs, &mut pending);
        assert!(!jobs.contains_key("encode-123"));
    }

    #[test]
    fn every_terminal_worker_event_requires_acknowledgement() {
        assert!(event_requires_ack("crf_search_completed"));
        assert!(event_requires_ack("encode_completed"));
        assert!(event_requires_ack("video_failed"));
        assert!(event_requires_ack("control_state"));
        assert!(!event_requires_ack("encode_progress"));
    }

    #[test]
    fn worker_frames_normalize_every_connection_loss() {
        for event in ["phx_error", "phx_close"] {
            let text = json!([null, null, CRF_SEARCH_TOPIC, event, {}]).to_string();

            assert!(decode_worker_frame(Some(Ok(Message::Text(text.into())))).is_err());
        }

        assert!(decode_worker_frame(Some(Ok(Message::Close(None)))).is_err());
        assert!(decode_worker_frame(None).is_err());
    }

    #[test]
    fn pending_event_ack_sets_liveness_deadline() {
        let timeout = Duration::from_secs(30);
        let ack = PendingEventAck {
            job_id: String::new(),
            name: "heartbeat",
            sent_at: Instant::now() - timeout,
        };

        assert!(ack.deadline(timeout) <= Instant::now());
    }

    #[test]
    fn discarded_active_attempt_is_cancelled() -> Result<()> {
        let (command, mut commands) = mpsc::unbounded_channel();
        let mut jobs = HashMap::from([(
            "encode-stale".into(),
            MultiplexJob {
                job: probe_phase_job("encode-stale"),
                command,
                state: WorkerJobReportState::default(),
                finished: false,
            },
        )]);
        let mut pending = HashMap::new();

        handle_multiplex_ack(
            &PendingEventAck::new("encode-stale", "job_active"),
            &serde_json::json!({
                "accepted": false,
                "discarded": true,
                "reason": "stale_worker_attempt"
            }),
            &mut jobs,
            &mut pending,
        )?;

        assert!(matches!(
            commands.try_recv(),
            Ok(JobCommand::Cancel(CancelPayload { ref job_id, .. }))
                if job_id == "encode-stale"
        ));
        assert!(!jobs.contains_key("encode-stale"));
        Ok(())
    }

    #[test]
    fn terminal_ack_identity_covers_every_terminal_outcome() {
        let states = [
            (
                WorkerJobReportState {
                    crf_completed: Some(CrfSearchCompletedPayload {
                        job_id: "crf-123".into(),
                        video_id: 123,
                        result: "ok".into(),
                        chosen_crf: 30.0,
                        results: Vec::new(),
                    }),
                    ..WorkerJobReportState::default()
                },
                "crf_search_completed",
            ),
            (
                WorkerJobReportState {
                    encode_completed: Some(EncodeCompletedPayload {
                        job_id: "encode-123".into(),
                        video_id: 123,
                        source_name: "movie.mkv".into(),
                        output_path: "movie.av1.mkv".into(),
                        output_bytes: 500,
                        output_percent: 50.0,
                    }),
                    ..WorkerJobReportState::default()
                },
                "encode_completed",
            ),
            (
                WorkerJobReportState {
                    failure: Some(FailureReportPayload {
                        job_id: "failed-123".into(),
                        video_id: 123,
                        stage: "encode".into(),
                        category: "process_failure".into(),
                        code: "EXIT_254".into(),
                        message: "failed".into(),
                        context: json!({"exit_code": 254}),
                        retriable: false,
                        stderr_excerpt: None,
                    }),
                    ..WorkerJobReportState::default()
                },
                "video_failed",
            ),
            (
                WorkerJobReportState {
                    control_state: Some(ControlStatePayload {
                        state: ControlState::Stopped,
                        active_video_id: Some(123),
                        job_id: Some("stopped-123".into()),
                        command_id: Some("stop-123".into()),
                    }),
                    ..WorkerJobReportState::default()
                },
                "control_state",
            ),
        ];

        for (state, event_name) in states {
            let (commands, _) = mpsc::unbounded_channel();
            let job = MultiplexJob {
                job: probe_phase_job("terminal-123"),
                command: commands,
                state,
                finished: true,
            };
            assert_eq!(terminal_event_name(&job), Some(event_name));
        }
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
    fn multiplex_reconnect_retains_job_control_state() {
        let mut state = WorkerJobReportState::default();
        let paused = ControlStatePayload {
            state: ControlState::Paused,
            active_video_id: Some(123),
            job_id: Some("crf-123".into()),
            command_id: Some("pause-123".into()),
        };

        record_multiplex_event(&mut state, &ClientEvent::ControlState(paused.clone()));

        assert_eq!(state.control_state, Some(paused));
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
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then_some(value.trim())
                })
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
    async fn stop_during_http_input_does_not_fall_through_to_execution() -> Result<()> {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        let address = listener.local_addr()?;
        let server = std::thread::spawn(move || -> Result<()> {
            let (mut stream, _) = listener.accept()?;
            let mut request = [0; 4096];
            let _ = stream.read(&mut request)?;
            stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 4\r\n\r\n")?;
            std::thread::sleep(Duration::from_millis(25));
            let _ = stream.write_all(b"data");
            Ok(())
        });
        let job_id = format!("stop-http-input-{}", std::process::id());
        let input_dir = worker_job_input_dir(&job_id);
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
                transfer: Some(TransferSpec {
                    url: format!("http://{address}"),
                    auth: TransferAuth {
                        scheme: "Bearer".into(),
                        header: "Authorization".into(),
                        value: "token".into(),
                    },
                }),
                output_transfer: None,
                output_shared_path: None,
                encode_args: Vec::new(),
                crf_search_args: Vec::new(),
            },
            input_dir.clone(),
            input_dir.join("movie.mkv"),
        );
        let (commands, mut command_receiver) = mpsc::unbounded_channel();
        let (output, mut outputs) = mpsc::unbounded_channel();
        commands.send(JobCommand::Control(ControlPayload {
            action: ControlAction::Stop,
            video_id: Some(123),
            job_id: Some(job_id),
            command_id: Some("stop-http-input".into()),
        }))?;

        assert_eq!(
            download_multiplex_input(&job, &mut command_receiver, &output).await?,
            Some(WorkerJobOutcome::Stopped)
        );
        assert!(matches!(
            outputs.recv().await,
            Some(MultiplexOutput::Event {
                event: ClientEvent::ControlState(ControlStatePayload {
                    state: ControlState::Stopped,
                    ..
                }),
                ..
            })
        ));
        assert!(!job.input_path().exists());
        server.join().expect("HTTP input server")?;
        let _ = fs::remove_dir_all(input_dir);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn stop_during_http_output_does_not_report_completion() -> Result<()> {
        let listener = std::net::TcpListener::bind("127.0.0.1:0")?;
        let address = listener.local_addr()?;
        let server = std::thread::spawn(move || -> Result<()> {
            let (mut stream, _) = listener.accept()?;
            let mut request = Vec::new();
            let mut buffer = [0; 4096];
            while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                let read = stream.read(&mut buffer)?;
                if read == 0 {
                    return Ok(());
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
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("content-length")
                        .then_some(value.trim())
                })
                .context("missing output content length")?
                .parse::<usize>()?;
            std::thread::sleep(Duration::from_millis(25));
            while request.len() < header_end + content_length {
                match stream.read(&mut buffer) {
                    Ok(0) | Err(_) => break,
                    Ok(read) => request.extend_from_slice(&buffer[..read]),
                }
            }
            let _ = stream.write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n");
            Ok(())
        });
        let job = probe_phase_job("stop-http-output");
        let output_path = std::env::temp_dir().join(format!(
            "ab-av1-stop-http-output-{}.mkv",
            std::process::id()
        ));
        let output_file = fs::File::create(&output_path)?;
        output_file.set_len(4 * 1024 * 1024)?;
        let transfer = TransferSpec {
            url: format!("http://{address}"),
            auth: TransferAuth {
                scheme: "Bearer".into(),
                header: "Authorization".into(),
                value: "token".into(),
            },
        };
        let (commands, mut command_receiver) = mpsc::unbounded_channel();
        let (output, mut outputs) = mpsc::unbounded_channel();
        commands.send(JobCommand::Control(ControlPayload {
            action: ControlAction::Stop,
            video_id: Some(123),
            job_id: Some("stop-http-output".into()),
            command_id: Some("stop-http-output".into()),
        }))?;

        assert_eq!(
            upload_multiplex_output(
                &job,
                &output_path,
                &transfer,
                4 * 1024 * 1024,
                &mut command_receiver,
                &output,
            )
            .await?,
            Some(WorkerJobOutcome::Stopped)
        );
        assert!(matches!(
            outputs.recv().await,
            Some(MultiplexOutput::Event {
                event: ClientEvent::ControlState(ControlStatePayload {
                    state: ControlState::Stopped,
                    ..
                }),
                ..
            })
        ));
        while let Ok(event) = outputs.try_recv() {
            assert!(!matches!(
                event,
                MultiplexOutput::Event {
                    event: ClientEvent::EncodeCompleted(_),
                    ..
                }
            ));
        }
        server.join().expect("HTTP output server")?;
        fs::remove_file(output_path)?;
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
            command_id: Some("stop-heartbeat-job".into()),
        }))?;
        assert!(matches!(run.await?, WorkerJobOutcome::Stopped));
        let mut stopped = false;
        while let Ok(item) = outputs.try_recv() {
            stopped |= matches!(
                item,
                MultiplexOutput::Event {
                    event: ClientEvent::ControlState(ControlStatePayload {
                        state: ControlState::Stopped,
                        active_video_id: Some(123),
                        ref job_id,
                        ref command_id,
                    }),
                    ..
                } if job_id.as_deref() == Some("heartbeat-job")
                    && command_id.as_deref() == Some("stop-heartbeat-job")
            );
        }
        assert!(stopped);
        crate::command::encode::test_hooks::clear();
        let _ = fs::remove_dir_all(input_dir);
        assert!(heartbeat);
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn worker_replays_finished_encode_until_acknowledged() -> Result<()> {
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
                    (serde_json::to_string(&ServerFrame::reply(
                        3,
                        ReplyBody::ok(ServerReply::JobAssigned(assignment)),
                    ))
                    .expect("encode assignment json"))
                    .into(),
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
            drop(writer);
            drop(reader);

            let (stream, _) = listener.accept().await.expect("accept reconnect");
            let socket = accept_async(stream)
                .await
                .expect("accept reconnect websocket");
            let (mut writer, mut reader) = socket.split();
            expect_join(&mut reader).await;
            send_join_reply(&mut writer).await;
            expect_announce_for_mode(&mut reader, 1, WorkerMode::Encode).await;
            send_announce_reply(&mut writer).await;
            loop {
                let Some(Ok(Message::Text(text))) = reader.next().await else {
                    panic!("worker disconnected before replaying encode completion");
                };
                let frame: Value = serde_json::from_str(&text).expect("worker replay event json");
                if frame[3] == "encode_completed" {
                    assert_eq!(frame[4]["output_path"], expected_output);
                    let request_ref = frame[1].as_str().expect("encode_completed replay ref");
                    writer
                        .send(Message::Text(
                            (json!(["1", request_ref, CRF_SEARCH_TOPIC, "phx_reply", {
                                "status": "ok",
                                "response": {}
                            }])
                            .to_string())
                            .into(),
                        ))
                        .await
                        .expect("send replayed encode_completed ack");
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
            max_active_jobs: None,
            crf_searches_per_encode: None,
        };
        let local = tokio::task::LocalSet::new();
        let result = local
            .run_until(run_worker_until(
                &config,
                WorkerRuntime {
                    idle_delay: Duration::from_millis(10),
                    reconnect_base_delay: Duration::from_millis(10),
                    reconnect_max_delay: Duration::from_millis(10),
                    offline_job_timeout: Duration::from_secs(1),
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
                offline_job_timeout: Duration::from_secs(1),
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
                offline_job_timeout: Duration::from_secs(1),
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
                offline_job_timeout: Duration::from_secs(1),
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
                    offline_job_timeout: Duration::from_secs(1),
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

    #[test]
    fn multiplexed_pull_error_requests_reconnect() -> Result<()> {
        let mut jobs = HashMap::new();
        let mut pending = HashMap::from([("3".into(), (JobKind::CrfSearch, None))]);
        let mut no_work = HashMap::new();
        let mut pending_acks = HashMap::new();
        let mut scheduler = WorkScheduler::new(std::num::NonZeroUsize::new(1).unwrap());
        let (output, _outputs) = mpsc::unbounded_channel();
        let mut completed_pulls = 0;
        let frame = Message::Text(
            (serde_json::to_string(&ServerFrame::reply(
                3,
                ReplyBody::error(ErrorReplyPayload::new("unmatched topic")),
            ))?)
            .into(),
        );

        let connection_is_alive = handle_multiplex_frame(
            decode_worker_frame(Some(Ok(frame))).map(|frame| frame.expect("text worker frame")),
            &mut jobs,
            &mut pending,
            &mut no_work,
            &mut pending_acks,
            &mut scheduler,
            &output,
            &mut completed_pulls,
            None,
        )?;

        assert!(!connection_is_alive);
        assert_eq!(completed_pulls, 0);
        Ok(())
    }

    #[test]
    fn multiplexed_event_error_keeps_connection_alive() -> Result<()> {
        let mut jobs = HashMap::new();
        let mut pending = HashMap::new();
        let mut no_work = HashMap::new();
        let mut pending_acks = HashMap::from([(
            "4".into(),
            PendingEventAck::new("encode-1", "control_state"),
        )]);
        let mut scheduler = WorkScheduler::new(std::num::NonZeroUsize::new(1).unwrap());
        let (output, _outputs) = mpsc::unbounded_channel();
        let mut completed_pulls = 0;
        let frame = Message::Text(
            (serde_json::to_string(&ServerFrame::reply(
                4,
                ReplyBody::error(ErrorReplyPayload::new("stale worker control")),
            ))?)
            .into(),
        );

        let connection_is_alive = handle_multiplex_frame(
            decode_worker_frame(Some(Ok(frame))).map(|frame| frame.expect("text worker frame")),
            &mut jobs,
            &mut pending,
            &mut no_work,
            &mut pending_acks,
            &mut scheduler,
            &output,
            &mut completed_pulls,
            None,
        )?;

        assert!(connection_is_alive);
        assert!(pending_acks.is_empty());
        Ok(())
    }

    #[test]
    fn multiplexed_channel_error_requests_reconnect() -> Result<()> {
        let mut jobs = HashMap::new();
        let mut pending = HashMap::new();
        let mut no_work = HashMap::new();
        let mut pending_acks = HashMap::new();
        let mut scheduler = WorkScheduler::new(std::num::NonZeroUsize::new(1).unwrap());
        let (output, _outputs) = mpsc::unbounded_channel();
        let mut completed_pulls = 0;
        let frame = Message::Text(
            (json!([null, null, CRF_SEARCH_TOPIC, "phx_error", {}]).to_string()).into(),
        );

        let connection_is_alive = handle_multiplex_frame(
            decode_worker_frame(Some(Ok(frame))).map(|frame| frame.expect("text worker frame")),
            &mut jobs,
            &mut pending,
            &mut no_work,
            &mut pending_acks,
            &mut scheduler,
            &output,
            &mut completed_pulls,
            None,
        )?;

        assert!(!connection_is_alive);
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
                offline_job_timeout: Duration::from_secs(1),
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
                command_id: None,
            },
        ))?;

        assert!(matches!(
            decode_worker_push(&text)?,
            Some(WorkerPush::Control(
                crate::command::worker_protocol::ControlPayload {
                    action: crate::command::worker_protocol::ControlAction::Pause,
                    video_id: Some(123),
                    job_id: None,
                    command_id: None,
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
            command_id: None,
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
    fn worker_job_input_dir_uses_system_temp_dir() {
        assert_eq!(
            worker_job_input_dir("job-123"),
            std::env::temp_dir().join("ab-av1-worker-job-123")
        );
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
        assert_eq!(job.input_dir, worker_dir);
        assert!(worker_dir.exists());
        fs::remove_dir_all(&worker_dir)?;
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
    fn transfer_progress_zero_rate_has_no_eta() {
        let payload = http_transfer_progress_payload(
            "job-zero-rate",
            1,
            "input.mkv",
            1024,
            0,
            Instant::now(),
        );

        assert_eq!(payload.eta, None);
    }

    #[test]
    fn transfer_progress_reports_eta_when_rate_is_nonzero() {
        let payload =
            http_transfer_progress_payload("job-rate", 1, "input.mkv", 2048, 1024, Instant::now());

        assert_eq!(payload.bytes_per_second, 1024);
        assert_eq!(payload.eta, Some(1));
    }

    #[test]
    fn controlled_transfer_reader_pauses_resumes_and_stops() -> Result<()> {
        let gate = Arc::new(TransferGate::running());
        gate.set(ControlState::Paused);
        let read_gate = Arc::clone(&gate);
        let (sent, received) = std::sync::mpsc::channel();
        let reader = std::thread::spawn(move || {
            let mut reader = ControlledReader {
                inner: io::Cursor::new(b"data"),
                gate: read_gate,
            };
            let mut buffer = Vec::new();
            sent.send(reader.read_to_end(&mut buffer).map(|_| buffer))
                .expect("send read result");
        });

        assert!(received.recv_timeout(Duration::from_millis(10)).is_err());
        gate.set(ControlState::Running);
        assert_eq!(received.recv_timeout(Duration::from_secs(1))??, b"data");
        reader.join().expect("controlled reader thread");

        gate.set(ControlState::Stopped);
        let mut stopped = ControlledReader {
            inner: io::Cursor::new(b"more"),
            gate,
        };
        assert_eq!(
            stopped.read(&mut [0; 4]).expect_err("stopped read").kind(),
            io::ErrorKind::ConnectionAborted
        );
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
    fn multiplex_worker_input_cleanup_removes_stopped_and_failed_jobs() -> Result<()> {
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
        assert!(!root.exists());

        fs::create_dir_all(&root)?;
        fs::write(job.input_path(), b"data")?;
        cleanup_multiplex_worker_input(&job, &Err(anyhow!("encode failed")))?;
        assert!(!root.exists());
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn expired_offline_lease_cancels_jobs_and_removes_owned_files() -> Result<()> {
        tokio::task::LocalSet::new()
            .run_until(async {
                let job_id = format!("offline-expired-{}", std::process::id());
                let root = worker_job_input_dir(&job_id);
                let _ = fs::remove_dir_all(&root);
                fs::create_dir_all(&root)?;
                let input_path = root.join("movie.mkv");
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
                            "movie.mkv".into(),
                        ],
                    },
                    root.clone(),
                    input_path,
                );
                let (output, mut outputs) = mpsc::unbounded_channel();
                let (command, commands) = mpsc::unbounded_channel();
                tokio::task::spawn_local(run_multiplex_job(job.clone(), commands, output));
                let mut jobs = HashMap::from([(
                    job_id,
                    MultiplexJob {
                        job,
                        command,
                        state: WorkerJobReportState::default(),
                        finished: false,
                    },
                )]);
                let mut pending = HashMap::new();

                expire_offline_jobs(&mut jobs, &mut pending, &mut outputs).await?;

                assert!(jobs.is_empty());
                assert!(!root.exists());
                Ok(())
            })
            .await
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
    fn completed_offered_local_encode_cleanup_preserves_source() -> Result<()> {
        let job_id = format!("cleanup-offered-local-output-{}", std::process::id());
        let root = worker_job_input_dir(&job_id);
        let _ = fs::remove_dir_all(&root);
        fs::create_dir_all(&root)?;
        let source_root =
            std::env::temp_dir().join(format!("ab-av1-worker-source-{}", std::process::id()));
        let source_path = source_root.join("movie.mkv");
        fs::create_dir_all(&source_root)?;
        fs::write(&source_path, b"input")?;
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
                    source_path.display().to_string(),
                    "--output".into(),
                    root.join("movie.av1.mkv").display().to_string(),
                ],
                crf_search_args: Vec::new(),
            },
            root.clone(),
            source_path.clone(),
        );

        cleanup_multiplex_worker_input(&job, &Ok(WorkerJobOutcome::Completed))?;

        assert!(source_path.exists());
        assert!(!root.exists());
        fs::remove_dir_all(source_root)?;
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
    async fn stop_during_input_transfer_acks_and_does_not_start_job() -> Result<()> {
        let root = worker_job_input_dir("stop-during-input");
        let _ = fs::remove_dir_all(&root);
        let job = WorkerJob::new(
            JobAssignedPayload {
                status: WorkStatus::JobAssigned,
                job_type: JobKind::CrfSearch,
                job_id: "stop-during-input".into(),
                video_id: 123,
                source_name: "movie.mkv".into(),
                size_bytes: 4,
                chunk_size_bytes: 4,
                target_vmaf: 95.0,
                transfer: None,
                output_transfer: None,
                output_shared_path: None,
                encode_args: Vec::new(),
                crf_search_args: Vec::new(),
            },
            root.clone(),
            root.join("movie.mkv"),
        );
        let (command, mut commands) = mpsc::unbounded_channel();
        let (output, mut outputs) = mpsc::unbounded_channel();
        command.send(JobCommand::Control(ControlPayload {
            action: ControlAction::Stop,
            video_id: Some(123),
            job_id: Some("stop-during-input".into()),
            command_id: Some("stop-command".into()),
        }))?;

        assert_eq!(
            receive_multiplex_input(&job, &mut commands, &output).await?,
            Some(WorkerJobOutcome::Stopped)
        );
        assert!(matches!(
            outputs.recv().await,
            Some(MultiplexOutput::Event {
                event: ClientEvent::ControlState(ControlStatePayload {
                    state: ControlState::Stopped,
                    command_id: Some(command_id),
                    ..
                }),
                ..
            }) if command_id == "stop-command"
        ));
        assert!(!job.input_path().exists());
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn pause_during_input_transfer_defers_chunks_until_resume() -> Result<()> {
        let job_id = format!("pause-during-input-{}", std::process::id());
        let root = worker_job_input_dir(&job_id);
        let _ = fs::remove_dir_all(&root);
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
                crf_search_args: Vec::new(),
            },
            root.clone(),
            root.join("movie.mkv"),
        );
        let (command, mut commands) = mpsc::unbounded_channel();
        let (output, mut outputs) = mpsc::unbounded_channel();
        command.send(JobCommand::Control(ControlPayload {
            action: ControlAction::Pause,
            video_id: Some(123),
            job_id: Some(job_id.clone()),
            command_id: Some("pause-command".into()),
        }))?;
        command.send(JobCommand::TransferChunk(TransferChunk {
            transfer_id: job_id.clone(),
            video_id: 123,
            chunk_index: 0,
            total_chunks: 1,
            bytes_sent: 4,
            total_bytes: 4,
            crc32: crc32fast::hash(b"data") as u64,
            bytes: b"data".to_vec(),
        }))?;

        let mut receive = std::pin::pin!(receive_multiplex_input(&job, &mut commands, &output));
        assert!(matches!(
            tokio::select! {
                item = outputs.recv() => item,
                result = &mut receive => panic!("input transfer ended while paused: {result:?}"),
            },
            Some(MultiplexOutput::Event {
                event: ClientEvent::ControlState(ControlStatePayload {
                    state: ControlState::Paused,
                    ..
                }),
                ..
            })
        ));
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut receive)
                .await
                .is_err()
        );
        assert!(!job.input_path().exists());

        command.send(JobCommand::Control(ControlPayload {
            action: ControlAction::Resume,
            video_id: Some(123),
            job_id: Some(job_id),
            command_id: Some("resume-command".into()),
        }))?;
        assert_eq!(receive.await?, None);
        assert_eq!(fs::read(job.input_path())?, b"data");
        fs::remove_dir_all(root)?;
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn probe_pause_defers_completion_and_stop_prevents_execution() -> Result<()> {
        let job = probe_phase_job("probe-control");
        let (output, mut outputs) = mpsc::unbounded_channel();
        let (commands, mut command_receiver) = mpsc::unbounded_channel();
        let (release, released) = tokio::sync::oneshot::channel();
        let probe = async move {
            released.await.expect("release probe");
            test_worker_probe()
        };
        let mut run = std::pin::pin!(drive_multiplex_probe(
            &job,
            &mut command_receiver,
            &output,
            probe,
        ));

        commands.send(JobCommand::Control(ControlPayload {
            action: ControlAction::Pause,
            video_id: Some(123),
            job_id: Some("probe-control".into()),
            command_id: Some("pause-probe".into()),
        }))?;
        let paused = tokio::select! {
            event = outputs.recv() => event,
            _ = &mut run => panic!("probe advanced before pause acknowledgement"),
        };
        assert!(matches!(
            paused,
            Some(MultiplexOutput::Event {
                event: ClientEvent::ControlState(ControlStatePayload {
                    state: ControlState::Paused,
                    ..
                }),
                ..
            })
        ));
        release.send(()).expect("release probe sender");
        assert!(
            tokio::time::timeout(Duration::from_millis(10), &mut run)
                .await
                .is_err(),
            "a completed probe must not advance while paused"
        );

        commands.send(JobCommand::Control(ControlPayload {
            action: ControlAction::Resume,
            video_id: Some(123),
            job_id: Some("probe-control".into()),
            command_id: Some("resume-probe".into()),
        }))?;
        assert!(run.await?.is_ok());

        let (commands, mut command_receiver) = mpsc::unbounded_channel();
        let stopped = std::pin::pin!(drive_multiplex_probe(
            &job,
            &mut command_receiver,
            &output,
            std::future::pending(),
        ));
        commands.send(JobCommand::Control(ControlPayload {
            action: ControlAction::Stop,
            video_id: Some(123),
            job_id: Some("probe-control".into()),
            command_id: Some("stop-probe".into()),
        }))?;
        assert!(matches!(stopped.await?, Err(WorkerJobOutcome::Stopped)));
        Ok(())
    }

    fn probe_phase_job(job_id: &str) -> WorkerJob {
        WorkerJob::new(
            JobAssignedPayload {
                status: WorkStatus::JobInProgress,
                job_type: JobKind::CrfSearch,
                job_id: job_id.into(),
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
            PathBuf::from("/tmp"),
            PathBuf::from("/tmp/movie.mkv"),
        )
    }

    fn test_worker_probe() -> Arc<Ffprobe> {
        Arc::new(Ffprobe {
            duration: Ok(Duration::from_secs(120)),
            has_audio: false,
            max_audio_channels: None,
            fps: Ok(24.0),
            resolution: Some((1920, 1080)),
            is_image: false,
            pix_fmt: Some("yuv420p".into()),
        })
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
            matches!(
                mode,
                WorkerMode::CrfSearch | WorkerMode::Both | WorkerMode::Weighted
            )
        );
        assert_eq!(
            frame[4]["capabilities"]["encode"],
            matches!(
                mode,
                WorkerMode::Encode | WorkerMode::Both | WorkerMode::Weighted
            )
        );
        assert_eq!(frame[4]["capabilities"]["mode"], mode.as_str());
        assert_eq!(
            frame[4]["capabilities"]["logical_cpus"],
            std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get)
        );
        assert_eq!(frame[4]["capabilities"]["max_active_jobs"], 1);
        assert_eq!(
            frame[4]["capabilities"]["crf_searches_per_encode"],
            Value::Null
        );
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
                (json!([null, "1", CRF_SEARCH_TOPIC, "phx_reply", {
                    "status": "ok",
                    "response": {"worker_id": "worker-123"}
                }])
                .to_string())
                .into(),
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
                (serde_json::to_string(&ServerFrame::<Value>::reply(
                    2,
                    ReplyBody::ok(json!({"accepted": true, "protocol_version": 1})),
                ))
                .expect("announce reply json"))
                .into(),
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
                (serde_json::to_string(&ServerFrame::reply(
                    request_ref,
                    ReplyBody::ok(ServerReply::NoWork(
                        crate::command::worker_protocol::NoWorkPayload {
                            status: WorkStatus::NoWork,
                        },
                    )),
                ))
                .expect("no_work reply json"))
                .into(),
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
                (serde_json::to_string(&ServerFrame::reply(
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
                .expect("job reply json"))
                .into(),
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
                (serde_json::to_string(&crate::command::worker_protocol::ServerFrame::reply(
                    request_ref,
                    ReplyBody::error(
                        serde_json::from_value::<ErrorReplyPayload>(response)
                            .expect("error payload"),
                    ),
                ))
                .expect("announce error reply json"))
                .into(),
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
                (serde_json::to_string(&ServerPushFrame::new(
                    "cancel",
                    CancelPayload {
                        job_id: job_id.into(),
                        reason: reason.into(),
                    },
                ))
                .expect("cancel push json"))
                .into(),
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
                (serde_json::to_string(&ServerPushFrame::new(
                    "control",
                    ControlPayload {
                        action,
                        video_id,
                        job_id: None,
                        command_id: None,
                    },
                ))
                .expect("control push json"))
                .into(),
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
