use crate::command::worker_protocol::{
    AnnouncePayload, CRF_SEARCH_TOPIC, Capabilities, ClientEvent, ClientFrame, ErrorReplyPayload,
    JobResultPayload, ReplyBody, ServerPushFrame, ServerReply,
};
use crate::command::{args, crf_search, sample_encode};
use crate::ffprobe::Ffprobe;
use crate::temporary;
use anyhow::{Context, Result, anyhow, bail};
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{path::PathBuf, sync::Arc, time::Duration};
use tokio::net::TcpStream;
use tokio_tungstenite::{
    MaybeTlsStream, WebSocketStream, connect_async,
    tungstenite::{Error as WsError, Message},
};

const PHOENIX_VSN: &str = "2.0.0";
const SUPPORTED_PROTOCOL_VERSION: u64 = 1;

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
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerConfig {
    connect: String,
    token: String,
    worker_id: String,
    version: String,
    protocol_version: u64,
    once: bool,
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
        }: Args,
    ) -> Self {
        Self {
            connect,
            token,
            worker_id,
            version,
            protocol_version,
            once,
        }
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
}

#[cfg_attr(not(test), allow(dead_code))]
impl WorkerJob {
    fn new(
        assignment: crate::command::worker_protocol::JobAssignedPayload,
        input_dir: PathBuf,
    ) -> Self {
        Self {
            assignment,
            input_dir,
        }
    }

    fn input_path(&self) -> PathBuf {
        self.input_dir.join(&self.assignment.source_name)
    }

    fn crf_search_config(&self, encoder: args::Encoder) -> Result<crf_search::CrfSearchConfig> {
        Ok(crf_search::CrfSearchConfig {
            args: args::Encode {
                encoder,
                input: self.input_path(),
                vfilter: None,
                pix_format: None,
                preset: None,
                keyint: None,
                scd: None,
                svt_args: vec![],
                enc_args: vec![],
                enc_input_args: vec![],
            },
            min_vmaf: Some(crf_search::MinScore::new(self.assignment.target_vmaf)?),
            min_xpsnr: None,
            max_encoded_percent: crf_search::MaxEncodedPercent::new(80.0)?,
            min_crf: None,
            max_crf: None,
            thorough: false,
            crf_increment: None,
            high_crf_means_hq: None,
            cache: true,
            sample: args::Sample {
                samples: None,
                sample_every: args::SampleDuration::new(Duration::from_secs(12 * 60))?,
                min_samples: None,
                sample_duration: args::SampleDuration::new(Duration::from_secs(20))?,
                keep: false,
                temp_dir: Some(self.input_dir.clone()),
                extension: None,
            },
            scoring: sample_encode::ScoringConfig {
                score: args::ScoreArgs {
                    reference_vfilter: None,
                }
                .into(),
                vmaf: args::Vmaf::default().into(),
                xpsnr: false,
                xpsnr_opts: args::Xpsnr::default().into(),
            },
            verbose: clap_verbosity_flag::Verbosity::new(0, 0),
        })
    }

    fn result_payload(&self, best: &crf_search::Sample) -> JobResultPayload {
        JobResultPayload {
            job_id: self.assignment.job_id.clone(),
            video_id: self.assignment.video_id,
            source_name: self.assignment.source_name.clone(),
            crf: best.crf,
            vmaf_score: best.enc.vmaf_score,
            xpsnr_score: best.enc.xpsnr_score,
            predicted_encode_size: best.enc.predicted_encode_size,
            encode_percent: best.enc.encode_percent,
            predicted_encode_time_secs: best.enc.predicted_encode_time.as_secs_f64(),
            from_cache: best.enc.from_cache,
        }
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
    let config = job.crf_search_config("libsvtav1".parse().expect("default encoder"))?;
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

fn worker_job_input_dir(job_id: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "ab-av1-worker-{}-{}-{}",
        std::process::id(),
        job_id,
        fastrand::u64(..)
    ))
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

type WorkerSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

struct ConnectedWorker {
    assigned_worker_id: String,
    negotiated_protocol_version: u64,
    next_ref: u64,
    socket: WorkerSocket,
}

impl ConnectedWorker {
    async fn connect(config: &WorkerConfig) -> Result<Self> {
        let request_url = worker_websocket_url(&config.connect, &config.token)?;
        let (mut socket, _) = connect_async(&request_url)
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
                    protocol_version: config.protocol_version,
                    version: config.version.clone(),
                    capabilities: Capabilities { crf_search: true },
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
        })
    }

    async fn request_work(&mut self) -> Result<ServerReply> {
        let request_ref = self.next_ref;
        self.next_ref += 1;

        send_json(
            &mut self.socket,
            ClientFrame::new(request_ref, ClientEvent::PullWork),
        )
        .await?;
        expect_reply(&mut self.socket, &request_ref.to_string(), "pull_work").await
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
    let mut reconnect_backoff =
        ReconnectBackoff::new(runtime.reconnect_base_delay, runtime.reconnect_max_delay);

    loop {
        match run_connected_worker(config, runtime, &mut completed_pulls).await {
            Ok(()) => {
                reconnect_backoff.reset();
                return Ok(());
            }
            Err(error) => {
                eprintln!("worker connection lost: {error}");
                tokio::time::sleep(reconnect_backoff.next_delay()).await;
            }
        }
    }
}

async fn run_connected_worker(
    config: &WorkerConfig,
    runtime: WorkerRuntime,
    completed_pulls: &mut usize,
) -> Result<()> {
    let mut worker = ConnectedWorker::connect(config).await?;

    loop {
        let work_status = worker.request_work().await?;
        *completed_pulls += 1;
        let status = work_status_label(&work_status);
        println!(
            "connected worker {} via {} and received {}",
            worker.assigned_worker_id, worker.negotiated_protocol_version, status
        );

        if let ServerReply::JobAssigned(assignment) = work_status {
            let input_dir = worker_job_input_dir(&assignment.job_id);
            std::fs::create_dir_all(&input_dir).context("create worker job dir")?;
            temporary::add(&input_dir, temporary::TempKind::NotKeepable);

            let job = WorkerJob::new(assignment, input_dir);
            let probe = Arc::new(crate::ffprobe::probe(&job.input_path()));
            let best = run_worker_job(job.clone(), probe).await;
            temporary::clean(true).await;
            let best = best?;

            println!(
                "{}",
                serde_json::to_string(&job.result_payload(&best))
                    .context("serialize worker job result")?
            );
            continue;
        }

        if runtime.max_pulls == Some(*completed_pulls) {
            return Ok(());
        }

        tokio::time::sleep(runtime.idle_delay).await;
    }
}

async fn run_worker_session(config: &WorkerConfig) -> Result<WorkerSession> {
    let mut worker = ConnectedWorker::connect(config).await?;
    let pull_work = worker.request_work().await?;
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
                let ServerPushFrame(_, msg_ref, topic, event, body): ServerPushFrame<Value> =
                    serde_json::from_str(&text).context("decode phoenix frame")?;
                if topic != CRF_SEARCH_TOPIC
                    || event != "phx_reply"
                    || msg_ref.as_deref() != Some(expected_ref)
                {
                    continue;
                }

                let ReplyBody { status, response } = serde_json::from_value::<ReplyBody<_>>(body)
                    .context("decode phoenix reply body")?;
                return match status.as_str() {
                    "ok" => serde_json::from_value(response).context("decode phoenix ok reply"),
                    "error" => {
                        let error: ErrorReplyPayload = serde_json::from_value(response)
                            .context("decode phoenix error reply")?;
                        let supported_versions = match error.supported_protocol_versions.is_empty()
                        {
                            true => String::new(),
                            false => format!(
                                " (supported_protocol_versions={:?})",
                                error.supported_protocol_versions
                            ),
                        };
                        Err(anyhow!(
                            "{expected_event} failed: {}{}",
                            error.reason,
                            supported_versions
                        ))
                    }
                    other => Err(anyhow!("unexpected phoenix status {other}")),
                };
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::worker_protocol::{
        CancelPayload, ErrorReplyPayload, JobAssignedPayload, ReplyBody, ServerFrame,
        ServerPushFrame, WorkStatus,
    };
    use crate::{command::crf_search::test_hooks as crf_test_hooks, ffprobe::Ffprobe};
    use anyhow::Result;
    use serde_json::{Value, json};
    use std::sync::Arc;
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
        });

        assert_eq!(config.connect, "http://127.0.0.1:4000");
        assert_eq!(config.token, "token");
        assert_eq!(config.worker_id, "abav1-dev");
        assert_eq!(config.version, "0.11.4");
        assert_eq!(config.protocol_version, 1);
        assert!(!config.once);
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

        let (worker, replacement) = tokio::join!(worker, replacement);
        worker?;
        let _ = replacement;
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
    fn worker_formats_assigned_job_status_with_job_id() {
        let status = work_status_label(&ServerReply::JobAssigned(JobAssignedPayload {
            status: WorkStatus::JobAssigned,
            job_id: "job-123".into(),
            video_id: 123,
            source_name: "movie.mkv".into(),
            size_bytes: 1024,
            chunk_size_bytes: 256,
            target_vmaf: 96.5,
        }));

        assert_eq!(status, "job_assigned (job_id=job-123)");
    }

    #[test]
    fn worker_job_lowering_uses_an_isolated_temp_dir_and_target_vmaf() {
        let job_dir =
            std::env::temp_dir().join(format!("ab-av1-worker-job-{}", std::process::id()));
        let job = WorkerJob::new(
            JobAssignedPayload {
                status: WorkStatus::JobAssigned,
                job_id: "job-123".into(),
                video_id: 123,
                source_name: "movie.mkv".into(),
                size_bytes: 1024,
                chunk_size_bytes: 256,
                target_vmaf: 96.5,
            },
            job_dir.clone(),
        );

        let config = job
            .crf_search_config("libsvtav1".parse().expect("encoder"))
            .expect("job config");

        assert_eq!(config.args.input, job_dir.join("movie.mkv"));
        assert_eq!(config.sample.temp_dir.as_deref(), Some(job_dir.as_path()));
        assert_eq!(config.min_vmaf.expect("target vmaf").get(), 96.5);
        assert!(config.cache);
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
        let job = WorkerJob::new(
            JobAssignedPayload {
                status: WorkStatus::JobAssigned,
                job_id: "job-123".into(),
                video_id: 123,
                source_name: "movie.mkv".into(),
                size_bytes: 1024,
                chunk_size_bytes: 256,
                target_vmaf: 96.5,
            },
            job_dir,
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
        assert_eq!(
            job.result_payload(&best),
            JobResultPayload {
                job_id: "job-123".into(),
                video_id: 123,
                source_name: "movie.mkv".into(),
                crf: best.crf,
                vmaf_score: Some(97.0),
                xpsnr_score: None,
                predicted_encode_size: 100,
                encode_percent: 50.0,
                predicted_encode_time_secs: 1.0,
                from_cache: false,
            }
        );
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
    fn reconnect_backoff_resets_after_success() {
        let mut backoff =
            ReconnectBackoff::new(Duration::from_millis(100), Duration::from_millis(1_000));

        assert_eq!(backoff.next_delay(), Duration::from_millis(100));
        assert_eq!(backoff.next_delay(), Duration::from_millis(200));

        backoff.reset();

        assert_eq!(backoff.next_delay(), Duration::from_millis(100));
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
        assert_text_message(
            reader
                .next()
                .await
                .expect("announce frame")
                .expect("announce message"),
            serde_json::to_value(ClientFrame::new(
                2,
                ClientEvent::Announce(AnnouncePayload {
                    worker_id: "abav1-dev".into(),
                    protocol_version,
                    version: "0.11.4".into(),
                    capabilities: Capabilities { crf_search: true },
                }),
            ))
            .expect("announce frame json"),
        );
    }

    async fn expect_pull_work<R>(reader: &mut R, request_ref: u64)
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
            serde_json::to_value(ClientFrame::new(request_ref, ClientEvent::PullWork))
                .expect("pull_work frame json"),
        );
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
        writer
            .send(Message::Text(
                serde_json::to_string(&ServerFrame::reply(
                    3,
                    ReplyBody::ok(ServerReply::JobAssigned(JobAssignedPayload {
                        status: WorkStatus::JobAssigned,
                        job_id: "job-123".into(),
                        video_id: 123,
                        source_name: "movie.mkv".into(),
                        size_bytes: 1024,
                        chunk_size_bytes: 256,
                        target_vmaf: 96.5,
                    })),
                ))
                .expect("job assigned reply json"),
            ))
            .await
            .expect("send job assigned reply");
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
