use anyhow::{Context, Result, anyhow, bail};
use clap::Parser;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_tungstenite::{
    connect_async,
    tungstenite::{Error as WsError, Message},
    MaybeTlsStream, WebSocketStream,
};

const PHOENIX_VSN: &str = "2.0.0";
const CRF_SEARCH_TOPIC: &str = "workers:crf_search";
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

#[derive(Debug, PartialEq, Eq)]
pub struct WorkerSession {
    pub assigned_worker_id: String,
    pub negotiated_protocol_version: u64,
    pub work_status: String,
}

#[allow(dead_code)]
#[derive(Debug, Deserialize)]
struct ReplyEnvelope(Option<String>, Option<String>, String, String, ReplyBody);

#[derive(Debug, Deserialize)]
struct ReplyBody {
    status: String,
    response: Value,
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

#[derive(Debug, Deserialize, Serialize, PartialEq, Eq)]
struct PullWorkResponse {
    status: String,
}

#[derive(Debug, Serialize)]
struct AnnouncePayload<'a> {
    worker_id: &'a str,
    protocol_version: u64,
    version: &'a str,
    capabilities: Capabilities,
}

#[derive(Debug, Serialize)]
struct Capabilities {
    crf_search: bool,
}

#[derive(Debug, Clone, Copy)]
struct WorkerRuntime {
    idle_delay: Duration,
    reconnect_delay: Duration,
    max_pulls: Option<usize>,
}

impl Default for WorkerRuntime {
    fn default() -> Self {
        Self {
            idle_delay: Duration::from_secs(5),
            reconnect_delay: Duration::from_secs(1),
            max_pulls: None,
        }
    }
}

type WorkerSocket = WebSocketStream<MaybeTlsStream<TcpStream>>;

struct ConnectedWorker {
    assigned_worker_id: String,
    negotiated_protocol_version: u64,
    next_ref: u64,
    socket: WorkerSocket,
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

    loop {
        match run_connected_worker(config, runtime, &mut completed_pulls).await {
            Ok(()) => return Ok(()),
            Err(error) => {
                eprintln!("worker connection lost: {error}");
                tokio::time::sleep(runtime.reconnect_delay).await;
            }
        }
    }
}

async fn run_connected_worker(
    config: &WorkerConfig,
    runtime: WorkerRuntime,
    completed_pulls: &mut usize,
) -> Result<()> {
    let mut worker = connect_worker(config).await?;

    loop {
        let work_status = request_work(&mut worker).await?;
        *completed_pulls += 1;
        println!(
            "connected worker {} via {} and received {}",
            worker.assigned_worker_id, worker.negotiated_protocol_version, work_status.status
        );

        if runtime.max_pulls == Some(*completed_pulls) {
            return Ok(());
        }

        tokio::time::sleep(runtime.idle_delay).await;
    }
}

async fn connect_worker(config: &WorkerConfig) -> Result<ConnectedWorker> {
    let request_url = worker_websocket_url(&config.connect, &config.token)?;
    let (mut socket, _) = connect_async(&request_url)
        .await
        .map_err(|error| websocket_connect_error(&request_url, error))?;

    send_json(&mut socket, json!(["1", "1", CRF_SEARCH_TOPIC, "phx_join", {}])).await?;
    let join: JoinResponse = expect_reply(&mut socket, "1", "phx_join").await?;

    send_json(
        &mut socket,
        json!([
            "1",
            "2",
            CRF_SEARCH_TOPIC,
            "announce",
            AnnouncePayload {
                worker_id: &config.worker_id,
                protocol_version: config.protocol_version,
                version: &config.version,
                capabilities: Capabilities { crf_search: true },
            }
        ]),
    )
    .await?;
    let announce: AnnounceResponse = expect_reply(&mut socket, "2", "announce").await?;
    if !announce.accepted {
        bail!("worker announcement was not accepted");
    }

    Ok(ConnectedWorker {
        assigned_worker_id: join.worker_id,
        negotiated_protocol_version: announce.protocol_version,
        next_ref: 3,
        socket,
    })
}

async fn request_work(worker: &mut ConnectedWorker) -> Result<PullWorkResponse> {
    let request_ref = worker.next_ref.to_string();
    worker.next_ref += 1;

    send_json(
        &mut worker.socket,
        json!(["1", request_ref, CRF_SEARCH_TOPIC, "pull_work", {}]),
    )
    .await?;
    expect_reply(&mut worker.socket, &request_ref, "pull_work").await
}

async fn run_worker_session(config: &WorkerConfig) -> Result<WorkerSession> {
    let mut worker = connect_worker(config).await?;
    let pull_work = request_work(&mut worker).await?;

    Ok(WorkerSession {
        assigned_worker_id: worker.assigned_worker_id,
        negotiated_protocol_version: worker.negotiated_protocol_version,
        work_status: pull_work.status,
    })
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
            anyhow!("connect websocket {request_url}: HTTP {status} {}", body.trim())
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

async fn send_json<W>(writer: &mut W, value: Value) -> Result<()>
where
    W: SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    writer
        .send(Message::Text(value.to_string()))
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
                let ReplyEnvelope(_, msg_ref, topic, event, body) =
                    serde_json::from_str(&text).context("decode phoenix reply")?;
                if topic != CRF_SEARCH_TOPIC || event != "phx_reply" || msg_ref.as_deref() != Some(expected_ref) {
                    continue;
                }

                return match body.status.as_str() {
                    "ok" => serde_json::from_value(body.response).context("decode phoenix ok reply"),
                    "error" => Err(anyhow!(
                        "{expected_event} failed: {}",
                        body.response
                    )),
                    other => Err(anyhow!("unexpected phoenix status {other}")),
                };
            }
            Message::Close(frame) => bail!("websocket closed before {expected_event} reply: {frame:?}"),
            Message::Ping(_) | Message::Pong(_) | Message::Binary(_) | Message::Frame(_) => continue,
        }
    }

    bail!("websocket ended before {expected_event} reply")
}

#[cfg(test)]
mod tests {
    use super::*;
    use anyhow::Result;
    use tokio::net::TcpListener;
    use tokio_tungstenite::{accept_async, tungstenite::Message};

    #[test]
    fn args_lower_to_worker_config() {
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
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept connection");
            let socket = accept_async(stream).await.expect("accept websocket");
            let (mut writer, mut reader) = socket.split();

            assert_text_message(
                reader.next().await.expect("join frame").expect("join message"),
                json!(["1", "1", CRF_SEARCH_TOPIC, "phx_join", {}]),
            );
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

            assert_text_message(
                reader
                    .next()
                    .await
                    .expect("announce frame")
                    .expect("announce message"),
                json!([
                    "1",
                    "2",
                    CRF_SEARCH_TOPIC,
                    "announce",
                    {
                        "worker_id": "abav1-dev",
                        "protocol_version": 1,
                        "version": "0.11.4",
                        "capabilities": {"crf_search": true}
                    }
                ]),
            );
            writer
                .send(Message::Text(
                    json!([null, "2", CRF_SEARCH_TOPIC, "phx_reply", {
                        "status": "ok",
                        "response": {"accepted": true, "protocol_version": 1}
                    }])
                    .to_string(),
                ))
                .await
                .expect("send announce reply");

            assert_text_message(
                reader
                    .next()
                    .await
                    .expect("pull_work frame")
                    .expect("pull_work message"),
                json!(["1", "3", CRF_SEARCH_TOPIC, "pull_work", {}]),
            );
            writer
                .send(Message::Text(
                    json!([null, "3", CRF_SEARCH_TOPIC, "phx_reply", {
                        "status": "ok",
                        "response": {"status": "no_work"}
                    }])
                    .to_string(),
                ))
                .await
                .expect("send pull_work reply");
        });

        let session = run_worker_session(&WorkerConfig {
            connect: format!("http://{address}"),
            token: "test-worker-token".into(),
            worker_id: "abav1-dev".into(),
            version: "0.11.4".into(),
            protocol_version: 1,
            once: false,
        })
        .await?;

        assert_eq!(
            session,
            WorkerSession {
                assigned_worker_id: "worker-123".into(),
                negotiated_protocol_version: 1,
                work_status: "no_work".into(),
            }
        );

        server.await.expect("server task");
        Ok(())
    }

    #[tokio::test(flavor = "current_thread")]
    async fn worker_stays_connected_and_pulls_work_again_after_no_work() -> Result<()> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;

        let server = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept connection");
            let socket = accept_async(stream).await.expect("accept websocket");
            let (mut writer, mut reader) = socket.split();

            assert_text_message(
                reader.next().await.expect("join frame").expect("join message"),
                json!(["1", "1", CRF_SEARCH_TOPIC, "phx_join", {}]),
            );
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

            assert_text_message(
                reader.next().await.expect("announce frame").expect("announce message"),
                json!([
                    "1",
                    "2",
                    CRF_SEARCH_TOPIC,
                    "announce",
                    {
                        "worker_id": "abav1-dev",
                        "protocol_version": 1,
                        "version": "0.11.4",
                        "capabilities": {"crf_search": true}
                    }
                ]),
            );
            writer
                .send(Message::Text(
                    json!([null, "2", CRF_SEARCH_TOPIC, "phx_reply", {
                        "status": "ok",
                        "response": {"accepted": true, "protocol_version": 1}
                    }])
                    .to_string(),
                ))
                .await
                .expect("send announce reply");

            assert_text_message(
                reader.next().await.expect("first pull").expect("first pull message"),
                json!(["1", "3", CRF_SEARCH_TOPIC, "pull_work", {}]),
            );
            writer
                .send(Message::Text(
                    json!([null, "3", CRF_SEARCH_TOPIC, "phx_reply", {
                        "status": "ok",
                        "response": {"status": "no_work"}
                    }])
                    .to_string(),
                ))
                .await
                .expect("send first pull reply");

            assert_text_message(
                reader.next().await.expect("second pull").expect("second pull message"),
                json!(["1", "4", CRF_SEARCH_TOPIC, "pull_work", {}]),
            );
            writer
                .send(Message::Text(
                    json!([null, "4", CRF_SEARCH_TOPIC, "phx_reply", {
                        "status": "ok",
                        "response": {"status": "no_work"}
                    }])
                    .to_string(),
                ))
                .await
                .expect("send second pull reply");
        });

        run_worker_until(
            &WorkerConfig {
                connect: format!("http://{address}"),
                token: "test-worker-token".into(),
                worker_id: "abav1-dev".into(),
                version: "0.11.4".into(),
                protocol_version: 1,
                once: false,
            },
            WorkerRuntime {
                idle_delay: Duration::from_millis(1),
                reconnect_delay: Duration::from_millis(1),
                max_pulls: Some(2),
            },
        )
        .await?;

        server.await.expect("server task");
        Ok(())
    }

    fn assert_text_message(message: Message, expected: Value) {
        let Message::Text(text) = message else {
            panic!("expected text frame, got {message:?}");
        };
        let actual: Value = serde_json::from_str(&text).expect("decode message");
        assert_eq!(actual, expected);
    }
}
