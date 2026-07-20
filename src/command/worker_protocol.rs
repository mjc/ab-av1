use serde::{Deserialize, Serialize, Serializer};

pub(crate) const CRF_SEARCH_TOPIC: &str = "workers:crf_search";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum JobKind {
    #[default]
    CrfSearch,
    Encode,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
pub(crate) struct ClientFrame(String, String, String, String, ClientPayload);

impl ClientFrame {
    pub(crate) fn new(reference: u64, event: ClientEvent) -> Self {
        let (event_name, payload) = event.into_parts();
        Self(
            "1".into(),
            reference.to_string(),
            CRF_SEARCH_TOPIC.into(),
            event_name.into(),
            payload,
        )
    }
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq)]
pub(crate) enum ClientEvent {
    Join,
    Announce(AnnouncePayload),
    PullWork(PullWorkPayload),
    Heartbeat(HeartbeatPayload),
    ControlState(ControlStatePayload),
    TransferProgress(TransferProgressPayload),
    TransferFailure(TransferFailurePayload),
    CrfSearchProgress(CrfSearchProgressPayload),
    CrfSearchResult(CrfSearchResultPayload),
    #[cfg_attr(not(test), allow(dead_code))]
    EncodeProgress(EncodeProgressPayload),
    EncodeCompleted(EncodeCompletedPayload),
    VideoFailed(FailureReportPayload),
}

impl ClientEvent {
    fn into_parts(self) -> (&'static str, ClientPayload) {
        match self {
            Self::Join => ("phx_join", ClientPayload::Empty(EmptyPayload {})),
            Self::Announce(payload) => ("announce", ClientPayload::Announce(payload)),
            Self::PullWork(payload) => ("pull_work", ClientPayload::PullWork(payload)),
            Self::Heartbeat(payload) => ("heartbeat", ClientPayload::Heartbeat(payload)),
            Self::ControlState(payload) => ("control_state", ClientPayload::ControlState(payload)),
            Self::TransferProgress(payload) => (
                "transfer_progress",
                ClientPayload::TransferProgress(payload),
            ),
            Self::TransferFailure(payload) => {
                ("transfer_failed", ClientPayload::TransferFailure(payload))
            }
            Self::CrfSearchProgress(payload) => {
                ("crf_search_progress", ClientPayload::Progress(payload))
            }
            Self::CrfSearchResult(payload) => ("crf_search_result", ClientPayload::Result(payload)),
            Self::EncodeProgress(payload) => {
                ("encode_progress", ClientPayload::EncodeProgress(payload))
            }
            Self::EncodeCompleted(payload) => {
                ("encode_completed", ClientPayload::EncodeCompleted(payload))
            }
            Self::VideoFailed(payload) => ("video_failed", ClientPayload::Failure(payload)),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(untagged)]
enum ClientPayload {
    Empty(EmptyPayload),
    Announce(AnnouncePayload),
    PullWork(PullWorkPayload),
    Heartbeat(HeartbeatPayload),
    ControlState(ControlStatePayload),
    TransferProgress(TransferProgressPayload),
    TransferFailure(TransferFailurePayload),
    Progress(CrfSearchProgressPayload),
    Result(CrfSearchResultPayload),
    EncodeProgress(EncodeProgressPayload),
    EncodeCompleted(EncodeCompletedPayload),
    Failure(FailureReportPayload),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct EmptyPayload {}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct PullWorkPayload {
    #[serde(default, skip_serializing_if = "is_false")]
    pub(crate) input_missing: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) job_type: Option<JobKind>,
}

impl PullWorkPayload {
    pub(crate) fn input_missing() -> Self {
        Self {
            input_missing: true,
            job_type: None,
        }
    }
}

fn is_false(value: &bool) -> bool {
    !*value
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct AnnouncePayload {
    pub(crate) worker_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) hostname: Option<String>,
    pub(crate) protocol_version: u64,
    pub(crate) version: String,
    pub(crate) capabilities: Capabilities,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Capabilities {
    pub(crate) crf_search: bool,
    pub(crate) encode: bool,
    pub(crate) mode: String,
    pub(crate) logical_cpus: usize,
    pub(crate) max_active_jobs: usize,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct HeartbeatPayload {
    #[serde(skip_serializing_if = "Option::is_none")]
    #[serde(serialize_with = "serialize_rounded_option_f32")]
    pub(crate) cpu_percent: Option<f32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) memory_rss_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) memory_total_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) disk_free_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) disk_total_bytes: Option<u64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub(crate) active_video_id: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct CrfSearchProgressPayload {
    pub(crate) video_id: u64,
    #[serde(serialize_with = "serialize_rounded_f32")]
    pub(crate) percent: f32,
    pub(crate) filename: String,
    pub(crate) eta: Option<u64>,
    #[serde(serialize_with = "serialize_rounded_f32")]
    pub(crate) fps: f32,
    #[serde(serialize_with = "serialize_rounded_f32")]
    pub(crate) crf: f32,
    pub(crate) sample_num: u64,
    pub(crate) total_samples: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct CrfSearchResultPayload {
    pub(crate) job_id: String,
    pub(crate) video_id: u64,
    pub(crate) source_name: String,
    #[serde(serialize_with = "serialize_rounded_f32")]
    pub(crate) crf: f32,
    #[serde(serialize_with = "serialize_rounded_option_f32")]
    pub(crate) vmaf_score: Option<f32>,
    #[serde(serialize_with = "serialize_rounded_option_f32")]
    pub(crate) xpsnr_score: Option<f32>,
    pub(crate) predicted_encode_size: u64,
    #[serde(serialize_with = "serialize_rounded_f64")]
    pub(crate) encode_percent: f64,
    #[serde(serialize_with = "serialize_rounded_f64")]
    pub(crate) predicted_encode_time_secs: f64,
    pub(crate) from_cache: bool,
    #[serde(serialize_with = "serialize_rounded_f32")]
    pub(crate) score: f32,
    #[serde(serialize_with = "serialize_rounded_f64")]
    pub(crate) percent: f64,
    pub(crate) size: u64,
    #[serde(serialize_with = "serialize_rounded_f64")]
    pub(crate) time: f64,
    pub(crate) params: serde_json::Value,
    #[serde(serialize_with = "serialize_rounded_f32")]
    pub(crate) target: f32,
    pub(crate) chosen: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct EncodeProgressPayload {
    pub(crate) job_id: String,
    pub(crate) video_id: u64,
    #[serde(serialize_with = "serialize_rounded_f64")]
    pub(crate) percent: f64,
    #[serde(serialize_with = "serialize_rounded_f32")]
    pub(crate) fps: f32,
    pub(crate) eta: Option<u64>,
    pub(crate) output_bytes: u64,
    #[serde(serialize_with = "serialize_rounded_f64")]
    pub(crate) output_percent: f64,
    pub(crate) throughput: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct EncodeCompletedPayload {
    pub(crate) job_id: String,
    pub(crate) video_id: u64,
    pub(crate) source_name: String,
    pub(crate) output_path: String,
    pub(crate) output_bytes: u64,
    #[serde(serialize_with = "serialize_rounded_f64")]
    pub(crate) output_percent: f64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct FailureReportPayload {
    pub(crate) video_id: u64,
    pub(crate) stage: String,
    pub(crate) category: String,
    pub(crate) message: String,
    pub(crate) code: String,
    pub(crate) context: serde_json::Value,
    pub(crate) retriable: bool,
    pub(crate) stderr_excerpt: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct ServerFrame<T>(
    pub(crate) Option<String>,
    pub(crate) String,
    pub(crate) String,
    pub(crate) String,
    pub(crate) ReplyBody<T>,
);

impl<T> ServerFrame<T> {
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn reply(reference: u64, body: ReplyBody<T>) -> Self {
        Self(
            None,
            reference.to_string(),
            CRF_SEARCH_TOPIC.into(),
            "phx_reply".into(),
            body,
        )
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct ServerPushFrame<T>(
    pub(crate) Option<String>,
    pub(crate) Option<String>,
    pub(crate) String,
    pub(crate) String,
    pub(crate) T,
);

impl<T> ServerPushFrame<T> {
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn new(event: &str, payload: T) -> Self {
        Self(None, None, CRF_SEARCH_TOPIC.into(), event.into(), payload)
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct ReplyBody<T> {
    pub(crate) status: String,
    pub(crate) response: T,
}

impl<T> ReplyBody<T> {
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn ok(response: T) -> Self {
        Self {
            status: "ok".into(),
            response,
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn error(response: T) -> Self {
        Self {
            status: "error".into(),
            response,
        }
    }
}

#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum ServerReply {
    JobAssigned(JobAssignedPayload),
    NoWork(NoWorkPayload),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WorkStatus {
    NoWork,
    JobAssigned,
    JobInProgress,
}

impl WorkStatus {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Self::NoWork => "no_work",
            Self::JobAssigned => "job_assigned",
            Self::JobInProgress => "job_in_progress",
        }
    }
}

impl<'de> Deserialize<'de> for WorkStatus {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let status = String::deserialize(deserializer)?;
        match status.as_str() {
            "no_work" => Ok(Self::NoWork),
            "job_assigned" => Ok(Self::JobAssigned),
            "job_in_progress" => Ok(Self::JobInProgress),
            other => Err(serde::de::Error::unknown_variant(
                other,
                &["no_work", "job_assigned", "job_in_progress"],
            )),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct NoWorkPayload {
    pub(crate) status: WorkStatus,
}

impl<'de> Deserialize<'de> for NoWorkPayload {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        #[serde(untagged)]
        enum NoWorkRepr {
            Null(()),
            Explicit { status: WorkStatus },
        }

        match NoWorkRepr::deserialize(deserializer)? {
            NoWorkRepr::Null(()) => Ok(NoWorkPayload {
                status: WorkStatus::NoWork,
            }),
            NoWorkRepr::Explicit { status } => Ok(NoWorkPayload { status }),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct JobAssignedPayload {
    pub(crate) status: WorkStatus,
    #[serde(default)]
    pub(crate) job_type: JobKind,
    pub(crate) job_id: String,
    pub(crate) video_id: u64,
    pub(crate) source_name: String,
    pub(crate) size_bytes: u64,
    #[serde(default)]
    pub(crate) chunk_size_bytes: u64,
    pub(crate) target_vmaf: f32,
    #[serde(default)]
    pub(crate) crf_search_args: Vec<String>,
    #[serde(default)]
    pub(crate) encode_args: Vec<String>,
    #[serde(default)]
    pub(crate) transfer: Option<TransferSpec>,
    #[serde(default)]
    pub(crate) output_transfer: Option<TransferSpec>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TransferSpec {
    pub(crate) url: String,
    pub(crate) auth: TransferAuth,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TransferAuth {
    pub(crate) scheme: String,
    pub(crate) header: String,
    pub(crate) value: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ErrorReplyPayload {
    pub(crate) reason: String,
    #[serde(default)]
    pub(crate) supported_protocol_versions: Vec<u64>,
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct CancelPayload {
    pub(crate) job_id: String,
    pub(crate) reason: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ControlAction {
    Pause,
    Resume,
    Start,
    Stop,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ControlPayload {
    pub(crate) action: ControlAction,
    #[serde(default)]
    pub(crate) video_id: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) job_id: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ControlStatePayload {
    pub(crate) state: ControlState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub(crate) active_video_id: Option<u64>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum ControlState {
    Running,
    Paused,
    Stopped,
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TransferStartedPayload {
    pub(crate) chunk_size_bytes: u64,
    pub(crate) size_bytes: u64,
    pub(crate) source_name: String,
    pub(crate) status: String,
    pub(crate) total_bytes: u64,
    pub(crate) total_chunks: u64,
    pub(crate) transfer_id: String,
    pub(crate) video_id: u64,
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct TransferProgressPayload {
    pub(crate) job_id: String,
    pub(crate) transfer_id: String,
    pub(crate) video_id: u64,
    pub(crate) filename: String,
    pub(crate) received_bytes: u64,
    pub(crate) expected_bytes: Option<u64>,
    #[serde(serialize_with = "serialize_rounded_f64")]
    pub(crate) percent: f64,
    pub(crate) bytes_per_second: u64,
    pub(crate) eta: Option<u64>,
    pub(crate) chunk_index: u64,
    pub(crate) total_chunks: u64,
}

fn serialize_rounded_f32<S>(value: &f32, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_f64(round_two(*value as f64))
}

fn serialize_rounded_option_f32<S>(value: &Option<f32>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    value
        .map(|value| round_two(value as f64))
        .serialize(serializer)
}

fn serialize_rounded_f64<S>(value: &f64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_f64(round_two(*value))
}

fn round_two(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TransferFailurePayload {
    pub(crate) job_id: String,
    pub(crate) stage: TransferStage,
    pub(crate) retriable: bool,
    pub(crate) reason: String,
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TransferStage {
    ReceiveChunk,
    ValidateChunk,
    FinalizeTransfer,
    RunCrfSearch,
}

impl ErrorReplyPayload {
    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn new(reason: impl Into<String>) -> Self {
        Self {
            reason: reason.into(),
            supported_protocol_versions: Vec::new(),
        }
    }

    #[cfg_attr(not(test), allow(dead_code))]
    pub(crate) fn with_supported_protocol_versions(mut self, versions: Vec<u64>) -> Self {
        self.supported_protocol_versions = versions;
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn announce_request_serializes_to_current_reencodarr_contract() {
        let frame = ClientFrame::new(
            2,
            ClientEvent::Announce(AnnouncePayload {
                worker_id: "abav1-dev".into(),
                hostname: None,
                protocol_version: 1,
                version: "0.11.4".into(),
                capabilities: Capabilities {
                    crf_search: true,
                    encode: false,
                    mode: "crf-search".into(),
                    logical_cpus: 8,
                    max_active_jobs: 1,
                },
            }),
        );

        assert_eq!(
            serde_json::to_value(frame).expect("serialize announce"),
            json!([
                "1",
                "2",
                "workers:crf_search",
                "announce",
                {
                    "worker_id": "abav1-dev",
                    "protocol_version": 1,
                    "version": "0.11.4",
                    "capabilities": {
                        "crf_search": true,
                        "encode": false,
                        "mode": "crf-search",
                        "logical_cpus": 8,
                        "max_active_jobs": 1
                    }
                }
            ])
        );
    }

    #[test]
    fn heartbeat_serializes_worker_telemetry_event() {
        let frame = ClientFrame::new(
            4,
            ClientEvent::Heartbeat(HeartbeatPayload {
                cpu_percent: Some(12.5),
                memory_rss_bytes: Some(1234),
                memory_total_bytes: Some(8192),
                disk_free_bytes: Some(4096),
                disk_total_bytes: Some(16_384),
                active_video_id: Some(123),
            }),
        );

        assert_eq!(
            serde_json::to_value(frame).expect("serialize heartbeat"),
            json!([
                "1",
                "4",
                "workers:crf_search",
                "heartbeat",
                {
                    "cpu_percent": 12.5,
                    "memory_rss_bytes": 1234,
                    "memory_total_bytes": 8192,
                    "disk_free_bytes": 4096,
                    "disk_total_bytes": 16_384,
                    "active_video_id": 123,
                }
            ])
        );
    }

    #[test]
    fn control_state_serializes_worker_acknowledgement() {
        let frame = ClientFrame::new(
            5,
            ClientEvent::ControlState(ControlStatePayload {
                state: ControlState::Paused,
                active_video_id: Some(123),
            }),
        );

        assert_eq!(
            serde_json::to_value(frame).expect("serialize control state"),
            json!([
                "1",
                "5",
                "workers:crf_search",
                "control_state",
                {"state": "paused", "active_video_id": 123}
            ])
        );
    }

    #[test]
    fn outbound_float_payloads_round_to_two_places() {
        assert_eq!(
            serde_json::to_value(HeartbeatPayload {
                cpu_percent: Some(12.346),
                memory_rss_bytes: None,
                memory_total_bytes: None,
                disk_free_bytes: None,
                disk_total_bytes: None,
                active_video_id: None,
            })
            .expect("serialize heartbeat floats"),
            json!({ "cpu_percent": 12.35 })
        );

        assert_eq!(
            serde_json::to_value(CrfSearchProgressPayload {
                video_id: 123,
                percent: 42.346,
                filename: "movie.mkv".into(),
                eta: None,
                fps: 27.346,
                crf: 31.346,
                sample_num: 2,
                total_samples: 4,
            })
            .expect("serialize crf progress floats"),
            json!({
                "video_id": 123,
                "percent": 42.35,
                "filename": "movie.mkv",
                "eta": null,
                "fps": 27.35,
                "crf": 31.35,
                "sample_num": 2,
                "total_samples": 4,
            })
        );

        assert_eq!(
            serde_json::to_value(TransferProgressPayload {
                job_id: "job-123".into(),
                transfer_id: "job-123".into(),
                video_id: 123,
                filename: "movie.mkv".into(),
                received_bytes: 512,
                expected_bytes: Some(1024),
                percent: 66.666,
                bytes_per_second: 256,
                eta: Some(2),
                chunk_index: 3,
                total_chunks: 8,
            })
            .expect("serialize transfer progress floats"),
            json!({
                "job_id": "job-123",
                "transfer_id": "job-123",
                "video_id": 123,
                "filename": "movie.mkv",
                "received_bytes": 512,
                "expected_bytes": 1024,
                "percent": 66.67,
                "bytes_per_second": 256,
                "eta": 2,
                "chunk_index": 3,
                "total_chunks": 8,
            })
        );

        assert_eq!(
            serde_json::to_value(CrfSearchResultPayload {
                job_id: "job-123".into(),
                video_id: 123,
                source_name: "movie.mkv".into(),
                crf: 31.346,
                vmaf_score: Some(96.346),
                xpsnr_score: Some(95.346),
                predicted_encode_size: 123,
                encode_percent: 42.346,
                predicted_encode_time_secs: 87.346,
                from_cache: false,
                score: 96.346,
                percent: 42.346,
                size: 123,
                time: 87.346,
                params: json!({}),
                target: 95.346,
                chosen: true,
            })
            .expect("serialize crf result floats"),
            json!({
                "job_id": "job-123",
                "video_id": 123,
                "source_name": "movie.mkv",
                "crf": 31.35,
                "vmaf_score": 96.35,
                "xpsnr_score": 95.35,
                "predicted_encode_size": 123,
                "encode_percent": 42.35,
                "predicted_encode_time_secs": 87.35,
                "from_cache": false,
                "score": 96.35,
                "percent": 42.35,
                "size": 123,
                "time": 87.35,
                "params": {},
                "target": 95.35,
                "chosen": true,
            })
        );
    }

    #[test]
    fn transfer_progress_serializes_transfer_stats_event() {
        let frame = ClientFrame::new(
            5,
            ClientEvent::TransferProgress(TransferProgressPayload {
                job_id: "job-123".into(),
                transfer_id: "job-123".into(),
                video_id: 123,
                filename: "movie.mkv".into(),
                received_bytes: 512,
                expected_bytes: Some(1024),
                percent: 50.0,
                bytes_per_second: 256,
                eta: Some(2),
                chunk_index: 3,
                total_chunks: 8,
            }),
        );

        assert_eq!(
            serde_json::to_value(frame).expect("serialize transfer progress"),
            json!([
                "1",
                "5",
                "workers:crf_search",
                "transfer_progress",
                {
                    "job_id": "job-123",
                    "transfer_id": "job-123",
                    "video_id": 123,
                    "filename": "movie.mkv",
                    "received_bytes": 512,
                    "expected_bytes": 1024,
                    "percent": 50.0,
                    "bytes_per_second": 256,
                    "eta": 2,
                    "chunk_index": 3,
                    "total_chunks": 8,
                }
            ])
        );
    }

    #[test]
    fn crf_search_progress_serializes_reencodarr_progress_event() {
        let frame = ClientFrame::new(
            5,
            ClientEvent::CrfSearchProgress(CrfSearchProgressPayload {
                video_id: 123,
                percent: 42.5,
                filename: "movie.mkv".into(),
                eta: None,
                fps: 27.25,
                crf: 31.0,
                sample_num: 2,
                total_samples: 4,
            }),
        );

        assert_eq!(
            serde_json::to_value(frame).expect("serialize crf progress"),
            json!([
                "1",
                "5",
                "workers:crf_search",
                "crf_search_progress",
                {
                    "video_id": 123,
                    "percent": 42.5,
                    "filename": "movie.mkv",
                    "eta": null,
                    "fps": 27.25,
                    "crf": 31.0,
                    "sample_num": 2,
                    "total_samples": 4,
                }
            ])
        );
    }

    #[test]
    fn crf_search_result_serializes_reencodarr_vmaf_model_event() {
        let frame = ClientFrame::new(
            6,
            ClientEvent::CrfSearchResult(CrfSearchResultPayload {
                job_id: "job-123".into(),
                video_id: 123,
                source_name: "movie.mkv".into(),
                crf: 31.5,
                vmaf_score: Some(96.2),
                xpsnr_score: None,
                predicted_encode_size: 123_456,
                encode_percent: 42.5,
                predicted_encode_time_secs: 87.5,
                from_cache: true,
                score: 96.2,
                percent: 42.5,
                size: 123_456,
                time: 87.5,
                params: json!({ "encoder": "libsvtav1", "preset": 8 }),
                target: 95.0,
                chosen: true,
            }),
        );

        assert_eq!(
            serde_json::to_value(frame).expect("serialize crf result"),
            json!([
                "1",
                "6",
                "workers:crf_search",
                "crf_search_result",
                {
                    "job_id": "job-123",
                    "video_id": 123,
                    "source_name": "movie.mkv",
                    "crf": 31.5,
                    "vmaf_score": 96.2,
                    "xpsnr_score": null,
                    "predicted_encode_size": 123456,
                    "encode_percent": 42.5,
                    "predicted_encode_time_secs": 87.5,
                    "from_cache": true,
                    "score": 96.2,
                    "percent": 42.5,
                    "size": 123456,
                    "time": 87.5,
                    "params": { "encoder": "libsvtav1", "preset": 8 },
                    "target": 95.0,
                    "chosen": true,
                }
            ])
        );
    }

    #[test]
    fn server_reply_parses_current_no_work_payload() {
        let reply: ServerFrame<ServerReply> = serde_json::from_value(json!([
            null,
            "3",
            "workers:crf_search",
            "phx_reply",
            {
                "status": "ok",
                "response": { "status": "no_work" }
            }
        ]))
        .expect("parse no_work reply");

        assert_eq!(
            reply,
            ServerFrame::reply(
                3,
                ReplyBody::ok(ServerReply::NoWork(NoWorkPayload {
                    status: WorkStatus::NoWork,
                })),
            )
        );
    }

    #[test]
    fn server_reply_parses_null_no_work_payload() {
        let reply: ServerFrame<ServerReply> = serde_json::from_value(json!([
            null,
            "3",
            "workers:crf_search",
            "phx_reply",
            {
                "status": "ok",
                "response": null
            }
        ]))
        .expect("parse null no_work reply");

        assert_eq!(
            reply,
            ServerFrame::reply(
                3,
                ReplyBody::ok(ServerReply::NoWork(NoWorkPayload {
                    status: WorkStatus::NoWork,
                })),
            )
        );
    }

    #[test]
    fn server_reply_parses_future_job_assignment_payload() {
        let reply: ServerFrame<ServerReply> = serde_json::from_value(json!([
            null,
            "4",
            "workers:crf_search",
            "phx_reply",
            {
                "status": "ok",
                "response": {
                    "status": "job_assigned",
                    "job_id": "job-123",
                    "video_id": 123,
                    "source_name": "movie.mkv",
                    "size_bytes": 1024,
                    "chunk_size_bytes": 256,
                    "target_vmaf": 96.5,
                    "transfer": {
                        "url": "http://10.0.0.10:4000/workers/files/123",
                        "auth": {
                            "scheme": "bearer",
                            "header": "authorization",
                            "value": "Bearer transfer-token"
                        }
                    },
                    "crf_search_args": [
                        "crf-search",
                        "--input",
                        "/server/movie.mkv",
                        "--min-vmaf",
                        "96.5"
                    ]
                }
            }
        ]))
        .expect("parse job_assigned reply");

        assert_eq!(
            reply,
            ServerFrame::reply(
                4,
                ReplyBody::ok(ServerReply::JobAssigned(JobAssignedPayload {
                    status: WorkStatus::JobAssigned,
                    job_type: JobKind::CrfSearch,
                    job_id: "job-123".into(),
                    video_id: 123,
                    source_name: "movie.mkv".into(),
                    size_bytes: 1024,
                    chunk_size_bytes: 256,
                    target_vmaf: 96.5,
                    transfer: Some(super::TransferSpec {
                        url: "http://10.0.0.10:4000/workers/files/123".into(),
                        auth: super::TransferAuth {
                            scheme: "bearer".into(),
                            header: "authorization".into(),
                            value: "Bearer transfer-token".into(),
                        },
                    }),
                    output_transfer: None,
                    encode_args: Vec::new(),
                    crf_search_args: vec![
                        "crf-search".into(),
                        "--input".into(),
                        "/server/movie.mkv".into(),
                        "--min-vmaf".into(),
                        "96.5".into(),
                    ],
                })),
            )
        );
    }

    #[test]
    fn server_reply_parses_in_progress_assignment_without_chunk_size() {
        let reply: ServerFrame<ServerReply> = serde_json::from_value(json!([
            null,
            "3",
            "workers:crf_search",
            "phx_reply",
            {
                "status": "ok",
                "response": {
                    "status": "job_in_progress",
                    "source_name": "movie.mkv",
                    "video_id": 123,
                    "job_id": "job-123",
                    "size_bytes": 1024,
                    "target_vmaf": 95,
                    "crf_search_args": [
                        "crf-search",
                        "--input",
                        "/server/movie.mkv",
                        "--min-vmaf",
                        "95"
                    ]
                }
            }
        ]))
        .expect("parse job_in_progress reply without chunk size");

        assert_eq!(
            reply,
            ServerFrame::reply(
                3,
                ReplyBody::ok(ServerReply::JobAssigned(JobAssignedPayload {
                    status: WorkStatus::JobInProgress,
                    job_type: JobKind::CrfSearch,
                    job_id: "job-123".into(),
                    video_id: 123,
                    source_name: "movie.mkv".into(),
                    size_bytes: 1024,
                    chunk_size_bytes: 0,
                    target_vmaf: 95.0,
                    transfer: None,
                    output_transfer: None,
                    encode_args: Vec::new(),
                    crf_search_args: vec![
                        "crf-search".into(),
                        "--input".into(),
                        "/server/movie.mkv".into(),
                        "--min-vmaf".into(),
                        "95".into(),
                    ],
                })),
            )
        );
    }

    #[test]
    fn server_error_reply_parses_protocol_mismatch_payload() {
        let reply: ServerFrame<ErrorReplyPayload> = serde_json::from_value(json!([
            null,
            "2",
            "workers:crf_search",
            "phx_reply",
            {
                "status": "error",
                "response": {
                    "reason": "unsupported_protocol_version",
                    "supported_protocol_versions": [1]
                }
            }
        ]))
        .expect("parse error reply");

        assert_eq!(
            reply,
            ServerFrame::reply(
                2,
                ReplyBody::error(
                    ErrorReplyPayload::new("unsupported_protocol_version")
                        .with_supported_protocol_versions(vec![1]),
                ),
            )
        );
    }

    #[test]
    fn server_push_parses_cancel_payload() {
        let push: ServerPushFrame<CancelPayload> = serde_json::from_value(json!([
            null,
            null,
            "workers:crf_search",
            "cancel",
            {
                "job_id": "job-123",
                "reason": "shutdown"
            }
        ]))
        .expect("parse cancel push");

        assert_eq!(
            push,
            ServerPushFrame::new(
                "cancel",
                CancelPayload {
                    job_id: "job-123".into(),
                    reason: "shutdown".into(),
                },
            )
        );
    }

    #[test]
    fn server_push_parses_worker_control_payload() {
        let push: ServerPushFrame<ControlPayload> = serde_json::from_value(json!([
            null,
            null,
            "workers:crf_search",
            "control",
            {
                "action": "pause",
                "video_id": 123
            }
        ]))
        .expect("parse worker control push");

        assert_eq!(
            push,
            ServerPushFrame::new(
                "control",
                ControlPayload {
                    action: ControlAction::Pause,
                    video_id: Some(123),
                    job_id: None,
                },
            )
        );
    }

    #[test]
    fn transfer_started_payload_serializes_metadata_side_channel() {
        let payload = TransferStartedPayload {
            chunk_size_bytes: 1_048_576,
            size_bytes: 9_560_739_312,
            source_name: "movie.mkv".into(),
            status: "transfer_started".into(),
            total_bytes: 9_560_739_312,
            total_chunks: 9_118,
            transfer_id: "job-123".into(),
            video_id: 123,
        };

        assert_eq!(
            serde_json::to_value(payload).expect("serialize transfer started"),
            json!({
                "chunk_size_bytes": 1_048_576,
                "size_bytes": 9_560_739_312u64,
                "source_name": "movie.mkv",
                "status": "transfer_started",
                "total_bytes": 9_560_739_312u64,
                "total_chunks": 9_118,
                "transfer_id": "job-123",
                "video_id": 123,
            })
        );
    }

    #[test]
    fn pull_work_payload_omits_default_and_reports_missing_input() {
        assert_eq!(
            serde_json::to_value(PullWorkPayload::default()).expect("serialize pull_work"),
            json!({})
        );
        assert_eq!(
            serde_json::to_value(PullWorkPayload::input_missing()).expect("serialize pull_work"),
            json!({"input_missing": true})
        );
    }

    #[test]
    fn encode_completion_serializes_job_identity_and_output_metrics() {
        let frame = ClientFrame::new(
            7,
            ClientEvent::EncodeCompleted(EncodeCompletedPayload {
                job_id: "encode-7".into(),
                video_id: 42,
                source_name: "movie.mkv".into(),
                output_path: "/worker/movie.av1.mkv".into(),
                output_bytes: 800,
                output_percent: 40.25,
            }),
        );

        assert_eq!(
            serde_json::to_value(frame).expect("serialize encode completion"),
            json!([
                "1",
                "7",
                "workers:crf_search",
                "encode_completed",
                {
                    "job_id": "encode-7",
                    "video_id": 42,
                    "source_name": "movie.mkv",
                    "output_path": "/worker/movie.av1.mkv",
                    "output_bytes": 800,
                    "output_percent": 40.25
                }
            ])
        );
    }

    #[test]
    fn transfer_failure_payload_serializes_stage_and_retry_hint() {
        let payload = TransferFailurePayload {
            job_id: "job-123".into(),
            stage: TransferStage::FinalizeTransfer,
            retriable: true,
            reason: "disk full".into(),
        };

        assert_eq!(
            serde_json::to_value(payload).expect("serialize transfer failure"),
            json!({
                "job_id": "job-123",
                "stage": "finalize_transfer",
                "retriable": true,
                "reason": "disk full",
            })
        );
    }
}
