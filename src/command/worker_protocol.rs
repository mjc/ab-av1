use serde::{Deserialize, Serialize};

pub(crate) const CRF_SEARCH_TOPIC: &str = "workers:crf_search";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
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

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ClientEvent {
    Join,
    Announce(AnnouncePayload),
    PullWork,
}

impl ClientEvent {
    fn into_parts(self) -> (&'static str, ClientPayload) {
        match self {
            Self::Join => ("phx_join", ClientPayload::Empty(EmptyPayload {})),
            Self::Announce(payload) => ("announce", ClientPayload::Announce(payload)),
            Self::PullWork => ("pull_work", ClientPayload::Empty(EmptyPayload {})),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(untagged)]
enum ClientPayload {
    Empty(EmptyPayload),
    Announce(AnnouncePayload),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
struct EmptyPayload {}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct AnnouncePayload {
    pub(crate) worker_id: String,
    pub(crate) protocol_version: u64,
    pub(crate) version: String,
    pub(crate) capabilities: Capabilities,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct Capabilities {
    pub(crate) crf_search: bool,
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

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(untagged)]
pub(crate) enum ServerReply {
    JobAssigned(JobAssignedPayload),
    NoWork(NoWorkPayload),
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(crate) enum WorkStatus {
    NoWork,
    JobAssigned,
}

impl WorkStatus {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Self::NoWork => "no_work",
            Self::JobAssigned => "job_assigned",
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct NoWorkPayload {
    pub(crate) status: WorkStatus,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct JobAssignedPayload {
    pub(crate) status: WorkStatus,
    pub(crate) job_id: String,
    pub(crate) video_id: u64,
    pub(crate) source_name: String,
    pub(crate) size_bytes: u64,
    pub(crate) chunk_size_bytes: u64,
    pub(crate) target_vmaf: f32,
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub(crate) struct JobResultPayload {
    pub(crate) job_id: String,
    pub(crate) video_id: u64,
    pub(crate) source_name: String,
    pub(crate) crf: f32,
    pub(crate) vmaf_score: Option<f32>,
    pub(crate) xpsnr_score: Option<f32>,
    pub(crate) predicted_encode_size: u64,
    pub(crate) encode_percent: f64,
    pub(crate) predicted_encode_time_secs: f64,
    pub(crate) from_cache: bool,
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
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ChunkTransferPayload {
    pub(crate) bytes_sent: u64,
    pub(crate) chunk_index: u64,
    pub(crate) crc32: u64,
    pub(crate) data: String,
    pub(crate) status: String,
    pub(crate) total_bytes: u64,
    pub(crate) total_chunks: u64,
    pub(crate) transfer_id: String,
    pub(crate) video_id: u64,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TransferProgressPayload {
    pub(crate) job_id: String,
    pub(crate) received_bytes: u64,
    pub(crate) expected_bytes: Option<u64>,
}

#[allow(dead_code)]
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct TransferCompletePayload {
    pub(crate) job_id: String,
    pub(crate) final_path: String,
    pub(crate) final_size_bytes: u64,
    pub(crate) final_digest: String,
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
                protocol_version: 1,
                version: "0.11.4".into(),
                capabilities: Capabilities { crf_search: true },
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
                    "capabilities": { "crf_search": true }
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
                    "target_vmaf": 96.5
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
                    job_id: "job-123".into(),
                    video_id: 123,
                    source_name: "movie.mkv".into(),
                    size_bytes: 1024,
                    chunk_size_bytes: 256,
                    target_vmaf: 96.5,
                })),
            )
        );
    }

    #[test]
    fn job_result_payload_serializes_structured_result_summary() {
        let payload = JobResultPayload {
            job_id: "job-123".into(),
            video_id: 123,
            source_name: "movie.mkv".into(),
            crf: 31.5,
            vmaf_score: Some(96.2),
            xpsnr_score: None,
            predicted_encode_size: 123_456,
            encode_percent: 42.5,
            predicted_encode_time_secs: 87.5,
            from_cache: false,
        };

        assert_eq!(
            serde_json::to_value(payload).expect("serialize job result"),
            json!({
                "job_id": "job-123",
                "video_id": 123,
                "source_name": "movie.mkv",
                "crf": 31.5,
                "vmaf_score": 96.19999694824219,
                "xpsnr_score": null,
                "predicted_encode_size": 123456,
                "encode_percent": 42.5,
                "predicted_encode_time_secs": 87.5,
                "from_cache": false,
            })
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
    fn chunk_transfer_payload_serializes_metadata_side_channel() {
        let payload = ChunkTransferPayload {
            bytes_sent: 4096,
            chunk_index: 7,
            crc32: 0xdead_beef,
            data: "deadbeef".into(),
            status: "transfer_chunk".into(),
            total_bytes: 9_560_739_312,
            total_chunks: 9_118,
            transfer_id: "job-123".into(),
            video_id: 123,
        };

        assert_eq!(
            serde_json::to_value(payload).expect("serialize chunk transfer"),
            json!({
                "bytes_sent": 4096,
                "chunk_index": 7,
                "crc32": 3735928559u64,
                "data": "deadbeef",
                "status": "transfer_chunk",
                "total_bytes": 9_560_739_312u64,
                "total_chunks": 9_118,
                "transfer_id": "job-123",
                "video_id": 123,
            })
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
