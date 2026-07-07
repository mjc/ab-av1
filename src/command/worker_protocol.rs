use serde::{Deserialize, Serialize};

pub(crate) const CRF_SEARCH_TOPIC: &str = "workers:crf_search";

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub(crate) struct ClientFrame(
    String,
    String,
    String,
    String,
    ClientPayload,
);

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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct JobAssignedPayload {
    pub(crate) status: WorkStatus,
    pub(crate) job_id: String,
    pub(crate) source_name: String,
    pub(crate) size_bytes: u64,
    pub(crate) chunk_size_bytes: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub(crate) struct ErrorReplyPayload {
    pub(crate) reason: String,
    #[serde(default)]
    pub(crate) supported_protocol_versions: Vec<u64>,
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
                    "source_name": "movie.mkv",
                    "size_bytes": 1024,
                    "chunk_size_bytes": 256
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
                    source_name: "movie.mkv".into(),
                    size_bytes: 1024,
                    chunk_size_bytes: 256,
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
}
