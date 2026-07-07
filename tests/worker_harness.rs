#![allow(unused_crate_dependencies)]

use anyhow::Result;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use std::process::Stdio;
use tokio::{net::TcpListener, process::Command};
use tokio_tungstenite::{accept_async, tungstenite::Message};

#[tokio::test(flavor = "current_thread")]
async fn worker_binary_handles_job_assignment() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;

    let server = tokio::spawn(async move {
        let (stream, _) = listener.accept().await.expect("accept connection");
        let socket = accept_async(stream).await.expect("accept websocket");
        let (mut writer, mut reader) = socket.split();

        expect_text_message(
            reader
                .next()
                .await
                .expect("join frame")
                .expect("join message"),
            json!(["1", "1", "workers:crf_search", "phx_join", {}]),
        );
        send_text_message(
            &mut writer,
            json!([null, "1", "workers:crf_search", "phx_reply", {
                "status": "ok",
                "response": {"worker_id": "worker-123"}
            }]),
        )
        .await;

        expect_text_message(
            reader
                .next()
                .await
                .expect("announce frame")
                .expect("announce message"),
            json!(["1", "2", "workers:crf_search", "announce", {
                "worker_id": "abav1-dev",
                "protocol_version": 1,
                "version": "0.11.4",
                "capabilities": {"crf_search": true}
            }]),
        );
        send_text_message(
            &mut writer,
            json!([null, "2", "workers:crf_search", "phx_reply", {
                "status": "ok",
                "response": {"accepted": true, "protocol_version": 1}
            }]),
        )
        .await;

        expect_text_message(
            reader
                .next()
                .await
                .expect("pull_work frame")
                .expect("pull_work message"),
            json!(["1", "3", "workers:crf_search", "pull_work", {}]),
        );
        send_text_message(
            &mut writer,
            json!([null, "3", "workers:crf_search", "phx_reply", {
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
            }]),
        )
        .await;
    });

    let output = Command::new(env!("CARGO_BIN_EXE_ab-av1"))
        .args([
            "worker",
            "--connect",
            &format!("http://{address}"),
            "--worker-id",
            "abav1-dev",
            "--once",
        ])
        .env("REENCODARR_WORKER_TOKEN", "test-worker-token")
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .output()
        .await?;

    assert!(output.status.success(), "worker failed: {:?}", output);
    let stdout = String::from_utf8_lossy(&output.stdout);
    assert!(
        stdout.contains("received job_assigned (job_id=job-123)"),
        "unexpected worker output: {stdout}"
    );

    server.await.expect("server task");
    Ok(())
}

async fn send_text_message<W>(writer: &mut W, value: Value)
where
    W: SinkExt<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    writer
        .send(Message::Text(value.to_string()))
        .await
        .expect("send websocket message");
}

fn expect_text_message(message: Message, expected: Value) {
    let Message::Text(text) = message else {
        panic!("expected text frame, got {message:?}");
    };
    let actual: Value = serde_json::from_str(&text).expect("decode websocket message");
    assert_eq!(actual, expected);
}
