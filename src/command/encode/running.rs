use super::{
    lifecycle::CompletedOutput,
    plan::EncodePlan,
    progress::{BarUpdate, ProgressState, StreamSizes, apply_ffmpeg_event},
    sink::ProgressSink,
    spawner::EncodeSpawner,
};
use crate::{command::SmallDuration, log::ProgressLogger};
use log::info;
use std::time::Instant;
use tokio_stream::StreamExt;

pub struct EncodeRun {
    pub input: std::path::PathBuf,
    pub output: CompletedOutput,
    pub stream_sizes: Option<StreamSizes>,
}

pub async fn run_encode(
    plan: EncodePlan,
    sink: &impl ProgressSink,
    spawner: &impl EncodeSpawner,
) -> anyhow::Result<EncodeRun> {
    run_encode_with_progress(plan, sink, spawner, |_fps, _time, _output| {}).await
}

pub async fn run_encode_with_progress<F>(
    plan: EncodePlan,
    sink: &impl ProgressSink,
    spawner: &impl EncodeSpawner,
    mut on_progress: F,
) -> anyhow::Result<EncodeRun>
where
    F: FnMut(f32, std::time::Duration, &std::path::Path),
{
    let (partial, session) = plan.begin();
    let input = session.input.clone();
    let output = partial.path().to_path_buf();

    info!(
        "encoding {}",
        output.file_name().and_then(|n| n.to_str()).unwrap_or("")
    );

    let enc_args = session.ffmpeg_args()?;
    let mut enc = spawner.spawn(&session, enc_args, &partial)?;

    let mut logger = ProgressLogger::new(module_path!(), Instant::now());
    let mut progress = ProgressState::default();
    while let Some(event) = enc.next().await {
        let event = event?;
        if let Some(BarUpdate::Fps { fps, time }) = apply_ffmpeg_event(&mut progress, event) {
            sink.set_message(format!("{fps} fps, "));
            if let Ok(d) = &session.probe.duration {
                sink.set_position(time.as_micros_u64());
                logger.update(*d, time, fps);
            }
            on_progress(fps, time, &output);
        }
    }
    enc.wait().await?;
    sink.finish();

    spawner.finalize_output(&output).await?;

    Ok(EncodeRun {
        input,
        output: partial.commit(),
        stream_sizes: progress.stream_sizes,
    })
}
