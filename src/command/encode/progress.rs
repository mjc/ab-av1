use crate::process::FfmpegOut;
use std::time::Duration;

/// Per-stream byte sizes reported by ffmpeg at end of encode.
#[derive(Debug, Clone, Copy)]
pub struct StreamSizes {
    pub video: u64,
    pub audio: u64,
    pub subtitle: u64,
    pub other: u64,
}

/// Progress bar updates derived from ffmpeg stderr events.
pub enum BarUpdate {
    Fps { fps: f32, time: Duration },
}

/// Mutable encode progress state updated by [`apply_ffmpeg_event`].
#[derive(Debug, Default)]
pub struct ProgressState {
    pub stream_sizes: Option<StreamSizes>,
}

pub fn apply_ffmpeg_event(state: &mut ProgressState, event: FfmpegOut) -> Option<BarUpdate> {
    match event {
        FfmpegOut::Progress { fps, time, .. } if fps > 0.0 => Some(BarUpdate::Fps { fps, time }),
        FfmpegOut::StreamSizes {
            video,
            audio,
            subtitle,
            other,
        } => {
            state.stream_sizes = Some(StreamSizes {
                video,
                audio,
                subtitle,
                other,
            });
            None
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_ffmpeg_event_records_stream_sizes() {
        let mut state = ProgressState::default();
        let update = apply_ffmpeg_event(
            &mut state,
            FfmpegOut::StreamSizes {
                video: 100,
                audio: 20,
                subtitle: 0,
                other: 0,
            },
        );
        assert!(update.is_none());
        let sizes = state.stream_sizes.expect("stream sizes");
        assert_eq!(sizes.video, 100);
        assert_eq!(sizes.audio, 20);
    }

    #[test]
    fn apply_ffmpeg_event_returns_fps_update() {
        let mut state = ProgressState::default();
        let update = apply_ffmpeg_event(
            &mut state,
            FfmpegOut::Progress {
                frame: 1,
                fps: 24.0,
                time: Duration::from_secs(1),
            },
        );
        match update {
            Some(BarUpdate::Fps { fps, time }) => {
                assert_eq!(fps, 24.0);
                assert_eq!(time, Duration::from_secs(1));
            }
            None => panic!("expected fps update"),
        }
    }

    #[test]
    fn apply_ffmpeg_event_ignores_zero_fps() {
        let mut state = ProgressState::default();
        let update = apply_ffmpeg_event(
            &mut state,
            FfmpegOut::Progress {
                frame: 0,
                fps: 0.0,
                time: Duration::ZERO,
            },
        );
        assert!(update.is_none());
    }
}
