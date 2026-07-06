use super::{default_output_name, error::EncodePlanError, lifecycle::PlannedOutput};
use crate::{
    command::args,
    ffprobe::Ffprobe,
};
use same_file::is_same_file;
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

/// Resolved output path and whether the CLI omitted `--output`.
#[derive(Debug)]
pub struct ResolvedOutput {
    pub planned: PlannedOutput,
    pub defaulting_output: bool,
}

/// Audio options derived from CLI flags and probe metadata.
#[derive(Debug)]
pub struct AudioConfig {
    pub has_audio: bool,
    pub stereo_downmix: bool,
    pub audio_codec: Option<Arc<str>>,
}

pub fn resolve_output(
    input: &Path,
    encoder: &args::Encoder,
    encode_to: &args::EncodeToOutput,
    probe: &Ffprobe,
) -> Result<ResolvedOutput, EncodePlanError> {
    let defaulting_output = encode_to.output.is_none();
    let output_path = encode_to.output.clone().unwrap_or_else(|| {
        default_output_name(input, encoder, probe.is_image)
    });

    if !encode_to.overwrite_input
        && is_same_file(&output_path, input).unwrap_or(false)
    {
        return Err(EncodePlanError::SameInputOutput);
    }

    Ok(ResolvedOutput {
        planned: PlannedOutput::new(output_path),
        defaulting_output,
    })
}

pub fn audio_config(
    encode_to: &args::EncodeToOutput,
    probe: &Ffprobe,
) -> Result<AudioConfig, EncodePlanError> {
    if encode_to.downmix_to_stereo && encode_to.audio_codec.as_deref() == Some("copy") {
        return Err(EncodePlanError::StereoDownmixWithCopy);
    }
    let stereo_downmix =
        encode_to.downmix_to_stereo && probe.max_audio_channels.is_some_and(|c| c > 3);
    let audio_codec = encode_to.audio_codec.clone().map(Into::into);
    Ok(AudioConfig {
        has_audio: probe.has_audio,
        stereo_downmix,
        audio_codec,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::args::{EncodeToOutput, Encoder};
    use std::{env, fs, time::Duration};

    fn probe(channels: Option<i64>) -> Ffprobe {
        Ffprobe {
            duration: Ok(Duration::from_secs(120)),
            has_audio: channels.is_some(),
            max_audio_channels: channels,
            fps: Ok(24.0),
            resolution: Some((1920, 1080)),
            is_image: false,
            pix_fmt: Some("yuv420p".into()),
        }
    }

    fn temp_input(label: &str) -> PathBuf {
        let path = env::temp_dir().join(format!(
            "ab-av1-preflight-test-{}-{}",
            label,
            std::process::id()
        ));
        fs::write(&path, b"x").expect("write temp input");
        path
    }

    #[test]
    fn resolve_output_rejects_same_input_and_output() {
        let input = temp_input("same-io");
        let encode_to = EncodeToOutput {
            output: Some(input.clone()),
            audio_codec: None,
            downmix_to_stereo: false,
            video_only: false,
            overwrite_input: false,
        };
        let encoder: Encoder = "libsvtav1".parse().unwrap();
        let err = match resolve_output(&input, &encoder, &encode_to, &probe(Some(6))) {
            Err(err) => err,
            Ok(_) => panic!("expected same-file error"),
        };
        assert_eq!(err, EncodePlanError::SameInputOutput);
        let _ = fs::remove_file(input);
    }

    #[test]
    fn audio_config_rejects_stereo_downmix_with_copy() {
        let encode_to = EncodeToOutput {
            output: None,
            audio_codec: Some("copy".into()),
            downmix_to_stereo: true,
            video_only: false,
            overwrite_input: false,
        };
        let err = match audio_config(&encode_to, &probe(Some(6))) {
            Err(err) => err,
            Ok(_) => panic!("expected error"),
        };
        assert_eq!(err, EncodePlanError::StereoDownmixWithCopy);
    }
}
