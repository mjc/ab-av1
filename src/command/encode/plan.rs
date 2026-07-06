use super::{
    Args,
    error::EncodePlanError,
    lifecycle::{PartialOutput, PlannedOutput},
    preflight::{ResolvedOutput, audio_config, resolve_output},
};
use crate::{command::args, ffmpeg::FfmpegEncodeArgs, ffprobe::Ffprobe};
use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

/// Spawn-time ffmpeg and audio configuration derived during plan build.
pub struct SpawnConfig {
    pub has_audio: bool,
    pub stereo_downmix: bool,
    pub audio_codec: Option<Arc<str>>,
    pub video_only: bool,
    encode: args::Encode,
    crf: f32,
}

/// Validated encode inputs lowered from the raw clap surface.
pub(crate) struct EncodeConfig {
    encode: args::Encode,
    crf: f32,
    encode_to: args::EncodeToOutput,
}

impl From<Args> for EncodeConfig {
    fn from(args: Args) -> Self {
        let Args {
            args: encode,
            crf,
            encode: encode_to,
        } = args;

        Self {
            encode,
            crf: crf.get(),
            encode_to,
        }
    }
}

/// Preflight encode configuration: validation and ffmpeg args before any spawn or cleanup.
pub struct EncodePlan {
    input: PathBuf,
    planned: PlannedOutput,
    defaulting_output: bool,
    probe: Arc<Ffprobe>,
    spawn: SpawnConfig,
}

impl EncodePlan {
    pub fn build(config: EncodeConfig, probe: Arc<Ffprobe>) -> Result<Self, EncodePlanError> {
        let EncodeConfig {
            encode,
            crf,
            encode_to,
        } = config;

        let ResolvedOutput {
            planned,
            defaulting_output,
        } = resolve_output(&encode.input, &encode.encoder, &encode_to, &probe)?;
        let audio = audio_config(&encode_to, &probe)?;

        // Validate ffmpeg arg construction during preflight.
        let mut enc_args = encode.to_ffmpeg_args(crf, &probe)?;
        enc_args.video_only = encode_to.video_only;
        drop(enc_args);

        Ok(Self {
            input: encode.input.clone(),
            planned,
            defaulting_output,
            probe,
            spawn: SpawnConfig {
                has_audio: audio.has_audio,
                stereo_downmix: audio.stereo_downmix,
                audio_codec: audio.audio_codec,
                encode,
                crf,
                video_only: encode_to.video_only,
            },
        })
    }

    pub fn probe(&self) -> &Ffprobe {
        &self.probe
    }

    pub fn defaulting_output(&self) -> bool {
        self.defaulting_output
    }

    pub fn output_path(&self) -> &Path {
        self.planned.path()
    }

    #[cfg(test)]
    pub fn spawn_config(&self) -> &SpawnConfig {
        &self.spawn
    }

    pub fn begin(self) -> (PartialOutput, EncodeSession) {
        let partial = self.planned.begin();
        let session = EncodeSession {
            input: self.input,
            probe: self.probe,
            spawn: self.spawn,
        };
        (partial, session)
    }
}

/// Remaining plan state after output cleanup is armed at the spawn boundary.
pub struct EncodeSession {
    pub input: PathBuf,
    pub probe: Arc<Ffprobe>,
    spawn: SpawnConfig,
}

impl EncodeSession {
    pub fn ffmpeg_args(&self) -> Result<FfmpegEncodeArgs<'_>, EncodePlanError> {
        let mut enc_args = self
            .spawn
            .encode
            .to_ffmpeg_args(self.spawn.crf, &self.probe)?;
        enc_args.video_only = self.spawn.video_only;
        Ok(enc_args)
    }

    pub fn has_audio(&self) -> bool {
        self.spawn.has_audio
    }

    pub fn stereo_downmix(&self) -> bool {
        self.spawn.stereo_downmix
    }

    pub fn audio_codec(&self) -> Option<&str> {
        self.spawn.audio_codec.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::encode::test_support::{arc_probe, encode_args, temp_input};
    use std::{env, fs};

    #[test]
    fn encode_config_from_args_does_not_allocate() {
        let args = encode_args(
            PathBuf::from("input.mkv"),
            Some(PathBuf::from("output.mkv")),
        );

        crate::test_support::assert_no_allocations(|| {
            std::hint::black_box(EncodeConfig::from(args));
        });
    }

    #[test]
    fn build_rejects_same_input_and_output() {
        let input = temp_input("plan", "same-io");
        let err = match EncodePlan::build(
            encode_args(input.clone(), Some(input.clone())).into(),
            arc_probe(Some(6)),
        ) {
            Err(err) => err,
            Ok(_) => panic!("expected same-file error"),
        };
        assert_eq!(err, EncodePlanError::SameInputOutput);
        let _ = fs::remove_file(input);
    }

    #[test]
    fn build_rejects_stereo_downmix_with_copy_codec() {
        let input = temp_input("plan", "downmix-copy");
        let output = env::temp_dir().join(format!("ab-av1-encode-plan-out-{}", std::process::id()));
        let mut args = encode_args(input.clone(), Some(output));
        args.encode.downmix_to_stereo = true;
        args.encode.audio_codec = Some("copy".into());

        let err = match EncodePlan::build(args.into(), arc_probe(Some(6))) {
            Err(err) => err,
            Ok(_) => panic!("expected downmix/copy error"),
        };
        assert_eq!(err, EncodePlanError::StereoDownmixWithCopy);
        let _ = fs::remove_file(input);
    }

    #[test]
    fn build_default_output_name_for_mkv_input() {
        let input = temp_input("plan", "default-out");
        let mut args = encode_args(input.clone(), None);
        args.args.input = PathBuf::from("movie.mkv");
        let plan = EncodePlan::build(args.into(), arc_probe(Some(6))).expect("plan build");
        assert!(plan.defaulting_output());
        assert_eq!(plan.output_path(), Path::new("movie.av1.mkv"));
        let _ = fs::remove_file(input);
    }

    #[test]
    fn build_carries_video_only_and_stereo_downmix_decisions() {
        let input = temp_input("plan", "flags");
        let output =
            env::temp_dir().join(format!("ab-av1-encode-plan-flags-{}", std::process::id()));
        let mut args = encode_args(input.clone(), Some(output));
        args.encode.video_only = true;
        args.encode.downmix_to_stereo = true;

        let plan = EncodePlan::build(args.into(), arc_probe(Some(6))).expect("plan build");
        assert!(plan.spawn_config().video_only);
        assert!(plan.spawn_config().stereo_downmix);
        assert!(plan.spawn_config().has_audio);
        let (_partial, session) = plan.begin();
        assert!(session.ffmpeg_args().expect("ffmpeg args").video_only);
        let _ = fs::remove_file(input);
    }

    #[test]
    fn build_skips_stereo_downmix_when_channels_at_most_three() {
        let input = temp_input("plan", "stereo-skip");
        let output = env::temp_dir().join(format!(
            "ab-av1-encode-plan-stereo-skip-{}",
            std::process::id()
        ));
        let mut args = encode_args(input.clone(), Some(output));
        args.encode.downmix_to_stereo = true;

        let plan = EncodePlan::build(args.into(), arc_probe(Some(2))).expect("plan build");
        assert!(!plan.spawn_config().stereo_downmix);
        let _ = fs::remove_file(input);
    }
}
