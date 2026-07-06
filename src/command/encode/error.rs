use thiserror::Error;

#[derive(Debug, Error)]
pub enum EncodePlanError {
    #[error(
        "Input and Output are specified as the same file. Not proceeding. \
         Pass in `--overwrite-input` to allow this."
    )]
    SameInputOutput,

    #[error("--stereo-downmix cannot be used with --acodec copy")]
    StereoDownmixWithCopy,

    #[error(transparent)]
    FfmpegArgs(#[from] anyhow::Error),
}

impl PartialEq for EncodePlanError {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Self::SameInputOutput, Self::SameInputOutput)
            | (Self::StereoDownmixWithCopy, Self::StereoDownmixWithCopy) => true,
            (Self::FfmpegArgs(a), Self::FfmpegArgs(b)) => a.to_string() == b.to_string(),
            _ => false,
        }
    }
}

impl Eq for EncodePlanError {}

impl EncodePlanError {
    pub fn into_anyhow(self) -> anyhow::Error {
        anyhow::Error::from(self)
    }
}
