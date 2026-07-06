use super::{lifecycle::PartialOutput, plan::EncodeSession};
use crate::{
    ffmpeg::{self, FfmpegEncodeArgs},
    process::FfmpegOutStream,
};
use std::path::Path;

pub trait EncodeSpawner {
    fn spawn(
        &self,
        session: &EncodeSession,
        enc_args: FfmpegEncodeArgs<'_>,
        output: &PartialOutput,
    ) -> anyhow::Result<FfmpegOutStream>;

    async fn finalize_output(&self, output: &Path) -> anyhow::Result<()> {
        let _ = output;
        Ok(())
    }
}

pub struct FfmpegSpawner;

impl EncodeSpawner for FfmpegSpawner {
    fn spawn(
        &self,
        session: &EncodeSession,
        enc_args: FfmpegEncodeArgs<'_>,
        output: &PartialOutput,
    ) -> anyhow::Result<FfmpegOutStream> {
        ffmpeg::encode(
            enc_args,
            output,
            session.has_audio(),
            session.audio_codec(),
            session.stereo_downmix(),
        )
    }
}

#[cfg(test)]
pub struct FixtureSpawner {
    fixture: &'static str,
}

#[cfg(test)]
impl FixtureSpawner {
    pub fn new(fixture: &'static str) -> Self {
        Self { fixture }
    }
}

#[cfg(test)]
impl EncodeSpawner for FixtureSpawner {
    fn spawn(
        &self,
        _session: &EncodeSession,
        _enc_args: FfmpegEncodeArgs<'_>,
        _output: &PartialOutput,
    ) -> anyhow::Result<FfmpegOutStream> {
        super::test_support::test_ffmpeg_stream(self.fixture)
    }

    async fn finalize_output(&self, output: &Path) -> anyhow::Result<()> {
        if !output.exists() {
            tokio::fs::write(output, b"fixture-encoded").await?;
        }
        Ok(())
    }
}

/// Honors thread-local encode fixtures for higher-level integration tests.
#[cfg(test)]
pub struct ThreadLocalFixtureSpawner;

#[cfg(test)]
impl EncodeSpawner for ThreadLocalFixtureSpawner {
    fn spawn(
        &self,
        session: &EncodeSession,
        enc_args: FfmpegEncodeArgs<'_>,
        output: &PartialOutput,
    ) -> anyhow::Result<FfmpegOutStream> {
        if let Some(fixture) = super::test_support::test_hooks::fixture() {
            return super::test_support::test_ffmpeg_stream(fixture);
        }
        FfmpegSpawner.spawn(session, enc_args, output)
    }

    async fn finalize_output(&self, output: &Path) -> anyhow::Result<()> {
        if super::test_support::test_hooks::fixture().is_some() && !output.exists() {
            tokio::fs::write(output, b"fixture-encoded").await?;
        }
        Ok(())
    }
}
