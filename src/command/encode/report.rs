use super::{lifecycle::CompletedOutput, progress::StreamSizes};
use crate::console_ext::style;
use console::style;
use indicatif::HumanBytes;
use std::{io::Write, path::Path, path::PathBuf};
use tokio::fs;

/// Pure encoded output size metrics.
pub struct EncodeMetrics {
    pub output_bytes: u64,
    pub input_bytes: u64,
    pub percent: f64,
    pub stream_sizes: Option<StreamSizes>,
}

impl EncodeMetrics {
    pub fn percent_of_input(output_bytes: u64, input_bytes: u64) -> f64 {
        if input_bytes == 0 {
            0.0
        } else {
            100.0 * output_bytes as f64 / input_bytes as f64
        }
    }

    pub fn from_bytes(
        output_bytes: u64,
        input_bytes: u64,
        stream_sizes: Option<StreamSizes>,
    ) -> Self {
        Self {
            output_bytes,
            input_bytes,
            percent: Self::percent_of_input(output_bytes, input_bytes),
            stream_sizes,
        }
    }

    pub async fn load(
        input: &Path,
        output: &CompletedOutput,
        stream_sizes: Option<StreamSizes>,
    ) -> anyhow::Result<Self> {
        let output_bytes = fs::metadata(output.path()).await?.len();
        let input_bytes = fs::metadata(input).await?.len();
        Ok(Self::from_bytes(output_bytes, input_bytes, stream_sizes))
    }
}

/// Successful encode with computed summary metrics.
pub struct FinishedEncode {
    pub input: PathBuf,
    pub output: CompletedOutput,
    pub metrics: EncodeMetrics,
}

impl FinishedEncode {
    pub async fn load(
        input: PathBuf,
        output: CompletedOutput,
        stream_sizes: Option<StreamSizes>,
    ) -> anyhow::Result<Self> {
        let metrics = EncodeMetrics::load(&input, &output, stream_sizes).await?;
        Ok(Self {
            input,
            output,
            metrics,
        })
    }

    pub fn render_summary(&self, out: &mut impl Write) -> std::io::Result<()> {
        render_encode_summary(&self.metrics, out)
    }
}

pub fn render_encode_summary(metrics: &EncodeMetrics, out: &mut impl Write) -> std::io::Result<()> {
    let output_size = style(HumanBytes(metrics.output_bytes)).dim().bold();
    let output_percent = style!("{}%", metrics.percent.round()).dim().bold();
    write!(
        out,
        "{} {output_size} {}{output_percent}",
        style("Encoded").dim(),
        style("(").dim(),
    )?;
    if let Some(stream_sizes) = &metrics.stream_sizes {
        let has_non_video =
            stream_sizes.audio > 0 || stream_sizes.subtitle > 0 || stream_sizes.other > 0;
        if has_non_video {
            for (label, size) in [
                ("video:", stream_sizes.video),
                ("audio:", stream_sizes.audio),
                ("subs:", stream_sizes.subtitle),
                ("other:", stream_sizes.other),
            ] {
                if size > 0 {
                    let size = style(HumanBytes(size)).dim();
                    write!(out, "{} {}{size}", style(",").dim(), style(label).dim(),)?;
                }
            }
        }
    }
    writeln!(out, "{}", style(")").dim())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn percent_of_input_is_correct() {
        let metrics = EncodeMetrics::from_bytes(50, 200, None);
        assert_eq!(metrics.percent, 25.0);
        assert_eq!(EncodeMetrics::percent_of_input(0, 0), 0.0);
    }

    #[test]
    fn render_encode_summary_includes_stream_breakdown() {
        let metrics = EncodeMetrics::from_bytes(
            100,
            400,
            Some(StreamSizes {
                video: 80,
                audio: 0,
                subtitle: 5,
                other: 0,
            }),
        );
        let mut buf = Vec::new();
        render_encode_summary(&metrics, &mut buf).expect("render");
        let text = String::from_utf8(buf).expect("utf8");
        assert!(text.contains("Encoded"));
        assert!(text.contains("subs:"));
    }
}
