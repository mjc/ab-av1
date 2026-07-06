//! xpsnr logic
use crate::process::{Chunks, CommandExt, FfmpegOut, managed::ManagedProcess};
use crate::score_stream::{
    ParsedScore, Score, ScoreError, ScoreStreamParse, build_score_ffmpeg_command,
    parse_buffered_score, run_score_stream,
};
use anyhow::Context;
use log::{debug, info};
use std::path::Path;
use std::time::Duration;
use tokio::process::Command;
use tokio_stream::Stream;

/// Build ffmpeg command for XPSNR scoring (testable without spawning).
pub(crate) fn build_ffmpeg_command(
    reference: &Path,
    distorted: &Path,
    filter_complex: &str,
    fps: Option<f32>,
) -> Command {
    build_score_ffmpeg_command(reference, distorted, filter_complex, fps)
}

/// Calculate XPSNR score using ffmpeg.
pub fn run(
    reference: &Path,
    distorted: &Path,
    filter_complex: &str,
    fps: Option<f32>,
) -> anyhow::Result<impl Stream<Item = XpsnrOut> + use<>> {
    info!(
        "xpsnr {} vs reference {}",
        distorted.file_name().and_then(|n| n.to_str()).unwrap_or(""),
        reference.file_name().and_then(|n| n.to_str()).unwrap_or(""),
    );

    let cmd = build_ffmpeg_command(reference, distorted, filter_complex, fps);
    let cmd_str = cmd.to_cmd_str();
    debug!("cmd `{cmd_str}`");
    let xpsnr = ManagedProcess::spawn("ffmpeg xpsnr", cmd).context("ffmpeg xpsnr")?;
    Ok(stream_process(xpsnr, cmd_str))
}

fn stream_process(process: ManagedProcess, cmd_str: String) -> impl Stream<Item = XpsnrOut> {
    run_score_stream(
        process,
        "ffmpeg xpsnr",
        cmd_str,
        XpsnrOut::try_parse_chunk,
        XpsnrOut::from_parse,
        XpsnrOut::Err,
    )
}

pub(crate) fn progress_time(
    time: Duration,
    source_fps: f64,
    override_fps: Option<f32>,
) -> Duration {
    match (source_fps.is_finite() && source_fps > 0.0, override_fps) {
        (true, Some(override_fps)) => time.mul_f64(override_fps as f64 / source_fps),
        _ => time,
    }
}

#[derive(Debug)]
pub enum XpsnrOut {
    Progress(FfmpegOut),
    Done(f32),
    Err(anyhow::Error),
}

impl XpsnrOut {
    fn from_parse(event: ScoreStreamParse) -> Self {
        match event {
            ScoreStreamParse::Progress(progress) => Self::Progress(progress),
            ScoreStreamParse::LogicalDone(score) => Self::Done(score.get()),
        }
    }

    fn try_parse_chunk(chunk: &[u8], chunks: &mut Chunks) -> Option<ScoreStreamParse> {
        match try_parse_xpsnr_score_chunk(chunk, chunks) {
            Ok(event) => event,
            Err(err) => unreachable!("pure XPSNR parser returned error event: {err}"),
        }
    }

    #[cfg(test)]
    fn try_from_chunk(chunk: &[u8], chunks: &mut Chunks) -> Option<Self> {
        match try_parse_xpsnr_score_chunk(chunk, chunks) {
            Ok(Some(event)) => Some(Self::from_parse(event)),
            Ok(None) => None,
            Err(err) => Some(Self::Err(err.into())),
        }
    }
}

fn try_parse_xpsnr_score_chunk(
    chunk: &[u8],
    chunks: &mut Chunks,
) -> Result<Option<ScoreStreamParse>, crate::process::ChunkLineError> {
    chunks.push(chunk);

    let score = match parse_buffered_score(chunks, score_from_minimum_line)? {
        Some(score) => Some(score),
        None => parse_buffered_score(chunks, score_from_average_line)?,
    };
    if let Some(score) = score {
        return Ok(Some(ScoreStreamParse::LogicalDone(score)));
    }
    Ok(FfmpegOut::try_parse(chunks.last_line()).map(ScoreStreamParse::Progress))
}

#[cfg(test)]
fn score_from_line(line: &str) -> Option<f32> {
    match parse_xpsnr_score_line(line) {
        ParsedScore::Score(score) => Some(score.get()),
        ParsedScore::Miss | ParsedScore::Invalid(_) => None,
    }
}

#[cfg(test)]
pub(crate) fn parse_xpsnr_score_line(line: &str) -> ParsedScore {
    match score_from_minimum_line(line) {
        ParsedScore::Miss => score_from_average_line(line),
        parsed => parsed,
    }
}

fn score_from_minimum_line(line: &str) -> ParsedScore {
    const MIN_PREFIX: &str = "minimum: ";

    if !line.contains("XPSNR") {
        return ParsedScore::Miss;
    }

    let Some(tail) = line
        .find(MIN_PREFIX)
        .map(|yidx| &line[yidx + MIN_PREFIX.len()..])
    else {
        return ParsedScore::Miss;
    };
    parse_score_number(tail)
}

fn score_from_average_line(line: &str) -> ParsedScore {
    if !line.contains("XPSNR average") {
        return ParsedScore::Miss;
    }
    let Some(yidx) = line.find("y:") else {
        return ParsedScore::Miss;
    };
    parse_score_number(line[yidx + 2..].trim_start())
}

fn parse_score_number(s: &str) -> ParsedScore {
    if s.starts_with("inf") {
        return ParsedScore::Score(Score::new(f32::INFINITY));
    }
    if s.starts_with("NaN") || s.starts_with("nan") {
        return ParsedScore::Invalid(ScoreError::Nan);
    }

    let Some(end_idx) = s
        .char_indices()
        .take_while(|(_, c)| *c == '-' || *c == '.' || c.is_numeric())
        .last()
        .map(|(idx, _)| idx)
    else {
        return ParsedScore::Miss;
    };
    s[..=end_idx].parse().map_or(ParsedScore::Miss, |score| {
        ParsedScore::from_score(Score::try_new(score))
    })
}

#[cfg(test)]
mod test {
    use super::*;
    use crate::process::CommandExt;
    use std::env;
    use std::path::Path;
    use std::pin::Pin;
    use tokio::process::Command;
    use tokio_stream::StreamExt;

    fn assert_suppresses_non_video_streams(cmd: &Command) {
        // setup
        let cmd_str = cmd.to_cmd_str();
        // execute — n/a (command already built)
        // assert
        for flag in ["-an", "-sn", "-dn"] {
            assert!(
                cmd_str.split_whitespace().any(|arg| arg == flag),
                "expected {flag} in `{cmd_str}`"
            );
        }
    }

    #[test]
    fn xpsnr_command_suppresses_non_video_streams() {
        // setup
        let cmd = build_ffmpeg_command(
            Path::new("ref.mkv"),
            Path::new("dist.mkv"),
            "[0:v][1:v]xpsnr",
            Some(25.0),
        );
        // execute — n/a
        // assert
        assert_suppresses_non_video_streams(&cmd);
    }

    #[test]
    fn xpsnr_build_command_distorted_input_is_stream_zero() {
        let cmd = build_ffmpeg_command(
            Path::new("ref.mkv"),
            Path::new("dist.mkv"),
            "[dis][ref]xpsnr",
            Some(25.0),
        );
        let cmd_str = cmd.to_cmd_str();
        let dist_pos = cmd_str.find("dist.mkv").expect("distorted path");
        let ref_pos = cmd_str.find("ref.mkv").expect("reference path");
        assert!(
            dist_pos < ref_pos,
            "distorted input must precede reference for [0:v] mapping: `{cmd_str}`"
        );
        assert!(cmd_str.contains("-r 25"));
    }

    #[test]
    fn xpsnr_build_command_omits_fps_override_when_none() {
        let cmd = build_ffmpeg_command(
            Path::new("ref.mkv"),
            Path::new("dist.mkv"),
            "[ref][dis]xpsnr",
            None,
        );
        let cmd_str = cmd.to_cmd_str();
        assert!(
            !cmd_str.split_whitespace().any(|arg| arg == "-r"),
            "native frame rate must omit -r: `{cmd_str}`"
        );
    }

    #[test]
    fn progress_time_without_fps_override_uses_ffmpeg_time() {
        assert_eq!(
            progress_time(Duration::from_secs(10), 30.0, None),
            Duration::from_secs(10)
        );
    }

    #[test]
    fn progress_time_scales_override_time_to_source_time() {
        assert_eq!(
            progress_time(Duration::from_secs(10), 30.0, Some(60.0)),
            Duration::from_secs(20)
        );
    }

    // bug-hunt-red: per-frame XPSNR progress lines must not emit Done early
    #[test]
    fn xpsnr_per_frame_lines_do_not_emit_done() {
        let mut chunks = Chunks::default();
        let line = b"n:   12  XPSNR y: 41.0726  XPSNR u: 39.7731  XPSNR v: 42.5210\n";
        assert!(
            XpsnrOut::try_from_chunk(line, &mut chunks).is_none(),
            "per-frame XPSNR lines must not complete scoring"
        );
    }

    // ab-kgc.95: score_from_line must handle special trailing formats
    #[test]
    fn score_from_line_parses_special_cases() {
        // setup (none)
        // execute / assert
        assert_eq!(
            score_from_line("[Parsed_xpsnr_0 @ 0x1] XPSNR  y: inf  u: inf  v: inf  (minimum: inf)",),
            Some(f32::INFINITY)
        );
        assert_eq!(
            score_from_line("XPSNR average, 1344 frames  y: 40.7139"),
            Some(40.7139)
        );
    }

    #[test]
    fn parse_xpsnr_score_line_returns_typed_minimum_score() {
        assert_eq!(
            parse_xpsnr_score_line(
                "[Parsed_xpsnr_0 @ 0x1] XPSNR  y: 33.6547  u: 41.8741  v: 42.2571  (minimum: 33.6547)",
            ),
            ParsedScore::Score(Score::new(33.6547))
        );
    }

    #[test]
    fn parse_xpsnr_score_line_success_does_not_allocate() {
        crate::test_support::assert_no_allocations(|| {
            let parsed = parse_xpsnr_score_line(
                "[Parsed_xpsnr_0 @ 0x1] XPSNR  y: 33.6547  u: 41.8741  v: 42.2571  (minimum: 33.6547)",
            );
            std::hint::black_box(parsed);
        });
    }

    #[test]
    fn parse_xpsnr_score_line_rejects_nan_minimum() {
        assert_eq!(
            parse_xpsnr_score_line(
                "[Parsed_xpsnr_0 @ 0x1] XPSNR  y: NaN  u: NaN  v: NaN  (minimum: NaN)",
            ),
            ParsedScore::Invalid(ScoreError::Nan)
        );
    }

    #[test]
    fn parse_xpsnr_score_line_misses_malformed_score() {
        assert_eq!(
            parse_xpsnr_score_line("[Parsed_xpsnr_0 @ 0x1] XPSNR  y: nope"),
            ParsedScore::Miss
        );
    }

    #[test]
    fn xpsnr_chunk_parser_reports_malformed_utf8() {
        let mut chunks = Chunks::default();
        let out = XpsnrOut::try_from_chunk(
            b"[Parsed_xpsnr_0 @ 0x1] XPSNR  y: 33.0  (minimum: \xff)\n",
            &mut chunks,
        );

        assert!(matches!(out, Some(XpsnrOut::Err(_))));
    }

    #[test]
    fn score_runners_suppress_non_video_streams_consistently() {
        // setup
        let reference = Path::new("ref.mkv");
        let distorted = Path::new("dist.mkv");
        let filter = "[0:v][1:v]score";
        let fps = Some(25.0);
        let vmaf_cmd = crate::vmaf::build_ffmpeg_command(reference, distorted, filter, fps);
        let xpsnr_cmd = build_ffmpeg_command(reference, distorted, filter, fps);
        // execute — n/a
        // assert
        assert_suppresses_non_video_streams(&vmaf_cmd);
        assert_suppresses_non_video_streams(&xpsnr_cmd);
    }

    const FIXTURE_ENV: &str = "AB_AV1_MANAGED_PROCESS_FIXTURE";
    const FIXTURE_TEST: &str = "process::managed::tests::managed_process_fixture_child";

    fn fixture_command(fixture: &str) -> Command {
        let mut cmd = Command::new(env::current_exe().expect("current test executable"));
        cmd.arg("--exact")
            .arg(FIXTURE_TEST)
            .arg("--nocapture")
            .env(FIXTURE_ENV, fixture);
        cmd
    }

    fn fixture_stream(fixture: &str) -> Pin<Box<dyn Stream<Item = XpsnrOut>>> {
        let process = ManagedProcess::spawn("ffmpeg xpsnr", fixture_command(fixture))
            .expect("spawn XPSNR fixture");
        Box::pin(stream_process(process, format!("fixture {fixture}")))
    }

    #[tokio::test]
    async fn xpsnr_stream_yields_progress_then_logical_done() {
        let mut stream = fixture_stream("xpsnr-progress-score");

        match stream.next().await.expect("progress") {
            XpsnrOut::Progress(FfmpegOut::Progress { frame, fps, time }) => {
                assert_eq!(frame, 12);
                assert_eq!(fps, 24.0);
                assert_eq!(time, std::time::Duration::new(1, 500_000_000));
            }
            other => panic!("expected XPSNR progress, got {other:?}"),
        }
        match stream.next().await.expect("score") {
            XpsnrOut::Done(score) => assert_eq!(score, 33.6547),
            other => panic!("expected XPSNR score, got {other:?}"),
        }
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn xpsnr_stream_reports_missing_logical_done_with_stderr_context() {
        let mut stream = fixture_stream("xpsnr-no-score");
        let mut err = None;
        while let Some(out) = stream.next().await {
            if let XpsnrOut::Err(next) = out {
                err = Some(next.to_string());
            }
        }

        let err = err.expect("missing score error");
        assert!(err.contains("could not parse ffmpeg xpsnr score"));
        assert!(err.contains("fixture xpsnr-no-score"));
        assert!(err.contains("frame="));
    }

    #[tokio::test]
    async fn xpsnr_stream_reports_child_failure_after_logical_done() {
        let mut stream = fixture_stream("xpsnr-score-exit-7");
        assert!(matches!(stream.next().await, Some(XpsnrOut::Done(33.6547))));

        match stream.next().await.expect("failure") {
            XpsnrOut::Err(err) => {
                let err = err.to_string();
                assert!(err.contains("ffmpeg xpsnr exit code 7"));
                assert!(err.contains("xpsnr badness"));
            }
            other => panic!("expected XPSNR process error, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn xpsnr_stream_ignores_stdout_noise() {
        let mut stream = fixture_stream("stdout-noise-xpsnr-progress-score");
        assert!(matches!(
            stream.next().await,
            Some(XpsnrOut::Progress(FfmpegOut::Progress { frame: 3, .. }))
        ));
        assert!(matches!(stream.next().await, Some(XpsnrOut::Done(34.0))));
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn xpsnr_stream_replays_score_emitted_before_subscription() {
        let process =
            ManagedProcess::spawn("ffmpeg xpsnr", fixture_command("xpsnr-progress-score"))
                .expect("spawn XPSNR fixture");
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        let mut stream = Box::pin(stream_process(process, "delayed xpsnr fixture".into()));

        let mut score = None;
        while let Some(out) = stream.next().await {
            if let XpsnrOut::Done(next) = out {
                score = Some(next);
            }
        }

        assert_eq!(score, Some(33.6547));
    }

    #[tokio::test]
    async fn xpsnr_stream_terminates_when_dropped_after_logical_done() {
        let mut stream = fixture_stream("xpsnr-score-then-sleep");
        assert!(matches!(stream.next().await, Some(XpsnrOut::Done(33.6547))));
        drop(stream);
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    #[test]
    fn parse_rgb_line() {
        let score = score_from_line(
            "XPSNR average, 1 frames  r: 40.6130  g: 41.0275  b: 40.6961  (minimum: 40.6130)",
        );
        assert_eq!(score, Some(40.6130));
    }

    #[test]
    fn parse_xpsnr_score() {
        // Note: some lines omitted for brevity
        const FFMPEG_OUT: &str = r#"Input #0, matroska,webm, from 'tmp.mkv':
  Metadata:
    COMPATIBLE_BRANDS: isomiso2avc1mp41
    MAJOR_BRAND     : isom
    MINOR_VERSION   : 512
    ENCODER         : Lavf61.7.100
  Duration: 00:00:53.77, start: -0.007000, bitrate: 2698 kb/s
  Stream #0:0(eng): Video: av1 (libdav1d) (Main), yuv420p10le(tv, progressive), 3840x2160, 25 fps, 25 tbr, 1k tbn (default)
      Metadata:
        HANDLER_NAME    : ?Mainconcept Video Media Handler
        VENDOR_ID       : [0][0][0][0]
        ENCODER         : Lavc61.19.100 libsvtav1
        DURATION        : 00:00:53.760000000
  Stream #0:1(eng): Audio: opus, 48000 Hz, stereo, fltp (default)
      Metadata:
        title           : Opus 96Kbps
        HANDLER_NAME    : #Mainconcept MP4 Sound Media Handler
        VENDOR_ID       : [0][0][0][0]
        ENCODER         : Lavc61.19.100 libopus
        DURATION        : 00:00:53.768000000
Input #1, mov,mp4,m4a,3gp,3g2,mj2, from 'pixabay-lemon-82602.mp4':
  Metadata:
    major_brand     : isom
    minor_version   : 512
    compatible_brands: isomiso2avc1mp41
    encoder         : Lavf58.20.100
  Duration: 00:00:53.76, start: 0.000000, bitrate: 14109 kb/s
  Stream #1:0[0x1](eng): Video: h264 (High) (avc1 / 0x31637661), yuv420p(progressive), 3840x2160, 14101 kb/s, 25 fps, 25 tbr, 12800 tbn (default)
      Metadata:
        handler_name    : ?Mainconcept Video Media Handler
        vendor_id       : [0][0][0][0]
  Stream #1:1[0x2](eng): Audio: aac (LC) (mp4a / 0x6134706D), 48000 Hz, stereo, fltp, 2 kb/s (default)
      Metadata:
        handler_name    : #Mainconcept MP4 Sound Media Handler
        vendor_id       : [0][0][0][0]
Stream mapping:
  Stream #0:0 (libdav1d) -> xpsnr
  Stream #1:0 (h264) -> xpsnr
  xpsnr:default -> Stream #0:0 (wrapped_avframe)
  Stream #0:1 -> #0:1 (opus (native) -> pcm_s16le (native))
Press [q] to stop, [?] for help
[Parsed_xpsnr_0 @ 0x78341c004d00] not matching timebases found between first input: 1/1000 and second input 1/12800, results may be incorrect!
Output #0, null, to 'pipe:':
  Metadata:
    COMPATIBLE_BRANDS: isomiso2avc1mp41
    MAJOR_BRAND     : isom
    MINOR_VERSION   : 512
    encoder         : Lavf61.7.100
  Stream #0:0: Video: wrapped_avframe, yuv420p10le(tv, progressive), 3840x2160 [SAR 1:1 DAR 16:9], q=2-31, 200 kb/s, 25 fps, 25 tbn
      Metadata:
        encoder         : Lavc61.19.100 wrapped_avframe
  Stream #0:1(eng): Audio: pcm_s16le, 48000 Hz, stereo, s16, 1536 kb/s (default)
      Metadata:
        title           : Opus 96Kbps
        HANDLER_NAME    : #Mainconcept MP4 Sound Media Handler
        VENDOR_ID       : [0][0][0][0]
        DURATION        : 00:00:53.768000000
        encoder         : Lavc61.19.100 pcm_s16le
frame=    9 fps=0.0 q=-0.0 size=N/A time=00:00:00.32 bitrate=N/A speed=0.64x    
frame=   28 fps= 28 q=-0.0 size=N/A time=00:00:01.08 bitrate=N/A speed=1.08x    
frame=   46 fps= 31 q=-0.0 size=N/A time=00:00:01.80 bitrate=N/A speed= 1.2x    
frame=   65 fps= 32 q=-0.0 size=N/A time=00:00:02.56 bitrate=N/A speed=1.28x    
n:    1  XPSNR y: 54.5266  XPSNR u: 56.3886  XPSNR v: 58.7794
n:    2  XPSNR y: 40.6035  XPSNR u: 39.3487  XPSNR v: 42.3634
n:    3  XPSNR y: 40.9764  XPSNR u: 38.8791  XPSNR v: 41.8961
n:   64  XPSNR y: 41.0726  XPSNR u: 39.7731  XPSNR v: 42.5210
n:   65  XPSNR y: 41.3476  XPSNR u: 39.6055  XPSNR v: 42.4262
n:   66  XPSNR y: 41.1029  XPSNR u: 39.8779  XPSNR v: 42.6400
frame=   84 fps= 34 q=-0.0 size=N/A time=00:00:03.32 bitrate=N/A speed=1.33x    
frame=  102 fps= 34 q=-0.0 size=N/A time=00:00:04.04 bitrate=N/A speed=1.35x    
frame=  120 fps= 34 q=-0.0 size=N/A time=00:00:04.76 bitrate=N/A speed=1.36x    
n:   67  XPSNR y: 40.9642  XPSNR u: 39.5204  XPSNR v: 42.1316
n:   68  XPSNR y: 40.2677  XPSNR u: 38.9371  XPSNR v: 41.9560
n:   69  XPSNR y: 40.6431  XPSNR u: 38.8864  XPSNR v: 41.6902
n: 1319  XPSNR y: 41.4316  XPSNR u: 40.5146  XPSNR v: 42.1970
n: 1320  XPSNR y: 41.4623  XPSNR u: 40.5527  XPSNR v: 42.3358
n: 1321  XPSNR y: 42.5312  XPSNR u: 41.2487  XPSNR v: 42.8495
frame= 1328 fps= 37 q=-0.0 size=N/A time=00:00:53.08 bitrate=N/A speed=1.47x    
[Parsed_xpsnr_0 @ 0x78341c004d00] XPSNR  y: 40.7139  u: 39.1440  v: 41.7907  (minimum: 39.1440)
[out#0/null @ 0x64006e11b1c0] video:578KiB audio:10080KiB subtitle:0KiB other streams:0KiB global headers:0KiB muxing overhead: unknown
frame= 1344 fps= 37 q=-0.0 Lsize=N/A time=00:00:53.72 bitrate=N/A speed=1.48x    
n: 1342  XPSNR y: 40.6841  XPSNR u: 39.0209  XPSNR v: 40.9250
n: 1343  XPSNR y: 41.0269  XPSNR u: 39.2465  XPSNR v: 41.1238
n: 1344  XPSNR y: 39.8468  XPSNR u: 38.4587  XPSNR v: 40.5844

XPSNR average, 1344 frames  y: 40.7139
"#;

        const CHUNK_SIZE: usize = 64;

        let ffmpeg = FFMPEG_OUT.as_bytes();

        let mut chunks = Chunks::default();
        let mut start_idx = 0;
        let mut xpsnr_score = None;
        while start_idx < ffmpeg.len() {
            let chunk = &ffmpeg[start_idx..(start_idx + CHUNK_SIZE).min(FFMPEG_OUT.len())];
            // println!("* {}", String::from_utf8_lossy(chunk).trim());

            if let Some(xpsnr) = XpsnrOut::try_from_chunk(chunk, &mut chunks) {
                println!("{xpsnr:?}");
                if let XpsnrOut::Done(score) = xpsnr {
                    xpsnr_score = Some(score);
                }
            }

            start_idx += CHUNK_SIZE;
        }

        assert_eq!(xpsnr_score, Some(39.1440), "failed to parse xpsnr score");
    }

    #[test]
    fn parse_xpsnr_negative_score() {
        // Note: some lines omitted for brevity
        const FFMPEG_OUT: &str = r#"ffmpeg version n8.0.1 Copyright (c) 2000-2025 the FFmpeg developers
          built with gcc 15.2.1 (GCC) 20260209
          configuration: --prefix=/usr --disable-debug --disable-static --disable-stripping --enable-amf --enable-avisynth --enable-cuda-llvm --enable-lto --enable-fontconfig --enable-frei0r --enable-gmp --enable-gnutls --enable-gpl --enable-ladspa --enable-libaom --enable-libass --enable-libbluray --enable-libbs2b --enable-libdav1d --enable-libdrm --enable-libdvdnav --enable-libdvdread --enable-libfreetype --enable-libfribidi --enable-libglslang --enable-libgsm --enable-libharfbuzz --enable-libiec61883 --enable-libjack --enable-libjxl --enable-libmodplug --enable-libmp3lame --enable-libopencore_amrnb --enable-libopencore_amrwb --enable-libopenjpeg --enable-libopenmpt --enable-libopus --enable-libplacebo --enable-libpulse --enable-librav1e --enable-librsvg --enable-librubberband --enable-libsnappy --enable-libsoxr --enable-libspeex --enable-libsrt --enable-libssh --enable-libsvtav1 --enable-libtheora --enable-libv4l2 --enable-libvidstab --enable-libvmaf --enable-libvorbis --enable-libvpl --enable-libvpx --enable-libwebp --enable-libx264 --enable-libx265 --enable-libxcb --enable-libxml2 --enable-libxvid --enable-libzimg --enable-libzmq --enable-nvdec --enable-nvenc --enable-opencl --enable-opengl --enable-shared --enable-vapoursynth --enable-version3 --enable-vulkan
          libavutil      60.  8.100 / 60.  8.100
          libavcodec     62. 11.100 / 62. 11.100
          libavformat    62.  3.100 / 62.  3.100
          libavdevice    62.  1.100 / 62.  1.100
          libavfilter    11.  4.100 / 11.  4.100
          libswscale      9.  1.100 /  9.  1.100
          libswresample   6.  1.100 /  6.  1.100
        Input #0, matroska,webm, from '/home/alex/ab-av1-test/.ab-av1-UHW8SqHReeew/vertical.sample20+600f.mkv':
          Metadata:
            ENCODER         : Lavf62.3.100
          Duration: 00:00:20.03, start: 0.000000, bitrate: 94 kb/s
          Stream #0:0: Video: h264 (High), yuv420p(tv, progressive), 720x1280 [SAR 1:1 DAR 9:16], 30 fps, 30 tbr, 1k tbn
            Metadata:
              ENCODER         : Lavc62.11.100 libx264
              DURATION        : 00:00:20.033000000
        Input #1, matroska,webm, from '/home/alex/ab-av1-test/.ab-av1-UHW8SqHReeew/vertical.sample20+600f.av1.crf32.8.mkv':
          Metadata:
            ENCODER         : Lavf62.3.100
          Duration: 00:00:20.03, start: 0.000000, bitrate: 66 kb/s
          Stream #1:0: Video: av1 (libdav1d) (Main), yuv420p10le(tv, progressive), 720x1280, SAR 1:1 DAR 9:16, 30 fps, 30 tbr, 1k tbn
            Metadata:
              ENCODER         : Lavc62.11.100 libsvtav1
              DURATION        : 00:00:20.033000000
        Stream mapping:
          Stream #0:0 (h264) -> format:default
          Stream #1:0 (libdav1d) -> format:default
          xpsnr:default -> Stream #0:0 (wrapped_avframe)
        Press [q] to stop, [?] for help
        Output #0, null, to 'pipe:':
          Metadata:
            encoder         : Lavf62.3.100
          Stream #0:0: Video: wrapped_avframe, yuv420p10le(tv, progressive), 720x1280 [SAR 1:1 DAR 9:16], q=2-31, 200 kb/s, 60 fps, 60 tbn
            Metadata:
              encoder         : Lavc62.11.100 wrapped_avframe
        [Parsed_xpsnr_2 @ 0x7fa708005280] XPSNR  y: -3.2830  u: -2.8081  v: -3.2703  (minimum: -3.2830)
        [out#0/null @ 0x5597832befc0] video:244KiB audio:0KiB subtitle:0KiB other streams:0KiB global headers:0KiB muxing overhead: unknown
        frame=  600 fps=257 q=-0.0 Lsize=N/A time=00:00:10.00 bitrate=N/A speed=4.28x elapsed=0:00:02.33
"#;

        const CHUNK_SIZE: usize = 64;

        let ffmpeg = FFMPEG_OUT.as_bytes();

        let mut chunks = Chunks::default();
        let mut start_idx = 0;
        let mut xpsnr_score = None;
        while start_idx < ffmpeg.len() {
            let chunk = &ffmpeg[start_idx..(start_idx + CHUNK_SIZE).min(FFMPEG_OUT.len())];
            println!("* {}", String::from_utf8_lossy(chunk).trim());

            if let Some(xpsnr) = XpsnrOut::try_from_chunk(chunk, &mut chunks) {
                println!("{xpsnr:?}");
                if let XpsnrOut::Done(score) = xpsnr {
                    xpsnr_score = Some(score);
                }
            }

            start_idx += CHUNK_SIZE;
        }

        assert_eq!(xpsnr_score, Some(-3.283), "failed to parse xpsnr score");
    }
}
