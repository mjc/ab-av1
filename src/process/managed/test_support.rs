use std::{
    env,
    io::{self, Write},
    thread,
    time::Duration,
};
use tokio::process::Command;

pub const FIXTURE_ENV: &str = "AB_AV1_MANAGED_PROCESS_FIXTURE";
pub const FIXTURE_TEST: &str = "process::managed::tests::managed_process_fixture_child";

pub fn fixture_command(fixture: &str) -> Command {
    let mut cmd = Command::new(env::current_exe().expect("current test executable"));
    cmd.arg("--exact")
        .arg(FIXTURE_TEST)
        .arg("--nocapture")
        .env(FIXTURE_ENV, fixture);
    cmd
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
/// Portable process fixtures executed by re-running the current test binary.
///
/// Keep expected output ordering in `expected_sequence` when adding a case;
/// the catalog test below guards the behaviors needed by streaming tests
/// without depending on shell, media files, or platform-specific signals.
pub enum ManagedProcessFixture {
    StderrProgress,
    StderrWarning,
    StderrDigits,
    StderrOneTwo,
    StderrOneSleepTwo,
    StderrFfmpegProgress,
    StderrFfmpegProgressTwice,
    StderrBadnessExit7,
    StderrManyLinesExit7,
    StdoutNoiseStderrFfmpegProgress,
    StdoutOneSleepTwo,
    SleepLong,
    VmafScoreThenSleep,
    VmafProgressScore,
    VmafNoScore,
    VmafScoreExit7,
    StdoutNoiseVmafProgressScore,
    XpsnrScoreThenSleep,
    XpsnrProgressScore,
    XpsnrNoScore,
    XpsnrScoreExit7,
    StdoutNoiseXpsnrProgressScore,
}

impl ManagedProcessFixture {
    pub const ALL: &'static [Self] = &[
        Self::StderrProgress,
        Self::StderrWarning,
        Self::StderrDigits,
        Self::StderrOneTwo,
        Self::StderrOneSleepTwo,
        Self::StderrFfmpegProgress,
        Self::StderrFfmpegProgressTwice,
        Self::StderrBadnessExit7,
        Self::StderrManyLinesExit7,
        Self::StdoutNoiseStderrFfmpegProgress,
        Self::StdoutOneSleepTwo,
        Self::SleepLong,
        Self::VmafScoreThenSleep,
        Self::VmafProgressScore,
        Self::VmafNoScore,
        Self::VmafScoreExit7,
        Self::StdoutNoiseVmafProgressScore,
        Self::XpsnrScoreThenSleep,
        Self::XpsnrProgressScore,
        Self::XpsnrNoScore,
        Self::XpsnrScoreExit7,
        Self::StdoutNoiseXpsnrProgressScore,
    ];

    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL
            .iter()
            .copied()
            .find(|fixture| fixture.name() == name)
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::StderrProgress => "stderr-progress",
            Self::StderrWarning => "stderr-warning",
            Self::StderrDigits => "stderr-digits",
            Self::StderrOneTwo => "stderr-onetwo",
            Self::StderrOneSleepTwo => "stderr-one-sleep-two",
            Self::StderrFfmpegProgress => "stderr-ffmpeg-progress",
            Self::StderrFfmpegProgressTwice => "stderr-ffmpeg-progress-twice",
            Self::StderrBadnessExit7 => "stderr-badness-exit-7",
            Self::StderrManyLinesExit7 => "stderr-many-lines-exit-7",
            Self::StdoutNoiseStderrFfmpegProgress => "stdout-noise-stderr-ffmpeg-progress",
            Self::StdoutOneSleepTwo => "stdout-one-sleep-two",
            Self::SleepLong => "sleep-long",
            Self::VmafScoreThenSleep => "vmaf-score-then-sleep",
            Self::VmafProgressScore => "vmaf-progress-score",
            Self::VmafNoScore => "vmaf-no-score",
            Self::VmafScoreExit7 => "vmaf-score-exit-7",
            Self::StdoutNoiseVmafProgressScore => "stdout-noise-vmaf-progress-score",
            Self::XpsnrScoreThenSleep => "xpsnr-score-then-sleep",
            Self::XpsnrProgressScore => "xpsnr-progress-score",
            Self::XpsnrNoScore => "xpsnr-no-score",
            Self::XpsnrScoreExit7 => "xpsnr-score-exit-7",
            Self::StdoutNoiseXpsnrProgressScore => "stdout-noise-xpsnr-progress-score",
        }
    }

    pub fn expected_sequence(self) -> &'static str {
        match self {
            Self::StderrProgress => "stderr: progress; exit 0",
            Self::StderrWarning => "stderr: warning; exit 0",
            Self::StderrDigits => "stderr: 1234567890; exit 0",
            Self::StderrOneTwo => "stderr: onetwo; exit 0",
            Self::StderrOneSleepTwo => "stderr: one; delay; stderr: two; exit 0",
            Self::StderrFfmpegProgress => "stderr: one ffmpeg progress record; exit 0",
            Self::StderrFfmpegProgressTwice => {
                "stderr: ffmpeg progress frame 12; delay; stderr: ffmpeg progress frame 24; exit 0"
            }
            Self::StderrBadnessExit7 => "stderr: badness; exit 7",
            Self::StderrManyLinesExit7 => "stderr: 5000 numbered lines; exit 7",
            Self::StdoutNoiseStderrFfmpegProgress => {
                "stdout: noise; stderr: one ffmpeg progress record; exit 0"
            }
            Self::StdoutOneSleepTwo => "stdout: one; delay; stdout: two; exit 0",
            Self::SleepLong => {
                "delay long enough for timeout/termination tests; exit 0 if not killed"
            }
            Self::VmafScoreThenSleep => {
                "stderr: VMAF score; delay long enough for cancellation tests"
            }
            Self::VmafProgressScore => "stderr: ffmpeg progress; delay; stderr: VMAF score; exit 0",
            Self::VmafNoScore => "stderr: ffmpeg progress without score; exit 0",
            Self::VmafScoreExit7 => "stderr: VMAF score; stderr: badness; exit 7",
            Self::StdoutNoiseVmafProgressScore => {
                "stdout: noise; stderr: ffmpeg progress; delay; stderr: VMAF score; exit 0"
            }
            Self::XpsnrScoreThenSleep => {
                "stderr: XPSNR score; delay long enough for cancellation tests"
            }
            Self::XpsnrProgressScore => {
                "stderr: ffmpeg progress; delay; stderr: XPSNR score; exit 0"
            }
            Self::XpsnrNoScore => "stderr: ffmpeg progress without score; exit 0",
            Self::XpsnrScoreExit7 => "stderr: XPSNR score; stderr: badness; exit 7",
            Self::StdoutNoiseXpsnrProgressScore => {
                "stdout: noise; stderr: ffmpeg progress; delay; stderr: XPSNR score; exit 0"
            }
        }
    }

    pub fn has_periodic_progress(self) -> bool {
        matches!(self, Self::StderrFfmpegProgressTwice)
    }

    pub fn has_score_before_continued_runtime(self) -> bool {
        matches!(self, Self::VmafScoreThenSleep | Self::XpsnrScoreThenSleep)
    }

    pub fn has_noisy_stderr(self) -> bool {
        matches!(self, Self::StderrManyLinesExit7)
    }

    pub fn has_noisy_stdout(self) -> bool {
        matches!(
            self,
            Self::StdoutNoiseStderrFfmpegProgress
                | Self::StdoutNoiseVmafProgressScore
                | Self::StdoutNoiseXpsnrProgressScore
        )
    }

    pub fn has_non_zero_exit(self) -> bool {
        matches!(
            self,
            Self::StderrBadnessExit7
                | Self::StderrManyLinesExit7
                | Self::VmafScoreExit7
                | Self::XpsnrScoreExit7
        )
    }

    pub fn has_delayed_eof(self) -> bool {
        matches!(self, Self::StderrOneSleepTwo | Self::StdoutOneSleepTwo)
    }

    pub fn has_timeout_cleanup(self) -> bool {
        matches!(self, Self::SleepLong)
    }

    pub fn has_truncation_volume(self) -> bool {
        matches!(self, Self::StderrManyLinesExit7)
    }

    pub fn supports_delayed_subscription_replay(self) -> bool {
        matches!(
            self,
            Self::StderrOneTwo | Self::VmafProgressScore | Self::XpsnrProgressScore
        )
    }

    pub fn run(self) {
        match self {
            Self::StderrProgress => eprint!("progress"),
            Self::StderrWarning => eprint!("warning"),
            Self::StderrDigits => eprint!("1234567890"),
            Self::StderrOneTwo => eprint!("onetwo"),
            Self::StderrOneSleepTwo => {
                eprint!("one");
                io::stderr().flush().expect("flush stderr");
                thread::sleep(Duration::from_millis(10));
                eprint!("two");
            }
            Self::StderrFfmpegProgress => {
                eprint!(
                    "frame=  12 fps= 24 q=-0.0 size=N/A time=00:00:01.50 bitrate=N/A speed=1x    \r"
                );
            }
            Self::StderrFfmpegProgressTwice => {
                eprint!(
                    "frame=  12 fps= 24 q=-0.0 size=N/A time=00:00:01.50 bitrate=N/A speed=1x    \r"
                );
                io::stderr().flush().expect("flush stderr");
                thread::sleep(Duration::from_millis(10));
                eprint!(
                    "frame=  24 fps= 24 q=-0.0 size=N/A time=00:00:03.00 bitrate=N/A speed=1x    \r"
                );
            }
            Self::StderrBadnessExit7 => {
                eprint!("badness");
                std::process::exit(7);
            }
            Self::StderrManyLinesExit7 => {
                for n in 0..5000 {
                    eprintln!("line-{n:04}");
                }
                std::process::exit(7);
            }
            Self::StdoutNoiseStderrFfmpegProgress => {
                print!("stdout-noise");
                eprint!(
                    "frame=  3 fps= 30 q=-0.0 size=N/A time=00:00:00.25 bitrate=N/A speed=1x    \r"
                );
            }
            Self::StdoutOneSleepTwo => {
                print!("one");
                io::stdout().flush().expect("flush stdout");
                thread::sleep(Duration::from_millis(10));
                print!("two");
            }
            Self::SleepLong => thread::sleep(Duration::from_secs(30)),
            Self::VmafScoreThenSleep => {
                eprintln!("VMAF score: 97.500000");
                thread::sleep(Duration::from_secs(30));
            }
            Self::VmafProgressScore => {
                eprint!(
                    "frame=  12 fps= 24 q=-0.0 size=N/A time=00:00:01.50 bitrate=N/A speed=1x    \r"
                );
                io::stderr().flush().expect("flush stderr");
                thread::sleep(Duration::from_millis(10));
                eprintln!("VMAF score: 97.500000");
            }
            Self::VmafNoScore => {
                eprintln!("frame=  1 fps=  1 q=-0.0 size=N/A time=00:00:00.10 bitrate=N/A speed=1x")
            }
            Self::VmafScoreExit7 => {
                eprintln!("VMAF score: 97.500000");
                eprintln!("vmaf badness");
                std::process::exit(7);
            }
            Self::StdoutNoiseVmafProgressScore => {
                print!("stdout-noise");
                io::stdout().flush().expect("flush stdout");
                eprint!(
                    "frame=  3 fps= 30 q=-0.0 size=N/A time=00:00:00.25 bitrate=N/A speed=1x    \r"
                );
                io::stderr().flush().expect("flush stderr");
                thread::sleep(Duration::from_millis(10));
                eprintln!("VMAF score: 98.000000");
            }
            Self::XpsnrScoreThenSleep => {
                eprintln!(
                    "[Parsed_xpsnr_0 @ 0x1] XPSNR y: 33.6547 u: 41.8741 v: 42.2571 (minimum: 33.6547)"
                );
                thread::sleep(Duration::from_secs(30));
            }
            Self::XpsnrProgressScore => {
                eprint!(
                    "frame=  12 fps= 24 q=-0.0 size=N/A time=00:00:01.50 bitrate=N/A speed=1x    \r"
                );
                io::stderr().flush().expect("flush stderr");
                thread::sleep(Duration::from_millis(10));
                eprintln!(
                    "[Parsed_xpsnr_0 @ 0x1] XPSNR y: 33.6547 u: 41.8741 v: 42.2571 (minimum: 33.6547)"
                );
            }
            Self::XpsnrNoScore => {
                eprintln!("frame=  1 fps=  1 q=-0.0 size=N/A time=00:00:00.10 bitrate=N/A speed=1x")
            }
            Self::XpsnrScoreExit7 => {
                eprintln!(
                    "[Parsed_xpsnr_0 @ 0x1] XPSNR y: 33.6547 u: 41.8741 v: 42.2571 (minimum: 33.6547)"
                );
                eprintln!("xpsnr badness");
                std::process::exit(7);
            }
            Self::StdoutNoiseXpsnrProgressScore => {
                print!("stdout-noise");
                io::stdout().flush().expect("flush stdout");
                eprint!(
                    "frame=  3 fps= 30 q=-0.0 size=N/A time=00:00:00.25 bitrate=N/A speed=1x    \r"
                );
                io::stderr().flush().expect("flush stderr");
                thread::sleep(Duration::from_millis(10));
                eprintln!(
                    "[Parsed_xpsnr_0 @ 0x1] XPSNR y: 34.0000 u: 41.8741 v: 42.2571 (minimum: 34.0000)"
                );
            }
        }
    }
}
