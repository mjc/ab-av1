use std::time::Duration;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SampleCount(u64);

impl SampleCount {
    pub fn new(samples: u64) -> Self {
        Self(samples.max(1))
    }

    pub fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SampleIndex(u64);

impl SampleIndex {
    pub fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameCount(u32);

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum FrameCountError {
    #[error("sample frame count exceeds u32::MAX")]
    Overflow,
}

impl FrameCount {
    fn try_new(frames: u64) -> Result<Self, FrameCountError> {
        if frames > u64::from(u32::MAX) {
            Err(FrameCountError::Overflow)
        } else {
            Ok(Self(frames.max(1) as u32))
        }
    }

    pub fn get(self) -> u32 {
        self.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct FrameRate {
    fps: f64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum FrameRateError {
    #[error("frame rate must be finite and positive")]
    Invalid,
}

impl FrameRate {
    pub fn try_new(fps: f64) -> Result<Self, FrameRateError> {
        if fps.is_finite() && fps > 0.0 {
            Ok(Self { fps })
        } else {
            Err(FrameRateError::Invalid)
        }
    }

    fn frames_per_second(self) -> f64 {
        self.fps
    }

    pub fn one_frame_duration(self) -> Duration {
        Duration::from_secs_f64(1.0 / self.frames_per_second())
    }

    pub fn try_frame_count(self, duration: Duration) -> Result<FrameCount, FrameCountError> {
        let frames = (duration.as_secs_f64() * self.frames_per_second()).round() as u64;
        FrameCount::try_new(frames)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SampleWindow {
    index: SampleIndex,
    start: Duration,
    duration: Duration,
    frames: FrameCount,
    floor_to_sec: bool,
}

impl SampleWindow {
    pub fn index(self) -> SampleIndex {
        self.index
    }

    pub fn start(self) -> Duration {
        self.start
    }

    pub fn frames(self) -> FrameCount {
        self.frames
    }

    pub fn floor_to_sec(self) -> bool {
        self.floor_to_sec
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct SamplePlan {
    input_duration: Duration,
    sample_duration: Duration,
    sample_count: SampleCount,
    frame_count: FrameCount,
    grid: SampleGrid,
    full_pass: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct SampleGrid {
    divisor: u32,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SampleGridError {
    #[error("sample count is too large for grid planning")]
    TooManySamples,
}

impl SampleGrid {
    fn try_new(samples: SampleCount) -> Result<Self, SampleGridError> {
        let divisor = samples
            .get()
            .checked_add(1)
            .and_then(|value| u32::try_from(value).ok())
            .ok_or(SampleGridError::TooManySamples)?;
        Ok(Self {
            divisor: divisor.max(1),
        })
    }

    fn divisor(self) -> u32 {
        self.divisor
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum SamplePlanError {
    #[error(transparent)]
    FrameRate(#[from] FrameRateError),
    #[error(transparent)]
    FrameCount(#[from] FrameCountError),
    #[error(transparent)]
    SampleGrid(#[from] SampleGridError),
}

impl SamplePlan {
    pub fn try_new(
        input_duration: Duration,
        fps: f64,
        sample_count: SampleCount,
        requested_sample_duration: Duration,
        input_is_image: bool,
    ) -> Result<Self, SamplePlanError> {
        Self::from_frame_rate(
            input_duration,
            Some(FrameRate::try_new(fps)?),
            sample_count,
            requested_sample_duration,
            input_is_image,
        )
    }

    fn from_frame_rate(
        input_duration: Duration,
        frame_rate: Option<FrameRate>,
        sample_count: SampleCount,
        requested_sample_duration: Duration,
        input_is_image: bool,
    ) -> Result<Self, SamplePlanError> {
        let samples = sample_count.get();
        if input_is_image {
            return Ok(Self {
                input_duration,
                sample_duration: input_duration.max(Duration::from_secs(1)),
                sample_count: SampleCount::new(1),
                frame_count: FrameCount(1),
                grid: SampleGrid { divisor: 1 },
                full_pass: true,
            });
        }

        if requested_sample_duration.is_zero()
            || requested_sample_duration * samples as _ >= input_duration.mul_f64(0.85)
        {
            return Ok(Self {
                input_duration,
                sample_duration: input_duration,
                sample_count: SampleCount::new(1),
                frame_count: FrameCount(1),
                grid: SampleGrid { divisor: 1 },
                full_pass: true,
            });
        }

        let sample_duration = frame_rate
            .map(FrameRate::one_frame_duration)
            .map_or(requested_sample_duration, |one_frame| {
                requested_sample_duration.max(one_frame)
            });
        let grid = SampleGrid::try_new(sample_count)?;
        let frame_count = sample_frame_count(sample_duration, frame_rate)?;

        Ok(Self {
            input_duration,
            sample_duration,
            sample_count,
            frame_count,
            grid,
            full_pass: false,
        })
    }

    pub fn sample_duration(self) -> Duration {
        self.sample_duration
    }

    pub fn sample_count(self) -> SampleCount {
        self.sample_count
    }

    pub fn full_pass(self) -> bool {
        self.full_pass
    }

    pub fn windows(self) -> impl Iterator<Item = SampleWindow> {
        let samples = self.sample_count.get();
        let grid_divisor = self.grid.divisor();
        let available_gap = self
            .input_duration
            .saturating_sub(self.sample_duration * samples as _);
        (0..samples)
            .filter(move |_| !self.full_pass)
            .map(move |idx| {
                let sample_n = idx + 1;
                let start = (available_gap / grid_divisor) * sample_n as _
                    + self.sample_duration * idx as _;
                SampleWindow {
                    index: SampleIndex(idx),
                    start,
                    duration: self.sample_duration,
                    frames: self.frame_count,
                    floor_to_sec: self.sample_duration >= Duration::from_secs(2),
                }
            })
    }
}

fn sample_frame_count(
    sample_duration: Duration,
    frame_rate: Option<FrameRate>,
) -> Result<FrameCount, FrameCountError> {
    frame_rate
        .map(|fps| fps.try_frame_count(sample_duration))
        .unwrap_or(Ok(FrameCount(1)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;
    use std::time::Duration;

    #[test]
    fn frame_rate_rejects_non_positive_and_nan_values() {
        assert!(FrameRate::try_new(0.0).is_err());
        assert!(FrameRate::try_new(-24.0).is_err());
        assert!(FrameRate::try_new(f64::NAN).is_err());
    }

    #[test]
    fn frame_rate_is_a_local_checked_f64_newtype() {
        let rate = FrameRate::try_new(29.97).expect("valid frame rate");
        assert!(rate.one_frame_duration().as_secs_f64() > 0.0);
    }

    #[test]
    fn sample_plan_try_new_rejects_invalid_frame_rate() {
        assert_eq!(
            SamplePlan::try_new(
                Duration::from_secs(10),
                0.0,
                SampleCount::new(3),
                Duration::from_secs(1),
                false,
            ),
            Err(SamplePlanError::FrameRate(FrameRateError::Invalid))
        );
    }

    #[test]
    fn sample_plan_try_new_rejects_frame_count_overflow() {
        assert_eq!(
            SamplePlan::try_new(
                Duration::from_secs(1_000_000),
                1_000_000.0,
                SampleCount(2),
                Duration::from_secs(100_000),
                false,
            ),
            Err(SamplePlanError::FrameCount(FrameCountError::Overflow))
        );
    }

    #[test]
    fn frame_rate_converts_duration_to_frame_count() {
        let rate = FrameRate::try_new(29.97).expect("valid frame rate");
        let frames = rate
            .try_frame_count(Duration::from_secs(10))
            .expect("frame count");
        assert!(frames.get() > 0);
    }

    #[test]
    fn sample_plan_full_pass_for_images() {
        let plan = valid_sample_plan(
            Duration::from_secs(100),
            30.0,
            SampleCount::new(3),
            Duration::from_secs(10),
            true,
        );

        assert!(plan.full_pass());
        assert_eq!(plan.sample_count().get(), 1);
        assert_eq!(plan.sample_duration(), Duration::from_secs(100));
    }

    #[test]
    fn sample_plan_uses_one_frame_minimum_duration() {
        let plan = valid_sample_plan(
            Duration::from_secs(100),
            60.0,
            SampleCount::new(3),
            Duration::from_millis(1),
            false,
        );

        assert!(plan.sample_duration() >= Duration::from_secs_f64(1.0 / 60.0));
    }

    #[test]
    fn sample_plan_yields_existing_sample_grid() {
        let plan = valid_sample_plan(
            Duration::from_secs(100),
            30.0,
            SampleCount::new(3),
            Duration::from_secs(10),
            false,
        );

        let windows = plan.windows().collect::<Vec<_>>();
        assert_eq!(
            windows
                .iter()
                .map(|window| window.index().get())
                .collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        assert_eq!(
            windows
                .iter()
                .map(|window| window.start())
                .collect::<Vec<_>>(),
            vec![
                Duration::from_millis(17_500),
                Duration::from_secs(45),
                Duration::from_millis(72_500)
            ]
        );
        assert_eq!(
            windows
                .iter()
                .map(|window| window.frames().get())
                .collect::<Vec<_>>(),
            vec![300, 300, 300]
        );
    }

    #[test]
    fn sample_plan_window_iteration_does_not_allocate() {
        let plan = valid_sample_plan(
            Duration::from_secs(100),
            30.0,
            SampleCount::new(16),
            Duration::from_secs(5),
            false,
        );

        crate::test_support::assert_no_allocations(|| {
            plan.windows().for_each(|window| {
                std::hint::black_box(window);
            });
        });
    }

    #[test]
    fn sample_plan_try_new_rejects_huge_sample_grid() {
        assert_eq!(
            SamplePlan::try_new(
                Duration::from_secs(100),
                30.0,
                SampleCount(u64::from(u32::MAX)),
                Duration::from_nanos(1),
                false,
            ),
            Err(SamplePlanError::SampleGrid(SampleGridError::TooManySamples))
        );
    }

    fn valid_sample_plan(
        input_duration: Duration,
        fps: f64,
        sample_count: SampleCount,
        requested_sample_duration: Duration,
        input_is_image: bool,
    ) -> SamplePlan {
        SamplePlan::try_new(
            input_duration,
            fps,
            sample_count,
            requested_sample_duration,
            input_is_image,
        )
        .expect("sample plan inputs should be valid")
    }

    mod proptest_sample_plan {
        use super::*;

        proptest! {
            #[test]
            fn sample_count_monotonic_in_duration(
                input_duration_secs in 2u64..3600,
                extra_secs in 1u64..3600,
            ) {
                let short = SamplePlan::try_new(
                    Duration::from_secs(input_duration_secs),
                    30.0,
                    SampleCount::new(3),
                    Duration::from_secs(10),
                    false,
                ).expect("short plan");
                let long = SamplePlan::try_new(
                    Duration::from_secs(input_duration_secs + extra_secs),
                    30.0,
                    SampleCount::new(3),
                    Duration::from_secs(10),
                    false,
                ).expect("long plan");

                prop_assert!(long.sample_count().get() >= short.sample_count().get());
            }
        }
    }
}
