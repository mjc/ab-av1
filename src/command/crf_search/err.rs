use crate::command::crf_search::Sample;
use std::fmt;

#[derive(Debug)]
pub enum Error {
    NoGoodCrf { last: Sample },
    Other(anyhow::Error),
}

impl Error {
    pub fn ensure_other(condition: bool, reason: &'static str) -> Result<(), Self> {
        if !condition {
            return Err(Self::Other(anyhow::anyhow!(reason)));
        }
        Ok(())
    }

    pub fn ensure_or_no_good_crf(condition: bool, last: &Sample) -> Result<(), Self> {
        if !condition {
            return Err(Self::NoGoodCrf { last: last.clone() });
        }
        Ok(())
    }
}

impl From<anyhow::Error> for Error {
    fn from(err: anyhow::Error) -> Self {
        Self::Other(err)
    }
}

impl From<tokio::task::JoinError> for Error {
    fn from(err: tokio::task::JoinError) -> Self {
        Self::Other(err.into())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NoGoodCrf { .. } => "Failed to find a suitable crf".fmt(f),
            Self::Other(err) => err.fmt(f),
        }
    }
}

impl std::error::Error for Error {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::sample_encode;
    use std::time::Duration;

    fn search_sample(crf: f32) -> super::super::Sample {
        super::super::Sample {
            crf,
            q: crf.round() as i64,
            enc: sample_encode::Output {
                vmaf_score: Some(90.0),
                xpsnr_score: None,
                predicted_encode_size: 1_000_000,
                encode_percent: 50.0,
                predicted_encode_time: Duration::from_secs(60),
                from_cache: false,
            },
        }
    }

    #[test]
    fn ensure_other_ok_when_condition_true() {
        // setup
        // execute
        let result = Error::ensure_other(true, "should not fail");
        // assert
        assert!(result.is_ok());
    }

    #[test]
    fn ensure_other_err_when_condition_false() {
        // setup
        // execute
        let err = Error::ensure_other(false, "bad state").expect_err("expected error");
        // assert
        assert!(matches!(err, Error::Other(_)));
        assert!(err.to_string().contains("bad state"));
    }

    #[test]
    fn ensure_or_no_good_crf_returns_no_good_crf() {
        // setup
        let last = search_sample(32.0);
        // execute
        let err = Error::ensure_or_no_good_crf(false, &last).expect_err("expected NoGoodCrf");
        // assert
        assert!(matches!(err, Error::NoGoodCrf { .. }));
        assert_eq!(err.to_string(), "Failed to find a suitable crf");
    }

    #[test]
    fn from_anyhow_error_is_other() {
        // setup
        // execute
        let err: Error = anyhow::anyhow!("boom").into();
        // assert
        assert!(matches!(err, Error::Other(_)));
        assert!(err.to_string().contains("boom"));
    }

    #[test]
    fn from_join_error_is_other_with_message() {
        // setup
        let join_err = tokio::runtime::Runtime::new()
            .expect("runtime")
            .block_on(async {
                tokio::spawn(async { panic!("task boom") })
                    .await
                    .expect_err("spawned task should panic")
            });

        // execute
        let err: Error = join_err.into();

        // assert
        assert!(matches!(err, Error::Other(_)));
        assert!(
            err.to_string().contains("task"),
            "JoinError should surface in Display: {}",
            err
        );
    }

    // ab-kgc.34: NoGoodCrf should include the last attempted crf for actionable errors
    #[test]
    fn no_good_crf_display_includes_last_crf() {
        // setup
        let last = search_sample(37.5);
        let err = Error::NoGoodCrf { last };

        // execute
        let message = err.to_string();

        // assert
        assert!(
            message.contains("37.5") || message.contains("37"),
            "NoGoodCrf display should mention last crf, got: {message}"
        );
    }

    #[test]
    fn ensure_or_no_good_crf_ok_when_condition_true() {
        // setup
        let last = search_sample(28.0);
        // execute
        // assert
        assert!(Error::ensure_or_no_good_crf(true, &last).is_ok());
    }
}
