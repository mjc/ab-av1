//! _sample-encode_ file system caching logic.
use crate::ffmpeg::FfmpegEncodeArgs;
use anyhow::Context;
use std::{
    ffi::OsStr,
    hash::Hash,
    path::{Path, PathBuf},
    time::{Duration, Instant},
};

/// Return a previous stored encode result for the same sample & args.
#[allow(clippy::too_many_arguments)]
pub async fn cached_encode(
    cache: bool,
    sample: &Path,
    source_input: &Path,
    input_duration: Duration,
    input_extension: Option<&OsStr>,
    input_size: u64,
    full_pass: bool,
    dest_ext: &str,
    enc_args: &FfmpegEncodeArgs<'_>,
    score: &impl Hash,
    vmaf: &impl Hash,
    use_xpsnr: bool,
    xpsnr_opts: &impl Hash,
) -> (Option<super::EncodeResult>, Option<Key>) {
    if !cache {
        return (None, None);
    }

    let key = encode_cache_key(
        sample,
        source_input,
        input_duration,
        input_extension,
        input_size,
        full_pass,
        dest_ext,
        enc_args,
        (score, vmaf, use_xpsnr, xpsnr_opts),
    );

    let cached = tokio::task::spawn_blocking::<_, anyhow::Result<_>>(move || {
        let db = open_db()?;
        Ok(match db.get(key.0.to_hex().as_bytes())? {
            Some(data) => Some(serde_json::from_slice::<super::EncodeResult>(&data)?),
            None => None,
        })
    })
    .await
    .context("db.get task failed")
    .and_then(|r| r);

    match cached {
        Ok(Some(mut result)) => {
            result.from_cache = true;
            (Some(result), Some(key))
        }
        Ok(None) => (None, Some(key)),
        Err(err) => {
            eprintln!("cache error: {err}");
            (None, None)
        }
    }
}

pub async fn cache_result(key: Key, result: &super::EncodeResult) -> anyhow::Result<()> {
    let data = serde_json::to_vec(result)?;
    let insert = tokio::task::spawn_blocking::<_, anyhow::Result<_>>(move || {
        let db = open_db()?;
        db.insert(key.0.to_hex().as_bytes(), data)?;
        db.flush()?;
        Ok(())
    })
    .await
    .context("db.insert task failed")
    .and_then(|r| Ok(r?));

    if let Err(err) = insert {
        eprintln!("cache error: {err}")
    }
    Ok(())
}

fn open_db() -> anyhow::Result<sled::Db> {
    const LOCK_MAX_WAIT: Duration = Duration::from_secs(2);

    let path = sample_encode_cache_path(dirs::cache_dir())?;
    let a = Instant::now();
    let mut db = sled::open(&path);
    while db.is_err() && a.elapsed() < LOCK_MAX_WAIT {
        std::thread::yield_now();
        db = sled::open(&path);
    }
    db.with_context(|| format!("failed to open sample-encode cache at {}", path.display()))
}

pub(crate) fn sample_encode_cache_path(cache_dir: Option<PathBuf>) -> anyhow::Result<PathBuf> {
    let mut path = cache_dir.with_context(|| {
        "sample-encode cache requires a cache directory; \
         set XDG_CACHE_HOME (or platform equivalent) or pass --no-cache"
    })?;
    path.push("ab-av1");
    path.push("sample-encode-cache");
    Ok(path)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Key(blake3::Hash);

/// Build a cache key from sample path, source input identity, and encode/scoring args.
#[allow(clippy::too_many_arguments)]
pub(crate) fn encode_cache_key(
    sample: &Path,
    source_input: &Path,
    input_duration: Duration,
    input_extension: Option<&OsStr>,
    input_size: u64,
    full_pass: bool,
    dest_ext: &str,
    enc_args: &FfmpegEncodeArgs<'_>,
    scoring: impl Hash,
) -> Key {
    Key(hash_encode(
        // Sample path + canonical source input path distinguish files that share
        // sample file names or weak metadata (duration, extension, size).
        (
            sample,
            source_input,
            input_duration,
            input_extension,
            input_size,
            full_pass,
            dest_ext,
        ),
        enc_args,
        scoring,
    ))
}

fn hash_encode(
    input_info: impl Hash,
    enc_args: &FfmpegEncodeArgs<'_>,
    scoring_info: impl Hash,
) -> blake3::Hash {
    let mut hasher = blake3::Hasher::new();
    let mut std_hasher = BlakeStdHasher(&mut hasher);
    input_info.hash(&mut std_hasher);
    enc_args.sample_encode_hash(&mut std_hasher);
    scoring_info.hash(&mut std_hasher);
    hasher.finalize()
}

struct BlakeStdHasher<'a>(&'a mut blake3::Hasher);
impl std::hash::Hasher for BlakeStdHasher<'_> {
    fn finish(&self) -> u64 {
        unimplemented!()
    }

    #[inline]
    fn write(&mut self, bytes: &[u8]) {
        self.0.update(bytes);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::command::args::{PixelFormat, ScoreArgs, Vmaf, Xpsnr};
    use crate::ffmpeg::FfmpegEncodeArgs;
    use std::{path::Path, sync::Arc, time::Duration};

    mod helpers {
        use super::*;

        use std::ffi::OsStr;

        pub fn minimal_enc_args(input: &Path) -> FfmpegEncodeArgs<'_> {
            FfmpegEncodeArgs {
                input,
                vcodec: Arc::from("libsvtav1"),
                vfilter: None,
                pix_fmt: None,
                crf: 30.0,
                preset: None,
                output_args: vec![],
                input_args: vec![],
                video_only: false,
            }
        }

        pub fn default_scoring(use_xpsnr: bool) -> (ScoreArgs, Vmaf, bool, Xpsnr) {
            (
                ScoreArgs {
                    reference_vfilter: None,
                },
                Vmaf::default(),
                use_xpsnr,
                Xpsnr {
                    xpsnr_fps: 60.0,
                    xpsnr_pix_format: None,
                },
            )
        }

        pub fn standard_key_inputs() -> (
            &'static Path,
            &'static Path,
            Option<&'static OsStr>,
            u64,
            Duration,
            (ScoreArgs, Vmaf, bool, Xpsnr),
        ) {
            (
                Path::new("/tmp/.ab-av1-abc/sample0+20f.mkv"),
                Path::new("/movies/a/clip.mkv"),
                Some(OsStr::new("mkv")),
                1_000_000_u64,
                Duration::from_secs(3600),
                default_scoring(false),
            )
        }
    }

    use helpers::{default_scoring, minimal_enc_args, standard_key_inputs};

    #[test]
    fn distinct_source_inputs_do_not_share_cache_keys_ab_kgc_3() {
        // setup
        let sample = Path::new("/tmp/.ab-av1-abc/sample0+20f.mkv");
        let input_a = Path::new("/movies/a/clip.mkv");
        let input_b = Path::new("/movies/b/clip.mkv");
        let duration = Duration::from_secs(3600);
        let extension = Some(std::ffi::OsStr::new("mkv"));
        let size = 1_000_000_u64;
        let enc = minimal_enc_args(input_a);
        let scoring = default_scoring(false);

        // execute
        let key_a = encode_cache_key(
            sample, input_a, duration, extension, size, false, "mkv", &enc, &scoring,
        );
        let key_b = encode_cache_key(
            sample, input_b, duration, extension, size, false, "mkv", &enc, &scoring,
        );

        // assert
        assert_ne!(
            key_a, key_b,
            "different source inputs must not share cache keys"
        );
    }

    #[test]
    fn identical_work_reuses_cache_key_ab_kgc_3() {
        // setup
        let sample = Path::new("/movies/a/.ab-av1-xyz/sample0+20f.mkv");
        let input = Path::new("/movies/a/clip.mkv");
        let duration = Duration::from_secs(3600);
        let extension = Some(std::ffi::OsStr::new("mkv"));
        let size = 1_000_000_u64;
        let enc = minimal_enc_args(input);
        let scoring = default_scoring(false);

        // execute
        let key_a = encode_cache_key(
            sample, input, duration, extension, size, false, "mkv", &enc, &scoring,
        );
        let key_b = encode_cache_key(
            sample, input, duration, extension, size, false, "mkv", &enc, &scoring,
        );

        // assert
        assert_eq!(
            key_a, key_b,
            "identical work must produce stable cache keys"
        );
    }

    #[test]
    fn missing_cache_dir_returns_actionable_error_ab_kgc_12() {
        // setup
        let err = sample_encode_cache_path(None).unwrap_err();

        // execute
        let message = format!("{err:#}");

        // assert
        assert!(
            message.contains("cache"),
            "error should mention cache: {message}"
        );
        assert!(
            message.contains("XDG_CACHE_HOME") || message.contains("no-cache"),
            "error should mention workaround: {message}"
        );
    }

    #[test]
    fn encode_cache_key_varies_with_crf_full_pass_and_duration() {
        // setup
        let (sample, input, extension, size, duration, scoring) = standard_key_inputs();
        let enc = minimal_enc_args(input);

        // execute — crf
        let mut enc_crf_a = enc.clone();
        enc_crf_a.crf = 30.0;
        let mut enc_crf_b = enc.clone();
        enc_crf_b.crf = 31.0;
        let mut enc_crf_frac = enc.clone();
        enc_crf_frac.crf = 30.5;
        let key_crf_a = encode_cache_key(
            sample, input, duration, extension, size, false, "mkv", &enc_crf_a, &scoring,
        );
        let key_crf_b = encode_cache_key(
            sample, input, duration, extension, size, false, "mkv", &enc_crf_b, &scoring,
        );
        let key_crf_frac = encode_cache_key(
            sample,
            input,
            duration,
            extension,
            size,
            false,
            "mkv",
            &enc_crf_frac,
            &scoring,
        );

        // execute — full_pass flag
        let key_full_pass_false = encode_cache_key(
            sample, input, duration, extension, size, false, "mkv", &enc, &scoring,
        );
        let key_full_pass_true = encode_cache_key(
            sample, input, duration, extension, size, true, "mkv", &enc, &scoring,
        );

        // execute — input duration
        let key_duration_a = encode_cache_key(
            sample,
            input,
            Duration::from_secs(100),
            extension,
            size,
            false,
            "mkv",
            &enc,
            &scoring,
        );
        let key_duration_b = encode_cache_key(
            sample,
            input,
            Duration::from_secs(200),
            extension,
            size,
            false,
            "mkv",
            &enc,
            &scoring,
        );

        // assert
        assert_ne!(key_crf_a, key_crf_b, "crf change must alter cache key");
        assert_ne!(
            key_crf_a, key_crf_frac,
            "fractional crf must alter cache key"
        );
        assert_ne!(key_full_pass_false, key_full_pass_true);
        assert_ne!(key_duration_a, key_duration_b);
    }

    // ab-kgc.21: xpsnr_pix_format must affect cache key
    #[test]
    fn xpsnr_pix_format_alters_cache_key() {
        // setup
        let (sample, input, extension, size, duration, _) = standard_key_inputs();
        let enc = minimal_enc_args(input);
        let scoring_420p = (
            ScoreArgs {
                reference_vfilter: None,
            },
            Vmaf::default(),
            true,
            Xpsnr {
                xpsnr_fps: 60.0,
                xpsnr_pix_format: Some(PixelFormat::Yuv420p),
            },
        );
        let scoring_420p10 = (
            ScoreArgs {
                reference_vfilter: None,
            },
            Vmaf::default(),
            true,
            Xpsnr {
                xpsnr_fps: 60.0,
                xpsnr_pix_format: Some(PixelFormat::Yuv420p10le),
            },
        );

        // execute
        let key_420p = encode_cache_key(
            sample,
            input,
            duration,
            extension,
            size,
            false,
            "mkv",
            &enc,
            &scoring_420p,
        );
        let key_420p10 = encode_cache_key(
            sample,
            input,
            duration,
            extension,
            size,
            false,
            "mkv",
            &enc,
            &scoring_420p10,
        );

        // assert
        assert_ne!(
            key_420p, key_420p10,
            "xpsnr_pix_format must be part of the cache key"
        );
    }

    // ab-kgc.25: production scoring tuple is (ScoreArgs, Vmaf, bool); xpsnr_opts.fps must affect key
    #[test]
    fn xpsnr_fps_alters_cache_key() {
        fn cache_key_for_xpsnr_fps(
            sample: &Path,
            input: &Path,
            enc: &FfmpegEncodeArgs<'_>,
            xpsnr_fps: f32,
        ) -> Key {
            let scoring = (
                ScoreArgs {
                    reference_vfilter: None,
                },
                Vmaf::default(),
                true,
                Xpsnr {
                    xpsnr_fps,
                    xpsnr_pix_format: None,
                },
            );
            encode_cache_key(
                sample,
                input,
                Duration::from_secs(3600),
                Some(std::ffi::OsStr::new("mkv")),
                1_000_000_u64,
                false,
                "mkv",
                enc,
                &scoring,
            )
        }

        // setup
        let sample = Path::new("/tmp/.ab-av1-abc/sample0+20f.mkv");
        let input = Path::new("/movies/a/clip.mkv");
        let enc = minimal_enc_args(input);

        // execute
        let key_fps_60 = cache_key_for_xpsnr_fps(sample, input, &enc, 60.0);
        let key_fps_30 = cache_key_for_xpsnr_fps(sample, input, &enc, 30.0);

        // assert
        assert_ne!(
            key_fps_60, key_fps_30,
            "xpsnr_fps must be part of the sample-encode cache key"
        );
    }

    // ab-kgc.26: encoded sample container extension affects encode output
    #[test]
    fn sample_dest_extension_alters_cache_key() {
        fn cache_key_for_dest_ext(
            sample: &Path,
            input: &Path,
            enc: &FfmpegEncodeArgs<'_>,
            dest_ext: &str,
        ) -> Key {
            let scoring = default_scoring(false);
            encode_cache_key(
                sample,
                input,
                Duration::from_secs(3600),
                Some(std::ffi::OsStr::new("mkv")),
                1_000_000_u64,
                false,
                dest_ext,
                enc,
                &scoring,
            )
        }

        // setup
        let sample = Path::new("/tmp/.ab-av1-abc/sample0+20f.mkv");
        let input = Path::new("/movies/a/clip.mkv");
        let enc = minimal_enc_args(input);

        // execute
        let key_mkv = cache_key_for_dest_ext(sample, input, &enc, "mkv");
        let key_av1 = cache_key_for_dest_ext(sample, input, &enc, "av1");

        // assert
        assert_ne!(
            key_mkv, key_av1,
            "sample output extension must be part of the cache key"
        );
    }

    /// Mirror production `cached_encode` scoring hash input.
    fn production_cache_key(
        sample: &Path,
        input: &Path,
        enc: &FfmpegEncodeArgs<'_>,
        score: &ScoreArgs,
        vmaf: &Vmaf,
        use_xpsnr: bool,
        xpsnr_opts: &Xpsnr,
    ) -> Key {
        encode_cache_key(
            sample,
            input,
            Duration::from_secs(3600),
            Some(std::ffi::OsStr::new("mkv")),
            1_000_000_u64,
            false,
            "mkv",
            enc,
            (score, vmaf, use_xpsnr, xpsnr_opts),
        )
    }

    // ab-kgc.46–54: scoring mode must affect cache key
    #[test]
    fn xpsnr_scoring_mode_must_alter_cache_key() {
        // setup — shared production tuple inputs
        let (sample, input, _, _, _, _) = standard_key_inputs();
        let enc = minimal_enc_args(input);
        let score = ScoreArgs {
            reference_vfilter: None,
        };
        let xpsnr_opts = Xpsnr {
            xpsnr_fps: 60.0,
            xpsnr_pix_format: None,
        };

        // execute — ab-kgc.46: vmaf-only vs xpsnr-only
        let vmaf = Vmaf::default();
        let key_vmaf_only =
            production_cache_key(sample, input, &enc, &score, &vmaf, false, &xpsnr_opts);
        let key_xpsnr_only =
            production_cache_key(sample, input, &enc, &score, &vmaf, true, &xpsnr_opts);

        // execute — ab-kgc.54: xpsnr-only vs xpsnr+and_vmaf
        let vmaf_and = Vmaf {
            and_vmaf: Some(true),
            ..Vmaf::default()
        };
        let key_vmaf_and_only =
            production_cache_key(sample, input, &enc, &score, &vmaf, true, &xpsnr_opts);
        let key_vmaf_and_both =
            production_cache_key(sample, input, &enc, &score, &vmaf_and, true, &xpsnr_opts);

        // assert
        assert_ne!(
            key_vmaf_only, key_xpsnr_only,
            "--xpsnr flag must be part of the sample-encode cache key"
        );
        assert_ne!(
            key_vmaf_and_only, key_vmaf_and_both,
            "xpsnr scoring mode must alter cache key when --and-vmaf is set"
        );
    }

    mod proptest_cache_keys {
        use super::*;
        use proptest::prelude::*;

        proptest! {
            #[test]
            fn distinct_sizes_produce_distinct_keys(
                size_a in 1_000u64..1_000_000u64,
                size_b in 1_000_000u64..2_000_000u64,
            ) {
                let sample = Path::new("/tmp/.ab-av1-abc/sample0+20f.mkv");
                let input = Path::new("/movies/a/clip.mkv");
                let duration = Duration::from_secs(3600);
                let extension = Some(std::ffi::OsStr::new("mkv"));
                let enc = minimal_enc_args(input);
                let scoring = default_scoring(false);

                let key_a = encode_cache_key(
                    sample, input, duration, extension, size_a, false, "mkv", &enc, &scoring,
                );
                let key_b = encode_cache_key(
                    sample, input, duration, extension, size_b, false, "mkv", &enc, &scoring,
                );
                prop_assert_ne!(key_a, key_b);
            }
        }
    }
}
