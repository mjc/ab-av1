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
    enc_args: &FfmpegEncodeArgs<'_>,
    scoring: impl Hash,
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
        enc_args,
        scoring,
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
    use crate::command::args::{ScoreArgs, Vmaf, Xpsnr};
    use crate::ffmpeg::FfmpegEncodeArgs;
    use std::{path::Path, sync::Arc, time::Duration};

    mod helpers {
        use super::*;

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

        pub fn default_scoring() -> (ScoreArgs, Vmaf, Xpsnr) {
            (
                ScoreArgs {
                    reference_vfilter: None,
                },
                Vmaf::default(),
                Xpsnr {
                    xpsnr_fps: 60.0,
                    xpsnr_pix_format: None,
                },
            )
        }
    }

    use helpers::{default_scoring, minimal_enc_args};

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
        let scoring = default_scoring();

        // execute
        let key_a = encode_cache_key(
            sample,
            input_a,
            duration,
            extension,
            size,
            false,
            &enc,
            &scoring,
        );
        let key_b = encode_cache_key(
            sample,
            input_b,
            duration,
            extension,
            size,
            false,
            &enc,
            &scoring,
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
        let scoring = default_scoring();

        // execute
        let key_a = encode_cache_key(
            sample,
            input,
            duration,
            extension,
            size,
            false,
            &enc,
            &scoring,
        );
        let key_b = encode_cache_key(
            sample,
            input,
            duration,
            extension,
            size,
            false,
            &enc,
            &scoring,
        );

        // assert
        assert_eq!(key_a, key_b, "identical work must produce stable cache keys");
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
}
