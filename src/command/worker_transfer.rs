use crate::temporary;
use anyhow::{Context, Result};
use blake3::Hash;
use std::{
    fs::{self, File, OpenOptions},
    io::Write,
    path::{Path, PathBuf},
};

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct Chunk {
    pub index: u64,
    pub offset: u64,
    pub bytes: Vec<u8>,
    pub checksum: Hash,
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug, thiserror::Error)]
pub(crate) enum ChunkReceiverError {
    #[error("chunk {index} arrived out of order; expected index {expected_index}")]
    OutOfOrder { index: u64, expected_index: u64 },
    #[error("chunk {index} starts at offset {offset}, expected {expected_offset}")]
    UnexpectedOffset {
        index: u64,
        offset: u64,
        expected_offset: u64,
    },
    #[error("chunk {index} failed checksum validation")]
    CorruptChunk { index: u64 },
    #[error("chunk {index} was already received")]
    DuplicateChunk { index: u64 },
    #[error("final digest mismatch")]
    DigestMismatch,
    #[error("final size mismatch")]
    SizeMismatch,
    #[error("destination already exists")]
    DestinationExists,
    #[error("file would exceed max size {max_size} bytes")]
    FileTooLarge { size: u64, max_size: u64 },
    #[error("receiver already finished")]
    Finished,
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

#[cfg_attr(not(test), allow(dead_code))]
#[derive(Debug)]
pub(crate) struct ChunkReceiver {
    final_path: PathBuf,
    temp_path: PathBuf,
    file: Option<File>,
    max_size: Option<u64>,
    next_index: u64,
    next_offset: u64,
    finished: bool,
    hasher: blake3::Hasher,
}

#[cfg_attr(not(test), allow(dead_code))]
impl ChunkReceiver {
    pub(crate) fn new(
        final_path: impl Into<PathBuf>,
        temp_dir: impl AsRef<Path>,
        max_size: Option<u64>,
    ) -> Result<Self> {
        let final_path = final_path.into();
        let temp_dir = temp_dir.as_ref();
        fs::create_dir_all(temp_dir).context("create chunk temp dir")?;
        let temp_path = temp_dir.join(format!(
            ".ab-av1-worker-{}-{}.part",
            std::process::id(),
            fastrand::u64(..)
        ));
        let file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temp_path)
            .context("create chunk temp file")?;
        temporary::add(&temp_path, temporary::TempKind::NotKeepable);

        Ok(Self {
            final_path,
            temp_path,
            file: Some(file),
            max_size,
            next_index: 0,
            next_offset: 0,
            finished: false,
            hasher: blake3::Hasher::new(),
        })
    }

    pub(crate) fn received_bytes(&self) -> u64 {
        self.next_offset
    }

    pub(crate) fn push(&mut self, chunk: Chunk) -> Result<(), ChunkReceiverError> {
        if self.finished {
            return Err(ChunkReceiverError::Finished);
        }
        if chunk.index < self.next_index {
            return Err(ChunkReceiverError::DuplicateChunk { index: chunk.index });
        }
        if chunk.index > self.next_index {
            return Err(ChunkReceiverError::OutOfOrder {
                index: chunk.index,
                expected_index: self.next_index,
            });
        }
        if chunk.offset != self.next_offset {
            return Err(ChunkReceiverError::UnexpectedOffset {
                index: chunk.index,
                offset: chunk.offset,
                expected_offset: self.next_offset,
            });
        }
        if self.max_size.is_some_and(|max_size| {
            self.next_offset.saturating_add(chunk.bytes.len() as u64) > max_size
        }) {
            return Err(ChunkReceiverError::FileTooLarge {
                size: self.next_offset.saturating_add(chunk.bytes.len() as u64),
                max_size: self.max_size.expect("checked max_size"),
            });
        }
        if blake3::hash(&chunk.bytes) != chunk.checksum {
            return Err(ChunkReceiverError::CorruptChunk { index: chunk.index });
        }

        self.file
            .as_mut()
            .expect("open chunk file")
            .write_all(&chunk.bytes)?;
        self.hasher.update(&chunk.bytes);
        self.next_index += 1;
        self.next_offset += chunk.bytes.len() as u64;
        Ok(())
    }

    pub(crate) fn finish(
        mut self,
        expected_size: Option<u64>,
        expected_digest: Option<Hash>,
    ) -> std::result::Result<PathBuf, ChunkReceiverError> {
        if self.finished {
            return Err(ChunkReceiverError::Finished);
        }
        if expected_size.is_some_and(|size| size != self.next_offset) {
            return Err(ChunkReceiverError::SizeMismatch);
        }
        let digest = self.hasher.finalize();
        if expected_digest.is_some_and(|expected| expected != digest) {
            return Err(ChunkReceiverError::DigestMismatch);
        }

        self.file.as_mut().expect("open chunk file").sync_all()?;
        let _ = self.file.take();
        if let Some(parent) = self.final_path.parent() {
            fs::create_dir_all(parent)?;
        }
        if self.final_path.exists() {
            return Err(ChunkReceiverError::DestinationExists);
        }
        fs::rename(&self.temp_path, &self.final_path)?;
        let final_path = self.final_path.clone();
        temporary::unadd(&self.temp_path);
        self.finished = true;
        Ok(final_path)
    }
}

impl Drop for ChunkReceiver {
    fn drop(&mut self) {
        if !self.finished {
            let _ = self.file.take();
            let _ = fs::remove_file(&self.temp_path);
            let _ = temporary::unadd(&self.temp_path);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serial_test::serial;

    fn temp_paths(label: &str) -> (PathBuf, PathBuf) {
        let root = std::env::temp_dir().join(format!(
            "ab-av1-worker-transfer-{}-{}-{}",
            label,
            std::process::id(),
            fastrand::u64(..)
        ));
        let final_path = root.join("movie.mkv");
        (root, final_path)
    }

    fn chunk(index: u64, offset: u64, bytes: &[u8]) -> Chunk {
        Chunk {
            index,
            offset,
            bytes: bytes.to_vec(),
            checksum: blake3::hash(bytes),
        }
    }

    #[serial]
    #[test]
    fn valid_transfer_writes_final_file() {
        let (temp_dir, final_path) = temp_paths("valid");
        let mut receiver = ChunkReceiver::new(&final_path, &temp_dir, None).expect("receiver");

        receiver.push(chunk(0, 0, b"hello ")).expect("chunk 0");
        receiver.push(chunk(1, 6, b"world")).expect("chunk 1");

        let written = receiver
            .finish(Some(11), Some(blake3::hash(b"hello world")))
            .expect("finish");

        assert_eq!(written, final_path);
        assert_eq!(fs::read(&final_path).expect("read final"), b"hello world");
        let _ = fs::remove_dir_all(temp_dir);
    }

    #[serial]
    #[test]
    fn finish_creates_parent_directory() {
        let temp_dir = std::env::temp_dir().join(format!(
            "ab-av1-worker-transfer-parent-{}-{}",
            std::process::id(),
            fastrand::u64(..)
        ));
        let final_path = temp_dir.join("nested").join("movie.mkv");
        let mut receiver = ChunkReceiver::new(&final_path, &temp_dir, None).expect("receiver");

        receiver.push(chunk(0, 0, b"hello")).expect("chunk 0");
        receiver
            .finish(Some(5), Some(blake3::hash(b"hello")))
            .expect("finish");

        assert_eq!(fs::read(&final_path).expect("read final"), b"hello");
        let _ = fs::remove_dir_all(temp_dir);
    }

    #[serial]
    #[test]
    fn finish_refuses_to_overwrite_existing_destination() {
        let temp_dir = std::env::temp_dir().join(format!(
            "ab-av1-worker-transfer-overwrite-{}-{}",
            std::process::id(),
            fastrand::u64(..)
        ));
        let final_path = temp_dir.join("nested").join("movie.mkv");
        fs::create_dir_all(final_path.parent().expect("parent")).expect("create parent");
        fs::write(&final_path, b"existing").expect("seed final");

        let mut receiver = ChunkReceiver::new(&final_path, &temp_dir, None).expect("receiver");
        receiver.push(chunk(0, 0, b"hello")).expect("chunk 0");

        assert!(matches!(
            receiver.finish(Some(5), Some(blake3::hash(b"hello"))),
            Err(ChunkReceiverError::DestinationExists)
        ));
        assert_eq!(fs::read(&final_path).expect("read final"), b"existing");
        let _ = fs::remove_dir_all(temp_dir);
    }

    #[serial]
    #[test]
    fn corrupt_chunk_is_rejected() {
        let (temp_dir, final_path) = temp_paths("corrupt");
        let mut receiver = ChunkReceiver::new(&final_path, &temp_dir, None).expect("receiver");

        let mut bad = chunk(0, 0, b"hello");
        bad.checksum = blake3::hash(b"hell0");

        assert!(matches!(
            receiver.push(bad),
            Err(ChunkReceiverError::CorruptChunk { index: 0 })
        ));
        let _ = fs::remove_dir_all(temp_dir);
    }

    #[serial]
    #[test]
    fn missing_chunk_is_rejected_by_offset() {
        let (temp_dir, final_path) = temp_paths("missing");
        let mut receiver = ChunkReceiver::new(&final_path, &temp_dir, None).expect("receiver");

        receiver.push(chunk(0, 0, b"hello")).expect("chunk 0");
        assert!(matches!(
            receiver.push(chunk(1, 10, b"world")),
            Err(ChunkReceiverError::UnexpectedOffset {
                index: 1,
                offset: 10,
                expected_offset: 5
            })
        ));
        let _ = fs::remove_dir_all(temp_dir);
    }

    #[serial]
    #[test]
    fn duplicate_chunk_is_rejected() {
        let (temp_dir, final_path) = temp_paths("duplicate");
        let mut receiver = ChunkReceiver::new(&final_path, &temp_dir, None).expect("receiver");

        receiver.push(chunk(0, 0, b"hello")).expect("chunk 0");
        assert!(matches!(
            receiver.push(chunk(0, 0, b"hello")),
            Err(ChunkReceiverError::DuplicateChunk { index: 0 })
        ));
        let _ = fs::remove_dir_all(temp_dir);
    }

    #[serial]
    #[test]
    fn out_of_order_chunk_is_rejected() {
        let (temp_dir, final_path) = temp_paths("out-of-order");
        let mut receiver = ChunkReceiver::new(&final_path, &temp_dir, None).expect("receiver");

        assert!(matches!(
            receiver.push(chunk(1, 0, b"hello")),
            Err(ChunkReceiverError::OutOfOrder {
                index: 1,
                expected_index: 0
            })
        ));
        let _ = fs::remove_dir_all(temp_dir);
    }

    #[serial]
    #[test]
    fn final_digest_mismatch_is_rejected() {
        let (temp_dir, final_path) = temp_paths("digest");
        let mut receiver = ChunkReceiver::new(&final_path, &temp_dir, None).expect("receiver");

        receiver.push(chunk(0, 0, b"hello")).expect("chunk 0");
        assert!(matches!(
            receiver.finish(Some(5), Some(blake3::hash(b"hell0"))),
            Err(ChunkReceiverError::DigestMismatch)
        ));
        let _ = fs::remove_dir_all(temp_dir);
    }

    #[serial]
    #[test]
    fn max_size_limit_is_enforced() {
        let (temp_dir, final_path) = temp_paths("max-size");
        let mut receiver = ChunkReceiver::new(&final_path, &temp_dir, Some(10)).expect("receiver");

        receiver.push(chunk(0, 0, b"hello")).expect("chunk 0");
        assert_eq!(receiver.received_bytes(), 5);
        assert!(matches!(
            receiver.push(chunk(1, 5, b"world!")),
            Err(ChunkReceiverError::FileTooLarge {
                size: 11,
                max_size: 10
            })
        ));
        let _ = fs::remove_dir_all(temp_dir);
    }
}
