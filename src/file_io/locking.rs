// File locking for concurrent access
use crate::error::{DdbError, Result};
use fs2::FileExt;
use std::fs::File;
use std::path::{Path, PathBuf};

pub struct FileLock {
    file: File,
    _path: PathBuf,
}

impl FileLock {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref().to_path_buf();
        let file = File::open(&path)
            .map_err(|e| DdbError::LockError(format!("Failed to open file: {}", e)))?;

        Ok(Self { file, _path: path })
    }

    pub fn lock_shared(&self) -> Result<()> {
        self.file
            .lock_shared()
            .map_err(|e| DdbError::LockError(format!("Failed to acquire shared lock: {}", e)))
    }

    pub fn lock_exclusive(&self) -> Result<()> {
        self.file
            .lock_exclusive()
            .map_err(|e| DdbError::LockError(format!("Failed to acquire exclusive lock: {}", e)))
    }

    pub fn unlock(&self) -> Result<()> {
        self.file
            .unlock()
            .map_err(|e| DdbError::LockError(format!("Failed to unlock: {}", e)))
    }
}

impl Drop for FileLock {
    fn drop(&mut self) {
        let _ = self.unlock();
    }
}
