//! Workspace archive storage.
//!
//! OpenShell retains a stopped workspace for the lifecycle retention window.
//! After that window, deleting the sandbox deletes the workspace. This module
//! defines the seam that lets an embedding service save a workspace before
//! deletion and restore it into a replacement sandbox.
//!
//! The runtime produces a tar archive in a temporary directory and passes the
//! path to [`SnapshotStore`]. The store owns transfer, encryption, retention,
//! and durability. No S3 client is included.

use std::fmt;
use std::path::{Path, PathBuf};
use std::time::Duration;

use async_trait::async_trait;

use crate::error::Error;
use crate::ids::{SessionKey, TenantId, ThreadId};

/// Identifies the workspace archive for one conversation thread.
///
/// The key is stable across sandbox generations. Do not key archives by
/// sandbox name; a replacement sandbox has a different name.
#[derive(Clone, Debug, Eq, PartialEq, Hash)]
pub struct SnapshotKey {
    /// Tenant that owns the thread.
    pub tenant: TenantId,
    /// Conversation thread.
    pub thread: ThreadId,
}

impl SnapshotKey {
    /// Construct a key.
    pub fn new(tenant: TenantId, thread: ThreadId) -> Self {
        Self { tenant, thread }
    }

    /// Derive the key from a session key.
    pub fn from_session(key: &SessionKey) -> Self {
        Self::new(key.tenant().clone(), key.thread().clone())
    }
}

impl fmt::Display for SnapshotKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}/{}", self.tenant, self.thread)
    }
}

/// Metadata returned after a successful archive write.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SnapshotMeta {
    /// Archive identity.
    pub key: SnapshotKey,
    /// Archive size in bytes.
    pub size_bytes: u64,
    /// Creation time in Unix milliseconds.
    pub created_at_ms: i64,
}

/// Storage for thread workspace archives.
///
/// The runtime writes a tar archive of the workspace to `source` and calls
/// [`SnapshotStore::put`]. On restore, it creates an empty `destination` path
/// and calls [`SnapshotStore::get`]. The store may copy, upload, encrypt, or
/// version the bytes however it needs.
///
/// The store must be idempotent for the same key and content. A retry after a
/// transport failure must not corrupt an earlier archive. Keep the previous
/// archive until the new one is complete.
#[async_trait]
pub trait SnapshotStore: Send + Sync + fmt::Debug + 'static {
    /// Store the archive at `source`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Snapshot`] when the archive cannot be stored.
    async fn put(&self, key: &SnapshotKey, source: &Path) -> Result<SnapshotMeta, Error>;

    /// Write the archive for `key` to `destination`.
    ///
    /// Return `Ok(false)` when no archive exists for the key. The destination
    /// file is created by the runtime and must be replaced or truncated.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Snapshot`] when the archive cannot be read.
    async fn get(&self, key: &SnapshotKey, destination: &Path) -> Result<bool, Error>;

    /// Delete the archive for `key`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Snapshot`] when the archive cannot be deleted. A
    /// missing archive is not an error.
    async fn delete(&self, key: &SnapshotKey) -> Result<(), Error>;
}

/// When to archive and restore session workspaces.
#[derive(Clone, Debug)]
pub struct SnapshotPolicy {
    /// Archive the workspace before the runtime deletes the sandbox.
    ///
    /// This covers an explicit close and deletion after the retention window.
    /// The runtime may briefly start a stopped sandbox to read the workspace.
    pub archive_on_delete: bool,
    /// Restore the latest archive into a replacement sandbox before the first
    /// command.
    ///
    /// A stop/start cycle keeps the workspace in place and does not restore.
    pub restore_on_create: bool,
    /// Directory for the temporary archive.
    pub temp_dir: PathBuf,
    /// Maximum archive size in bytes.
    pub max_archive_bytes: u64,
    /// Timeout for one archive export or restore.
    pub timeout: Duration,
}

impl Default for SnapshotPolicy {
    fn default() -> Self {
        Self {
            archive_on_delete: false,
            restore_on_create: false,
            temp_dir: std::env::temp_dir(),
            max_archive_bytes: 1024 * 1024 * 1024,
            timeout: Duration::from_secs(900),
        }
    }
}

impl SnapshotPolicy {
    /// Return whether any snapshot behavior is enabled.
    pub fn enabled(&self) -> bool {
        self.archive_on_delete || self.restore_on_create
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_to_disabled() {
        let policy = SnapshotPolicy::default();
        assert!(!policy.enabled());
        assert!(policy.max_archive_bytes > 0);
    }

    #[test]
    fn key_is_stable_across_sandbox_generations() {
        let key = SessionKey::new(
            TenantId::new("tenant").unwrap(),
            ThreadId::new("thread").unwrap(),
        );
        let first = SnapshotKey::from_session(&key);
        let second = SnapshotKey::from_session(&key);
        assert_eq!(first, second);
        assert_eq!(first.to_string(), "tenant/thread");
    }
}
