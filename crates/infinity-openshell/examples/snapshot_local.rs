//! Persist a workspace across sandbox deletion with a local archive store.
//!
//! The store is three small methods. Replace it with S3, GCS, or an internal
//! object store; the runtime only needs the trait.
//!
//! ```shell
//! OPENSHELL_TEST_GATEWAY=demo \
//!   cargo run -p infinity-openshell --example snapshot_local --features bootstrap
//! ```

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use openshell_session::{
    BootstrapTokenSource, Error, ExecRequest, GatewayAuth, GatewayConfig, RuntimeConfig,
    SandboxPath, SessionKey, SessionRuntime, SnapshotKey, SnapshotMeta, SnapshotStore, TenantId,
    ThreadId,
};

/// Stores each thread archive under `root/<tenant>-<thread>/workspace.tar`.
#[derive(Debug)]
struct LocalSnapshotStore {
    root: PathBuf,
}

impl LocalSnapshotStore {
    fn new(root: PathBuf) -> Self {
        Self { root }
    }

    fn archive_path(&self, key: &SnapshotKey) -> PathBuf {
        self.root
            .join(format!(
                "{}-{}",
                sanitize(key.tenant.as_str()),
                sanitize(key.thread.as_str())
            ))
            .join("workspace.tar")
    }
}

#[async_trait]
impl SnapshotStore for LocalSnapshotStore {
    async fn put(&self, key: &SnapshotKey, source: &Path) -> Result<SnapshotMeta, Error> {
        let destination = self.archive_path(key);
        if let Some(parent) = destination.parent() {
            tokio::fs::create_dir_all(parent).await.map_err(|error| {
                Error::Snapshot(format!("cannot create the archive directory: {error}"))
            })?;
        }
        tokio::fs::copy(source, &destination)
            .await
            .map_err(|error| Error::Snapshot(format!("cannot copy the archive: {error}")))?;
        let size_bytes = tokio::fs::metadata(&destination)
            .await
            .map_err(|error| Error::Snapshot(format!("cannot stat the archive: {error}")))?
            .len();
        Ok(SnapshotMeta {
            key: key.clone(),
            size_bytes,
            created_at_ms: now_ms(),
        })
    }

    async fn get(&self, key: &SnapshotKey, destination: &Path) -> Result<bool, Error> {
        let source = self.archive_path(key);
        match tokio::fs::copy(&source, destination).await {
            Ok(_) => Ok(true),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(false),
            Err(error) => Err(Error::Snapshot(format!("cannot read the archive: {error}"))),
        }
    }

    async fn delete(&self, key: &SnapshotKey) -> Result<(), Error> {
        let directory = self
            .archive_path(key)
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_default();
        match tokio::fs::remove_dir_all(&directory).await {
            Ok(()) => Ok(()),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(error) => Err(Error::Snapshot(format!(
                "cannot delete the archive: {error}"
            ))),
        }
    }
}

fn sanitize(value: &str) -> String {
    value
        .chars()
        .map(|character| {
            if character.is_ascii_alphanumeric() || matches!(character, '-' | '_') {
                character
            } else {
                '_'
            }
        })
        .collect()
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_millis() as i64)
}

fn unique_thread() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    format!("snapshot-{}-{nanos}", std::process::id())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let gateway_name =
        std::env::var("OPENSHELL_TEST_GATEWAY").unwrap_or_else(|_| "demo".to_owned());
    let directory = tempfile::tempdir()?;

    let source = Arc::new(BootstrapTokenSource::new(&gateway_name)?);
    let mut config = RuntimeConfig::new(GatewayConfig::new(
        source.endpoint()?,
        "default",
        GatewayAuth::OidcSource { source },
    ));
    config.snapshot.archive_on_delete = true;
    config.snapshot.restore_on_create = true;
    config.snapshot_store = Some(Arc::new(LocalSnapshotStore::new(
        directory.path().to_path_buf(),
    )));
    config.lifecycle.idle_after = Duration::from_secs(300);
    config.lifecycle.retain_for = Duration::from_secs(3600);
    config.lifecycle.ready_timeout = Duration::from_secs(180);
    config.lifecycle.exec_timeout = Duration::from_secs(60);
    config.validate()?;

    let runtime = SessionRuntime::open(config).await?;
    let key = SessionKey::new(TenantId::new("example")?, ThreadId::new(unique_thread())?);
    let path = SandboxPath::new("work/state.txt")?;

    let session = runtime.acquire(key.clone()).await?;
    println!("first sandbox: {}", session.name());
    session
        .exec(ExecRequest::new(vec![
            "sh".to_owned(),
            "-c".to_owned(),
            "mkdir -p work && printf 'persisted\\n' > work/state.txt".to_owned(),
        ]))
        .await?;
    let before = String::from_utf8_lossy(&session.read_file(&path).await?).to_string();
    println!("before delete: {before:?}");
    drop(session);

    // Close deletes the sandbox. The archive runs before the delete.
    runtime.close(&key).await?;
    println!("archived and deleted");

    let archive = LocalSnapshotStore::new(directory.path().to_path_buf()).archive_path(
        &SnapshotKey::new(key.tenant().clone(), key.thread().clone()),
    );
    let size = tokio::fs::metadata(&archive).await?.len();
    println!("archive: {} ({size} bytes)", archive.display());
    assert!(size > 0, "the archive is empty");

    // A new sandbox is created for the same thread and the archive is restored
    // before the first command runs.
    let session = runtime.acquire(key.clone()).await?;
    println!("replacement sandbox: {}", session.name());
    let after = String::from_utf8_lossy(&session.read_file(&path).await?).to_string();
    println!("after restore: {after:?}");
    assert_eq!(after, "persisted\n");
    drop(session);

    runtime.close(&key).await?;
    println!("closed");
    Ok(())
}
