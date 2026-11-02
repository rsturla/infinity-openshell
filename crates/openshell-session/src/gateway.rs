//! The gateway seam.
//!
//! The runtime talks to OpenShell only through [`Gateway`]. The real adapter
//! lives in [`crate::openshell`]; tests use the fake in [`crate::fake`].

use std::collections::BTreeMap;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;

use crate::error::Error;
use crate::ids::{SandboxName, SandboxPath};

/// Observed sandbox phase, normalized from the OpenShell SDK.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Phase {
    /// The sandbox does not exist.
    Absent,
    /// The runtime is provisioning compute.
    Provisioning,
    /// The sandbox is ready for exec.
    Ready,
    /// Compute is stopping while the workspace is retained.
    Stopping,
    /// Compute is stopped and the workspace is retained.
    Stopped,
    /// Compute is starting.
    Starting,
    /// State is suspended and the workspace is retained.
    Suspended,
    /// The canonical process exited successfully.
    Completed,
    /// The sandbox is in an error state.
    Error,
    /// The sandbox is being deleted.
    Deleting,
    /// The phase is not known to this client.
    Unknown,
}

/// Snapshot of one sandbox.
#[derive(Clone, Debug)]
pub struct SandboxInfo {
    /// Sandbox name.
    pub name: SandboxName,
    /// Observed phase.
    pub phase: Phase,
    /// Labels attached to the sandbox.
    pub labels: BTreeMap<String, String>,
    /// Creation or main-process start time in Unix milliseconds.
    pub created_at_ms: Option<i64>,
    /// Exit code of the canonical process, when available.
    pub exit_code: Option<i32>,
}

/// Request to create a sandbox.
#[derive(Clone, Debug)]
pub struct CreateSpec {
    /// Sandbox name.
    pub name: SandboxName,
    /// Workload template. `None` uses the gateway default image.
    pub template: Option<String>,
    /// Labels.
    pub labels: BTreeMap<String, String>,
    /// Optional canonical command. Empty selects the image's default shell.
    pub command: Vec<String>,
}

/// One command to execute in a sandbox.
#[derive(Clone, Debug)]
pub struct ExecRequest {
    /// Command and arguments. No shell is implied.
    pub argv: Vec<String>,
    /// Working directory. `None` uses the runtime workdir.
    pub workdir: Option<String>,
    /// Environment overrides.
    pub env: BTreeMap<String, String>,
    /// Optional stdin payload.
    pub stdin: Option<Bytes>,
    /// Optional timeout. `None` uses the runtime default.
    pub timeout: Option<Duration>,
    /// Maximum bytes retained from stdout or stderr.
    pub max_output_bytes: usize,
}

impl ExecRequest {
    /// Construct a request with a 1 MiB output cap.
    pub fn new(argv: Vec<String>) -> Self {
        Self {
            argv,
            workdir: None,
            env: BTreeMap::new(),
            stdin: None,
            timeout: None,
            max_output_bytes: 1024 * 1024,
        }
    }
}

/// Result of a command.
#[derive(Clone, Debug)]
pub struct ExecOutput {
    /// Process exit code.
    pub code: i32,
    /// Captured stdout.
    pub stdout: Bytes,
    /// Captured stderr.
    pub stderr: Bytes,
    /// Whether output was truncated at the configured limit.
    pub truncated: bool,
}

impl ExecOutput {
    /// Whether the command exited with status 0.
    pub fn is_success(&self) -> bool {
        self.code == 0
    }
}

/// Everything the session runtime needs from a sandbox provider.
///
/// Implementations must be safe to call concurrently. The runtime serializes
/// lifecycle transitions per session, but different sessions run in parallel.
#[async_trait]
pub trait Gateway: Send + Sync + 'static {
    /// Create a sandbox and return its snapshot.
    async fn create(&self, spec: CreateSpec) -> Result<SandboxInfo, Error>;

    /// Fetch a sandbox, or `None` when it does not exist.
    async fn get(&self, name: &SandboxName) -> Result<Option<SandboxInfo>, Error>;

    /// List sandboxes matching every label in `labels`.
    async fn list_managed(
        &self,
        labels: &BTreeMap<String, String>,
    ) -> Result<Vec<SandboxInfo>, Error>;

    /// Start a stopped or completed sandbox.
    async fn start(&self, name: &SandboxName) -> Result<(), Error>;

    /// Stop compute while retaining the workspace.
    async fn stop(&self, name: &SandboxName) -> Result<(), Error>;

    /// Suspend a sandbox. Providers that cannot retain state return
    /// [`Error::Unsupported`].
    async fn suspend(&self, _name: &SandboxName) -> Result<(), Error> {
        Err(Error::Unsupported("suspend".to_owned()))
    }

    /// Resume a suspended sandbox.
    async fn resume(&self, _name: &SandboxName) -> Result<(), Error> {
        Err(Error::Unsupported("resume".to_owned()))
    }

    /// Request deletion. Missing sandboxes are not an error.
    async fn delete(&self, name: &SandboxName) -> Result<(), Error>;

    /// Wait until the sandbox is ready or completed.
    async fn wait_ready(&self, name: &SandboxName, timeout: Duration) -> Result<(), Error>;

    /// Wait until the sandbox is gone.
    async fn wait_deleted(&self, name: &SandboxName, timeout: Duration) -> Result<(), Error>;

    /// Execute a command.
    async fn exec(&self, name: &SandboxName, request: ExecRequest) -> Result<ExecOutput, Error>;

    /// Read a file relative to the sandbox workspace.
    async fn read_file(&self, name: &SandboxName, path: &SandboxPath) -> Result<Bytes, Error>;

    /// Write a file relative to the sandbox workspace.
    async fn write_file(
        &self,
        name: &SandboxName,
        path: &SandboxPath,
        bytes: Bytes,
    ) -> Result<(), Error>;
}
