//! Deterministic in-memory gateway for tests and examples.
//!
//! Enable the `testkit` feature to use this module from another crate.

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use bytes::Bytes;

use crate::error::Error;
use crate::gateway::{CreateSpec, ExecOutput, ExecRequest, Gateway, Phase, SandboxInfo};
use crate::ids::{SandboxName, SandboxPath};

/// One sandbox in the fake gateway.
#[derive(Clone, Debug)]
struct FakeSandbox {
    info: SandboxInfo,
    files: BTreeMap<String, Bytes>,
    execs: Vec<ExecRequest>,
}

/// In-memory gateway with fault injection.
#[derive(Default)]
pub struct FakeGateway {
    state: Mutex<State>,
}

#[derive(Default)]
struct State {
    sandboxes: BTreeMap<String, FakeSandbox>,
    calls: Vec<String>,
    create_phase: Option<Phase>,
    fail_next: Option<String>,
    next_stdout: Option<Bytes>,
    next_exit_code: Option<i32>,
}

impl FakeGateway {
    /// Create an empty fake gateway.
    pub fn new() -> Self {
        Self::default()
    }

    /// Names of the operations invoked so far.
    pub fn calls(&self) -> Vec<String> {
        self.state
            .lock()
            .expect("bug: fake state poisoned")
            .calls
            .clone()
    }

    /// Force the phase used by the next create.
    pub fn set_create_phase(&self, phase: Phase) {
        self.state
            .lock()
            .expect("bug: fake state poisoned")
            .create_phase = Some(phase);
    }

    /// Set the phase of an existing sandbox.
    pub fn set_phase(&self, name: &SandboxName, phase: Phase) {
        let mut state = self.state.lock().expect("bug: fake state poisoned");
        if let Some(sandbox) = state.sandboxes.get_mut(name.as_str()) {
            sandbox.info.phase = phase;
        }
    }

    /// Make the next call to `operation` fail with a transient gateway error.
    pub fn fail_next(&self, operation: &str) {
        self.state
            .lock()
            .expect("bug: fake state poisoned")
            .fail_next = Some(operation.to_owned());
    }

    /// Set the stdout returned by the next exec.
    pub fn set_next_stdout(&self, stdout: impl Into<Bytes>) {
        self.state
            .lock()
            .expect("bug: fake state poisoned")
            .next_stdout = Some(stdout.into());
    }

    /// Set the exit code returned by the next exec.
    pub fn set_next_exit_code(&self, code: i32) {
        self.state
            .lock()
            .expect("bug: fake state poisoned")
            .next_exit_code = Some(code);
    }

    /// Return the labels stored for a sandbox.
    pub fn labels(&self, name: &SandboxName) -> BTreeMap<String, String> {
        self.state
            .lock()
            .expect("bug: fake state poisoned")
            .sandboxes
            .get(name.as_str())
            .map_or_else(BTreeMap::new, |sandbox| sandbox.info.labels.clone())
    }

    /// Return the files stored for a sandbox.
    pub fn files(&self, name: &SandboxName) -> BTreeMap<String, Bytes> {
        self.state
            .lock()
            .expect("bug: fake state poisoned")
            .sandboxes
            .get(name.as_str())
            .map_or_else(BTreeMap::new, |sandbox| sandbox.files.clone())
    }

    /// Return the commands executed in a sandbox.
    pub fn execs(&self, name: &SandboxName) -> Vec<ExecRequest> {
        self.state
            .lock()
            .expect("bug: fake state poisoned")
            .sandboxes
            .get(name.as_str())
            .map_or_else(Vec::new, |sandbox| sandbox.execs.clone())
    }

    fn take_failure(state: &mut State, operation: &str) -> Result<(), Error> {
        if state.fail_next.as_deref() == Some(operation) {
            state.fail_next = None;
            Err(Error::Unavailable(format!(
                "injected failure for {operation}"
            )))
        } else {
            Ok(())
        }
    }
}

#[async_trait]
impl Gateway for FakeGateway {
    async fn create(&self, spec: CreateSpec) -> Result<SandboxInfo, Error> {
        let mut state = self.state.lock().expect("bug: fake state poisoned");
        state.calls.push("create".to_owned());
        Self::take_failure(&mut state, "create")?;
        let info = SandboxInfo {
            name: spec.name.clone(),
            phase: state.create_phase.unwrap_or(Phase::Ready),
            labels: spec.labels,
            created_at_ms: Some(now_ms()),
            exit_code: None,
        };
        state.sandboxes.insert(
            spec.name.as_str().to_owned(),
            FakeSandbox {
                info: info.clone(),
                files: BTreeMap::new(),
                execs: Vec::new(),
            },
        );
        Ok(info)
    }

    async fn get(&self, name: &SandboxName) -> Result<Option<SandboxInfo>, Error> {
        let mut state = self.state.lock().expect("bug: fake state poisoned");
        state.calls.push("get".to_owned());
        Self::take_failure(&mut state, "get")?;
        Ok(state
            .sandboxes
            .get(name.as_str())
            .map(|sandbox| sandbox.info.clone()))
    }

    async fn list_managed(
        &self,
        labels: &BTreeMap<String, String>,
    ) -> Result<Vec<SandboxInfo>, Error> {
        let mut state = self.state.lock().expect("bug: fake state poisoned");
        state.calls.push("list".to_owned());
        Self::take_failure(&mut state, "list")?;
        Ok(state
            .sandboxes
            .values()
            .filter(|sandbox| {
                labels
                    .iter()
                    .all(|(key, value)| sandbox.info.labels.get(key) == Some(value))
            })
            .map(|sandbox| sandbox.info.clone())
            .collect())
    }

    async fn start(&self, name: &SandboxName) -> Result<(), Error> {
        let mut state = self.state.lock().expect("bug: fake state poisoned");
        state.calls.push("start".to_owned());
        Self::take_failure(&mut state, "start")?;
        let sandbox = state
            .sandboxes
            .get_mut(name.as_str())
            .ok_or_else(|| Error::NotFound(name.to_string()))?;
        sandbox.info.phase = Phase::Ready;
        Ok(())
    }

    async fn stop(&self, name: &SandboxName) -> Result<(), Error> {
        let mut state = self.state.lock().expect("bug: fake state poisoned");
        state.calls.push("stop".to_owned());
        Self::take_failure(&mut state, "stop")?;
        let sandbox = state
            .sandboxes
            .get_mut(name.as_str())
            .ok_or_else(|| Error::NotFound(name.to_string()))?;
        sandbox.info.phase = Phase::Stopped;
        Ok(())
    }

    async fn suspend(&self, name: &SandboxName) -> Result<(), Error> {
        let mut state = self.state.lock().expect("bug: fake state poisoned");
        state.calls.push("suspend".to_owned());
        Self::take_failure(&mut state, "suspend")?;
        let sandbox = state
            .sandboxes
            .get_mut(name.as_str())
            .ok_or_else(|| Error::NotFound(name.to_string()))?;
        sandbox.info.phase = Phase::Suspended;
        Ok(())
    }

    async fn resume(&self, name: &SandboxName) -> Result<(), Error> {
        let mut state = self.state.lock().expect("bug: fake state poisoned");
        state.calls.push("resume".to_owned());
        Self::take_failure(&mut state, "resume")?;
        let sandbox = state
            .sandboxes
            .get_mut(name.as_str())
            .ok_or_else(|| Error::NotFound(name.to_string()))?;
        sandbox.info.phase = Phase::Ready;
        Ok(())
    }

    async fn delete(&self, name: &SandboxName) -> Result<(), Error> {
        let mut state = self.state.lock().expect("bug: fake state poisoned");
        state.calls.push("delete".to_owned());
        Self::take_failure(&mut state, "delete")?;
        state.sandboxes.remove(name.as_str());
        Ok(())
    }

    async fn wait_ready(&self, name: &SandboxName, _timeout: Duration) -> Result<(), Error> {
        let mut state = self.state.lock().expect("bug: fake state poisoned");
        state.calls.push("wait_ready".to_owned());
        Self::take_failure(&mut state, "wait_ready")?;
        let sandbox = state
            .sandboxes
            .get_mut(name.as_str())
            .ok_or_else(|| Error::NotFound(name.to_string()))?;
        match sandbox.info.phase {
            Phase::Ready | Phase::Completed => Ok(()),
            Phase::Provisioning | Phase::Starting => {
                sandbox.info.phase = Phase::Ready;
                Ok(())
            }
            Phase::Error => Err(Error::Gateway("sandbox entered error phase".to_owned())),
            Phase::Stopped => Err(Error::Timeout("sandbox is stopped".to_owned())),
            Phase::Suspended | Phase::Stopping => Err(Error::Gateway(
                "sandbox cannot become ready from its current phase".to_owned(),
            )),
            Phase::Deleting | Phase::Absent => Err(Error::NotFound(name.to_string())),
            _ => Ok(()),
        }
    }

    async fn wait_deleted(&self, name: &SandboxName, _timeout: Duration) -> Result<(), Error> {
        let mut state = self.state.lock().expect("bug: fake state poisoned");
        state.calls.push("wait_deleted".to_owned());
        Self::take_failure(&mut state, "wait_deleted")?;
        state.sandboxes.remove(name.as_str());
        Ok(())
    }

    async fn exec(&self, name: &SandboxName, request: ExecRequest) -> Result<ExecOutput, Error> {
        let mut state = self.state.lock().expect("bug: fake state poisoned");
        state.calls.push("exec".to_owned());
        Self::take_failure(&mut state, "exec")?;
        let stdout = state
            .next_stdout
            .take()
            .unwrap_or_else(|| Bytes::from_static(b"ok\n"));
        let code = state.next_exit_code.take().unwrap_or(0);
        let sandbox = state
            .sandboxes
            .get_mut(name.as_str())
            .ok_or_else(|| Error::NotFound(name.to_string()))?;
        sandbox.execs.push(request);
        Ok(ExecOutput {
            code,
            stdout,
            stderr: Bytes::new(),
            truncated: false,
        })
    }

    async fn read_file(&self, name: &SandboxName, path: &SandboxPath) -> Result<Bytes, Error> {
        let mut state = self.state.lock().expect("bug: fake state poisoned");
        state.calls.push("read_file".to_owned());
        Self::take_failure(&mut state, "read_file")?;
        let sandbox = state
            .sandboxes
            .get(name.as_str())
            .ok_or_else(|| Error::NotFound(name.to_string()))?;
        sandbox
            .files
            .get(path.as_str())
            .cloned()
            .ok_or_else(|| Error::NotFound(path.to_string()))
    }

    async fn write_file(
        &self,
        name: &SandboxName,
        path: &SandboxPath,
        bytes: Bytes,
    ) -> Result<(), Error> {
        let mut state = self.state.lock().expect("bug: fake state poisoned");
        state.calls.push("write_file".to_owned());
        Self::take_failure(&mut state, "write_file")?;
        let sandbox = state
            .sandboxes
            .get_mut(name.as_str())
            .ok_or_else(|| Error::NotFound(name.to_string()))?;
        sandbox.files.insert(path.as_str().to_owned(), bytes);
        Ok(())
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| {
            i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
        })
}
