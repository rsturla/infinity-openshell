//! A held lease on a ready sandbox.

use std::fmt;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use bytes::Bytes;
use futures_util::StreamExt;

use crate::actor::Lease;
use crate::error::Error;
use crate::gateway::{ExecOutput, ExecRequest, ExecStream, Gateway};
use crate::ids::{Generation, SandboxName, SandboxPath};

/// A lease on a ready sandbox.
///
/// The sandbox stays ready while at least one session exists. Dropping the
/// session releases the lease; the runtime stops the sandbox after the idle
/// window. A session is not `Clone`: call
/// [`SessionRuntime::acquire`](crate::SessionRuntime::acquire) again for
/// concurrent work on the same thread.
pub struct Session {
    gateway: Arc<dyn Gateway>,
    lease: Arc<Lease>,
}

impl Session {
    pub(crate) fn new(gateway: Arc<dyn Gateway>, lease: Lease) -> Self {
        Self {
            gateway,
            lease: Arc::new(lease),
        }
    }

    /// Return the sandbox name.
    pub fn name(&self) -> &SandboxName {
        &self.lease.name
    }

    /// Return the sandbox generation.
    pub fn generation(&self) -> Generation {
        self.lease.generation
    }

    /// Return the maximum bytes retained per output channel.
    pub fn max_output_bytes(&self) -> usize {
        self.lease.max_output_bytes
    }

    /// Execute a command in the sandbox.
    ///
    /// The request's output cap is lowered to the runtime limit, and a missing
    /// timeout is filled from the runtime default. Arguments and environment
    /// are validated against OpenShell's exec limits before dispatch.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`] for a request that exceeds OpenShell's
    /// limits, [`Error::StaleSession`] when the sandbox no longer exists, and a
    /// gateway error for execution failures.
    pub async fn exec(&self, request: ExecRequest) -> Result<ExecOutput, Error> {
        self.ensure_alive()?;
        let request = self.prepare(request)?;
        let _permit = self
            .lease
            .exec_permits
            .acquire()
            .await
            .map_err(|_| Error::Internal("exec semaphore closed".to_owned()))?;

        self.gateway
            .exec(&self.lease.name, request)
            .await
            .map_err(|error| match error {
                Error::NotFound(_) => Error::StaleSession,
                other => other,
            })
    }

    /// Execute a command and stream its output.
    ///
    /// The semaphore permit is held for the life of the stream, so
    /// `max_concurrent_execs` also bounds streaming commands.
    ///
    /// # Errors
    ///
    /// Returns [`Error::StaleSession`] when the sandbox is no longer current,
    /// or a gateway error when execution cannot start.
    pub async fn exec_stream(&self, request: ExecRequest) -> Result<ExecStream, Error> {
        self.ensure_alive()?;
        let request = self.prepare(request)?;
        let permit = self
            .lease
            .exec_permits
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| Error::Internal("exec semaphore closed".to_owned()))?;
        let lease = Arc::clone(&self.lease);
        let mut stream = self
            .gateway
            .exec_stream(&self.lease.name, request)
            .await
            .map_err(|error| match error {
                Error::NotFound(_) => Error::StaleSession,
                other => other,
            })?;
        let output = async_stream::try_stream! {
            let _permit = permit;
            let _lease = lease;
            while let Some(event) = stream.next().await {
                yield event?;
            }
        };
        Ok(Box::pin(output))
    }

    fn prepare(&self, mut request: ExecRequest) -> Result<ExecRequest, Error> {
        request.max_output_bytes = if request.max_output_bytes == 0 {
            self.lease.max_output_bytes
        } else {
            request.max_output_bytes.min(self.lease.max_output_bytes)
        };
        request.timeout = Some(
            request
                .timeout
                .unwrap_or(self.lease.exec_timeout)
                .min(self.lease.exec_timeout),
        );
        if let Some(stdin) = &request.stdin
            && stdin.len() > self.lease.max_input_bytes
        {
            return Err(Error::InvalidInput(
                "stdin exceeds max_input_bytes".to_owned(),
            ));
        }
        validate_exec(&request)?;
        Ok(request)
    }

    /// Read a file relative to the sandbox workspace.
    ///
    /// # Errors
    ///
    /// Returns [`Error::StaleSession`] when the sandbox is no longer current,
    /// and [`Error::NotFound`] when the file does not exist.
    pub async fn read_file(&self, path: &SandboxPath) -> Result<Bytes, Error> {
        self.ensure_alive()?;
        self.gateway.read_file(&self.lease.name, path).await
    }

    /// Write a file relative to the sandbox workspace.
    ///
    /// # Errors
    ///
    /// Returns [`Error::StaleSession`] when the sandbox is no longer current.
    pub async fn write_file(
        &self,
        path: &SandboxPath,
        bytes: impl Into<Bytes>,
    ) -> Result<(), Error> {
        self.write_file_with_request_id(path, bytes, None).await
    }

    /// Write a file with an optional OpenShell durable-admission request ID.
    ///
    /// Use a stable ID for a retried mutating write. The gateway then treats a
    /// second attempt with the same ID as the same execution.
    ///
    /// # Errors
    ///
    /// Returns [`Error::StaleSession`] when the sandbox is no longer current,
    /// or [`Error::InvalidInput`] when the payload exceeds the runtime limit.
    pub async fn write_file_with_request_id(
        &self,
        path: &SandboxPath,
        bytes: impl Into<Bytes>,
        request_id: Option<&str>,
    ) -> Result<(), Error> {
        self.ensure_alive()?;
        let bytes = bytes.into();
        if bytes.len() > self.lease.max_input_bytes {
            return Err(Error::InvalidInput(
                "file write exceeds max_input_bytes".to_owned(),
            ));
        }
        self.gateway
            .write_file(&self.lease.name, path, bytes, request_id)
            .await
    }

    fn ensure_alive(&self) -> Result<(), Error> {
        if self.lease.alive.load(Ordering::SeqCst)
            && !self.lease.shutdown.load(Ordering::SeqCst)
            && self.lease.current_generation.load(Ordering::SeqCst)
                == self.lease.generation.as_u64()
        {
            Ok(())
        } else {
            Err(Error::StaleSession)
        }
    }
}

impl fmt::Debug for Session {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("Session")
            .field("name", &self.lease.name)
            .field("generation", &self.lease.generation)
            .field("alive", &self.lease.alive.load(Ordering::SeqCst))
            .finish()
    }
}

fn validate_exec(request: &ExecRequest) -> Result<(), Error> {
    if request.argv.is_empty() {
        return Err(Error::InvalidInput(
            "command must contain at least one argument".to_owned(),
        ));
    }
    if request.argv.len() > 256 {
        return Err(Error::InvalidInput(
            "command must contain at most 256 arguments".to_owned(),
        ));
    }
    let mut total = 0_usize;
    for argument in &request.argv {
        if argument.len() > 32 * 1024 {
            return Err(Error::InvalidInput(
                "each command argument must be at most 32 KiB".to_owned(),
            ));
        }
        if argument.contains('\0') {
            return Err(Error::InvalidInput(
                "command arguments must not contain NUL".to_owned(),
            ));
        }
        total = total.saturating_add(argument.len());
    }
    if total > 256 * 1024 {
        return Err(Error::InvalidInput(
            "command arguments must total at most 256 KiB".to_owned(),
        ));
    }

    if let Some(workdir) = &request.workdir {
        if workdir.is_empty() || workdir.len() > 4096 {
            return Err(Error::InvalidInput(
                "workdir must contain between 1 and 4096 bytes".to_owned(),
            ));
        }
        if workdir.chars().any(char::is_control) {
            return Err(Error::InvalidInput(
                "workdir must not contain control characters".to_owned(),
            ));
        }
        if std::path::Path::new(workdir)
            .components()
            .any(|component| matches!(component, std::path::Component::ParentDir))
        {
            return Err(Error::InvalidInput(
                "workdir must not contain '..'".to_owned(),
            ));
        }
    }

    for (key, value) in &request.env {
        if key.is_empty() || key.len() > 256 {
            return Err(Error::InvalidInput(
                "environment keys must contain between 1 and 256 bytes".to_owned(),
            ));
        }
        if key
            .chars()
            .any(|character| character.is_control() || character == '=')
        {
            return Err(Error::InvalidInput(
                "environment keys must not contain control characters or '='".to_owned(),
            ));
        }
        if value.len() > 8 * 1024 {
            return Err(Error::InvalidInput(
                "environment values must be at most 8 KiB".to_owned(),
            ));
        }
        if value.chars().any(char::is_control) {
            return Err(Error::InvalidInput(
                "environment values must not contain control characters".to_owned(),
            ));
        }
    }

    Ok(())
}
