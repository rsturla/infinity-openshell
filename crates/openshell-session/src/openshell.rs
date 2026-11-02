//! OpenShell SDK implementation of [`Gateway`].

use std::collections::{BTreeMap, HashMap};
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use openshell_sdk::{
    AuthConfig, ClientConfig, ClientIdentity, DeleteOptions, ExecOptions, ListOptions,
    OpenShellClient, ResumeOptions, SandboxPhase, SandboxRef, SandboxSpec,
    SandboxTemplateCreateSpec, SdkError,
};

use crate::config::RuntimeConfig;
use crate::error::Error;
use crate::gateway::{CreateSpec, ExecOutput, ExecRequest, Gateway, Phase, SandboxInfo};
use crate::ids::{SandboxName, SandboxPath};

/// OpenShell gateway client.
#[derive(Clone)]
pub struct OpenShellGateway {
    client: OpenShellClient,
    workspace: String,
    workdir: String,
    exec_timeout: Duration,
    control_timeout: Duration,
    max_output_bytes: usize,
}

impl OpenShellGateway {
    /// Connect to the gateway described by `config`.
    ///
    /// # Errors
    ///
    /// Returns a configuration, transport, authentication, or health error.
    pub async fn connect(config: &RuntimeConfig) -> Result<Self, Error> {
        let mut client_config = ClientConfig::new(config.gateway.endpoint.as_str());
        match &config.gateway.auth {
            crate::config::GatewayAuth::None => {}
            crate::config::GatewayAuth::Oidc { token } => {
                client_config.auth = Some(AuthConfig::oidc(token.clone()));
            }
            crate::config::GatewayAuth::Mtls { ca, cert, key } => {
                client_config.ca_cert = Some(ca.clone());
                client_config.client_identity =
                    Some(ClientIdentity::new(cert.clone(), key.clone()));
            }
        }

        let client = OpenShellClient::connect(client_config)
            .await
            .map_err(map_sdk)?;
        let health = tokio::time::timeout(config.lifecycle.ready_timeout, client.health())
            .await
            .map_err(|_| Error::Timeout("gateway health check timed out".to_owned()))?
            .map_err(map_sdk)?;
        tracing::info!(version = %health.version, "connected to OpenShell gateway");

        Ok(Self {
            client,
            workspace: config.gateway.workspace.clone(),
            workdir: config.workdir.clone(),
            exec_timeout: config.lifecycle.exec_timeout,
            control_timeout: config.lifecycle.ready_timeout,
            max_output_bytes: config.lifecycle.max_output_bytes,
        })
    }

    fn workspace(&self) -> openshell_sdk::WorkspaceScopedClient {
        self.client.workspace(&self.workspace)
    }

    async fn control<T>(
        &self,
        future: impl std::future::Future<Output = Result<T, SdkError>>,
    ) -> Result<T, Error> {
        self.control_with(future, map_sdk).await
    }

    async fn control_with<T>(
        &self,
        future: impl std::future::Future<Output = Result<T, SdkError>>,
        map: fn(SdkError) -> Error,
    ) -> Result<T, Error> {
        tokio::time::timeout(self.control_timeout, future)
            .await
            .map_err(|_| Error::Timeout("gateway control request timed out".to_owned()))?
            .map_err(map)
    }
}

#[async_trait]
impl Gateway for OpenShellGateway {
    async fn create(&self, spec: CreateSpec) -> Result<SandboxInfo, Error> {
        let workspace = self.workspace();
        let labels: HashMap<String, String> = spec.labels.into_iter().collect();
        let created = if let Some(template) = spec.template {
            self.control(
                workspace.create_sandbox_from_template(SandboxTemplateCreateSpec {
                    name: Some(spec.name.as_str().to_owned()),
                    template_name: template,
                    labels,
                    command: spec.command,
                    ..Default::default()
                }),
            )
            .await?
        } else {
            self.control(workspace.create_sandbox(SandboxSpec {
                name: Some(spec.name.as_str().to_owned()),
                labels,
                command: spec.command,
                ..Default::default()
            }))
            .await?
        };
        info_from_ref(created)
    }

    async fn get(&self, name: &SandboxName) -> Result<Option<SandboxInfo>, Error> {
        match self
            .control(self.workspace().get_sandbox(name.as_str()))
            .await
        {
            Ok(sandbox) => info_from_ref(sandbox).map(Some),
            Err(Error::NotFound(_)) => Ok(None),
            Err(error) => Err(error),
        }
    }

    async fn list_managed(
        &self,
        labels: &BTreeMap<String, String>,
    ) -> Result<Vec<SandboxInfo>, Error> {
        let selector = labels
            .iter()
            .map(|(key, value)| format!("{key}={value}"))
            .collect::<Vec<_>>()
            .join(",");
        let sandboxes = self
            .control(self.workspace().list_all_sandboxes(ListOptions {
                page_size: 0,
                page_token: String::new(),
                label_selector: Some(selector),
            }))
            .await?;
        sandboxes.into_iter().map(info_from_ref).collect()
    }

    async fn start(&self, name: &SandboxName) -> Result<(), Error> {
        self.control(self.workspace().start_sandbox(name.as_str()))
            .await
            .map(|_| ())
    }

    async fn stop(&self, name: &SandboxName) -> Result<(), Error> {
        self.control(self.workspace().stop_sandbox(name.as_str()))
            .await
            .map(|_| ())
    }

    async fn suspend(&self, name: &SandboxName) -> Result<(), Error> {
        self.control_with(
            self.workspace().suspend_sandbox(name.as_str()),
            map_control_sdk,
        )
        .await
        .map(|_| ())
    }

    async fn resume(&self, name: &SandboxName) -> Result<(), Error> {
        self.control_with(
            self.workspace()
                .resume_sandbox(name.as_str(), ResumeOptions::default()),
            map_control_sdk,
        )
        .await
        .map(|_| ())
    }

    async fn delete(&self, name: &SandboxName) -> Result<(), Error> {
        match self
            .control(self.workspace().delete_sandbox(
                name.as_str(),
                DeleteOptions {
                    allow_missing: true,
                },
            ))
            .await
        {
            Ok(_) | Err(Error::NotFound(_)) => Ok(()),
            Err(error) => Err(error),
        }
    }

    async fn wait_ready(&self, name: &SandboxName, timeout: Duration) -> Result<(), Error> {
        tokio::time::timeout(
            timeout + Duration::from_secs(5),
            self.workspace().wait_ready(name.as_str(), timeout),
        )
        .await
        .map_err(|_| Error::Timeout("wait_ready timed out".to_owned()))?
        .map(|_| ())
        .map_err(map_sdk)
    }

    async fn wait_deleted(&self, name: &SandboxName, timeout: Duration) -> Result<(), Error> {
        match tokio::time::timeout(
            timeout + Duration::from_secs(5),
            self.workspace().wait_deleted(name.as_str(), timeout, None),
        )
        .await
        .map_err(|_| Error::Timeout("wait_deleted timed out".to_owned()))?
        {
            Ok(()) | Err(SdkError::NotFound { .. }) => Ok(()),
            Err(error) => Err(map_sdk(error)),
        }
    }

    async fn exec(&self, name: &SandboxName, request: ExecRequest) -> Result<ExecOutput, Error> {
        let cap = if request.max_output_bytes == 0 {
            self.max_output_bytes
        } else {
            request.max_output_bytes.min(self.max_output_bytes)
        };
        let result = self
            .workspace()
            .exec(
                name.as_str(),
                &request.argv,
                ExecOptions {
                    workdir: Some(request.workdir.unwrap_or_else(|| self.workdir.clone())),
                    environment: request.env.into_iter().collect(),
                    timeout: request.timeout.or(Some(self.exec_timeout)),
                    stdin: request.stdin.map(|bytes| bytes.to_vec()),
                    no_login_shell: true,
                },
            )
            .await
            .map_err(map_exec_sdk)?;

        let stdout_truncated = result.stdout.len() > cap;
        let stderr_truncated = result.stderr.len() > cap;
        let mut stdout = result.stdout;
        let mut stderr = result.stderr;
        stdout.truncate(cap);
        stderr.truncate(cap);

        Ok(ExecOutput {
            code: result.exit_code,
            stdout: Bytes::from(stdout),
            stderr: Bytes::from(stderr),
            truncated: stdout_truncated || stderr_truncated,
        })
    }

    async fn read_file(&self, name: &SandboxName, path: &SandboxPath) -> Result<Bytes, Error> {
        let output = self
            .exec(
                name,
                ExecRequest {
                    argv: vec![
                        "sh".to_owned(),
                        "-c".to_owned(),
                        "cat -- \"$1\"".to_owned(),
                        "sh".to_owned(),
                        path.as_str().to_owned(),
                    ],
                    workdir: Some(self.workdir.clone()),
                    env: BTreeMap::new(),
                    stdin: None,
                    timeout: Some(self.exec_timeout),
                    max_output_bytes: self.max_output_bytes,
                },
            )
            .await?;
        if output.code != 0 {
            let stderr = String::from_utf8_lossy(&output.stderr);
            if stderr.contains("No such file") || stderr.contains("cannot open") {
                return Err(Error::NotFound(path.to_string()));
            }
            return Err(Error::Gateway(format!("read failed: {stderr}")));
        }
        if output.truncated {
            return Err(Error::Gateway(format!(
                "file '{path}' exceeds max_output_bytes"
            )));
        }
        Ok(output.stdout)
    }

    async fn write_file(
        &self,
        name: &SandboxName,
        path: &SandboxPath,
        bytes: Bytes,
    ) -> Result<(), Error> {
        let output = self
            .exec(
                name,
                ExecRequest {
                    argv: vec![
                        "sh".to_owned(),
                        "-c".to_owned(),
                        "mkdir -p -- \"$(dirname -- \"$1\")\" && cat > \"$1\"".to_owned(),
                        "sh".to_owned(),
                        path.as_str().to_owned(),
                    ],
                    workdir: Some(self.workdir.clone()),
                    env: BTreeMap::new(),
                    stdin: Some(bytes),
                    timeout: Some(self.exec_timeout),
                    max_output_bytes: self.max_output_bytes,
                },
            )
            .await?;
        if output.code != 0 {
            let stderr = String::from_utf8_lossy(&output.stderr);
            return Err(Error::Gateway(format!("write failed: {stderr}")));
        }
        Ok(())
    }
}

fn info_from_ref(sandbox: SandboxRef) -> Result<SandboxInfo, Error> {
    let name = SandboxName::new(sandbox.name).map_err(|error| Error::Gateway(error.to_string()))?;
    Ok(SandboxInfo {
        name,
        phase: map_phase(sandbox.phase),
        labels: sandbox.labels.into_iter().collect(),
        created_at_ms: sandbox.main_process_started_at_ms,
        exit_code: sandbox.exit_code,
    })
}

fn map_phase(phase: SandboxPhase) -> Phase {
    match phase {
        SandboxPhase::Unspecified => Phase::Unknown,
        SandboxPhase::Provisioning => Phase::Provisioning,
        SandboxPhase::Ready => Phase::Ready,
        SandboxPhase::Error => Phase::Error,
        SandboxPhase::Deleting => Phase::Deleting,
        SandboxPhase::Unknown => Phase::Unknown,
        SandboxPhase::Stopping => Phase::Stopping,
        SandboxPhase::Stopped => Phase::Stopped,
        SandboxPhase::Starting => Phase::Starting,
        SandboxPhase::Completed => Phase::Completed,
        SandboxPhase::Suspending => Phase::Stopping,
        SandboxPhase::Suspended => Phase::Suspended,
        _ => Phase::Unknown,
    }
}

fn map_exec_sdk(error: SdkError) -> Error {
    match error {
        SdkError::InvalidConfig { message, .. } => Error::InvalidInput(message),
        other => map_sdk(other),
    }
}

fn map_control_sdk(error: SdkError) -> Error {
    match error {
        // gRPC FAILED_PRECONDITION / UNIMPLEMENTED mean the driver does not
        // support state retention; fall back to stop/start.
        SdkError::Rpc { code, message, .. } if code == 9 || code == 12 => {
            Error::Unsupported(message)
        }
        other => map_sdk(other),
    }
}

fn map_sdk(error: SdkError) -> Error {
    match error {
        SdkError::NotFound { message, .. } => Error::NotFound(message),
        SdkError::AlreadyExists { message, .. } => Error::Conflict(message),
        SdkError::Connect { message } => Error::Unavailable(message),
        SdkError::Auth {
            message, retryable, ..
        } => {
            if retryable {
                Error::Unavailable(message)
            } else {
                Error::Gateway(message)
            }
        }
        SdkError::InvalidConfig { message, .. } => Error::InvalidConfig(message),
        SdkError::Tls { message } => Error::Gateway(message),
        SdkError::Io { source } => Error::Gateway(source.to_string()),
        SdkError::Rpc { message, .. } => Error::Gateway(message),
        SdkError::OutOfRange { message, .. } => Error::Gateway(message),
    }
}
