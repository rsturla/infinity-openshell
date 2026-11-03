//! OpenShell SDK implementation of [`Gateway`].

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use bytes::Bytes;
use futures_util::StreamExt;
use openshell_sdk::raw::proto;
use openshell_sdk::raw::proto::ExecSandboxRequest;
use openshell_sdk::{
    AuthConfig, ClientConfig, ClientIdentity, DeleteOptions, ListOptions, OpenShellClient,
    ResumeOptions, SandboxPhase, SandboxRef, SandboxSpec, SandboxTemplateCreateSpec, SdkError,
};

use crate::config::RuntimeConfig;
use crate::error::Error;
use crate::gateway::{CreateSpec, ExecEvent, ExecRequest, ExecStream, Gateway, Phase, SandboxInfo};
use crate::ids::{SandboxName, SandboxPath};

/// OpenShell gateway client.
#[derive(Clone)]
pub struct OpenShellGateway {
    client: OpenShellClient,
    workspace: String,
    workdir: Option<String>,
    policy: Option<proto::SandboxPolicy>,
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
        let client = connect_client(config).await?;
        let policy =
            match &config.policy {
                Some(yaml) => Some(openshell_policy::parse_sandbox_policy(yaml).map_err(
                    |error| Error::InvalidConfig(format!("invalid sandbox policy: {error}")),
                )?),
                None => None,
            };

        Ok(Self {
            client,
            workspace: config.gateway.workspace.clone(),
            workdir: config.workdir.clone(),
            policy,
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

    async fn create_raw(
        &self,
        name: SandboxName,
        template_name: Option<String>,
        image: Option<String>,
        providers: Vec<String>,
        labels: HashMap<String, String>,
        command: Vec<String>,
    ) -> Result<SandboxInfo, Error> {
        let template = image.map(|image| proto::SandboxTemplate {
            image,
            ..proto::SandboxTemplate::default()
        });
        let request = proto::CreateSandboxRequest {
            request_id: String::new(),
            spec: Some(proto::SandboxSpec {
                template,
                providers,
                policy: self.policy.clone(),
                command,
                ..proto::SandboxSpec::default()
            }),
            name: name.as_str().to_owned(),
            labels,
            annotations: HashMap::new(),
            workspace_scope: Some(proto::workspace_selector(&self.workspace)),
            await_main_process_attachment: false,
            workload_template: template_name.unwrap_or_default(),
            service_exposures: Vec::new(),
        };
        let mut grpc = self.client.raw_grpc_fresh().await.map_err(map_sdk)?;
        let response = self
            .control_with(
                async move {
                    grpc.create_sandbox(request)
                        .await
                        .map_err(SdkError::from_status)
                },
                map_sdk,
            )
            .await?;
        let sandbox = response.into_inner().sandbox.ok_or_else(|| {
            Error::Gateway("gateway response did not include a sandbox".to_owned())
        })?;
        info_from_proto(sandbox)
    }
}

#[async_trait]
impl Gateway for OpenShellGateway {
    async fn create(&self, spec: CreateSpec) -> Result<SandboxInfo, Error> {
        let CreateSpec {
            name,
            template,
            image,
            providers,
            labels,
            command,
        } = spec;
        let workspace = self.workspace();
        let labels: HashMap<String, String> = labels.into_iter().collect();
        let created = if image.is_some() || self.policy.is_some() {
            self.create_raw(name, template, image, providers, labels, command)
                .await?
        } else if let Some(template) = template {
            info_from_ref(
                self.control(
                    workspace.create_sandbox_from_template(SandboxTemplateCreateSpec {
                        name: Some(name.as_str().to_owned()),
                        template_name: template,
                        labels,
                        providers,
                        command,
                        ..Default::default()
                    }),
                )
                .await?,
            )?
        } else {
            info_from_ref(
                self.control(workspace.create_sandbox(SandboxSpec {
                    name: Some(name.as_str().to_owned()),
                    labels,
                    providers,
                    command,
                    ..Default::default()
                }))
                .await?,
            )?
        };
        Ok(created)
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

    async fn wait_policy(&self, name: &SandboxName, timeout: Duration) -> Result<(), Error> {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            let mut grpc = self.client.raw_grpc_fresh().await.map_err(map_sdk)?;
            let response = grpc
                .get_sandbox_policy_status(proto::GetSandboxPolicyStatusRequest {
                    workspace_scope: Some(proto::workspace_selector(&self.workspace)),
                    version: 0,
                    global: false,
                    sandbox: name.as_str().to_owned(),
                })
                .await
                .map_err(|status| map_sdk(SdkError::from_status(status)))?
                .into_inner();
            match response.revision {
                Some(revision) => match proto::PolicyStatus::try_from(revision.status) {
                    Ok(proto::PolicyStatus::Loaded) => return Ok(()),
                    Ok(proto::PolicyStatus::Failed) => {
                        return Err(Error::InvalidConfig(format!(
                            "sandbox policy failed to load: {}",
                            revision.load_error
                        )));
                    }
                    _ => {}
                },
                None if response.active_version > 0 => return Ok(()),
                None => {}
            }
            if std::time::Instant::now() >= deadline {
                return Err(Error::Timeout(
                    "sandbox policy did not load before the deadline".to_owned(),
                ));
            }
            tokio::time::sleep(Duration::from_millis(250)).await;
        }
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

    async fn exec_stream(
        &self,
        name: &SandboxName,
        request: ExecRequest,
    ) -> Result<ExecStream, Error> {
        let timeout = request
            .timeout
            .or(Some(self.exec_timeout))
            .unwrap_or_default();
        let execution_timeout = prost_types::Duration {
            seconds: i64::try_from(timeout.as_secs()).unwrap_or(i64::MAX),
            nanos: timeout.subsec_nanos() as i32,
        };

        let mut grpc = self.client.raw_grpc_fresh().await.map_err(map_exec_sdk)?;
        let response = grpc
            .exec_sandbox(ExecSandboxRequest {
                workspace_scope: Some(proto::workspace_selector(&self.workspace)),
                sandbox: name.as_str().to_owned(),
                command: request.argv,
                workdir: request
                    .workdir
                    .or_else(|| self.workdir.clone())
                    .unwrap_or_default(),
                environment: request.env.into_iter().collect(),
                execution_timeout: Some(execution_timeout),
                stdin: request
                    .stdin
                    .map(|bytes| bytes.to_vec())
                    .unwrap_or_default(),
                tty: false,
                cols: 0,
                rows: 0,
                no_login_shell: true,
                request_id: request.request_id.unwrap_or_default(),
            })
            .await
            .map_err(|status| map_exec_sdk(SdkError::from_status(status)))?;

        let stream = response.into_inner().filter_map(|item| async move {
            match item {
                Err(status) => Some(Err(map_exec_sdk(SdkError::from_status(status)))),
                Ok(event) => match event.payload {
                    Some(proto::exec_sandbox_event::Payload::Stdout(chunk)) => {
                        Some(Ok(ExecEvent::Stdout(Bytes::from(chunk.data))))
                    }
                    Some(proto::exec_sandbox_event::Payload::Stderr(chunk)) => {
                        Some(Ok(ExecEvent::Stderr(Bytes::from(chunk.data))))
                    }
                    Some(proto::exec_sandbox_event::Payload::Exit(exit)) => {
                        Some(Ok(ExecEvent::Exited {
                            code: exit.exit_code,
                        }))
                    }
                    None => None,
                },
            }
        });
        Ok(Box::pin(stream))
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
                    workdir: self.workdir.clone(),
                    env: BTreeMap::new(),
                    stdin: None,
                    timeout: Some(self.exec_timeout),
                    max_output_bytes: self.max_output_bytes,
                    request_id: None,
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
        request_id: Option<&str>,
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
                    workdir: self.workdir.clone(),
                    env: BTreeMap::new(),
                    stdin: Some(bytes),
                    timeout: Some(self.exec_timeout),
                    max_output_bytes: self.max_output_bytes,
                    request_id: request_id.map(str::to_owned),
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

pub(crate) async fn connect_client(config: &RuntimeConfig) -> Result<OpenShellClient, Error> {
    let mut client_config = ClientConfig::new(config.gateway.endpoint.as_str());
    match &config.gateway.auth {
        crate::config::GatewayAuth::None => {}
        crate::config::GatewayAuth::Oidc { token } => {
            client_config.auth = Some(AuthConfig::oidc(token.clone()));
        }
        crate::config::GatewayAuth::OidcSource { source } => {
            let token = source.token().await?;
            client_config.auth = Some(AuthConfig::Oidc {
                token,
                expires_at: None,
                refresh: Some(Arc::new(SourceRefresh {
                    source: source.clone(),
                })),
            });
        }
        crate::config::GatewayAuth::Mtls { ca, cert, key } => {
            client_config.ca_cert = Some(ca.clone());
            client_config.client_identity = Some(ClientIdentity::new(cert.clone(), key.clone()));
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
    Ok(client)
}

struct SourceRefresh {
    source: Arc<dyn crate::token::TokenSource>,
}

#[async_trait]
impl openshell_sdk::Refresh for SourceRefresh {
    async fn refresh(&self) -> Result<openshell_sdk::RefreshedToken, openshell_sdk::RefreshError> {
        let token = self
            .source
            .token()
            .await
            .map_err(|error| openshell_sdk::RefreshError::Transient(error.to_string()))?;
        Ok(openshell_sdk::RefreshedToken::new(token))
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

fn info_from_proto(sandbox: proto::Sandbox) -> Result<SandboxInfo, Error> {
    let metadata = sandbox.metadata.unwrap_or_default();
    let status = sandbox.status.unwrap_or_default();
    let name =
        SandboxName::new(metadata.name).map_err(|error| Error::Gateway(error.to_string()))?;
    let phase =
        proto::SandboxPhase::try_from(status.phase).map_or(Phase::Unknown, |phase| match phase {
            proto::SandboxPhase::Unspecified | proto::SandboxPhase::Unknown => Phase::Unknown,
            proto::SandboxPhase::Provisioning => Phase::Provisioning,
            proto::SandboxPhase::Ready => Phase::Ready,
            proto::SandboxPhase::Error => Phase::Error,
            proto::SandboxPhase::Deleting => Phase::Deleting,
            proto::SandboxPhase::Stopping | proto::SandboxPhase::Suspending => Phase::Stopping,
            proto::SandboxPhase::Stopped => Phase::Stopped,
            proto::SandboxPhase::Starting => Phase::Starting,
            proto::SandboxPhase::Completed => Phase::Completed,
            proto::SandboxPhase::Suspended => Phase::Suspended,
        });
    Ok(SandboxInfo {
        name,
        phase,
        labels: metadata.labels.into_iter().collect(),
        created_at_ms: status.main_process_started_time.and_then(timestamp_millis),
        exit_code: status.exit_code,
    })
}

fn timestamp_millis(timestamp: prost_types::Timestamp) -> Option<i64> {
    timestamp
        .seconds
        .checked_mul(1000)?
        .checked_add(i64::from(timestamp.nanos / 1_000_000))
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
    let reason = error
        .error_details()
        .and_then(|details| details.error_info().map(|info| info.reason.clone()));
    let uncertain = reason.is_some_and(|reason| {
        matches!(
            reason.as_str(),
            "REQUEST_OUTCOME_UNCERTAIN"
                | "REQUEST_STREAM_UNAVAILABLE"
                | "REQUEST_ID_PAYLOAD_MISMATCH"
        )
    });
    if uncertain {
        return Error::Uncertain(error.to_string());
    }
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

pub(crate) fn map_sdk(error: SdkError) -> Error {
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
