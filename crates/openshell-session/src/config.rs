//! Runtime configuration.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;
use std::time::Duration;

use url::Url;

use crate::error::Error;
use crate::token::TokenSource;

/// How a session releases compute when it goes idle.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum WakeStrategy {
    /// Stop the sandbox and start it again on the next call. Portable across
    /// OpenShell drivers.
    #[default]
    StopStart,
    /// Suspend the sandbox and resume it later. Falls back to stop/start when
    /// the driver does not support state retention.
    SuspendResume,
    /// Keep the sandbox ready until it is deleted. Intended for interactive
    /// workloads with a short idle window.
    KeepWarm,
}

/// Gateway authentication.
///
/// `Debug` never prints token or certificate material.
#[derive(Clone)]
#[non_exhaustive]
pub enum GatewayAuth {
    /// Plaintext or anonymous TLS. Only for trusted development gateways.
    None,
    /// OIDC bearer token.
    Oidc {
        /// Access token.
        token: String,
    },
    /// OIDC bearer token from a refreshable source.
    OidcSource {
        /// Token source. The SDK refreshes it when the gateway rejects the
        /// current token.
        source: Arc<dyn TokenSource>,
    },
    /// Mutual TLS with PEM-encoded material.
    Mtls {
        /// PEM-encoded CA certificate.
        ca: Vec<u8>,
        /// PEM-encoded client certificate chain.
        cert: Vec<u8>,
        /// PEM-encoded private key.
        key: Vec<u8>,
    },
}

impl fmt::Debug for GatewayAuth {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => formatter.write_str("None"),
            Self::Oidc { .. } => formatter
                .debug_struct("Oidc")
                .field("token", &"<redacted>")
                .finish(),
            Self::OidcSource { .. } => formatter
                .debug_struct("OidcSource")
                .field("source", &"<redacted>")
                .finish(),
            Self::Mtls { .. } => formatter
                .debug_struct("Mtls")
                .field("material", &"<redacted>")
                .finish(),
        }
    }
}

/// Connection settings for one OpenShell gateway workspace.
#[derive(Clone, Debug)]
pub struct GatewayConfig {
    /// Gateway endpoint, for example `https://gateway.example.com:443`.
    pub endpoint: Url,
    /// OpenShell workspace name.
    pub workspace: String,
    /// Authentication mode.
    pub auth: GatewayAuth,
}

impl GatewayConfig {
    /// Construct a gateway configuration.
    pub fn new(endpoint: Url, workspace: impl Into<String>, auth: GatewayAuth) -> Self {
        Self {
            endpoint,
            workspace: workspace.into(),
            auth,
        }
    }
}

/// Which workload templates callers may use.
#[derive(Clone, Debug, Default)]
pub struct TemplatePolicy {
    /// Default template. `None` uses the gateway's default image.
    pub default: Option<String>,
    /// Optional allowlist. When non-empty, `default` must appear in it.
    pub allowed: Vec<String>,
}

impl TemplatePolicy {
    /// Use the gateway's default image.
    pub fn default_image() -> Self {
        Self::default()
    }

    /// Use one named template.
    pub fn named(template: impl Into<String>) -> Self {
        Self {
            default: Some(template.into()),
            allowed: Vec::new(),
        }
    }
}

/// Timing and output limits for session lifecycle and execution.
#[derive(Clone, Debug)]
pub struct LifecyclePolicy {
    /// How to release compute when the last lease is released.
    pub wake: WakeStrategy,
    /// Time between the last release and going idle.
    pub idle_after: Duration,
    /// Time a stopped sandbox is retained before deletion.
    pub retain_for: Duration,
    /// Timeout for a sandbox to become ready.
    pub ready_timeout: Duration,
    /// Timeout for a sandbox to stop.
    pub stop_timeout: Duration,
    /// Timeout for a sandbox to be deleted.
    pub delete_timeout: Duration,
    /// Default timeout for one command.
    pub exec_timeout: Duration,
    /// Maximum bytes captured from stdout or stderr.
    pub max_output_bytes: usize,
    /// Maximum bytes accepted as stdin or as one file write.
    pub max_input_bytes: usize,
    /// Maximum concurrent commands per sandbox.
    pub max_concurrent_execs: usize,
}

impl Default for LifecyclePolicy {
    fn default() -> Self {
        Self {
            wake: WakeStrategy::StopStart,
            idle_after: Duration::from_secs(60),
            retain_for: Duration::from_secs(24 * 60 * 60),
            ready_timeout: Duration::from_secs(120),
            stop_timeout: Duration::from_secs(60),
            delete_timeout: Duration::from_secs(60),
            exec_timeout: Duration::from_secs(300),
            max_output_bytes: 1024 * 1024,
            max_input_bytes: 8 * 1024 * 1024,
            max_concurrent_execs: 4,
        }
    }
}

/// Process-wide limits.
#[derive(Clone, Copy, Debug)]
pub struct Limits {
    /// Maximum number of live sessions tracked by one runtime.
    pub max_sessions: usize,
}

impl Default for Limits {
    fn default() -> Self {
        Self {
            max_sessions: 10_000,
        }
    }
}

/// Configuration for a [`crate::SessionRuntime`].
#[derive(Clone, Debug)]
pub struct RuntimeConfig {
    /// Gateway connection.
    pub gateway: GatewayConfig,
    /// Allowed workload templates.
    pub template: TemplatePolicy,
    /// Lifecycle and execution limits.
    pub lifecycle: LifecyclePolicy,
    /// Process-wide limits.
    pub limits: Limits,
    /// Value for the `app.kubernetes.io/managed-by` label.
    pub managed_by: String,
    /// Prefix for the runtime, key, tenant, thread, and generation labels.
    ///
    /// The core defaults to `openshell-session`. A product layer can set its
    /// own prefix; the Infinity RAP adapter uses `infinity.hydro.run`.
    pub label_prefix: String,
    /// Additional labels added to every sandbox this runtime creates.
    pub labels: BTreeMap<String, String>,
    /// Working directory inside the sandbox.
    pub workdir: String,
}

impl RuntimeConfig {
    /// Construct a configuration with default lifecycle and limits.
    pub fn new(gateway: GatewayConfig) -> Self {
        Self {
            gateway,
            template: TemplatePolicy::default(),
            lifecycle: LifecyclePolicy::default(),
            limits: Limits::default(),
            managed_by: "openshell-session".to_owned(),
            label_prefix: "openshell-session".to_owned(),
            labels: BTreeMap::new(),
            workdir: "/sandbox".to_owned(),
        }
    }

    /// Validate the configuration before the runtime starts.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidConfig`] for an unsupported scheme, an empty
    /// workspace or workdir, inconsistent durations, or a zero limit.
    pub fn validate(&self) -> Result<(), Error> {
        if !matches!(self.gateway.endpoint.scheme(), "http" | "https") {
            return Err(Error::InvalidConfig(
                "gateway endpoint must use http or https".to_owned(),
            ));
        }
        if self.gateway.workspace.trim().is_empty() {
            return Err(Error::InvalidConfig(
                "gateway workspace must not be empty".to_owned(),
            ));
        }
        if self.workdir.trim().is_empty() {
            return Err(Error::InvalidConfig("workdir must not be empty".to_owned()));
        }
        if !self.workdir.starts_with('/') {
            return Err(Error::InvalidConfig(
                "workdir must be an absolute path inside the sandbox".to_owned(),
            ));
        }
        if self.lifecycle.idle_after.is_zero() {
            return Err(Error::InvalidConfig(
                "idle_after must be greater than zero".to_owned(),
            ));
        }
        if self.lifecycle.retain_for <= self.lifecycle.idle_after {
            return Err(Error::InvalidConfig(
                "retain_for must be greater than idle_after".to_owned(),
            ));
        }
        for (name, value) in [
            ("ready_timeout", self.lifecycle.ready_timeout),
            ("stop_timeout", self.lifecycle.stop_timeout),
            ("delete_timeout", self.lifecycle.delete_timeout),
            ("exec_timeout", self.lifecycle.exec_timeout),
        ] {
            if value.is_zero() {
                return Err(Error::InvalidConfig(format!(
                    "{name} must be greater than zero"
                )));
            }
        }
        if self.lifecycle.max_output_bytes == 0 {
            return Err(Error::InvalidConfig(
                "max_output_bytes must be greater than zero".to_owned(),
            ));
        }
        if self.lifecycle.max_input_bytes == 0 {
            return Err(Error::InvalidConfig(
                "max_input_bytes must be greater than zero".to_owned(),
            ));
        }
        if self.lifecycle.max_concurrent_execs == 0 {
            return Err(Error::InvalidConfig(
                "max_concurrent_execs must be greater than zero".to_owned(),
            ));
        }
        if self.limits.max_sessions == 0 {
            return Err(Error::InvalidConfig(
                "max_sessions must be greater than zero".to_owned(),
            ));
        }
        if let Some(default) = &self.template.default
            && !self.template.allowed.is_empty()
            && !self.template.allowed.contains(default)
        {
            return Err(Error::InvalidConfig(format!(
                "template '{default}' is not in the allowlist"
            )));
        }
        if self.managed_by.trim().is_empty()
            || self.managed_by.len() > 63
            || self.managed_by.chars().any(char::is_control)
        {
            return Err(Error::InvalidConfig(
                "managed_by must be 1 to 63 bytes without control characters".to_owned(),
            ));
        }
        if self.label_prefix.trim().is_empty()
            || self.label_prefix.len() > 253
            || self.label_prefix.contains('/')
            || self.label_prefix.chars().any(char::is_whitespace)
        {
            return Err(Error::InvalidConfig(
                "label_prefix must be a DNS subdomain without whitespace or '/'".to_owned(),
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn gateway() -> GatewayConfig {
        GatewayConfig::new(
            Url::parse("https://gateway.example.com").unwrap(),
            "default",
            GatewayAuth::None,
        )
    }

    #[test]
    fn default_config_is_valid() {
        RuntimeConfig::new(gateway()).validate().unwrap();
    }

    #[test]
    fn rejects_non_http_scheme() {
        let mut config = RuntimeConfig::new(GatewayConfig::new(
            Url::parse("ftp://gateway.example.com").unwrap(),
            "default",
            GatewayAuth::None,
        ));
        assert!(config.validate().is_err());
        config.gateway.endpoint = Url::parse("https://gateway.example.com").unwrap();
        config.validate().unwrap();
    }

    #[test]
    fn rejects_retain_shorter_than_idle() {
        let mut config = RuntimeConfig::new(gateway());
        config.lifecycle.idle_after = Duration::from_secs(100);
        config.lifecycle.retain_for = Duration::from_secs(50);
        assert!(config.validate().is_err());
    }

    #[test]
    fn rejects_over_budget_output() {
        let mut config = RuntimeConfig::new(gateway());
        config.lifecycle.max_output_bytes = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn auth_debug_redacts_secrets() {
        let auth = GatewayAuth::Oidc {
            token: "super-secret".to_owned(),
        };
        let debug = format!("{auth:?}");
        assert!(!debug.contains("super-secret"));
    }
}
