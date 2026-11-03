//! Server configuration.

use std::fmt;
use std::path::PathBuf;
use std::time::Duration;

use openshell_session::TenantId;
use url::Url;

use crate::error::Error;

/// How the server authenticates incoming RAP requests.
#[derive(Clone)]
#[non_exhaustive]
pub enum InboundAuth {
    /// No authentication. Only for a trusted network boundary.
    None,
    /// Require `Authorization: Bearer <token>`.
    Bearer {
        /// Shared bearer token.
        token: String,
    },
}

impl fmt::Debug for InboundAuth {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::None => formatter.write_str("None"),
            Self::Bearer { .. } => formatter
                .debug_struct("Bearer")
                .field("token", &"<redacted>")
                .finish(),
        }
    }
}

/// RAP server configuration.
#[derive(Clone, Debug)]
pub struct ServerConfig {
    /// Public base URL of this server. Used to build the discovery endpoint.
    pub base_url: Url,
    /// Tenant used for every session key.
    pub tenant: TenantId,
    /// Toolset name advertised at discovery.
    pub toolset_name: String,
    /// Incoming request authentication.
    pub inbound_auth: InboundAuth,
    /// Optional append-only journal path. `None` keeps the journal in memory.
    pub journal_path: Option<PathBuf>,
    /// HTTP timeout for one callback attempt.
    pub callback_timeout: Duration,
    /// Maximum callback delivery attempts.
    pub callback_attempts: usize,
    /// Base backoff between callback attempts.
    pub callback_backoff: Duration,
    /// Allowed callback hosts. Empty allows any HTTPS host.
    pub callback_allowed_hosts: Vec<String>,
    /// Allow non-loopback plain HTTP callback URLs. Development only.
    pub allow_insecure_callback_http: bool,
}

impl ServerConfig {
    /// Construct a configuration with safe defaults.
    pub fn new(base_url: Url, tenant: TenantId) -> Self {
        Self {
            base_url,
            tenant,
            toolset_name: "openshell-sandbox".to_owned(),
            inbound_auth: InboundAuth::None,
            journal_path: None,
            callback_timeout: Duration::from_secs(10),
            callback_attempts: 5,
            callback_backoff: Duration::from_millis(250),
            callback_allowed_hosts: Vec::new(),
            allow_insecure_callback_http: false,
        }
    }

    /// Validate the configuration.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidConfig`] for an unsupported scheme, an empty
    /// name, or a zero retry or timeout setting.
    pub fn validate(&self) -> Result<(), Error> {
        if !matches!(self.base_url.scheme(), "http" | "https") {
            return Err(Error::InvalidConfig(
                "base_url must use http or https".to_owned(),
            ));
        }
        if self.toolset_name.trim().is_empty() {
            return Err(Error::InvalidConfig(
                "toolset_name must not be empty".to_owned(),
            ));
        }
        if self.callback_timeout.is_zero() {
            return Err(Error::InvalidConfig(
                "callback_timeout must be greater than zero".to_owned(),
            ));
        }
        if self.callback_attempts == 0 {
            return Err(Error::InvalidConfig(
                "callback_attempts must be greater than zero".to_owned(),
            ));
        }
        if self.callback_backoff.is_zero() {
            return Err(Error::InvalidConfig(
                "callback_backoff must be greater than zero".to_owned(),
            ));
        }
        if self
            .callback_allowed_hosts
            .iter()
            .any(|host| host.trim().is_empty() || host.chars().any(char::is_control))
        {
            return Err(Error::InvalidConfig(
                "callback_allowed_hosts must contain non-empty host names".to_owned(),
            ));
        }
        Ok(())
    }

    /// Return the invocation endpoint advertised at discovery.
    pub fn endpoint(&self) -> String {
        format!("{}/invoke", self.base_url.as_str().trim_end_matches('/'))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> ServerConfig {
        ServerConfig::new(
            Url::parse("https://tools.example.com").unwrap(),
            TenantId::new("acme").unwrap(),
        )
    }

    #[test]
    fn defaults_are_valid() {
        config().validate().unwrap();
    }

    #[test]
    fn rejects_zero_retries() {
        let mut config = config();
        config.callback_attempts = 0;
        assert!(config.validate().is_err());
    }

    #[test]
    fn auth_debug_redacts_the_token() {
        let auth = InboundAuth::Bearer {
            token: "secret".to_owned(),
        };
        assert!(!format!("{auth:?}").contains("secret"));
    }
}
