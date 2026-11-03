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

/// How the server authenticates callback delivery.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub enum CallbackAuth {
    /// Plain HTTP POST.
    #[default]
    Plain,
    /// AWS SigV4 signing with the default credential chain.
    SigV4 {
        /// Signing region. `None` uses the AWS default region.
        region: Option<String>,
        /// Signing service, usually `lambda`.
        service: String,
    },
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
    /// Callback delivery authentication.
    pub callback_auth: CallbackAuth,
    /// Optional append-only journal path. `None` keeps the journal in memory.
    pub journal_path: Option<PathBuf>,
    /// Allow an in-memory journal.
    ///
    /// Development and tests only. Leave `false` for a production server.
    pub allow_ephemeral: bool,
    /// Allow inbound authentication to be disabled on a non-loopback base URL.
    ///
    /// Development only. Leave `false` for a production server.
    pub allow_unauthenticated: bool,
    /// HTTP timeout for one callback attempt.
    pub callback_timeout: Duration,
    /// Maximum callback delivery attempts.
    pub callback_attempts: usize,
    /// Base backoff between callback attempts.
    pub callback_backoff: Duration,
    /// Allowed callback hosts. Empty allows loopback only.
    pub callback_allowed_hosts: Vec<String>,
    /// Allow non-loopback plain HTTP callback URLs. Development only.
    pub allow_insecure_callback_http: bool,
    /// How often to emit a subscription event while a command streams.
    pub stream_interval: Duration,
    /// Maximum bytes retained for one tool result.
    pub max_output_bytes: usize,
    /// Maximum bytes accepted in one request body.
    pub max_input_bytes: usize,
}

impl ServerConfig {
    /// Construct a configuration with safe defaults.
    pub fn new(base_url: Url, tenant: TenantId) -> Self {
        Self {
            base_url,
            tenant,
            toolset_name: "openshell-sandbox".to_owned(),
            inbound_auth: InboundAuth::None,
            callback_auth: CallbackAuth::Plain,
            journal_path: None,
            allow_ephemeral: false,
            allow_unauthenticated: false,
            callback_timeout: Duration::from_secs(10),
            callback_attempts: 5,
            callback_backoff: Duration::from_millis(250),
            callback_allowed_hosts: Vec::new(),
            allow_insecure_callback_http: false,
            stream_interval: Duration::from_secs(10),
            max_output_bytes: 1024 * 1024,
            max_input_bytes: 8 * 1024 * 1024,
        }
    }

    /// Construct a development configuration.
    ///
    /// It allows an in-memory journal and no inbound authentication. Do not use
    /// it for a production server.
    pub fn ephemeral(base_url: Url, tenant: TenantId) -> Self {
        let mut config = Self::new(base_url, tenant);
        config.allow_ephemeral = true;
        config.allow_unauthenticated = true;
        config
    }

    /// Validate the configuration.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidConfig`] for an unsupported scheme, an empty
    /// name, a zero retry or timeout setting, or an unauthenticated
    /// non-loopback base URL.
    pub fn validate(&self) -> Result<(), Error> {
        if !self.allow_ephemeral
            && !self.allow_unauthenticated
            && matches!(self.inbound_auth, InboundAuth::None)
            && !is_loopback_url(&self.base_url)
        {
            return Err(Error::InvalidConfig(
                "inbound_auth is required for a non-loopback base_url".to_owned(),
            ));
        }
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
        if self.stream_interval.is_zero() {
            return Err(Error::InvalidConfig(
                "stream_interval must be greater than zero".to_owned(),
            ));
        }
        if self.max_output_bytes == 0 {
            return Err(Error::InvalidConfig(
                "max_output_bytes must be greater than zero".to_owned(),
            ));
        }
        if self.max_input_bytes == 0 {
            return Err(Error::InvalidConfig(
                "max_input_bytes must be greater than zero".to_owned(),
            ));
        }
        if let CallbackAuth::SigV4 { service, .. } = &self.callback_auth
            && service.trim().is_empty()
        {
            return Err(Error::InvalidConfig(
                "SigV4 callback service must not be empty".to_owned(),
            ));
        }
        Ok(())
    }

    /// Return the invocation endpoint advertised at discovery.
    pub fn endpoint(&self) -> String {
        format!("{}/invoke", self.base_url.as_str().trim_end_matches('/'))
    }
}

fn is_loopback_url(url: &Url) -> bool {
    match url.host() {
        Some(url::Host::Ipv4(address)) => address.is_loopback(),
        Some(url::Host::Ipv6(address)) => address.is_loopback(),
        Some(url::Host::Domain(domain)) => domain.eq_ignore_ascii_case("localhost"),
        None => false,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> ServerConfig {
        ServerConfig::ephemeral(
            Url::parse("https://tools.example.com").unwrap(),
            TenantId::new("acme").unwrap(),
        )
    }

    #[test]
    fn ephemeral_defaults_are_valid() {
        config().validate().unwrap();
    }

    #[test]
    fn a_non_loopback_server_requires_authentication() {
        let mut config = ServerConfig::new(
            Url::parse("https://tools.example.com").unwrap(),
            TenantId::new("acme").unwrap(),
        );
        assert!(config.validate().is_err());
        config.inbound_auth = InboundAuth::Bearer {
            token: "shared-secret".to_owned(),
        };
        config.validate().unwrap();
    }

    #[test]
    fn a_loopback_server_may_skip_authentication() {
        let config = ServerConfig::new(
            Url::parse("http://127.0.0.1:3001").unwrap(),
            TenantId::new("acme").unwrap(),
        );
        config.validate().unwrap();
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
