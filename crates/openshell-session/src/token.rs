//! Token sources for long-lived gateway connections.
//!
//! A static token is fine for a short-lived process. A long-lived agent needs
//! to refresh OIDC credentials and persist the rotated refresh token, or the
//! identity provider will reject the old one.

use std::fmt;
use std::sync::Arc;
#[cfg(feature = "bootstrap")]
use std::time::{SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use openshell_sdk::oidc::{RefreshTokenInput, refresh_token};
use tokio::sync::Mutex;

use crate::error::Error;

/// Supplies a bearer token when the gateway needs one.
#[async_trait]
pub trait TokenSource: Send + Sync + 'static {
    /// Return a currently valid access token.
    async fn token(&self) -> Result<String, Error>;
}

/// A token that never changes.
#[derive(Clone)]
pub struct StaticToken {
    token: String,
}

impl StaticToken {
    /// Construct a static token.
    pub fn new(token: impl Into<String>) -> Self {
        Self {
            token: token.into(),
        }
    }
}

impl fmt::Debug for StaticToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("StaticToken(<redacted>)")
    }
}

#[async_trait]
impl TokenSource for StaticToken {
    async fn token(&self) -> Result<String, Error> {
        Ok(self.token.clone())
    }
}

/// OIDC refresh credentials.
#[derive(Clone)]
pub struct OidcCredentials {
    issuer: String,
    client_id: String,
    refresh_token: String,
}

impl OidcCredentials {
    /// Construct refresh credentials.
    pub fn new(
        issuer: impl Into<String>,
        client_id: impl Into<String>,
        refresh_token: impl Into<String>,
    ) -> Self {
        Self {
            issuer: issuer.into(),
            client_id: client_id.into(),
            refresh_token: refresh_token.into(),
        }
    }

    /// Return the issuer.
    pub fn issuer(&self) -> &str {
        &self.issuer
    }

    /// Return the client ID.
    pub fn client_id(&self) -> &str {
        &self.client_id
    }

    /// Return the refresh token.
    pub fn refresh_token(&self) -> &str {
        &self.refresh_token
    }
}

impl fmt::Debug for OidcCredentials {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OidcCredentials")
            .field("issuer", &self.issuer)
            .field("client_id", &self.client_id)
            .field("refresh_token", &"<redacted>")
            .finish()
    }
}

/// Persists a rotated refresh token after a successful refresh.
#[async_trait]
pub trait RefreshTokenStore: Send + Sync + 'static {
    /// Store the latest credentials.
    async fn store(&self, credentials: &OidcCredentials) -> Result<(), Error>;
}

/// Refreshes an OIDC access token and persists the rotated refresh token.
pub struct OidcRefreshToken {
    credentials: Arc<Mutex<OidcCredentials>>,
    store: Option<Arc<dyn RefreshTokenStore>>,
}

impl OidcRefreshToken {
    /// Construct a source from refresh credentials.
    pub fn new(credentials: OidcCredentials) -> Self {
        Self {
            credentials: Arc::new(Mutex::new(credentials)),
            store: None,
        }
    }

    /// Attach a store for rotated refresh tokens.
    pub fn with_store(mut self, store: Arc<dyn RefreshTokenStore>) -> Self {
        self.store = Some(store);
        self
    }
}

impl fmt::Debug for OidcRefreshToken {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("OidcRefreshToken")
            .field("store", &self.store.is_some())
            .finish()
    }
}

#[async_trait]
impl TokenSource for OidcRefreshToken {
    async fn token(&self) -> Result<String, Error> {
        // Hold the lock across the refresh and persistence. Refresh tokens
        // rotate, so two concurrent refreshes with the same token would make
        // one fail and can revoke the whole token family.
        let mut credentials = self.credentials.lock().await;
        let input = RefreshTokenInput::new(
            &credentials.refresh_token,
            &credentials.issuer,
            &credentials.client_id,
        );
        let refreshed = refresh_token(&input)
            .await
            .map_err(|error| Error::Unavailable(format!("OIDC refresh failed: {error}")))?;

        if let Some(next) = refreshed.refresh_token {
            credentials.refresh_token = next;
        }
        if let Some(store) = &self.store {
            store.store(&credentials).await?;
        }
        Ok(refreshed.access_token)
    }
}

/// Reads and refreshes the OpenShell CLI token cache.
///
/// This is for local development and live tests only. It keeps the CLI's
/// rotated refresh token, so a refresh does not invalidate the CLI session.
#[cfg(feature = "bootstrap")]
pub struct BootstrapTokenSource {
    gateway: String,
    bundle: Arc<Mutex<openshell_bootstrap::oidc_token::OidcTokenBundle>>,
}

#[cfg(feature = "bootstrap")]
impl BootstrapTokenSource {
    /// Load the token for a registered gateway.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Gateway`] when the gateway has no cached token.
    pub fn new(gateway: impl Into<String>) -> Result<Self, Error> {
        let gateway = gateway.into();
        let bundle =
            openshell_bootstrap::oidc_token::load_oidc_token(&gateway).ok_or_else(|| {
                Error::Gateway(format!(
                    "gateway '{gateway}' has no OIDC token; run `openshell gateway login`"
                ))
            })?;
        Ok(Self {
            gateway,
            bundle: Arc::new(Mutex::new(bundle)),
        })
    }

    /// Return the registered gateway endpoint.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Gateway`] when the metadata cannot be read or parsed.
    pub fn endpoint(&self) -> Result<url::Url, Error> {
        let metadata = openshell_bootstrap::load_gateway_metadata(&self.gateway)
            .map_err(|error| Error::Gateway(format!("cannot load gateway metadata: {error}")))?;
        metadata.gateway_endpoint.parse().map_err(|error| {
            Error::Gateway(format!("gateway endpoint is not a valid URL: {error}"))
        })
    }
}

#[cfg(feature = "bootstrap")]
impl fmt::Debug for BootstrapTokenSource {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("BootstrapTokenSource")
            .field("gateway", &self.gateway)
            .finish()
    }
}

#[cfg(feature = "bootstrap")]
#[async_trait]
impl TokenSource for BootstrapTokenSource {
    async fn token(&self) -> Result<String, Error> {
        let mut bundle = self.bundle.lock().await;
        let now = now_seconds();
        if bundle.expires_at.is_none_or(|expiry| expiry > now + 60) {
            return Ok(bundle.access_token.clone());
        }

        let refresh = bundle.refresh_token.clone().ok_or_else(|| {
            Error::Gateway(format!(
                "OIDC token for gateway '{}' expired and has no refresh token",
                self.gateway
            ))
        })?;
        let input = RefreshTokenInput::new(&refresh, &bundle.issuer, &bundle.client_id);
        let refreshed = refresh_token(&input)
            .await
            .map_err(|error| Error::Unavailable(format!("OIDC refresh failed: {error}")))?;

        bundle.access_token = refreshed.access_token;
        if let Some(next) = refreshed.refresh_token {
            bundle.refresh_token = Some(next);
        }
        if let Some(expiry) = refreshed.expires_at {
            bundle.expires_at = Some(expiry);
        }
        if let Err(error) =
            openshell_bootstrap::oidc_token::store_oidc_token(&self.gateway, &bundle)
        {
            tracing::warn!(%error, "cannot persist the rotated OIDC token");
        }
        Ok(bundle.access_token.clone())
    }
}

#[cfg(feature = "bootstrap")]
fn now_seconds() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn static_token_returns_its_value() {
        let source = StaticToken::new("token");
        assert_eq!(source.token().await.unwrap(), "token");
    }

    #[test]
    fn credentials_debug_redacts_the_refresh_token() {
        let credentials = OidcCredentials::new("issuer", "client", "secret");
        let debug = format!("{credentials:?}");
        assert!(!debug.contains("secret"));
    }
}
