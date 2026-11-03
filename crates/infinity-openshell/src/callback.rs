//! RAP callback delivery.

use async_trait::async_trait;
use rap_protocol::RapCallback;
use std::time::Duration;

/// Callback delivery error.
#[derive(Debug, thiserror::Error)]
pub enum CallbackError {
    /// The callback URL is invalid or not allowed.
    #[error("callback URL is invalid: {0}")]
    InvalidUrl(String),
    /// The request failed in transport.
    #[error("callback request failed: {0}")]
    Transport(String),
    /// The receiver returned a non-success status.
    #[error("callback returned HTTP {0}")]
    Rejected(u16),
}

/// Delivers RAP callback messages.
#[async_trait]
pub trait CallbackClient: Send + Sync + 'static {
    /// POST a callback message.
    async fn post(&self, url: &str, callback: &RapCallback) -> Result<(), CallbackError>;
}

/// Plain HTTP callback client with URL policy.
#[derive(Clone)]
pub struct PlainCallbackClient {
    http: reqwest::Client,
    allowed_hosts: Vec<String>,
    allow_insecure_http: bool,
}

impl PlainCallbackClient {
    /// Build a client with a per-request timeout and URL policy.
    ///
    /// Loopback HTTP callbacks are always allowed for local development. Any
    /// other HTTP callback requires `allow_insecure_http`. When
    /// `allowed_hosts` is non-empty, the callback host must match one entry.
    ///
    /// # Errors
    ///
    /// Returns [`CallbackError::Transport`] when the HTTP client cannot be
    /// constructed.
    pub fn new(
        timeout: Duration,
        allowed_hosts: Vec<String>,
        allow_insecure_http: bool,
    ) -> Result<Self, CallbackError> {
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| CallbackError::Transport(sanitize_reqwest(error)))?;
        Ok(Self {
            http,
            allowed_hosts,
            allow_insecure_http,
        })
    }
}

#[async_trait]
impl CallbackClient for PlainCallbackClient {
    async fn post(&self, url: &str, callback: &RapCallback) -> Result<(), CallbackError> {
        let url =
            url::Url::parse(url).map_err(|error| CallbackError::InvalidUrl(error.to_string()))?;
        validate_callback_url(&url, &self.allowed_hosts, self.allow_insecure_http)?;

        let response = self
            .http
            .post(url)
            .json(callback)
            .send()
            .await
            .map_err(|error| CallbackError::Transport(sanitize_reqwest(error)))?;
        let status = response.status();
        if !status.is_success() {
            return Err(CallbackError::Rejected(status.as_u16()));
        }
        Ok(())
    }
}

fn validate_callback_url(
    url: &url::Url,
    allowed_hosts: &[String],
    allow_insecure_http: bool,
) -> Result<(), CallbackError> {
    let https = url.scheme() == "https";
    let loopback_http = url.scheme() == "http" && is_loopback(url);
    if !(https || loopback_http || (allow_insecure_http && url.scheme() == "http")) {
        return Err(CallbackError::InvalidUrl(
            "callback URL must use https (or loopback http)".to_owned(),
        ));
    }
    if !url.username().is_empty() || url.password().is_some() || url.fragment().is_some() {
        return Err(CallbackError::InvalidUrl(
            "callback URL must not contain userinfo or a fragment".to_owned(),
        ));
    }
    if allowed_hosts.is_empty() && !is_loopback(url) {
        return Err(CallbackError::InvalidUrl(
            "callback host allowlist is empty; configure callback_allowed_hosts".to_owned(),
        ));
    }
    if !allowed_hosts.is_empty() {
        let Some(host) = url.host_str() else {
            return Err(CallbackError::InvalidUrl(
                "callback URL has no host".to_owned(),
            ));
        };
        if !allowed_hosts
            .iter()
            .any(|allowed| allowed.eq_ignore_ascii_case(host))
        {
            return Err(CallbackError::InvalidUrl(format!(
                "callback host '{host}' is not allowed"
            )));
        }
    }
    Ok(())
}

fn is_loopback(url: &url::Url) -> bool {
    matches!(url.host_str(), Some("localhost" | "127.0.0.1" | "::1"))
}

/// Strip the URL from a reqwest error. Callback URLs can contain bearer
/// tokens, and those must not reach logs.
fn sanitize_reqwest(error: reqwest::Error) -> String {
    let error = error.without_url();
    if error.is_timeout() {
        "request timed out".to_owned()
    } else if error.is_connect() {
        "connection failed".to_owned()
    } else {
        "request failed".to_owned()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn url(value: &str) -> url::Url {
        url::Url::parse(value).unwrap()
    }

    #[test]
    fn rejects_external_http_by_default() {
        assert!(validate_callback_url(&url("http://example.com/cb"), &[], false).is_err());
    }

    #[test]
    fn allows_loopback_http() {
        validate_callback_url(&url("http://127.0.0.1:1234/cb"), &[], false).unwrap();
    }

    #[test]
    fn requires_https_and_a_host_allowlist_for_external_callbacks() {
        let allowed = vec!["example.com".to_owned()];
        validate_callback_url(&url("https://example.com/cb"), &allowed, false).unwrap();
        assert!(validate_callback_url(&url("https://example.com/cb"), &[], false).is_err());
        validate_callback_url(&url("http://example.com/cb"), &allowed, true).unwrap();
    }

    #[test]
    fn enforces_the_host_allowlist() {
        let allowed = vec!["receiver.example".to_owned()];
        validate_callback_url(&url("https://receiver.example/cb"), &allowed, false).unwrap();
        assert!(validate_callback_url(&url("https://other.example/cb"), &allowed, false).is_err());
    }

    #[test]
    fn rejects_userinfo_and_fragments() {
        assert!(
            validate_callback_url(&url("https://user:pass@example.com/cb"), &[], false).is_err()
        );
        assert!(validate_callback_url(&url("https://example.com/cb#frag"), &[], false).is_err());
    }
}
