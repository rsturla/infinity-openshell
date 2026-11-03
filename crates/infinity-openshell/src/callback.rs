//! RAP callback delivery.

use async_trait::async_trait;
use rap_protocol::RapCallback;
use std::time::Duration;

/// Callback delivery error.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
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
    /// SigV4 signing failed.
    #[error("callback signing failed: {0}")]
    Signing(String),
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

/// SigV4-signed callback client for Lambda Function URLs and IAM-protected
/// endpoints.
#[cfg(feature = "sigv4")]
#[derive(Clone)]
pub struct SigV4CallbackClient {
    http: reqwest::Client,
    credentials: aws_credential_types::provider::SharedCredentialsProvider,
    region: String,
    service: String,
    allowed_hosts: Vec<String>,
    allow_insecure_http: bool,
}

#[cfg(feature = "sigv4")]
impl SigV4CallbackClient {
    /// Build a client with the AWS default credential chain.
    ///
    /// # Errors
    ///
    /// Returns a transport or signing-configuration error.
    pub async fn from_environment(
        timeout: Duration,
        allowed_hosts: Vec<String>,
        allow_insecure_http: bool,
        region: Option<String>,
        service: impl Into<String>,
    ) -> Result<Self, CallbackError> {
        let config = aws_config::load_defaults(aws_config::BehaviorVersion::latest()).await;
        let credentials = config
            .credentials_provider()
            .ok_or_else(|| CallbackError::Signing("no AWS credentials provider".to_owned()))?;
        let region = region
            .or_else(|| config.region().map(ToString::to_string))
            .unwrap_or_else(|| "us-east-1".to_owned());
        Self::new(
            credentials,
            region,
            service,
            allowed_hosts,
            allow_insecure_http,
            timeout,
        )
    }

    /// Build a client with explicit credentials.
    ///
    /// # Errors
    ///
    /// Returns a transport error when the HTTP client cannot be built.
    pub fn new(
        credentials: aws_credential_types::provider::SharedCredentialsProvider,
        region: impl Into<String>,
        service: impl Into<String>,
        allowed_hosts: Vec<String>,
        allow_insecure_http: bool,
        timeout: Duration,
    ) -> Result<Self, CallbackError> {
        let http = reqwest::Client::builder()
            .timeout(timeout)
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|error| CallbackError::Transport(sanitize_reqwest(error)))?;
        Ok(Self {
            http,
            credentials,
            region: region.into(),
            service: service.into(),
            allowed_hosts,
            allow_insecure_http,
        })
    }
}

#[cfg(feature = "sigv4")]
#[async_trait]
impl CallbackClient for SigV4CallbackClient {
    async fn post(&self, url: &str, callback: &RapCallback) -> Result<(), CallbackError> {
        use aws_credential_types::provider::ProvideCredentials;
        use aws_sigv4::http_request::{
            SignableBody, SignableRequest, SignatureLocation, SigningSettings, sign,
        };
        use aws_sigv4::sign::v4::SigningParams;
        use aws_smithy_runtime_api::client::identity::Identity;

        let url =
            url::Url::parse(url).map_err(|error| CallbackError::InvalidUrl(error.to_string()))?;
        validate_callback_url(&url, &self.allowed_hosts, self.allow_insecure_http)?;
        let host = url
            .host_str()
            .ok_or_else(|| CallbackError::InvalidUrl("missing host".to_owned()))?
            .to_owned();
        let body = serde_json::to_vec(callback)
            .map_err(|error| CallbackError::Transport(error.to_string()))?;
        let credentials = self
            .credentials
            .provide_credentials()
            .await
            .map_err(|error| CallbackError::Signing(error.to_string()))?;
        let identity = Identity::new(credentials, None);
        let mut settings = SigningSettings::default();
        settings.signature_location = SignatureLocation::Headers;
        let params = SigningParams::builder()
            .identity(&identity)
            .region(&self.region)
            .name(&self.service)
            .time(std::time::SystemTime::now())
            .settings(settings)
            .build()
            .map_err(|error| CallbackError::Signing(error.to_string()))?;
        let request = SignableRequest::new(
            "POST",
            url.as_str(),
            std::iter::once(("host", host.as_str()))
                .chain(std::iter::once(("content-type", "application/json"))),
            SignableBody::Bytes(&body),
        )
        .map_err(|error| CallbackError::Signing(error.to_string()))?;
        let (instructions, _) = sign(request, &params.into())
            .map_err(|error| CallbackError::Signing(error.to_string()))?
            .into_parts();

        let mut request = self
            .http
            .post(url)
            .header("host", &host)
            .header("content-type", "application/json");
        for (name, value) in instructions.headers() {
            request = request.header(name, value);
        }
        let response = request
            .body(body)
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
    matches!(
        url.host_str(),
        Some("localhost" | "127.0.0.1" | "::1" | "[::1]")
    )
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
    #[cfg(feature = "sigv4")]
    use rap_protocol::{RapToolResult, ThreadId, ToolCallId};
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

    #[cfg(feature = "sigv4")]
    #[tokio::test]
    async fn sigv4_signs_with_the_configured_region_and_service() {
        use aws_credential_types::{Credentials, provider::SharedCredentialsProvider};
        use axum::Router;
        use axum::http::{HeaderMap, StatusCode};
        use axum::routing::post;

        let (sender, mut receiver) = tokio::sync::mpsc::unbounded_channel();
        let app = Router::new().route(
            "/cb",
            post(move |headers: HeaderMap| {
                let sender = sender.clone();
                async move {
                    let authorization = headers
                        .get("authorization")
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or_default()
                        .to_owned();
                    let _ = sender.send(authorization);
                    StatusCode::OK
                }
            }),
        );
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });

        let provider = SharedCredentialsProvider::new(Credentials::new(
            "AKIDEXAMPLE",
            "secret",
            None,
            None,
            "test",
        ));
        let client = SigV4CallbackClient::new(
            provider,
            "us-east-1",
            "lambda",
            Vec::new(),
            false,
            Duration::from_secs(5),
        )
        .unwrap();
        let result = RapToolResult {
            group_id: ThreadId::from("thread-1"),
            id: ToolCallId::from("call-1"),
            call_id: None,
            text: Some("ok".to_owned()),
            content: None,
            display_as: None,
            subscription: None,
        };
        client
            .post(
                &format!("http://127.0.0.1:{port}/cb"),
                &RapCallback::ToolResult(result),
            )
            .await
            .unwrap();

        let authorization = receiver.recv().await.unwrap();
        assert!(
            authorization.starts_with("AWS4-HMAC-SHA256 Credential=AKIDEXAMPLE/"),
            "authorization: {authorization}"
        );
        assert!(
            authorization.contains("/us-east-1/lambda/aws4_request"),
            "authorization: {authorization}"
        );
    }
}
