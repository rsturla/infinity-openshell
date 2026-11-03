//! Live network access through an attached provider and a custom image.
//!
//! The example imports the provider profile and creates the provider through
//! the gateway API. It does not call the OpenShell CLI.
//!
//! ```shell
//! OPENSHELL_TEST_GATEWAY=demo \
//!   cargo run -p infinity-openshell --example network_access --features bootstrap
//! ```
//!
//! The sandbox uses `curlimages/curl`, which supplies a non-root user and the
//! `curl` binary that the profile authorizes. The provider profile contributes
//! the `api.github.com` network rule and binds `GITHUB_TOKEN` to it. The
//! sandbox sees a placeholder, never the real token.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use openshell_session::{
    BootstrapTokenSource, ExecRequest, GatewayAuth, GatewayConfig, ProviderAdmin, ProviderSpec,
    RuntimeConfig, SessionKey, SessionRuntime, TenantId, ThreadId,
};

const PROFILE_ID: &str = "infinity-example";
const PROFILE_YAML: &str = include_str!("../../../examples/profiles/infinity-example.yaml");
const ALLOWED_HOST: &str = "api.github.com";
const DENIED_HOST: &str = "example.com";

fn unique_thread() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    format!("network-{}-{nanos}", std::process::id())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let gateway_name =
        std::env::var("OPENSHELL_TEST_GATEWAY").unwrap_or_else(|_| "demo".to_owned());
    let provider = std::env::var("OPENSHELL_PROVIDER").unwrap_or_else(|_| PROFILE_ID.to_owned());
    let token = std::env::var("GITHUB_TOKEN").unwrap_or_else(|_| "example-token".to_owned());
    let image =
        std::env::var("OPENSHELL_IMAGE").unwrap_or_else(|_| "curlimages/curl:8.10.1".to_owned());

    let source = Arc::new(BootstrapTokenSource::new(&gateway_name)?);
    let mut config = RuntimeConfig::new(GatewayConfig::new(
        source.endpoint()?,
        "default",
        GatewayAuth::OidcSource { source },
    ));
    config.image = Some(image.clone());
    config.workdir = "/sandbox".to_owned();
    config.lifecycle.idle_after = Duration::from_secs(60);
    config.lifecycle.retain_for = Duration::from_secs(3600);
    config.lifecycle.ready_timeout = Duration::from_secs(180);
    config.lifecycle.exec_timeout = Duration::from_secs(60);
    config.validate()?;

    // Import the profile and create the provider through the API, so the
    // service does not depend on an operator running the CLI first.
    let admin = ProviderAdmin::connect(&config).await?;
    admin.ensure_profile(PROFILE_YAML, false).await?;
    admin
        .ensure_provider(
            &ProviderSpec::new(&provider, PROFILE_ID)
                .workspace_profile(config.gateway.workspace.clone())
                .credential("GITHUB_TOKEN", token),
        )
        .await?;
    println!("provider: {provider} (profile {PROFILE_ID})");

    config.providers = vec![provider];
    let runtime = SessionRuntime::open(config).await?;
    let key = SessionKey::new(TenantId::new("example")?, ThreadId::new(unique_thread())?);
    let session = runtime.acquire(key.clone()).await?;
    println!("sandbox: {} (image {image})", session.name());

    let script = format!(
        "echo token=$(printenv GITHUB_TOKEN); \
         echo allowed=$(curl -sS -m 20 -o /dev/null -w '%{{http_code}}' https://{ALLOWED_HOST}/user || echo curl-failed); \
         echo denied=$(curl -sS -m 10 -o /dev/null -w '%{{http_code}}' https://{DENIED_HOST} || echo curl-failed)"
    );
    let output = session
        .exec(ExecRequest::new(vec![
            "sh".to_owned(),
            "-c".to_owned(),
            script,
        ]))
        .await?;
    let text = String::from_utf8_lossy(&output.stdout);
    println!("{text}");

    let token = field(&text, "token=").unwrap_or_default();
    let allowed = field(&text, "allowed=").unwrap_or_default();
    let denied = field(&text, "denied=").unwrap_or_default();

    // The credential is replaced with an opaque placeholder. A real value here
    // would mean the provider did not rewrite the environment.
    assert!(
        token.starts_with("openshell:resolve:"),
        "GITHUB_TOKEN was not a provider placeholder: {token:?}"
    );
    // The fake token is rejected with 401, which proves both that the endpoint
    // is reachable and that the proxy injected the credential.
    assert_eq!(
        allowed, "401",
        "expected the allowed endpoint to receive the placeholder token, got {allowed:?}"
    );
    assert!(
        denied.starts_with("000") || denied.contains("curl-failed"),
        "expected the unlisted host to be denied, got {denied:?}"
    );

    drop(session);
    runtime.close(&key).await?;
    println!("closed");
    Ok(())
}

fn field<'a>(text: &'a str, prefix: &str) -> Option<&'a str> {
    text.lines()
        .find_map(|line| line.strip_prefix(prefix))
        .map(str::trim)
}
