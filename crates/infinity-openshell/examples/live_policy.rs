//! Live sandbox policy: grant one host with a create-time policy and deny
//! everything else.
//!
//! ```shell
//! OPENSHELL_TEST_GATEWAY=demo \
//!   cargo run -p infinity-openshell --example live_policy --features bootstrap
//! ```
//!
//! The policy applies to every sandbox this runtime creates. Use a separate
//! runtime per toolset when different threads need different egress.

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use openshell_session::{
    BootstrapTokenSource, ExecRequest, GatewayAuth, GatewayConfig, RuntimeConfig, SessionKey,
    SessionRuntime, TenantId, ThreadId,
};

const POLICY: &str = r#"version: 1
filesystem_policy:
  include_workdir: true
  read_only: [/usr, /lib, /etc, /bin, /proc, /dev/urandom, /var/log]
  read_write: [/tmp, /dev/null]
network_policies:
  example_https:
    endpoints:
      - host: example.com
        port: 443
        protocol: rest
        access: read-only
        enforcement: enforce
    binaries:
      - path: /usr/bin/curl
"#;

fn unique_thread() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    format!("policy-{}-{nanos}", std::process::id())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let gateway_name =
        std::env::var("OPENSHELL_TEST_GATEWAY").unwrap_or_else(|_| "demo".to_owned());
    let image =
        std::env::var("OPENSHELL_IMAGE").unwrap_or_else(|_| "curlimages/curl:8.10.1".to_owned());

    let source = Arc::new(BootstrapTokenSource::new(&gateway_name)?);
    let mut config = RuntimeConfig::new(GatewayConfig::new(
        source.endpoint()?,
        "default",
        GatewayAuth::OidcSource { source },
    ));
    config.image = Some(image.clone());
    config.policy = Some(POLICY.to_owned());
    config.lifecycle.idle_after = Duration::from_secs(60);
    config.lifecycle.retain_for = Duration::from_secs(3600);
    config.lifecycle.ready_timeout = Duration::from_secs(180);
    config.lifecycle.exec_timeout = Duration::from_secs(60);
    config.validate()?;

    let runtime = SessionRuntime::open(config).await?;
    let key = SessionKey::new(TenantId::new("example")?, ThreadId::new(unique_thread())?);
    let session = runtime.acquire(key.clone()).await?;
    println!("sandbox: {} (image {image})", session.name());

    let output = session
        .exec(ExecRequest::new(vec![
            "sh".to_owned(),
            "-c".to_owned(),
            "echo allowed=$(curl -sS -m 20 -o /dev/null -w '%{http_code}' https://example.com || echo curl-failed); \
             echo denied=$(curl -sS -m 10 -o /dev/null -w '%{http_code}' https://api.github.com || echo curl-failed)"
                .to_owned(),
        ]))
        .await?;
    let text = String::from_utf8_lossy(&output.stdout);
    println!("{text}");

    let allowed = field(&text, "allowed=").unwrap_or_default();
    let denied = field(&text, "denied=").unwrap_or_default();
    assert_eq!(
        allowed, "200",
        "expected the policy host to be reachable, got {allowed:?}"
    );
    assert!(
        denied.starts_with("000") || denied.contains("curl-failed"),
        "expected an unlisted host to be denied, got {denied:?}"
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
