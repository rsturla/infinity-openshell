//! Live session lifecycle: create, exec, write, idle stop, start, close.
//!
//! ```shell
//! OPENSHELL_TEST_GATEWAY=demo cargo run -p infinity-openshell --example live_session --features bootstrap
//! ```

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use openshell_session::{
    BootstrapTokenSource, ExecRequest, GatewayAuth, GatewayConfig, RuntimeConfig, SandboxPath,
    SessionKey, SessionRuntime, TenantId, ThreadId,
};

fn unique_thread() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    format!("example-{}-{nanos}", std::process::id())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let gateway_name =
        std::env::var("OPENSHELL_TEST_GATEWAY").unwrap_or_else(|_| "demo".to_owned());
    let source = Arc::new(BootstrapTokenSource::new(&gateway_name)?);
    let mut config = RuntimeConfig::new(GatewayConfig::new(
        source.endpoint()?,
        "default",
        GatewayAuth::OidcSource { source },
    ));
    config.workdir = "/sandbox".to_owned();
    config.lifecycle.idle_after = Duration::from_secs(1);
    config.lifecycle.retain_for = Duration::from_secs(3600);
    config.lifecycle.ready_timeout = Duration::from_secs(120);
    config.lifecycle.exec_timeout = Duration::from_secs(60);

    let runtime = SessionRuntime::open(config).await?;
    let key = SessionKey::new(TenantId::new("example")?, ThreadId::new(unique_thread())?);

    let session = runtime.acquire(key.clone()).await?;
    println!("sandbox: {}", session.name());

    let output = session
        .exec(ExecRequest::new(vec![
            "sh".to_owned(),
            "-c".to_owned(),
            "uname -a && mkdir -p work && printf 'persisted\\n' > work/state.txt".to_owned(),
        ]))
        .await?;
    println!(
        "exec exit {}:\n{}",
        output.code,
        String::from_utf8_lossy(&output.stdout)
    );

    let path = SandboxPath::new("work/state.txt")?;
    println!(
        "read before idle: {:?}",
        String::from_utf8_lossy(&session.read_file(&path).await?)
    );
    drop(session);

    println!("waiting for idle stop...");
    tokio::time::sleep(Duration::from_secs(3)).await;

    let session = runtime.acquire(key.clone()).await?;
    println!("restarted sandbox: {}", session.name());
    println!(
        "read after start: {:?}",
        String::from_utf8_lossy(&session.read_file(&path).await?)
    );
    drop(session);

    runtime.close(&key).await?;
    println!("closed");
    Ok(())
}
