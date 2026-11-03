//! Live verification against a registered OpenShell gateway.
//!
//! Run with:
//!
//! ```shell
//! OPENSHELL_TEST_GATEWAY=demo cargo test --features bootstrap --test live_gateway -- --ignored --nocapture
//! ```
#![cfg(feature = "bootstrap")]

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use openshell_session::{
    BootstrapTokenSource, ExecRequest, GatewayAuth, GatewayConfig, RuntimeConfig, SandboxPath,
    SessionKey, SessionRuntime, TenantId, ThreadId,
};

fn live_config(gateway_name: &str) -> RuntimeConfig {
    let metadata =
        openshell_bootstrap::load_gateway_metadata(gateway_name).unwrap_or_else(|error| {
            panic!("cannot load gateway metadata for '{gateway_name}': {error}")
        });
    let source =
        Arc::new(BootstrapTokenSource::new(gateway_name).expect("load the cached OIDC token"));

    let endpoint = metadata
        .gateway_endpoint
        .parse()
        .expect("gateway endpoint must be a URL");
    let mut config = RuntimeConfig::new(GatewayConfig::new(
        endpoint,
        "default",
        GatewayAuth::OidcSource { source },
    ));
    config.lifecycle.idle_after = Duration::from_secs(1);
    config.lifecycle.retain_for = Duration::from_secs(3600);
    config.lifecycle.ready_timeout = Duration::from_secs(120);
    config.lifecycle.exec_timeout = Duration::from_secs(60);
    config
}

fn unique_thread() -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    format!("live-{}-{nanos}", std::process::id())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a live OpenShell gateway and OPENSHELL_TEST_GATEWAY"]
async fn live_stop_start_preserves_the_workspace() {
    let gateway_name =
        std::env::var("OPENSHELL_TEST_GATEWAY").unwrap_or_else(|_| "demo".to_owned());
    let runtime = SessionRuntime::open(live_config(&gateway_name))
        .await
        .expect("connect to the gateway");

    let key = SessionKey::new(
        TenantId::new("live").expect("tenant"),
        ThreadId::new(unique_thread()).expect("thread"),
    );

    let session = runtime.acquire(key.clone()).await.expect("acquire");
    let name = session.name().to_string();
    eprintln!("sandbox: {name}");

    let output = session
        .exec(ExecRequest::new(vec![
            "sh".to_owned(),
            "-c".to_owned(),
            "mkdir -p work && printf live > work/state.txt && cat work/state.txt".to_owned(),
        ]))
        .await
        .expect("exec");
    assert_eq!(output.code, 0, "stderr: {:?}", output.stderr);
    assert_eq!(&output.stdout[..], b"live");

    let binary_path = SandboxPath::new("work/bytes.bin").expect("path");
    session
        .write_file(&binary_path, vec![0_u8, 1, 2, 255])
        .await
        .expect("write binary");
    assert_eq!(
        &session.read_file(&binary_path).await.expect("read binary")[..],
        &[0, 1, 2, 255]
    );

    drop(session);
    tokio::time::sleep(Duration::from_secs(3)).await;

    let session = runtime.acquire(key.clone()).await.expect("reacquire");
    assert_eq!(session.name().as_str(), name.as_str());

    let state = session
        .read_file(&SandboxPath::new("work/state.txt").expect("path"))
        .await
        .expect("read after restart");
    assert_eq!(&state[..], b"live");

    drop(session);
    runtime.close(&key).await.expect("close");
}
