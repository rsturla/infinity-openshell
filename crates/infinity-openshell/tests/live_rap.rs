//! Live RAP server verification against a registered OpenShell gateway.
//!
//! ```shell
//! OPENSHELL_TEST_GATEWAY=demo cargo test --features bootstrap --test live_rap -- --ignored --nocapture
//! ```
#![cfg(feature = "bootstrap")]

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use openshell_session::{
    BootstrapTokenSource, GatewayAuth, GatewayConfig, RuntimeConfig, SessionRuntime, TenantId,
};
use rap_client::callback_server::start_callback_channel;
use rap_protocol::{ProviderCallId, RapCallback, RapInvocation, ThreadId, ToolCallId};
use url::Url;

fn live_config(gateway_name: &str) -> RuntimeConfig {
    let metadata = openshell_bootstrap::load_gateway_metadata(gateway_name)
        .unwrap_or_else(|error| panic!("cannot load gateway metadata: {error}"));
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
    infinity_openshell::configure_labels(&mut config);
    config.lifecycle.idle_after = Duration::from_secs(300);
    config.lifecycle.retain_for = Duration::from_secs(3600);
    config.lifecycle.ready_timeout = Duration::from_secs(120);
    config.lifecycle.exec_timeout = Duration::from_secs(120);
    config
}

fn unique(name: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    format!("{name}-{}-{nanos}", std::process::id())
}

fn invocation(
    id: &str,
    operation: &str,
    arguments: serde_json::Value,
    callback_url: &str,
    thread: &str,
) -> RapInvocation {
    RapInvocation {
        operation: operation.to_owned(),
        arguments,
        id: ToolCallId::from(id),
        call_id: None::<ProviderCallId>,
        callback_url: callback_url.to_owned(),
        group_id: ThreadId::from(thread),
        user_id: None,
        thread_ancestors: None,
    }
}

async fn call(
    port: u16,
    invocation: &RapInvocation,
    receiver: &mut tokio::sync::mpsc::UnboundedReceiver<RapCallback>,
) -> String {
    let response = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{port}/invoke"))
        .json(invocation)
        .send()
        .await
        .expect("POST /invoke");
    assert_eq!(response.status(), 200, "invocation was not acknowledged");

    loop {
        let callback = tokio::time::timeout(Duration::from_secs(240), receiver.recv())
            .await
            .expect("callback timed out")
            .expect("callback channel closed");
        if let RapCallback::ToolResult(result) = callback
            && result.id == invocation.id
        {
            return result.text.unwrap_or_default();
        }
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "requires a live OpenShell gateway and OPENSHELL_TEST_GATEWAY"]
async fn live_rap_exec_read_and_close() {
    let gateway_name =
        std::env::var("OPENSHELL_TEST_GATEWAY").unwrap_or_else(|_| "demo".to_owned());
    let runtime = SessionRuntime::open(live_config(&gateway_name))
        .await
        .expect("connect to the gateway");

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("bind the RAP server");
    let port = listener.local_addr().expect("local address").port();
    let config = infinity_openshell::ServerConfig::new(
        Url::parse(&format!("http://127.0.0.1:{port}")).expect("base URL"),
        TenantId::new("live").expect("tenant"),
    );
    let app = infinity_openshell::router(runtime, config)
        .await
        .expect("build the RAP router");
    tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("serve the RAP router");
    });

    let (callback_url, mut receiver) = start_callback_channel()
        .await
        .expect("start the callback channel");
    let thread = unique("live-rap");

    let exec = invocation(
        &unique("call"),
        "openshell_exec",
        serde_json::json!({
            "command": [
                "sh",
                "-c",
                "mkdir -p live && printf rap-live > live/state.txt && cat live/state.txt"
            ]
        }),
        &callback_url,
        &thread,
    );
    let text = call(port, &exec, &mut receiver).await;
    assert!(text.contains("rap-live"), "exec result: {text:?}");

    let read = invocation(
        &unique("call"),
        "openshell_read",
        serde_json::json!({"path": "live/state.txt"}),
        &callback_url,
        &thread,
    );
    let text = call(port, &read, &mut receiver).await;
    assert_eq!(text, "rap-live");

    let close = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{port}/close_thread"))
        .json(&serde_json::json!({"thread_id": thread}))
        .send()
        .await
        .expect("POST /close_thread");
    assert_eq!(close.status(), 200);
}
