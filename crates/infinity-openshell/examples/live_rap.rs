//! Live RAP server: exec, read, and close against a real gateway.
//!
//! ```shell
//! OPENSHELL_TEST_GATEWAY=demo cargo run -p infinity-openshell --example live_rap --features bootstrap
//! ```

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use infinity_openshell::{ServerConfig, configure_labels};
use openshell_session::{
    BootstrapTokenSource, GatewayAuth, GatewayConfig, RuntimeConfig, SessionRuntime, TenantId,
};
use rap_client::callback_server::start_callback_channel;
use rap_protocol::{ProviderCallId, RapCallback, RapInvocation, ThreadId, ToolCallId};
use url::Url;

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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let gateway_name =
        std::env::var("OPENSHELL_TEST_GATEWAY").unwrap_or_else(|_| "demo".to_owned());
    let source = Arc::new(BootstrapTokenSource::new(&gateway_name)?);
    let mut runtime_config = RuntimeConfig::new(GatewayConfig::new(
        source.endpoint()?,
        "default",
        GatewayAuth::OidcSource { source },
    ));
    configure_labels(&mut runtime_config);
    runtime_config.lifecycle.idle_after = Duration::from_secs(300);
    runtime_config.lifecycle.retain_for = Duration::from_secs(3600);
    runtime_config.lifecycle.ready_timeout = Duration::from_secs(120);
    runtime_config.lifecycle.exec_timeout = Duration::from_secs(120);

    let runtime = SessionRuntime::open(runtime_config).await?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let config = ServerConfig::new(
        Url::parse(&format!("http://127.0.0.1:{port}"))?,
        TenantId::new("example")?,
    );
    let app = infinity_openshell::router(runtime, config).await?;
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });
    println!("RAP server: http://127.0.0.1:{port}");

    let (callback_url, mut receiver) = start_callback_channel().await?;
    let thread = unique("example-thread");

    let exec = invocation(
        &unique("exec"),
        "openshell_exec",
        serde_json::json!({
            "command": [
                "sh", "-c",
                "mkdir -p live && printf rap-live > live/state.txt && cat live/state.txt"
            ]
        }),
        &callback_url,
        &thread,
    );
    let response = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{port}/invoke"))
        .json(&exec)
        .send()
        .await?;
    println!("ack: {}", response.status());

    loop {
        match tokio::time::timeout(Duration::from_secs(240), receiver.recv()).await? {
            Some(RapCallback::ToolResult(result)) if result.id == exec.id => {
                println!("exec result: {}", result.text.unwrap_or_default());
                break;
            }
            Some(RapCallback::SubscriptionEvent(event)) => {
                println!("event: final={:?} {}", event.r#final, event.text);
            }
            Some(_) => {}
            None => return Err("callback channel closed".into()),
        }
    }

    let read = invocation(
        &unique("read"),
        "openshell_read",
        serde_json::json!({"path": "live/state.txt"}),
        &callback_url,
        &thread,
    );
    reqwest::Client::new()
        .post(format!("http://127.0.0.1:{port}/invoke"))
        .json(&read)
        .send()
        .await?;
    loop {
        if let Some(RapCallback::ToolResult(result)) =
            tokio::time::timeout(Duration::from_secs(240), receiver.recv()).await?
            && result.id == read.id
        {
            println!("read result: {:?}", result.text);
            break;
        }
    }

    let close = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{port}/close_thread"))
        .json(&serde_json::json!({"thread_id": thread}))
        .send()
        .await?;
    println!("close: {}", close.status());
    Ok(())
}
