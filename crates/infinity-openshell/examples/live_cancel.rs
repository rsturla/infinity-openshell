//! Live cancellation: interrupt a running command.
//!
//! ```shell
//! OPENSHELL_TEST_GATEWAY=demo cargo run -p infinity-openshell --example live_cancel --features bootstrap
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
    let config = ServerConfig::ephemeral(
        Url::parse(&format!("http://127.0.0.1:{port}"))?,
        TenantId::new("example")?,
    );
    let app = infinity_openshell::router(runtime, config).await?;
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });

    let (callback_url, mut receiver) = start_callback_channel().await?;
    let thread = unique("cancel-thread");
    let invocation = RapInvocation {
        operation: "openshell_exec".to_owned(),
        arguments: serde_json::json!({"command": ["sleep", "60"]}),
        id: ToolCallId::from(unique("cancel")),
        call_id: None::<ProviderCallId>,
        callback_url,
        group_id: ThreadId::from(thread.clone()),
        user_id: None,
        thread_ancestors: None,
    };

    let response = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{port}/invoke"))
        .json(&invocation)
        .send()
        .await?;
    println!("ack: {}", response.status());

    tokio::time::sleep(Duration::from_secs(2)).await;
    let cancel = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{port}/cancel_tool_call"))
        .json(&serde_json::json!({
            "thread_id": thread,
            "tool_call_id": invocation.id,
        }))
        .send()
        .await?;
    println!("cancel: {}", cancel.status());

    loop {
        match tokio::time::timeout(Duration::from_secs(30), receiver.recv()).await? {
            Some(RapCallback::ToolResult(result)) if result.id == invocation.id => {
                println!("result:\n{}", result.text.unwrap_or_default());
                break;
            }
            Some(_) => {}
            None => return Err("callback channel closed".into()),
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
