//! Local end-to-end example: the real RAP server over the fake gateway.
//!
//! ```shell
//! cargo run -p infinity-openshell --example fake_end_to_end
//! ```

use std::sync::Arc;

use async_trait::async_trait;
use axum::Router;
use infinity_openshell::{
    CallbackClient, CallbackError, MemoryJournal, ServerConfig, router_with_callback,
};
use openshell_session::fake::FakeGateway;
use openshell_session::{GatewayAuth, GatewayConfig, RuntimeConfig, SessionRuntime, TenantId};
use rap_protocol::{
    ProviderCallId, RapCallback, RapInvocation, RapToolResult, ThreadId, ToolCallId,
};
use tokio::sync::mpsc;
use url::Url;

#[derive(Clone)]
struct PrintCallback {
    results: mpsc::UnboundedSender<RapToolResult>,
}

#[async_trait]
impl CallbackClient for PrintCallback {
    async fn post(&self, _url: &str, callback: &RapCallback) -> Result<(), CallbackError> {
        if let RapCallback::ToolResult(result) = callback {
            let _ = self.results.send(result.clone());
        }
        Ok(())
    }
}

fn invocation(
    id: &str,
    operation: &str,
    arguments: serde_json::Value,
    thread: &str,
) -> RapInvocation {
    RapInvocation {
        operation: operation.to_owned(),
        arguments,
        id: ToolCallId::from(id),
        call_id: None::<ProviderCallId>,
        callback_url: "http://callback.invalid/".to_owned(),
        group_id: ThreadId::from(thread),
        user_id: None,
        thread_ancestors: None,
    }
}

async fn call(
    port: u16,
    invocation: &RapInvocation,
    results: &mut mpsc::UnboundedReceiver<RapToolResult>,
) -> String {
    let response = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{port}/invoke"))
        .json(invocation)
        .send()
        .await
        .expect("POST /invoke");
    assert_eq!(response.status(), 200);
    tokio::time::timeout(std::time::Duration::from_secs(2), results.recv())
        .await
        .expect("callback timed out")
        .expect("callback channel closed")
        .text
        .unwrap_or_default()
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let gateway = Arc::new(FakeGateway::new());
    gateway.set_next_stdout("hello from the fake gateway\n");

    let runtime = SessionRuntime::new(
        gateway,
        RuntimeConfig::new(GatewayConfig::new(
            Url::parse("https://gateway.example.com")?,
            "default",
            GatewayAuth::None,
        )),
    )?;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let config = ServerConfig::new(
        Url::parse(&format!("http://127.0.0.1:{port}"))?,
        TenantId::new("demo")?,
    );

    let (sender, mut results) = mpsc::unbounded_channel();
    let callback = Arc::new(PrintCallback { results: sender });
    let app: Router =
        router_with_callback(runtime, config, Arc::new(MemoryJournal::new()), callback).await?;
    tokio::spawn(async move {
        axum::serve(listener, app).await.expect("serve");
    });

    println!("RAP server: http://127.0.0.1:{port}");

    let exec = invocation(
        "call-1",
        "openshell_exec",
        serde_json::json!({"command": ["sh", "-c", "printf 'hello from the sandbox\\n'"]}),
        "thread-1",
    );
    println!("exec: {}", call(port, &exec, &mut results).await);

    let write = invocation(
        "call-2",
        "openshell_write",
        serde_json::json!({"path": "notes/state.txt", "content": "persisted\n"}),
        "thread-1",
    );
    println!("write: {}", call(port, &write, &mut results).await);

    let read = invocation(
        "call-3",
        "openshell_read",
        serde_json::json!({"path": "notes/state.txt"}),
        "thread-1",
    );
    println!("read: {}", call(port, &read, &mut results).await);

    let close = reqwest::Client::new()
        .post(format!("http://127.0.0.1:{port}/close_thread"))
        .json(&serde_json::json!({"thread_id": "thread-1"}))
        .send()
        .await?;
    println!("close: {}", close.status());

    Ok(())
}
