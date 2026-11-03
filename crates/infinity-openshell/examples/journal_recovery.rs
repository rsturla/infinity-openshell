//! Crash recovery: an acknowledged invocation that never finished is reported
//! as interrupted on the next start.
//!
//! ```shell
//! cargo run -p infinity-openshell --example journal_recovery
//! ```

use std::sync::Arc;

use async_trait::async_trait;
use infinity_openshell::{
    CallbackClient, CallbackError, FileJournal, Journal, ServerConfig, router_with_callback,
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

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("journal.jsonl");
    let invocation = RapInvocation {
        operation: "openshell_exec".to_owned(),
        arguments: serde_json::json!({"command": ["true"]}),
        id: ToolCallId::from("interrupted-call"),
        call_id: None::<ProviderCallId>,
        callback_url: "http://callback.test/".to_owned(),
        group_id: ThreadId::from("thread-1"),
        user_id: None,
        thread_ancestors: None,
    };

    // Simulate an acknowledged invocation that never finished.
    {
        let journal = FileJournal::open(&path).await?;
        journal.accept(&invocation).await?;
    }
    println!("seeded an accepted-only journal at {}", path.display());

    let gateway = Arc::new(FakeGateway::new());
    let runtime = SessionRuntime::new(
        gateway,
        RuntimeConfig::new(GatewayConfig::new(
            Url::parse("https://gateway.example.com")?,
            "default",
            GatewayAuth::None,
        )),
    )?;
    let config = ServerConfig::ephemeral(Url::parse("http://tools.test")?, TenantId::new("demo")?);
    let (sender, mut results) = mpsc::unbounded_channel();
    let callback = Arc::new(PrintCallback { results: sender });
    let journal = Arc::new(FileJournal::open(&path).await?);
    let _app = router_with_callback(runtime, config, journal, callback).await?;

    let result = tokio::time::timeout(std::time::Duration::from_secs(2), results.recv())
        .await?
        .ok_or("callback channel closed")?;
    println!("recovered result: {}", result.text.unwrap_or_default());
    Ok(())
}
