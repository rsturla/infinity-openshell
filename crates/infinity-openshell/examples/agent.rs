//! Full integration: an Infinity agent runs a tool through the RAP server and
//! a real OpenShell sandbox.
//!
//! ```shell
//! OPENSHELL_TEST_GATEWAY=demo cargo run -p infinity-openshell --example agent --features bootstrap
//! ```

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use async_trait::async_trait;
use infinity_agent_core::stores::{InMemoryConversationStore, InMemoryStateStore};
use infinity_agent_core::system::{AgentEvent, AgentSystemBuilder, StaticModel};
use infinity_openshell::{ServerConfig, configure_labels};
use infinity_provider_protocol::{
    CompletionError, CompletionRequest, FinalResponse, Message, ModelEntry, ModelProvider,
    ModelStream, StreamChunk, ToolCall,
};
use infinity_rap_bridge::RapToolSet;
use openshell_session::{
    BootstrapTokenSource, GatewayAuth, GatewayConfig, RuntimeConfig, SessionRuntime, TenantId,
};
use url::Url;

const MODEL_ID: &str = "example-model";
const TOOL_COMMAND: &str = "printf infinity-openshell-agent";
const FINAL_TEXT: &str = "The sandbox ran successfully.";

struct ScenarioModel;

fn has_tool_result(request: &CompletionRequest) -> bool {
    request.chat_history.iter().any(|message| {
        matches!(
            message,
            Message::User { content }
                if content.iter().any(|item| matches!(
                    item,
                    infinity_provider_protocol::message::UserContent::ToolResult(_)
                ))
        )
    })
}

#[async_trait]
impl ModelProvider for ScenarioModel {
    async fn list_models(
        &self,
    ) -> Result<Vec<ModelEntry>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(vec![ModelEntry {
            model_id: MODEL_ID.to_owned(),
            display_name: "Example scenario".to_owned(),
            context_window: 200_000,
            max_output_tokens: None,
            supports_image_input: false,
        }])
    }

    async fn invoke_model(
        &self,
        _model_id: &str,
        request: CompletionRequest,
    ) -> Result<ModelStream, CompletionError> {
        let chunks = if has_tool_result(&request) {
            vec![
                Ok(StreamChunk::Text(FINAL_TEXT.to_owned())),
                Ok(StreamChunk::Final(FinalResponse { usage: None })),
            ]
        } else {
            vec![
                Ok(StreamChunk::ToolCall(ToolCall::new(
                    "example-call",
                    "openshell_exec",
                    serde_json::json!({"command": ["sh", "-c", TOOL_COMMAND]}),
                ))),
                Ok(StreamChunk::Final(FinalResponse { usage: None })),
            ]
        };
        Ok(Box::pin(futures_util::stream::iter(chunks)))
    }
}

fn unique(name: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |duration| duration.as_nanos());
    format!("{name}-{}-{nanos}", std::process::id())
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let local = tokio::task::LocalSet::new();
    local.run_until(run()).await
}

async fn run() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let gateway_name =
        std::env::var("OPENSHELL_TEST_GATEWAY").unwrap_or_else(|_| "demo".to_owned());
    let source = Arc::new(BootstrapTokenSource::new(&gateway_name)?);
    let mut runtime_config = RuntimeConfig::new(GatewayConfig::new(
        source.endpoint()?,
        "default",
        GatewayAuth::OidcSource { source },
    ));
    configure_labels(&mut runtime_config);
    runtime_config.workdir = "/sandbox".to_owned();
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
    let endpoint = format!("http://127.0.0.1:{port}");

    let bridge = infinity_rap_bridge::RapCallbackBridge::bind().await?;
    let callback_url = bridge.callback_url().to_owned();
    let session = rap_protocol::ThreadId::from(unique("agent-thread"));
    let toolset = RapToolSet::connect([endpoint.clone()], &session, callback_url).await?;

    let provider: Arc<dyn ModelProvider> = Arc::new(ScenarioModel);
    let model = StaticModel::new(provider, MODEL_ID).await?;
    let system = AgentSystemBuilder::new_local(
        InMemoryConversationStore::new(),
        InMemoryStateStore::new(),
        model,
    )
    .tools(toolset.tools())
    .start();
    let (_views, callback_task) = bridge.serve_into(system.sender());

    let mut thread = system.thread_builder().launch().await;
    thread
        .send_user_text("Run the example command in the OpenShell sandbox.")
        .await?;

    let mut tool_text = String::new();
    let mut final_text = String::new();
    let mut rounds = 0;
    loop {
        let event = tokio::time::timeout(Duration::from_secs(240), thread.recv())
            .await
            .map_err(|_| "agent turn timed out")?
            .ok_or("agent system closed")?;
        match event {
            AgentEvent::ToolCall { name, .. } => println!("tool call: {name}"),
            AgentEvent::ToolResult { segments } => {
                tool_text = segments
                    .iter()
                    .find_map(|segment| match segment {
                        rap_protocol::DisplaySegment::Text(text) => Some(text.clone()),
                        _ => None,
                    })
                    .unwrap_or_default();
                println!("tool result: {tool_text:?}");
            }
            AgentEvent::TextChunk { text } if text == FINAL_TEXT => {
                final_text = text;
            }
            AgentEvent::CompletionFinished { .. } => {
                rounds += 1;
                if rounds >= 2 {
                    break;
                }
            }
            _ => {}
        }
    }
    assert!(tool_text.contains("infinity-openshell-agent"));
    assert_eq!(final_text, FINAL_TEXT);
    println!("agent final response: {final_text}");

    let thread_id = thread.thread_id().as_str().to_owned();
    system.shutdown().await;
    drop(callback_task);
    let close = reqwest::Client::new()
        .post(format!("{endpoint}/close_thread"))
        .json(&serde_json::json!({"thread_id": thread_id}))
        .send()
        .await?;
    println!("close: {}", close.status());
    Ok(())
}
