//! Semi-large live test: ten Infinity agents run in parallel through one RAP
//! server, each with its own OpenShell sandbox.
//!
//! ```shell
//! OPENSHELL_TEST_GATEWAY=demo cargo test --features bootstrap --test live_scale -- --ignored --nocapture
//! ```
#![cfg(feature = "bootstrap")]

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

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

const AGENTS: usize = 10;
const MODEL_ID: &str = "scale-model";

struct ScaleModel {
    marker: String,
}

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
impl ModelProvider for ScaleModel {
    async fn list_models(
        &self,
    ) -> Result<Vec<ModelEntry>, Box<dyn std::error::Error + Send + Sync>> {
        Ok(vec![ModelEntry {
            model_id: MODEL_ID.to_owned(),
            display_name: "Scale scenario".to_owned(),
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
                Ok(StreamChunk::Text(format!("{}-done", self.marker))),
                Ok(StreamChunk::Final(FinalResponse { usage: None })),
            ]
        } else {
            vec![
                Ok(StreamChunk::ToolCall(ToolCall::new(
                    format!("scale-call-{}", self.marker),
                    "openshell_exec",
                    serde_json::json!({
                        "command": ["sh", "-c", format!("printf {}", self.marker)]
                    }),
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

async fn run_agent(
    index: usize,
    endpoint: String,
) -> Result<(String, String, String, Duration), Box<dyn std::error::Error + Send + Sync>> {
    let started = Instant::now();
    let marker = format!("agent-{index}");
    let bridge = infinity_rap_bridge::RapCallbackBridge::bind().await?;
    let callback_url = bridge.callback_url().to_owned();
    let session = rap_protocol::ThreadId::from(unique(&format!("scale-session-{index}")));
    let toolset = RapToolSet::connect([endpoint], &session, callback_url).await?;

    let provider: Arc<dyn ModelProvider> = Arc::new(ScaleModel {
        marker: marker.clone(),
    });
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
        .send_user_text(format!("Run the scale check for {marker}."))
        .await?;

    let mut tool_text = String::new();
    let mut final_text = String::new();
    let mut rounds = 0;
    loop {
        let event = tokio::time::timeout(Duration::from_secs(300), thread.recv())
            .await
            .map_err(|_| format!("{marker}: agent turn timed out"))?
            .ok_or_else(|| format!("{marker}: agent system closed"))?;
        match event {
            AgentEvent::ToolResult { segments } => {
                tool_text = segments
                    .iter()
                    .find_map(|segment| match segment {
                        rap_protocol::DisplaySegment::Text(text) => Some(text.clone()),
                        _ => None,
                    })
                    .unwrap_or_default();
            }
            AgentEvent::TextChunk { text } if text == format!("{marker}-done") => {
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

    let thread_id = thread.thread_id().as_str().to_owned();
    system.shutdown().await;
    drop(callback_task);
    Ok((thread_id, tool_text, final_text, started.elapsed()))
}

#[test]
#[ignore = "requires a live OpenShell gateway and OPENSHELL_TEST_GATEWAY"]
fn ten_agents_run_in_parallel() {
    let Some(gateway_name) = std::env::var("OPENSHELL_TEST_GATEWAY").ok() else {
        eprintln!("skipped: set OPENSHELL_TEST_GATEWAY");
        return;
    };

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("runtime");
    let local = tokio::task::LocalSet::new();
    local.block_on(&runtime, async move {
        let source = Arc::new(BootstrapTokenSource::new(&gateway_name).expect("token source"));
        let mut runtime_config = RuntimeConfig::new(GatewayConfig::new(
            source.endpoint().expect("gateway endpoint"),
            "default",
            GatewayAuth::OidcSource { source },
        ));
        configure_labels(&mut runtime_config);
        runtime_config.workdir = "/sandbox".to_owned();
        runtime_config.lifecycle.idle_after = Duration::from_secs(300);
        runtime_config.lifecycle.retain_for = Duration::from_secs(3600);
        runtime_config.lifecycle.ready_timeout = Duration::from_secs(180);
        runtime_config.lifecycle.exec_timeout = Duration::from_secs(120);

        let runtime = SessionRuntime::open(runtime_config)
            .await
            .expect("open the session runtime");
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind the RAP server");
        let port = listener.local_addr().expect("address").port();
        let config = ServerConfig::new(
            Url::parse(&format!("http://127.0.0.1:{port}")).expect("base URL"),
            TenantId::new("scale").expect("tenant"),
        );
        let app = infinity_openshell::router(runtime, config)
            .await
            .expect("build the RAP router");
        tokio::spawn(async move {
            axum::serve(listener, app).await.expect("serve");
        });
        let endpoint = format!("http://127.0.0.1:{port}");

        let started = Instant::now();
        let handles: Vec<_> = (0..AGENTS)
            .map(|index| {
                let endpoint = endpoint.clone();
                tokio::task::spawn_local(async move { (index, run_agent(index, endpoint).await) })
            })
            .collect();
        let outcomes = futures_util::future::join_all(handles).await;
        let elapsed = started.elapsed();

        let mut thread_ids = Vec::new();
        let mut failures = Vec::new();
        for outcome in outcomes {
            let (index, result) = outcome.expect("agent task panicked");
            match result {
                Ok((thread_id, tool_text, final_text, duration)) => {
                    let marker = format!("agent-{index}");
                    println!(
                        "{marker}: {:.1?}, tool={:?}, final={:?}",
                        duration, tool_text, final_text
                    );
                    if !tool_text.contains(&marker) || final_text != format!("{marker}-done") {
                        failures.push(format!("{marker}: wrong result"));
                    }
                    thread_ids.push(thread_id);
                }
                Err(error) => failures.push(format!("agent-{index}: {error}")),
            }
        }

        println!(
            "{AGENTS} agents finished in {:.1?} (~{:.1?} each)",
            elapsed,
            elapsed / AGENTS as u32
        );

        for thread_id in &thread_ids {
            let response = reqwest::Client::new()
                .post(format!("{endpoint}/close_thread"))
                .json(&serde_json::json!({"thread_id": thread_id}))
                .send()
                .await;
            if let Ok(response) = response {
                println!("close {thread_id}: {}", response.status());
            }
        }

        assert!(failures.is_empty(), "failures: {failures:#?}");
    });
}
