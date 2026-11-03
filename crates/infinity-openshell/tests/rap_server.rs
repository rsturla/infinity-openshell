//! End-to-end RAP server tests over the fake gateway.

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use openshell_session::fake::FakeGateway;
use openshell_session::{GatewayAuth, GatewayConfig, RuntimeConfig, SessionRuntime, TenantId};
use rap_protocol::{
    ProviderCallId, RapCallback, RapInvocation, RapSubscriptionEvent, RapToolResult, ThreadId,
    ToolCallId, ToolsetManifest,
};
use tokio::sync::mpsc;
use tower::ServiceExt;
use url::Url;

use infinity_openshell::{
    CallbackClient, CallbackError, FileJournal, InboundAuth, Journal, MemoryJournal, ServerConfig,
    router_with_callback,
};

#[derive(Clone)]
struct RecordingCallback {
    results: mpsc::UnboundedSender<RapToolResult>,
    events: mpsc::UnboundedSender<RapSubscriptionEvent>,
}

#[async_trait]
impl CallbackClient for RecordingCallback {
    async fn post(&self, _url: &str, callback: &RapCallback) -> Result<(), CallbackError> {
        match callback {
            RapCallback::ToolResult(result) => {
                let _ = self.results.send(result.clone());
            }
            RapCallback::SubscriptionEvent(event) => {
                let _ = self.events.send(event.clone());
            }
            _ => {}
        }
        Ok(())
    }
}

struct Harness {
    app: Router,
    gateway: Arc<FakeGateway>,
    results: mpsc::UnboundedReceiver<RapToolResult>,
    events: mpsc::UnboundedReceiver<RapSubscriptionEvent>,
}

async fn harness_with_auth(auth: InboundAuth) -> Harness {
    harness_with_auth_and_journal(auth, Arc::new(MemoryJournal::new())).await
}

async fn harness_with_auth_and_journal(auth: InboundAuth, journal: Arc<dyn Journal>) -> Harness {
    let gateway = Arc::new(FakeGateway::new());
    let gateway_config = GatewayConfig::new(
        Url::parse("https://gateway.example.com").unwrap(),
        "default",
        GatewayAuth::None,
    );
    let runtime = SessionRuntime::new(gateway.clone(), RuntimeConfig::new(gateway_config)).unwrap();

    let mut config = ServerConfig::ephemeral(
        Url::parse("http://tools.test").unwrap(),
        TenantId::new("acme").unwrap(),
    );
    config.inbound_auth = auth;
    config.callback_attempts = 2;
    config.callback_backoff = Duration::from_millis(1);
    config.stream_interval = Duration::from_millis(5);

    let (results, receiver) = mpsc::unbounded_channel();
    let (events, event_receiver) = mpsc::unbounded_channel();
    let callback = Arc::new(RecordingCallback { results, events });
    let app = router_with_callback(runtime, config, journal, callback)
        .await
        .unwrap();
    Harness {
        app,
        gateway,
        results: receiver,
        events: event_receiver,
    }
}

async fn harness() -> Harness {
    harness_with_auth(InboundAuth::None).await
}

fn invocation(id: &str, operation: &str, arguments: serde_json::Value) -> RapInvocation {
    RapInvocation {
        operation: operation.to_owned(),
        arguments,
        id: ToolCallId::from(id),
        call_id: None::<ProviderCallId>,
        callback_url: "http://callback.test/".to_owned(),
        group_id: ThreadId::from("thread-1"),
        user_id: None,
        thread_ancestors: None,
    }
}

async fn invoke(app: &Router, invocation: &RapInvocation) -> StatusCode {
    let body = serde_json::to_vec(invocation).unwrap();
    let request = Request::builder()
        .method("POST")
        .uri("/invoke")
        .header("content-type", "application/json")
        .body(Body::from(body))
        .unwrap();
    app.clone().oneshot(request).await.unwrap().status()
}

async fn next_result(results: &mut mpsc::UnboundedReceiver<RapToolResult>) -> RapToolResult {
    tokio::time::timeout(Duration::from_secs(2), results.recv())
        .await
        .expect("callback timed out")
        .expect("callback channel closed")
}

fn count(gateway: &FakeGateway, operation: &str) -> usize {
    gateway
        .calls()
        .iter()
        .filter(|call| call.as_str() == operation)
        .count()
}

#[tokio::test]
async fn discovery_advertises_the_configured_endpoint() {
    let harness = harness().await;
    let response = harness
        .app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/.well-known/rap-toolset")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    let manifest: ToolsetManifest = serde_json::from_slice(&body).unwrap();
    assert_eq!(manifest.endpoint, "http://tools.test/invoke");
    assert_eq!(manifest.tools.len(), 3);
}

#[tokio::test]
async fn exec_runs_in_the_sandbox_and_delivers_a_result() {
    let mut harness = harness().await;
    harness.gateway.set_next_stdout("hello\n");
    let invocation = invocation(
        "call-1",
        "openshell_exec",
        serde_json::json!({"command": ["printf", "hello"]}),
    );
    assert_eq!(invoke(&harness.app, &invocation).await, StatusCode::OK);
    let result = next_result(&mut harness.results).await;
    assert!(
        result.text.as_deref().unwrap().contains("hello"),
        "result: {result:?}"
    );
    assert_eq!(count(&harness.gateway, "create"), 1);
    assert_eq!(count(&harness.gateway, "exec"), 1);
    let execs = harness.gateway.all_execs();
    assert_eq!(
        execs[0].request_id.as_deref(),
        Some(openshell_session::request_id_from("call-1").as_str())
    );
}

#[tokio::test]
async fn exec_emits_subscription_events_before_the_result() {
    let mut harness = harness().await;
    harness.gateway.set_next_stdout("streamed\n");
    harness.gateway.set_exec_delay(Duration::from_millis(25));
    let invocation = invocation(
        "call-stream",
        "openshell_exec",
        serde_json::json!({"command": ["printf", "streamed"]}),
    );
    assert_eq!(invoke(&harness.app, &invocation).await, StatusCode::OK);

    let event = tokio::time::timeout(Duration::from_secs(2), harness.events.recv())
        .await
        .expect("subscription event timed out")
        .expect("event channel closed");
    assert!(event.text.contains("streamed"), "event: {event:?}");
    assert_eq!(event.r#final, None);

    let final_event = tokio::time::timeout(Duration::from_secs(2), harness.events.recv())
        .await
        .expect("final subscription event timed out")
        .expect("event channel closed");
    assert_eq!(final_event.r#final, Some(true));

    let result = next_result(&mut harness.results).await;
    assert!(result.text.as_deref().unwrap().contains("streamed"));
}

#[tokio::test]
async fn cancel_interrupts_a_streaming_command() {
    let mut harness = harness().await;
    harness.gateway.set_next_stdout("partial\n");
    harness.gateway.set_exec_delay(Duration::from_millis(200));
    let invocation = invocation(
        "call-cancel",
        "openshell_exec",
        serde_json::json!({"command": ["sleep", "10"]}),
    );
    assert_eq!(invoke(&harness.app, &invocation).await, StatusCode::OK);

    tokio::time::sleep(Duration::from_millis(20)).await;
    let response = harness
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/cancel_tool_call")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"thread_id":"thread-1","tool_call_id":"call-cancel"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);

    let result = next_result(&mut harness.results).await;
    assert!(
        result.text.as_deref().unwrap().contains("cancelled"),
        "result: {result:?}"
    );
}

#[tokio::test]
async fn output_is_capped_and_marked_truncated() {
    let mut harness = harness().await;
    harness.gateway.set_next_stdout(vec![b'x'; 2 * 1024 * 1024]);
    let invocation = invocation(
        "call-large",
        "openshell_exec",
        serde_json::json!({"command": ["cat", "large"]}),
    );
    assert_eq!(invoke(&harness.app, &invocation).await, StatusCode::OK);
    let result = next_result(&mut harness.results).await;
    let text = result.text.unwrap();
    assert!(
        text.len() <= 1024 * 1024 + 64,
        "result length {}",
        text.len()
    );
    assert!(text.contains("[truncated]"));
}

#[tokio::test]
async fn invalid_arguments_fail_without_creating_a_sandbox() {
    let mut harness = harness().await;
    let invocation = invocation(
        "call-1",
        "openshell_exec",
        serde_json::json!({"command": "not-an-array"}),
    );
    assert_eq!(invoke(&harness.app, &invocation).await, StatusCode::OK);
    let result = next_result(&mut harness.results).await;
    assert!(result.text.as_deref().unwrap().starts_with("Error: "));
    assert_eq!(count(&harness.gateway, "create"), 0);
}

#[tokio::test]
async fn write_then_read_preserves_the_file_across_calls() {
    let mut harness = harness().await;
    let write = invocation(
        "call-write",
        "openshell_write",
        serde_json::json!({"path": "notes.txt", "content": "persisted\n"}),
    );
    assert_eq!(invoke(&harness.app, &write).await, StatusCode::OK);
    let result = next_result(&mut harness.results).await;
    assert_eq!(result.text.as_deref(), Some("Wrote notes.txt"));

    let read = invocation(
        "call-read",
        "openshell_read",
        serde_json::json!({"path": "notes.txt"}),
    );
    assert_eq!(invoke(&harness.app, &read).await, StatusCode::OK);
    let result = next_result(&mut harness.results).await;
    assert_eq!(result.text.as_deref(), Some("persisted\n"));
    assert_eq!(count(&harness.gateway, "create"), 1);
}

#[tokio::test]
async fn close_thread_deletes_the_sandbox_and_always_returns_200() {
    let mut harness = harness().await;
    let exec = invocation(
        "call-1",
        "openshell_exec",
        serde_json::json!({"command": ["true"]}),
    );
    invoke(&harness.app, &exec).await;
    let _ = next_result(&mut harness.results).await;

    for _ in 0..2 {
        let response = harness
            .app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/close_thread")
                    .header("content-type", "application/json")
                    .body(Body::from(r#"{"thread_id":"thread-1"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
    }
    assert_eq!(count(&harness.gateway, "delete"), 1);
}

#[tokio::test]
async fn duplicate_invocation_runs_once() {
    let mut harness = harness().await;
    let invocation = invocation(
        "call-1",
        "openshell_exec",
        serde_json::json!({"command": ["true"]}),
    );
    assert_eq!(invoke(&harness.app, &invocation).await, StatusCode::OK);
    assert_eq!(invoke(&harness.app, &invocation).await, StatusCode::OK);

    let _ = next_result(&mut harness.results).await;
    tokio::time::sleep(Duration::from_millis(50)).await;
    assert!(
        tokio::time::timeout(Duration::from_millis(100), harness.results.recv())
            .await
            .is_err()
    );
    assert_eq!(count(&harness.gateway, "exec"), 1);
}

#[tokio::test]
async fn bearer_auth_rejects_a_missing_token() {
    let harness = harness_with_auth(InboundAuth::Bearer {
        token: "shared-secret".to_owned(),
    })
    .await;
    let invocation = invocation(
        "call-1",
        "openshell_exec",
        serde_json::json!({"command": ["true"]}),
    );
    assert_eq!(
        invoke(&harness.app, &invocation).await,
        StatusCode::UNAUTHORIZED
    );
}

#[tokio::test]
async fn startup_recovers_an_interrupted_invocation() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("journal.jsonl");
    let invocation = invocation(
        "call-interrupted",
        "openshell_exec",
        serde_json::json!({"command": ["true"]}),
    );
    {
        let journal = FileJournal::open(&path).await.unwrap();
        journal.accept(&invocation).await.unwrap();
    }

    let journal = FileJournal::open(&path).await.unwrap();
    let mut harness = harness_with_auth_and_journal(InboundAuth::None, Arc::new(journal)).await;
    let result = next_result(&mut harness.results).await;
    assert!(
        result.text.as_deref().unwrap().contains("interrupted"),
        "result: {result:?}"
    );
}

#[tokio::test]
async fn cancel_tool_call_always_returns_200() {
    let harness = harness().await;
    let response = harness
        .app
        .clone()
        .oneshot(
            Request::builder()
                .method("POST")
                .uri("/cancel_tool_call")
                .header("content-type", "application/json")
                .body(Body::from(
                    r#"{"thread_id":"thread-1","tool_call_id":"missing"}"#,
                ))
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(response.status(), StatusCode::OK);
}
