//! Test harness for the Infinity OpenShell RAP server.
//!
//! The kit wires a [`FakeGateway`] to the real RAP router. It is the same seam
//! the production server uses, so an example or test exercises the actual
//! discovery, journal, and callback paths without a gateway.

#![deny(missing_docs)]

use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use axum::Router;
use infinity_openshell::{
    CallbackClient, CallbackError, MemoryJournal, ServerConfig, router_with_callback,
};
use openshell_session::{GatewayAuth, GatewayConfig, RuntimeConfig, SessionRuntime, TenantId};
use rap_protocol::{
    ProviderCallId, RapCallback, RapInvocation, RapSubscriptionEvent, RapToolResult, ThreadId,
    ToolCallId,
};
use tokio::sync::mpsc;
use url::Url;

pub use openshell_session::fake::FakeGateway;

/// Callback client that records results and subscription events.
#[derive(Clone)]
pub struct RecordingCallback {
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

/// A running RAP server over the fake gateway.
pub struct TestServer {
    app: Router,
    gateway: Arc<FakeGateway>,
    results: mpsc::UnboundedReceiver<RapToolResult>,
    events: mpsc::UnboundedReceiver<RapSubscriptionEvent>,
}

impl TestServer {
    /// Build a server with the default test configuration.
    ///
    /// # Panics
    ///
    /// Panics only on an internal invariant violation in the testkit.
    pub async fn start() -> Self {
        Self::start_with(|config| {
            config.stream_interval = Duration::from_millis(5);
            config.callback_attempts = 2;
            config.callback_backoff = Duration::from_millis(1);
        })
        .await
    }

    /// Build a server and customize its configuration.
    ///
    /// # Panics
    ///
    /// Panics only on an internal invariant violation in the testkit.
    pub async fn start_with(configure: impl FnOnce(&mut ServerConfig)) -> Self {
        let gateway = Arc::new(FakeGateway::new());
        let runtime = SessionRuntime::new(
            gateway.clone(),
            RuntimeConfig::new(GatewayConfig::new(
                Url::parse("https://gateway.example.com").expect("valid test URL"),
                "default",
                GatewayAuth::None,
            )),
        )
        .expect("test runtime configuration");

        let mut config = ServerConfig::ephemeral(
            Url::parse("http://tools.test").expect("valid test URL"),
            TenantId::new("test").expect("valid tenant"),
        );
        configure(&mut config);

        let (results, result_receiver) = mpsc::unbounded_channel();
        let (events, event_receiver) = mpsc::unbounded_channel();
        let callback = Arc::new(RecordingCallback { results, events });
        let app = router_with_callback(runtime, config, Arc::new(MemoryJournal::new()), callback)
            .await
            .expect("test router");

        Self {
            app,
            gateway,
            results: result_receiver,
            events: event_receiver,
        }
    }

    /// Return the router for `tower::ServiceExt::oneshot`.
    pub fn app(&self) -> &Router {
        &self.app
    }

    /// Return the fake gateway.
    pub fn gateway(&self) -> &Arc<FakeGateway> {
        &self.gateway
    }

    /// Wait for the next tool result.
    ///
    /// # Panics
    ///
    /// Panics when the callback does not arrive within two seconds.
    pub async fn next_result(&mut self) -> RapToolResult {
        tokio::time::timeout(Duration::from_secs(2), self.results.recv())
            .await
            .expect("tool result timed out")
            .expect("result channel closed")
    }

    /// Wait for the next subscription event.
    ///
    /// # Panics
    ///
    /// Panics when the event does not arrive within two seconds.
    pub async fn next_event(&mut self) -> RapSubscriptionEvent {
        tokio::time::timeout(Duration::from_secs(2), self.events.recv())
            .await
            .expect("subscription event timed out")
            .expect("event channel closed")
    }
}

/// Build a RAP invocation for tests and examples.
pub fn invocation(
    id: impl Into<String>,
    operation: impl Into<String>,
    arguments: serde_json::Value,
    callback_url: impl Into<String>,
    thread: impl Into<String>,
) -> RapInvocation {
    RapInvocation {
        operation: operation.into(),
        arguments,
        id: ToolCallId::from(id.into()),
        call_id: None::<ProviderCallId>,
        callback_url: callback_url.into(),
        group_id: ThreadId::from(thread.into()),
        user_id: None,
        thread_ancestors: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use tower::ServiceExt;

    #[tokio::test]
    async fn discovery_serves_the_toolset() {
        let server = TestServer::start().await;
        let response = server
            .app()
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
    }

    #[tokio::test]
    async fn exec_round_trip() {
        let mut server = TestServer::start().await;
        server.gateway().set_next_stdout("from-the-kit\n");
        let invocation = invocation(
            "call-1",
            "openshell_exec",
            serde_json::json!({"command": ["printf", "from-the-kit"]}),
            "http://callback.test/",
            "thread-1",
        );
        let response = server
            .app()
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/invoke")
                    .header("content-type", "application/json")
                    .body(Body::from(serde_json::to_vec(&invocation).unwrap()))
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        let result = server.next_result().await;
        assert!(result.text.unwrap().contains("from-the-kit"));
    }
}
