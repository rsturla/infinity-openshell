//! RAP HTTP server.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode, header};
use axum::routing::{get, post};
use axum::{Json, Router};
use bytes::Bytes;
use openshell_session::{ExecRequest, SandboxPath, SessionKey, SessionRuntime, ThreadId};
use rap_protocol::{RapCallback, RapInvocation, RapToolResult};
use serde::Deserialize;
use tokio::sync::Mutex;
use tokio_util::sync::CancellationToken;

use crate::callback::{CallbackClient, CallbackError, PlainCallbackClient};
use crate::config::{InboundAuth, ServerConfig};
use crate::error::Error;
use crate::journal::{Accept, FileJournal, Journal, MemoryJournal};
use crate::toolset;

struct InFlight {
    thread: String,
    token: CancellationToken,
}

struct AppState {
    runtime: SessionRuntime,
    config: ServerConfig,
    journal: Arc<dyn Journal>,
    callback: Arc<dyn CallbackClient>,
    inflight: Mutex<HashMap<String, InFlight>>,
    extra_callbacks: Mutex<HashMap<String, Vec<String>>>,
    closed_threads: Mutex<HashSet<String>>,
    thread_gates: Mutex<HashMap<String, Arc<Mutex<()>>>>,
}

/// Build a RAP router with a plain callback client and a journal selected from
/// [`ServerConfig::journal_path`].
///
/// # Errors
///
/// Returns a configuration, journal, or callback client error.
pub async fn router(runtime: SessionRuntime, config: ServerConfig) -> Result<Router, Error> {
    config.validate()?;
    let callback = Arc::new(
        PlainCallbackClient::new(
            config.callback_timeout,
            config.callback_allowed_hosts.clone(),
            config.allow_insecure_callback_http,
        )
        .map_err(|error| Error::Callback(error.to_string()))?,
    );
    let journal: Arc<dyn Journal> = if let Some(path) = &config.journal_path {
        Arc::new(FileJournal::open(path).await?)
    } else {
        Arc::new(MemoryJournal::new())
    };
    router_with_callback(runtime, config, journal, callback).await
}

/// Build a RAP router with an injected journal and callback client.
///
/// Recovery runs before the router is returned, so a newly acknowledged
/// invocation cannot be mistaken for a record from a previous process.
///
/// # Errors
///
/// Returns a journal error when recovery cannot read or update the journal.
pub async fn router_with_callback(
    runtime: SessionRuntime,
    config: ServerConfig,
    journal: Arc<dyn Journal>,
    callback: Arc<dyn CallbackClient>,
) -> Result<Router, Error> {
    let state = Arc::new(AppState {
        runtime,
        config,
        journal,
        callback,
        inflight: Mutex::new(HashMap::new()),
        extra_callbacks: Mutex::new(HashMap::new()),
        closed_threads: Mutex::new(HashSet::new()),
        thread_gates: Mutex::new(HashMap::new()),
    });
    recover_pending(&state).await?;
    start_outbox(&state);
    Ok(Router::new()
        .route("/.well-known/rap-toolset", get(toolset_handler))
        .route("/invoke", post(invoke_handler))
        .route("/close_thread", post(close_thread_handler))
        .route("/cancel_tool_call", post(cancel_tool_call_handler))
        .route("/healthz", get(health_handler))
        .route("/readyz", get(health_handler))
        .with_state(state))
}

async fn recover_pending(state: &Arc<AppState>) -> Result<(), Error> {
    for item in state.journal.recover().await? {
        let result = match item.result {
            Some(result) => result,
            None => {
                let result = tool_result(
                    &item.invocation,
                    "Error: invocation was interrupted by a server restart; please retry."
                        .to_owned(),
                );
                state.journal.finish(&item.invocation.id, &result).await?;
                result
            }
        };
        let state = state.clone();
        let callback_url = item.invocation.callback_url.clone();
        tokio::spawn(async move {
            deliver_many(&state, &callback_url, &[], &result).await;
        });
    }
    Ok(())
}

fn start_outbox(state: &Arc<AppState>) {
    let weak = Arc::downgrade(state);
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_secs(30));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticker.tick().await;
            let Some(state) = weak.upgrade() else {
                break;
            };
            match state.journal.pending().await {
                Ok(pending) => {
                    for item in pending {
                        deliver_many(&state, &item.invocation.callback_url, &[], &item.result)
                            .await;
                    }
                }
                Err(error) => tracing::error!(%error, "cannot load the callback outbox"),
            }
        }
    });
}

async fn toolset_handler(
    State(state): State<Arc<AppState>>,
) -> Json<rap_protocol::ToolsetManifest> {
    Json(toolset::manifest(&state.config))
}

async fn health_handler() -> StatusCode {
    StatusCode::OK
}

async fn invoke_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(invocation): Json<RapInvocation>,
) -> StatusCode {
    if !authorized(&state.config, &headers) {
        return StatusCode::UNAUTHORIZED;
    }

    match state.journal.accept(&invocation).await {
        Ok(Accept::New) => {}
        Ok(Accept::DuplicatePending) => {
            let original = state
                .journal
                .callback_url(&invocation.id)
                .await
                .ok()
                .flatten();
            if original.as_deref() != Some(invocation.callback_url.as_str()) {
                if let Ok(Some(result)) = state.journal.result(&invocation.id).await {
                    let state = state.clone();
                    let callback_url = invocation.callback_url.clone();
                    tokio::spawn(async move {
                        let _ = deliver_direct(&state, &callback_url, &result).await;
                    });
                } else {
                    let mut callbacks = state.extra_callbacks.lock().await;
                    let entry = callbacks
                        .entry(invocation.id.as_str().to_owned())
                        .or_default();
                    if !entry.contains(&invocation.callback_url) {
                        entry.push(invocation.callback_url.clone());
                    }
                }
            }
            return StatusCode::OK;
        }
        Ok(Accept::DuplicateDelivered) => {
            let original = state
                .journal
                .callback_url(&invocation.id)
                .await
                .ok()
                .flatten();
            if original.as_deref() != Some(invocation.callback_url.as_str())
                && let Ok(Some(result)) = state.journal.result(&invocation.id).await
            {
                let state = state.clone();
                let callback_url = invocation.callback_url.clone();
                tokio::spawn(async move {
                    let _ = deliver_direct(&state, &callback_url, &result).await;
                });
            }
            return StatusCode::OK;
        }
        Ok(Accept::Conflict) => {
            let callback_url = invocation.callback_url.clone();
            let result = tool_result(
                &invocation,
                "Error: conflicting duplicate invocation with the same call ID".to_owned(),
            );
            let state = state.clone();
            tokio::spawn(async move {
                deliver_direct(&state, &callback_url, &result).await;
            });
            return StatusCode::OK;
        }
        Err(error) => {
            tracing::error!(%error, "cannot journal the invocation");
            return StatusCode::SERVICE_UNAVAILABLE;
        }
    }

    let token = CancellationToken::new();
    state.inflight.lock().await.insert(
        invocation.id.as_str().to_owned(),
        InFlight {
            thread: invocation.group_id.as_str().to_owned(),
            token: token.clone(),
        },
    );

    let state = state.clone();
    tokio::spawn(async move {
        process(state, invocation, token).await;
    });
    StatusCode::OK
}

async fn process(state: Arc<AppState>, invocation: RapInvocation, token: CancellationToken) {
    let thread = invocation.group_id.as_str().to_owned();
    let gate = thread_gate(&state, &thread).await;
    let _gate = gate.lock().await;

    let text = if state.closed_threads.lock().await.contains(&thread) {
        "Error: thread is closed".to_owned()
    } else {
        match tokio::select! {
            _ = token.cancelled() => Err(Error::Cancelled),
            result = execute(&state, &invocation) => result,
        } {
            Ok(text) => text,
            Err(error) => format!("Error: {error}"),
        }
    };

    let result = tool_result(&invocation, text);
    if let Err(error) = state.journal.finish(&invocation.id, &result).await {
        tracing::error!(%error, call_id = %invocation.id, "cannot journal the result; not delivering");
        state.inflight.lock().await.remove(invocation.id.as_str());
        return;
    }

    // Delivery can take seconds. Do not hold the thread gate across it, or a
    // close or a queued invocation for the same thread would block.
    drop(_gate);

    let extras = state
        .extra_callbacks
        .lock()
        .await
        .remove(invocation.id.as_str())
        .unwrap_or_default();
    deliver_many(&state, &invocation.callback_url, &extras, &result).await;
    state.inflight.lock().await.remove(invocation.id.as_str());
}

async fn execute(state: &AppState, invocation: &RapInvocation) -> Result<String, Error> {
    match invocation.operation.as_str() {
        "openshell_exec" => {
            let args: ExecArgs = parse_args(invocation)?;
            let session = acquire(state, invocation).await?;
            let mut request = ExecRequest::new(args.command);
            request.workdir = args.cwd;
            request.stdin = args.stdin.map(Bytes::from);
            let output = session.exec(request).await?;
            Ok(render_exec(&output))
        }
        "openshell_read" => {
            let args: ReadArgs = parse_args(invocation)?;
            let path = SandboxPath::new(args.path)
                .map_err(|error| Error::InvalidArguments(error.to_string()))?;
            let session = acquire(state, invocation).await?;
            let bytes = session.read_file(&path).await?;
            let content = String::from_utf8(bytes.to_vec()).map_err(|error| {
                Error::InvalidArguments(format!("file is not valid UTF-8: {error}"))
            })?;
            slice_lines(&content, args.start_line, args.end_line)
        }
        "openshell_write" => {
            let args: WriteArgs = parse_args(invocation)?;
            let path = SandboxPath::new(args.path)
                .map_err(|error| Error::InvalidArguments(error.to_string()))?;
            let session = acquire(state, invocation).await?;
            session.write_file(&path, args.content.into_bytes()).await?;
            Ok(format!("Wrote {}", path.as_str()))
        }
        other => Err(Error::InvalidArguments(format!(
            "unknown operation: {other}"
        ))),
    }
}

fn parse_args<T: serde::de::DeserializeOwned>(invocation: &RapInvocation) -> Result<T, Error> {
    serde_json::from_value(invocation.arguments.clone())
        .map_err(|error| Error::InvalidArguments(error.to_string()))
}

async fn acquire(
    state: &AppState,
    invocation: &RapInvocation,
) -> Result<openshell_session::Session, Error> {
    let key = session_key(&state.config, invocation)?;
    Ok(state.runtime.acquire(key).await?)
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Delivery {
    Delivered,
    Retry,
    Terminal,
}

async fn deliver_many(state: &AppState, primary: &str, extras: &[String], result: &RapToolResult) {
    let mut delivery = deliver_direct(state, primary, result).await;
    for url in extras {
        if url != primary {
            delivery = merge_delivery(delivery, deliver_direct(state, url, result).await);
        }
    }
    if matches!(delivery, Delivery::Delivered | Delivery::Terminal)
        && let Err(error) = state.journal.mark_delivered(&result.id).await
    {
        tracing::warn!(%error, call_id = %result.id, "cannot mark the result delivered");
    }
}

async fn deliver_direct(state: &AppState, callback_url: &str, result: &RapToolResult) -> Delivery {
    let callback = RapCallback::ToolResult(result.clone());
    let mut delay = state.config.callback_backoff;
    for attempt in 0..state.config.callback_attempts {
        match state.callback.post(callback_url, &callback).await {
            Ok(()) => return Delivery::Delivered,
            Err(CallbackError::Rejected(code)) if (400..500).contains(&code) => {
                tracing::warn!(
                    call_id = %result.id,
                    code,
                    "callback receiver rejected the result; not retrying"
                );
                return Delivery::Terminal;
            }
            Err(error) => {
                tracing::warn!(
                    attempt,
                    call_id = %result.id,
                    %error,
                    "callback delivery failed"
                );
                if attempt + 1 < state.config.callback_attempts {
                    tokio::time::sleep(delay).await;
                    delay = delay.saturating_mul(2);
                }
            }
        }
    }
    tracing::error!(call_id = %result.id, "callback delivery exhausted");
    Delivery::Retry
}

fn merge_delivery(left: Delivery, right: Delivery) -> Delivery {
    match (left, right) {
        (Delivery::Delivered, _) | (_, Delivery::Delivered) => Delivery::Delivered,
        (Delivery::Terminal, _) | (_, Delivery::Terminal) => Delivery::Terminal,
        _ => Delivery::Retry,
    }
}

async fn close_thread_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(request): Json<ThreadRequest>,
) -> StatusCode {
    if !authorized(&state.config, &headers) {
        return StatusCode::UNAUTHORIZED;
    }
    let thread = request.thread_id.as_str().to_owned();
    state.closed_threads.lock().await.insert(thread.clone());

    let tokens: Vec<_> = state
        .inflight
        .lock()
        .await
        .iter()
        .filter(|(_, inflight)| inflight.thread == thread)
        .map(|(_, inflight)| inflight.token.clone())
        .collect();
    for token in tokens {
        token.cancel();
    }

    let gate = thread_gate(&state, &thread).await;
    let _gate = gate.lock().await;
    let Ok(thread_id) = ThreadId::new(thread) else {
        return StatusCode::OK;
    };
    let key = SessionKey::new(state.config.tenant.clone(), thread_id);
    if let Err(error) = state.runtime.close(&key).await {
        tracing::warn!(%error, "thread cleanup failed");
    }
    StatusCode::OK
}

async fn cancel_tool_call_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Json(request): Json<CancelRequest>,
) -> StatusCode {
    if !authorized(&state.config, &headers) {
        return StatusCode::UNAUTHORIZED;
    }
    let inflight = state
        .inflight
        .lock()
        .await
        .remove(request.tool_call_id.as_str());
    if let Some(inflight) = inflight {
        inflight.token.cancel();
    }
    StatusCode::OK
}

async fn thread_gate(state: &AppState, thread: &str) -> Arc<Mutex<()>> {
    let mut gates = state.thread_gates.lock().await;
    gates
        .entry(thread.to_owned())
        .or_insert_with(|| Arc::new(Mutex::new(())))
        .clone()
}

#[derive(Deserialize)]
struct ThreadRequest {
    thread_id: rap_protocol::ThreadId,
}

#[derive(Deserialize)]
struct CancelRequest {
    #[allow(dead_code)]
    thread_id: rap_protocol::ThreadId,
    tool_call_id: rap_protocol::ToolCallId,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecArgs {
    command: Vec<String>,
    cwd: Option<String>,
    stdin: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ReadArgs {
    path: String,
    start_line: Option<u64>,
    end_line: Option<u64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WriteArgs {
    path: String,
    content: String,
}

fn session_key(config: &ServerConfig, invocation: &RapInvocation) -> Result<SessionKey, Error> {
    let thread = ThreadId::new(invocation.group_id.as_str())
        .map_err(|error| Error::InvalidArguments(error.to_string()))?;
    Ok(SessionKey::new(config.tenant.clone(), thread))
}

fn tool_result(invocation: &RapInvocation, text: String) -> RapToolResult {
    RapToolResult {
        group_id: invocation.group_id.clone(),
        id: invocation.id.clone(),
        call_id: invocation.call_id.clone(),
        text: Some(text),
        content: None,
        display_as: None,
        subscription: None,
    }
}

fn render_exec(output: &openshell_session::ExecOutput) -> String {
    let mut text = String::new();
    text.push_str(&String::from_utf8_lossy(&output.stdout));
    if !output.stderr.is_empty() {
        if !text.is_empty() && !text.ends_with('\n') {
            text.push('\n');
        }
        text.push_str("[stderr]\n");
        text.push_str(&String::from_utf8_lossy(&output.stderr));
    }
    if text.is_empty() {
        text = format!("Command exited with status {}.", output.code);
    } else if !text.ends_with('\n') {
        text.push('\n');
    }
    text.push_str(&format!("[exit {}]", output.code));
    text
}

fn slice_lines(content: &str, start: Option<u64>, end: Option<u64>) -> Result<String, Error> {
    let Some(start) = start else {
        if end.is_some() {
            return Err(Error::InvalidArguments(
                "end_line requires start_line".to_owned(),
            ));
        }
        return Ok(content.to_owned());
    };
    if start == 0 {
        return Err(Error::InvalidArguments(
            "start_line must be at least 1".to_owned(),
        ));
    }
    let end = end.unwrap_or(u64::MAX);
    if end < start {
        return Err(Error::InvalidArguments(
            "end_line must be greater than or equal to start_line".to_owned(),
        ));
    }
    Ok(content
        .lines()
        .enumerate()
        .filter(|(index, _)| {
            let line = *index as u64 + 1;
            line >= start && line <= end
        })
        .map(|(_, line)| line)
        .collect::<Vec<_>>()
        .join("\n"))
}

fn authorized(config: &ServerConfig, headers: &HeaderMap) -> bool {
    match &config.inbound_auth {
        InboundAuth::None => true,
        InboundAuth::Bearer { token } => headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .and_then(|value| value.strip_prefix("Bearer "))
            .is_some_and(|value| constant_time_eq(value.as_bytes(), token.as_bytes())),
    }
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (*left ^ *right)
        })
        == 0
}
