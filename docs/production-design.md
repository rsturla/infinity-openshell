# Production design: elastic OpenShell sandboxes for Infinity agents

Status: design proposal. Supersedes the prototype in `src/`.

## 1. Summary

Build a small workspace of crates, not one crate:

| Crate | Role | Depends on Infinity? |
|---|---|---|
| `openshell-session` | Lease-managed OpenShell sandbox sessions: create, exec, read, write, idle, wake, delete | No |
| `infinity-openshell` | RAP server adapter, streaming, cancellation, durable invocation journal, callback outbox | Yes (`rap-protocol`) |
| `infinity-openshell-testkit` | Deterministic fake gateway, virtual clock, fault injection, contract helpers | No |

The binary teams run is their **agent binary**. The OpenShell tool server is a
library mounted into it, not a separate product. A standalone server process is
only needed when the agent runtime is request-scoped and exits after the
acknowledgement.

The core idea is a **session lease**. A caller holds a lease for the duration of
one tool call. The runtime keeps the sandbox `Ready` while any lease is held,
stops it after the last lease is released plus an idle window, and deletes it only
after a retention window or an explicit thread close. A new call starts or resumes
the same sandbox, with the same filesystem.

This is the semantic the prototype was reaching for. It is not one sandbox per
call, and it is not an immortal sandbox per thread.

```
infinity-openshell/
├── crates/openshell-session/
├── crates/infinity-openshell/          # library only
├── crates/infinity-openshell-testkit/
├── examples/
│   ├── agent/                          # one binary: Infinity agent + embedded tool server
│   └── split-service/                  # request-scoped agent + separate long-lived server
├── deploy/                             # agent image, Helm, OpenShell policy
├── docs/
├── Cargo.toml            # workspace
├── deny.toml
├── rust-toolchain.toml
└── .github/workflows/
```

### 1.1 Delivery shape: the agent binary embeds the tool server

The binary teams run is their **agent binary**. The OpenShell tool server is a
library that binary mounts, not a second product. There is no separate
"tool server binary" to deploy.

```rust
// inside the agent binary
let runtime = SessionRuntime::open(runtime_config).await?;
let server = EmbeddedServer::start(runtime, server_config).await?;

let toolset = RapToolSet::connect([server.url()], &thread, callback_url).await?;
```

`EmbeddedServer::start` binds `127.0.0.1:0`, serves `router(...)`, and returns
the base URL plus a drain handle. The agent registers that URL like any other
RAP server. One process, one deployment, one binary.

There is one constraint, and it comes from Infinity, not from this crate: the
tool server must outlive the HTTP acknowledgement. A long-lived agent process
(daemon, container, VM) does. A request-scoped slice (Lambda) does not: the
slice exits after acknowledging the tool call, so an in-process server dies
before the work finishes.

| Agent runtime | Tool server shape | Why |
|---|---|---|
| Daemon, container, or VM (long-lived) | Embedded in the agent binary | The process outlives the acknowledgement |
| Lambda or other request-scoped slice | Separate long-lived service, or a separate Lambda with a durable queue | The process exits after the acknowledgement |

Who owns the agent's `main` decides whether a true single binary is possible:

- **The team builds its own agent application** with `infinity-agent-core` (or
  `AgentSystemBuilder`). It controls `main`, so it can embed the tool server.
  This is the one-binary shape.
- **The team uses the stock `infinity` daemon or CLI.** It does not control that
  `main`, so the tool server runs as a subprocess registered with
  `toolset_command`, or as an external `toolset_server` URL. That is still one
  deployment the team operates, but two processes. It is not a second product.

The library API is the same in all cases:

```rust
pub fn router(runtime: SessionRuntime, config: ServerConfig) -> axum::Router;
pub async fn serve(runtime: SessionRuntime, config: ServerConfig, listener: TcpListener) -> Result<(), Error>;
pub struct EmbeddedServer; // ::start(...) -> { url(), drain() }
```

- `EmbeddedServer::start(...)` is the one-binary shape.
- `serve(...)` is the split shape.
- `router(...)` is for mounting in an existing axum app.

What the library must expose for every shape:

- `router(...)`, `EmbeddedServer::start(...)`, `serve(...)`.
- `health_router()` for `/healthz` and `/readyz`.
- A `Metrics` trait and a no-op implementation.
- `shutdown_signal()` and a `DrainGuard` that stops accepting invocations and
  waits for in-flight work.
- `ServerConfig` with validation, so the embedder does not reimplement checks.

Do not ship a separate "tool server product" binary as the default. The only
reason to run a standalone process is the request-scoped topology, and that
process should use the same library and the same `serve(...)` path.

## 2. What "a great Rust crate" means here

The checklist comes from the Rust API Guidelines, the Cargo feature rules, and
current release practice (`cargo-semver-checks`, `cargo-public-api`, `cargo-deny`).
Each principle below maps to a concrete decision in this design.

| Principle | Decision |
|---|---|
| Deep module: small interface, large behaviour | Callers see `acquire -> Session -> exec/release`. Lifecycle, single-flight, retries, and phase handling are hidden. |
| Illegal states unrepresentable | Newtypes for `SessionKey`, `SandboxName`, `SandboxPath`, `Generation`; enums for policy and phase; no `bool` flags in public signatures. |
| Private fields, `#[non_exhaustive]` on config and errors | All public structs have private fields and builders. Enums that may grow are `#[non_exhaustive]`. |
| Errors are values, not strings | `thiserror` enums with stable `kind()`, `is_retryable()`, and `source()`. No `anyhow` in public APIs. |
| Async hygiene | No lock held across `.await`; every network call has a timeout; futures are `Send`; cancellation behaviour is documented and tested. |
| Features are additive and small | `default = []` for the core. Optional `mtls`, `oidc`, `sqlite-journal`, `postgres-journal`, `sigv4`, `metrics`, `coding-tools`. No binary-only feature and no mutually exclusive features. |
| Semver is enforced, not hoped for | `cargo-semver-checks` in CI, `cargo-public-api` diff on every PR, MSRV job, `CHANGELOG.md`. |
| Docs are the contract | `#![deny(missing_docs)]`, crate-level quickstart, one doctest per public type, architecture and lifecycle diagrams. |
| Tests can fail | A fake gateway and a virtual clock drive every state transition. Live tests are gated by an env var **and** run in a release CI job, so they cannot silently skip forever. |
| Operable by default | `tracing` spans, a `Metrics` trait, `/healthz`, graceful shutdown, bounded queues, explicit limits. |
| Supply chain | `cargo-deny` (licenses, advisories, duplicates), `cargo-audit`, Apache-2.0, pinned upstream revisions. |
| No hidden global state | All dependencies are passed in. `SessionRuntime::open(config)` is the only constructor. |

Two principles are especially important for this domain:

- **The interface is the test surface.** The fake gateway implements the same
  `Gateway` trait as the real one, so the state machine is tested through the
  public seam, not around it.
- **One adapter means a hypothetical seam.** `Gateway` exists because there are
  already two real adapters: the OpenShell SDK and the fake used by every test.

## 3. Domain model

### 3.1 Identity

```rust
pub struct TenantId(Arc<str>);      // opaque, validated
pub struct ThreadId(Arc<str>);      // Infinity group_id
pub struct SessionKey { tenant: TenantId, thread: ThreadId }

pub struct SandboxName(Arc<str>);   // gateway-safe, derived, never user input
pub struct Generation(u64);         // bumped on every create
```

The name is derived, not concatenated:

```
i-{base32(sha256(tenant || 0x00 || thread || 0x00 || generation))[..17]}
```

OpenShell caps sandbox names at 19 characters. The `i-` prefix leaves 17
lowercase base32 characters, which is 85 bits. A 40-bit hash is not enough; 128
bits does not fit in a 19-character DNS label. Spend the full budget and include
the generation. Never reuse a name after deletion, so a late request cannot
attach to a replacement sandbox.

Labels carry ownership, not identity reversal:

```
app.kubernetes.io/managed-by = infinity-openshell
infinity.hydro.run/runtime  = <semver>
infinity.hydro.run/tenant   = <hash>
infinity.hydro.run/thread   = <hash>
infinity.hydro.run/gen      = <generation>
infinity.hydro.run/owner    = <replica-id>   (HA)
```

Raw tenant and thread IDs do not go into labels. The journal keeps the mapping.

These are the Infinity deployment labels. `openshell-session` is
product-neutral: it defaults to `managed-by=openshell-session` and an
`openshell-session/*` label prefix. The `infinity-openshell` crate applies the
Infinity policy with `configure_labels`, and CI checks that the core has no
Infinity crate dependency.

### 3.2 Desired and observed state

```rust
pub enum Desired { Active, Idle, Deleted }

#[non_exhaustive]
pub enum Phase {
    Absent,
    Provisioning,
    Ready,
    Stopping,
    Stopped,
    Starting,
    Suspended,
    Error { exit_code: Option<i32> },
    Deleting,
}
```

Observed state comes from OpenShell. Desired state comes from leases and policy.
The reconciler converges one to the other. The session actor is the only writer
for a key, so transitions are serialized.

### 3.3 Invariants

1. A sandbox is never stopped or deleted while `leases > 0`.
2. A sandbox is never deleted before `retain_for` elapses after it became idle,
   unless `/close_thread` was received.
3. A lease is bound to a `(name, generation)`. A stale lease is rejected with
   `StaleSession`, never silently attached to a new sandbox.
4. One `SessionKey` has at most one in-flight lifecycle transition.
5. A `Phase::Error` sandbox is never treated as usable. It is stopped and started,
   or deleted and recreated if storage is gone.
6. `Phase::Provisioning`, `Starting`, and `Deleting` are waited on, not assumed
   usable. This is where the prototype's `ensure_thread_sandbox` was unsafe.
7. After a crash, adopted sandboxes are stopped, not deleted. Their filesystem is
   the durable state.

### 3.4 Lifecycle

```
                 acquire                 idle_after               retain_for
Absent ──create──▶ Ready ──release──▶ timer ──stop──▶ Stopped ──timer──▶ Deleting ──▶ Absent
                    ▲                                     │
                    └──────────── start / resume ─────────┘

Ready / Stopped / Suspended ──/close_thread──▶ Deleting ──▶ Absent

Any state ──unrecoverable error──▶ Error ──stop,start──▶ Ready
                                         └─delete,create─▶ Ready
```

`Stop` is the default wake strategy because it is portable across OpenShell
drivers and retains the persistent workspace. `Suspend`/`Resume` is opt-in and
is probed at startup; it preserves process memory but is not guaranteed by every
driver. The design must never depend on process memory for correctness.

### 3.5 The elastic sequence

```
Agent turn 1
  runtime -> RAP /invoke
  RAP     -> session.acquire(key)       # create, wait_ready
  RAP     -> session.exec(...)          # stream to subscription events
  RAP     -> callback(tool_result)
  RAP     -> drop(session)              # lease count -> 0, idle timer starts
  runtime -> session.stop()             # after idle_after, workspace retained

Agent turn 2, hours later
  runtime -> RAP /invoke
  RAP     -> session.acquire(key)       # start, wait_ready
  RAP     -> session.exec(...)          # same filesystem, same thread state

Thread ends
  runtime -> RAP /close_thread          # always 200
  RAP     -> session.close(key)         # delete, purge credentials
```

This is what "live as long as they need to, wait, then spin up again" means in
practice.

### 3.6 Relationship to Infinity's `SandboxBackend`

The concept of a pluggable sandbox backend is right. The current
`sandbox_core::sandbox::SandboxBackend` interface is not usable as the seam for a
remote provider. Three independent mismatches:

**1. It is a local-workspace trait.** `create_sandbox` returns a `PathBuf`. The
core then does direct filesystem work on that path rather than calling the
backend:

```rust
// crates/sandbox-core/src/server.rs
let bytes = tokio::fs::read(&file_path).await?;            // read_file
let content = tokio::fs::read_to_string(&file_path).await?; // edit_file
tokio::fs::write(&file_path, &new_content).await?;          // edit_file
tokio::process::Command::new(cmd_parts[0])                  // grep runs rg locally
```

Implementing `SandboxBackend` for OpenShell would not route `read_file`,
`edit_file`, `create_file`, or `grep` into OpenShell. They would still hit
whatever local path `create_sandbox` returned. Sync-before-and-after is the only
workaround, and that is incorrect under concurrency and expensive for large
files.

**2. It is a local-process trait.** `spawn_command` returns
`SpawnedCommand { child: tokio::process::Child }`. The core takes
`child.stdout`/`child.stderr`, waits on `child.wait()`, and on cancellation sends
`SIGTERM` to a local process group by PID. An OpenShell exec is a gRPC stream,
not a `Child`; a local proxy process would be killed while the remote process
keeps running, so cancellation would silently fail.

**3. Its lifecycle is per-operation, not elastic.** `with_sandbox` creates a
sandbox, runs the action, pushes, and cleans it up on **every** call:

```rust
let sandbox_dir = state.backend.create_sandbox(&repo_state).await?;
let result = action(sandbox_dir.clone()).await;
state.backend.push_sandbox(&sandbox_dir, group_id, description).await?;
state.backend.cleanup_sandbox(&sandbox_dir).await?;
```

There is no `ensure`, `start`, `stop`, `idle`, or `wake`. The trait's contract is
"create a fresh workspace for this operation", which is the opposite of "keep
this thread's workspace, stop it when idle, and start it when work arrives". It
is also built around `RepoState`, jj/git bookmarks, and `ModeProvider`, because
Infinity's sandbox is a repo worktree manager. OpenShell is a general execution
sandbox.

There are three ways forward:

| Option | What it means | Verdict |
|---|---|---|
| A. Implement the trait as-is | Local staging mirror plus a proxy child | Rejected. File I/O still bypasses the backend, and cancellation kills the proxy, not the workload. |
| B. Widen the seam upstream | Add `SandboxFs`, `SandboxProcess`, and lifecycle methods, then implement them for OpenShell | Best end state. Inherits the RAP server, tools, streaming, cancellation, approvals, and migration. Costs a real `sandbox-core` refactor. |
| C. Invert the deployment | Run `sandbox-core`/`sandbox-local` **inside** the OpenShell sandbox as the canonical process | Works today without upstream changes. Needs a thread-to-server router, an in-sandbox supervisor, and sandbox lifecycle management. Different topology; keeps the full toolset. |

The recommendation is to ship the standalone `openshell-session` plus RAP adapter
first, because it does not fork Infinity and it models stop/start natively, then
pursue **B** as an upstream RFC. If the team is willing to block on upstream
work, **B** is the better long-term architecture. **C** is the pragmatic bridge
for teams that need Infinity's coding toolset immediately and accept one RAP
server per sandbox.

### 3.7 Cost of not sharing the sandbox implementation

`sandbox-core` plus `sandbox-local` is about 5,100 source lines and 3,400 test
lines. Not using it is cheap for a narrow product and expensive for a drop-in
one.

| Capability | In `sandbox-core`? | Rebuild cost, standalone |
|---|---|---|
| RAP endpoints, error semantics, callback plumbing | Yes | 1-2 weeks |
| Streaming output and cancellation | Yes | 1-2 weeks |
| `exec`, `read_file`, `write_file`, `list`, `stat` | Partial | 2-3 weeks |
| `edit_file` unique-match semantics, unified diffs | Yes | 2-3 weeks |
| `grep` via ripgrep | Yes | 3-5 days |
| `clone_repo`, jj/git push/pull, repo modes | Yes | 3-5 weeks |
| Write approvals and `thread_ancestors` grants | Yes | 2-3 weeks |
| Session migration (`/migrate`, bundle import) | Yes | 2-4 weeks |
| Metadata stores (local JSON, DynamoDB) | Yes | 3-5 days |
| Stop/start/idle lifecycle | No | 2-3 weeks either way |
| Journal, outbox, idempotency | No | 2-3 weeks either way |

Rough totals, for a production-quality result with tests:

| Scope | Standalone | Widen the trait (B) | Run inside OpenShell (C) |
|---|---|---|---|
| Core execution tools only (`exec`, read, write, list, stat) | 6-9 weeks | 8-12 weeks, for capability you do not need | Not applicable |
| Drop-in Infinity sandbox, Infinity Code compatible | 16-24 weeks, plus permanent divergence | 6-10 weeks, plus upstream coordination | 3-5 weeks for router, supervisor, and lifecycle |

These are order-of-magnitude estimates, not commitments. The important asymmetry
is that the standalone path scales with the **toolset surface**, while the trait
path scales with the **refactor depth** and then inherits the whole surface.

The ongoing cost matters more than the build cost:

- Every RAP change (new callback type, changed cancel semantics, migration) has
  to be ported twice.
- Tool names and descriptions are model-facing. A different `execute_command`
  contract changes agent behaviour and invalidates prompt tuning.
- Auth, callback validation, path validation, and limits are a second security
  surface to review and test.
- Upstream bug fixes never reach the standalone implementation.

The shared part is `openshell-session`: lifecycle, exec, file I/O, and the fake
gateway are needed in all three options. Build that first. It is the hedge, not
the bet.

## 4. Core API: `openshell-session`

### 4.1 Public surface

```rust
pub struct SessionRuntime { /* private */ }

impl SessionRuntime {
    pub async fn open(config: RuntimeConfig) -> Result<Self, Error>;

    /// Ensure a sandbox is ready and hold it for the caller.
    pub async fn acquire(&self, key: SessionKey) -> Result<Session, Error>;

    /// Release the sandbox now and delete it. Always safe to call twice.
    pub async fn close(&self, key: &SessionKey) -> Result<(), Error>;

    /// Stop idle sandboxes and delete expired ones. Called by the controller
    /// loop, or manually by an operator.
    pub async fn reap(&self) -> Result<ReapReport, Error>;

    pub async fn shutdown(self) -> Result<(), Error>;
}

pub struct Session { /* private, !Clone, Drop releases */ }

impl Session {
    pub fn name(&self) -> &SandboxName;
    pub fn generation(&self) -> Generation;
    pub fn phase(&self) -> Phase;

    pub async fn exec(&self, request: ExecRequest) -> Result<ExecOutput, Error>;
    pub fn exec_stream(&self, request: ExecRequest) -> ExecStream;

    pub async fn read(&self, path: &SandboxPath) -> Result<Bytes, Error>;
    pub async fn write(&self, path: &SandboxPath, bytes: impl Into<Bytes>) -> Result<(), Error>;
    pub async fn list(&self, path: &SandboxPath) -> Result<Vec<DirEntry>, Error>;
    pub async fn stat(&self, path: &SandboxPath) -> Result<Metadata, Error>;
    pub async fn remove(&self, path: &SandboxPath) -> Result<(), Error>;
}
```

`Session` is deliberately not `Clone`. Concurrent calls in the same thread call
`acquire` again; the runtime increments the lease count and returns another
handle. Dropping a `Session` sends a non-blocking `Release` message to the
per-key actor. There is no async `Drop`.

### 4.2 Exec and filesystem types

```rust
pub struct ExecRequest {
    pub argv: Vec<String>,                  // no shell by default
    pub cwd: Option<SandboxPath>,
    pub env: BTreeMap<String, String>,
    pub stdin: Option<Bytes>,
    pub timeout: Option<Duration>,
    pub max_output_bytes: usize,
}

#[non_exhaustive]
pub enum ExecEvent {
    Stdout(Bytes),
    Stderr(Bytes),
    Exited { code: i32 },
}

pub struct ExecOutput {
    pub code: i32,
    pub stdout: Bytes,
    pub stderr: Bytes,
    pub truncated: bool,
}
```

`SandboxPath` is a newtype with `TryFrom<&str>`:

- relative only, no `..`, no NUL, no empty segment;
- maximum length and segment count;
- validated at the API edge and again in the adapter.

File I/O is binary-safe (`Bytes`), not `String`. UTF-8 validation belongs in the
RAP layer, where an image or binary file can become structured `content`.
Line-range reads are a RAP concern: read bytes, then slice lines locally. Do not
build `sed` expressions from user input.

Writes use the SDK's `ExecOptions.stdin` or the native file-transfer protocol,
not base64 inside a heredoc. If the SDK lacks a file API, use a chunked,
length-prefixed binary protocol over stdin with explicit size limits.

### 4.3 Configuration

```rust
pub struct RuntimeConfig {
    pub gateway: GatewayConfig,
    pub template: TemplatePolicy,
    pub lifecycle: LifecyclePolicy,
    pub limits: Limits,
    pub labels: BTreeMap<String, String>,
}

pub struct GatewayConfig {
    pub endpoint: Url,
    pub workspace: String,
    pub auth: GatewayAuth,          // None | Mtls | Oidc
}

pub enum GatewayAuth {
    None,
    Mtls { ca: PathBuf, cert: PathBuf, key: PathBuf },
    Oidc { token: Arc<dyn TokenSource> },
}

pub struct LifecyclePolicy {
    pub wake: WakeStrategy,         // StopStart | SuspendResume | KeepWarm
    pub idle_after: Duration,       // release -> stop
    pub retain_for: Duration,       // stop -> delete
    pub ready_timeout: Duration,
    pub stop_timeout: Duration,
    pub delete_timeout: Duration,
    pub exec_timeout: Duration,
    pub max_output_bytes: usize,
    pub max_concurrent_execs: usize,
}

pub struct TemplatePolicy {
    pub allowed: Vec<TemplateName>, // admin allowlist, not caller input
    pub default: TemplateName,
}
```

Rules:

- No `openshell-bootstrap` in the production path. The CLI's on-disk metadata
  and token cache are not a service credential store. `TokenSource` is a trait
  with `StaticToken`, `OidcRefresh`, and an explicit `FileToken` for local dev.
  A non-default `bootstrap` feature may use `openshell-bootstrap` for live tests
  and local verification only; production builds must not enable it.
- Every duration is validated at startup: non-zero, bounded, `idle_after <
  retain_for`, `max_output_bytes` bounded.
- Templates and images are an allowlist. A caller cannot choose an arbitrary
  image, because that is a privilege escalation.

### 4.4 The actor

One actor per `SessionKey`, created on first use and evicted when the sandbox is
gone:

```rust
enum Command {
    Acquire { reply: oneshot::Sender<Result<SessionHandle, Error>> },
    Release { generation: Generation },
    Close   { reply: oneshot::Sender<Result<(), Error>> },
    Tick    { now: Instant },
}
```

The actor is the only writer for that key. It owns:

- `desired`, `observed`, `generation`, `leases`, `last_release`;
- the current `SandboxName`;
- the single-flight state for create/start/resume.

`Acquire` on `Provisioning` or `Starting` does not call OpenShell again. It
queues the waiter and replies when the current transition finishes. This removes
the prototype's create race.

`Tick` is driven by one global timer, not one task per sandbox. The timer sends
`Tick` to each actor at a coarse interval (for example `min(idle_after, 30s)`).
Tests inject a virtual clock through a `Clock` trait.

## 5. RAP adapter: `infinity-openshell`

### 5.1 Endpoints

| Endpoint | Behaviour |
|---|---|
| `GET /.well-known/rap-toolset` | Full manifest from the configured toolset. `endpoint` comes from `base_url`, never from the `Host` header. |
| `POST /invoke` | Validate, journal, return 200, process asynchronously. |
| `POST /close_thread` | Always 200. Mark deleted, cancel in-flight calls, delete sandbox. |
| `POST /cancel_tool_call` | Always 200. Cancel the token; drop the exec stream. |
| `GET /healthz` | Liveness. |
| `GET /readyz` | Gateway and journal reachable; not draining. |
| `GET /metrics` | Optional, feature-gated. |

The prototype's `/thread/stop` is removed. It is not a RAP endpoint.

### 5.2 Invocation pipeline

```
parse -> validate -> journal.accept -> 200 -> enqueue -> acquire -> execute
      -> stream events -> journal.finish -> callback with retry -> journal.delivered
```

The `200` is sent only after the invocation is durable. If the journal write
fails, return 503 so the runtime retries. This is the key difference from the
prototype's `tokio::spawn`: a crash after acknowledgement must not lose the call.

### 5.3 Idempotency and reconciliation

```rust
#[async_trait]
pub trait InvocationJournal: Send + Sync {
    async fn accept(&self, record: InvocationRecord) -> Result<Accept, Error>;
    async fn finish(&self, id: &ToolCallId, result: ToolResultRecord) -> Result<(), Error>;
    async fn pending(&self, limit: usize) -> Result<Vec<InvocationRecord>, Error>;
    async fn mark_delivered(&self, id: &ToolCallId) -> Result<(), Error>;
}

pub enum Accept {
    New,
    DuplicatePending,
    DuplicateDelivered,
    Conflict { reason: String },
}
```

- `New`: process it.
- `DuplicatePending`: attach to the existing job; do not execute twice.
- `DuplicateDelivered`: return 200 and do nothing.
- `Conflict`: the same `id` arrived with different arguments. Return 200 and
  deliver an `Error: conflicting duplicate invocation` result.

OpenShell's durable admission is used deliberately:

- create/start/exec carry `request_id = hash(tenant, thread, tool_call_id)`.
- A retry of the same invocation reuses the same ID, so the gateway can replay a
  confirmed success instead of running again.
- `REQUEST_OUTCOME_UNCERTAIN` and `REQUEST_STREAM_UNAVAILABLE` mean the effect may
  have happened. Do **not** retry with a new ID. Mark the record
  `NeedsReconcile`, deliver an actionable error, and let an operator or a
  tool-specific reconciler decide. This matches OpenShell's documented contract.

Retry policy depends on the tool's effect class:

| Effect | Retry |
|---|---|
| `ReadOnly` | Safe with backoff. |
| `Mutating` | At-most-once. Reconcile on uncertain outcome. |
| `Destructive` | At-most-once. Never auto-retry. |

### 5.4 Callback outbox

The result is persisted before delivery. A background sender retries pending
results with exponential backoff and jitter. Success is a 2xx; 4xx is terminal
and logged. The callback client has connect and total timeouts.

The callback URL is validated before any request is signed:

- `https` only (unless an explicit `allow_http` dev flag is set);
- host must match `callback_allowlist`;
- no userinfo, no fragment;
- SigV4 service and region are configured, not inferred from the URL.

This removes the prototype's ability to turn the adapter into a signing oracle
for an arbitrary host.

### 5.5 Streaming and cancellation

`exec_stream` maps the OpenShell exec stream to RAP:

- accumulate output in a bounded buffer;
- emit a `subscription_event` every `stream_interval` (default 10s) when there is
  new text;
- emit a final `subscription_event` with `final: true`;
- always send the final `tool_result`, even after cancellation, with
  `[cancelled]` appended.

`/cancel_tool_call` cancels the token, which drops the gRPC stream and marks the
record cancelled. If the gateway cannot stop the remote process on stream drop,
document that and use `execution_timeout` as the backstop. A test must prove
which behaviour the deployed gateway actually has.

### 5.6 Toolsets

Ship two manifests behind features:

- `core` (default): `exec`, `read_file`, `write_file`, `list_dir`, `stat_file`,
  `remove_file`. This is the secure primitive.
- `coding` (opt-in): Infinity-compatible `execute_command`, `read_file`,
  `edit_file`, `create_file`, `grep`. These are the names models already know.
  `clone_repo` and `squash_sandbox` require git/jj in the image and a network
  policy; they come later.

Tool descriptions must state the lifetime explicitly: "Files written here
persist for the lifetime of the thread, including while the sandbox is stopped,
until the thread is closed or the retention window expires."

`exec` is argv-only. If a shell is needed, expose a separate `exec_shell` tool
with a clear warning; do not quietly pass a string to `sh -c`.

## 6. Security model

- **Boundary**: OpenShell's policy is the real boundary. The adapter never
  bypasses it, never mounts the host filesystem, and never injects credentials
  into the sandbox. Providers supply credentials at approved endpoints.
- **Inbound auth**: mTLS, bearer, or SigV4, selected explicitly. `None` is
  allowed only with `allow_unauthenticated = true` and a loopback bind; startup
  logs a warning.
- **Tenancy**: `tenant` scopes the key, name, labels, workspace, and journal.
  Cross-tenant access is a `NotFound`, not a `PermissionDenied`, to avoid
  enumeration.
- **Paths**: `SandboxPath` at the edge and in the adapter.
- **Commands**: argv vector; no shell; bounded length; bounded output.
- **Templates**: admin allowlist; no caller-supplied image or driver config.
- **Secrets**: the gateway token is redacted in `Debug`; never logged; never sent
  to the sandbox.
- **Limits**: max sandboxes per tenant, max concurrent execs per session, max
  output bytes, max journal record size, request body limit.
- **Audit**: log `tenant`, hashed `thread`, `tool_call_id`, `user_id`, operation,
  outcome, duration. Do not log arguments by default.

## 7. Durability and HA

Correctness target:

- **Single active controller per workspace** for lifecycle decisions.
- HTTP replicas may be many, because a sandbox is named globally in a workspace
  and any replica can exec into it.
- Lifecycle decisions go through a `Coordinator`:

```rust
#[async_trait]
pub trait Coordinator: Send + Sync {
    async fn try_own(&self, key: &SessionKey, replica: &ReplicaId, ttl: Duration) -> Result<bool, Error>;
    async fn renew(&self, key: &SessionKey, replica: &ReplicaId) -> Result<(), Error>;
    async fn release(&self, key: &SessionKey, replica: &ReplicaId) -> Result<(), Error>;
}
```

Adapters:

- `LocalCoordinator`: in-process, for one replica and tests.
- `PostgresCoordinator`: a lease row with `owner`, `expires_at`, and a
  `FOR UPDATE SKIP LOCKED` claim. This is the recommended HA backend.
- A DynamoDB conditional-write adapter can come later.

The journal uses the same database. SQLite is for single-node deployments and
local development; Postgres is for HA. Both are feature-gated so the default
build stays small.

Crash recovery:

1. On startup, list sandboxes with the managed-by label.
2. Adopt or stop them; never delete on sight. The filesystem is the state.
3. Replay `New`/`DuplicatePending` journal records.
4. Retry undelivered callbacks.
5. Delete only records past `retain_for` whose coordinator lease is free.

## 8. Observability

Spans:

```
session.acquire  tenant, thread_hash, generation, outcome, wait_ms
session.create   sandbox, template, duration_ms
session.wake     sandbox, strategy, duration_ms
session.stop     sandbox, reason, duration_ms
session.delete   sandbox, reason, duration_ms
exec             sandbox, tool_call_id, argv_len, exit_code, duration_ms, stdout_bytes
callback         tool_call_id, attempt, status, duration_ms
journal          op, outcome
reconcile        action, sandbox, age_ms
```

Metrics (through a `Metrics` trait, with a `metrics` crate adapter):

- `sandbox_creates_total{outcome}`
- `sandbox_wakes_total{strategy,outcome}`
- `sandbox_ready_seconds` (histogram)
- `sandbox_idle_stops_total`
- `sandbox_deletes_total{reason}`
- `exec_duration_seconds`
- `exec_output_bytes`
- `callback_attempts_total{outcome}`
- `journal_pending`
- `reconcile_actions_total{action}`

Health: `/readyz` fails when the gateway is unreachable, the journal is
unwritable, or the server is draining.

## 9. Testing

The test seam is `Gateway`, not `OpenShellClient`:

```rust
#[async_trait]
pub trait Gateway: Send + Sync {
    async fn create(&self, spec: CreateSpec, request_id: &str) -> Result<SandboxName, Error>;
    async fn get(&self, name: &SandboxName) -> Result<SandboxRef, Error>;
    async fn list_managed(&self, labels: &BTreeMap<String, String>) -> Result<Vec<SandboxRef>, Error>;
    async fn start(&self, name: &SandboxName) -> Result<(), Error>;
    async fn stop(&self, name: &SandboxName) -> Result<(), Error>;
    async fn suspend(&self, name: &SandboxName) -> Result<(), Error>;
    async fn resume(&self, name: &SandboxName) -> Result<(), Error>;
    async fn delete(&self, name: &SandboxName) -> Result<(), Error>;
    async fn exec(&self, name: &SandboxName, req: ExecRequest) -> Result<ExecStream, Error>;
    async fn read(&self, name: &SandboxName, path: &SandboxPath) -> Result<Bytes, Error>;
    async fn write(&self, name: &SandboxName, path: &SandboxPath, bytes: Bytes) -> Result<(), Error>;
}
```

Test layers:

1. **State machine unit tests** with a virtual clock: idle stop, wake, close
   during exec, error recovery, generation bump, stale lease.
2. **Fake gateway scenario tests**: create race, phase transitions, stream
   errors, uncertain exec, delete pending, callback 500 then 200.
3. **RAP contract tests** with `insta` snapshots for the manifest and callback
   JSON. Round-trip against upstream `rap-protocol` types, not a local copy.
4. **Property tests** with `proptest`: names are stable and collision-resistant,
   paths never escape, binary file round-trips, limits are enforced.
5. **Live tests** against a local OpenShell gateway in Docker. Gated by
   `OPENSHELL_TEST_GATEWAY`, but a release job sets it and fails if it is unset.
   No silent skips in the release pipeline.
6. **Soak test**: 1k threads, random think times, assert every sandbox is stopped
   or deleted and no lease leaks.

Tooling: `cargo nextest`, `cargo llvm-cov`, `cargo clippy -D warnings`,
`cargo fmt --check`, `cargo doc -D warnings`, `cargo semver-checks`,
`cargo public-api`, `cargo deny`, `cargo audit`, `miri` for any unsafe (none
expected), `loom` only if the actor uses lock-free hand-rolled structures.

## 10. Release and versioning

- Workspace version, single `CHANGELOG.md`, Keep a Changelog.
- `rust-version = "1.94"`, edition 2024, matching the pinned upstream crates.
  MSRV bumps are minor releases and are checked in CI.
- `openshell-session` can reach `1.0` once the lifecycle contract and the
  `Gateway` trait are stable.
- `infinity-openshell` stays `0.x` while `rap-protocol` is git-only. It cannot be
  published to crates.io until Infinity publishes `rap-protocol`, unless the
  adapter vendors the wire types. If it vendors them, add a compile-time or
  snapshot compatibility test against the upstream crate.
- Pin upstream git revisions. Depend on `openshell-sdk` by release tag once
  NVIDIA publishes one; until then, a rev with a comment and a monthly bump job.
- `docs.rs` metadata: `all-features = true`, `rustdoc-args = ["--cfg", "docsrs"]`.
- Feature matrix CI: `--no-default-features`, each feature alone, `--all-features`.

## 11. Migration from the prototype

| Prototype | Production |
|---|---|
| `openshell.rs` one-shot/retained mix | `openshell-session` actor and state machine |
| `sandbox.rs` duplicate dispatch | one `Session` interface; no duplicate executor |
| `server.rs` `tokio::spawn` | journal + worker + outbox |
| `callback.rs` | same SigV4 logic plus timeouts, retries, URL allowlist |
| `protocol.rs` local copy | depend on `rap-protocol`; snapshot test |
| `oidc.rs` CLI token cache | explicit `TokenSource`; `openshell-bootstrap` only behind the non-default test feature |
| `local.rs` host execution | delete; tests use the fake gateway |
| `workspace.rs` snapshots | not needed: OpenShell's `Stop` retains the workspace |
| `/thread/stop` | remove; use `/cancel_tool_call` and `/close_thread` |
| lease by creation time | lease by activity, never delete while leased |

The `stderr means failure` workaround is removed. OpenShell returns `-1` when the
exit event is missing, so the workaround only misreported successful commands
that wrote to stderr.

## 12. Milestones

Each milestone ends with a usable library. The binary is the team's agent
binary; there is no separate tool-server milestone. The example agent is the
proof that the one-binary shape works, not a product of its own.

**M0: `openshell-session` skeleton (1-2 weeks).**
`Gateway` trait, real adapter, fake adapter, virtual clock, actor, create/start/
stop/delete, exec, read/write, lifecycle unit tests. No RAP yet.

**M1: RAP adapter (2-3 weeks).**
All endpoints, journal (SQLite), outbox, streaming, cancellation, SigV4 callback
with allowlist, core toolset, `EmbeddedServer::start`, contract tests. Single
replica. The `examples/agent` binary proves the one-binary shape end to end.

**M2: operations (2 weeks).**
Postgres coordinator and journal, reaper, metrics, health, graceful shutdown,
live CI job, soak test. Add `examples/split-service` for the request-scoped
topology. The agent image and Helm chart are deployment artifacts, not a
separate product.

**M3: coding toolset (optional).**
Infinity-compatible tools, image with git/jj/ripgrep, network policy examples,
and an RFC to upstream a filesystem/process abstraction into `sandbox-core` so
OpenShell can become a first-class `SandboxBackend` instead of a parallel server.

## 13. Open decisions

1. Is `SuspendResume` ever the default? Recommendation: no. Default to
   `StopStart`; probe suspend and use it only when the driver advertises support
   and the workload is latency-sensitive.
2. Default `idle_after` and `retain_for`? Recommendation: 60s and 24h, both
   configurable per tenant/retention class.
3. Publish `openshell-session` publicly, or keep the family internal until the
   lifecycle API is proven? Recommendation: publish the core at `1.0`; keep the
   adapter internal.
4. Vendor `rap-protocol` or wait for a crates.io release? Recommendation: wait;
   vendor only if the adapter must ship externally before Infinity publishes.
5. Does the deployed OpenShell driver stop the remote process when the exec
   stream is dropped? This must be measured, not assumed; it determines the
   cancellation guarantee.
6. Is the default one binary or split? Recommendation: one binary for long-lived
   agents, where the tool server is embedded in the agent process. Split only
   for request-scoped runtimes such as Lambda, where the slice exits after the
   acknowledgement. Do not ship a separate tool-server product binary; ship
   `EmbeddedServer::start` and an example agent binary.

## Further reading

- Rust API Guidelines: <https://rust-lang.github.io/api-guidelines/>
- Cargo features are additive: <https://doc.rust-lang.org/cargo/reference/features.html>
- SemVer compatibility: <https://doc.rust-lang.org/cargo/reference/semver.html>
- `cargo-semver-checks`: <https://github.com/obi1kenobi/cargo-semver-checks>
- Effective Rust, item 21 (SemVer): <https://www.lurklurk.org/effective-rust/semver.html>
- OpenShell sandbox lifecycle: `docs/how-it-works/sandboxes/overview.mdx`
- OpenShell API errors and durable admission: `docs/sdk/api-errors.mdx`
- RAP specification: `docs/docs/rap/spec/` in `hydro-project/infinity`
