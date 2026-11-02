//! Lease-managed OpenShell sandbox sessions.
//!
//! An agent thread gets one sandbox workspace. A [`Session`] holds the
//! sandbox ready for the duration of one tool call. When the last session is
//! dropped, the runtime stops the sandbox after an idle window and starts or
//! resumes it on the next call. The filesystem survives the stop; the agent
//! process does not need to.
//!
//! # Example
//!
//! ```no_run
//! use openshell_session::{
//!     GatewayAuth, GatewayConfig, RuntimeConfig, SessionKey, SessionRuntime, TenantId, ThreadId,
//! };
//!
//! # async fn run() -> Result<(), openshell_session::Error> {
//! let gateway = GatewayConfig::new(
//!     "https://gateway.example.com".parse().unwrap(),
//!     "default",
//!     GatewayAuth::Oidc { token: std::env::var("OPENSHELL_TOKEN").unwrap() },
//! );
//! let runtime = SessionRuntime::open(RuntimeConfig::new(gateway)).await?;
//! let key = SessionKey::new(TenantId::new("acme")?, ThreadId::new("thread-1")?);
//!
//! let session = runtime.acquire(key.clone()).await?;
//! let output = session
//!     .exec(openshell_session::ExecRequest::new(vec![
//!         "sh".to_owned(),
//!         "-c".to_owned(),
//!         "printf hello".to_owned(),
//!     ]))
//!     .await?;
//! assert_eq!(&output.stdout[..], b"hello");
//! drop(session);
//!
//! runtime.close(&key).await?;
//! # Ok(())
//! # }
//! ```
//!
//! # Lifecycle
//!
//! - `acquire` creates, starts, or resumes the sandbox and returns a lease.
//! - Dropping the lease starts the idle timer.
//! - `idle_after` stops the sandbox; `retain_for` deletes it.
//! - `close` deletes it immediately.
//!
//! A lease is bound to a sandbox generation. If the sandbox is deleted and
//! recreated, old sessions return [`Error::StaleSession`] instead of touching
//! the replacement.

#![deny(missing_docs)]
#![forbid(unsafe_code)]

mod actor;
mod config;
mod error;
#[cfg(any(test, feature = "testkit"))]
pub mod fake;
mod gateway;
mod ids;
#[cfg(feature = "openshell")]
mod openshell;
mod runtime;
mod session;

pub use config::{
    GatewayAuth, GatewayConfig, LifecyclePolicy, Limits, RuntimeConfig, TemplatePolicy,
    WakeStrategy,
};
pub use error::{Error, ErrorKind};
pub use gateway::{CreateSpec, ExecOutput, ExecRequest, Gateway, Phase, SandboxInfo};
pub use ids::{Generation, SandboxName, SandboxPath, SessionKey, TenantId, ThreadId};
pub use runtime::{ReapReport, SessionRuntime};
pub use session::Session;

#[cfg(feature = "openshell")]
pub use openshell::OpenShellGateway;
