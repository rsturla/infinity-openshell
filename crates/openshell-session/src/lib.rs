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
#[cfg(feature = "openshell")]
mod providers;
mod runtime;
mod session;
mod snapshot;
mod token;

pub use config::{
    GatewayAuth, GatewayConfig, LifecyclePolicy, Limits, RuntimeConfig, TemplatePolicy,
    WakeStrategy,
};
pub use error::{Error, ErrorKind};
pub use gateway::{
    CreateSpec, ExecEvent, ExecOutput, ExecRequest, ExecStream, Gateway, Phase, SandboxInfo,
};
pub use ids::{Generation, SandboxName, SandboxPath, SessionKey, TenantId, ThreadId};
#[cfg(feature = "openshell")]
pub use providers::{ProviderAdmin, ProviderSpec};
pub use runtime::{ReapReport, SessionRuntime};
pub use session::Session;
pub use snapshot::{SnapshotKey, SnapshotMeta, SnapshotPolicy, SnapshotStore};
pub use token::{OidcCredentials, OidcRefreshToken, RefreshTokenStore, StaticToken, TokenSource};

#[cfg(feature = "bootstrap")]
pub use token::BootstrapTokenSource;

/// Derive a nonzero UUID from an arbitrary RAP call ID.
///
/// OpenShell durable admission requires a UUID `request_id`. The derivation is
/// stable, so a retry of the same RAP call carries the same OpenShell ID.
pub fn request_id_from(value: &str) -> String {
    use sha2::{Digest, Sha256};

    let digest = Sha256::digest(value.as_bytes());
    let mut bytes = [0_u8; 16];
    bytes.copy_from_slice(&digest[..16]);
    bytes[6] = (bytes[6] & 0x0f) | 0x40;
    bytes[8] = (bytes[8] & 0x3f) | 0x80;
    if bytes.iter().all(|byte| *byte == 0) {
        bytes[0] = 1;
    }
    uuid::Uuid::from_bytes(bytes).to_string()
}

#[cfg(feature = "openshell")]
pub use openshell::OpenShellGateway;
