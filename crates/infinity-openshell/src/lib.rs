//! RAP tool server backed by OpenShell sandboxes.
//!
//! The server exposes `openshell_exec`, `openshell_read`, and
//! `openshell_write` over the Reactive Agent Protocol. Each conversation
//! thread maps to one persistent OpenShell workspace; the session runtime
//! stops it when idle and starts it again on the next call.
//!
//! The server is a library. Mount [`router`] in an existing process, or run
//! [`router`] behind a listener in a dedicated service.
//!
//! # Example
//!
//! ```no_run
//! use std::sync::Arc;
//! use openshell_session::{GatewayAuth, GatewayConfig, RuntimeConfig, SessionRuntime, TenantId};
//! use infinity_openshell::{InboundAuth, ServerConfig};
//!
//! # async fn run() -> Result<(), Box<dyn std::error::Error>> {
//! let gateway = GatewayConfig::new(
//!     "https://gateway.example.com".parse()?,
//!     "default",
//!     GatewayAuth::Oidc { token: std::env::var("OPENSHELL_TOKEN")? },
//! );
//! let runtime = SessionRuntime::open(RuntimeConfig::new(gateway)).await?;
//!
//! let mut config = ServerConfig::new(
//!     "https://tools.example.com".parse()?,
//!     TenantId::new("acme")?,
//! );
//! config.inbound_auth = InboundAuth::Bearer { token: "shared-secret".to_owned() };
//! config.journal_path = Some("journal.jsonl".into());
//! let app = infinity_openshell::router(runtime, config).await?;
//!
//! let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
//! axum::serve(listener, app).await?;
//! # Ok(())
//! # }
//! ```

#![deny(missing_docs)]
#![forbid(unsafe_code)]

mod callback;
mod config;
mod error;
mod journal;
mod server;
mod toolset;

#[cfg(feature = "sigv4")]
pub use callback::SigV4CallbackClient;
pub use callback::{CallbackClient, CallbackError, PlainCallbackClient};
pub use config::{CallbackAuth, InboundAuth, ServerConfig};
pub use error::Error;
pub use journal::{Accept, FileJournal, Journal, MemoryJournal, Pending, Recovered};
pub use server::{router, router_with_callback};
pub use toolset::manifest;

/// Apply the Infinity label policy to a session runtime configuration.
///
/// `openshell-session` is product-neutral. The Infinity adapter owns the
/// `infinity.hydro.run/*` labels and the `infinity-openshell` managed-by value
/// that allow a restarted runtime to adopt retained sandboxes.
pub fn configure_labels(config: &mut openshell_session::RuntimeConfig) {
    config.label_prefix = "infinity.hydro.run".to_owned();
    config.managed_by = "infinity-openshell".to_owned();
}
