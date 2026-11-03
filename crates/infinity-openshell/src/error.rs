//! Errors for the RAP server.

use thiserror::Error;

/// Error returned by the RAP server.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// Configuration is invalid.
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    /// A tool invocation carried invalid arguments.
    #[error("invalid arguments: {0}")]
    InvalidArguments(String),

    /// The session runtime failed.
    #[error("session error: {0}")]
    Session(#[from] openshell_session::Error),

    /// The invocation journal failed.
    #[error("journal error: {0}")]
    Journal(String),

    /// Callback delivery failed.
    #[error("callback error: {0}")]
    Callback(String),

    /// The invocation was cancelled.
    #[error("invocation cancelled")]
    Cancelled,

    /// An internal invariant was violated.
    #[error("internal error: {0}")]
    Internal(String),
}
