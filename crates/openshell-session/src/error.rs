//! Error types for the session runtime.

use thiserror::Error;

/// Stable category for an [`Error`].
///
/// Callers should match on this rather than on error message text.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum ErrorKind {
    /// The runtime or gateway configuration is invalid.
    InvalidConfig,
    /// A caller supplied an invalid identifier, path, or request.
    InvalidInput,
    /// The requested sandbox does not exist.
    NotFound,
    /// The sandbox is in a state that conflicts with the request.
    Conflict,
    /// The gateway is unreachable or temporarily unavailable.
    Unavailable,
    /// An operation exceeded its deadline.
    Timeout,
    /// The operation was cancelled.
    Cancelled,
    /// The session belongs to a sandbox generation that no longer exists.
    StaleSession,
    /// The gateway does not support the requested operation.
    Unsupported,
    /// The execution outcome is unknown and must be reconciled.
    Uncertain,
    /// The gateway returned an error.
    Gateway,
    /// An internal invariant was violated.
    Internal,
}

/// Error returned by the session runtime.
#[derive(Debug, Error)]
#[non_exhaustive]
pub enum Error {
    /// The runtime or gateway configuration is invalid.
    #[error("invalid configuration: {0}")]
    InvalidConfig(String),

    /// A caller supplied an invalid identifier, path, or request.
    #[error("invalid input: {0}")]
    InvalidInput(String),

    /// The requested sandbox does not exist.
    #[error("not found: {0}")]
    NotFound(String),

    /// The sandbox is in a state that conflicts with the request.
    #[error("conflict: {0}")]
    Conflict(String),

    /// The gateway is unreachable or temporarily unavailable.
    #[error("gateway unavailable: {0}")]
    Unavailable(String),

    /// An operation exceeded its deadline.
    #[error("timed out: {0}")]
    Timeout(String),

    /// The operation was cancelled.
    #[error("cancelled")]
    Cancelled,

    /// The session belongs to a sandbox generation that no longer exists.
    #[error("stale session: the sandbox generation has changed")]
    StaleSession,

    /// The gateway does not support the requested operation.
    #[error("unsupported: {0}")]
    Unsupported(String),

    /// The execution outcome is unknown. Do not retry a mutating operation
    /// without reconciling its effects.
    #[error("execution outcome is uncertain: {0}")]
    Uncertain(String),

    /// The gateway returned an error.
    #[error("gateway error: {0}")]
    Gateway(String),

    /// An internal invariant was violated.
    #[error("internal error: {0}")]
    Internal(String),
}

impl Error {
    /// Return the stable category for this error.
    pub fn kind(&self) -> ErrorKind {
        match self {
            Self::InvalidConfig(_) => ErrorKind::InvalidConfig,
            Self::InvalidInput(_) => ErrorKind::InvalidInput,
            Self::NotFound(_) => ErrorKind::NotFound,
            Self::Conflict(_) => ErrorKind::Conflict,
            Self::Unavailable(_) => ErrorKind::Unavailable,
            Self::Timeout(_) => ErrorKind::Timeout,
            Self::Cancelled => ErrorKind::Cancelled,
            Self::StaleSession => ErrorKind::StaleSession,
            Self::Unsupported(_) => ErrorKind::Unsupported,
            Self::Uncertain(_) => ErrorKind::Uncertain,
            Self::Gateway(_) => ErrorKind::Gateway,
            Self::Internal(_) => ErrorKind::Internal,
        }
    }

    /// Whether retrying the same operation may succeed.
    ///
    /// Only transport-level failures are retryable. A timeout is not retryable
    /// for a mutating operation without reconciliation, so callers must decide
    /// at the operation boundary.
    pub fn is_retryable(&self) -> bool {
        matches!(self, Self::Unavailable(_))
    }
}
