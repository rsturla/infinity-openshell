//! Identifier and path newtypes.
//!
//! Every value that reaches OpenShell is validated here first. Sandbox names
//! are derived from a hash and never contain caller-supplied text.

use std::fmt;

use sha2::{Digest, Sha256};

use crate::error::Error;

/// Tenant scope for a sandbox.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct TenantId(String);

impl TenantId {
    /// Validate and construct a tenant identifier.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`] when the value is empty, longer than
    /// 128 bytes, or contains a control character.
    pub fn new(value: impl Into<String>) -> Result<Self, Error> {
        let value = value.into();
        validate_opaque("tenant", &value, 128)?;
        Ok(Self(value))
    }

    /// Return the identifier as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for TenantId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Conversation thread scope for a sandbox.
///
/// This is RAP's `group_id`. Treat it as opaque.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct ThreadId(String);

impl ThreadId {
    /// Validate and construct a thread identifier.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`] when the value is empty, longer than
    /// 256 bytes, or contains a control character.
    pub fn new(value: impl Into<String>) -> Result<Self, Error> {
        let value = value.into();
        validate_opaque("thread", &value, 256)?;
        Ok(Self(value))
    }

    /// Return the identifier as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ThreadId {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// Identifies one sandbox workspace: a tenant plus a conversation thread.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct SessionKey {
    tenant: TenantId,
    thread: ThreadId,
}

impl SessionKey {
    /// Construct a session key.
    pub fn new(tenant: TenantId, thread: ThreadId) -> Self {
        Self { tenant, thread }
    }

    /// Return the tenant.
    pub fn tenant(&self) -> &TenantId {
        &self.tenant
    }

    /// Return the thread.
    pub fn thread(&self) -> &ThreadId {
        &self.thread
    }

    /// Return a short, stable hash used in labels.
    pub(crate) fn hash_hex(&self) -> String {
        let mut hasher = Sha256::new();
        hasher.update(self.tenant.as_str().as_bytes());
        hasher.update([0]);
        hasher.update(self.thread.as_str().as_bytes());
        format!("{:x}", hasher.finalize())[..16].to_owned()
    }

    /// Return a short hash of the tenant for labels.
    pub(crate) fn tenant_hash_hex(&self) -> String {
        hash_hex(self.tenant.as_str())
    }

    /// Return a short hash of the thread for labels.
    pub(crate) fn thread_hash_hex(&self) -> String {
        hash_hex(self.thread.as_str())
    }
}

impl fmt::Display for SessionKey {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}/{}", self.tenant, self.thread)
    }
}

/// Opaque generation of a sandbox.
///
/// A new generation is chosen whenever a sandbox is created or recreated.
/// Leases are bound to a generation so a late call cannot attach to a
/// replacement sandbox.
#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct Generation(u64);

impl Generation {
    /// Create a new random generation.
    pub fn new() -> Self {
        Self(uuid::Uuid::new_v4().as_u128() as u64)
    }

    /// Construct a generation from a raw value. Intended for tests and
    /// persistence.
    pub const fn from_raw(value: u64) -> Self {
        Self(value)
    }

    /// Return the raw value.
    pub const fn as_u64(self) -> u64 {
        self.0
    }
}

impl Default for Generation {
    fn default() -> Self {
        Self::new()
    }
}

/// Gateway-safe sandbox name.
///
/// OpenShell caps sandbox names at 19 characters. The name is `i-` plus 17
/// lowercase base32 characters derived from the session key and generation.
/// That is 85 bits of hash, which is the most a 19-character DNS label can
/// carry with a recognizable prefix. The name never contains user text.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct SandboxName(String);

impl SandboxName {
    /// Derive the name for a session key and generation.
    pub fn derive(key: &SessionKey, generation: Generation) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(key.tenant.as_str().as_bytes());
        hasher.update([0]);
        hasher.update(key.thread.as_str().as_bytes());
        hasher.update([0]);
        hasher.update(generation.as_u64().to_le_bytes());
        let digest = hasher.finalize();
        let encoded = base32_lower(&digest);
        Self(format!("i-{}", &encoded[..17]))
    }

    /// Validate and construct a sandbox name, for adoption of existing
    /// sandboxes.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`] unless the value is a lowercase DNS
    /// label of at most 19 bytes with no consecutive hyphens.
    pub fn new(value: impl Into<String>) -> Result<Self, Error> {
        let value = value.into();
        if value.is_empty() || value.len() > 19 {
            return Err(Error::InvalidInput(
                "sandbox name must contain between 1 and 19 bytes".to_owned(),
            ));
        }
        if !value
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
        {
            return Err(Error::InvalidInput(
                "sandbox name must contain only lowercase letters, digits, and '-'".to_owned(),
            ));
        }
        if value.starts_with('-') || value.ends_with('-') {
            return Err(Error::InvalidInput(
                "sandbox name must not start or end with '-'".to_owned(),
            ));
        }
        if value.contains("--") {
            return Err(Error::InvalidInput(
                "sandbox name must not contain consecutive hyphens".to_owned(),
            ));
        }
        Ok(Self(value))
    }

    /// Return the name as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SandboxName {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// A path inside the sandbox workspace.
///
/// Paths are relative, contain no parent traversal, no control characters, and
/// no NUL bytes. They are passed to commands as arguments, never interpolated
/// into a shell string.
#[derive(Clone, Debug, Eq, PartialEq, Hash, Ord, PartialOrd)]
pub struct SandboxPath(String);

impl SandboxPath {
    /// Validate and construct a sandbox path.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`] for absolute paths, `..` components,
    /// control characters, empty values, or values over 4096 bytes.
    pub fn new(value: impl Into<String>) -> Result<Self, Error> {
        let value = value.into();
        if value.is_empty() || value.len() > 4096 {
            return Err(Error::InvalidInput(
                "path must contain between 1 and 4096 bytes".to_owned(),
            ));
        }
        if value.chars().any(char::is_control) {
            return Err(Error::InvalidInput(
                "path must not contain control characters".to_owned(),
            ));
        }
        let path = std::path::Path::new(&value);
        if path.is_absolute() {
            return Err(Error::InvalidInput(
                "path must be relative to the sandbox workspace".to_owned(),
            ));
        }
        if path
            .components()
            .any(|component| matches!(component, std::path::Component::ParentDir))
        {
            return Err(Error::InvalidInput("path must not contain '..'".to_owned()));
        }
        Ok(Self(value))
    }

    /// Return the path as a string slice.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for SandboxPath {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

fn hash_hex(value: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(value.as_bytes());
    format!("{:x}", hasher.finalize())[..16].to_owned()
}

fn base32_lower(bytes: &[u8]) -> String {
    const ALPHABET: &[u8; 32] = b"abcdefghijklmnopqrstuvwxyz234567";
    let mut output = String::with_capacity(bytes.len().div_ceil(5) * 8);
    for chunk in bytes.chunks(5) {
        let mut block = [0_u8; 8];
        block[3..3 + chunk.len()].copy_from_slice(chunk);
        let value = u64::from_be_bytes(block);
        let characters = (chunk.len() * 8).div_ceil(5);
        for index in 0..characters {
            let shift = 35 - index * 5;
            output.push(ALPHABET[((value >> shift) & 0x1f) as usize] as char);
        }
    }
    output
}

fn validate_opaque(kind: &str, value: &str, max_len: usize) -> Result<(), Error> {
    if value.is_empty() || value.len() > max_len {
        return Err(Error::InvalidInput(format!(
            "{kind} must contain between 1 and {max_len} bytes"
        )));
    }
    if value.chars().any(char::is_control) {
        return Err(Error::InvalidInput(format!(
            "{kind} must not contain control characters"
        )));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn key(thread: &str) -> SessionKey {
        SessionKey::new(
            TenantId::new("acme").unwrap(),
            ThreadId::new(thread).unwrap(),
        )
    }

    #[test]
    fn names_are_stable_and_generation_scoped() {
        let key = key("thread-a");
        let first = SandboxName::derive(&key, Generation::from_raw(1));
        let second = SandboxName::derive(&key, Generation::from_raw(2));
        assert_eq!(first, SandboxName::derive(&key, Generation::from_raw(1)));
        assert_ne!(first, second);
        assert!(first.as_str().starts_with("i-"));
        assert_eq!(first.as_str().len(), 19);
    }

    #[test]
    fn base32_matches_rfc4648_vectors() {
        assert_eq!(base32_lower(b""), "");
        assert_eq!(base32_lower(b"f"), "my");
        assert_eq!(base32_lower(b"fo"), "mzxq");
        assert_eq!(base32_lower(b"foo"), "mzxw6");
        assert_eq!(base32_lower(b"foob"), "mzxw6yq");
        assert_eq!(base32_lower(b"fooba"), "mzxw6ytb");
        assert_eq!(base32_lower(b"foobar"), "mzxw6ytboi");
    }

    #[test]
    fn different_keys_get_different_names() {
        let first = SandboxName::derive(&key("thread-a"), Generation::from_raw(1));
        let second = SandboxName::derive(&key("thread-b"), Generation::from_raw(1));
        assert_ne!(first, second);
    }

    #[test]
    fn rejects_traversal_and_absolute_paths() {
        assert!(SandboxPath::new("../secret").is_err());
        assert!(SandboxPath::new("a/../../b").is_err());
        assert!(SandboxPath::new("/etc/passwd").is_err());
        assert!(SandboxPath::new("a\0b").is_err());
        assert!(SandboxPath::new("a\nb").is_err());
        assert!(SandboxPath::new("a/b.txt").is_ok());
        assert!(SandboxPath::new("./a/b.txt").is_ok());
    }

    #[test]
    fn rejects_unsafe_names() {
        assert!(SandboxName::new("Infinity-1").is_err());
        assert!(SandboxName::new("-infinity").is_err());
        assert!(SandboxName::new("infinity_1").is_err());
        assert!(SandboxName::new("infinity-1").is_ok());
    }

    #[test]
    fn rejects_empty_or_control_identifiers() {
        assert!(ThreadId::new("").is_err());
        assert!(ThreadId::new("a\nb").is_err());
        assert!(TenantId::new("a\0b").is_err());
        assert!(ThreadId::new("a".repeat(257)).is_err());
    }
}
