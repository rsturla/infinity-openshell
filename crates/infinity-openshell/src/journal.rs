//! Durable invocation journal.
//!
//! The server acknowledges a RAP invocation only after the journal records it.
//! A finished result is also journaled before delivery, so a restart can retry
//! the callback without re-running the tool.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use rap_protocol::{RapInvocation, RapToolResult, ToolCallId};
use serde::{Deserialize, Serialize};
use tokio::io::AsyncWriteExt;
use tokio::sync::Mutex;

use crate::error::Error;

/// Result of accepting an invocation.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
#[non_exhaustive]
pub enum Accept {
    /// The invocation is new and must be processed.
    New,
    /// The same invocation is already in flight.
    DuplicatePending,
    /// The same invocation already delivered its result.
    DuplicateDelivered,
    /// The same call ID arrived with different arguments.
    Conflict,
}

/// A finished result that has not been delivered yet.
#[derive(Debug)]
pub struct Pending {
    /// Original invocation.
    pub invocation: RapInvocation,
    /// Finished result.
    pub result: RapToolResult,
}

/// An undelivered invocation recovered at startup.
#[derive(Debug)]
pub struct Recovered {
    /// Original invocation.
    pub invocation: RapInvocation,
    /// Finished result, or `None` when the process stopped mid-execution.
    pub result: Option<RapToolResult>,
}

/// Stores invocations and results.
#[async_trait]
pub trait Journal: Send + Sync + 'static {
    /// Record an invocation, or classify it as a duplicate.
    async fn accept(&self, invocation: &RapInvocation) -> Result<Accept, Error>;

    /// Record the finished result.
    async fn finish(&self, id: &ToolCallId, result: &RapToolResult) -> Result<(), Error>;

    /// Mark the result as delivered.
    async fn mark_delivered(&self, id: &ToolCallId) -> Result<(), Error>;

    /// Return finished but undelivered results.
    async fn pending(&self) -> Result<Vec<Pending>, Error>;

    /// Return all undelivered invocations, including interrupted ones.
    async fn recover(&self) -> Result<Vec<Recovered>, Error>;

    /// Return a stored result for a delivered invocation.
    async fn result(&self, id: &ToolCallId) -> Result<Option<RapToolResult>, Error>;

    /// Return the callback URL recorded with an invocation.
    async fn callback_url(&self, id: &ToolCallId) -> Result<Option<String>, Error>;
}

#[derive(Clone, Debug)]
struct Record {
    invocation: serde_json::Value,
    result: Option<serde_json::Value>,
    delivered: bool,
}

/// In-memory journal for tests and single-process development.
#[derive(Default)]
pub struct MemoryJournal {
    records: Mutex<HashMap<String, Record>>,
}

impl MemoryJournal {
    /// Create an empty journal.
    pub fn new() -> Self {
        Self::default()
    }
}

#[async_trait]
impl Journal for MemoryJournal {
    async fn accept(&self, invocation: &RapInvocation) -> Result<Accept, Error> {
        let mut records = self.records.lock().await;
        accept_record(&mut records, invocation)
    }

    async fn finish(&self, id: &ToolCallId, result: &RapToolResult) -> Result<(), Error> {
        let mut records = self.records.lock().await;
        let record = records
            .get_mut(id.as_str())
            .ok_or_else(|| Error::Journal(format!("unknown invocation '{}'", id.as_str())))?;
        record.result = Some(
            serde_json::to_value(result)
                .map_err(|error| Error::Journal(format!("cannot serialize result: {error}")))?,
        );
        Ok(())
    }

    async fn mark_delivered(&self, id: &ToolCallId) -> Result<(), Error> {
        let mut records = self.records.lock().await;
        if let Some(record) = records.get_mut(id.as_str()) {
            record.delivered = true;
        }
        Ok(())
    }

    async fn pending(&self) -> Result<Vec<Pending>, Error> {
        let records = self.records.lock().await;
        pending_from(&records)
    }

    async fn recover(&self) -> Result<Vec<Recovered>, Error> {
        let records = self.records.lock().await;
        recovered_from(&records)
    }

    async fn result(&self, id: &ToolCallId) -> Result<Option<RapToolResult>, Error> {
        let records = self.records.lock().await;
        result_from(&records, id)
    }

    async fn callback_url(&self, id: &ToolCallId) -> Result<Option<String>, Error> {
        let records = self.records.lock().await;
        callback_url_from(&records, id)
    }
}

/// Append-only JSON-lines journal.
pub struct FileJournal {
    state: Mutex<HashMap<String, Record>>,
    file: Mutex<tokio::fs::File>,
    poisoned: AtomicBool,
}

impl FileJournal {
    /// Open or create a journal at `path`, replaying existing records.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Journal`] when the file cannot be read or opened.
    pub async fn open(path: impl AsRef<Path>) -> Result<Self, Error> {
        let path = path.as_ref().to_path_buf();
        let contents = match tokio::fs::read(&path).await {
            Ok(contents) => contents,
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(error) => {
                return Err(Error::Journal(format!(
                    "cannot read journal '{}': {error}",
                    path.display()
                )));
            }
        };

        let mut state = HashMap::new();
        let mut offset = 0_usize;
        let mut valid_end = 0_usize;
        for line in contents.split_inclusive(|byte| *byte == b'\n') {
            let start = offset;
            offset += line.len();
            let mut trimmed = line;
            if let Some(stripped) = trimmed.strip_suffix(b"\n") {
                trimmed = stripped;
            }
            if let Some(stripped) = trimmed.strip_suffix(b"\r") {
                trimmed = stripped;
            }
            if trimmed.iter().all(u8::is_ascii_whitespace) {
                valid_end = offset;
                continue;
            }
            match serde_json::from_slice::<LogRecord>(trimmed) {
                Ok(record) => {
                    apply_log_record(&mut state, record);
                    valid_end = offset;
                }
                Err(error) if offset == contents.len() => {
                    tracing::warn!(
                        %error,
                        path = %path.display(),
                        "discarding an incomplete final journal record"
                    );
                    valid_end = start;
                }
                Err(error) => {
                    return Err(Error::Journal(format!(
                        "cannot parse journal '{}': {error}",
                        path.display()
                    )));
                }
            }
        }

        if valid_end < contents.len() {
            let file = tokio::fs::OpenOptions::new()
                .write(true)
                .open(&path)
                .await
                .map_err(|error| {
                    Error::Journal(format!(
                        "cannot truncate journal '{}': {error}",
                        path.display()
                    ))
                })?;
            file.set_len(valid_end as u64).await.map_err(|error| {
                Error::Journal(format!(
                    "cannot truncate journal '{}': {error}",
                    path.display()
                ))
            })?;
        }

        let mut options = tokio::fs::OpenOptions::new();
        options.create(true).append(true);
        #[cfg(unix)]
        options.mode(0o600);
        let mut file = options.open(&path).await.map_err(|error| {
            Error::Journal(format!("cannot open journal '{}': {error}", path.display()))
        })?;
        #[cfg(unix)]
        {
            use std::os::unix::fs::PermissionsExt;
            let metadata = file.metadata().await.map_err(|error| {
                Error::Journal(format!(
                    "cannot inspect journal '{}': {error}",
                    path.display()
                ))
            })?;
            if metadata.permissions().mode() & 0o077 != 0 {
                let mut permissions = metadata.permissions();
                permissions.set_mode(0o600);
                file.set_permissions(permissions).await.map_err(|error| {
                    Error::Journal(format!(
                        "cannot restrict journal '{}': {error}",
                        path.display()
                    ))
                })?;
            }
        }

        // A valid final record without a trailing newline would otherwise
        // concatenate with the next append and corrupt the journal.
        if valid_end == contents.len() && !contents.is_empty() && !contents.ends_with(b"\n") {
            file.write_all(b"\n").await.map_err(|error| {
                Error::Journal(format!(
                    "cannot terminate journal '{}': {error}",
                    path.display()
                ))
            })?;
            file.flush().await.map_err(|error| {
                Error::Journal(format!(
                    "cannot flush journal '{}': {error}",
                    path.display()
                ))
            })?;
        }

        Ok(Self {
            state: Mutex::new(state),
            file: Mutex::new(file),
            poisoned: AtomicBool::new(false),
        })
    }

    async fn append(&self, record: &LogRecord) -> Result<(), Error> {
        if self.poisoned.load(Ordering::SeqCst) {
            return Err(Error::Journal(
                "journal is poisoned after a failed write; restart the server".to_owned(),
            ));
        }
        let mut line = serde_json::to_vec(record)
            .map_err(|error| Error::Journal(format!("cannot serialize journal record: {error}")))?;
        line.push(b'\n');
        let mut file = self.file.lock().await;
        let offset = file
            .metadata()
            .await
            .map_err(|error| Error::Journal(format!("cannot inspect journal length: {error}")))?
            .len();
        let result = async {
            file.write_all(&line).await?;
            file.flush().await?;
            file.sync_data().await?;
            Ok::<(), std::io::Error>(())
        }
        .await;
        if let Err(error) = result {
            if let Err(rollback) = file.set_len(offset).await {
                self.poisoned.store(true, Ordering::SeqCst);
                return Err(Error::Journal(format!(
                    "cannot append journal record: {error}; truncate failed: {rollback}"
                )));
            }
            let _ = file.sync_data().await;
            return Err(Error::Journal(format!(
                "cannot append journal record: {error}"
            )));
        }
        Ok(())
    }
}

#[async_trait]
impl Journal for FileJournal {
    async fn accept(&self, invocation: &RapInvocation) -> Result<Accept, Error> {
        let mut state = self.state.lock().await;
        let id = invocation.id.as_str().to_owned();
        let value = serde_json::to_value(invocation)
            .map_err(|error| Error::Journal(format!("cannot serialize invocation: {error}")))?;

        if let Some(record) = state.get(&id) {
            return classify(record, invocation);
        }

        self.append(&LogRecord::Accepted {
            id: id.clone(),
            invocation: value.clone(),
        })
        .await?;
        state.insert(
            id,
            Record {
                invocation: value,
                result: None,
                delivered: false,
            },
        );
        Ok(Accept::New)
    }

    async fn finish(&self, id: &ToolCallId, result: &RapToolResult) -> Result<(), Error> {
        let mut state = self.state.lock().await;
        let value = serde_json::to_value(result)
            .map_err(|error| Error::Journal(format!("cannot serialize result: {error}")))?;
        let record = state
            .get_mut(id.as_str())
            .ok_or_else(|| Error::Journal(format!("unknown invocation '{}'", id.as_str())))?;
        self.append(&LogRecord::Finished {
            id: id.as_str().to_owned(),
            result: value.clone(),
        })
        .await?;
        record.result = Some(value);
        Ok(())
    }

    async fn mark_delivered(&self, id: &ToolCallId) -> Result<(), Error> {
        let mut state = self.state.lock().await;
        if let Some(record) = state.get_mut(id.as_str()) {
            self.append(&LogRecord::Delivered {
                id: id.as_str().to_owned(),
            })
            .await?;
            record.delivered = true;
        }
        Ok(())
    }

    async fn pending(&self) -> Result<Vec<Pending>, Error> {
        let state = self.state.lock().await;
        pending_from(&state)
    }

    async fn recover(&self) -> Result<Vec<Recovered>, Error> {
        let state = self.state.lock().await;
        recovered_from(&state)
    }

    async fn result(&self, id: &ToolCallId) -> Result<Option<RapToolResult>, Error> {
        let state = self.state.lock().await;
        result_from(&state, id)
    }

    async fn callback_url(&self, id: &ToolCallId) -> Result<Option<String>, Error> {
        let state = self.state.lock().await;
        callback_url_from(&state, id)
    }
}

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
enum LogRecord {
    Accepted {
        id: String,
        invocation: serde_json::Value,
    },
    Finished {
        id: String,
        result: serde_json::Value,
    },
    Delivered {
        id: String,
    },
}

fn accept_record(
    records: &mut HashMap<String, Record>,
    invocation: &RapInvocation,
) -> Result<Accept, Error> {
    let id = invocation.id.as_str().to_owned();
    let value = serde_json::to_value(invocation)
        .map_err(|error| Error::Journal(format!("cannot serialize invocation: {error}")))?;

    if let Some(record) = records.get(&id) {
        return classify(record, invocation);
    }

    records.insert(
        id,
        Record {
            invocation: value,
            result: None,
            delivered: false,
        },
    );
    Ok(Accept::New)
}

fn classify(record: &Record, invocation: &RapInvocation) -> Result<Accept, Error> {
    let stored: RapInvocation = serde_json::from_value(record.invocation.clone())
        .map_err(|error| Error::Journal(format!("cannot decode invocation: {error}")))?;
    if identity(&stored) != identity(invocation) {
        return Ok(Accept::Conflict);
    }
    Ok(if record.delivered {
        Accept::DuplicateDelivered
    } else {
        Accept::DuplicatePending
    })
}

/// Identity for duplicate detection. The callback URL is deliberately excluded:
/// a reconnecting runtime may present a new callback URL for the same call.
fn identity(invocation: &RapInvocation) -> serde_json::Value {
    serde_json::json!({
        "operation": invocation.operation,
        "arguments": invocation.arguments,
        "group_id": invocation.group_id,
        "call_id": invocation.call_id,
    })
}

fn callback_url_from(
    records: &HashMap<String, Record>,
    id: &ToolCallId,
) -> Result<Option<String>, Error> {
    let Some(record) = records.get(id.as_str()) else {
        return Ok(None);
    };
    let invocation: RapInvocation = serde_json::from_value(record.invocation.clone())
        .map_err(|error| Error::Journal(format!("cannot decode invocation: {error}")))?;
    Ok(Some(invocation.callback_url))
}

fn apply_log_record(state: &mut HashMap<String, Record>, record: LogRecord) {
    match record {
        LogRecord::Accepted { id, invocation } => {
            state.insert(
                id,
                Record {
                    invocation,
                    result: None,
                    delivered: false,
                },
            );
        }
        LogRecord::Finished { id, result } => {
            if let Some(record) = state.get_mut(&id) {
                record.result = Some(result);
            }
        }
        LogRecord::Delivered { id } => {
            if let Some(record) = state.get_mut(&id) {
                record.delivered = true;
            }
        }
    }
}

fn recovered_from(records: &HashMap<String, Record>) -> Result<Vec<Recovered>, Error> {
    let mut recovered = Vec::new();
    for record in records.values() {
        if record.delivered {
            continue;
        }
        let invocation = serde_json::from_value(record.invocation.clone())
            .map_err(|error| Error::Journal(format!("cannot decode invocation: {error}")))?;
        let result = record
            .result
            .as_ref()
            .map(|value| {
                serde_json::from_value(value.clone())
                    .map_err(|error| Error::Journal(format!("cannot decode result: {error}")))
            })
            .transpose()?;
        recovered.push(Recovered { invocation, result });
    }
    Ok(recovered)
}

fn result_from(
    records: &HashMap<String, Record>,
    id: &ToolCallId,
) -> Result<Option<RapToolResult>, Error> {
    let Some(record) = records.get(id.as_str()) else {
        return Ok(None);
    };
    record
        .result
        .as_ref()
        .map(|value| {
            serde_json::from_value(value.clone())
                .map_err(|error| Error::Journal(format!("cannot decode result: {error}")))
        })
        .transpose()
}

fn pending_from(records: &HashMap<String, Record>) -> Result<Vec<Pending>, Error> {
    let mut pending = Vec::new();
    for record in records.values() {
        let (Some(result), false) = (&record.result, record.delivered) else {
            continue;
        };
        let invocation = serde_json::from_value(record.invocation.clone())
            .map_err(|error| Error::Journal(format!("cannot decode invocation: {error}")))?;
        let result = serde_json::from_value(result.clone())
            .map_err(|error| Error::Journal(format!("cannot decode result: {error}")))?;
        pending.push(Pending { invocation, result });
    }
    Ok(pending)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rap_protocol::{ProviderCallId, ThreadId};

    fn invocation(id: &str, operation: &str) -> RapInvocation {
        RapInvocation {
            operation: operation.to_owned(),
            arguments: serde_json::json!({"command": ["true"]}),
            id: ToolCallId::from(id),
            call_id: None::<ProviderCallId>,
            callback_url: "https://callback.test/".to_owned(),
            group_id: ThreadId::from("thread-1"),
            user_id: None,
            thread_ancestors: None,
        }
    }

    fn result(invocation: &RapInvocation, text: &str) -> RapToolResult {
        RapToolResult {
            group_id: invocation.group_id.clone(),
            id: invocation.id.clone(),
            call_id: invocation.call_id.clone(),
            text: Some(text.to_owned()),
            content: None,
            display_as: None,
            subscription: None,
        }
    }

    #[tokio::test]
    async fn memory_journal_classifies_duplicates_and_conflicts() {
        let journal = MemoryJournal::new();
        let first = invocation("call-1", "openshell_exec");
        assert_eq!(journal.accept(&first).await.unwrap(), Accept::New);
        assert_eq!(
            journal
                .accept(&invocation("call-1", "openshell_exec"))
                .await
                .unwrap(),
            Accept::DuplicatePending
        );
        assert_eq!(
            journal
                .accept(&invocation("call-1", "openshell_read"))
                .await
                .unwrap(),
            Accept::Conflict
        );
    }

    #[tokio::test]
    async fn memory_journal_tracks_finished_results() {
        let journal = MemoryJournal::new();
        let invocation = invocation("call-1", "openshell_exec");
        journal.accept(&invocation).await.unwrap();
        let result = result(&invocation, "ok");
        journal.finish(&invocation.id, &result).await.unwrap();
        assert_eq!(journal.pending().await.unwrap().len(), 1);
        journal.mark_delivered(&invocation.id).await.unwrap();
        assert!(journal.pending().await.unwrap().is_empty());
    }

    #[tokio::test]
    async fn identity_ignores_a_replacement_callback_url() {
        let journal = MemoryJournal::new();
        let first = invocation("call-1", "openshell_exec");
        journal.accept(&first).await.unwrap();

        let mut replacement = invocation("call-1", "openshell_exec");
        replacement.callback_url = "https://other.test/".to_owned();
        assert_eq!(
            journal.accept(&replacement).await.unwrap(),
            Accept::DuplicatePending
        );
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn file_journal_is_private_and_tightens_an_existing_file() {
        use std::os::unix::fs::PermissionsExt;

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("journal.jsonl");
        tokio::fs::write(&path, b"").await.unwrap();
        tokio::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644))
            .await
            .unwrap();

        let journal = FileJournal::open(&path).await.unwrap();
        journal
            .accept(&invocation("call-1", "openshell_exec"))
            .await
            .unwrap();

        let mode = tokio::fs::metadata(&path)
            .await
            .unwrap()
            .permissions()
            .mode()
            & 0o777;
        assert_eq!(mode, 0o600);
    }

    #[tokio::test]
    async fn file_journal_truncates_a_torn_tail() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("journal.jsonl");
        let first = invocation("call-1", "openshell_exec");

        {
            let journal = FileJournal::open(&path).await.unwrap();
            journal.accept(&first).await.unwrap();
        }
        let mut file = tokio::fs::OpenOptions::new()
            .append(true)
            .open(&path)
            .await
            .unwrap();
        file.write_all(b"{\"kind\":\"accepted\"\xc3").await.unwrap();
        file.flush().await.unwrap();
        drop(file);

        let second = invocation("call-2", "openshell_exec");
        {
            let journal = FileJournal::open(&path).await.unwrap();
            journal.accept(&second).await.unwrap();
        }

        let journal = FileJournal::open(&path).await.unwrap();
        let recovered = journal.recover().await.unwrap();
        assert_eq!(recovered.len(), 2);
    }

    #[tokio::test]
    async fn file_journal_repairs_a_missing_final_newline() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("journal.jsonl");
        let first = invocation("call-1", "openshell_exec");

        {
            let journal = FileJournal::open(&path).await.unwrap();
            journal.accept(&first).await.unwrap();
        }
        let contents = tokio::fs::read(&path).await.unwrap();
        let without_newline = contents.strip_suffix(b"\n").unwrap();
        tokio::fs::write(&path, without_newline).await.unwrap();

        let second = invocation("call-2", "openshell_exec");
        {
            let journal = FileJournal::open(&path).await.unwrap();
            journal.accept(&second).await.unwrap();
        }

        let journal = FileJournal::open(&path).await.unwrap();
        assert_eq!(journal.recover().await.unwrap().len(), 2);
    }

    #[tokio::test]
    async fn file_journal_survives_reopen() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("journal.jsonl");
        let invocation = invocation("call-1", "openshell_exec");
        let result = result(&invocation, "persisted");

        {
            let journal = FileJournal::open(&path).await.unwrap();
            journal.accept(&invocation).await.unwrap();
            journal.finish(&invocation.id, &result).await.unwrap();
        }

        let journal = FileJournal::open(&path).await.unwrap();
        let pending = journal.pending().await.unwrap();
        assert_eq!(pending.len(), 1);
        assert_eq!(pending[0].result.text.as_deref(), Some("persisted"));
        assert_eq!(
            journal.accept(&invocation).await.unwrap(),
            Accept::DuplicatePending
        );
    }
}
