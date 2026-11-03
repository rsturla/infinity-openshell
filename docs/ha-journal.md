# The journal and HA deployments

The journal exists because RAP is an acknowledgement protocol. Once `/invoke`
returns 200, the Infinity runtime may have no process at all while the tool
works. The server therefore owns the invocation until it produces a result.

The journal is three things:

1. **Acceptance record.** Written and fsynced before the 200. A crash after
   acknowledgement does not lose the call.
2. **Outbox.** The result is written before delivery. A crash between execution
   and callback does not lose the result.
3. **Idempotency ledger.** The RAP call ID maps to one execution and one result.
   Duplicates attach; conflicting arguments are rejected.

It is not conversation state, a workflow engine, or a replacement for the
client. Infinity still owns the conversation, pending calls, and result dedupe.

## `FileJournal`

`FileJournal` is an append-only JSON-lines store for a single writer:

- every append is flushed and `sync_data`d before the call is acknowledged;
- a torn final record is truncated on open;
- a valid final record without a newline gets one before the next append;
- accepted-but-unfinished records are recovered as an explicit
  "interrupted, retry" result;
- finished-but-undelivered records are replayed through the callback outbox.

It does not compact. For a long-lived deployment, rotate the file on a size or
age threshold and keep the last N files. A delivered record is only needed for
duplicate detection within the client's retry window.

Start one server process per journal file. Startup recovery treats every
unfinished record as abandoned, so it cannot distinguish an abandoned call from
one another process is still running. A shared adapter needs an owner lease and
a heartbeat before more than one replica may call `recover`.

## HA

Multiple server replicas need a shared `Journal`. The trait is the seam:

```rust
#[async_trait]
pub trait Journal: Send + Sync + 'static {
    async fn accept(&self, invocation: &RapInvocation) -> Result<Accept, Error>;
    async fn finish(&self, id: &ToolCallId, result: &RapToolResult) -> Result<(), Error>;
    async fn mark_delivered(&self, id: &ToolCallId) -> Result<(), Error>;
    async fn pending(&self) -> Result<Vec<Pending>, Error>;
    async fn recover(&self) -> Result<Vec<Recovered>, Error>;
    async fn result(&self, id: &ToolCallId) -> Result<Option<RapToolResult>, Error>;
    async fn callback_url(&self, id: &ToolCallId) -> Result<Option<String>, Error>;
}
```

A Postgres or DynamoDB adapter should map `accept` to an idempotent insert keyed
by `id`, `finish` to an update, and `pending`/`recover` to a query on
`delivered = false`. Use a transaction or conditional write for `accept` so two
replicas cannot both claim a new call. The result and the finished flag must be
written together; use a compare-and-set on the record version.

SQS is a good fit when the server is request-scoped: the invocation is the
message, the result is the outbox record, and `id` is the deduplication key. The
`Journal` trait then wraps the queue and the result table.

The design target is one active lifecycle controller per workspace. HTTP
replicas can be many because sandboxes are named globally in the workspace, but
idle/close decisions should go through a coordinator lease. The journal is not
the coordinator; it records accepted work.

## Why not let the client own it

The client can time out and retry, but it cannot produce a result the tool never
sent. Retrying a mutating call without server-side idempotency is worse: it can
run the command twice. The journal plus OpenShell's `request_id` admission gives
the server a safe answer to both problems.

Read-only tools can relax durability. A deployment that only exposes reads can
use `MemoryJournal` and accept a lost call on restart. A deployment that exposes
`openshell_exec` or `openshell_write` should not.
