# RAP callbacks beyond tool results

The adapter sends `tool_result`, `subscription_event`, and, when a tool needs
them, any other `RapCallback` variant. The core toolset uses only the first two.

## Implemented for the core tools

- **`tool_result`**: one final result per invocation. Errors use the normal
  result path with an `Error: ` prefix.
- **`subscription_event`**: emitted while `openshell_exec` streams output. A
  non-final event carries the output since the previous event; the final event
  has `final: true`. The final `tool_result` still carries the complete output.
- **`cancel_tool_call`** / **`close_thread`**: advisory endpoints, always 200.
  Cancellation aborts the local stream and is bounded by the exec timeout;
  close cancels in-flight calls, waits for the thread gate, and deletes the
  sandbox.

## Supported by the callback client but unused

`CallbackClient::post` accepts the whole `RapCallback` enum, so a future tool can
send these without changing the transport:

- **`oauth`**: an authorization URL for a provider.
- **`user_choice`**: an approval prompt. A tool that needs write approval would
  send this and wait for the runtime's `user_choice_response` endpoint.
- **`view_update`**: a named view payload, such as a file diff. A coding toolset
  would emit this after an edit.

The core `exec`/`read`/`write` tools do not need approvals or diffs, so sending
these today would be noise. When the coding toolset lands, the approval and
diff paths belong in that toolset, not in the transport.

## Delivery

- callback URLs are validated before every attempt: HTTPS, loopback HTTP only
  unless explicitly allowed, no userinfo or fragment, optional host allowlist,
  and redirects are disabled;
- 4xx is terminal and marks the record delivered; 5xx and transport errors are
  retried inline and by the background outbox;
- the result is journaled before delivery, so a crash between execution and
  callback replays the result;
- a duplicate invocation with a changed callback URL attaches to the existing
  job or receives the stored result directly.
