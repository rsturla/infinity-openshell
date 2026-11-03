# Go agent loop against the Rust RAP server

This example keeps the OpenShell lifecycle in Rust and writes the agent loop in
Go. The Go process calls the RAP endpoints over HTTP/JSON and serves the
callback URL. It never touches OpenShell and needs no Go OpenShell SDK.

Use this shape when the agent loop is written in Go but the team wants the
tested lease, idle, restart, and journal behavior from `openshell-session`.

## Run it

Terminal 1, start the Rust tool server:

```shell
cargo run --features cli --bin infinity-openshell -- serve \
  --gateway demo --base-url http://127.0.0.1:3001
```

Terminal 2, run the Go client:

```shell
cd examples/go-agent
go run .
```

Expected output:

```text
ack: 200 OK
event final=false: go-agent-ok
event final=true: [exit 0]
result:
go-agent-ok
[exit 0]
close: 200 OK
```

## Configuration

| Variable | Default | Purpose |
|---|---|---|
| `RAP_URL` | `http://127.0.0.1:3001` | Rust server base URL |
| `RAP_TOKEN` | empty | Bearer token when the server runs with `--bearer-token` |

The example uses `openshell_exec` with the gateway default image, so it needs no
provider or custom image. Start the server with `--provider` and `--image` to
exercise the network example instead.

## What this does not show

The Go SDK can create sandboxes itself:

```go
client, _ := v1.NewClient(v1.Config{Address: gateway, Auth: v1.StaticToken(token)})
sandbox, _ := client.Sandboxes().Create(ctx, "default", name, &v1.SandboxSpec{
    Template: &v1.SandboxTemplate{Image: "curlimages/curl:8.10.1"},
})
```

That path replaces `openshell-session`: it must also implement generations,
adoption by label, idle stop, start on demand, journaling, and cancellation.
This example avoids that duplication.
