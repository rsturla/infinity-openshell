# infinity-openshell

Elastic OpenShell sandbox sessions for agents built with the
[Infinity](https://github.com/hydro-project/infinity) framework.

An agent thread gets one sandbox workspace. The sandbox stays ready while a tool
call is in flight, stops when the thread goes idle, and starts again when the
next call arrives. The filesystem survives the stop. The agent process does not
need to.

This repository is a library, not an agent runtime. Teams build their agent with
Infinity and mount the OpenShell tool server into that process. The binary is a
thin optional front end for teams that have no service to embed it in.

## Crates

| Crate | Role |
|---|---|
| `openshell-session` | Lease-managed OpenShell sandbox sessions, streaming exec, token refresh |
| `infinity-openshell` | RAP tool server: discovery, invoke, journal, callback outbox, core tools |
| `infinity-openshell-testkit` | Fake gateway and a ready-wired test server |

## Features

- One persistent workspace per conversation thread.
- Idle stop, start on the next call, retention deletion, explicit close.
- Generation-bound leases, so a stale call cannot touch a replacement sandbox.
- Cross-runtime adoption by label after a restart.
- Streaming `exec` with subscription events and a bounded output cap.
- OpenShell `request_id` durable admission for exec and writes.
- Durable append-only journal with torn-tail repair and crash recovery.
- Callback retries, a background outbox, HTTPS/host policy, and SigV4 signing.
- Refreshable OIDC tokens with rotated-token persistence.
- RAP `tool_result`, `subscription_event`, cancellation, and close handling.

## Quick start: local, no gateway

```shell
cargo run -p infinity-openshell --example fake_end_to_end
```

This starts the real RAP router over the fake gateway and exercises exec, write,
read, and close.

## Quick start: binary

```shell
cargo run --features cli --bin infinity-openshell -- \
  check --gateway demo --base-url http://127.0.0.1:3001

cargo run --features cli --bin infinity-openshell -- \
  serve --gateway demo --base-url http://127.0.0.1:3001
```

Registration with Infinity:

```json
{
  "tool_sets": [
    { "type": "toolset_server", "server_url": "http://127.0.0.1:3001" }
  ]
}
```

## Network access, providers, and custom images

A sandbox starts with no outbound network access. Manage the three layers from
Rust, without the OpenShell CLI:

```rust
let mut config = RuntimeConfig::new(gateway);
config.image = Some("curlimages/curl:8.10.1".to_owned());
config.policy = Some(POLICY_YAML.to_owned());

let admin = ProviderAdmin::connect(&config).await?;
admin.ensure_profile(PROFILE_YAML, false).await?;
admin
    .ensure_provider(
        &ProviderSpec::new("work-github", "github")
            .credential("GITHUB_TOKEN", token),
    )
    .await?;

config.providers = vec!["work-github".to_owned()];
let runtime = SessionRuntime::open(config).await?;
```

- The policy authorizes traffic: binary, host, port, path, and method.
- The profile declares credentials, endpoints, and binaries, and contributes
  network rules to the sandbox.
- The provider stores the credential values. The sandbox sees only a
  placeholder.

Read [Network access](docs/network-access.md) for the policy format, per-sandbox
policy updates, and custom image requirements.

## Examples

All examples are runnable and were validated against the demo gateway.

```shell
# Local, no gateway
cargo run -p infinity-openshell --example fake_end_to_end
cargo run -p infinity-openshell --example journal_recovery
cargo run -p infinity-openshell --example sigv4_callback --features sigv4

# Live
OPENSHELL_TEST_GATEWAY=demo cargo run -p infinity-openshell --example live_session --features bootstrap
OPENSHELL_TEST_GATEWAY=demo cargo run -p infinity-openshell --example live_rap --features bootstrap
OPENSHELL_TEST_GATEWAY=demo cargo run -p infinity-openshell --example live_streaming --features bootstrap
OPENSHELL_TEST_GATEWAY=demo cargo run -p infinity-openshell --example live_cancel --features bootstrap

# Network access: provider profile, provider instance, custom image
OPENSHELL_TEST_GATEWAY=demo cargo run -p infinity-openshell --example network_access --features bootstrap

# Create-time sandbox policy
OPENSHELL_TEST_GATEWAY=demo cargo run -p infinity-openshell --example live_policy --features bootstrap

# Full Infinity agent -> RAP -> OpenShell
OPENSHELL_TEST_GATEWAY=demo cargo run -p infinity-openshell --example agent --features bootstrap
```

## Build and test

```shell
cargo fmt --all --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all-targets --all-features
./scripts/check-separation.sh
```

Live tests:

```shell
OPENSHELL_TEST_GATEWAY=demo cargo test --features bootstrap -- --ignored
```

## Documentation

- [Production design](docs/production-design.md)
- [Network access, providers, and custom images](docs/network-access.md)
- [Journal and HA deployments](docs/ha-journal.md)
- [RAP callbacks beyond tool results](docs/rap-callbacks.md)

## Known limits

- `FileJournal` is single-writer. HA needs a shared `Journal` adapter
  (Postgres, DynamoDB, or SQS); see [the HA note](docs/ha-journal.md).
- The core toolset is `exec`/`read`/`write`. Coding tools, approvals, and diff
  views are a later toolset; the transport already supports `user_choice` and
  `view_update`.
- SigV4 signing is validated locally; a live Lambda Function URL needs AWS
  credentials and is deployment-specific.

## License

Apache-2.0.
