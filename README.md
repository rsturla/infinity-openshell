# infinity-openshell

Elastic OpenShell sandbox sessions for agents built with the
[Infinity](https://github.com/hydro-project/infinity) framework.

An agent thread gets one sandbox workspace. The sandbox stays ready while a tool
call is in flight, stops when the thread goes idle, and starts again when the
next call arrives. The filesystem survives the stop. The agent process does not
need to.

This repository is a library, not an agent runtime. Teams build their agent with
Infinity and mount the OpenShell tool server into that process.

## Status

Early implementation. `openshell-session` and `infinity-openshell` are
implemented. A separate testkit crate and an operational binary are next. The
design is in [`docs/production-design.md`](docs/production-design.md).

## Crates

| Crate | Role |
|---|---|
| `openshell-session` | Lease-managed OpenShell sandbox sessions; the fake gateway lives here behind the `testkit` feature |
| `infinity-openshell` | RAP tool server: discovery, invoke, journal, callback outbox, core tools |

## Build

```shell
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```

Live tests need a registered OpenShell gateway and the `bootstrap` feature:

```shell
OPENSHELL_TEST_GATEWAY=demo cargo test --features bootstrap -- --ignored
```

## License

Apache-2.0.
