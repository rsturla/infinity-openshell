# Network access, providers, and custom images

A sandbox starts with no outbound network access. A tool can reach another
network only when a policy authorizes the traffic. A credential reaches that
network only when a provider binds it to the same endpoint.

## Three separate layers

| Layer | What it decides | Where it lives |
|---|---|---|
| Sandbox policy | Which binary may reach which host, port, path, and method | `RuntimeConfig::policy` or the gateway policy |
| Provider profile | Which credentials, endpoints, and binaries belong together | Gateway profile catalog |
| Provider instance | The credential values for one profile | Gateway workspace, attached to a sandbox by name |

A profile contributes network rules to the sandbox effective policy. The
attached provider then supplies the credential. Network access and credential
binding are independent checks:

- the policy must allow the binary, host, port, and request;
- the credential binding must cover the same host, port, and path.

The workload never receives the real credential. It receives an opaque
placeholder such as `openshell:resolve:env:v123_GITHUB_TOKEN`. The sandbox
proxy replaces the placeholder only for an authorized request.

## Do it from Rust

`openshell-session` exposes the same gateway operations as the CLI. No
`openshell` process is needed.

```rust
use std::collections::BTreeMap;
use openshell_session::{
    ProviderAdmin, ProviderSpec, RuntimeConfig, SessionRuntime,
};

let mut config = RuntimeConfig::new(gateway);
config.image = Some("curlimages/curl:8.10.1".to_owned());
config.policy = Some(POLICY_YAML.to_owned());

let admin = ProviderAdmin::connect(&config).await?;
admin.ensure_profile(PROFILE_YAML, false).await?;
admin
    .ensure_provider(
        &ProviderSpec::new("work-github", "github")
            .workspace_profile("default")
            .credential("GITHUB_TOKEN", token),
    )
    .await?;

config.providers = vec!["work-github".to_owned()];
let runtime = SessionRuntime::open(config).await?;
```

- `ensure_profile` imports the profile, or updates it when it exists.
- `ensure_provider` creates the provider, or updates it when it exists.
- `ProviderSpec` redacts credentials in its `Debug` output.
- The profile scope is the second argument: `false` uses the runtime workspace,
  `true` uses the platform scope.

`RuntimeConfig::validate` parses the policy YAML before any sandbox is created.
It rejects an invalid policy, an empty provider name, and an image combined with
a template.

## Write the policy

A policy is YAML with `version: 1`. The default policy denies all network
traffic. A policy that defines `network_policies` replaces that default, so
include the filesystem paths the image needs:

```yaml
version: 1
filesystem_policy:
  include_workdir: true
  read_only: [/usr, /lib, /etc, /bin, /proc, /dev/urandom, /var/log]
  read_write: [/tmp, /dev/null]
network_policies:
  github_api:
    endpoints:
      - host: api.github.com
        port: 443
        protocol: rest
        access: read-only
        enforcement: enforce
    binaries:
      - path: /usr/bin/curl
```

- `binaries` names the real executable path in the image, not the command you
  type. A Python client needs the interpreter path.
- `access: read-only` allows `GET`, `HEAD`, and `OPTIONS`. Use `full` only when
  the tool must write.
- `protocol: rest` inspects each request. Omit it to allow any method and path.
- `enforcement: audit` logs violations without blocking them. Use it to test a
  new rule, then switch to `enforce`.

## Apply a policy to a sandbox

`RuntimeConfig::policy` applies to every sandbox that one runtime creates. One
runtime therefore serves one toolset and one egress shape. Run a second runtime
when a tenant needs a different policy.

The runtime adopts existing sandboxes by label. A changed policy affects new
sandboxes, not an adopted one. To change a live sandbox:

```shell
openshell policy update <sandbox> \
  --rule-name github_api \
  --binary /usr/bin/curl \
  --add-endpoint api.github.com:443:read-only:rest:enforce \
  --wait
```

Inspect the result:

```shell
openshell policy get <sandbox> --base
openshell policy get <sandbox> --full
```

`--full` includes the `_provider_*` rules that attached providers contribute.
`--base` omits them. A gateway-global policy suppresses provider-derived rules.

## Use a custom image

Set `RuntimeConfig::image` or pass `--image` to the binary. It is mutually
exclusive with a named template.

```shell
cargo run --features cli --bin infinity-openshell -- serve \
  --gateway demo \
  --image curlimages/curl:8.10.1 \
  --provider infinity-example \
  --policy examples/policies/example.yaml
```

The image must meet these conditions:

- The gateway compute driver can pull it. A remote gateway needs a registry
  reference, not a local Docker tag.
- It runs as a non-root user. OpenShell rejects a root image unless the policy
  sets `process_policy.run_as_user`.
- It contains the client binaries that the policy and profile name. The built-in
  Ubuntu image is minimal and has no `curl`, `wget`, or `python3`.
- The workspace directory is writable. OpenShell uses the image `WORKDIR`, or
  `/sandbox` when the image has none. Keep `RuntimeConfig::workdir` aligned.

## CLI alternative

The CLI calls the same RPCs. Use it for inspection or for manual setup:

```shell
openshell profile lint -f examples/profiles/infinity-example.yaml -g demo
openshell profile import -f examples/profiles/infinity-example.yaml -g demo
openshell provider create -g demo \
  --name infinity-example --type infinity-example \
  --credential GITHUB_TOKEN="$GITHUB_TOKEN"

openshell profile describe infinity-example -g demo -o yaml
openshell provider list -g demo
openshell sandbox provider list <sandbox> -g demo
```

Clean up with `ProviderAdmin::delete_provider` and
`ProviderAdmin::delete_profile`, or with `openshell provider delete` and
`openshell profile delete`.

## Validated examples

```shell
# Provider profile + provider instance + custom image.
OPENSHELL_TEST_GATEWAY=demo \
  cargo run -p infinity-openshell --example network_access --features bootstrap

# Create-time policy with no provider.
OPENSHELL_TEST_GATEWAY=demo \
  cargo run -p infinity-openshell --example live_policy --features bootstrap
```

`network_access` prints a placeholder token, a `401` from the allowed GitHub
endpoint, and a connection failure for `example.com`. `live_policy` prints
`200` for the policy host and a connection failure for an unlisted host.
