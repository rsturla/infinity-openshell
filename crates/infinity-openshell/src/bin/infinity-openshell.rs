//! Operational entry point for the RAP server.
//!
//! The binary is a thin composition of the library. All configuration and
//! lifecycle logic lives in `infinity-openshell` and `openshell-session`.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use clap::{Args, Parser, Subcommand, ValueEnum};
use infinity_openshell::{CallbackAuth, InboundAuth, ServerConfig, configure_labels};
use openshell_session::{
    BootstrapTokenSource, GatewayAuth, GatewayConfig, RuntimeConfig, SessionRuntime,
    TemplatePolicy, TenantId,
};
use tracing_subscriber::EnvFilter;
use url::Url;

#[derive(Parser)]
#[command(name = "infinity-openshell", version, about)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Validate configuration and connect to the gateway.
    Check(ServeArgs),
    /// Serve the RAP tool server.
    Serve(ServeArgs),
}

#[derive(Clone, Copy, Debug, ValueEnum)]
enum CallbackAuthArg {
    Plain,
    Sigv4,
}

#[derive(Args, Clone)]
struct ServeArgs {
    /// Address to bind the RAP server.
    #[arg(
        long,
        env = "INFINITY_OPENSHELL_BIND",
        default_value = "127.0.0.1:3001"
    )]
    bind: String,
    /// Public base URL advertised at discovery. Defaults to `http://{bind}`.
    #[arg(long, env = "INFINITY_OPENSHELL_BASE_URL")]
    base_url: Option<String>,
    /// Tenant used for every session key.
    #[arg(long, env = "INFINITY_OPENSHELL_TENANT", default_value = "default")]
    tenant: String,
    /// Registered OpenShell gateway name. Loads the cached OIDC token and
    /// refreshes it, persisting rotated tokens.
    #[arg(long, env = "OPENSHELL_GATEWAY")]
    gateway: Option<String>,
    /// Direct OpenShell gateway endpoint. Use with --token.
    #[arg(long, env = "OPENSHELL_ENDPOINT")]
    endpoint: Option<String>,
    /// OpenShell workspace.
    #[arg(long, env = "OPENSHELL_WORKSPACE", default_value = "default")]
    workspace: String,
    /// Static OIDC bearer token. Prefer --gateway for a long-lived server.
    #[arg(long, env = "OPENSHELL_TOKEN", hide_env_values = true)]
    token: Option<String>,
    /// Workload template.
    #[arg(long, env = "OPENSHELL_TEMPLATE")]
    template: Option<String>,
    /// Custom OCI image for sandboxes. Do not combine with --template.
    #[arg(long, env = "OPENSHELL_IMAGE")]
    image: Option<String>,
    /// Provider instance to attach to every sandbox. Repeatable.
    #[arg(long = "provider", env = "OPENSHELL_PROVIDERS", value_delimiter = ',')]
    providers: Vec<String>,
    /// Sandbox policy YAML file applied at sandbox creation.
    #[arg(long, env = "OPENSHELL_POLICY")]
    policy: Option<PathBuf>,
    /// Append-only journal path. Omit for an in-memory journal.
    #[arg(long, env = "INFINITY_OPENSHELL_JOURNAL")]
    journal: Option<PathBuf>,
    /// Require this bearer token on inbound RAP requests.
    #[arg(long, env = "INFINITY_OPENSHELL_BEARER_TOKEN", hide_env_values = true)]
    bearer_token: Option<String>,
    /// Allow an unauthenticated server on a non-loopback bind. Development only.
    #[arg(long, default_value_t = false)]
    allow_unauthenticated: bool,
    /// Allow non-loopback plain HTTP callbacks. Development only.
    #[arg(long, default_value_t = false)]
    allow_insecure_callback_http: bool,
    /// Allowed callback host. Repeatable. Empty allows loopback only.
    #[arg(long = "callback-allowed-host")]
    callback_allowed_hosts: Vec<String>,
    /// Callback authentication.
    #[arg(long, value_enum, default_value = "plain")]
    callback_auth: CallbackAuthArg,
    /// SigV4 signing region. Defaults to the AWS default region.
    #[arg(long, env = "AWS_REGION")]
    aws_region: Option<String>,
    /// SigV4 signing service.
    #[arg(long, default_value = "lambda")]
    aws_service: String,
    /// Idle seconds before the sandbox stops.
    #[arg(long, default_value_t = 60)]
    idle_after_seconds: u64,
    /// Seconds a stopped sandbox is retained before deletion.
    #[arg(long, default_value_t = 86_400)]
    retain_for_seconds: u64,
    /// Seconds between streaming subscription events.
    #[arg(long, default_value_t = 10)]
    stream_interval_seconds: u64,
    /// Working directory inside the sandbox.
    #[arg(long, default_value = "/sandbox")]
    workdir: String,
}

impl ServeArgs {
    fn runtime_config(&self) -> Result<RuntimeConfig, Box<dyn std::error::Error>> {
        let (endpoint, auth) = match (&self.gateway, &self.endpoint, &self.token) {
            (Some(gateway), _, _) => {
                let source = Arc::new(BootstrapTokenSource::new(gateway)?);
                (source.endpoint()?, GatewayAuth::OidcSource { source })
            }
            (None, Some(endpoint), Some(token)) => (
                endpoint.parse::<Url>()?,
                GatewayAuth::Oidc {
                    token: token.clone(),
                },
            ),
            (None, Some(endpoint), None) => {
                return Err(format!("--token is required with --endpoint ({endpoint})").into());
            }
            _ => {
                return Err("--gateway or --endpoint/--token is required".into());
            }
        };

        let mut config =
            RuntimeConfig::new(GatewayConfig::new(endpoint, self.workspace.clone(), auth));
        configure_labels(&mut config);
        if let Some(template) = &self.template {
            config.template = TemplatePolicy::named(template.clone());
        }
        config.image = self.image.clone();
        config.providers = self.providers.clone();
        if let Some(path) = &self.policy {
            config.policy = Some(std::fs::read_to_string(path)?);
        }
        config.workdir = self.workdir.clone();
        config.lifecycle.idle_after = Duration::from_secs(self.idle_after_seconds);
        config.lifecycle.retain_for = Duration::from_secs(self.retain_for_seconds);
        config.validate()?;
        Ok(config)
    }

    fn server_config(&self) -> Result<ServerConfig, Box<dyn std::error::Error>> {
        let base_url = self
            .base_url
            .clone()
            .unwrap_or_else(|| format!("http://{}", self.bind));
        let mut config = ServerConfig::new(base_url.parse()?, TenantId::new(self.tenant.clone())?);
        config.inbound_auth = self
            .bearer_token
            .clone()
            .map_or(InboundAuth::None, |token| InboundAuth::Bearer { token });
        if matches!(config.inbound_auth, InboundAuth::None)
            && !is_loopback_bind(&self.bind)
            && !self.allow_unauthenticated
        {
            return Err(format!(
                "refusing an unauthenticated server on {}; pass --bearer-token or --allow-unauthenticated",
                self.bind
            )
            .into());
        }
        if matches!(config.inbound_auth, InboundAuth::None) && !is_loopback_bind(&self.bind) {
            tracing::warn!(
                bind = %self.bind,
                "serving an unauthenticated RAP server on a non-loopback address"
            );
        }
        config.callback_auth = match self.callback_auth {
            CallbackAuthArg::Plain => CallbackAuth::Plain,
            CallbackAuthArg::Sigv4 => CallbackAuth::SigV4 {
                region: self.aws_region.clone(),
                service: self.aws_service.clone(),
            },
        };
        config.journal_path = self.journal.clone();
        config.callback_allowed_hosts = self.callback_allowed_hosts.clone();
        config.allow_insecure_callback_http = self.allow_insecure_callback_http;
        config.stream_interval = Duration::from_secs(self.stream_interval_seconds);
        config.validate()?;
        Ok(config)
    }
}

fn is_loopback_bind(bind: &str) -> bool {
    bind.parse::<std::net::SocketAddr>().map_or_else(
        |_| bind.starts_with("localhost:"),
        |address| address.ip().is_loopback(),
    )
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_ansi(false)
        .init();
    let cli = Cli::parse();

    match cli.command {
        Command::Check(args) => {
            args.server_config()?;
            let runtime = SessionRuntime::open(args.runtime_config()?).await?;
            drop(runtime);
            println!("ok");
        }
        Command::Serve(args) => {
            let runtime = SessionRuntime::open(args.runtime_config()?).await?;
            let config = args.server_config()?;
            let bind = args.bind.clone();
            let app = infinity_openshell::router(runtime, config).await?;
            let listener = tokio::net::TcpListener::bind(&bind).await?;
            tracing::info!(address = %listener.local_addr()?, "Infinity OpenShell RAP server listening");
            axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = tokio::signal::ctrl_c().await;
                })
                .await?;
        }
    }
    Ok(())
}
