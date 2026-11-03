//! Provider profiles and instances managed through the gateway API.
//!
//! The OpenShell CLI is a convenience wrapper around the same RPCs. An
//! embedding service can use [`ProviderAdmin`] to import a profile and ensure a
//! provider before it creates any sandbox, without shelling out.

use std::collections::BTreeMap;
use std::fmt;

use openshell_sdk::SdkError;
use openshell_sdk::raw::AuthedGrpcClient;
use openshell_sdk::raw::proto;

use crate::config::RuntimeConfig;
use crate::error::Error;
use crate::openshell::{connect_client, map_sdk};

/// One provider instance to create or update.
///
/// Credentials are secret values. The manual [`fmt::Debug`] implementation
/// redacts them.
#[derive(Clone)]
pub struct ProviderSpec {
    /// Provider name. Sandboxes attach this name.
    pub name: String,
    /// Profile ID, such as `github` or a custom profile.
    pub profile: String,
    /// Workspace that stores the profile. `None` selects the platform scope.
    pub profile_workspace: Option<String>,
    /// Credential values keyed by the names the profile declares.
    pub credentials: BTreeMap<String, String>,
    /// Non-secret provider configuration.
    pub config: BTreeMap<String, String>,
    /// Labels attached to the provider record.
    pub labels: BTreeMap<String, String>,
}

impl ProviderSpec {
    /// Construct a provider spec with no credentials.
    pub fn new(name: impl Into<String>, profile: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            profile: profile.into(),
            profile_workspace: None,
            credentials: BTreeMap::new(),
            config: BTreeMap::new(),
            labels: BTreeMap::new(),
        }
    }

    /// Add one credential value.
    #[must_use]
    pub fn credential(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.credentials.insert(key.into(), value.into());
        self
    }

    /// Store the profile in the runtime workspace instead of the platform scope.
    #[must_use]
    pub fn workspace_profile(mut self, workspace: impl Into<String>) -> Self {
        self.profile_workspace = Some(workspace.into());
        self
    }

    fn validate(&self) -> Result<(), Error> {
        if self.name.trim().is_empty() {
            return Err(Error::InvalidInput(
                "provider name must not be empty".to_owned(),
            ));
        }
        if self.profile.trim().is_empty() {
            return Err(Error::InvalidInput(
                "provider profile must not be empty".to_owned(),
            ));
        }
        Ok(())
    }
}

impl fmt::Debug for ProviderSpec {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProviderSpec")
            .field("name", &self.name)
            .field("profile", &self.profile)
            .field("profile_workspace", &self.profile_workspace)
            .field("credentials", &"<redacted>")
            .field("config", &self.config)
            .field("labels", &self.labels)
            .finish()
    }
}

/// Provider profile and instance administration for one gateway workspace.
pub struct ProviderAdmin {
    client: openshell_sdk::OpenShellClient,
    workspace: String,
}

impl ProviderAdmin {
    /// Connect with the same gateway and authentication as a runtime.
    ///
    /// # Errors
    ///
    /// Returns a configuration, transport, authentication, or health error.
    pub async fn connect(config: &RuntimeConfig) -> Result<Self, Error> {
        Ok(Self {
            client: connect_client(config).await?,
            workspace: config.gateway.workspace.clone(),
        })
    }

    /// Import a profile from YAML into the configured workspace or platform
    /// scope.
    ///
    /// Import is create-only. Use [`ProviderAdmin::ensure_profile`] for a
    /// re-runnable setup.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`] for invalid YAML or a gateway
    /// diagnostic, and a gateway error for transport or permission failures.
    pub async fn import_profile(&self, yaml: &str, global: bool) -> Result<(), Error> {
        let profile = parse_profile(yaml)?;
        let mut grpc = self.grpc().await?;
        let response = grpc
            .import_provider_profiles(proto::ImportProviderProfilesRequest {
                workspace_scope: self.scope(global),
                profiles: vec![proto::ProviderProfileImportItem {
                    profile: Some(profile),
                    source: "openshell-session".to_owned(),
                }],
                request_id: String::new(),
            })
            .await
            .map_err(|status| map_sdk(SdkError::from_status(status)))?
            .into_inner();
        if !response.imported {
            return Err(Error::InvalidInput(format!(
                "provider profile import failed: {}",
                diagnostics(&response.diagnostics)
            )));
        }
        Ok(())
    }

    /// Create the profile, or update it when it already exists.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`] for invalid YAML or a gateway
    /// diagnostic, and a gateway error for transport or permission failures.
    pub async fn ensure_profile(&self, yaml: &str, global: bool) -> Result<(), Error> {
        let mut profile = parse_profile(yaml)?;
        let id = profile.id.clone();
        let mut grpc = self.grpc().await?;
        let existing = grpc
            .get_provider_profile(proto::GetProviderProfileRequest {
                workspace_scope: self.scope(global),
                id: id.clone(),
            })
            .await
            .map_err(|status| map_sdk(SdkError::from_status(status)));

        match existing {
            Ok(response) => {
                let version = response
                    .into_inner()
                    .profile
                    .map_or(0, |profile| profile.resource_version);
                profile.resource_version = version;
                let response = grpc
                    .update_provider_profiles(proto::UpdateProviderProfilesRequest {
                        workspace_scope: self.scope(global),
                        profile: Some(proto::ProviderProfileImportItem {
                            profile: Some(profile),
                            source: "openshell-session".to_owned(),
                        }),
                        expected_resource_version: version,
                        id,
                        request_id: String::new(),
                    })
                    .await
                    .map_err(|status| map_sdk(SdkError::from_status(status)))?
                    .into_inner();
                if !response.updated {
                    return Err(Error::InvalidInput(format!(
                        "provider profile update failed: {}",
                        diagnostics(&response.diagnostics)
                    )));
                }
                Ok(())
            }
            Err(Error::NotFound(_)) => self.import_profile(yaml, global).await,
            Err(error) => Err(error),
        }
    }

    /// Create the provider, or update it when it already exists.
    ///
    /// # Errors
    ///
    /// Returns [`Error::InvalidInput`] for an empty name or profile, and a
    /// gateway error for transport or permission failures.
    pub async fn ensure_provider(&self, spec: &ProviderSpec) -> Result<(), Error> {
        spec.validate()?;
        let mut grpc = self.grpc().await?;
        let existing = grpc
            .get_provider(proto::GetProviderRequest {
                workspace_scope: self.scope(false),
                name: spec.name.clone(),
            })
            .await
            .map_err(|status| map_sdk(SdkError::from_status(status)));

        let mut provider = self.provider(spec);
        match existing {
            Ok(response) => {
                let version = response
                    .into_inner()
                    .provider
                    .and_then(|provider| provider.metadata)
                    .map_or(0, |metadata| metadata.resource_version);
                if let Some(metadata) = provider.metadata.as_mut() {
                    metadata.resource_version = version;
                }
                grpc.update_provider(proto::UpdateProviderRequest {
                    workspace_scope: self.scope(false),
                    provider: Some(provider),
                    credential_expiration_times: std::collections::HashMap::new(),
                    clear_credential_expiration_keys: Vec::new(),
                    request_id: String::new(),
                })
                .await
                .map_err(|status| map_sdk(SdkError::from_status(status)))?;
                Ok(())
            }
            Err(Error::NotFound(_)) => {
                grpc.create_provider(proto::CreateProviderRequest {
                    workspace_scope: self.scope(false),
                    provider: Some(provider),
                    request_id: String::new(),
                })
                .await
                .map_err(|status| map_sdk(SdkError::from_status(status)))?;
                Ok(())
            }
            Err(error) => Err(error),
        }
    }

    /// Delete a provider. A missing provider is not an error.
    ///
    /// # Errors
    ///
    /// Returns a gateway error for transport or permission failures.
    pub async fn delete_provider(&self, name: &str) -> Result<(), Error> {
        let mut grpc = self.grpc().await?;
        grpc.delete_provider(proto::DeleteProviderRequest {
            workspace_scope: self.scope(false),
            name: name.to_owned(),
            allow_missing: true,
            request_id: String::new(),
        })
        .await
        .map_err(|status| map_sdk(SdkError::from_status(status)))?;
        Ok(())
    }

    /// Delete a profile. A missing profile is not an error.
    ///
    /// # Errors
    ///
    /// Returns a gateway error for transport or permission failures, including
    /// a profile that a sandbox-attached provider still uses.
    pub async fn delete_profile(&self, id: &str, global: bool) -> Result<(), Error> {
        let mut grpc = self.grpc().await?;
        grpc.delete_provider_profile(proto::DeleteProviderProfileRequest {
            workspace_scope: self.scope(global),
            id: id.to_owned(),
            allow_missing: true,
            request_id: String::new(),
        })
        .await
        .map_err(|status| map_sdk(SdkError::from_status(status)))?;
        Ok(())
    }

    async fn grpc(&self) -> Result<AuthedGrpcClient, Error> {
        self.client.raw_grpc_fresh().await.map_err(map_sdk)
    }

    fn scope(&self, global: bool) -> Option<proto::WorkspaceSelector> {
        (!global).then(|| proto::workspace_selector(&self.workspace))
    }

    fn provider(&self, spec: &ProviderSpec) -> proto::Provider {
        proto::Provider {
            metadata: Some(proto::ObjectMeta {
                name: spec.name.clone(),
                workspace: self.workspace.clone(),
                labels: spec.labels.clone().into_iter().collect(),
                ..proto::ObjectMeta::default()
            }),
            r#type: spec.profile.clone(),
            credentials: spec.credentials.clone().into_iter().collect(),
            config: spec.config.clone().into_iter().collect(),
            profile_workspace: spec.profile_workspace.clone().unwrap_or_default(),
            ..proto::Provider::default()
        }
    }
}

fn parse_profile(yaml: &str) -> Result<proto::ProviderProfile, Error> {
    openshell_providers::parse_profile_yaml(yaml)
        .map(|profile| profile.to_proto())
        .map_err(|error| Error::InvalidInput(format!("invalid provider profile: {error}")))
}

fn diagnostics(diagnostics: &[proto::ProviderProfileDiagnostic]) -> String {
    let text = diagnostics
        .iter()
        .filter(|diagnostic| diagnostic.severity.eq_ignore_ascii_case("error"))
        .map(|diagnostic| {
            format!(
                "{} {}: {}",
                diagnostic.profile_id, diagnostic.field, diagnostic.message
            )
        })
        .collect::<Vec<_>>()
        .join("; ");
    if text.is_empty() {
        "no diagnostic detail".to_owned()
    } else {
        text
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn spec_debug_redacts_credentials() {
        let spec = ProviderSpec::new("work-github", "github").credential("GITHUB_TOKEN", "secret");
        let text = format!("{spec:?}");
        assert!(!text.contains("secret"), "{text}");
        assert!(text.contains("<redacted>"), "{text}");
    }

    #[test]
    fn invalid_profile_yaml_is_rejected() {
        let error = parse_profile("id: Bad_ID").unwrap_err();
        assert!(matches!(error, Error::InvalidInput(_)), "{error}");
    }

    #[test]
    fn spec_requires_a_name_and_profile() {
        assert!(ProviderSpec::new("", "github").validate().is_err());
        assert!(ProviderSpec::new("work", "").validate().is_err());
        assert!(ProviderSpec::new("work", "github").validate().is_ok());
    }
}
