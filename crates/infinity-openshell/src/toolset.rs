//! Toolset discovery.

use rap_protocol::{ToolDef, ToolsetManifest};
use serde_json::json;

use crate::config::ServerConfig;

/// Build the RAP toolset manifest.
pub fn manifest(config: &ServerConfig) -> ToolsetManifest {
    ToolsetManifest {
        name: config.toolset_name.clone(),
        description: Some(
            "Run commands and move files in persistent OpenShell sandboxes. Each conversation thread has one sandbox workspace that survives idle periods until the thread is closed."
                .to_owned(),
        ),
        endpoint: config.endpoint(),
        needs_migration: false,
        tools: vec![
            ToolDef {
                name: "openshell_exec".to_owned(),
                description: "Run one command in the thread's OpenShell sandbox. Files written by previous calls are still present while the thread is alive. The command is an argv array and does not use a shell unless you invoke one.".to_owned(),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "command": {
                            "type": "array",
                            "items": { "type": "string" },
                            "minItems": 1,
                            "description": "Command and arguments."
                        },
                        "cwd": {
                            "type": "string",
                            "description": "Optional working directory inside the sandbox."
                        },
                        "stdin": {
                            "type": "string",
                            "description": "Optional UTF-8 stdin payload."
                        }
                    },
                    "required": ["command"],
                    "additionalProperties": false
                }),
                annotations: None,
                display_script: Some(r#""$ " + args.command.join(" ")"#.to_owned()),
            },
            ToolDef {
                name: "openshell_read".to_owned(),
                description: "Read a UTF-8 file from the thread's OpenShell sandbox. Paths are relative to the sandbox workspace. Use start_line and end_line for a range; line numbers are 1-based and inclusive.".to_owned(),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "path": { "type": "string" },
                        "start_line": { "type": "integer", "minimum": 1 },
                        "end_line": { "type": "integer", "minimum": 1 }
                    },
                    "required": ["path"],
                    "additionalProperties": false
                }),
                annotations: None,
                display_script: Some(r#"let s = "Read " + args.path; if args.start_line != () { s += ":" + args.start_line; } s"#.to_owned()),
            },
            ToolDef {
                name: "openshell_write".to_owned(),
                description: "Write a UTF-8 file in the thread's OpenShell sandbox. Parent directories are created. Paths are relative to the sandbox workspace.".to_owned(),
                input_schema: json!({
                    "type": "object",
                    "properties": {
                        "path": { "type": "string" },
                        "content": { "type": "string" }
                    },
                    "required": ["path", "content"],
                    "additionalProperties": false
                }),
                annotations: None,
                display_script: Some(r#""Write " + args.path"#.to_owned()),
            },
        ],
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use openshell_session::TenantId;
    use url::Url;

    #[test]
    fn manifest_names_are_unique_and_endpoint_comes_from_config() {
        let config = ServerConfig::new(
            Url::parse("https://tools.example.com/base/").unwrap(),
            TenantId::new("acme").unwrap(),
        );
        let manifest = manifest(&config);
        assert_eq!(manifest.endpoint, "https://tools.example.com/base/invoke");
        let names: std::collections::HashSet<_> = manifest
            .tools
            .iter()
            .map(|tool| tool.name.as_str())
            .collect();
        assert_eq!(names.len(), manifest.tools.len());
        assert!(manifest.tools.iter().all(|tool| {
            tool.input_schema.get("additionalProperties") == Some(&serde_json::Value::Bool(false))
        }));
    }
}
