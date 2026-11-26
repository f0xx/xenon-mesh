use anyhow::{Context, Result};
use figment::{
    providers::{Env, Format, Yaml},
    Figment,
};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeMode {
    Master,
    Node,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    pub node: NodeSection,
    pub storage: StorageSection,
    pub git: GitSection,
    #[serde(default)]
    pub gemini: GeminiSection,
    #[serde(default)]
    pub models: ModelsSection,
    #[serde(default)]
    pub deploy_pipelines: HashMap<String, DeployPipeline>,
}

fn default_http_addr() -> String {
    "0.0.0.0:8080".into()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeSection {
    pub id: String,
    pub mode: NodeMode,
    pub listen_addr: String,
    #[serde(default = "default_http_addr")]
    pub http_addr: String,
    /// Optional second listener for the same UI over TLS.
    /// Self-signed certificates are enough for a browser secure context
    /// after the operator allows the certificate.
    #[serde(default)]
    pub https_addr: Option<String>,
    #[serde(default)]
    pub tls_cert_path: Option<String>,
    #[serde(default)]
    pub tls_key_path: Option<String>,
    pub master_url: Option<String>,
    /// Hub HTTP UI, used by a node to list models and to run a completion.
    /// Inference is a separate request per chat, not a process-wide queue.
    #[serde(default)]
    pub master_http: Option<String>,
    pub hmac_secret_path: String,
    /// This node's own identity key. The hub checks it on mutating calls.
    #[serde(default)]
    pub node_key_path: Option<String>,
    /// Hub only. Directory of `<node-id>.key` files, one per peer.
    #[serde(default)]
    pub peer_keys_dir: Option<String>,
    #[serde(default)]
    pub allowed_workspaces: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageSection {
    pub primary_url: Option<String>,
    pub fallback_sqlite: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GitSection {
    pub bare_repos_root: Option<String>,
    #[serde(default)]
    pub watched_repos: Vec<WatchedRepo>,
    /// Working trees to show as channels and to read during chat.
    /// Each immediate child directory is one project.
    #[serde(default)]
    pub project_roots: Vec<ProjectRoot>,
    /// inotify watch of project roots and bare repos. On by default.
    /// Turn it off when a tree is too noisy to rescan on every save.
    #[serde(default = "default_true")]
    pub watch_changes: bool,
    /// Optional argv. The unified diff is written to its stdin.
    /// Findings are appended to the built-in diff inspection.
    #[serde(default)]
    pub inspect_command: Vec<String>,
    /// Optional argv. The project path is the last argument. Stdout replaces
    /// the built-in project outline in chat context.
    #[serde(default)]
    pub harvest_command: Vec<String>,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProjectRoot {
    pub path: String,
    /// `public` publishes the chat to the hub. `private` keeps it on this node.
    #[serde(default = "default_project_visibility")]
    pub visibility: String,
}

fn default_project_visibility() -> String {
    "private".into()
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatchedRepo {
    pub namespace: String,
    pub local_path: String,
    pub base_branch: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeployPipeline {
    pub workdir: String,
    pub command: Vec<String>,
    pub use_bwrap: bool,
    pub allow_network: bool,
    /// Declared destination ref when this pipeline pushes. Recorded in the hub audit.
    #[serde(default)]
    pub git_ref: String,
    /// The operator marked this pipeline as rewriting remote history.
    #[serde(default)]
    pub git_force: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct GeminiSection {
    pub chat_model: String,
    pub embed_model: String,
    pub api_key_env: String,
    pub available_models: Vec<String>,
}

impl Default for GeminiSection {
    fn default() -> Self {
        Self {
            chat_model: "gemini-2.5-flash".into(),
            embed_model: "gemini-embedding-001".into(),
            api_key_env: "GEMINI_API_KEY".into(),
            available_models: vec!["gemini-2.5-flash".into(), "gemini-2.5-pro".into()],
        }
    }
}

/// One remote model endpoint.
///
/// `kind` is `openai` (OpenAI-compatible HTTP, including Ollama `/v1`),
/// `ollama` (native `/api/chat`), `gemini`, or `auto` (probe an OpenAPI
/// document, then well-known routes).
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ProviderConfig {
    pub id: String,
    /// Shown after the model name when two hosts serve the same name.
    /// Empty uses `id`.
    #[serde(default)]
    pub host: String,
    pub kind: String,
    pub base_url: String,
    pub api_key_env: String,
    pub chat_model: String,
    pub embed_model: String,
    pub models: Vec<String>,
    pub discover: bool,
    pub coding_bias: bool,
    pub timeout_secs: u64,
    pub openapi_url: String,
    /// Argv of the web-search script. The query is written to its stdin.
    #[serde(default)]
    pub command: Vec<String>,
    /// Browser identity for the web-search script. Empty uses the script default.
    #[serde(default)]
    pub user_agent: String,
}

impl Default for ProviderConfig {
    fn default() -> Self {
        Self {
            id: String::new(),
            host: String::new(),
            kind: "openai".into(),
            base_url: String::new(),
            api_key_env: String::new(),
            chat_model: String::new(),
            embed_model: String::new(),
            models: Vec::new(),
            discover: true,
            coding_bias: true,
            timeout_secs: 300,
            openapi_url: String::new(),
            command: Vec::new(),
            user_agent: String::new(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct ModelsSection {
    /// `provider/model`. Empty means the coding-biased default after discovery.
    pub default: String,
    pub embed_provider: String,
    pub system_prompt: String,
    pub providers: Vec<ProviderConfig>,
}

impl Default for ModelsSection {
    fn default() -> Self {
        Self {
            default: String::new(),
            embed_provider: String::new(),
            system_prompt: String::new(),
            providers: Vec::new(),
        }
    }
}

impl AppConfig {
    pub fn load(cluster_path: &str, local_path: &str) -> Result<Self> {
        let mut figment = Figment::new();

        if Path::new(cluster_path).exists() {
            figment = figment.merge(Yaml::file(cluster_path));
        }
        if Path::new(local_path).exists() {
            figment = figment.merge(Yaml::file(local_path));
        }

        figment
            .merge(Env::prefixed("XMESHD_").split("__"))
            .extract()
            .context("Failed to parse merged configuration")
    }
}

/// Hide userinfo passwords in database URLs before they reach logs or stdout.
pub fn redact_url(url: &str) -> String {
    let Some(scheme_at) = url.find("://") else {
        return url.to_string();
    };
    let rest = &url[scheme_at + 3..];
    let Some(at) = rest.find('@') else {
        return url.to_string();
    };
    let userinfo = &rest[..at];
    let host = &rest[at..];
    let user = userinfo.split_once(':').map(|(u, _)| u).unwrap_or(userinfo);
    format!("{}://{user}:***{host}", &url[..scheme_at])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn redacts_only_the_password() {
        assert_eq!(
            redact_url("postgres://app:s3cret@127.0.0.1:5432/mesh"),
            "postgres://app:***@127.0.0.1:5432/mesh"
        );
        assert_eq!(redact_url("sqlite:///tmp/mesh.db"), "sqlite:///tmp/mesh.db");
    }

    #[test]
    fn loads_provider_overlay_and_keeps_legacy_gemini() {
        let dir = std::env::temp_dir().join(format!("xmesh-cfg-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let cluster = dir.join("cluster.yaml");
        let local = dir.join("local.yaml");
        std::fs::write(
            &cluster,
            r#"
node:
  id: node-a
  mode: node
  listen_addr: "127.0.0.1:8443"
  hmac_secret_path: "/tmp/hmac.key"
storage:
  primary_url: null
  fallback_sqlite: "/tmp/mesh.db"
git:
  bare_repos_root: null
gemini:
  chat_model: "gemini-2.5-pro"
  api_key_env: "GEMINI_API_KEY"
"#,
        )
        .unwrap();
        std::fs::write(
            &local,
            r#"
models:
  default: "local/coder"
  providers:
    - id: local
      kind: auto
      base_url: "http://127.0.0.1:11434/v1"
      discover: true
      coding_bias: true
"#,
        )
        .unwrap();

        let cfg = AppConfig::load(cluster.to_str().unwrap(), local.to_str().unwrap()).unwrap();
        assert!(matches!(cfg.node.mode, NodeMode::Node));
        assert_eq!(cfg.node.http_addr, "0.0.0.0:8080");
        assert_eq!(cfg.models.default, "local/coder");
        assert_eq!(cfg.models.providers.len(), 1);
        assert_eq!(cfg.models.providers[0].kind, "auto");
        assert_eq!(cfg.models.providers[0].timeout_secs, 300);
        assert_eq!(cfg.gemini.chat_model, "gemini-2.5-pro");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
