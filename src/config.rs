use anyhow::{bail, Context, Result};
use figment::{
    providers::{Env, Format, Yaml},
    Figment,
};
use serde::{Deserialize, Deserializer, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::path::Path;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum NodeMode {
    Master,
    Node,
}

/// Config schema. `1` and `"v1"` are the same. A missing key is v1.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchemaVersion(pub u32);

impl Default for SchemaVersion {
    fn default() -> Self {
        SchemaVersion(1)
    }
}

impl Serialize for SchemaVersion {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u32(self.0)
    }
}

impl<'de> Deserialize<'de> for SchemaVersion {
    fn deserialize<D: Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        struct Visit;
        impl<'de> serde::de::Visitor<'de> for Visit {
            type Value = SchemaVersion;

            fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
                formatter.write_str("1 or \"v1\"")
            }

            fn visit_u64<E: serde::de::Error>(self, value: u64) -> Result<SchemaVersion, E> {
                Ok(SchemaVersion(value as u32))
            }

            fn visit_i64<E: serde::de::Error>(self, value: i64) -> Result<SchemaVersion, E> {
                if value < 0 {
                    return Err(E::custom("schema version is negative"));
                }
                Ok(SchemaVersion(value as u32))
            }

            fn visit_str<E: serde::de::Error>(self, value: &str) -> Result<SchemaVersion, E> {
                let number = value.trim().trim_start_matches(['v', 'V']);
                number
                    .parse::<u32>()
                    .map(SchemaVersion)
                    .map_err(|_| E::custom(format!("unknown schema {value}")))
            }
        }
        deserializer.deserialize_any(Visit)
    }
}

fn default_schema() -> SchemaVersion {
    SchemaVersion(1)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AppConfig {
    /// Schema of this file. This build reads v1. Omitted means v1.
    #[serde(default = "default_schema")]
    pub schema: SchemaVersion,
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
    /// Advertise this daemon on the local network and list neighbours.
    #[serde(default = "default_true")]
    pub discover: bool,
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

/// Sampling knobs sent with a chat request.
///
/// An unset field is left to the provider. OpenAI-compatible and Gemini calls
/// already use temperature 0.2 when this is empty. Ollama uses the model file
/// when `temperature` is unset, and that file is often 0.8.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default)]
#[serde(default)]
pub struct Sampling {
    pub temperature: Option<f32>,
    pub top_p: Option<f32>,
    pub repeat_penalty: Option<f32>,
    pub max_tokens: Option<u32>,
}

impl Sampling {
    pub fn overlay(self, over: Self) -> Self {
        Self {
            temperature: over.temperature.or(self.temperature),
            top_p: over.top_p.or(self.top_p),
            repeat_penalty: over.repeat_penalty.or(self.repeat_penalty),
            max_tokens: over.max_tokens.or(self.max_tokens),
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
    /// Inline key from an external providers file. Prefer `api_key_env`.
    /// Empty, or a value that starts with `PASTE_`, is ignored.
    #[serde(default)]
    pub api_key: String,
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
    #[serde(default)]
    pub max_tokens: u32,
    #[serde(default)]
    pub sampling: Sampling,
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
            api_key: String::new(),
            max_tokens: 0,
            sampling: Sampling::default(),
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
    /// Optional JSON file of extra providers. Same shape as
    /// `configs/providers.example.json`. Missing file adds nothing.
    #[serde(default)]
    pub providers_file: String,
    /// Applied to every provider. A provider's own `sampling` wins per field.
    #[serde(default)]
    pub sampling: Sampling,
    /// Max follow-up critique rounds when multi-model debate is enabled. 0 disables debates.
    #[serde(default)]
    pub debate_max_rounds: u32,
}

impl Default for ModelsSection {
    fn default() -> Self {
        Self {
            default: String::new(),
            embed_provider: String::new(),
            system_prompt: String::new(),
            providers: Vec::new(),
            providers_file: String::new(),
            sampling: Sampling::default(),
            debate_max_rounds: 0,
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

        let mut cfg: Self = figment
            .merge(Env::prefixed("XMESHD_").split("__"))
            .extract()
            .context("Failed to parse merged configuration")?;
        cfg.normalize()?;
        Ok(cfg)
    }

    fn normalize(&mut self) -> Result<()> {
        if self.schema != SchemaVersion(1) {
            bail!(
                "config schema is v{}, and this build reads v1",
                self.schema.0
            );
        }
        self.node.id = self.node.id.trim().to_string();
        if self.node.id.is_empty() {
            bail!("node.id is required and must not be empty");
        }
        self.merge_providers_file()?;
        Ok(())
    }

    fn merge_providers_file(&mut self) -> Result<()> {
        let path = self.models.providers_file.trim();
        if path.is_empty() {
            return Ok(());
        }
        if !Path::new(path).exists() {
            return Ok(());
        }
        let text = std::fs::read_to_string(path)
            .with_context(|| format!("reading providers file {path}"))?;
        for extra in providers_from_json(&text)? {
            if !self.models.providers.iter().any(|p| p.id == extra.id) {
                self.models.providers.push(extra);
            }
        }
        Ok(())
    }
}

/// Parse the external providers JSON used by other lab tools.
///
/// A provider with an empty key, or a key that still says `PASTE_`, is left
/// out unless `auth_optional` is true. The file can sit on disk before the
/// key exists.
pub fn providers_from_json(text: &str) -> Result<Vec<ProviderConfig>> {
    let raw: serde_json::Map<String, serde_json::Value> = serde_json::from_str(text)
        .context("providers file is not a JSON object")?;
    let mut out = Vec::new();
    for (id, value) in raw {
        let Some(obj) = value.as_object() else {
            continue;
        };
        let auth_optional = obj.get("auth_optional").and_then(|v| v.as_bool()).unwrap_or(false);
        let inline = usable_key(obj.get("api_key").and_then(|v| v.as_str()).unwrap_or(""));
        let api_key_env = obj
            .get("api_key_env")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .trim()
            .to_string();
        let env_set = !api_key_env.is_empty()
            && std::env::var(&api_key_env)
                .ok()
                .is_some_and(|v| !v.trim().is_empty());
        if !auth_optional && inline.is_empty() && !env_set {
            continue;
        }
        let models = obj
            .get("models")
            .and_then(|v| v.as_array())
            .map(|items| {
                items
                    .iter()
                    .filter_map(|v| v.as_str().map(|s| s.to_string()))
                    .filter(|s| !s.is_empty())
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        let kind = match obj
            .get("type")
            .or_else(|| obj.get("kind"))
            .and_then(|v| v.as_str())
            .unwrap_or("openai_chat")
        {
            "ollama" => "ollama",
            "gemini" => "gemini",
            "google" | "web" => "google",
            _ => "openai",
        };
        let label = obj
            .get("label")
            .or_else(|| obj.get("host"))
            .and_then(|v| v.as_str())
            .unwrap_or(&id)
            .to_string();
        let discover = obj
            .get("discover")
            .and_then(|v| v.as_bool())
            .unwrap_or(models.is_empty());
        let mut sampling = Sampling::default();
        if let Some(n) = obj.get("temperature").and_then(|v| v.as_f64()) {
            sampling.temperature = Some(n as f32);
        }
        if let Some(n) = obj.get("top_p").and_then(|v| v.as_f64()) {
            sampling.top_p = Some(n as f32);
        }
        if let Some(n) = obj.get("repeat_penalty").and_then(|v| v.as_f64()) {
            sampling.repeat_penalty = Some(n as f32);
        }
        let max_tokens = obj.get("max_tokens").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
        if max_tokens > 0 {
            sampling.max_tokens = Some(max_tokens);
        }
        out.push(ProviderConfig {
            id,
            host: label,
            kind: kind.into(),
            base_url: obj
                .get("base_url")
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .trim_end_matches('/')
                .to_string(),
            api_key_env,
            api_key: inline,
            chat_model: models.first().cloned().unwrap_or_default(),
            models,
            discover,
            coding_bias: true,
            timeout_secs: obj.get("timeout_secs").and_then(|v| v.as_u64()).unwrap_or(180),
            max_tokens,
            sampling,
            ..ProviderConfig::default()
        });
    }
    Ok(out)
}

fn usable_key(raw: &str) -> String {
    let key = raw.trim();
    if key.is_empty() || key.starts_with("PASTE_") {
        String::new()
    } else {
        key.to_string()
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
    fn providers_json_keeps_open_hosts_and_skips_placeholders() {
        let parsed = providers_from_json(
            r#"{
              "atria": {"type":"openai_chat","base_url":"https://api.example/v1","api_key":"PASTE_ATRIA_API_KEY_HERE","models":["Dawn"]},
              "open": {"label":"Open","type":"openai_chat","base_url":"https://open.example/v1","api_key":"","auth_optional":true,"models":["auto"],"max_tokens":1000,"temperature":0.2}
            }"#,
        )
        .unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].id, "open");
        assert_eq!(parsed[0].host, "Open");
        assert!(!parsed[0].discover);
        assert_eq!(parsed[0].sampling.temperature, Some(0.2));
        assert_eq!(parsed[0].sampling.max_tokens, Some(1000));
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
        assert_eq!(cfg.schema, SchemaVersion(1));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn schema_v1_is_accepted_and_empty_node_id_is_rejected() {
        let dir = std::env::temp_dir().join(format!("xmesh-schema-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let cluster = dir.join("cluster.yaml");
        let local = dir.join("local.yaml");
        std::fs::write(&local, "schema: v1\n").unwrap();
        std::fs::write(
            &cluster,
            r#"
schema: 1
node:
  id: "  node-a  "
  mode: node
  listen_addr: "127.0.0.1:8443"
  hmac_secret_path: "/tmp/hmac.key"
storage:
  primary_url: null
  fallback_sqlite: "/tmp/mesh.db"
git:
  bare_repos_root: null
"#,
        )
        .unwrap();
        let cfg = AppConfig::load(cluster.to_str().unwrap(), local.to_str().unwrap()).unwrap();
        assert_eq!(cfg.schema, SchemaVersion(1));
        assert_eq!(cfg.node.id, "node-a");
        std::fs::write(
            &cluster,
            r#"
node:
  id: "   "
  mode: node
  listen_addr: "127.0.0.1:8443"
  hmac_secret_path: "/tmp/hmac.key"
storage:
  primary_url: null
  fallback_sqlite: "/tmp/mesh.db"
git:
  bare_repos_root: null
"#,
        )
        .unwrap();
        let err = AppConfig::load(cluster.to_str().unwrap(), local.to_str().unwrap())
            .unwrap_err()
            .to_string();
        assert!(err.contains("node.id"), "{err}");
        std::fs::write(&local, "schema: 2\n").unwrap();
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
"#,
        )
        .unwrap();
        let err = AppConfig::load(cluster.to_str().unwrap(), local.to_str().unwrap())
            .unwrap_err()
            .to_string();
        assert!(err.contains("schema"), "{err}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
