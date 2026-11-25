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
    pub deploy_pipelines: HashMap<String, DeployPipeline>,
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
            chat_model: "gemini-3.8-flash".to_string(),
            embed_model: "gemini-embedding-001".to_string(),
            api_key_env: "GEMINI_API_KEY".to_string(),
            available_models: vec![
                "gemini-3.8-flash".to_string(),
                "gemini-4-hydrogen".to_string(),
                "gemini-2.5-pro".to_string(),
            ],
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeSection {
    pub id: String,
    pub mode: NodeMode,
    pub listen_addr: String,
    pub master_url: Option<String>,
    pub hmac_secret_path: String,
    #[serde(default)]
    pub allowed_workspaces: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageSection {
    pub primary_url: Option<String>, // postgres://... or mysql://...
    pub fallback_sqlite: String,     // /var/lib/xmeshd/local.db
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GitSection {
    pub bare_repos_root: Option<String>, // Master: /srv/git
    #[serde(default)]
    pub watched_repos: Vec<WatchedRepo>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WatchedRepo {
    pub namespace: String,
    pub local_path: String,
    pub base_branch: String, // local diff vs origin/<base_branch>
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeployPipeline {
    pub workdir: String,
    pub command: Vec<String>,
    pub use_bwrap: bool,
    pub allow_network: bool,
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

