//! Model providers.
//!
//! `openai` speaks the OpenAI chat-completions and embeddings HTTP API.
//! That is the portable path for Ollama `/v1`, vLLM, llama.cpp, and any
//! server that publishes the same routes in an OpenAPI document.
//! `ollama` speaks the native `/api/chat` and `/api/tags` routes.
//! `gemini` keeps the original Generative Language API.
//! `auto` probes an OpenAPI document, then those well-known routes.

mod gemini;
mod google;
mod remote;

use crate::config::{AppConfig, GeminiSection, ProviderConfig, Sampling};
use anyhow::{bail, Context, Result};
use reqwest::Client;
use serde::Serialize;
use std::cmp::Reverse;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, RwLock};
use tracing::warn;

pub use remote::detect_kind;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ConcreteKind {
    Gemini,
    OpenAi,
    Ollama,
    Google,
}

impl ConcreteKind {
    pub fn as_str(self) -> &'static str {
        match self {
            Self::Gemini => "gemini",
            Self::OpenAi => "openai",
            Self::Ollama => "ollama",
            Self::Google => "google",
        }
    }
}

#[derive(Serialize, Deserialize, Clone)]
pub struct Attachment {
    pub name: String,
    pub mime_type: String,
    pub data_base64: String,
}

#[derive(Serialize, Deserialize, Clone)]
pub struct ChatTurn {
    pub role: String,
    pub text: String,
    #[serde(default)]
    pub attachments: Vec<Attachment>,
}

use serde::Deserialize;

#[derive(Serialize, Deserialize, Clone)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum StreamEvent {
    Status {
        text: String,
        #[serde(default)]
        model: String,
    },
    Thought {
        text: String,
        #[serde(default)]
        model: String,
    },
    Delta {
        text: String,
        #[serde(default)]
        model: String,
    },
    Done {
        model_used: String,
        #[serde(default)]
        model: String,
    },
    Saved {
        seq: u64,
        /// Set when the reply was stored in a thread of its own.
        #[serde(default)]
        thread: String,
        #[serde(default)]
        model: String,
        /// True for the last reply when several models were asked together.
        #[serde(default = "default_true")]
        last: bool,
    },
    /// The page could not be fetched by the daemon. The UI opens it.
    Browse { url: String, text: String },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ModelChoice {
    pub id: String,
    pub provider: String,
    pub host: String,
    pub model: String,
    pub label: String,
    pub kind: String,
    pub coding_rank: i32,
    /// The hub holds the API key and performs the call. Nodes select the model
    /// and send the completion to the hub.
    #[serde(default)]
    pub proxied: bool,
}

#[derive(Clone, Debug, Serialize)]
pub struct ProbeReport {
    pub kind: String,
    pub suggested_default: String,
    pub models: Vec<ProbeModel>,
}

#[derive(Clone, Debug, Serialize)]
pub struct ProbeModel {
    pub name: String,
    pub coding_rank: i32,
}

#[derive(Clone)]
struct ProviderState {
    id: String,
    host: String,
    kind: ConcreteKind,
    configured_kind: String,
    base_url: String,
    api_key_env: String,
    chat_model: String,
    embed_model: String,
    models: Vec<String>,
    coding_bias: bool,
    timeout: Duration,
    openapi_url: String,
    discover: bool,
    command: Vec<String>,
    user_agent: String,
    api_key: String,
    sampling: Sampling,
    proxied: bool,
}

fn default_true() -> bool {
    true
}

struct ModelState {
    providers: Vec<ProviderState>,
    default_ref: String,
    embed_provider: String,
    system_prompt: String,
}

pub struct ModelHub {
    http: Client,
    embed_blocked: AtomicBool,
    inner: RwLock<ModelState>,
}

pub fn coding_rank(name: &str) -> i32 {
    let n = name.to_ascii_lowercase();
    let mut score = 0;
    if n.contains("coder") {
        score += 100;
    } else if n.contains("code") {
        score += 50;
    }
    if n.contains("instruct") {
        score += 10;
    }
    if n.contains("reason") || n.contains("think") || n.contains("-r1") {
        score += 5;
    }
    score
}

pub fn pick_coding_model(names: &[String]) -> Option<String> {
    names
        .iter()
        .max_by_key(|n| (coding_rank(n), Reverse(n.len())))
        .cloned()
}

/// A stored author is a person when it is not a model id.
/// Model ids look like `provider/model`. Older rows used `user`.
pub fn is_human_role(role: &str) -> bool {
    let role = role.trim();
    !role.is_empty()
        && role != "system"
        && role != "assistant"
        && role != "model"
        && role != "bot"
        && role != "error"
        && role != "deploy"
        && !role.contains('/')
}

pub fn map_role(kind: ConcreteKind, role: &str) -> &'static str {
    let user = is_human_role(role);
    match kind {
        ConcreteKind::Gemini => {
            if user {
                "user"
            } else {
                "model"
            }
        }
        _ => {
            if user {
                "user"
            } else {
                "assistant"
            }
        }
    }
}

pub fn should_fallback(err: &str) -> bool {
    let e = err.to_ascii_lowercase();
    if e.contains("status=401") || e.contains("status=403") || e.contains("api key") {
        return false;
    }
    if e.contains("error sending request")
        || e.contains("connection refused")
        || e.contains("tcp connect")
    {
        return false;
    }
    e.contains("status=404")
        || e.contains("status=408")
        || e.contains("status=429")
        || e.contains("status=5")
        || e.contains("not found")
        || e.contains("overloaded")
        || e.contains("unavailable")
        || e.contains("high demand")
        || e.contains("timed out")
        || e.contains("timeout")
}

fn parse_kind(kind: &str) -> Option<ConcreteKind> {
    match kind.trim().to_ascii_lowercase().as_str() {
        "gemini" => Some(ConcreteKind::Gemini),
        "openai" | "openapi" | "openai-compat" => Some(ConcreteKind::OpenAi),
        "ollama" => Some(ConcreteKind::Ollama),
        "google" | "web" => Some(ConcreteKind::Google),
        _ => None,
    }
}

fn legacy_gemini(g: &GeminiSection) -> ProviderConfig {
    ProviderConfig {
        id: "gemini".into(),
        kind: "gemini".into(),
        api_key_env: g.api_key_env.clone(),
        chat_model: g.chat_model.clone(),
        embed_model: g.embed_model.clone(),
        models: g.available_models.clone(),
        discover: false,
        coding_bias: false,
        timeout_secs: 120,
        ..ProviderConfig::default()
    }
}

pub fn providers_from_config(cfg: &AppConfig) -> Vec<ProviderConfig> {
    let mut list = if !cfg.models.providers.is_empty() {
        cfg.models.providers.clone()
    } else {
        let routed = cfg
            .node
            .master_http
            .as_deref()
            .is_some_and(|url| !url.trim().is_empty());
        if routed {
            Vec::new()
        } else {
            vec![legacy_gemini(&cfg.gemini)]
        }
    };
    let base = cfg.models.sampling;
    for provider in &mut list {
        provider.sampling = base.overlay(provider.sampling);
    }
    list
}

impl ProviderState {
    fn from_config(cfg: &ProviderConfig) -> Self {
        let configured = cfg.kind.clone();
        let kind = parse_kind(&configured).unwrap_or(ConcreteKind::OpenAi);
        let host = if cfg.host.trim().is_empty() {
            cfg.id.clone()
        } else {
            cfg.host.clone()
        };
        Self {
            id: cfg.id.clone(),
            host,
            kind,
            configured_kind: configured,
            base_url: cfg.base_url.clone(),
            api_key_env: cfg.api_key_env.clone(),
            chat_model: cfg.chat_model.clone(),
            embed_model: cfg.embed_model.clone(),
            models: cfg.models.clone(),
            coding_bias: cfg.coding_bias,
            timeout: Duration::from_secs(cfg.timeout_secs.max(1)),
            openapi_url: cfg.openapi_url.clone(),
            discover: cfg.discover,
            command: cfg.command.clone(),
            user_agent: cfg.user_agent.clone(),
            api_key: {
                let key = cfg.api_key.trim();
                if key.is_empty() || key.starts_with("PASTE_") {
                    String::new()
                } else {
                    key.to_string()
                }
            },
            sampling: cfg.sampling,
            proxied: cfg.kind == "gemini" || cfg.base_url.starts_with("https://"),
        }
    }

    fn host_label(&self) -> &str {
        &self.host
    }

    fn serves(&self, model: &str) -> bool {
        self.chat_model == model || self.models.iter().any(|m| m == model)
    }

    fn key(&self) -> String {
        if !self.api_key_env.trim().is_empty() {
            if let Ok(value) = std::env::var(&self.api_key_env) {
                if !value.trim().is_empty() {
                    return value;
                }
            }
        }
        self.api_key.clone()
    }
}

fn split_ref<'a>(providers: &[ProviderState], requested: &'a str) -> (&'a str, &'a str) {
    if let Some((prov, model)) = requested.split_once('/') {
        if !model.is_empty() && providers.iter().any(|p| p.id == prov) {
            return (prov, model);
        }
    }
    ("", requested)
}

fn candidates(provider: &ProviderState, model: &str) -> Vec<String> {
    let mut out = vec![model.to_string()];
    let mut rest: Vec<String> = provider
        .models
        .iter()
        .filter(|m| m.as_str() != model)
        .cloned()
        .collect();
    if provider.coding_bias {
        rest.sort_by_key(|m| Reverse(coding_rank(m)));
    }
    for m in rest.into_iter().take(2) {
        if !out.contains(&m) {
            out.push(m);
        }
    }
    out
}

impl ModelHub {
    pub fn from_config(cfg: &AppConfig) -> Result<Arc<Self>> {
        let providers: Vec<ProviderState> = providers_from_config(cfg)
            .iter()
            .map(ProviderState::from_config)
            .collect();
        let default_ref = if !cfg.models.default.trim().is_empty() {
            cfg.models.default.clone()
        } else if let Some(p) = providers.iter().find(|p| !p.chat_model.is_empty()) {
            format!("{}/{}", p.id, p.chat_model)
        } else {
            String::new()
        };
        let http = Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .build()
            .unwrap_or_else(|_| Client::new());
        Ok(Arc::new(Self {
            http,
            embed_blocked: AtomicBool::new(false),
            inner: RwLock::new(ModelState {
                providers,
                default_ref,
                embed_provider: cfg.models.embed_provider.clone(),
                system_prompt: cfg.models.system_prompt.clone(),
            }),
        }))
    }

    pub async fn default_model(&self) -> String {
        self.inner.read().await.default_ref.clone()
    }

    pub async fn embeddings_configured(&self) -> bool {
        let g = self.inner.read().await;
        !self.embed_blocked.load(Ordering::Relaxed)
            && g.providers.iter().any(|p| !p.embed_model.is_empty())
    }

    pub async fn model_choices(&self) -> Vec<ModelChoice> {
        let g = self.inner.read().await;
        let mut out = Vec::new();
        for p in &g.providers {
            let mut names = p.models.clone();
            if !p.chat_model.is_empty() && !names.contains(&p.chat_model) {
                names.insert(0, p.chat_model.clone());
            }
            for model in names {
                let host = p.host_label().to_string();
                out.push(ModelChoice {
                    id: format!("{}/{}", p.id, model),
                    provider: p.id.clone(),
                    label: format!("{model} : {host}"),
                    host,
                    kind: p.kind.as_str().into(),
                    coding_rank: coding_rank(&model),
                    proxied: p.proxied,
                    model,
                });
            }
        }
        out.sort_by(|a, b| {
            b.coding_rank
                .cmp(&a.coding_rank)
                .then_with(|| a.model.cmp(&b.model))
                .then_with(|| a.host.cmp(&b.host))
        });
        out
    }

    pub async fn has_local_providers(&self) -> bool {
        !self.inner.read().await.providers.is_empty()
    }

    pub async fn owns_provider(&self, id: &str) -> bool {
        !id.is_empty() && self.inner.read().await.providers.iter().any(|p| p.id == id)
    }

    pub async fn discover(&self) -> Result<()> {
        let snapshot = self.inner.read().await.providers.clone();
        let mut updated = Vec::with_capacity(snapshot.len());
        for mut p in snapshot {
            let unresolved = parse_kind(&p.configured_kind).is_none();
            if unresolved || p.discover {
                match detect_kind(&self.http, &p.base_url, &p.openapi_url).await {
                    Ok(kind) => p.kind = kind,
                    Err(err) => {
                        if unresolved {
                            warn!("provider {} kind probe failed: {err}", p.id);
                        }
                    }
                }
            }
            if p.discover && p.kind != ConcreteKind::Gemini && p.kind != ConcreteKind::Google {
                let listed = match p.kind {
                    ConcreteKind::OpenAi => {
                        remote::openai_list_models(&self.http, &p.base_url, &p.key()).await
                    }
                    ConcreteKind::Ollama => {
                        remote::ollama_list_models(&self.http, &p.base_url).await
                    }
                    ConcreteKind::Gemini | ConcreteKind::Google => Ok(Vec::new()),
                };
                match listed {
                    Ok(ids) if !ids.is_empty() => {
                        let mut models = Vec::new();
                        if !p.chat_model.is_empty() {
                            models.push(p.chat_model.clone());
                        }
                        for id in ids {
                            if !models.contains(&id) {
                                models.push(id);
                            }
                        }
                        p.models = models;
                    }
                    Ok(_) => {}
                    Err(err) => warn!("provider {} model discovery failed: {err}", p.id),
                }
            }
            if p.chat_model.is_empty() && p.coding_bias {
                if let Some(best) = pick_coding_model(&p.models) {
                    p.chat_model = best;
                }
            }
            updated.push(p);
        }

        let mut g = self.inner.write().await;
        g.providers = updated;
        if g.default_ref.trim().is_empty() {
            if let Some(p) = g.providers.iter().find(|p| !p.chat_model.is_empty()) {
                g.default_ref = format!("{}/{}", p.id, p.chat_model);
            }
        }
        Ok(())
    }

    fn resolve(state: &ModelState, requested: &str) -> Result<(ProviderState, String)> {
        let requested = if requested.trim().is_empty() {
            state.default_ref.as_str()
        } else {
            requested
        };
        if requested.is_empty() {
            bail!("no model configured");
        }
        let (prov_id, model_name) = split_ref(&state.providers, requested);
        let provider = if prov_id.is_empty() {
            let hits: Vec<_> = state
                .providers
                .iter()
                .filter(|p| p.serves(model_name))
                .collect();
            match hits.len() {
                1 => hits[0].clone(),
                n if n > 1 => {
                    let hosts: Vec<_> = hits.iter().map(|p| p.host_label()).collect();
                    bail!(
                        "model '{model_name}' is served by more than one host ({}); choose host/model",
                        hosts.join(", ")
                    );
                }
                _ if state.providers.len() == 1 => state.providers[0].clone(),
                _ => bail!("no model providers configured"),
            }
        } else {
            state
                .providers
                .iter()
                .find(|p| p.id == prov_id)
                .context("unknown model provider")?
                .clone()
        };
        let model = if model_name.is_empty() {
            provider.chat_model.clone()
        } else {
            model_name.to_string()
        };
        if model.is_empty() {
            bail!("provider {} has no chat model", provider.id);
        }
        Ok((provider, model))
    }

    pub async fn embed_text(&self, text: &str) -> Result<Vec<f32>> {
        if text.trim().is_empty() || self.embed_blocked.load(Ordering::Relaxed) {
            return Ok(Vec::new());
        }
        let g = self.inner.read().await;
        let Some((provider, model)) = embed_target(&g) else {
            return Ok(Vec::new());
        };
        drop(g);
        let result = match provider.kind {
            ConcreteKind::Gemini => {
                gemini::embed(&self.http, &provider.key(), &model, text, provider.timeout).await
            }
            ConcreteKind::OpenAi => {
                remote::openai_embed(
                    &self.http,
                    &provider.base_url,
                    &provider.key(),
                    &model,
                    text,
                    provider.timeout,
                )
                .await
            }
            ConcreteKind::Ollama => {
                remote::ollama_embed(
                    &self.http,
                    &provider.base_url,
                    &model,
                    text,
                    provider.timeout,
                )
                .await
            }
            ConcreteKind::Google => Ok(Vec::new()),
        };
        match result {
            Ok(v) => Ok(v),
            Err(err) => {
                let msg = err.to_string().to_ascii_lowercase();
                if msg.contains("embedding") {
                    if !self.embed_blocked.swap(true, Ordering::Relaxed) {
                        warn!("embeddings unavailable, using lexical retrieval ({err})");
                    }
                    Ok(Vec::new())
                } else {
                    Err(err)
                }
            }
        }
    }

    pub async fn generate(
        &self,
        model_ref: &str,
        system_ctx: &str,
        turns: &[ChatTurn],
    ) -> Result<String> {
        self.complete(model_ref, system_ctx, turns, None)
            .await
            .map(|(text, _)| text)
    }

    pub async fn stream_chat(
        &self,
        model_ref: &str,
        system_ctx: &str,
        turns: &[ChatTurn],
        tx: mpsc::Sender<StreamEvent>,
    ) -> Result<(String, String)> {
        self.complete(model_ref, system_ctx, turns, Some(tx)).await
    }

    pub async fn distill(&self, text: &str) -> Result<String> {
        let sys = "Extract strict technical invariants and code conventions as a concise list of at most 10 lines.";
        let turns = [ChatTurn {
            role: "user".into(),
            text: text.into(),
            attachments: vec![],
        }];
        self.generate("", sys, &turns).await
    }

    async fn complete(
        &self,
        model_ref: &str,
        system_ctx: &str,
        turns: &[ChatTurn],
        tx: Option<mpsc::Sender<StreamEvent>>,
    ) -> Result<(String, String)> {
        // Snapshot provider state and drop the lock before any HTTP await.
        // Concurrent chats must not queue behind each other here.
        let state = self.inner.read().await;
        let (provider, model) = Self::resolve(&state, model_ref)?;
        let extra = state.system_prompt.clone();
        drop(state);
        let system = if extra.trim().is_empty() {
            system_ctx.to_string()
        } else {
            format!("{extra}\n\n{system_ctx}")
        };
        let mut last_err = String::new();
        let list = candidates(&provider, &model);
        for (idx, m) in list.iter().enumerate() {
            if let Some(tx) = &tx {
                let _ = tx
                    .send(StreamEvent::Status {
                        text: format!("Awaiting {m} : {}...", provider.host_label()),
                        model: String::new(),
                    })
                    .await;
            }
            let result = self.one(&provider, m, &system, turns, tx.as_ref()).await;
            match result {
                Ok(text) => {
                    if let Some(tx) = &tx {
                        let _ = tx
                            .send(StreamEvent::Done {
                                model_used: format!("{}/{}", provider.id, m),
                                model: String::new(),
                            })
                            .await;
                    }
                    return Ok((text, format!("{}/{}", provider.id, m)));
                }
                Err(err) => {
                    last_err = err.to_string();
                    let more = idx + 1 < list.len() && should_fallback(&last_err);
                    if let Some(tx) = &tx {
                        if more {
                            let brief: String = last_err.chars().take(80).collect();
                            let _ = tx
                                .send(StreamEvent::Status {
                                    text: format!("{m} failed ({brief}), trying another model"),
                                    model: String::new(),
                                })
                                .await;
                        }
                    }
                    if !more {
                        break;
                    }
                }
            }
        }
        bail!(last_err)
    }

    async fn one(
        &self,
        provider: &ProviderState,
        model: &str,
        system: &str,
        turns: &[ChatTurn],
        tx: Option<&mpsc::Sender<StreamEvent>>,
    ) -> Result<String> {
        match (provider.kind, tx) {
            (ConcreteKind::Google, tx) => {
                google::ask(
                    &provider.command,
                    &provider.user_agent,
                    &provider.base_url,
                    turns,
                    provider.timeout,
                    tx,
                )
                .await
            }
            (ConcreteKind::Gemini, Some(tx)) => {
                gemini::stream(
                    &self.http,
                    &provider.key(),
                    model,
                    system,
                    turns,
                    provider.timeout,
                    provider.sampling,
                    tx,
                )
                .await
            }
            (ConcreteKind::Gemini, None) => {
                gemini::generate(
                    &self.http,
                    &provider.key(),
                    model,
                    system,
                    turns,
                    provider.timeout,
                    provider.sampling,
                )
                .await
            }
            (ConcreteKind::OpenAi, Some(tx)) => {
                remote::openai_stream(
                    &self.http,
                    &provider.base_url,
                    &provider.key(),
                    model,
                    system,
                    turns,
                    provider.timeout,
                    provider.sampling,
                    tx,
                )
                .await
            }
            (ConcreteKind::OpenAi, None) => {
                remote::openai_generate(
                    &self.http,
                    &provider.base_url,
                    &provider.key(),
                    model,
                    system,
                    turns,
                    provider.timeout,
                    provider.sampling,
                )
                .await
            }
            (ConcreteKind::Ollama, Some(tx)) => {
                remote::ollama_stream(
                    &self.http,
                    &provider.base_url,
                    model,
                    system,
                    turns,
                    provider.timeout,
                    provider.sampling,
                    tx,
                )
                .await
            }
            (ConcreteKind::Ollama, None) => {
                remote::ollama_generate(
                    &self.http,
                    &provider.base_url,
                    model,
                    system,
                    turns,
                    provider.timeout,
                    provider.sampling,
                )
                .await
            }
        }
    }

    pub async fn probe(base_url: &str) -> Result<ProbeReport> {
        let http = Client::builder()
            .connect_timeout(Duration::from_secs(10))
            .build()?;
        let kind = detect_kind(&http, base_url, "").await?;
        let mut models = match kind {
            ConcreteKind::OpenAi => remote::openai_list_models(&http, base_url, "").await?,
            ConcreteKind::Ollama => remote::ollama_list_models(&http, base_url).await?,
            ConcreteKind::Gemini | ConcreteKind::Google => Vec::new(),
        };
        models.sort_by_key(|m| Reverse(coding_rank(m)));
        let suggested_default = models.first().cloned().unwrap_or_default();
        let models = models
            .into_iter()
            .map(|name| ProbeModel {
                coding_rank: coding_rank(&name),
                name,
            })
            .collect();
        Ok(ProbeReport {
            kind: kind.as_str().into(),
            suggested_default,
            models,
        })
    }
}

fn embed_target(state: &ModelState) -> Option<(ProviderState, String)> {
    let provider = if !state.embed_provider.is_empty() {
        state
            .providers
            .iter()
            .find(|p| p.id == state.embed_provider)?
    } else {
        state.providers.iter().find(|p| !p.embed_model.is_empty())?
    };
    if provider.embed_model.is_empty() {
        None
    } else {
        Some((provider.clone(), provider.embed_model.clone()))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn coding_rank_prefers_coder_models() {
        assert!(coding_rank("qwen2.5-coder:14b") > coding_rank("qwen2.5:14b-instruct"));
        assert!(coding_rank("qwen2.5:14b-instruct") > coding_rank("notes:latest"));
        assert_eq!(
            pick_coding_model(&[
                "notes:latest".into(),
                "qwen2.5-coder:14b".into(),
                "qwen2.5:14b-instruct".into()
            ])
            .as_deref(),
            Some("qwen2.5-coder:14b")
        );
    }

    #[test]
    fn fallback_policy_skips_auth_and_connection_loss() {
        assert!(!should_fallback("status=401 invalid key"));
        assert!(!should_fallback("gemini API key is empty"));
        assert!(!should_fallback(
            "error sending request: connection refused"
        ));
        assert!(should_fallback("status=503 overloaded"));
        assert!(should_fallback("status=404 model not found"));
    }

    #[test]
    fn roles_follow_the_provider() {
        assert_eq!(map_role(ConcreteKind::Gemini, "assistant"), "model");
        assert_eq!(map_role(ConcreteKind::OpenAi, "model"), "assistant");
        assert_eq!(map_role(ConcreteKind::Ollama, "user"), "user");
        assert_eq!(map_role(ConcreteKind::OpenAi, "node-a"), "user");
        assert_eq!(map_role(ConcreteKind::Ollama, "local/coder"), "assistant");
        assert!(is_human_role("node-a"));
        assert!(!is_human_role("local/coder"));
    }

    #[tokio::test]
    async fn legacy_gemini_section_is_the_only_provider_when_unset() {
        let dir = std::env::temp_dir().join(format!("xmesh-models-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("c.yaml");
        std::fs::write(
            &path,
            r#"
node:
  id: hub
  mode: master
  listen_addr: "127.0.0.1:1"
  hmac_secret_path: "/tmp/hmac"
storage:
  primary_url: null
  fallback_sqlite: "/tmp/m.db"
git:
  bare_repos_root: null
gemini:
  chat_model: "gemini-2.5-flash"
  available_models: ["gemini-2.5-flash", "gemini-2.5-pro"]
"#,
        )
        .unwrap();
        let cfg = AppConfig::load(path.to_str().unwrap(), "/no/such/local.yaml").unwrap();
        let hub = ModelHub::from_config(&cfg).unwrap();
        assert_eq!(hub.default_model().await, "gemini/gemini-2.5-flash");
        let choices = hub.model_choices().await;
        assert_eq!(choices.len(), 2);
        assert!(choices.iter().all(|c| c.kind == "gemini"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    fn write_cfg(body: &str) -> (std::path::PathBuf, AppConfig) {
        let dir = std::env::temp_dir().join(format!(
            "xmesh-models-{}-{}",
            std::process::id(),
            now_token()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("c.yaml");
        std::fs::write(&path, body).unwrap();
        let cfg = AppConfig::load(path.to_str().unwrap(), "/no/such/local.yaml").unwrap();
        (dir, cfg)
    }

    fn now_token() -> u128 {
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    }

    #[tokio::test]
    async fn same_model_name_on_two_hosts_stays_two_choices() {
        let (dir, cfg) = write_cfg(
            r#"
node:
  id: hub
  mode: master
  listen_addr: "127.0.0.1:1"
  hmac_secret_path: "/tmp/hmac"
storage:
  primary_url: null
  fallback_sqlite: "/tmp/m.db"
git:
  bare_repos_root: null
models:
  providers:
    - id: gpu-a
      host: gpu-a
      kind: openai
      base_url: "http://127.0.0.1:9/v1"
      chat_model: coder
      models: ["coder"]
      discover: false
    - id: gpu-b
      host: gpu-b
      kind: openai
      base_url: "http://127.0.0.1:9/v1"
      chat_model: coder
      models: ["coder"]
      discover: false
"#,
        );
        let hub = ModelHub::from_config(&cfg).unwrap();
        let choices = hub.model_choices().await;
        assert_eq!(choices.len(), 2);
        assert_eq!(choices[0].label, "coder : gpu-a");
        assert_eq!(choices[1].label, "coder : gpu-b");
        assert_ne!(choices[0].id, choices[1].id);
        let err = hub
            .generate("coder", "", &[])
            .await
            .unwrap_err()
            .to_string();
        assert!(err.contains("gpu-a") && err.contains("gpu-b"));
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn two_hosts_are_awaited_together() {
        use axum::extract::State;
        use axum::Json;
        let gate = Arc::new(tokio::sync::Barrier::new(2));
        let app = axum::Router::new()
            .route(
                "/v1/chat/completions",
                axum::routing::post(|State(gate): State<Arc<tokio::sync::Barrier>>| async move {
                    gate.wait().await;
                    Json(serde_json::json!({
                        "choices": [{ "message": { "content": "pong" } }]
                    }))
                }),
            )
            .with_state(gate);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            axum::serve(listener, app).await.ok();
        });
        let body = format!(
            r#"
node:
  id: hub
  mode: master
  listen_addr: "127.0.0.1:1"
  hmac_secret_path: "/tmp/hmac"
storage:
  primary_url: null
  fallback_sqlite: "/tmp/m.db"
git:
  bare_repos_root: null
models:
  providers:
    - id: gpu-a
      host: gpu-a
      kind: openai
      base_url: "http://{addr}/v1"
      chat_model: coder
      models: ["coder"]
      discover: false
      timeout_secs: 2
    - id: gpu-b
      host: gpu-b
      kind: openai
      base_url: "http://{addr}/v1"
      chat_model: coder
      models: ["coder"]
      discover: false
      timeout_secs: 2
"#
        );
        let (dir, cfg) = write_cfg(&body);
        let hub = ModelHub::from_config(&cfg).unwrap();
        let turns = [ChatTurn {
            role: "user".into(),
            text: "ping".into(),
            attachments: vec![],
        }];
        let joined = tokio::time::timeout(Duration::from_secs(3), async {
            tokio::join!(
                hub.generate("gpu-a/coder", "", &turns),
                hub.generate("gpu-b/coder", "", &turns),
            )
        })
        .await
        .expect("hosts were awaited one after another");
        assert_eq!(joined.0.unwrap(), "pong");
        assert_eq!(joined.1.unwrap(), "pong");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
