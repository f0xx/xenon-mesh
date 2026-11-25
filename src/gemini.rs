use anyhow::{bail, Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

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

pub struct GeminiClient {
    http: Client,
    api_key: String,
    chat_model: String,
    embed_model: String,
    fallback_models: Vec<String>,
}

impl GeminiClient {
    pub fn new(api_key: String, chat_model: &str, embed_model: &str, fallback_models: Vec<String>) -> Self {
        let http = Client::builder()
            .timeout(std::time::Duration::from_secs(60))
            .build()
            .unwrap_or_else(|_| Client::new());
        Self {
            http,
            api_key,
            chat_model: chat_model.into(),
            embed_model: embed_model.into(),
            fallback_models,
        }
    }

    fn endpoint(&self, model: &str, action: &str) -> String {
        format!(
            "https://generativelanguage.googleapis.com/v1beta/models/{model}:{action}?key={}",
            self.api_key
        )
    }

    pub async fn embed_text(&self, text: &str) -> Result<Vec<f32>> {
        let body = json!({ "model": format!("models/{}", self.embed_model), "content": { "parts": [{ "text": text }] } });
        let res: Value = self.http.post(self.endpoint(&self.embed_model, "embedContent")).json(&body).send().await?.json().await?;
        let vals = res["embedding"]["values"].as_array().context("Missing embedding values")?;
        Ok(vals.iter().filter_map(|v| v.as_f64().map(|f| f as f32)).collect())
    }

    pub async fn generate_chat(&self, system_ctx: &str, turns: &[ChatTurn]) -> Result<String> {
        self.generate_chat_with_model(&self.chat_model, system_ctx, turns).await
    }

    pub async fn generate_chat_with_model(&self, model: &str, system_ctx: &str, turns: &[ChatTurn]) -> Result<String> {
        let contents: Vec<Value> = turns.iter().map(|t| {
            let mut parts = vec![json!({ "text": t.text })];
            for att in &t.attachments {
                parts.push(json!({ "inlineData": { "mimeType": att.mime_type, "data": att.data_base64 } }));
            }
            json!({ "role": t.role, "parts": parts })
        }).collect();

        let mut body = json!({ "contents": contents, "generationConfig": { "temperature": 0.3 } });
        if !system_ctx.trim().is_empty() {
            body["systemInstruction"] = json!({ "parts": [{ "text": system_ctx }] });
        }

        let mut candidates = vec![model.to_string()];
        for m in &self.fallback_models {
            if m != model && !candidates.contains(m) {
                candidates.push(m.clone());
            }
        }

        let mut last_err = String::new();
        for m in &candidates {
            let res: Value = self.http.post(self.endpoint(m, "generateContent")).json(&body).send().await?.json().await?;
            if let Some(txt) = res["candidates"].get(0).and_then(|c| c["content"]["parts"].get(0)).and_then(|p| p["text"].as_str()) {
                return Ok(txt.to_string());
            }
            last_err = res["error"]["message"].as_str().unwrap_or("Empty response").to_string();
            if !last_err.contains("high demand") && !last_err.contains("overloaded") && !last_err.contains("503") {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(800)).await;
        }
        bail!("Gemini API error: {last_err}")
    }

    pub async fn distill_lesson(&self, thread_summary: &str) -> Result<String> {
        let sys = "Extract strict technical invariants, node/router configs and code conventions as a concise list (max 10 lines).";
        self.generate_chat(sys, &[ChatTurn { role: "user".into(), text: thread_summary.into(), attachments: vec![] }]).await
    }
}

use tokio::sync::mpsc;

#[derive(Serialize, Clone)]
#[serde(tag = "type", rename_all = "lowercase")]
pub enum StreamEvent {
    Status { text: String },
    Thought { text: String },
    Delta { text: String },
    Done { model_used: String },
}

impl GeminiClient {
    pub async fn stream_chat_with_fallback(
        &self,
        model: &str,
        system_ctx: &str,
        turns: &[ChatTurn],
        tx: mpsc::Sender<StreamEvent>,
    ) -> Result<String> {
        let contents: Vec<Value> = turns.iter().map(|t| {
            let mut parts = vec![json!({ "text": t.text })];
            for att in &t.attachments {
                parts.push(json!({ "inlineData": { "mimeType": att.mime_type, "data": att.data_base64 } }));
            }
            json!({ "role": t.role, "parts": parts })
        }).collect();

        let mut body = json!({
            "contents": contents,
            "generationConfig": { "temperature": 0.3, "thinkingConfig": { "includeThoughts": true } }
        });
        if !system_ctx.trim().is_empty() {
            body["systemInstruction"] = json!({ "parts": [{ "text": system_ctx }] });
        }

        let mut candidates = vec![model.to_string()];
        for m in &self.fallback_models {
            if m != model && !candidates.contains(m) { candidates.push(m.clone()); }
        }

        let mut last_err = String::new();
        for (idx, m) in candidates.iter().enumerate() {
            let _ = tx.send(StreamEvent::Status { text: format!("Awaiting {m}...") }).await;
            let url = format!("{}&alt=sse", self.endpoint(m, "streamGenerateContent"));
            let mut resp = self.http.post(&url).json(&body).send().await?;

            if !resp.status().is_success() {
                let err_txt = resp.text().await.unwrap_or_default();
                last_err = serde_json::from_str::<Value>(&err_txt).ok()
                    .and_then(|v| v["error"]["message"].as_str().map(String::from))
                    .unwrap_or(err_txt);
                if let Some(next_m) = candidates.get(idx + 1) {
                    let _ = tx.send(StreamEvent::Status {
                        text: format!("⚠ {m} rejected ({}), falling back to {next_m}...", last_err.chars().take(60).collect::<String>())
                    }).await;
                    continue;
                }
                break;
            }

            let mut full_reply = String::new();
            let mut buf = String::new();
            while let Some(chunk) = resp.chunk().await? {
                buf.push_str(&String::from_utf8_lossy(&chunk));
                while let Some(pos) = buf.find('\n') {
                    let line = buf[..pos].trim().to_string();
                    buf.drain(..=pos);
                    if let Some(data) = line.strip_prefix("data: ") {
                        if let Ok(v) = serde_json::from_str::<Value>(data) {
                            if let Some(parts) = v["candidates"].get(0).and_then(|c| c["content"]["parts"].as_array()) {
                                for p in parts {
                                    let txt = p["text"].as_str().unwrap_or("");
                                    if p["thought"].as_bool().unwrap_or(false) {
                                        let _ = tx.send(StreamEvent::Thought { text: txt.to_string() }).await;
                                    } else if !txt.is_empty() {
                                        full_reply.push_str(txt);
                                        let _ = tx.send(StreamEvent::Delta { text: txt.to_string() }).await;
                                    }
                                }
                            }
                        }
                    }
                }
            }
            let _ = tx.send(StreamEvent::Done { model_used: m.clone() }).await;
            return Ok(full_reply);
        }
        bail!("Gemini API error: {last_err}")
    }
}

