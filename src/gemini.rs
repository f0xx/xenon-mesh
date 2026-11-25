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
}

impl GeminiClient {
    pub fn new(api_key: String, chat_model: &str, embed_model: &str) -> Self {
        let http = Client::builder()
            .timeout(std::time::Duration::from_secs(15))
            .build()
            .unwrap_or_else(|_| Client::new());
        Self { http, api_key, chat_model: chat_model.into(), embed_model: embed_model.into() }
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

        let res: Value = self.http.post(self.endpoint(model, "generateContent")).json(&body).send().await?.json().await?;
        if let Some(err_msg) = res["error"]["message"].as_str() {
            bail!("Gemini API error: {err_msg}");
        }
        res["candidates"].get(0).and_then(|c| c["content"]["parts"].get(0)).and_then(|p| p["text"].as_str())
            .map(str::to_string).context("Empty Gemini response")
    }

    pub async fn distill_lesson(&self, thread_summary: &str) -> Result<String> {
        let sys = "Extract strict technical invariants, node/router configs and code conventions as a concise list (max 10 lines).";
        self.generate_chat(sys, &[ChatTurn { role: "user".into(), text: thread_summary.into(), attachments: vec![] }]).await
    }
}

