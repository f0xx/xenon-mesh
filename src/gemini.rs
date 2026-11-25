use anyhow::{Context, Result};
use reqwest::Client;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

#[derive(Serialize, Deserialize, Clone)]
pub struct ChatTurn {
    pub role: String,
    pub text: String,
}

pub struct GeminiClient {
    http: Client,
    api_key: String,
    chat_model: String,
    embed_model: String,
}

impl GeminiClient {
    pub fn new(api_key: String, chat_model: &str, embed_model: &str) -> Self {
        Self { http: Client::new(), api_key, chat_model: chat_model.into(), embed_model: embed_model.into() }
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
        let contents: Vec<Value> = turns.iter().map(|t| json!({ "role": t.role, "parts": [{ "text": t.text }] })).collect();
        let mut body = json!({ "contents": contents, "generationConfig": { "temperature": 0.3 } });
        if !system_ctx.trim().is_empty() {
            body["systemInstruction"] = json!({ "parts": [{ "text": system_ctx }] });
        }
        let res: Value = self.http.post(self.endpoint(model, "generateContent")).json(&body).send().await?.json().await?;
        res["candidates"][0]["content"]["parts"][0]["text"].as_str().map(str::to_string).context("Empty Gemini response")
    }

    pub async fn distill_lesson(&self, thread_summary: &str) -> Result<String> {
        let sys = "Extract strict technical invariants, node/router configs and code conventions as a concise list (max 10 lines).";
        self.generate_chat(sys, &[ChatTurn { role: "user".into(), text: thread_summary.into() }]).await
    }
}

