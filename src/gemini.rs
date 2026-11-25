use anyhow::{Context, Result};
use reqwest::Client;
use serde_json::{json, Value};

pub struct GeminiClient {
    http: Client,
    api_key: String,
    chat_model: String,
    embed_model: String,
}

impl GeminiClient {
    pub fn new(api_key: String, chat_model: &str, embed_model: &str) -> Self {
        Self {
            http: Client::new(),
            api_key,
            chat_model: chat_model.to_string(),
            embed_model: embed_model.to_string(),
        }
    }

    pub async fn embed_text(&self, text: &str) -> Result<Vec<f32>> {
        let url = format!(
            "https://googleapis.com{}:embedContent?key={}",
            self.embed_model, self.api_key
        );
        let body = json!({
            "model": format!("models/{}", self.embed_model),
            "content": { "parts": [{ "text": text }] }
        });

        let res: Value = self.http.post(&url).json(&body).send().await?.json().await?;
        let vals = res["embedding"]["values"]
            .as_array()
            .context("Missing embedding values in Gemini response")?;

        Ok(vals.iter().filter_map(|v| v.as_f64().map(|f| f as f32)).collect())
    }

    pub async fn generate(&self, system_ctx: &str, user_prompt: &str) -> Result<String> {
        let url = format!(
            "https://googleapis.com{}:generateContent?key={}",
            self.chat_model, self.api_key
        );
        let body = json!({
            "systemInstruction": { "parts": [{ "text": system_ctx }] },
            "contents": [{ "role": "user", "parts": [{ "text": user_prompt }] }],
            "generationConfig": { "temperature": 0.2 }
        });

        let res: Value = self.http.post(&url).json(&body).send().await?.json().await?;
        let reply = res["candidates"][0]["content"]["parts"][0]["text"]
            .as_str()
            .context("Empty generation from Gemini")?;
        Ok(reply.to_string())
    }

    pub async fn distill_lesson(&self, thread_summary: &str) -> Result<String> {
        let sys = "You are lab system architect. Extract from dialog only strict technical invariants, \
                   configuration specifics for nodes/routers and code conductions formalized up to 10 lines of code. \
                   No water, no comments, no duplications.";
        self.generate(sys, thread_summary).await
    }
}



#[derive(serde::Serialize, serde::Deserialize, Clone)]
pub struct ChatTurn {
    pub role: String, // "user" or "model"
    pub text: String,
}

impl GeminiClient {
    pub async fn generate_chat(&self, system_ctx: &str, turns: &[ChatTurn]) -> Result<String> {
        let url = format!(
            "https://googleapis.com{}:generateContent?key={}",
            self.chat_model, self.api_key
        );
        let contents: Vec<Value> = turns.iter().map(|t| json!({
            "role": t.role,
            "parts": [{ "text": t.text }]
        })).collect();

        let mut body = json!({ "contents": contents, "generationConfig": { "temperature": 0.4 } });
        if !system_ctx.trim().is_empty() {
            body["systemInstruction"] = json!({ "parts": [{ "text": system_ctx }] });
        }

        let res: Value = self.http.post(&url).json(&body).send().await?.json().await?;
        let reply = res["candidates"]["content"]["parts"]["text"]
            .as_str()
            .context("Empty generation from Gemini")?;
        Ok(reply.to_string())
    }
}

