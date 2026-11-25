use crate::gemini::{ChatTurn, GeminiClient};
use crate::git_sync::scan_bare_repo;
use crate::storage::pb::cluster_service_client::ClusterServiceClient;
use crate::storage::pb::{ContextEntry, EntryType, NodeTaskRequest, SessionMessage};
use crate::storage::Storage;
use crate::watcher::build_prompt_context;
use axum::{extract::{Query, State}, http::StatusCode, response::Html, routing::{get, post}, Json, Router};
use serde::{Deserialize, Serialize};
use std::{path::Path, sync::Arc};

pub struct WebState {
    pub db: Arc<Storage>,
    pub gemini: Arc<GeminiClient>,
    pub hmac_key: [u8; 32],
    pub bare_repos_root: Option<String>,
}

#[derive(Deserialize)] pub struct ChatReq { pub channel: String, pub thread: String, pub prompt: String }
#[derive(Serialize)] pub struct ChatRes { pub reply: String }
#[derive(Deserialize)] pub struct HistoryQuery { pub channel: String, pub thread: String }
#[derive(Deserialize)] pub struct ThreadsQuery { pub channel: String }
#[derive(Deserialize)] pub struct ReindexQuery { pub namespace: String }

pub fn router(state: Arc<WebState>) -> Router {
    Router::new()
        .route("/", get(|| async { Html(include_str!("../assets/index.html")) }))
        .route("/api/chat", post(handle_persistent_chat))
        .route("/api/history", get(handle_history))
        .route("/api/threads", get(handle_list_threads))
        .route("/api/memory/learn", post(handle_learn))
        .route("/api/task/approve", post(handle_approve))
        .route("/api/internal/reindex", post(handle_reindex))
        .route("/v1/chat/completions", post(handle_oai_compat))
        .with_state(state)
}

async fn handle_history(State(st): State<Arc<WebState>>, Query(q): Query<HistoryQuery>) -> Result<Json<Vec<ChatTurn>>, StatusCode> {
    let msgs = st.db.load_thread(&q.channel, &q.thread, 200).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(msgs.into_iter().map(|m| ChatTurn { role: m.author, text: m.content }).collect()))
}

async fn handle_list_threads(State(st): State<Arc<WebState>>, Query(q): Query<ThreadsQuery>) -> Result<Json<Vec<String>>, StatusCode> {
    st.db.list_threads(&q.channel).await.map(Json).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn handle_persistent_chat(State(st): State<Arc<WebState>>, Json(req): Json<ChatReq>) -> Result<Json<ChatRes>, StatusCode> {
    let mut past = st.db.load_thread(&req.channel, &req.thread, 100).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let next_seq = past.last().map(|m| m.seq + 1).unwrap_or(1);

    let user_msg = SessionMessage {
        channel_id: req.channel.clone(), thread_id: req.thread.clone(),
        seq: next_seq, author: "user".into(), content: req.prompt.clone(), created_at_ms: 0,
    };
    st.db.append_message(&user_msg).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    past.push(user_msg);

    let turns: Vec<ChatTurn> = past.into_iter().map(|m| ChatTurn { role: m.author, text: m.content }).collect();
    let q_emb = st.gemini.embed_text(&req.prompt).await.unwrap_or_default();
    let ctx = build_prompt_context(&st.db, &req.channel, &q_emb, 8).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let reply = st.gemini.generate_chat(&ctx, &turns).await.map_err(|_| StatusCode::BAD_GATEWAY)?;

    let bot_msg = SessionMessage {
        channel_id: req.channel, thread_id: req.thread,
        seq: next_seq + 1, author: "model".into(), content: reply.clone(), created_at_ms: 0,
    };
    st.db.append_message(&bot_msg).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(ChatRes { reply }))
}
#[derive(Deserialize)] pub struct LearnReq { pub channel: String, pub title: String, pub text: String, pub pin: bool }
#[derive(Deserialize)] pub struct ApproveReq {
    pub node_grpc_url: String, pub target_repo: String, pub unified_diff: String, pub deploy_pipeline: String,
}
#[derive(Deserialize)] pub struct OaiChatReq { pub model: String, pub messages: Vec<OaiMsg> }
#[derive(Deserialize, Serialize)] pub struct OaiMsg { pub role: String, pub content: String }

async fn handle_learn(State(st): State<Arc<WebState>>, Json(req): Json<LearnReq>) -> Result<StatusCode, StatusCode> {
    let summary = if req.pin { req.text } else {
        st.gemini.distill_lesson(&req.text).await.map_err(|_| StatusCode::BAD_GATEWAY)?
    };
    let id = blake3::hash(format!("mem:{}:{}", req.channel, req.title).as_bytes()).to_hex().to_string();
    let hash = blake3::hash(summary.as_bytes()).to_hex().to_string();
    let entry = ContextEntry {
        id, namespace: req.channel,
        entry_type: if req.pin { EntryType::Pinned as i32 } else { EntryType::Learned as i32 },
        source_path: format!("memory/{}", req.title), content: summary, content_hash: hash,
        origin_node: "master-ui".into(), ..Default::default()
    };
    st.db.upsert_proto(&entry, true).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(StatusCode::OK)
}

async fn handle_approve(State(st): State<Arc<WebState>>, Json(req): Json<ApproveReq>) -> Result<Json<serde_json::Value>, StatusCode> {
    let task_id = blake3::hash(req.unified_diff.as_bytes()).to_hex()[..16].to_string();
    let exp = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_millis() as i64 + 30_000;
    let payload = format!("{}:{}:{}:{}:{}", task_id, req.target_repo, req.unified_diff, req.deploy_pipeline, exp);
    let sig = blake3::keyed_hash(&st.hmac_key, payload.as_bytes()).as_bytes().to_vec();

    let mut client = ClusterServiceClient::connect(req.node_grpc_url).await.map_err(|_| StatusCode::BAD_GATEWAY)?;
    let res = client.execute_task(NodeTaskRequest {
        task_id, target_repo: req.target_repo, unified_diff: req.unified_diff,
        deploy_pipeline: req.deploy_pipeline, master_hmac_sig: sig, expires_at_ms: exp,
    }).await.map_err(|_| StatusCode::FORBIDDEN)?.into_inner();

    Ok(Json(serde_json::json!({ "success": res.success, "code": res.exit_code, "out": res.stdout_log, "err": res.stderr_log })))
}

async fn handle_reindex(State(st): State<Arc<WebState>>, Query(q): Query<ReindexQuery>) -> Result<StatusCode, StatusCode> {
    let Some(root) = &st.bare_repos_root else { return Ok(StatusCode::NO_CONTENT); };
    let repo_path = Path::new(root).join(format!("{}.git", q.namespace));
    if let Ok(entries) = scan_bare_repo(&repo_path, &q.namespace) {
        for mut e in entries {
            if let Ok(emb) = st.gemini.embed_text(&e.content).await { e.embedding = emb; }
            let _ = st.db.upsert_proto(&e, false).await;
        }
    }
    Ok(StatusCode::OK)
}

async fn handle_oai_compat(State(st): State<Arc<WebState>>, Json(req): Json<OaiChatReq>) -> Result<Json<serde_json::Value>, StatusCode> {
    let channel = req.model.strip_prefix("lab:").unwrap_or(&req.model);
    let last_user = req.messages.iter().rev().find(|m| m.role == "user").map(|m| m.content.as_str()).unwrap_or("");
    let q_emb = st.gemini.embed_text(last_user).await.unwrap_or_default();
    let ctx = build_prompt_context(&st.db, channel, &q_emb, 8).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let turns: Vec<ChatTurn> = req.messages.into_iter().map(|m| ChatTurn {
        role: if m.role == "assistant" { "model".into() } else { "user".into() }, text: m.content,
    }).collect();
    let reply = st.gemini.generate_chat(&ctx, &turns).await.map_err(|_| StatusCode::BAD_GATEWAY)?;

    Ok(Json(serde_json::json!({
        "id": "chatcmpl-lab", "object": "chat.completion",
        "choices": [{ "index": 0, "message": { "role": "assistant", "content": reply }, "finish_reason": "stop" }]
    })))
}

