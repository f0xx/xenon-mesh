use crate::gemini::{Attachment, ChatTurn, GeminiClient};
use crate::git_sync::{discover_bare_repos, scan_bare_repo};
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
    pub default_model: String,
    pub available_models: Vec<String>,
    pub status: Arc<RwLock<IndexStatus>>,
}

#[derive(Deserialize)]
pub struct ChatReq {
    pub channel: String,
    pub thread: String,
    pub prompt: String,
    pub model: Option<String>,
    #[serde(default)]
    pub attachments: Vec<Attachment>,
}

#[derive(Deserialize)]
pub struct ForkThreadReq {
    pub channel: String,
    pub src_thread: String,
    pub dst_thread: String,
    pub keep_head: usize,
    pub keep_tail: usize,
}

#[derive(Serialize)] pub struct ChatRes { pub reply: String }
#[derive(Deserialize)] pub struct HistoryQuery { pub channel: String, pub thread: String }
#[derive(Deserialize)] pub struct ThreadsQuery { pub channel: String }
#[derive(Deserialize)] pub struct ReindexQuery { pub namespace: String }

use crate::storage::MemoryItem;

#[derive(Deserialize)] pub struct MemListQuery { pub channel: String }
#[derive(Deserialize)] pub struct MemDelQuery { pub id: String }

use tokio::sync::RwLock;

#[derive(Clone, Serialize, Default)]
pub struct IndexStatus {
    pub busy: bool,
    pub phase: String,      // "idle" | "indexing"
    pub current_repo: String,
    pub done_repos: usize,
    pub total_repos: usize,
    pub indexed_files: usize,
}

pub fn router(state: Arc<WebState>) -> Router {
    Router::new()
        .route("/", get(|| async { Html(include_str!("../assets/index.html")) }))
        .route("/api/chat", post(handle_persistent_chat))
        .route("/api/history", get(handle_history))
        .route("/api/threads", get(handle_list_threads))
        .route("/api/memory/learn", post(handle_learn))
        .route("/api/task/approve", post(handle_approve))
        .route("/api/internal/reindex", post(handle_reindex))
        .route("/api/config", get(|State(st): State<Arc<WebState>>| async move {
            Json(serde_json::json!({
                "default_model": st.default_model,
                "models": st.available_models
            }))
        }))
        .route("/api/threads/fork", post(handle_fork_thread))
        .route("/api/memory", get(handle_list_memory).delete(handle_delete_memory))
        .route("/api/channels", get(|State(st): State<Arc<WebState>>| async move {
            st.db.list_channels().await.map(Json).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
        }))
        .route("/api/status", get(|State(st): State<Arc<WebState>>| async move {
            Json(st.status.read().await.clone())
        }))
        .route("/v1/chat/completions", post(handle_oai_compat))
        .layer(axum::extract::DefaultBodyLimit::max(25 * 1024 * 1024))
        .with_state(state)
}

async fn handle_history(State(st): State<Arc<WebState>>, Query(q): Query<HistoryQuery>) -> Result<Json<Vec<ChatTurn>>, StatusCode> {
    let msgs = st.db.load_thread(&q.channel, &q.thread, 200).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(msgs.into_iter().map(|m| ChatTurn { role: m.author, text: m.content, attachments: vec![] }).collect()))
}

async fn handle_list_threads(State(st): State<Arc<WebState>>, Query(q): Query<ThreadsQuery>) -> Result<Json<Vec<String>>, StatusCode> {
    st.db.list_threads(&q.channel).await.map(Json).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn handle_persistent_chat(State(st): State<Arc<WebState>>, Json(req): Json<ChatReq>) -> Result<Json<ChatRes>, StatusCode> {
    let past = st.db.load_thread(&req.channel, &req.thread, 100).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let next_seq = past.last().map(|m| m.seq + 1).unwrap_or(1);

    // В историю БД сохраняем пометку о прикрепленных файлах
    let att_note = if req.attachments.is_empty() { String::new() } else {
        let names: Vec<&str> = req.attachments.iter().map(|a| a.name.as_str()).collect();
        format!("\n[Attached: {}]", names.join(", "))
    };
    let user_msg = SessionMessage {
        channel_id: req.channel.clone(), thread_id: req.thread.clone(),
        seq: next_seq, author: "user".into(), content: format!("{}{}", req.prompt, att_note), created_at_ms: 0,
    };
    st.db.append_message(&user_msg).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;

    let mut turns: Vec<ChatTurn> = past.into_iter().map(|m| ChatTurn {
        role: m.author, text: m.content, attachments: vec![],
    }).collect();
    turns.push(ChatTurn { role: "user".into(), text: req.prompt.clone(), attachments: req.attachments });

    let q_emb = st.gemini.embed_text(&req.prompt).await.unwrap_or_default();
    let ctx = build_prompt_context(&st.db, &req.channel, &q_emb, 8).await.map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let model = req.model.as_deref().filter(|s| !s.is_empty()).unwrap_or(&st.default_model);

    let reply = match st.gemini.generate_chat_with_model(model, &ctx, &turns).await {
        Ok(r) => r,
        Err(e) => return Ok(Json(ChatRes { reply: format!("[ERROR] {e}") })),
    };

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

async fn handle_reindex(
    State(st): State<Arc<WebState>>,
    Query(q): Query<ReindexQuery>,
) -> Result<StatusCode, StatusCode> {
    let Some(root) = &st.bare_repos_root else {
        return Ok(StatusCode::NO_CONTENT);
    };

    let repos = discover_bare_repos(Path::new(root));
    let Some((_, repo_path)) = repos.into_iter().find(|(ns, _)| ns == &q.namespace) else {
        return Err(StatusCode::NOT_FOUND);
    };

    if let Ok(entries) = scan_bare_repo(&repo_path, &q.namespace) {
        for mut e in entries {
            if let Ok(emb) = st.gemini.embed_text(&e.content).await {
                e.embedding = emb;
            }
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
        attachments: vec![],
    }).collect();
    let reply = st.gemini.generate_chat(&ctx, &turns).await.map_err(|_| StatusCode::BAD_GATEWAY)?;

    Ok(Json(serde_json::json!({
        "id": "chatcmpl-lab", "object": "chat.completion",
        "choices": [{ "index": 0, "message": { "role": "assistant", "content": reply }, "finish_reason": "stop" }]
    })))
}

async fn handle_fork_thread(
    State(st): State<Arc<WebState>>,
    Json(req): Json<ForkThreadReq>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let copied = st.db
        .fork_thread(&req.channel, &req.src_thread, &req.dst_thread, req.keep_head, req.keep_tail)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(serde_json::json!({ "copied": copied, "thread": req.dst_thread })))
}

async fn handle_list_memory(
    State(st): State<Arc<WebState>>,
    Query(q): Query<MemListQuery>,
) -> Result<Json<Vec<MemoryItem>>, StatusCode> {
    st.db.list_channel_memory(&q.channel).await.map(Json).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn handle_delete_memory(
    State(st): State<Arc<WebState>>,
    Query(q): Query<MemDelQuery>,
) -> Result<StatusCode, StatusCode> {
    st.db.delete_entry(&q.id).await.map(|_| StatusCode::OK).map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}


