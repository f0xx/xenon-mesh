use crate::config::{AppConfig, NodeMode};
use crate::git_sync::{discover_bare_repos, scan_bare_repo, scan_workdir};
use crate::hub;
use crate::models::{Attachment, ChatTurn, ModelChoice, ModelHub, StreamEvent};
use crate::retrieve::{assemble_context, coding_preamble, map_entries};
use crate::storage::pb::cluster_service_client::ClusterServiceClient;
use crate::storage::pb::{ContextEntry, EntryType, NodeTaskRequest, SessionMessage, SyncEnvelope};
use crate::storage::{
    channel_group, cmp_sidebar, now_ms, AuditEvent, ChannelGroup, ChannelPref, ChannelRow,
    SidebarKey, Storage,
};
use axum::response::sse::{Event, KeepAlive, Sse};
use axum::{
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::Html,
    routing::{get, post},
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::convert::Infallible;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tokio::sync::RwLock;
use tokio_stream::{wrappers::ReceiverStream, StreamExt};
use tracing::warn;

#[derive(Clone)]
pub struct Presence {
    pub channel: String,
    pub viewing: bool,
}

#[derive(Clone)]
pub struct WebState {
    pub cfg: Arc<AppConfig>,
    pub db: Arc<Storage>,
    pub models: Arc<ModelHub>,
    pub hmac_key: [u8; 32],
    pub node_key: Option<[u8; 32]>,
    pub peer_keys: HashMap<String, [u8; 32]>,
    pub status: Arc<RwLock<IndexStatus>>,
    pub presence: Arc<std::sync::Mutex<Presence>>,
    pub neighbors: Arc<Mutex<Vec<crate::discover::Neighbor>>>,
    /// channel -> (typing|reply, expiry epoch ms)
    pub live: Arc<Mutex<HashMap<String, (String, i64)>>>,
}

#[derive(Clone, Serialize, Default)]
pub struct IndexStatus {
    pub busy: bool,
    pub phase: String,
    pub current_repo: String,
    pub done_repos: usize,
    pub total_repos: usize,
    pub indexed_files: usize,
    pub mode: String,
    pub embeddings: bool,
}

#[derive(Deserialize)]
pub struct ChatReq {
    pub channel: String,
    pub thread: String,
    pub prompt: String,
    pub model: Option<String>,
    #[serde(default)]
    pub models: Vec<String>,
    #[serde(default)]
    pub attachments: Vec<Attachment>,
    /// Re-run the model on the thread as stored. Used after an edit.
    #[serde(default)]
    pub rerun: bool,
    #[serde(default)]
    pub debate: bool,
}

#[derive(Deserialize)]
pub struct ForkThreadReq {
    pub channel: String,
    pub src_thread: String,
    pub dst_thread: String,
    pub keep_head: usize,
    pub keep_tail: usize,
}

#[derive(Deserialize)]
pub struct HistoryQuery {
    pub channel: String,
    pub thread: String,
}
#[derive(Deserialize)]
pub struct ThreadsQuery {
    pub channel: String,
}
#[derive(Deserialize)]
pub struct ReindexQuery {
    pub namespace: String,
}
#[derive(Deserialize)]
pub struct MemListQuery {
    pub channel: String,
}
#[derive(Deserialize)]
pub struct MemDelQuery {
    pub id: String,
}
#[derive(Deserialize)]
pub struct LearnReq {
    pub channel: String,
    pub title: String,
    pub text: String,
    pub pin: bool,
}
#[derive(Deserialize)]
pub struct ApproveReq {
    pub node_grpc_url: String,
    pub target_repo: String,
    pub unified_diff: String,
    pub deploy_pipeline: String,
}
#[derive(Deserialize)]
pub struct OaiChatReq {
    pub model: String,
    pub messages: Vec<OaiMsg>,
}
#[derive(Deserialize, Serialize)]
pub struct OaiMsg {
    pub role: String,
    pub content: String,
}
#[derive(Deserialize)]
pub struct ChannelUpsert {
    pub name: String,
    #[serde(default)]
    pub visibility: String,
    #[serde(default)]
    pub namespace: String,
    #[serde(default)]
    pub context_namespaces: Vec<String>,
}

pub fn router(state: Arc<WebState>) -> Router {
    Router::new()
        .route("/", get(|| async { Html(include_str!("../assets/index.html")) }))
        .route("/api/health", get(handle_health))
        .route("/api/chat", post(handle_persistent_chat))
        .route("/api/history", get(handle_history))
        .route("/api/threads", get(handle_list_threads))
        .route("/api/memory/learn", post(handle_learn))
        .route("/api/task/approve", post(handle_approve))
        .route("/api/internal/reindex", post(handle_reindex))
        .route("/api/internal/complete", post(handle_complete))
        .route("/api/config", get(handle_config))
        .route("/api/threads/fork", post(handle_fork_thread))
        .route("/api/threads/open", post(handle_open_thread))
        .route("/api/alias", get(handle_alias_get).post(handle_alias_set))
        .route("/api/chat-pref", get(handle_chat_pref_get).post(handle_chat_pref_set))
        .route("/api/memory", get(handle_list_memory).delete(handle_delete_memory))
        .route("/api/channels", get(handle_list_channels).post(handle_upsert_channel))
        .route("/api/channel-state", post(handle_channel_state))
        .route("/api/presence", post(handle_presence))
        .route("/api/share", post(handle_share))
        .route("/api/permalink", post(handle_permalink))
        .route("/api/messages/debug", get(handle_message_debug))
        .route("/api/messages/edit", post(handle_message_edit))
        .route("/api/messages/delete", post(handle_message_delete))
        .route("/api/channels/clone", post(handle_clone_channel))
        .route("/api/neighbors", get(handle_neighbors))
        .route("/api/typing", post(handle_typing))
        .route("/api/p2p/open", post(handle_p2p_open))
        .route("/api/p2p/hello", post(handle_p2p_hello))
        .route("/api/p2p/push", post(handle_p2p_push))
        .route("/api/p2p/pull", get(handle_p2p_pull))
        .route(
            "/api/mesh/context",
            get(handle_mesh_context).post(handle_mesh_context_put),
        )
        .route("/api/mesh/append", post(handle_mesh_append))
        .route("/api/mesh/edit", post(handle_mesh_edit))
        .route("/api/mesh/delete", post(handle_mesh_delete))
        .route("/api/mesh/typing", post(handle_mesh_typing))
        .route("/api/mesh/live", get(handle_mesh_live))
        .route("/api/internal/web", post(handle_internal_web))
        .route("/api/join", post(handle_join))
        .route("/api/inspect", post(handle_inspect))
        .route("/api/audit", get(handle_audit).post(handle_audit_post))
        .route("/api/mesh/channels", get(handle_mesh_channels))
        .route("/api/mesh/activity", get(handle_mesh_activity))
        .route("/api/mesh/share", post(handle_mesh_share))
        .route("/api/mesh/join", post(handle_mesh_join))
        .route("/api/mesh/history", get(handle_mesh_history))
        .route("/api/status", get(|State(st): State<Arc<WebState>>| async move {
            Json(st.status.read().await.clone())
        }))
        .route("/v1/chat/completions", post(handle_oai_compat))
        .layer(axum::extract::DefaultBodyLimit::max(25 * 1024 * 1024))
        .with_state(state)
}

fn mode_name(mode: &NodeMode) -> &'static str {
    match mode {
        NodeMode::Master => "master",
        NodeMode::Node => "node",
    }
}

fn master_url(cfg: &AppConfig) -> Option<&str> {
    match cfg.node.mode {
        NodeMode::Node => cfg.node.master_url.as_deref().filter(|s| !s.is_empty()),
        NodeMode::Master => None,
    }
}

fn master_http(cfg: &AppConfig) -> Option<&str> {
    match cfg.node.mode {
        NodeMode::Node => cfg
            .node
            .master_http
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty()),
        NodeMode::Master => None,
    }
}

async fn handle_health(State(st): State<Arc<WebState>>) -> Json<serde_json::Value> {
    Json(serde_json::json!({
        "ok": true,
        "mode": mode_name(&st.cfg.node.mode),
        "embeddings": st.models.embeddings_configured().await
    }))
}

async fn handle_config(State(st): State<Arc<WebState>>) -> Json<serde_json::Value> {
    let mut models = st.models.model_choices().await;
    let mut default_model = st.models.default_model().await;
    if let Some(base) = master_http(&st.cfg) {
        if let Some(remote) = fetch_hub_config(base).await {
            if default_model.is_empty() {
                default_model = remote.default_model;
            }
            for choice in remote.models {
                if !models.iter().any(|m| m.id == choice.id) {
                    models.push(choice);
                }
            }
        }
    }
    if master_http(&st.cfg).is_none() {
        for choice in models.iter_mut() {
            if choice.kind == "gemini" || choice.proxied {
                choice.host = st.cfg.node.id.clone();
                choice.label = format!("{} : {}", choice.model, choice.host);
                choice.proxied = true;
            }
        }
    }
    Json(serde_json::json!({
        "default_model": default_model,
        "models": models,
        "mode": mode_name(&st.cfg.node.mode),
        "node_id": st.cfg.node.id,
        "hub_http": master_http(&st.cfg).unwrap_or(""),
        "embeddings": st.models.embeddings_configured().await
    }))
}

#[derive(Deserialize)]
struct RemoteCatalog {
    #[serde(default)]
    default_model: String,
    #[serde(default)]
    models: Vec<ModelChoice>,
}

async fn fetch_hub_config(base: &str) -> Option<RemoteCatalog> {
    let url = format!("{}/api/config", base.trim_end_matches('/'));
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(8))
        .build()
        .ok()?;
    client.get(url).send().await.ok()?.json().await.ok()
}

async fn handle_list_channels(
    State(st): State<Arc<WebState>>,
) -> Result<Json<Vec<SidebarChannel>>, StatusCode> {
    let mut rows = st
        .db
        .list_channel_rows()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let mut remote_seq = std::collections::HashMap::new();
    if let Some(base) = master_http(&st.cfg) {
        if let Some(mesh) = fetch_mesh(base, &st.cfg.node.id).await {
            remote_seq = mesh.activity;
            for ch in mesh.channels {
                if !rows.iter().any(|r| r.name == ch.name) {
                    let _ = st.db.upsert_channel(&ch).await;
                    rows.push(ch);
                }
            }
        }
    } else if let Some(url) = master_url(&st.cfg) {
        if let Ok(remote) = hub::list_public(url).await {
            for ch in remote {
                let row = ChannelRow::from_proto(&ch);
                if !rows.iter().any(|r| r.name == row.name) {
                    rows.push(row);
                }
            }
        }
    }
    if matches!(st.cfg.node.mode, NodeMode::Master) {
        if let Ok(local_seq) = st.db.activity().await {
            for (name, seq) in local_seq {
                remote_seq.insert(name, seq);
            }
        }
    }
    if !rows.iter().any(|r| r.name == "general") {
        rows.insert(
            0,
            ChannelRow {
                name: "general".into(),
                visibility: "public".into(),
                namespace: "general".into(),
                origin_node: st.cfg.node.id.clone(),
                context_namespaces: vec!["general".into()],
                updated_at_ms: 0,
            },
        );
    }
    let remote_live = if let Some(base) = master_http(&st.cfg) {
        fetch_remote_live(base).await.unwrap_or_default()
    } else {
        HashMap::new()
    };
    let mut sidebar = Vec::with_capacity(rows.len());
    for row in rows {
        let pref = st
            .db
            .get_pref(&row.name)
            .await
            .unwrap_or_else(|_| ChannelPref::bare(&row.name));
        let local_max = st.db.max_seq(&row.name).await.unwrap_or(0);
        let max_seq = local_max.max(*remote_seq.get(&row.name).unwrap_or(&0));
        let unread = (max_seq - pref.last_read_seq).max(0);
        let members = st.db.list_members(&row.name).await.unwrap_or_default();
        let local_live = st
            .live
            .lock()
            .ok()
            .and_then(|map| {
                map.get(&row.name).and_then(|(kind, until)| {
                    (*until > now_ms()).then(|| kind.clone())
                })
            })
            .unwrap_or_default();
        let live = if local_live.is_empty() {
            remote_live.get(&row.name).cloned().unwrap_or_default()
        } else {
            local_live
        };
        let p2p = st
            .db
            .get_p2p(&row.name)
            .await
            .ok()
            .flatten()
            .is_some();
        sidebar.push(SidebarChannel {
            group: channel_group(&row.name, &row.visibility),
            pinned: pref.pinned,
            muted: pref.muted,
            archived: pref.archived && row.name != "general",
            closed: pref.closed && row.name != "general",
            read_count: pref.last_read_seq.max(0),
            unread_count: unread,
            use_count: pref.use_count,
            members,
            live,
            p2p,
            name: row.name,
            visibility: row.visibility,
            namespace: row.namespace,
            origin_node: row.origin_node,
        });
    }
    sidebar.sort_by(|a, b| {
        cmp_sidebar(
            &SidebarKey {
                group: a.group,
                pinned: a.pinned,
                use_count: a.use_count,
                name: a.name.clone(),
            },
            &SidebarKey {
                group: b.group,
                pinned: b.pinned,
                use_count: b.use_count,
                name: b.name.clone(),
            },
        )
    });
    Ok(Json(sidebar))
}

#[derive(Clone, Serialize)]
struct SidebarChannel {
    name: String,
    visibility: String,
    namespace: String,
    origin_node: String,
    group: ChannelGroup,
    pinned: bool,
    muted: bool,
    archived: bool,
    closed: bool,
    read_count: i64,
    unread_count: i64,
    use_count: i64,
    members: Vec<String>,
    #[serde(default)]
    live: String,
    #[serde(default)]
    p2p: bool,
}

#[derive(Deserialize)]
struct MeshView {
    #[serde(default)]
    channels: Vec<ChannelRow>,
    #[serde(default)]
    activity: std::collections::HashMap<String, i64>,
}

async fn fetch_mesh(base: &str, node_id: &str) -> Option<MeshView> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(8))
        .build()
        .ok()?;
    let root = base.trim_end_matches('/');
    let channels: Vec<ChannelRow> = client
        .get(format!("{root}/api/mesh/channels"))
        .query(&[("node_id", node_id)])
        .send()
        .await
        .ok()?
        .json()
        .await
        .ok()?;
    let activity: std::collections::HashMap<String, i64> = client
        .get(format!("{root}/api/mesh/activity"))
        .query(&[("node_id", node_id)])
        .send()
        .await
        .ok()?
        .json()
        .await
        .ok()?;
    Some(MeshView { channels, activity })
}

async fn handle_upsert_channel(
    State(st): State<Arc<WebState>>,
    Json(req): Json<ChannelUpsert>,
) -> Result<Json<ChannelRow>, StatusCode> {
    let namespace = if req.namespace.trim().is_empty() {
        req.name.clone()
    } else {
        req.namespace
    };
    let mut contexts = req.context_namespaces;
    if contexts.is_empty() {
        contexts.push(namespace.clone());
    }
    let row = ChannelRow {
        name: req.name,
        visibility: if req.visibility.trim().is_empty() {
            "private".into()
        } else {
            req.visibility
        },
        namespace,
        origin_node: st.cfg.node.id.clone(),
        context_namespaces: contexts,
        updated_at_ms: now_ms(),
    };
    st.db
        .upsert_channel(&row)
        .await
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let stored = st
        .db
        .get_channel(&row.name)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .unwrap_or(row);
    if stored.visibility == "public" {
        if let Some(url) = master_url(&st.cfg) {
            let env = SyncEnvelope {
                schema_version: 2,
                source_node: st.cfg.node.id.clone(),
                timestamp_ms: now_ms(),
                channels: vec![stored.to_proto()],
                ..Default::default()
            };
            if let Err(err) = hub::push_envelope(url, env).await {
                warn!("public channel was saved locally but not published: {err}");
            }
        }
    }
    Ok(Json(stored))
}

async fn channel_visibility(st: &WebState, name: &str) -> String {
    match st.db.get_channel(name).await.ok().flatten() {
        Some(ch) => ch.visibility,
        None if name == "general" => "public".into(),
        None => "private".into(),
    }
}

async fn channel_is_public(st: &WebState, name: &str) -> bool {
    channel_visibility(st, name).await == "public"
}

async fn handle_history(
    State(st): State<Arc<WebState>>,
    Query(q): Query<HistoryQuery>,
) -> Result<Json<Vec<HistoryItem>>, StatusCode> {
    if channel_is_public(&st, &q.channel).await
        && st
            .db
            .thread_visibility(&q.channel, &q.thread)
            .await
            .unwrap_or_default()
            != "private"
    {
        if let Some(url) = master_url(&st.cfg) {
            if let Ok(env) = hub::query_namespace(url, &q.channel).await {
                for msg in env.messages.into_iter().filter(|m| m.thread_id == q.thread) {
                    let _ = st.db.append_message(&msg).await;
                }
            }
        }
    }
    if channel_visibility(&st, &q.channel).await == "shared" {
        if let Some(base) = master_http(&st.cfg) {
            if let Some(msgs) =
                fetch_shared_history(base, &st.cfg.node.id, &q.channel, &q.thread).await
            {
                for msg in msgs {
                    apply_synced(&st, &q.channel, msg).await;
                }
            }
        }
    }
    if let Some(link) = st.db.get_p2p(&q.channel).await.ok().flatten() {
        if let Some(msgs) = pull_p2p(&link, &q.thread).await {
            for msg in msgs {
                apply_synced(&st, &q.channel, msg).await;
            }
        }
    }
    let msgs = st
        .db
        .load_shown(&q.channel, &q.thread, 200)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let threads = st
        .db
        .list_threads(&q.channel)
        .await
        .unwrap_or_default();
    Ok(Json(
        msgs.into_iter()
            .map(|m| {
                let child = format!("m{}", m.message.seq);
                HistoryItem {
                    role: m.message.author,
                    text: m.message.content,
                    created_at_ms: m.message.created_at_ms,
                    seq: m.message.seq,
                    edited_at_ms: m.edited_at_ms,
                    started_at_ms: m.started_at_ms,
                    finished_at_ms: m.finished_at_ms,
                    reply_thread: if q.thread == "main" && threads.iter().any(|t| t == &child) {
                        child
                    } else {
                        String::new()
                    },
                }
            })
            .collect(),
    ))
}

#[derive(Serialize)]
struct HistoryItem {
    role: String,
    text: String,
    created_at_ms: i64,
    seq: u64,
    #[serde(default)]
    edited_at_ms: i64,
    #[serde(default)]
    started_at_ms: i64,
    #[serde(default)]
    finished_at_ms: i64,
    #[serde(default)]
    reply_thread: String,
}

async fn handle_list_threads(
    State(st): State<Arc<WebState>>,
    Query(q): Query<ThreadsQuery>,
) -> Result<Json<Vec<String>>, StatusCode> {
    st.db
        .list_threads(&q.channel)
        .await
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn handle_learn(
    State(st): State<Arc<WebState>>,
    Json(req): Json<LearnReq>,
) -> Result<StatusCode, StatusCode> {
    let summary = if req.pin {
        req.text
    } else {
        st.models
            .distill(&req.text)
            .await
            .map_err(|_| StatusCode::BAD_GATEWAY)?
    };
    let id = blake3::hash(format!("mem:{}:{}", req.channel, req.title).as_bytes())
        .to_hex()
        .to_string();
    let hash = blake3::hash(summary.as_bytes()).to_hex().to_string();
    let entry = ContextEntry {
        id,
        namespace: req.channel.clone(),
        entry_type: if req.pin {
            EntryType::Pinned as i32
        } else {
            EntryType::Learned as i32
        },
        source_path: format!("memory/{}", req.title),
        content: summary,
        content_hash: hash,
        origin_node: st.cfg.node.id.clone(),
        updated_at_ms: now_ms(),
        ..Default::default()
    };
    st.db
        .upsert_proto(&entry, true)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    if channel_is_public(&st, &req.channel).await {
        if let Some(url) = master_url(&st.cfg) {
            let env = SyncEnvelope {
                schema_version: 2,
                source_node: st.cfg.node.id.clone(),
                timestamp_ms: now_ms(),
                entries: vec![entry],
                ..Default::default()
            };
            let _ = hub::push_envelope(url, env).await;
        }
    }
    Ok(StatusCode::OK)
}

async fn handle_approve(
    State(st): State<Arc<WebState>>,
    Json(req): Json<ApproveReq>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let pipeline_name = req.deploy_pipeline.clone();
    let repo_name = req.target_repo.clone();
    let task_id = blake3::hash(req.unified_diff.as_bytes()).to_hex()[..16].to_string();
    let exp = now_ms() + 30_000;
    let payload = format!(
        "{}:{}:{}:{}:{}",
        task_id, req.target_repo, req.unified_diff, req.deploy_pipeline, exp
    );
    let sig = blake3::keyed_hash(&st.hmac_key, payload.as_bytes())
        .as_bytes()
        .to_vec();
    let mut client = ClusterServiceClient::connect(req.node_grpc_url)
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    let res = client
        .execute_task(NodeTaskRequest {
            task_id,
            target_repo: req.target_repo,
            unified_diff: req.unified_diff,
            deploy_pipeline: req.deploy_pipeline,
            master_hmac_sig: sig,
            expires_at_ms: exp,
        })
        .await
        .map_err(|_| StatusCode::FORBIDDEN)?
        .into_inner();
    let mut action = if pipeline_name.contains("--force") || pipeline_name.contains("force-with-lease")
    {
        "git.force_push"
    } else if pipeline_name.contains("push") {
        "git.push"
    } else {
        "task.execute"
    };
    let mut detail = serde_json::json!({"pipeline": pipeline_name, "exit": res.exit_code});
    if let Some(line) = res.stdout_log.lines().find(|line| line.starts_with("XMESH_GIT ")) {
        if let Ok(git) = serde_json::from_str::<serde_json::Value>(line.trim_start_matches("XMESH_GIT ")) {
            action = if git.get("force").and_then(|v| v.as_bool()).unwrap_or(false) {
                "git.force_push"
            } else {
                "git.push"
            };
            detail = serde_json::json!({
                "pipeline": pipeline_name,
                "exit": res.exit_code,
                "force": git.get("force").cloned().unwrap_or(serde_json::Value::Bool(false)),
                "ref": git.get("ref").cloned().unwrap_or(serde_json::Value::String(String::new())),
                "old": git.get("old").cloned().unwrap_or(serde_json::Value::String(String::new())),
                "new": git.get("new").cloned().unwrap_or(serde_json::Value::String(String::new()))
            });
        }
    }
    publish_audit(&st, action, &repo_name, "", &detail.to_string()).await;
    Ok(Json(
        serde_json::json!({ "success": res.success, "code": res.exit_code, "out": res.stdout_log, "err": res.stderr_log }),
    ))
}

async fn handle_list_memory(
    State(st): State<Arc<WebState>>,
    Query(q): Query<MemListQuery>,
) -> Result<Json<Vec<crate::storage::MemoryItem>>, StatusCode> {
    st.db
        .list_channel_memory(&q.channel)
        .await
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn handle_delete_memory(
    State(st): State<Arc<WebState>>,
    Query(q): Query<MemDelQuery>,
) -> Result<StatusCode, StatusCode> {
    st.db
        .delete_entry(&q.id)
        .await
        .map(|_| StatusCode::OK)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn handle_fork_thread(
    State(st): State<Arc<WebState>>,
    Json(req): Json<ForkThreadReq>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    if channel_is_public(&st, &req.channel).await {
        if let Some(url) = master_url(&st.cfg) {
            if let Ok(env) = hub::query_namespace(url, &req.channel).await {
                for msg in env
                    .messages
                    .into_iter()
                    .filter(|m| m.thread_id == req.src_thread)
                {
                    let _ = st.db.append_message(&msg).await;
                }
            }
        }
    }
    let copied = st
        .db
        .fork_thread(
            &req.channel,
            &req.src_thread,
            &req.dst_thread,
            req.keep_head,
            req.keep_tail,
        )
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(
        serde_json::json!({ "copied": copied, "thread": req.dst_thread, "visibility": "private" }),
    ))
}

#[derive(Deserialize)]
struct OpenThreadReq {
    channel: String,
    seq: u64,
}

async fn handle_open_thread(
    State(st): State<Arc<WebState>>,
    Json(req): Json<OpenThreadReq>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let rows = st
        .db
        .load_shown(&req.channel, "main", 500)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let parent = rows
        .into_iter()
        .find(|m| m.message.seq == req.seq)
        .ok_or(StatusCode::NOT_FOUND)?;
    let public = channel_is_public(&st, &req.channel).await;
    let name = open_reply_thread(&st, &req.channel, &parent.message, public).await;
    Ok(Json(serde_json::json!({ "thread": name })))
}

#[derive(Deserialize)]
struct AliasReq {
    alias: String,
}

async fn handle_alias_get(State(st): State<Arc<WebState>>) -> Json<serde_json::Value> {
    let alias = clean_alias(&st.db.get_setting("alias").await.unwrap_or_default());
    let display = if alias.is_empty() {
        st.cfg.node.id.clone()
    } else {
        alias.clone()
    };
    Json(serde_json::json!({
        "alias": alias,
        "node_id": st.cfg.node.id,
        "display": display
    }))
}

async fn handle_alias_set(
    State(st): State<Arc<WebState>>,
    Json(req): Json<AliasReq>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let alias = clean_alias(&req.alias);
    st.db
        .set_setting("alias", &alias)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let display = if alias.is_empty() {
        st.cfg.node.id.clone()
    } else {
        alias.clone()
    };
    Ok(Json(serde_json::json!({ "alias": alias, "display": display })))
}

#[derive(Deserialize)]
struct ChatPrefQuery {
    channel: String,
    #[serde(default)]
    thread: String,
}

#[derive(Deserialize)]
struct ChatPrefBody {
    channel: String,
    #[serde(default)]
    thread: String,
    #[serde(default)]
    models: Vec<String>,
    #[serde(default)]
    multi: bool,
}

async fn handle_chat_pref_get(
    State(st): State<Arc<WebState>>,
    Query(q): Query<ChatPrefQuery>,
) -> Json<serde_json::Value> {
    let thread = if q.thread.is_empty() { "main" } else { &q.thread };
    let (models, multi) = st
        .db
        .get_chat_models(&q.channel, thread)
        .await
        .unwrap_or_default();
    Json(serde_json::json!({ "models": models, "multi": multi }))
}

async fn handle_chat_pref_set(
    State(st): State<Arc<WebState>>,
    Json(req): Json<ChatPrefBody>,
) -> Result<StatusCode, StatusCode> {
    let thread = if req.thread.is_empty() {
        "main".to_string()
    } else {
        req.thread
    };
    let models: Vec<String> = req
        .models
        .into_iter()
        .map(|m| m.trim().to_string())
        .filter(|m| !m.is_empty())
        .collect();
    st.db
        .set_chat_models(&req.channel, &thread, &models, req.multi && models.len() > 1)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(StatusCode::OK)
}

async fn handle_reindex(
    State(st): State<Arc<WebState>>,
    Query(q): Query<ReindexQuery>,
) -> Result<StatusCode, StatusCode> {
    let Some(root) = &st.cfg.git.bare_repos_root else {
        return Ok(StatusCode::NO_CONTENT);
    };
    let repos = discover_bare_repos(Path::new(root));
    let Some((_, repo_path)) = repos.into_iter().find(|(ns, _)| ns == &q.namespace) else {
        return Err(StatusCode::NOT_FOUND);
    };
    if let Ok(entries) = scan_bare_repo(&repo_path, &q.namespace) {
        for mut e in entries {
            if let Ok(emb) = st.models.embed_text(&e.content).await {
                e.embedding = emb;
            }
            let _ = st.db.upsert_proto(&e, false).await;
        }
    }
    Ok(StatusCode::OK)
}

async fn handle_oai_compat(
    State(st): State<Arc<WebState>>,
    Json(req): Json<OaiChatReq>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let channel = req.model.strip_prefix("channel:").unwrap_or(&req.model);
    let last_user = req
        .messages
        .iter()
        .rev()
        .find(|m| m.role == "user")
        .map(|m| m.content.as_str())
        .unwrap_or("");
    let ctx = prompt_for(&st, channel, "main", last_user, "")
        .await
        .unwrap_or_default();
    let turns: Vec<ChatTurn> = req
        .messages
        .into_iter()
        .map(|m| ChatTurn {
            role: m.role,
            text: m.content,
            attachments: vec![],
        })
        .collect();
    let reply = st
        .models
        .generate("", &ctx, &turns)
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    Ok(Json(serde_json::json!({
        "id": "chatcmpl-xmesh",
        "object": "chat.completion",
        "choices": [{ "index": 0, "message": { "role": "assistant", "content": reply }, "finish_reason": "stop" }]
    })))
}

async fn prompt_for(
    st: &WebState,
    channel: &str,
    _thread: &str,
    query: &str,
    model: &str,
) -> Result<String, StatusCode> {
    let choices = st.models.model_choices().await;
    if model.split('/').next() == Some("google")
        || choices
            .iter()
            .any(|choice| choice.id == model && choice.kind == "google")
    {
        return Ok(
            "Answer from the web result only. Do not invent repository source. \
             A short summary of the recent chat is included in the search."
                .into(),
        );
    }
    pull_remote_entries(st, channel).await;
    let channel_row = st.db.get_channel(channel).await.ok().flatten();
    let namespaces = channel_row
        .as_ref()
        .map(|c| c.context_namespaces.clone())
        .filter(|v| !v.is_empty())
        .unwrap_or_else(|| vec![channel.to_string()]);
    let mut merged: HashMap<String, ContextEntry> = HashMap::new();
    let mut outline_dir = project_dir(&st.cfg, channel);
    for ns in &namespaces {
        if let Ok(entries) = st.db.load_entries(Some(ns)).await {
            for e in entries {
                merged.insert(e.id.clone(), e);
            }
        }
        if channel_is_public(st, channel).await {
            if let Some(url) = master_url(&st.cfg) {
                if let Ok(env) = hub::query_namespace(url, ns).await {
                    for e in env.entries {
                        merged.entry(e.id.clone()).or_insert(e);
                    }
                }
            }
        }
        if outline_dir.is_none() {
            outline_dir = project_dir(&st.cfg, ns);
        }
        if let Some(dir) = project_dir(&st.cfg, ns) {
            let origin = st.cfg.node.id.clone();
            let ns = ns.clone();
            if let Ok(Ok(files)) =
                tokio::task::spawn_blocking(move || scan_workdir(&dir, &ns, &origin)).await
            {
                for e in files {
                    merged.entry(e.id.clone()).or_insert(e);
                }
            }
        }
        if let Some(repo) = st.cfg.git.watched_repos.iter().find(|r| &r.namespace == ns) {
            let path = repo.local_path.clone();
            let ns = ns.clone();
            let origin = st.cfg.node.id.clone();
            if let Ok(Ok(files)) =
                tokio::task::spawn_blocking(move || scan_workdir(Path::new(&path), &ns, &origin))
                    .await
            {
                for e in files {
                    merged.entry(e.id.clone()).or_insert(e);
                }
            }
        }
    }
    let entries: Vec<_> = merged.into_values().collect();
    let q_emb = st.models.embed_text(query).await.unwrap_or_default();
    let retrieved = assemble_context(&map_entries(&entries), query, &q_emb, 10, 64_000);
    let mut outline = String::new();
    if let Some(dir) = outline_dir {
        outline = project_outline(&st.cfg, dir).await;
    }
    Ok(format!(
        "{}\n\n{outline}\n\n{retrieved}",
        coding_preamble(channel)
    ))
}

fn number_harvest_code(text: &str) -> String {
    let mut out = String::new();
    let mut numbering = false;
    let mut n = 0usize;
    for line in text.lines() {
        if line.starts_with("### ") {
            numbering = line.starts_with("### [CODE:");
            n = 0;
            out.push_str(line);
            out.push('\n');
            continue;
        }
        if numbering {
            n += 1;
            out.push_str(&format!("{n:>5}|{line}\n"));
        } else {
            out.push_str(line);
            out.push('\n');
        }
    }
    out
}

async fn project_outline(cfg: &AppConfig, dir: std::path::PathBuf) -> String {
    if !cfg.git.harvest_command.is_empty() {
        let program = cfg.git.harvest_command[0].clone();
        let args: Vec<String> = cfg.git.harvest_command[1..].to_vec();
        let path = dir.clone();
        let harvested = tokio::process::Command::new(&program)
            .args(&args)
            .arg(&path)
            .output();
        if let Ok(Ok(out)) = tokio::time::timeout(std::time::Duration::from_secs(20), harvested).await
        {
            if out.status.success() {
                let text = String::from_utf8_lossy(&out.stdout).trim().to_string();
                if !text.is_empty() {
                    let numbered = number_harvest_code(&text);
                    return numbered.chars().take(28_000).collect();
                }
            }
        }
    }
    tokio::task::spawn_blocking(move || crate::git_sync::project_outline(&dir))
        .await
        .ok()
        .and_then(|r| r.ok())
        .unwrap_or_default()
}

async fn store_message(
    st: &WebState,
    msg: SessionMessage,
    sync_hub: bool,
    detail: &str,
) -> SessionMessage {
    let stored = if sync_hub {
        if let Some(url) = master_url(&st.cfg) {
            match hub::append_public(url, msg.clone(), &st.cfg.node.id, st.node_key.as_ref()).await {
                Ok(stored) => {
                    let _ = st.db.append_message(&stored).await;
                    stored
                }
                Err(err) => {
                    warn!("hub append failed, keeping the message locally: {err}");
                    if let Some(stored) = http_append(st, &msg).await {
                        let _ = st.db.append_message(&stored).await;
                        stored
                    } else {
                        local_append(st, msg).await
                    }
                }
            }
        } else if let Ok(stored) = st.db.append_public_message(&msg).await {
            let action = if crate::models::is_human_role(&stored.author) {
                "chat.request"
            } else {
                "chat.reply"
            };
            let _ = st
                .db
                .record_audit(
                    &st.cfg.node.id,
                    action,
                    &stored.channel_id,
                    &stored.thread_id,
                    &stored.content,
                )
                .await;
            stored
        } else {
            local_append(st, msg).await
        }
    } else {
        local_append(st, msg).await
    };
    let viewing = st
        .presence
        .lock()
        .ok()
        .map(|p| p.viewing && p.channel == stored.channel_id)
        .unwrap_or(false);
    if crate::models::is_human_role(&stored.author) || viewing {
        let _ = st.db.mark_read(&stored.channel_id, stored.seq as i64).await;
    }
    if !detail.is_empty() {
        let _ = st
            .db
            .save_message_detail(&stored.channel_id, &stored.thread_id, stored.seq, detail)
            .await;
    }
    if let Ok(Some(link)) = st.db.get_p2p(&stored.channel_id).await {
        push_one(&link, &stored, false, 0).await;
    }
    stored
}

async fn local_append(st: &WebState, mut msg: SessionMessage) -> SessionMessage {
    if msg.seq == 0 {
        msg.seq = st
            .db
            .next_seq(&msg.channel_id, &msg.thread_id)
            .await
            .unwrap_or(1);
    }
    let _ = st.db.append_message(&msg).await;
    msg
}

fn project_dir(cfg: &AppConfig, ns: &str) -> Option<std::path::PathBuf> {
    cfg.git.project_roots.iter().find_map(|root| {
        let path = std::path::PathBuf::from(&root.path).join(ns);
        path.is_dir().then_some(path)
    })
}

#[derive(Deserialize)]
struct CompleteReq {
    #[serde(default)]
    model: String,
    #[serde(default)]
    system: String,
    #[serde(default)]
    turns: Vec<ChatTurn>,
    #[serde(default)]
    audit: Option<AuditHint>,
}

#[derive(Clone, Deserialize)]
struct AuditHint {
    #[serde(default)]
    node_id: String,
    #[serde(default)]
    channel: String,
    #[serde(default)]
    thread: String,
}

async fn handle_complete(
    State(st): State<Arc<WebState>>,
    Json(req): Json<CompleteReq>,
) -> Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>> {
    if let Some(hint) = req.audit.clone() {
        let preview = req
            .turns
            .iter()
            .rev()
            .find(|t| t.role == "user")
            .map(|t| t.text.chars().take(400).collect::<String>())
            .unwrap_or_default();
        let detail = serde_json::json!({
            "model": req.model,
            "preview": preview
        })
        .to_string();
        let _ = st
            .db
            .record_audit(
                if hint.node_id.is_empty() {
                    &st.cfg.node.id
                } else {
                    &hint.node_id
                },
                "model.request",
                &hint.channel,
                &hint.thread,
                &detail,
            )
            .await;
    }
    let (tx, rx) = tokio::sync::mpsc::channel::<StreamEvent>(64);
    tokio::spawn(async move {
        if let Err(err) = st
            .models
            .stream_chat(&req.model, &req.system, &req.turns, tx.clone())
            .await
        {
            let _ = tx
                .send(StreamEvent::Delta {
                    text: format!("[ERROR] {err}"),
                    model: String::new(),
                })
                .await;
            let _ = tx
                .send(StreamEvent::Done {
                    model_used: "error".into(),
                    model: String::new(),
                })
                .await;
        }
    });
    let stream = ReceiverStream::new(rx)
        .map(|ev| Ok(Event::default().data(serde_json::to_string(&ev).unwrap_or_default())));
    Sse::new(stream).keep_alive(KeepAlive::default())
}

async fn dispatch_chat(
    st: &WebState,
    model: &str,
    system: &str,
    turns: &[ChatTurn],
    tx: tokio::sync::mpsc::Sender<StreamEvent>,
    audit: Option<serde_json::Value>,
) -> anyhow::Result<(String, String)> {
    if let Some(base) = master_http(&st.cfg) {
        let prefix = model
            .split_once('/')
            .map(|(p, rest)| if rest.is_empty() { "" } else { p })
            .unwrap_or("");
        let remote = if prefix.is_empty() {
            !st.models.has_local_providers().await
        } else {
            !st.models.owns_provider(prefix).await
        };
        if remote {
            return relay_complete(base, model, system, turns, tx, audit).await;
        }
    }
    st.models.stream_chat(model, system, turns, tx).await
}

async fn relay_complete(
    base: &str,
    model: &str,
    system: &str,
    turns: &[ChatTurn],
    tx: tokio::sync::mpsc::Sender<StreamEvent>,
    audit: Option<serde_json::Value>,
) -> anyhow::Result<(String, String)> {
    let client = reqwest::Client::builder()
        .connect_timeout(std::time::Duration::from_secs(10))
        .build()?;
    let url = format!("{}/api/internal/complete", base.trim_end_matches('/'));
    let mut resp = client
        .post(url)
        .json(&serde_json::json!({
            "model": model,
            "system": system,
            "turns": turns,
            "audit": audit
        }))
        .send()
        .await?;
    if !resp.status().is_success() {
        anyhow::bail!("hub model route status {}", resp.status());
    }
    let mut carry = Vec::new();
    let mut text = String::new();
    let mut used = String::new();
    let idle = std::time::Duration::from_secs(300);
    loop {
        let chunk = match tokio::time::timeout(idle, resp.chunk()).await {
            Ok(Ok(chunk)) => chunk,
            Ok(Err(err)) => anyhow::bail!("{err}"),
            Err(_) => anyhow::bail!("hub model route went quiet for {}s", idle.as_secs()),
        };
        let Some(chunk) = chunk else {
            break;
        };
        carry.extend_from_slice(&chunk);
        while let Some(pos) = carry.iter().position(|b| *b == b'\n') {
            let raw: Vec<u8> = carry.drain(..=pos).collect();
            let line = String::from_utf8_lossy(&raw);
            let Some(data) = line.trim().strip_prefix("data:") else {
                continue;
            };
            let Ok(ev) = serde_json::from_str::<StreamEvent>(data.trim()) else {
                continue;
            };
            match &ev {
                StreamEvent::Delta { text: delta, .. } => text.push_str(delta),
                StreamEvent::Done { model_used, .. } => used = model_used.clone(),
                _ => {}
            }
            let _ = tx.send(ev).await;
        }
    }
    if used.is_empty() {
        used = model.to_string();
    }
    Ok((text, used))
}

fn stamp(ev: StreamEvent, model: &str) -> StreamEvent {
    match ev {
        StreamEvent::Status { text, .. } => StreamEvent::Status {
            text,
            model: model.to_string(),
        },
        StreamEvent::Thought { text, .. } => StreamEvent::Thought {
            text,
            model: model.to_string(),
        },
        StreamEvent::Delta { text, .. } => StreamEvent::Delta {
            text,
            model: model.to_string(),
        },
        StreamEvent::Done { model_used, .. } => StreamEvent::Done {
            model_used,
            model: model.to_string(),
        },
        other => other,
    }
}

async fn run_one_reply(
    st: Arc<WebState>,
    model: String,
    ctx: String,
    turns: Vec<ChatTurn>,
    tx: tokio::sync::mpsc::Sender<StreamEvent>,
    audit: Option<serde_json::Value>,
    channel: String,
    thread: String,
    reply_thread: String,
    sync_hub: bool,
    left: Arc<AtomicUsize>,
    send_saved: bool,
) {
    let (local_tx, mut local_rx) = tokio::sync::mpsc::channel::<StreamEvent>(32);
    let forward = tx.clone();
    let label = model.clone();
    let pump = tokio::spawn(async move {
        while let Some(ev) = local_rx.recv().await {
            let _ = forward.send(stamp(ev, &label)).await;
        }
    });
    let started = now_ms();
    let (author, final_text) = match dispatch_chat(&st, &model, &ctx, &turns, local_tx.clone(), audit).await
    {
        Ok((reply, used)) => {
            let (clean, tools) = crate::browse::take_requests(&reply);
            if tools.is_empty() || used == "error" {
                (used, reply)
            } else {
                let _ = local_tx
                    .send(StreamEvent::Status {
                        text: "Fetching the web...".into(),
                        model: String::new(),
                    })
                    .await;
                let pages = run_lookups(&st, &tools).await;
                let mut next = turns.clone();
                next.push(ChatTurn {
                    role: "assistant".into(),
                    text: clean.clone(),
                    attachments: vec![],
                });
                next.push(ChatTurn {
                    role: "user".into(),
                    text: format!("Web results:\n{pages}"),
                    attachments: vec![],
                });
                match dispatch_chat(&st, &model, &ctx, &next, local_tx.clone(), None).await {
                    Ok((second, used2)) => (used2, second),
                    Err(err) => (used, format!("{clean}\n\n[web lookup failed: {err:#}]")),
                }
            }
        }
        Err(err) => {
            let err_txt = format!("[ERROR] {err:#}");
            let _ = local_tx
                .send(StreamEvent::Delta {
                    text: err_txt.clone(),
                    model: String::new(),
                })
                .await;
            ("error".into(), err_txt)
        }
    };
    let author = if author.is_empty() { model.clone() } else { author };
    let finished = now_ms();
    let detail = serde_json::json!({
        "started_at_ms": started,
        "finished_at_ms": finished,
        "model": author,
        "raw": final_text,
        "processed": final_text,
        "error": if author == "error" { final_text.as_str() } else { "" },
        "actions": ["chat"]
    })
    .to_string();
    let stored = store_message(
        &st,
        SessionMessage {
            channel_id: channel,
            thread_id: thread,
            seq: 0,
            author: author.clone(),
            content: final_text,
            created_at_ms: 0,
        },
        sync_hub,
        &detail,
    )
    .await;
    drop(local_tx);
    let _ = pump.await;
    let last = left.fetch_sub(1, Ordering::SeqCst) == 1;
    if send_saved {
        let _ = tx
            .send(StreamEvent::Saved {
                seq: stored.seq,
                thread: reply_thread,
                model: author,
                last,
            })
            .await;
    }
}

async fn handle_persistent_chat(
    State(st): State<Arc<WebState>>,
    Json(req): Json<ChatReq>,
) -> Result<Sse<impl tokio_stream::Stream<Item = Result<Event, Infallible>>>, StatusCode> {
    let past = st
        .db
        .load_thread(&req.channel, &req.thread, 100)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let visibility = channel_visibility(&st, &req.channel).await;
    let public = visibility == "public" || visibility == "shared";
    let private_thread = st
        .db
        .thread_visibility(&req.channel, &req.thread)
        .await
        .unwrap_or_default()
        == "private";
    let sync_hub = public && !private_thread;

    let att_note = if req.attachments.is_empty() {
        String::new()
    } else {
        format!(
            "\n[Attached: {}]",
            req.attachments
                .iter()
                .map(|a| a.name.as_str())
                .collect::<Vec<_>>()
                .join(", ")
        )
    };
    let who = display_name(&st).await;
    let user_msg = if req.rerun {
        None
    } else {
        Some(SessionMessage {
            channel_id: req.channel.clone(),
            thread_id: req.thread.clone(),
            seq: 0,
            author: who.clone(),
            content: format!("{}{}", req.prompt, att_note),
            created_at_ms: 0,
        })
    };

    let mut turns: Vec<ChatTurn> = past
        .into_iter()
        .map(|m| ChatTurn {
            role: m.author,
            text: m.content,
            attachments: vec![],
        })
        .collect();
    if !req.rerun {
        turns.push(ChatTurn {
            role: who,
            text: req.prompt.clone(),
            attachments: req.attachments,
        });
    }

    let mut models: Vec<String> = req
        .models
        .iter()
        .map(|m| m.trim().to_string())
        .filter(|m| !m.is_empty())
        .collect();
    if models.is_empty() {
        if let Some(one) = req
            .model
            .as_deref()
            .map(str::trim)
            .filter(|s| !s.is_empty())
        {
            models.push(one.to_string());
        }
    }
    if models.is_empty() {
        let fallback = st.models.default_model().await;
        if !fallback.is_empty() {
            models.push(fallback);
        }
    }
    {
        let mut seen = HashSet::new();
        models.retain(|m| seen.insert(m.clone()));
    }
    let _ = st
        .db
        .set_chat_models(&req.channel, &req.thread, &models, models.len() > 1)
        .await;

    let (tx, rx) = tokio::sync::mpsc::channel::<StreamEvent>(64);
    let st_bg = st.clone();
    let models_bg = models;
    tokio::spawn(async move {
        let _clear = LiveClear {
            st: st_bg.clone(),
            channel: req.channel.clone(),
        };
        set_live(&st_bg, &req.channel, "reply", now_ms() + 600_000);
        let mut stored_user = None;
        if let Some(user_msg) = user_msg {
            let _ = tx
                .send(StreamEvent::Status {
                    text: "Saving the message...".into(),
                    model: String::new(),
                })
                .await;
            stored_user = Some(store_message(&st_bg, user_msg, sync_hub, "").await);
        }
        let reply_thread = if visibility == "public" && req.thread == "main" {
            if let Some(parent) = stored_user.as_ref() {
                Some(open_reply_thread(&st_bg, &req.channel, parent, sync_hub).await)
            } else {
                None
            }
        } else {
            None
        };
        let model_thread = reply_thread.clone().unwrap_or_else(|| req.thread.clone());
        if reply_thread.is_some() {
            if let Some(last) = turns.pop() {
                turns = vec![last];
            }
            let _ = st_bg
                .db
                .set_chat_models(
                    &req.channel,
                    model_thread.as_str(),
                    &models_bg,
                    models_bg.len() > 1,
                )
                .await;
        }
        let _ = tx
            .send(StreamEvent::Status {
                text: "Building context...".into(),
                model: String::new(),
            })
            .await;
        let prompt_model = models_bg
            .iter()
            .find(|m| !m.starts_with("google/"))
            .cloned()
            .unwrap_or_else(|| models_bg.first().cloned().unwrap_or_default());
        let mut ctx = match tokio::time::timeout(
            std::time::Duration::from_secs(45),
            prompt_for(
                &st_bg,
                &req.channel,
                &model_thread,
                &req.prompt,
                &prompt_model,
            ),
        )
        .await
        {
            Ok(Ok(text)) => text,
            _ => "Project context was not ready in time. Answer from the conversation.".into(),
        };
        let pages = prefetch_pages(&st_bg, &req.prompt).await;
        if !pages.is_empty() {
            ctx.push_str("\n\n## Web results\n");
            ctx.push_str(&pages);
        }
        let audit = if sync_hub {
            Some(serde_json::json!({
                "node_id": st_bg.cfg.node.id,
                "channel": req.channel,
                "thread": model_thread.clone()
            }))
        } else {
            None
        };
        if models_bg.is_empty() {
            let _ = tx
                .send(StreamEvent::Delta {
                    text: "[ERROR] no model is configured".into(),
                    model: String::new(),
                })
                .await;
            let _ = tx
                .send(StreamEvent::Saved {
                    seq: 0,
                    thread: reply_thread.unwrap_or_default(),
                    model: "error".into(),
                    last: true,
                })
                .await;
            return;
        }
        if models_bg.len() > 1 {
            let _ = tx
                .send(StreamEvent::Status {
                    text: format!(
                        "Asking {} models. Each reply stands alone, and they may disagree.",
                        models_bg.len()
                    ),
                    model: String::new(),
                })
                .await;
        }
        let left = Arc::new(AtomicUsize::new(models_bg.len()));
        let max_rounds = if req.debate && models_bg.len() > 1 {
            st_bg.cfg.models.debate_max_rounds.min(6)
        } else {
            0
        };
        let has_debate = max_rounds > 0;
        let reply_name = reply_thread.unwrap_or_default();
        let mut pending = tokio::task::JoinSet::new();
        for model in &models_bg {
            pending.spawn(run_one_reply(
                st_bg.clone(),
                model.clone(),
                ctx.clone(),
                turns.clone(),
                tx.clone(),
                audit.clone(),
                req.channel.clone(),
                model_thread.clone(),
                reply_name.clone(),
                sync_hub,
                left.clone(),
                !has_debate,
            ));
        }
        while pending.join_next().await.is_some() {}

        if has_debate {
            'debate: for round in 1..=max_rounds {
                for (idx, model) in models_bg.iter().enumerate() {
                    let is_final_turn = round == max_rounds && idx + 1 == models_bg.len();
                    let _ = tx
                        .send(StreamEvent::Status {
                            text: format!("Debate round {round}/{max_rounds}: {model} reviewing..."),
                            model: model.clone(),
                        })
                        .await;
                    let history = st_bg
                        .db
                        .load_thread(&req.channel, &model_thread, 100)
                        .await
                        .unwrap_or_default();
                    let mut debate_turns: Vec<ChatTurn> = history
                        .into_iter()
                        .map(|m| ChatTurn {
                            role: m.author,
                            text: m.content,
                            attachments: vec![],
                        })
                        .collect();
                    debate_turns.push(ChatTurn {
                        role: "system".into(),
                        text: format!(
                            "You are participating in a peer code/architecture review (round {round} of {max_rounds}) as `{model}`.\n\
                             Critically examine the previous responses from other models in this thread.\n\
                             Point out concrete bugs, edge cases, race conditions, or flawed assumptions.\n\
                             If all issues are resolved and you fully agree on the final solution, start your reply with `CONSENSUS:` and provide the clean unified ```diff.\n\
                             Otherwise, explain the flaws and propose an improved solution or ```diff."
                        ),
                        attachments: vec![],
                    });
                    let one_left = Arc::new(AtomicUsize::new(1));
                    run_one_reply(
                        st_bg.clone(),
                        model.clone(),
                        ctx.clone(),
                        debate_turns,
                        tx.clone(),
                        audit.clone(),
                        req.channel.clone(),
                        model_thread.clone(),
                        reply_name.clone(),
                        sync_hub,
                        one_left,
                        false,
                    )
                    .await;
                    let latest = st_bg
                        .db
                        .load_thread(&req.channel, &model_thread, 100)
                        .await
                        .unwrap_or_default();
                    let agreed = latest
                        .last()
                        .map(|m| m.content.trim_start().starts_with("CONSENSUS:"))
                        .unwrap_or(false);
                    if agreed || is_final_turn {
                        let last_seq = latest.last().map(|m| m.seq).unwrap_or(0);
                        let _ = tx
                            .send(StreamEvent::Saved {
                                seq: last_seq,
                                thread: reply_name.clone(),
                                model: model.clone(),
                                last: true,
                            })
                            .await;
                        break 'debate;
                    }
                }
            }
        }
    });

    let stream = ReceiverStream::new(rx)
        .map(|ev| Ok(Event::default().data(serde_json::to_string(&ev).unwrap_or_default())));
    Ok(Sse::new(stream).keep_alive(KeepAlive::default()))
}

#[derive(Serialize, Deserialize)]
struct NodeQuery {
    #[serde(default)]
    node_id: String,
    #[serde(default)]
    channel: String,
    #[serde(default)]
    thread: String,
    #[serde(default)]
    token: String,
}

#[derive(Deserialize)]
struct PrefBody {
    name: String,
    #[serde(default)]
    pinned: Option<bool>,
    #[serde(default)]
    muted: Option<bool>,
    #[serde(default)]
    archived: Option<bool>,
    #[serde(default)]
    closed: Option<bool>,
    #[serde(default)]
    mark_read: bool,
    #[serde(default)]
    bump_use: bool,
}

#[derive(Deserialize)]
struct PresenceBody {
    channel: String,
    #[serde(default)]
    viewing: bool,
}

#[derive(Serialize, Deserialize)]
struct ShareMsg {
    thread_id: String,
    seq: u64,
    author: String,
    content: String,
    created_at_ms: i64,
    #[serde(default)]
    deleted: bool,
    #[serde(default)]
    edited_at_ms: i64,
}

#[derive(Serialize, Deserialize)]
struct ShareEntry {
    id: String,
    namespace: String,
    source_path: String,
    content: String,
    content_hash: String,
    entry_type: i32,
}

#[derive(Serialize, Deserialize)]
struct ShareBody {
    channel: String,
    #[serde(default)]
    origin_node: String,
    #[serde(default)]
    members: Vec<String>,
    #[serde(default)]
    messages: Vec<ShareMsg>,
    #[serde(default)]
    entries: Vec<ShareEntry>,
    #[serde(default)]
    token_hash: String,
}

#[derive(Serialize, Deserialize)]
struct AuditBody {
    #[serde(default)]
    node_id: String,
    action: String,
    #[serde(default)]
    channel: String,
    #[serde(default)]
    thread: String,
    #[serde(default)]
    detail: String,
}

fn authorize(st: &WebState, headers: &HeaderMap, body: &[u8]) -> Result<String, StatusCode> {
    let node = headers
        .get("x-xmesh-node")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("")
        .to_string();
    let ts = headers
        .get("x-xmesh-ts")
        .and_then(|value| value.to_str().ok())
        .and_then(|text| text.parse().ok())
        .unwrap_or(0);
    let sig = headers
        .get("x-xmesh-sig")
        .and_then(|value| value.to_str().ok())
        .unwrap_or("");
    crate::identity::resolve_peer(&st.peer_keys, &node, ts, sig, body).map_err(|_| StatusCode::FORBIDDEN)
}

async fn signed_post(
    st: &WebState,
    url: &str,
    kind: &str,
    parts: &[&str],
    payload: &impl Serialize,
) -> Result<reqwest::Response, StatusCode> {
    let body = serde_json::to_vec(payload).map_err(|_| StatusCode::BAD_REQUEST)?;
    let canon = crate::identity::canonical(kind, parts);
    let ts = now_ms();
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(60))
        .build()
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    let mut req = client
        .post(url)
        .header("content-type", "application/json")
        .header("x-xmesh-node", &st.cfg.node.id)
        .body(body);
    if let Some(key) = &st.node_key {
        let sig = crate::identity::sign(key, &st.cfg.node.id, ts, &canon);
        req = req
            .header("x-xmesh-ts", ts.to_string())
            .header("x-xmesh-sig", sig);
    }
    req.send().await.map_err(|_| StatusCode::BAD_GATEWAY)
}

async fn publish_audit(st: &WebState, action: &str, channel: &str, thread: &str, detail: &str) {
    if let Some(base) = master_http(&st.cfg) {
        let payload = AuditBody {
            node_id: st.cfg.node.id.clone(),
            action: action.to_string(),
            channel: channel.to_string(),
            thread: thread.to_string(),
            detail: detail.to_string(),
        };
        let _ = signed_post(
            st,
            &format!("{}/api/audit", base.trim_end_matches('/')),
            "audit",
            &[action, channel, thread, detail],
            &payload,
        )
        .await;
        return;
    }
    let _ = st
        .db
        .record_audit(&st.cfg.node.id, action, channel, thread, detail)
        .await;
}

async fn handle_channel_state(
    State(st): State<Arc<WebState>>,
    Json(body): Json<PrefBody>,
) -> Result<StatusCode, StatusCode> {
    if storage_name(&body.name).is_err() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let mut pref = st
        .db
        .get_pref(&body.name)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    if let Some(v) = body.pinned {
        pref.pinned = v;
    }
    if let Some(v) = body.muted {
        pref.muted = v;
    }
    if body.name != "general" {
        if let Some(v) = body.archived {
            pref.archived = v;
        }
        if let Some(v) = body.closed {
            pref.closed = v;
        }
    }
    if body.bump_use {
        pref.use_count = pref.use_count.saturating_add(1);
    }
    if body.mark_read {
        let seq = high_water(&st, &body.name).await;
        pref.last_read_seq = pref.last_read_seq.max(seq);
    }
    st.db
        .save_pref(&pref)
        .await
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    Ok(StatusCode::OK)
}

fn storage_name(name: &str) -> Result<(), ()> {
    crate::storage::validate_channel_name(name).map_err(|_| ())
}

async fn handle_presence(
    State(st): State<Arc<WebState>>,
    Json(body): Json<PresenceBody>,
) -> StatusCode {
    if let Ok(mut presence) = st.presence.lock() {
        presence.channel = body.channel;
        presence.viewing = body.viewing;
    }
    StatusCode::OK
}

async fn handle_audit(
    State(st): State<Arc<WebState>>,
) -> Result<Json<Vec<AuditEvent>>, StatusCode> {
    if let Some(base) = master_http(&st.cfg) {
        let client = reqwest::Client::builder()
            .timeout(std::time::Duration::from_secs(8))
            .build()
            .map_err(|_| StatusCode::BAD_GATEWAY)?;
        let rows = client
            .get(format!("{}/api/audit", base.trim_end_matches('/')))
            .send()
            .await
            .map_err(|_| StatusCode::BAD_GATEWAY)?
            .json()
            .await
            .map_err(|_| StatusCode::BAD_GATEWAY)?;
        return Ok(Json(rows));
    }
    st.db
        .list_audit(80)
        .await
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn handle_audit_post(
    State(st): State<Arc<WebState>>,
    headers: HeaderMap,
    Json(body): Json<AuditBody>,
) -> Result<StatusCode, StatusCode> {
    if master_http(&st.cfg).is_some() {
        return Err(StatusCode::FORBIDDEN);
    }
    let canon = crate::identity::canonical(
        "audit",
        &[&body.action, &body.channel, &body.thread, &body.detail],
    );
    let verified = authorize(&st, &headers, &canon)?;
    let node = if st.peer_keys.is_empty() {
        if body.node_id.is_empty() {
            st.cfg.node.id.clone()
        } else {
            body.node_id
        }
    } else {
        verified
    };
    st.db
        .record_audit(
            &node,
            &body.action,
            &body.channel,
            &body.thread,
            &body.detail,
        )
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(StatusCode::OK)
}

async fn handle_mesh_channels(
    State(st): State<Arc<WebState>>,
    Query(q): Query<NodeQuery>,
) -> Result<Json<Vec<ChannelRow>>, StatusCode> {
    st.db
        .channels_visible_to(&q.node_id)
        .await
        .map(Json)
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)
}

async fn handle_mesh_activity(
    State(st): State<Arc<WebState>>,
    Query(q): Query<NodeQuery>,
) -> Result<Json<std::collections::HashMap<String, i64>>, StatusCode> {
    let visible = st
        .db
        .channels_visible_to(&q.node_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let allowed: std::collections::HashSet<_> = visible.into_iter().map(|c| c.name).collect();
    let rows = st
        .db
        .activity()
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(
        rows.into_iter()
            .filter(|(name, _)| allowed.contains(name))
            .collect(),
    ))
}

async fn handle_mesh_history(
    State(st): State<Arc<WebState>>,
    Query(q): Query<NodeQuery>,
) -> Result<Json<Vec<ShareMsg>>, StatusCode> {
    let Some(ch) = st
        .db
        .get_channel(&q.channel)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(StatusCode::NOT_FOUND);
    };
    if ch.visibility == "private" {
        return Err(StatusCode::FORBIDDEN);
    }
    if ch.visibility == "shared" {
        let member = st
            .db
            .member_state(&q.channel, &q.node_id)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        if member.is_none() {
            return Err(StatusCode::FORBIDDEN);
        }
    }
    let thread = if q.thread.is_empty() {
        "main"
    } else {
        &q.thread
    };
    let msgs = st
        .db
        .load_sync(&q.channel, thread, 400)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(
        msgs.into_iter()
            .map(|m| ShareMsg {
                thread_id: m.message.thread_id,
                seq: m.message.seq,
                author: m.message.author,
                content: m.message.content,
                created_at_ms: m.message.created_at_ms,
                deleted: m.deleted,
                edited_at_ms: m.edited_at_ms,
            })
            .collect(),
    ))
}

async fn fetch_shared_history(
    base: &str,
    node_id: &str,
    channel: &str,
    thread: &str,
) -> Option<Vec<ShareMsg>> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(8))
        .build()
        .ok()?;
    let msgs: Vec<ShareMsg> = client
        .get(format!("{}/api/mesh/history", base.trim_end_matches('/')))
        .query(&[
            ("node_id", node_id),
            ("channel", channel),
            ("thread", thread),
        ])
        .send()
        .await
        .ok()?
        .json()
        .await
        .ok()?;
    Some(msgs)
}

async fn apply_share(st: &WebState, body: ShareBody) -> Result<(), StatusCode> {
    if storage_name(&body.channel).is_err() || body.channel == "general" {
        return Err(StatusCode::BAD_REQUEST);
    }
    st.db
        .upsert_channel(&ChannelRow {
            name: body.channel.clone(),
            visibility: "shared".into(),
            namespace: body.channel.clone(),
            origin_node: body.origin_node.clone(),
            context_namespaces: vec![body.channel.clone()],
            updated_at_ms: 0,
        })
        .await
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let mut members = Vec::new();
    let mut seen = std::collections::HashSet::new();
    for node in std::iter::once(body.origin_node.clone()).chain(body.members.into_iter()) {
        if node.is_empty() || !seen.insert(node.clone()) {
            continue;
        }
        let role = if node == body.origin_node {
            "owner"
        } else {
            "member"
        };
        members.push((node, role.to_string(), "active".to_string()));
    }
    st.db
        .set_members(&body.channel, &members)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    for msg in body.messages.into_iter().take(400) {
        let _ = st
            .db
            .append_message(&SessionMessage {
                channel_id: body.channel.clone(),
                thread_id: msg.thread_id,
                seq: msg.seq,
                author: msg.author,
                content: msg.content,
                created_at_ms: msg.created_at_ms,
            })
            .await;
    }
    for entry in body.entries.into_iter().take(200) {
        let _ = st
            .db
            .upsert_proto(
                &ContextEntry {
                    id: entry.id,
                    namespace: entry.namespace,
                    entry_type: entry.entry_type,
                    source_path: entry.source_path,
                    content: entry.content,
                    content_hash: entry.content_hash,
                    embedding: vec![],
                    updated_at_ms: now_ms(),
                    origin_node: body.origin_node.clone(),
                    meta: Default::default(),
                },
                false,
            )
            .await;
    }
    let _ = st
        .db
        .record_audit(
            &body.origin_node,
            "channel.share",
            &body.channel,
            "main",
            &members
                .iter()
                .map(|(n, _, _)| n.as_str())
                .collect::<Vec<_>>()
                .join(","),
        )
        .await;
    if !body.token_hash.is_empty() {
        let _ = st.db.put_invite(&body.channel, &body.token_hash).await;
    }
    Ok(())
}

async fn handle_mesh_share(
    State(st): State<Arc<WebState>>,
    headers: HeaderMap,
    Json(mut body): Json<ShareBody>,
) -> Result<StatusCode, StatusCode> {
    let listed = body.members.join(",");
    let canon = crate::identity::canonical(
        "share",
        &[&body.channel, &body.origin_node, &listed, &body.token_hash],
    );
    if !st.peer_keys.is_empty() {
        body.origin_node = authorize(&st, &headers, &canon)?;
    }
    apply_share(&st, body).await?;
    Ok(StatusCode::OK)
}

async fn apply_join(st: &WebState, node_id: &str, channel: &str, token: &str) -> Result<(), StatusCode> {
    let member = st
        .db
        .member_state(channel, node_id)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    if member.is_none() {
        let hash = blake3::hash(token.as_bytes()).to_hex().to_string();
        let invited = !token.is_empty()
            && st
                .db
                .invite_matches(channel, &hash)
                .await
                .unwrap_or(false);
        if !invited {
            return Err(StatusCode::FORBIDDEN);
        }
    }
    let ch = st
        .db
        .get_channel(channel)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;
    let mut names = st
        .db
        .list_members(channel)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    if !names.iter().any(|name| name == node_id) {
        names.push(node_id.to_string());
    }
    let members = names
        .into_iter()
        .map(|name| {
            let role = if name == ch.origin_node {
                "owner"
            } else {
                "member"
            };
            (name, role.to_string(), "active".to_string())
        })
        .collect::<Vec<_>>();
    st.db
        .set_members(channel, &members)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let _ = st
        .db
        .record_audit(node_id, "channel.join", channel, "main", "")
        .await;
    Ok(())
}

async fn handle_mesh_join(
    State(st): State<Arc<WebState>>,
    headers: HeaderMap,
    Json(body): Json<NodeQuery>,
) -> Result<StatusCode, StatusCode> {
    let canon = crate::identity::canonical("join", &[&body.channel, &body.node_id, &body.token]);
    let node = if st.peer_keys.is_empty() {
        body.node_id.clone()
    } else {
        authorize(&st, &headers, &canon)?
    };
    apply_join(&st, &node, &body.channel, &body.token).await?;
    Ok(StatusCode::OK)
}

async fn handle_join(
    State(st): State<Arc<WebState>>,
    Json(body): Json<NodeQuery>,
) -> Result<StatusCode, StatusCode> {
    if let Some(base) = master_http(&st.cfg) {
        let payload = NodeQuery {
            node_id: st.cfg.node.id.clone(),
            channel: body.channel.clone(),
            thread: String::new(),
            token: body.token.clone(),
        };
        let resp = signed_post(
            &st,
            &format!("{}/api/mesh/join", base.trim_end_matches('/')),
            "join",
            &[&payload.channel, &payload.node_id, &payload.token],
            &payload,
        )
        .await?;
        return if resp.status().is_success() {
            Ok(StatusCode::OK)
        } else if resp.status() == StatusCode::FORBIDDEN {
            Err(StatusCode::FORBIDDEN)
        } else {
            Err(StatusCode::BAD_GATEWAY)
        };
    }
    apply_join(&st, &st.cfg.node.id, &body.channel, &body.token).await?;
    Ok(StatusCode::OK)
}

#[derive(Deserialize)]
struct InspectBody {
    #[serde(default)]
    diff: String,
}

async fn handle_inspect(
    State(st): State<Arc<WebState>>,
    Json(body): Json<InspectBody>,
) -> Json<serde_json::Value> {
    let mut findings = crate::executor::inspect_diff(&body.diff);
    if let Some(bin) = st.cfg.git.inspect_command.first() {
        let mut cmd = tokio::process::Command::new(bin);
        cmd.args(st.cfg.git.inspect_command.iter().skip(1));
        cmd.stdin(std::process::Stdio::piped());
        cmd.stdout(std::process::Stdio::piped());
        cmd.stderr(std::process::Stdio::piped());
        match cmd.spawn() {
            Ok(mut child) => {
                if let Some(mut stdin) = child.stdin.take() {
                    use tokio::io::AsyncWriteExt;
                    let _ = stdin.write_all(body.diff.as_bytes()).await;
                }
                match tokio::time::timeout(
                    std::time::Duration::from_secs(20),
                    child.wait_with_output(),
                )
                .await
                {
                    Ok(Ok(out)) => {
                        for line in String::from_utf8_lossy(&out.stdout).lines().take(20) {
                            if !line.trim().is_empty() {
                                findings.push(line.trim().to_string());
                            }
                        }
                        if !out.status.success() {
                            findings.push("external inspector reported a problem".into());
                        }
                    }
                    _ => findings.push("external inspector timed out".into()),
                }
            }
            Err(_) => findings.push("external inspector could not start".into()),
        }
    }
    Json(serde_json::json!({ "findings": findings }))
}

async fn handle_share(
    State(st): State<Arc<WebState>>,
    Json(body): Json<ShareBody>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let origin = st.cfg.node.id.clone();
    let channel = body.channel.clone();
    let mut members = body.members;
    let messages = if body.messages.is_empty() {
        st.db
            .load_channel_messages(&channel)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
            .into_iter()
            .take(400)
            .map(|m| ShareMsg {
                thread_id: m.thread_id,
                seq: m.seq,
                author: m.author,
                content: m.content,
                created_at_ms: m.created_at_ms,
                deleted: false,
                edited_at_ms: 0,
            })
            .collect()
    } else {
        body.messages
    };
    let entries = if body.entries.is_empty() {
        snapshot_entries(&st, &channel).await
    } else {
        body.entries
    };
    let payload = ShareBody {
        channel: channel.clone(),
        origin_node: origin,
        members: members.split_off(0),
        messages,
        entries,
        token_hash: String::new(),
    };
    let (token, token_hash) = crate::identity::new_token();
    let mut payload = payload;
    payload.token_hash = token_hash.clone();
    let listed = payload.members.join(",");
    if let Some(base) = master_http(&st.cfg) {
        let resp = signed_post(
            &st,
            &format!("{}/api/mesh/share", base.trim_end_matches('/')),
            "share",
            &[&payload.channel, &payload.origin_node, &listed, &token_hash],
            &payload,
        )
        .await?;
        if resp.status() == StatusCode::FORBIDDEN {
            return Err(StatusCode::FORBIDDEN);
        }
        if !resp.status().is_success() {
            warn!(status = %resp.status(), "hub rejected the share");
            return Err(StatusCode::BAD_GATEWAY);
        }
    }
    apply_share(&st, payload).await?;
    Ok(Json(serde_json::json!({ "token": token })))
}

#[derive(Deserialize)]
struct MsgBody {
    channel: String,
    #[serde(default)]
    thread: String,
    #[serde(default)]
    seq: u64,
    #[serde(default)]
    text: String,
    #[serde(default)]
    mode: String,
    #[serde(default)]
    name: String,
}

async fn handle_permalink(
    State(st): State<Arc<WebState>>,
    Json(body): Json<MsgBody>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let vis = channel_visibility(&st, &body.channel).await;
    if vis == "public" {
        return Ok(Json(serde_json::json!({ "public": true, "token": "" })));
    }
    let (token, hash) = crate::identity::new_token();
    st.db
        .add_invite(&body.channel, &hash)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(serde_json::json!({ "public": false, "token": token })))
}

async fn handle_message_debug(
    State(st): State<Arc<WebState>>,
    Query(q): Query<MsgBody>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let thread = if q.thread.is_empty() { "main" } else { &q.thread };
    st.db
        .message_record(&q.channel, thread, q.seq)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .map(Json)
        .ok_or(StatusCode::NOT_FOUND)
}

async fn handle_message_delete(
    State(st): State<Arc<WebState>>,
    Json(body): Json<MsgBody>,
) -> Result<StatusCode, StatusCode> {
    let thread = if body.thread.is_empty() {
        "main".into()
    } else {
        body.thread
    };
    st.db
        .set_message_deleted(&body.channel, &thread, body.seq, true)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    publish_delete(&st, &body.channel, &thread, body.seq).await;
    Ok(StatusCode::OK)
}

async fn handle_message_edit(
    State(st): State<Arc<WebState>>,
    Json(body): Json<MsgBody>,
) -> Result<StatusCode, StatusCode> {
    let thread = if body.thread.is_empty() {
        "main".into()
    } else {
        body.thread.clone()
    };
    let record = st
        .db
        .message_record(&body.channel, &thread, body.seq)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;
    let _ = record;
    st.db
        .rewrite_message(&body.channel, &thread, body.seq, &body.text)
        .await
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    let edited_at_ms = now_ms();
    let detail = serde_json::json!({ "edited_at_ms": edited_at_ms, "actions": ["edit"] }).to_string();
    let _ = st
        .db
        .save_message_detail(&body.channel, &thread, body.seq, &detail)
        .await;
    publish_edit(&st, &body.channel, &thread, body.seq, &body.text, edited_at_ms).await;
    Ok(StatusCode::OK)
}

async fn handle_clone_channel(
    State(st): State<Arc<WebState>>,
    Json(body): Json<MsgBody>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let thread = if body.thread.is_empty() {
        "main".into()
    } else {
        body.thread.clone()
    };
    let name = body.name.trim();
    if storage_name(name).is_err() || name == "general" {
        return Err(StatusCode::BAD_REQUEST);
    }
    let source = match st
        .db
        .get_channel(&body.channel)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    {
        Some(row) => row,
        None => ChannelRow {
            name: body.channel.clone(),
            visibility: channel_visibility(&st, &body.channel).await,
            namespace: body.channel.clone(),
            origin_node: st.cfg.node.id.clone(),
            context_namespaces: vec![body.channel.clone()],
            updated_at_ms: 0,
        },
    };
    let msgs = st
        .db
        .load_thread(&body.channel, &thread, 500)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    let kept: Vec<_> = msgs
        .into_iter()
        .filter(|msg| {
            if body.mode == "until" {
                msg.seq <= body.seq
            } else {
                msg.seq >= body.seq
            }
        })
        .collect();
    let mut namespaces = source.context_namespaces.clone();
    if !namespaces.iter().any(|ns| ns == &body.channel) {
        namespaces.insert(0, body.channel.clone());
    }
    if !namespaces.iter().any(|ns| ns == name) {
        namespaces.insert(0, name.to_string());
    }
    let visibility = if source.visibility == "shared" {
        "shared"
    } else {
        "private"
    };
    st.db
        .upsert_channel(&ChannelRow {
            name: name.to_string(),
            visibility: visibility.into(),
            namespace: name.to_string(),
            origin_node: st.cfg.node.id.clone(),
            context_namespaces: namespaces,
            updated_at_ms: 0,
        })
        .await
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    if visibility == "shared" {
        let members = st
            .db
            .list_members(&body.channel)
            .await
            .unwrap_or_default();
        let rows = members
            .into_iter()
            .map(|node| (node, "member".into(), "active".into()))
            .collect::<Vec<_>>();
        let _ = st.db.set_members(name, &rows).await;
    }
    for (idx, msg) in kept.iter().enumerate() {
        let _ = st
            .db
            .append_message(&SessionMessage {
                channel_id: name.to_string(),
                thread_id: "main".into(),
                seq: (idx as u64) + 1,
                author: msg.author.clone(),
                content: msg.content.clone(),
                created_at_ms: msg.created_at_ms,
            })
            .await;
    }
    Ok(Json(serde_json::json!({
        "channel": name,
        "thread": "main",
        "seq": 1
    })))
}

pub async fn publish_shared_outlines(st: Arc<WebState>) {
    let Ok(rows) = st.db.list_channel_rows().await else {
        return;
    };
    for row in rows {
        if row.visibility != "shared" && row.visibility != "public" {
            continue;
        }
        let entries = snapshot_entries(&st, &row.name).await;
        if entries.is_empty() {
            continue;
        }
        push_entries(&st, &row.name, &entries).await;
    }
}

async fn push_entries(st: &WebState, channel: &str, entries: &[ShareEntry]) {
    let Some(base) = master_http(&st.cfg) else {
        for entry in entries {
            let _ = st
                .db
                .upsert_proto(&entry_proto(st, entry), false)
                .await;
        }
        return;
    };
    let _ = signed_post(
        st,
        &format!(
            "{}/api/mesh/context?channel={}",
            base.trim_end_matches('/'),
            urlencoding(channel)
        ),
        "context",
        &[channel],
        &entries,
    )
    .await;
}

fn entry_proto(st: &WebState, entry: &ShareEntry) -> ContextEntry {
    ContextEntry {
        id: entry.id.clone(),
        namespace: entry.namespace.clone(),
        entry_type: entry.entry_type,
        source_path: entry.source_path.clone(),
        content: entry.content.clone(),
        content_hash: entry.content_hash.clone(),
        embedding: vec![],
        updated_at_ms: now_ms(),
        origin_node: st.cfg.node.id.clone(),
        meta: Default::default(),
    }
}

fn set_live(st: &WebState, channel: &str, kind: &str, until: i64) {
    if let Ok(mut map) = st.live.lock() {
        map.insert(channel.to_string(), (kind.to_string(), until));
    }
}

fn clear_live(st: &WebState, channel: &str) {
    if let Ok(mut map) = st.live.lock() {
        map.remove(channel);
    }
}

struct LiveClear {
    st: Arc<WebState>,
    channel: String,
}

impl Drop for LiveClear {
    fn drop(&mut self) {
        clear_live(&self.st, &self.channel);
    }
}

async fn display_name(st: &WebState) -> String {
    let alias = st.db.get_setting("alias").await.unwrap_or_default();
    let alias = clean_alias(&alias);
    if alias.is_empty() {
        st.cfg.node.id.clone()
    } else {
        alias
    }
}

fn clean_alias(raw: &str) -> String {
    raw.trim()
        .chars()
        .filter(|c| *c != '/' && !c.is_control())
        .take(40)
        .collect::<String>()
        .trim()
        .to_string()
}

async fn open_reply_thread(
    st: &WebState,
    channel: &str,
    parent: &SessionMessage,
    sync_hub: bool,
) -> String {
    let name = format!("m{}", parent.seq);
    if st
        .db
        .load_thread(channel, &name, 1)
        .await
        .map(|rows| !rows.is_empty())
        .unwrap_or(false)
    {
        return name;
    }
    let _ = st.db.set_thread_visibility(channel, &name, "inherit").await;
    let copy = SessionMessage {
        thread_id: name.clone(),
        seq: 0,
        ..parent.clone()
    };
    let _ = store_message(st, copy, sync_hub, "").await;
    name
}

async fn high_water(st: &WebState, name: &str) -> i64 {
    let local = st.db.max_seq(name).await.unwrap_or(0);
    let remote = if let Some(base) = master_http(&st.cfg) {
        fetch_mesh(base, &st.cfg.node.id)
            .await
            .and_then(|mesh| mesh.activity.get(name).copied())
            .unwrap_or(0)
    } else if matches!(st.cfg.node.mode, NodeMode::Master) {
        st.db
            .activity()
            .await
            .ok()
            .and_then(|rows| rows.into_iter().find(|(n, _)| n == name).map(|(_, s)| s))
            .unwrap_or(0)
    } else {
        0
    };
    local.max(remote)
}

async fn fetch_remote_live(base: &str) -> Option<HashMap<String, String>> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(4))
        .build()
        .ok()?;
    client
        .get(format!("{}/api/mesh/live", base.trim_end_matches('/')))
        .send()
        .await
        .ok()?
        .json()
        .await
        .ok()
}

async fn apply_synced(st: &WebState, channel: &str, msg: ShareMsg) {
    let thread = msg.thread_id.clone();
    let _ = st
        .db
        .append_message(&SessionMessage {
            channel_id: channel.to_string(),
            thread_id: thread.clone(),
            seq: msg.seq,
            author: msg.author,
            content: msg.content,
            created_at_ms: msg.created_at_ms,
        })
        .await;
    if msg.deleted {
        let _ = st
            .db
            .set_message_deleted(channel, &thread, msg.seq, true)
            .await;
    }
    if msg.edited_at_ms > 0 {
        let detail = serde_json::json!({ "edited_at_ms": msg.edited_at_ms }).to_string();
        let _ = st
            .db
            .save_message_detail(channel, &thread, msg.seq, &detail)
            .await;
    }
}

fn share_entry(entry: ContextEntry) -> ShareEntry {
    ShareEntry {
        id: entry.id,
        namespace: entry.namespace,
        source_path: entry.source_path,
        content: entry.content.chars().take(8_000).collect(),
        content_hash: entry.content_hash,
        entry_type: entry.entry_type,
    }
}

async fn snapshot_entries(st: &WebState, channel: &str) -> Vec<ShareEntry> {
    let mut entries = Vec::new();
    if let Ok(rows) = st.db.load_entries(Some(channel)).await {
        for entry in rows.into_iter().take(40) {
            entries.push(share_entry(entry));
        }
    }
    let namespaces = st
        .db
        .get_channel(channel)
        .await
        .ok()
        .flatten()
        .map(|row| row.context_namespaces)
        .filter(|names| !names.is_empty())
        .unwrap_or_else(|| vec![channel.to_string()]);
    for ns in namespaces {
        let Some(dir) = project_dir(&st.cfg, &ns) else {
            continue;
        };
        let outline = project_outline(&st.cfg, dir.clone()).await;
        if !outline.is_empty() {
            entries.push(ShareEntry {
                id: blake3::hash(format!("outline:{ns}").as_bytes())
                    .to_hex()
                    .to_string(),
                namespace: ns.clone(),
                source_path: "_xmesh/outline.md".into(),
                content: outline.chars().take(28_000).collect(),
                content_hash: String::new(),
                entry_type: EntryType::Pinned as i32,
            });
        }
        let origin = st.cfg.node.id.clone();
        let ns_scan = ns.clone();
        if let Ok(Ok(files)) =
            tokio::task::spawn_blocking(move || scan_workdir(&dir, &ns_scan, &origin)).await
        {
            for entry in files.into_iter().take(40) {
                if entries.len() >= 80 {
                    break;
                }
                entries.push(share_entry(entry));
            }
        }
    }
    entries
}

async fn pull_remote_entries(st: &WebState, channel: &str) {
    let Some(base) = master_http(&st.cfg) else {
        return;
    };
    let client = match reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(8))
        .build()
    {
        Ok(client) => client,
        Err(_) => return,
    };
    let rows: Vec<ShareEntry> = match client
        .get(format!("{}/api/mesh/context", base.trim_end_matches('/')))
        .query(&[("node_id", st.cfg.node.id.as_str()), ("channel", channel)])
        .send()
        .await
    {
        Ok(resp) => resp.json().await.unwrap_or_default(),
        Err(_) => return,
    };
    for entry in rows {
        let _ = st
            .db
            .upsert_proto(
                &ContextEntry {
                    id: entry.id,
                    namespace: entry.namespace,
                    entry_type: entry.entry_type,
                    source_path: entry.source_path,
                    content: entry.content,
                    content_hash: entry.content_hash,
                    embedding: vec![],
                    updated_at_ms: now_ms(),
                    origin_node: st.cfg.node.id.clone(),
                    meta: Default::default(),
                },
                false,
            )
            .await;
    }
}

async fn http_append(st: &WebState, msg: &SessionMessage) -> Option<SessionMessage> {
    let base = master_http(&st.cfg)?;
    let payload = ShareMsg {
        thread_id: msg.thread_id.clone(),
        seq: msg.seq,
        author: msg.author.clone(),
        content: msg.content.clone(),
        created_at_ms: msg.created_at_ms,
        deleted: false,
        edited_at_ms: 0,
    };
    let resp = signed_post(
        st,
        &format!(
            "{}/api/mesh/append?channel={}",
            base.trim_end_matches('/'),
            urlencoding(&msg.channel_id)
        ),
        "append",
        &[&msg.channel_id, &msg.thread_id, &msg.author, &msg.content],
        &payload,
    )
    .await
    .ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let body: ShareMsg = resp.json().await.ok()?;
    Some(SessionMessage {
        channel_id: msg.channel_id.clone(),
        thread_id: body.thread_id,
        seq: body.seq,
        author: body.author,
        content: body.content,
        created_at_ms: body.created_at_ms,
    })
}

fn urlencoding(text: &str) -> String {
    let mut out = String::new();
    for b in text.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

async fn publish_edit(st: &WebState, channel: &str, thread: &str, seq: u64, text: &str, edited_at_ms: i64) {
    if let Some(base) = master_http(&st.cfg) {
        let payload = ShareMsg {
            thread_id: thread.into(),
            seq,
            author: String::new(),
            content: text.into(),
            created_at_ms: 0,
            deleted: false,
            edited_at_ms,
        };
        let _ = signed_post(
            st,
            &format!(
                "{}/api/mesh/edit?channel={}",
                base.trim_end_matches('/'),
                urlencoding(channel)
            ),
            "edit",
            &[channel, thread, &seq.to_string(), text],
            &payload,
        )
        .await;
    }
    if let Ok(Some(link)) = st.db.get_p2p(channel).await {
        let msg = SessionMessage {
            channel_id: channel.into(),
            thread_id: thread.into(),
            seq,
            author: String::new(),
            content: text.into(),
            created_at_ms: 0,
        };
        push_one(&link, &msg, false, edited_at_ms).await;
    }
}

async fn publish_delete(st: &WebState, channel: &str, thread: &str, seq: u64) {
    if let Some(base) = master_http(&st.cfg) {
        let payload = ShareMsg {
            thread_id: thread.into(),
            seq,
            author: String::new(),
            content: String::new(),
            created_at_ms: 0,
            deleted: true,
            edited_at_ms: 0,
        };
        let _ = signed_post(
            st,
            &format!(
                "{}/api/mesh/delete?channel={}",
                base.trim_end_matches('/'),
                urlencoding(channel)
            ),
            "delete",
            &[channel, thread, &seq.to_string()],
            &payload,
        )
        .await;
    }
    if let Ok(Some(link)) = st.db.get_p2p(channel).await {
        let msg = SessionMessage {
            channel_id: channel.into(),
            thread_id: thread.into(),
            seq,
            author: String::new(),
            content: String::new(),
            created_at_ms: 0,
        };
        push_one(&link, &msg, true, 0).await;
    }
}

async fn prefetch_pages(st: &WebState, prompt: &str) -> String {
    let mut out = String::new();
    for url in crate::browse::urls_in(prompt) {
        let page = run_lookup(
            st,
            &crate::browse::Lookup {
                kind: "fetch",
                query: url.clone(),
            },
        )
        .await;
        out.push_str(&format!("\n### {url}\n{page}\n"));
    }
    out
}

async fn run_lookups(st: &WebState, tools: &[crate::browse::Lookup]) -> String {
    let mut out = String::new();
    for tool in tools.iter().take(2) {
        let page = run_lookup(st, tool).await;
        out.push_str(&format!("\n### {} {}\n{page}\n", tool.kind, tool.query));
    }
    out
}

async fn run_lookup(st: &WebState, lookup: &crate::browse::Lookup) -> String {
    if let Some(base) = master_http(&st.cfg) {
        if let Some(text) = remote_lookup(base, lookup).await {
            return text;
        }
    }
    crate::browse::run(lookup)
        .await
        .unwrap_or_else(|err| format!("lookup failed: {err}"))
}

async fn remote_lookup(base: &str, lookup: &crate::browse::Lookup) -> Option<String> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(25))
        .build()
        .ok()?;
    let resp = client
        .post(format!("{}/api/internal/web", base.trim_end_matches('/')))
        .json(&serde_json::json!({ "kind": lookup.kind, "query": lookup.query }))
        .send()
        .await
        .ok()?;
    if !resp.status().is_success() {
        return None;
    }
    let body: serde_json::Value = resp.json().await.ok()?;
    body.get("text").and_then(|v| v.as_str()).map(str::to_string)
}

#[derive(Deserialize)]
struct WebBody {
    #[serde(default)]
    kind: String,
    #[serde(default)]
    query: String,
}

async fn handle_internal_web(Json(body): Json<WebBody>) -> Json<serde_json::Value> {
    let lookup = crate::browse::Lookup {
        kind: if body.kind == "fetch" { "fetch" } else { "web" },
        query: body.query,
    };
    let text = crate::browse::run(&lookup)
        .await
        .unwrap_or_else(|err| format!("lookup failed: {err}"));
    Json(serde_json::json!({ "text": text }))
}

async fn handle_neighbors(State(st): State<Arc<WebState>>) -> Json<Vec<crate::discover::Neighbor>> {
    let rows = st.neighbors.lock().map(|g| g.clone()).unwrap_or_default();
    Json(rows)
}

#[derive(Serialize, Deserialize)]
struct TypingBody {
    channel: String,
    #[serde(default)]
    kind: String,
}

async fn handle_typing(
    State(st): State<Arc<WebState>>,
    Json(body): Json<TypingBody>,
) -> StatusCode {
    if body.kind.is_empty() {
        clear_live(&st, &body.channel);
    } else {
        set_live(&st, &body.channel, &body.kind, now_ms() + 6_000);
    }
    if let Some(base) = master_http(&st.cfg) {
        let _ = signed_post(
            &st,
            &format!("{}/api/mesh/typing", base.trim_end_matches('/')),
            "typing",
            &[&body.channel, &body.kind],
            &body,
        )
        .await;
    }
    StatusCode::OK
}

async fn handle_mesh_typing(
    State(st): State<Arc<WebState>>,
    Json(body): Json<TypingBody>,
) -> StatusCode {
    if body.kind.is_empty() {
        clear_live(&st, &body.channel);
    } else {
        set_live(&st, &body.channel, &body.kind, now_ms() + 6_000);
    }
    StatusCode::OK
}

async fn handle_mesh_live(State(st): State<Arc<WebState>>) -> Json<HashMap<String, String>> {
    let now = now_ms();
    let rows = st
        .live
        .lock()
        .map(|map| {
            map.iter()
                .filter(|(_, (_, until))| *until > now)
                .map(|(name, (kind, _))| (name.clone(), kind.clone()))
                .collect()
        })
        .unwrap_or_default();
    Json(rows)
}

#[derive(Deserialize)]
struct NodeChannelQuery {
    #[serde(default)]
    node_id: String,
    #[serde(default)]
    channel: String,
}

async fn member_or_public(st: &WebState, channel: &str, node_id: &str) -> Result<(), StatusCode> {
    let Some(row) = st
        .db
        .get_channel(channel)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
    else {
        return Err(StatusCode::NOT_FOUND);
    };
    if row.visibility == "private" {
        return Err(StatusCode::FORBIDDEN);
    }
    if row.visibility == "shared" {
        let member = st
            .db
            .member_state(channel, node_id)
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        if member.is_none() {
            return Err(StatusCode::FORBIDDEN);
        }
    }
    Ok(())
}

async fn handle_mesh_context(
    State(st): State<Arc<WebState>>,
    Query(q): Query<NodeChannelQuery>,
) -> Result<Json<Vec<ShareEntry>>, StatusCode> {
    member_or_public(&st, &q.channel, &q.node_id).await?;
    let mut names = st
        .db
        .get_channel(&q.channel)
        .await
        .ok()
        .flatten()
        .map(|row| row.context_namespaces)
        .unwrap_or_default();
    if !names.iter().any(|ns| ns == &q.channel) {
        names.push(q.channel.clone());
    }
    let mut out = Vec::new();
    for ns in names {
        if let Ok(rows) = st.db.load_entries(Some(&ns)).await {
            for entry in rows {
                if out.len() >= 80 {
                    break;
                }
                out.push(share_entry(entry));
            }
        }
    }
    Ok(Json(out))
}

async fn handle_mesh_context_put(
    State(st): State<Arc<WebState>>,
    headers: HeaderMap,
    Query(q): Query<NodeChannelQuery>,
    Json(entries): Json<Vec<ShareEntry>>,
) -> Result<StatusCode, StatusCode> {
    let node = header_node(&st, &headers);
    member_or_public(&st, &q.channel, &node).await?;
    for entry in entries.into_iter().take(80) {
        let _ = st.db.upsert_proto(&entry_proto(&st, &entry), false).await;
    }
    Ok(StatusCode::OK)
}

async fn handle_mesh_append(
    State(st): State<Arc<WebState>>,
    headers: HeaderMap,
    Query(q): Query<NodeChannelQuery>,
    Json(body): Json<ShareMsg>,
) -> Result<Json<ShareMsg>, StatusCode> {
    let node = header_node(&st, &headers);
    member_or_public(&st, &q.channel, &node).await?;
    let stored = st
        .db
        .append_public_message(&SessionMessage {
            channel_id: q.channel,
            thread_id: body.thread_id,
            seq: body.seq,
            author: body.author,
            content: body.content,
            created_at_ms: body.created_at_ms,
        })
        .await
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    Ok(Json(ShareMsg {
        thread_id: stored.thread_id,
        seq: stored.seq,
        author: stored.author,
        content: stored.content,
        created_at_ms: stored.created_at_ms,
        deleted: false,
        edited_at_ms: 0,
    }))
}

async fn handle_mesh_edit(
    State(st): State<Arc<WebState>>,
    headers: HeaderMap,
    Query(q): Query<NodeChannelQuery>,
    Json(body): Json<ShareMsg>,
) -> Result<StatusCode, StatusCode> {
    let node = header_node(&st, &headers);
    member_or_public(&st, &q.channel, &node).await?;
    st.db
        .rewrite_message(&q.channel, &body.thread_id, body.seq, &body.content)
        .await
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    if body.edited_at_ms > 0 {
        let detail = serde_json::json!({ "edited_at_ms": body.edited_at_ms }).to_string();
        let _ = st
            .db
            .save_message_detail(&q.channel, &body.thread_id, body.seq, &detail)
            .await;
    }
    Ok(StatusCode::OK)
}

async fn handle_mesh_delete(
    State(st): State<Arc<WebState>>,
    headers: HeaderMap,
    Query(q): Query<NodeChannelQuery>,
    Json(body): Json<ShareMsg>,
) -> Result<StatusCode, StatusCode> {
    let node = header_node(&st, &headers);
    member_or_public(&st, &q.channel, &node).await?;
    st.db
        .set_message_deleted(&q.channel, &body.thread_id, body.seq, true)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(StatusCode::OK)
}

fn header_node(st: &WebState, headers: &HeaderMap) -> String {
    headers
        .get("x-xmesh-node")
        .and_then(|v| v.to_str().ok())
        .unwrap_or(&st.cfg.node.id)
        .to_string()
}

#[derive(Deserialize)]
struct P2pOpen {
    peer_base: String,
    peer_name: String,
}

#[derive(Serialize, Deserialize)]
struct P2pHello {
    channel: String,
    token: String,
    from_name: String,
    from_base: String,
}

#[derive(Deserialize)]
struct P2pPush {
    token: String,
    channel: String,
    #[serde(default)]
    messages: Vec<ShareMsg>,
}

async fn handle_p2p_open(
    State(st): State<Arc<WebState>>,
    Json(body): Json<P2pOpen>,
) -> Result<Json<serde_json::Value>, StatusCode> {
    let peer_name = body.peer_name.trim();
    if peer_name.is_empty() || storage_name(peer_name).is_err() {
        return Err(StatusCode::BAD_REQUEST);
    }
    let channel = format!("p2p-{peer_name}");
    let (token, _) = crate::identity::new_token();
    let base = body.peer_base.trim_end_matches('/').to_string();
    let hello = P2pHello {
        channel: channel.clone(),
        token: token.clone(),
        from_name: st.cfg.node.id.clone(),
        from_base: local_http_base(&st),
    };
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(8))
        .build()
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    let resp = client
        .post(format!("{base}/api/p2p/hello"))
        .json(&hello)
        .send()
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;
    if !resp.status().is_success() {
        return Err(StatusCode::BAD_GATEWAY);
    }
    st.db
        .upsert_channel(&ChannelRow {
            name: channel.clone(),
            visibility: "private".into(),
            namespace: channel.clone(),
            origin_node: st.cfg.node.id.clone(),
            context_namespaces: vec![channel.clone()],
            updated_at_ms: 0,
        })
        .await
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    st.db
        .put_p2p(&channel, &base, peer_name, &token)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(serde_json::json!({ "channel": channel })))
}

async fn handle_p2p_hello(
    State(st): State<Arc<WebState>>,
    Json(body): Json<P2pHello>,
) -> Result<StatusCode, StatusCode> {
    if storage_name(&body.channel).is_err() {
        return Err(StatusCode::BAD_REQUEST);
    }
    st.db
        .upsert_channel(&ChannelRow {
            name: body.channel.clone(),
            visibility: "private".into(),
            namespace: body.channel.clone(),
            origin_node: body.from_name.clone(),
            context_namespaces: vec![body.channel.clone()],
            updated_at_ms: 0,
        })
        .await
        .map_err(|_| StatusCode::BAD_REQUEST)?;
    st.db
        .put_p2p(
            &body.channel,
            body.from_base.trim_end_matches('/'),
            &body.from_name,
            &body.token,
        )
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(StatusCode::OK)
}

async fn handle_p2p_push(
    State(st): State<Arc<WebState>>,
    Json(body): Json<P2pPush>,
) -> Result<StatusCode, StatusCode> {
    let link = st
        .db
        .get_p2p(&body.channel)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;
    if link.token != body.token {
        return Err(StatusCode::FORBIDDEN);
    }
    for msg in body.messages {
        apply_synced(&st, &body.channel, msg).await;
    }
    Ok(StatusCode::OK)
}

#[derive(Deserialize)]
struct P2pPullQuery {
    channel: String,
    token: String,
    #[serde(default)]
    thread: String,
}

async fn handle_p2p_pull(
    State(st): State<Arc<WebState>>,
    Query(q): Query<P2pPullQuery>,
) -> Result<Json<Vec<ShareMsg>>, StatusCode> {
    let link = st
        .db
        .get_p2p(&q.channel)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?
        .ok_or(StatusCode::NOT_FOUND)?;
    if link.token != q.token {
        return Err(StatusCode::FORBIDDEN);
    }
    let thread = if q.thread.is_empty() { "main" } else { &q.thread };
    let rows = st
        .db
        .load_sync(&q.channel, thread, 400)
        .await
        .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
    Ok(Json(
        rows.into_iter()
            .map(|row| ShareMsg {
                thread_id: row.message.thread_id,
                seq: row.message.seq,
                author: row.message.author,
                content: row.message.content,
                created_at_ms: row.message.created_at_ms,
                deleted: row.deleted,
                edited_at_ms: row.edited_at_ms,
            })
            .collect(),
    ))
}

fn local_http_base(st: &WebState) -> String {
    let port = st
        .cfg
        .node
        .http_addr
        .rsplit(':')
        .next()
        .unwrap_or("8080");
    if let Ok(socket) = std::net::UdpSocket::bind("0.0.0.0:0") {
        if socket.connect("8.8.8.8:80").is_ok() {
            if let Ok(addr) = socket.local_addr() {
                if let std::net::IpAddr::V4(ip) = addr.ip() {
                    if !ip.is_loopback() {
                        return format!("http://{ip}:{port}");
                    }
                }
            }
        }
    }
    format!("http://127.0.0.1:{port}")
}

async fn pull_p2p(link: &crate::storage::P2pLink, thread: &str) -> Option<Vec<ShareMsg>> {
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(8))
        .build()
        .ok()?;
    client
        .get(format!(
            "{}/api/p2p/pull",
            link.peer_base.trim_end_matches('/')
        ))
        .query(&[
            ("channel", link.channel.as_str()),
            ("token", link.token.as_str()),
            ("thread", thread),
        ])
        .send()
        .await
        .ok()?
        .json()
        .await
        .ok()
}

async fn push_one(link: &crate::storage::P2pLink, msg: &SessionMessage, deleted: bool, edited_at_ms: i64) {
    let client = match reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(8))
        .build()
    {
        Ok(client) => client,
        Err(_) => return,
    };
    let payload = P2pPushBody {
        token: link.token.clone(),
        channel: link.channel.clone(),
        messages: vec![ShareMsg {
            thread_id: msg.thread_id.clone(),
            seq: msg.seq,
            author: msg.author.clone(),
            content: msg.content.clone(),
            created_at_ms: msg.created_at_ms,
            deleted,
            edited_at_ms,
        }],
    };
    let _ = client
        .post(format!(
            "{}/api/p2p/push",
            link.peer_base.trim_end_matches('/')
        ))
        .json(&payload)
        .send()
        .await;
}

#[derive(Serialize)]
struct P2pPushBody {
    token: String,
    channel: String,
    messages: Vec<ShareMsg>,
}
