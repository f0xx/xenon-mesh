mod config;
mod executor;
mod git_sync;
mod hub;
mod migrate;
mod models;
mod retrieve;
mod storage;
mod watcher;
mod web_ui;

use anyhow::{bail, Context, Result};
use config::{redact_url, AppConfig, NodeMode};
use models::ModelHub;
use std::sync::Arc;
use storage::pb::cluster_service_server::{ClusterService, ClusterServiceServer};
use storage::pb::{
    ChannelRecord, ContextQuery, ListChannelsRequest, ListChannelsResponse, NodeTaskRequest,
    NodeTaskResponse, SessionMessage, SyncAck, SyncEnvelope,
};
use storage::{ChannelRow, Storage};
use tonic::{transport::Server, Request, Response, Status};
use tracing::{error, info, warn};

struct NodeRpcServer {
    cfg: Arc<AppConfig>,
    db: Arc<Storage>,
    hmac_key: [u8; 32],
}

#[tonic::async_trait]
impl ClusterService for NodeRpcServer {
    async fn sync_contexts(&self, req: Request<SyncEnvelope>) -> Result<Response<SyncAck>, Status> {
        let env = req.into_inner();
        let (mut ok, mut skip) = (0u32, 0u32);
        for ch in &env.channels {
            if ch.visibility != "public" {
                skip += 1;
                continue;
            }
            self.db
                .upsert_channel(&ChannelRow::from_proto(ch))
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
            ok += 1;
        }
        for e in &env.entries {
            match self.db.upsert_proto(e, false).await {
                Ok(true) => ok += 1,
                Ok(false) => skip += 1,
                Err(err) => return Err(Status::internal(err.to_string())),
            }
        }
        for msg in &env.messages {
            if self
                .db
                .thread_visibility(&msg.channel_id, &msg.thread_id)
                .await
                .unwrap_or_default()
                == "private"
            {
                skip += 1;
                continue;
            }
            self.db
                .append_message(msg)
                .await
                .map_err(|e| Status::internal(e.to_string()))?;
            ok += 1;
        }
        Ok(Response::new(SyncAck {
            accepted_entries: ok,
            skipped_unchanged: skip,
        }))
    }

    async fn query_node_context(
        &self,
        req: Request<ContextQuery>,
    ) -> Result<Response<SyncEnvelope>, Status> {
        let ns = req.into_inner().namespace;
        self.db
            .export_public_envelope(&self.cfg.node.id, Some(&ns))
            .await
            .map(Response::new)
            .map_err(|e| Status::internal(e.to_string()))
    }

    async fn execute_task(
        &self,
        req: Request<NodeTaskRequest>,
    ) -> Result<Response<NodeTaskResponse>, Status> {
        executor::run_node_task(&self.cfg, &req.into_inner(), &self.hmac_key)
            .await
            .map(Response::new)
            .map_err(|e| Status::permission_denied(e.to_string()))
    }

    async fn list_channels(
        &self,
        _req: Request<ListChannelsRequest>,
    ) -> Result<Response<ListChannelsResponse>, Status> {
        let channels: Vec<ChannelRecord> = self
            .db
            .list_public_channels()
            .await
            .map_err(|e| Status::internal(e.to_string()))?
            .iter()
            .map(ChannelRow::to_proto)
            .collect();
        Ok(Response::new(ListChannelsResponse { channels }))
    }

    async fn append_message(
        &self,
        req: Request<SessionMessage>,
    ) -> Result<Response<SessionMessage>, Status> {
        self.db
            .append_public_message(&req.into_inner())
            .await
            .map(Response::new)
            .map_err(|e| Status::invalid_argument(e.to_string()))
    }
}

fn usage() -> &'static str {
    "usage:\n  xmeshd\n  xmeshd check [local.yaml] [cluster.yaml]\n  xmeshd models <base_url>\n  xmeshd migrate <from_url> <to_url>\n  xmeshd export <db_url> <out.pb.zst>\n  xmeshd import <db_url> <in.pb.zst>"
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("migrate") => {
            migrate::direct_migrate(
                args.get(2).context(usage())?,
                args.get(3).context(usage())?,
                false,
            )
            .await?;
            return Ok(());
        }
        Some("export") => {
            let db = Storage::connect(Some(args.get(2).context(usage())?), "/tmp/xmeshd-export.db")
                .await?;
            migrate::export_to_file(&db, "cli", None, args.get(3).context(usage())?).await?;
            return Ok(());
        }
        Some("import") => {
            let db = Storage::connect(Some(args.get(2).context(usage())?), "/tmp/xmeshd-import.db")
                .await?;
            db.init_messages_schema().await?;
            migrate::import_from_file(&db, args.get(3).context(usage())?, false).await?;
            return Ok(());
        }
        Some("models") => {
            let base = args.get(2).context(usage())?;
            let report = ModelHub::probe(base).await?;
            println!("{}", serde_json::to_string_pretty(&report)?);
            return Ok(());
        }
        Some("check") => {
            let local = args
                .get(2)
                .map(String::as_str)
                .unwrap_or("/etc/xmeshd/local.yaml");
            let cluster = args
                .get(3)
                .map(String::as_str)
                .unwrap_or("/etc/xmeshd/cluster.yaml");
            let cfg = AppConfig::load(cluster, local)?;
            println!(
                "node={} mode={:?} http={} grpc={}",
                cfg.node.id, cfg.node.mode, cfg.node.http_addr, cfg.node.listen_addr
            );
            match &cfg.storage.primary_url {
                Some(url) if !url.is_empty() => println!("storage={}", redact_url(url)),
                _ => println!("storage={}", cfg.storage.fallback_sqlite),
            }
            if cfg.models.providers.is_empty() {
                println!(
                    "provider id=gemini kind=gemini chat={}",
                    cfg.gemini.chat_model
                );
            }
            for p in &cfg.models.providers {
                println!(
                    "provider id={} kind={} base={} chat={}",
                    p.id, p.kind, p.base_url, p.chat_model
                );
            }
            return Ok(());
        }
        Some(other) => bail!("{}\nunknown command: {other}", usage()),
        None => {}
    }

    let cfg = Arc::new(AppConfig::load(
        "/etc/xmeshd/cluster.yaml",
        "/etc/xmeshd/local.yaml",
    )?);
    let db = Arc::new(
        Storage::connect(
            cfg.storage.primary_url.as_deref(),
            &cfg.storage.fallback_sqlite,
        )
        .await?,
    );
    db.init_messages_schema().await?;
    if matches!(cfg.node.mode, NodeMode::Master) && db.get_channel("general").await?.is_none() {
        db.upsert_channel(&ChannelRow {
            name: "general".into(),
            visibility: "public".into(),
            namespace: "general".into(),
            origin_node: cfg.node.id.clone(),
            context_namespaces: vec!["general".into()],
            updated_at_ms: 0,
        })
        .await?;
    }

    let hmac_bytes = std::fs::read(&cfg.node.hmac_secret_path)
        .with_context(|| format!("reading HMAC secret {}", cfg.node.hmac_secret_path))?;
    let hmac_key = *blake3::hash(&hmac_bytes).as_bytes();
    let model_hub = ModelHub::from_config(&cfg)?;
    if let Err(err) = model_hub.discover().await {
        warn!("model discovery failed: {err}");
    }
    info!("default model {}", model_hub.default_model().await);

    let status = Arc::new(tokio::sync::RwLock::new(web_ui::IndexStatus {
        phase: "ready".into(),
        mode: match cfg.node.mode {
            NodeMode::Master => "master".into(),
            NodeMode::Node => "node".into(),
        },
        embeddings: model_hub.embeddings_configured().await,
        ..Default::default()
    }));
    let web = Arc::new(web_ui::WebState {
        cfg: cfg.clone(),
        db: db.clone(),
        models: model_hub.clone(),
        hmac_key,
        status: status.clone(),
    });
    let listener = tokio::net::TcpListener::bind(&cfg.node.http_addr).await?;
    let http_addr = listener.local_addr()?;
    tokio::spawn(async move {
        if let Err(err) = axum::serve(listener, web_ui::router(web)).await {
            error!("HTTP server stopped: {err}");
        }
    });
    info!("HTTP UI listening on http://{http_addr}");

    if matches!(cfg.node.mode, NodeMode::Master) {
        if let Some(root) = cfg.git.bare_repos_root.clone() {
            let (db_bg, models_bg, st_bg) = (db.clone(), model_hub.clone(), status.clone());
            tokio::spawn(async move {
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
                let r_clone = root.clone();
                let repos = tokio::task::spawn_blocking(move || {
                    git_sync::discover_bare_repos(std::path::Path::new(&r_clone))
                })
                .await
                .unwrap_or_default();
                let total = repos.len();
                {
                    let mut s = st_bg.write().await;
                    s.busy = true;
                    s.phase = "indexing".into();
                    s.total_repos = total;
                }
                for (idx, (ns, path)) in repos.into_iter().enumerate() {
                    {
                        let mut s = st_bg.write().await;
                        s.done_repos = idx;
                        s.current_repo = ns.clone();
                    }
                    info!("scanning repo [{}/{}]: {}", idx + 1, total, ns);
                    let ns_c = ns.clone();
                    let scan_fut =
                        tokio::task::spawn_blocking(move || git_sync::scan_bare_repo(&path, &ns_c));
                    let entries =
                        match tokio::time::timeout(std::time::Duration::from_secs(20), scan_fut)
                            .await
                        {
                            Ok(Ok(Ok(list))) => list,
                            Ok(Ok(Err(err))) => {
                                warn!("{err}");
                                continue;
                            }
                            _ => {
                                warn!("timed out scanning repo: {ns}");
                                continue;
                            }
                        };
                    let mut embed_ok = true;
                    for mut e in entries.into_iter().take(300) {
                        if db_bg
                            .is_unchanged(&e.id, &e.content_hash)
                            .await
                            .unwrap_or(false)
                        {
                            continue;
                        }
                        if embed_ok {
                            match models_bg.embed_text(&e.content).await {
                                Ok(emb) if !emb.is_empty() => {
                                    e.embedding = emb;
                                    tokio::time::sleep(std::time::Duration::from_millis(60)).await;
                                }
                                Ok(_) => {}
                                Err(err) => {
                                    warn!("embedding paused ({err}), saving raw text");
                                    embed_ok = false;
                                }
                            }
                        }
                        if db_bg.upsert_proto(&e, false).await.unwrap_or(false) {
                            st_bg.write().await.indexed_files += 1;
                        }
                    }
                }
                let mut s = st_bg.write().await;
                s.busy = false;
                s.phase = "ready".into();
                s.done_repos = total;
                s.current_repo.clear();
                s.embeddings = models_bg.embeddings_configured().await;
                info!("background indexing complete ({total} repos)");
            });
        }
    }

    if !cfg.git.watched_repos.is_empty() {
        let (c, d, m) = (cfg.clone(), db.clone(), model_hub.clone());
        tokio::spawn(async move {
            if let Err(err) = watcher::run_node_watcher(c, d, m).await {
                error!("watcher: {err}");
            }
        });
    }

    let addr = cfg.node.listen_addr.parse()?;
    Server::builder()
        .add_service(ClusterServiceServer::new(NodeRpcServer {
            cfg,
            db,
            hmac_key,
        }))
        .serve(addr)
        .await?;
    Ok(())
}
