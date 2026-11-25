mod config; mod executor; mod gemini; mod git_sync; mod migrate; mod storage; mod watcher; mod web_ui;

use anyhow::Result;
use config::{AppConfig, NodeMode};
use gemini::GeminiClient;
use std::sync::Arc;
use storage::pb::cluster_service_server::{ClusterService, ClusterServiceServer};
use storage::pb::{ContextQuery, NodeTaskRequest, NodeTaskResponse, SyncAck, SyncEnvelope};
use storage::Storage;
use tonic::{transport::Server, Request, Response, Status};
use tracing::{error, info};

struct NodeRpcServer { cfg: Arc<AppConfig>, db: Arc<Storage>, hmac_key: [u8; 32] }

#[tonic::async_trait]
impl ClusterService for NodeRpcServer {
    async fn sync_contexts(&self, req: Request<SyncEnvelope>) -> Result<Response<SyncAck>, Status> {
        let (mut ok, mut skip) = (0, 0);
        for e in &req.into_inner().entries {
            match self.db.upsert_proto(e, false).await {
                Ok(true) => ok += 1, Ok(false) => skip += 1, Err(err) => return Err(Status::internal(err.to_string())),
            }
        }
        Ok(Response::new(SyncAck { accepted_entries: ok, skipped_unchanged: skip }))
    }
    async fn query_node_context(&self, req: Request<ContextQuery>) -> Result<Response<SyncEnvelope>, Status> {
        self.db.export_envelope(&self.cfg.node.id, Some(&req.into_inner().namespace)).await
            .map(Response::new).map_err(|e| Status::internal(e.to_string()))
    }
    async fn execute_task(&self, req: Request<NodeTaskRequest>) -> Result<Response<NodeTaskResponse>, Status> {
        executor::run_node_task(&self.cfg, &req.into_inner(), &self.hmac_key).await
            .map(Response::new).map_err(|e| Status::permission_denied(e.to_string()))
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args: Vec<String> = std::env::args().collect();
    match args.get(1).map(String::as_str) {
        Some("migrate") => {
            migrate::direct_migrate(args.get(2).expect("from_url"), args.get(3).expect("to_url"), false).await?;
            return Ok(());
        }
        Some("export") => {
            let db = Storage::connect(Some(args.get(2).expect("db_url")), "/tmp/exp.db").await?;
            migrate::export_to_file(&db, "cli", None, args.get(3).expect("out.pb.zst")).await?;
            return Ok(());
        }
        Some("import") => {
            let db = Storage::connect(Some(args.get(2).expect("db_url")), "/tmp/imp.db").await?;
            db.init_messages_schema().await?;
            migrate::import_from_file(&db, args.get(3).expect("in.pb.zst"), false).await?;
            return Ok(());
        }
        _ => {}
    }

    let cfg = Arc::new(AppConfig::load("/etc/xmeshd/cluster.yaml", "/etc/xmeshd/local.yaml")?);
    let db = Arc::new(Storage::connect(cfg.storage.primary_url.as_deref(), &cfg.storage.fallback_sqlite).await?);
    db.init_messages_schema().await?;

    let hmac_key = *blake3::hash(&std::fs::read(&cfg.node.hmac_secret_path)?).as_bytes();

    let api_key = std::env::var(&cfg.gemini.api_key_env).unwrap_or_default();
    let gemini = Arc::new(GeminiClient::new(
        api_key,
        &cfg.gemini.chat_model,
        &cfg.gemini.embed_model,
    ));

    match cfg.node.mode {
        NodeMode::Master => {
            let status = Arc::new(tokio::sync::RwLock::new(web_ui::IndexStatus::default()));
            let st = Arc::new(web_ui::WebState {
                db: db.clone(),
                gemini: gemini.clone(),
                hmac_key,
                bare_repos_root: cfg.git.bare_repos_root.clone(),
                default_model: cfg.gemini.chat_model.clone(),
                status: status.clone(),
            });

            let listener = tokio::net::TcpListener::bind("0.0.0.0:8080").await?;
            tokio::spawn(async move { axum::serve(listener, web_ui::router(st)).await.unwrap(); });
            info!("Master Web UI listening on http://0.0.0.0");

            if let Some(root) = cfg.git.bare_repos_root.clone() {
                let (db_bg, gem_bg, st_bg) = (db.clone(), gemini.clone(), status);
                tokio::spawn(async move {
                    let repos = git_sync::discover_bare_repos(std::path::Path::new(&root));
                    let total = repos.len();
                    {
                        let mut s = st_bg.write().await;
                        s.busy = true; s.phase = "indexing".into(); s.total_repos = total;
                    }
                    for (idx, (ns, path)) in repos.into_iter().enumerate() {
                        {
                            let mut s = st_bg.write().await;
                            s.done_repos = idx; s.current_repo = ns.clone();
                        }
                        if let Ok(entries) = git_sync::scan_bare_repo(&path, &ns) {
                            for mut e in entries {
                                if db_bg.is_unchanged(&e.id, &e.content_hash).await.unwrap_or(false) {
                                    continue;
                                }
                                if let Ok(emb) = gem_bg.embed_text(&e.content).await { e.embedding = emb; }
                                if db_bg.upsert_proto(&e, false).await.unwrap_or(false) {
                                    st_bg.write().await.indexed_files += 1;
                                }
                                tokio::time::sleep(std::time::Duration::from_millis(50)).await;
                            }
                        }
                    }
                    let mut s = st_bg.write().await;
                    s.busy = false; s.phase = "ready".into(); s.done_repos = total; s.current_repo.clear();
                    info!("Background indexing complete ({total} repos)");
                });
            }
        }

        NodeMode::Node => {
            let (c, d, g) = (cfg.clone(), db.clone(), gemini.clone());
            tokio::spawn(async move { if let Err(e) = watcher::run_node_watcher(c, d, g).await { error!("Watcher: {e}"); } });
        }
    }

    let addr = cfg.node.listen_addr.parse()?;
    Server::builder().add_service(ClusterServiceServer::new(NodeRpcServer { cfg, db, hmac_key })).serve(addr).await?;
    Ok(())
}

