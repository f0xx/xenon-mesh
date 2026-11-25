use crate::config::AppConfig;
use crate::gemini::GeminiClient;
use crate::git_sync::extract_local_diff;
use crate::storage::pb::cluster_service_client::ClusterServiceClient;
use crate::storage::pb::{ContextEntry, EntryType, SyncEnvelope};
use crate::storage::Storage;
use anyhow::Result;
use notify_debouncer_mini::{new_debouncer, notify::RecursiveMode, DebounceEventResult};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info};

pub async fn build_prompt_context(db: &Storage, namespace: &str, query_emb: &[f32], top_k: usize) -> Result<String> {
    let env = db.export_envelope("master", Some(namespace)).await?;
    let mut static_blocks = Vec::new();
    let mut scored_dynamic = Vec::new();

    for e in env.entries {
        if e.entry_type == EntryType::Pinned as i32 || e.entry_type == EntryType::Learned as i32 {
            static_blocks.push(format!("### [PINNED/LEARNED] {}\n{}", e.source_path, e.content));
        } else if !e.embedding.is_empty() && !query_emb.is_empty() {
            let score = cosine_sim(&e.embedding, query_emb);
            scored_dynamic.push((score, e));
        }
    }

    scored_dynamic.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    for (_, e) in scored_dynamic.into_iter().take(top_k) {
        static_blocks.push(format!("### [CODE: {} @ {}]\n{}", e.source_path, e.origin_node, e.content));
    }
    Ok(static_blocks.join("\n\n"))
}

fn cosine_sim(a: &[f32], b: &[f32]) -> f32 {
    let (mut dot, mut na, mut nb) = (0.0, 0.0, 0.0);
    for (x, y) in a.iter().zip(b.iter()) {
        dot += x * y;
        na += x * x;
        nb += y * y;
    }
    if na == 0.0 || nb == 0.0 { 0.0 } else { dot / (na.sqrt() * nb.sqrt()) }
}

pub async fn run_node_watcher(cfg: Arc<AppConfig>, db: Arc<Storage>, gemini: Arc<GeminiClient>) -> Result<()> {
    let (tx, mut rx) = tokio::sync::mpsc::channel(16);
    let mut debouncer = new_debouncer(Duration::from_secs(10), move |res: DebounceEventResult| {
        if res.is_ok() { let _ = tx.blocking_send(()); }
    })?;

    for repo in &cfg.git.watched_repos {
        debouncer.watcher().watch(Path::new(&repo.local_path), RecursiveMode::Recursive)?;
    }

    while rx.recv().await.is_some() {
        let mut batch: Vec<ContextEntry> = Vec::new();
        for repo in &cfg.git.watched_repos {
            if let Ok(Some(mut entry)) = extract_local_diff(
                Path::new(&repo.local_path), &repo.namespace, &repo.base_branch, &cfg.node.id
            ) {
                if let Ok(emb) = gemini.embed_text(&entry.content).await {
                    entry.embedding = emb;
                }
                let _ = db.upsert_proto(&entry, false).await;
                batch.push(entry);
            }
        }

        if !batch.is_empty() {
            if let Some(master_url) = &cfg.node.master_url {
                if let Ok(mut client) = ClusterServiceClient::connect(master_url.clone()).await {
                    let env = SyncEnvelope { schema_version: 1, source_node: cfg.node.id.clone(), entries: batch, ..Default::default() };
                    match client.sync_contexts(env).await {
                        Ok(_) => info!("Synced local diffs to master"),
                        Err(e) => error!("Failed to sync diffs: {e}"),
                    }
                }
            }
        }
    }
    Ok(())
}

