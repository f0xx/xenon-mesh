use crate::config::AppConfig;
use crate::git_sync::{discover_bare_repos, discover_project_dirs, extract_local_diff, scan_bare_repo, scan_workdir};
use crate::hub;
use crate::models::ModelHub;
use crate::storage::pb::{ContextEntry, SyncEnvelope};
use crate::storage::{now_ms, ChannelRow, Storage};
use anyhow::Result;
use notify::event::{AccessKind, AccessMode, ModifyKind};
use notify::{EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use tracing::{error, info, warn};

pub async fn run_node_watcher(
    cfg: Arc<AppConfig>,
    db: Arc<Storage>,
    models: Arc<ModelHub>,
) -> Result<()> {
    let roots: Vec<PathBuf> = cfg
        .git
        .watched_repos
        .iter()
        .map(|repo| PathBuf::from(&repo.local_path))
        .collect();
    let (_watcher, mut rx) = content_watch(&roots, Duration::from_secs(10))?;

    while rx.recv().await.is_some() {
        let mut batch: Vec<ContextEntry> = Vec::new();
        for repo in &cfg.git.watched_repos {
            let Ok(Some(mut entry)) = extract_local_diff(
                Path::new(&repo.local_path),
                &repo.namespace,
                &repo.base_branch,
                &cfg.node.id,
            ) else {
                continue;
            };
            if let Ok(emb) = models.embed_text(&entry.content).await {
                entry.embedding = emb;
            }
            let _ = db.upsert_proto(&entry, false).await;
            if db
                .namespace_is_public(&repo.namespace)
                .await
                .unwrap_or(false)
            {
                batch.push(entry);
            }
        }

        if batch.is_empty() {
            continue;
        }
        let Some(master_url) = cfg.node.master_url.as_deref() else {
            continue;
        };
        let env = SyncEnvelope {
            schema_version: 2,
            source_node: cfg.node.id.clone(),
            timestamp_ms: now_ms(),
            entries: batch,
            ..Default::default()
        };
        let node_key = cfg
            .node
            .node_key_path
            .as_deref()
            .and_then(|p| crate::identity::key_from_file(Path::new(p)));
        match hub::push_envelope(master_url, env, &cfg.node.id, node_key.as_ref()).await {
            Ok(()) => info!("synced public diffs to the hub"),
            Err(err) => error!("failed to sync diffs: {err}"),
        }
    }
    Ok(())
}

/// Opens and atime updates are not content changes. notify's Linux backend
/// reports both, and a reindex reads git objects, so those events were
/// scheduling the next reindex on an idle tree.
fn worth_reindex(kind: EventKind) -> bool {
    match kind {
        EventKind::Access(AccessKind::Close(AccessMode::Write)) => true,
        EventKind::Access(_) => false,
        EventKind::Modify(ModifyKind::Metadata(_)) => false,
        EventKind::Modify(_) => true,
        EventKind::Create(_) | EventKind::Remove(_) => true,
        EventKind::Any => true,
        EventKind::Other => false,
    }
}

fn content_watch(
    roots: &[PathBuf],
    debounce: Duration,
) -> Result<(RecommendedWatcher, tokio::sync::mpsc::Receiver<Vec<PathBuf>>)> {
    let (tx, rx) = tokio::sync::mpsc::channel::<PathBuf>(256);
    let mut watcher = RecommendedWatcher::new(
        move |res: Result<notify::Event, notify::Error>| {
            let Ok(event) = res else {
                return;
            };
            if !worth_reindex(event.kind) {
                return;
            }
            for path in event.paths {
                let _ = tx.blocking_send(path);
            }
        },
        notify::Config::default(),
    )?;
    for root in roots {
        if let Err(err) = watcher.watch(root, RecursiveMode::Recursive) {
            warn!(path = %root.display(), error = %err, "could not watch path");
        }
    }
    let (batch_tx, batch_rx) = tokio::sync::mpsc::channel(8);
    tokio::spawn(async move {
        let mut incoming = rx;
        while let Some(first) = incoming.recv().await {
            let mut paths = vec![first];
            let deadline = tokio::time::Instant::now() + debounce;
            loop {
                let left = deadline.saturating_duration_since(tokio::time::Instant::now());
                if left.is_zero() {
                    break;
                }
                match tokio::time::timeout(left, incoming.recv()).await {
                    Ok(Some(path)) => paths.push(path),
                    Ok(None) => break,
                    Err(_) => break,
                }
            }
            if batch_tx.send(paths).await.is_err() {
                break;
            }
        }
    });
    Ok((watcher, batch_rx))
}

fn path_note(paths: &[&PathBuf]) -> String {
    let mut shown: Vec<String> = paths
        .iter()
        .take(6)
        .map(|path| path.display().to_string())
        .collect();
    let extra = paths.len().saturating_sub(shown.len());
    if extra > 0 {
        shown.push(format!("+{extra} more"));
    }
    shown.join(", ")
}

fn noisy(path: &Path) -> bool {
    if path.components().any(|part| {
        matches!(
            part.as_os_str().to_str(),
            Some(
                ".git"
                    | "target"
                    | "node_modules"
                    | "dist"
                    | ".venv"
                    | "vendor"
                    | "build"
                    | ".xmesh"
                    | "__pycache__"
                    | ".cache"
                    | ".idea"
                    | ".vscode"
            )
        )
    }) {
        return true;
    }
    let name = path.file_name().and_then(|s| s.to_str()).unwrap_or("");
    name.ends_with('~')
        || name.ends_with(".swp")
        || name.ends_with(".swo")
        || name.ends_with(".tmp")
        || name.starts_with(".#")
}

/// Watches project roots and bare repos.
/// A directory that disappears leaves the channel list.
/// A file saved outside the browser is reindexed.
/// `git.watch_changes: false` turns this off. The startup scan still runs.
pub async fn run_tree_watcher(cfg: Arc<AppConfig>, db: Arc<Storage>) -> Result<()> {
    if !cfg.git.watch_changes {
        info!("tree watch is off (git.watch_changes)");
        return Ok(());
    }
    let mut roots: Vec<PathBuf> = cfg
        .git
        .project_roots
        .iter()
        .map(|root| PathBuf::from(&root.path))
        .collect();
    if let Some(root) = &cfg.git.bare_repos_root {
        roots.push(PathBuf::from(root));
    }
    let (_watcher, mut rx) = content_watch(&roots, Duration::from_secs(15))?;
    info!("watching project and bare trees for changes");
    while let Some(paths) = rx.recv().await {
        let paths: Vec<PathBuf> = paths.into_iter().filter(|path| !noisy(path)).collect();
        if paths.is_empty() {
            continue;
        }
        if let Err(err) = reconcile(&cfg, &db, &paths).await {
            warn!(error = %err, "tree reconcile failed");
        }
    }
    Ok(())
}

async fn reconcile(cfg: &AppConfig, db: &Storage, paths: &[PathBuf]) -> Result<()> {
    let mut live_projects = HashSet::new();
    let mut touched = HashMap::new();
    for root in &cfg.git.project_roots {
        let root_path = PathBuf::from(&root.path);
        let dirs = discover_project_dirs(&root_path);
        for (name, dir) in &dirs {
            live_projects.insert(name.clone());
            let hits: Vec<&PathBuf> = paths.iter().filter(|path| path.starts_with(dir)).collect();
            if !hits.is_empty() {
                touched.insert(name.clone(), path_note(&hits));
            }
        }
        for (name, dir) in dirs {
            let Some(why) = touched.get(&name) else {
                continue;
            };
            if crate::storage::validate_channel_name(&name).is_err() {
                continue;
            }
            let vis = crate::storage::normalize_visibility(&root.visibility).unwrap_or("private");
            db.upsert_channel(&ChannelRow {
                name: name.clone(),
                visibility: vis.into(),
                namespace: name.clone(),
                origin_node: "project".into(),
                context_namespaces: vec![name.clone()],
                updated_at_ms: 0,
            })
            .await?;
            let ns = name.clone();
            let files = tokio::task::spawn_blocking(move || scan_workdir(&dir, &ns, "project"))
                .await
                .ok()
                .and_then(|r| r.ok())
                .unwrap_or_default();
            for entry in files {
                let _ = db.upsert_proto(&entry, false).await;
            }
            info!(channel = %name, paths = %why, "reindexed a project after a change on disk");
        }
    }
    if let Some(root) = &cfg.git.bare_repos_root {
        let root_path = PathBuf::from(root);
        let repos = discover_bare_repos(&root_path);
        let names: HashSet<String> = repos.iter().map(|(name, _)| name.clone()).collect();
        for channel in db.list_channel_rows().await? {
            if channel.origin_node == "index" && !names.contains(&channel.name) {
                db.delete_channel(&channel.name).await?;
                info!(channel = %channel.name, "removed a bare repo channel that is no longer on disk");
            }
        }
        for (name, dir) in repos {
            let hits: Vec<&PathBuf> = paths.iter().filter(|path| path.starts_with(&dir)).collect();
            if hits.is_empty() {
                continue;
            }
            let why = path_note(&hits);
            db.upsert_channel(&ChannelRow {
                name: name.clone(),
                visibility: "public".into(),
                namespace: name.clone(),
                origin_node: "index".into(),
                context_namespaces: vec![name.clone()],
                updated_at_ms: 0,
            })
            .await?;
            let ns = name.clone();
            let files = tokio::task::spawn_blocking(move || scan_bare_repo(&dir, &ns))
                .await
                .ok()
                .and_then(|r| r.ok())
                .unwrap_or_default();
            for entry in files {
                let _ = db.upsert_proto(&entry, false).await;
            }
            info!(channel = %name, paths = %why, "reindexed a bare repo after a change on disk");
        }
    }
    for channel in db.list_channel_rows().await? {
        if channel.origin_node == "project" && !live_projects.contains(&channel.name) {
            db.delete_channel(&channel.name).await?;
            info!(channel = %channel.name, "removed a project channel that is no longer on disk");
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_read_or_atime_update_is_not_a_reindex() {
        use notify::event::{AccessKind, AccessMode, MetadataKind, ModifyKind};
        assert!(!worth_reindex(EventKind::Access(AccessKind::Open(
            AccessMode::Any
        ))));
        assert!(!worth_reindex(EventKind::Access(AccessKind::Close(
            AccessMode::Read
        ))));
        assert!(!worth_reindex(EventKind::Modify(ModifyKind::Metadata(
            MetadataKind::Any
        ))));
        assert!(worth_reindex(EventKind::Access(AccessKind::Close(
            AccessMode::Write
        ))));
        assert!(worth_reindex(EventKind::Modify(ModifyKind::Data(
            notify::event::DataChange::Any
        ))));
        assert!(worth_reindex(EventKind::Create(
            notify::event::CreateKind::File
        )));
    }
}
