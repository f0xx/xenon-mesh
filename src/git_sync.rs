use crate::storage::pb::{ContextEntry, EntryType};
use anyhow::Result;
use git2::{DiffOptions, Repository};
use std::path::Path;

pub fn scan_bare_repo(bare_path: &Path, namespace: &str) -> Result<Vec<ContextEntry>> {
    if bare_path.join(".xmeshignore").exists() {
        return Ok(vec![]);
    }
    let repo = Repository::open_bare(bare_path)?;
    let head_tree = repo.head()?.peel_to_tree()?;
    let mut entries = Vec::new();

    head_tree.walk(git2::TreeWalkMode::PreOrder, |root, entry| {
        if entry.kind() != Some(git2::ObjectType::Blob) {
            return git2::TreeWalkResult::Ok;
        }
        let Ok(obj) = entry.to_object(&repo) else { return git2::TreeWalkResult::Ok; };
        let Some(blob) = obj.as_blob() else { return git2::TreeWalkResult::Ok; };

        if blob.is_binary() || blob.size() > 64 * 1024 {
            return git2::TreeWalkResult::Ok;
        }
        let name = entry.name().unwrap_or_default();
        if name.ends_with(".lock") || name.ends_with("-lock.json") || name.ends_with(".min.js") || name.ends_with(".map") {
            return git2::TreeWalkResult::Ok;
        }

        if let Ok(text) = std::str::from_utf8(blob.content()) {
            let rel_path = format!("{}{}", root, entry.name().unwrap_or_default());
            let id = blake3::hash(format!("master:{namespace}:{rel_path}").as_bytes()).to_hex().to_string();
            let content_hash = blake3::hash(text.as_bytes()).to_hex().to_string();

            entries.push(ContextEntry {
                id,
                namespace: namespace.to_string(),
                entry_type: EntryType::Dynamic as i32,
                source_path: rel_path,
                content: text.to_string(),
                content_hash,
                embedding: vec![],
                updated_at_ms: 0,
                origin_node: "git-master".to_string(),
                meta: Default::default(),
            });
        }
        git2::TreeWalkResult::Ok
    })?;

    Ok(entries)
}

pub fn extract_local_diff(repo_path: &Path, namespace: &str, base_ref: &str, node_id: &str) -> Result<Option<ContextEntry>> {
    let repo = Repository::open(repo_path)?;
    let base_obj = repo.revparse_single(base_ref)?;
    let base_tree = base_obj.peel_to_tree()?;

    let mut opts = DiffOptions::new();
    opts.include_untracked(true).recurse_untracked_dirs(true);

    let diff = repo.diff_tree_to_workdir_with_index(Some(&base_tree), Some(&mut opts))?;
    let mut patch_buf = String::new();

    diff.print(git2::DiffFormat::Patch, |_delta, _hunk, line| {
        if let Ok(s) = std::str::from_utf8(line.content()) {
            patch_buf.push(line.origin());
            patch_buf.push_str(s);
        }
        true
    })?;

    if patch_buf.trim().is_empty() {
        return Ok(None);
    }

    let id = blake3::hash(format!("diff:{node_id}:{namespace}").as_bytes()).to_hex().to_string();
    let content_hash = blake3::hash(patch_buf.as_bytes()).to_hex().to_string();

    Ok(Some(ContextEntry {
        id,
        namespace: namespace.to_string(),
        entry_type: EntryType::Dynamic as i32,
        source_path: format!("@diff:{node_id}"),
        content: patch_buf,
        content_hash,
        embedding: vec![],
        updated_at_ms: 0,
        origin_node: node_id.to_string(),
        meta: Default::default(),
    }))
}


use std::collections::HashSet;
use std::path::PathBuf;

pub fn discover_bare_repos(root: &Path) -> Vec<(String, PathBuf)> {
    let mut found = Vec::new();
    let mut visited = HashSet::new();
    let mut stack = vec![root.to_path_buf()];

    while let Some(dir) = stack.pop() {
        let Ok(canon) = std::fs::canonicalize(&dir) else { continue; };
        if !visited.insert(canon) { continue; } // Защита от циклических симлинков

        let Ok(entries) = std::fs::read_dir(&dir) else { continue; };
        for ent in entries.flatten() {
            let path = ent.path();
            if !path.is_dir() { continue; }
            let is_bare = path.join("HEAD").is_file() && path.join("objects").is_dir() && path.join("refs").is_dir();
            if is_bare {
                let rel = path.strip_prefix(root).unwrap_or(&path);
                let ns = rel.with_extension("").to_string_lossy().replace('\\', "/");
                found.push((ns, path));
            } else {
                stack.push(path);
            }
        }
    }
    found.sort_by(|a, b| a.0.cmp(&b.0));
    found
}


