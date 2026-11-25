use crate::storage::pb::{ContextEntry, EntryType};
use anyhow::Result;
use git2::{DiffOptions, Repository};
use std::path::Path;

pub fn scan_bare_repo(bare_path: &Path, namespace: &str) -> Result<Vec<ContextEntry>> {
    if bare_path.join(".xmeshignore").exists() {
        return Ok(vec![]);
    }

    let check = std::process::Command::new("git")
        .arg("--git-dir")
        .arg(bare_path)
        .args(["ls-tree", "-r", "--name-only", "HEAD"])
        .output()?;

    if !check.status.success() {
        let err_msg = String::from_utf8_lossy(&check.stderr);
        anyhow::bail!(
            "Skipping corrupt/empty repo '{namespace}' (run `git --git-dir={bare_path:?} fsck --full`): {}",
            err_msg.trim()
        );
    }

    let repo = Repository::open_bare(bare_path)?;

    let head_tree = repo.head()?.peel_to_tree()?;
    let mut entries = Vec::new();

    head_tree.walk(git2::TreeWalkMode::PreOrder, |root, entry| {
        if entry.kind() != Some(git2::ObjectType::Blob) {
            return git2::TreeWalkResult::Ok;
        }
        let Ok(obj) = entry.to_object(&repo) else {
            return git2::TreeWalkResult::Ok;
        };
        let Some(blob) = obj.as_blob() else {
            return git2::TreeWalkResult::Ok;
        };

        if blob.is_binary() || blob.size() > 64 * 1024 {
            return git2::TreeWalkResult::Ok;
        }
        let name = entry.name().unwrap_or_default();
        if name.ends_with(".lock")
            || name.ends_with("-lock.json")
            || name.ends_with(".min.js")
            || name.ends_with(".map")
        {
            return git2::TreeWalkResult::Ok;
        }

        if let Ok(text) = std::str::from_utf8(blob.content()) {
            let rel_path = format!("{}{}", root, entry.name().unwrap_or_default());
            let id = blake3::hash(format!("master:{namespace}:{rel_path}").as_bytes())
                .to_hex()
                .to_string();
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

pub fn extract_local_diff(
    repo_path: &Path,
    namespace: &str,
    base_ref: &str,
    node_id: &str,
) -> Result<Option<ContextEntry>> {
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

    let id = blake3::hash(format!("diff:{node_id}:{namespace}").as_bytes())
        .to_hex()
        .to_string();
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

/// Immediate child directories of a shared working tree.
pub fn discover_project_dirs(root: &Path) -> Vec<(String, PathBuf)> {
    let mut found = Vec::new();
    let Ok(rd) = std::fs::read_dir(root) else {
        return found;
    };
    for ent in rd.flatten() {
        let path = ent.path();
        if !path.is_dir() {
            continue;
        }
        let name = ent.file_name().to_string_lossy().to_string();
        if name.starts_with('.') || name.is_empty() {
            continue;
        }
        found.push((name, path));
    }
    found.sort_by(|a, b| a.0.cmp(&b.0));
    found
}

pub fn discover_bare_repos(root: &Path) -> Vec<(String, PathBuf)> {
    let mut found = Vec::new();
    let mut visited = HashSet::new();
    let mut stack = vec![root.to_path_buf()];

    while let Some(dir) = stack.pop() {
        let Ok(canon) = std::fs::canonicalize(&dir) else {
            continue;
        };
        if !visited.insert(canon) {
            continue;
        }

        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for ent in entries.flatten() {
            let path = ent.path();
            if !path.is_dir() {
                continue;
            }
            let is_bare = path.join("HEAD").is_file()
                && path.join("objects").is_dir()
                && path.join("refs").is_dir();
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

const SKIP_DIRS: &[&str] = &[
    ".git",
    "target",
    "node_modules",
    "dist",
    ".venv",
    "vendor",
    "build",
    ".xmesh",
];

fn skip_file(name: &str) -> bool {
    name.ends_with(".lock")
        || name.ends_with("-lock.json")
        || name.ends_with(".min.js")
        || name.ends_with(".map")
        || name.ends_with(".png")
        || name.ends_with(".jpg")
        || name.ends_with(".jpeg")
        || name.ends_with(".gif")
        || name.ends_with(".webp")
        || name.ends_with(".pdf")
        || name.ends_with(".zip")
        || name.ends_with(".gz")
        || name.ends_with(".woff")
        || name.ends_with(".wasm")
        || name.ends_with(".so")
        || name.ends_with(".a")
        || name.ends_with(".o")
}

/// Read a working tree into context entries.
///
/// Symlinks that leave `root` are ignored. Build and dependency directories
/// are skipped. The caller ranks the result; this function only caps volume.
pub fn scan_workdir(root: &Path, namespace: &str, origin: &str) -> Result<Vec<ContextEntry>> {
    let root = std::fs::canonicalize(root)?;
    let mut out: Vec<ContextEntry> = Vec::new();
    let mut stack = vec![root.clone()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for ent in entries.flatten() {
            if out.len() >= 200 {
                out.sort_by(|a, b| a.source_path.cmp(&b.source_path));
                return Ok(out);
            }
            let path = ent.path();
            let name = ent.file_name();
            let name = name.to_string_lossy();
            let Ok(canon) = std::fs::canonicalize(&path) else {
                continue;
            };
            if !canon.starts_with(&root) {
                continue;
            }
            if canon.is_dir() {
                if SKIP_DIRS.iter().any(|s| name == *s) {
                    continue;
                }
                stack.push(canon);
                continue;
            }
            if skip_file(&name) {
                continue;
            }
            let Ok(meta) = std::fs::metadata(&canon) else {
                continue;
            };
            if meta.len() == 0 || meta.len() > 32 * 1024 {
                continue;
            }
            let Ok(bytes) = std::fs::read(&canon) else {
                continue;
            };
            if bytes.contains(&0) {
                continue;
            }
            let Ok(text) = std::str::from_utf8(&bytes) else {
                continue;
            };
            let rel = canon.strip_prefix(&root).unwrap_or(&canon);
            let rel_path = rel.to_string_lossy().replace('\\', "/");
            let id = blake3::hash(format!("local:{origin}:{namespace}:{rel_path}").as_bytes())
                .to_hex()
                .to_string();
            let content_hash = blake3::hash(text.as_bytes()).to_hex().to_string();
            out.push(ContextEntry {
                id,
                namespace: namespace.to_string(),
                entry_type: EntryType::Dynamic as i32,
                source_path: rel_path,
                content: text.to_string(),
                content_hash,
                embedding: vec![],
                updated_at_ms: 0,
                origin_node: origin.to_string(),
                meta: Default::default(),
            });
        }
    }
    out.sort_by(|a, b| a.source_path.cmp(&b.source_path));
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workdir_scan_skips_build_dir_and_symlinks_that_escape() {
        let root = std::env::temp_dir().join(format!("xmesh-tree-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        std::fs::create_dir_all(root.join("src")).unwrap();
        std::fs::create_dir_all(root.join("target")).unwrap();
        std::fs::write(root.join("src/lib.rs"), "fn apply_diff() {}\n").unwrap();
        std::fs::write(root.join("target/junk.rs"), "fn hidden() {}\n").unwrap();
        let outside = std::env::temp_dir().join(format!("xmesh-outside-{}", std::process::id()));
        let _ = std::fs::remove_file(&outside);
        std::fs::write(&outside, "secret\n").unwrap();
        let _ = std::os::unix::fs::symlink(&outside, root.join("src/leak.txt"));

        let found = scan_workdir(&root, "demo", "node-a").unwrap();
        let paths: Vec<_> = found.iter().map(|e| e.source_path.as_str()).collect();
        assert!(paths.contains(&"src/lib.rs"), "{paths:?}");
        assert!(paths
            .iter()
            .all(|p| !p.contains("target") && !p.contains("leak")));
        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_file(&outside);
    }
}
