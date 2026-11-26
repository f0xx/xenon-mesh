use crate::storage::pb::{ContextEntry, EntryType};
use anyhow::{Context, Result};
use git2::{DiffOptions, Repository};
use std::io::{Read, Write};
use std::path::Path;
use std::process::{Command, Stdio};
use std::time::Duration;
use tracing::warn;

/// A bare-repo index is a sample, not a full checkout.
/// `git` runs as a child so a huge or corrupt object can be killed.
/// An in-process inflate cannot be stopped, and a timed-out task would keep the CPU.
const BARE_SCAN_FILES: usize = 200;
const BARE_SCAN_BUDGET: Duration = Duration::from_secs(8);

pub fn scan_bare_repo(bare_path: &Path, namespace: &str) -> Result<Vec<ContextEntry>> {
    if bare_path.join(".xmeshignore").exists() {
        return Ok(vec![]);
    }

    let listing = git_budget(bare_path, &["ls-tree", "-r", "-l", "HEAD"])?;
    if !listing.status.success() && listing.stdout.is_empty() {
        let err = String::from_utf8_lossy(&listing.stderr);
        anyhow::bail!(
            "Skipping corrupt/empty repo '{namespace}' (run `git --git-dir={bare_path:?} fsck --full`): {}",
            err.trim()
        );
    }
    let text = String::from_utf8_lossy(&listing.stdout);
    let mut wanted: Vec<(String, u64, String)> = Vec::new();
    for line in text.lines() {
        if wanted.len() >= BARE_SCAN_FILES {
            break;
        }
        let Some((oid, size, path)) = parse_lstree(line) else {
            continue;
        };
        if size == 0 || size > 32 * 1024 || skip_indexed_path(&path) {
            continue;
        }
        wanted.push((oid, size, path));
    }
    if wanted.is_empty() {
        return Ok(vec![]);
    }

    let mut child = Command::new("timeout")
        .arg(BARE_SCAN_BUDGET.as_secs().to_string())
        .arg("git")
        .arg("--git-dir")
        .arg(bare_path)
        .args(["cat-file", "--batch"])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .or_else(|_| {
            Command::new("git")
                .arg("--git-dir")
                .arg(bare_path)
                .args(["cat-file", "--batch"])
                .stdin(Stdio::piped())
                .stdout(Stdio::piped())
                .stderr(Stdio::null())
                .spawn()
        })
        .context("git cat-file")?;
    {
        let mut stdin = child.stdin.take().context("git cat-file stdin")?;
        for (oid, _, _) in &wanted {
            writeln!(stdin, "{oid}")?;
        }
    }
    let mut stdout = child.stdout.take().context("git cat-file stdout")?;
    let mut entries = Vec::new();
    let mut short = false;
    for (_, size, path) in &wanted {
        let Some(body) = read_batch_object(&mut stdout, *size) else {
            short = true;
            break;
        };
        if body.contains(&0) {
            continue;
        }
        let Ok(text) = std::str::from_utf8(&body) else {
            continue;
        };
        let id = blake3::hash(format!("master:{namespace}:{path}").as_bytes())
            .to_hex()
            .to_string();
        let content_hash = blake3::hash(text.as_bytes()).to_hex().to_string();
        entries.push(ContextEntry {
            id,
            namespace: namespace.to_string(),
            entry_type: EntryType::Dynamic as i32,
            source_path: path.clone(),
            content: text.to_string(),
            content_hash,
            embedding: vec![],
            updated_at_ms: 0,
            origin_node: "git-master".to_string(),
            meta: Default::default(),
        });
    }
    let _ = child.kill();
    let _ = child.wait();
    if short {
        warn!(
            namespace,
            files = entries.len(),
            "bare repo scan stopped before every selected file was read"
        );
    }
    Ok(entries)
}

fn git_budget(bare_path: &Path, args: &[&str]) -> Result<std::process::Output> {
    let mut cmd = Command::new("timeout");
    cmd.arg(BARE_SCAN_BUDGET.as_secs().to_string())
        .arg("git")
        .arg("--git-dir")
        .arg(bare_path);
    cmd.args(args);
    cmd.output().or_else(|_| {
        let mut plain = Command::new("git");
        plain.arg("--git-dir").arg(bare_path).args(args);
        plain.output().context("git")
    })
}

fn parse_lstree(line: &str) -> Option<(String, u64, String)> {
    let (meta, path) = line.split_once('\t')?;
    let mut parts = meta.split_whitespace();
    let _mode = parts.next()?;
    if parts.next()? != "blob" {
        return None;
    }
    let oid = parts.next()?.to_string();
    let size: u64 = parts.next()?.parse().ok()?;
    if path.is_empty() {
        return None;
    }
    Some((oid, size, path.to_string()))
}

fn skip_indexed_path(path: &str) -> bool {
    let name = path.rsplit('/').next().unwrap_or(path);
    if name.ends_with(".lock")
        || name.ends_with("-lock.json")
        || name.ends_with(".min.js")
        || name.ends_with(".map")
    {
        return true;
    }
    path.split('/').any(|part| {
        SKIP_DIRS.contains(&part) || (part.starts_with('.') && part != ".gitignore")
    })
}

/// `cat-file --batch` writes `<oid> <type> <size>\n<body>\n`.
/// The size on the listing is what we asked for; a mismatch means stop.
fn read_batch_object(reader: &mut impl Read, expect: u64) -> Option<Vec<u8>> {
    let mut header = Vec::new();
    let mut byte = [0u8; 1];
    loop {
        if reader.read(&mut byte).ok()? == 0 {
            return None;
        }
        if byte[0] == b'\n' {
            break;
        }
        header.push(byte[0]);
        if header.len() > 128 {
            return None;
        }
    }
    let header = String::from_utf8(header).ok()?;
    let size: u64 = header.split_whitespace().nth(2)?.parse().ok()?;
    if size != expect || size > 32 * 1024 {
        return None;
    }
    let mut body = vec![0u8; size as usize];
    reader.read_exact(&mut body).ok()?;
    let mut nl = [0u8; 1];
    reader.read_exact(&mut nl).ok()?;
    (nl[0] == b'\n').then_some(body)
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

/// Path index plus the short project files a general question needs.
/// Build directories are skipped. The result is capped so a chat prompt stays bounded.
pub fn project_outline(root: &Path) -> Result<String> {
    let root = std::fs::canonicalize(root)?;
    let mut paths = Vec::new();
    let mut stack = vec![root.clone()];
    while let Some(dir) = stack.pop() {
        let Ok(entries) = std::fs::read_dir(&dir) else {
            continue;
        };
        for ent in entries.flatten() {
            if paths.len() >= 500 {
                break;
            }
            let path = ent.path();
            let name = ent.file_name();
            let name = name.to_string_lossy();
            if name.starts_with('.') && name != ".gitignore" {
                continue;
            }
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
            let rel = canon.strip_prefix(&root).unwrap_or(&canon);
            paths.push(rel.to_string_lossy().replace('\\', "/"));
        }
    }
    paths.sort();
    let mut out = String::from("### [TREE]\n");
    for path in &paths {
        out.push_str(path);
        out.push('\n');
        if out.len() > 12_000 {
            out.push_str("…\n");
            break;
        }
    }
    for path in &paths {
        let lower = path.to_ascii_lowercase();
        let name = lower.rsplit('/').next().unwrap_or(&lower);
        let overview = name.starts_with("readme")
            || name == "cargo.toml"
            || name == "package.json"
            || name == "agents.md"
            || lower.starts_with("docs/");
        if !overview {
            continue;
        }
        let full = root.join(path);
        let Ok(bytes) = std::fs::read(&full) else {
            continue;
        };
        if bytes.contains(&0) {
            continue;
        }
        let Ok(text) = std::str::from_utf8(&bytes) else {
            continue;
        };
        let body: String = text.chars().take(4_000).collect();
        out.push_str(&format!("\n### [CODE: {path}]\n{body}\n"));
        if out.len() > 28_000 {
            break;
        }
    }
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

    #[test]
    fn lstree_line_keeps_small_blobs_only() {
        let parsed = parse_lstree("100644 blob abcdef 42\tsrc/main.rs").unwrap();
        assert_eq!(parsed.0, "abcdef");
        assert_eq!(parsed.1, 42);
        assert_eq!(parsed.2, "src/main.rs");
        assert!(parse_lstree("040000 tree abc 0\tvendor").is_none());
        assert!(skip_indexed_path("vendor/lib.rs"));
        assert!(skip_indexed_path("src/app.min.js"));
        assert!(!skip_indexed_path("src/main.rs"));
    }
}
