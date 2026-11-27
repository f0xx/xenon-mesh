use crate::config::{AppConfig, DeployPipeline};
use crate::storage::pb::{NodeTaskRequest, NodeTaskResponse};
use anyhow::{bail, Context, Result};
use std::path::PathBuf;
use std::process::Stdio;
use tokio::io::AsyncWriteExt;
use tokio::process::Command;

pub fn verify_task_sig(req: &NodeTaskRequest, secret_key: &[u8; 32]) -> Result<()> {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_millis() as i64;
    if req.expires_at_ms < now_ms {
        bail!("Task token expired");
    }

    let payload = format!(
        "{}:{}:{}:{}:{}",
        req.task_id, req.target_repo, req.unified_diff, req.deploy_pipeline, req.expires_at_ms
    );
    let expected = blake3::keyed_hash(secret_key, payload.as_bytes());
    if expected.as_bytes().as_slice() != req.master_hmac_sig.as_slice() {
        bail!("Invalid HMAC signature from master");
    }
    Ok(())
}

fn resolve_safe_workspace(target: &str, allowed: &[String]) -> Result<PathBuf> {
    let canon = std::fs::canonicalize(target).context("Workspace path does not exist")?;
    for root in allowed {
        if let Ok(canon_root) = std::fs::canonicalize(root) {
            if canon.starts_with(&canon_root) {
                return Ok(canon);
            }
        }
    }
    bail!(
        "Path traversal blocked: {:?} is outside allowed_workspaces",
        canon
    );
}

pub async fn run_node_task(
    cfg: &AppConfig,
    req: &NodeTaskRequest,
    key: &[u8; 32],
) -> Result<NodeTaskResponse> {
    verify_task_sig(req, key)?;

    if !req.unified_diff.is_empty() {
        let repo_dir = resolve_safe_workspace(&req.target_repo, &cfg.node.allowed_workspaces)?;
        let _ = Command::new("git")
            .args(["stash", "push", "-m", &req.task_id])
            .current_dir(&repo_dir)
            .output()
            .await;

        let mut child = Command::new("git")
            .args(["apply", "--whitespace=nowarn", "-"])
            .current_dir(&repo_dir)
            .stdin(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()?;

        if let Some(mut stdin) = child.stdin.take() {
            stdin.write_all(req.unified_diff.as_bytes()).await?;
        }
        let out = child.wait_with_output().await?;
        if !out.status.success() {
            return Ok(NodeTaskResponse {
                task_id: req.task_id.clone(),
                success: false,
                exit_code: out.status.code().unwrap_or(1),
                stdout_log: String::new(),
                stderr_log: String::from_utf8_lossy(&out.stderr).into_owned(),
            });
        }
    }

    if !req.deploy_pipeline.is_empty() {
        let pipe = cfg
            .deploy_pipelines
            .get(&req.deploy_pipeline)
            .context("Unknown deploy pipeline")?;
        return exec_pipeline(&req.task_id, pipe).await;
    }

    Ok(NodeTaskResponse {
        task_id: req.task_id.clone(),
        success: true,
        exit_code: 0,
        ..Default::default()
    })
}

async fn exec_pipeline(task_id: &str, pipe: &DeployPipeline) -> Result<NodeTaskResponse> {
    if pipe.command.is_empty() {
        bail!("deploy pipeline command is empty");
    }
    let mut cmd = if pipe.use_bwrap {
        let mut c = Command::new("bwrap");
        c.args([
            "--ro-bind",
            "/",
            "/",
            "--dev",
            "/dev",
            "--proc",
            "/proc",
            "--tmpfs",
            "/tmp",
            "--tmpfs",
            "/etc/xmeshd",
            "--tmpfs",
            "/root",
            "--die-with-parent",
        ]);
        c.args([
            "--bind",
            &pipe.workdir,
            &pipe.workdir,
            "--chdir",
            &pipe.workdir,
        ]);
        if !pipe.allow_network {
            c.arg("--unshare-net");
        }
        c.args(&pipe.command);
        c
    } else {
        let mut c = Command::new(&pipe.command[0]);
        c.args(&pipe.command[1..]).current_dir(&pipe.workdir);
        c
    };

    let mut push = classify_push(&pipe.command, pipe.git_force, &pipe.git_ref);
    if let Some(rec) = push.as_mut() {
        rec.old_sha = git_rev(&pipe.workdir, &rec.refspec);
    }
    let out = cmd.output().await?;
    if let Some(rec) = push.as_mut() {
        rec.new_sha = git_rev(&pipe.workdir, "HEAD");
    }
    let mut stdout_log = String::from_utf8_lossy(&out.stdout).into_owned();
    if let Some(rec) = push {
        let line = serde_json::json!({
            "force": rec.force,
            "ref": rec.refspec,
            "old": rec.old_sha,
            "new": rec.new_sha
        });
        stdout_log = format!("XMESH_GIT {line}\n{stdout_log}");
    }
    Ok(NodeTaskResponse {
        task_id: task_id.to_string(),
        success: out.status.success(),
        exit_code: out.status.code().unwrap_or(-1),
        stdout_log,
        stderr_log: String::from_utf8_lossy(&out.stderr).into_owned(),
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct GitPushRecord {
    pub force: bool,
    pub refspec: String,
    pub old_sha: String,
    pub new_sha: String,
}

pub fn classify_push(argv: &[String], declared_force: bool, declared_ref: &str) -> Option<GitPushRecord> {
    let is_git = argv
        .first()
        .map(|bin| bin == "git" || bin.ends_with("/git"))
        .unwrap_or(false);
    if !is_git || !argv.iter().any(|arg| arg == "push") {
        return None;
    }
    let force = declared_force
        || argv.iter().any(|arg| {
            arg == "--force"
                || arg == "--force-with-lease"
                || (arg.starts_with('+') && arg.contains(':'))
        });
    let refspec = if !declared_ref.is_empty() {
        declared_ref.to_string()
    } else {
        argv.iter()
            .find(|arg| arg.contains(':') && !arg.starts_with('-'))
            .cloned()
            .unwrap_or_default()
    };
    Some(GitPushRecord {
        force,
        refspec,
        old_sha: String::new(),
        new_sha: String::new(),
    })
}

fn git_rev(workdir: &str, rev: &str) -> String {
    if rev.is_empty() || workdir.is_empty() {
        return String::new();
    }
    let spec = rev.split(':').next_back().unwrap_or(rev);
    let out = std::process::Command::new("git")
        .args(["rev-parse", "--verify", spec])
        .current_dir(workdir)
        .output();
    match out {
        Ok(out) if out.status.success() => String::from_utf8_lossy(&out.stdout).trim().to_string(),
        _ => String::new(),
    }
}

pub fn inspect_diff(diff: &str) -> Vec<String> {
    let mut notes = Vec::new();
    if diff.trim().is_empty() {
        notes.push("diff is empty".into());
        return notes;
    }
    let files = diff
        .lines()
        .filter(|line| line.starts_with("diff --git "))
        .count();
    let adds = diff
        .lines()
        .filter(|line| line.starts_with('+') && !line.starts_with("+++"))
        .count();
    let dels = diff
        .lines()
        .filter(|line| line.starts_with('-') && !line.starts_with("---"))
        .count();
    notes.push(format!("{files} files, +{adds} / -{dels}"));
    if diff.contains("<<<<<<<") || diff.contains(">>>>>>>") {
        notes.push("conflict markers in the diff".into());
    }
    if adds + dels > 2000 {
        notes.push("large diff; review it before approving".into());
    }
    notes.extend(lint_added_lines(diff));
    notes
}

/// One finding per added line that looks wrong. This does not apply the diff.
pub fn lint_added_lines(diff: &str) -> Vec<String> {
    let mut file = String::new();
    let mut new_line = 0usize;
    let mut notes = Vec::new();
    for line in diff.lines() {
        if let Some(rest) = line.strip_prefix("diff --git ") {
            file = rest
                .split_whitespace()
                .nth(1)
                .unwrap_or("")
                .trim_start_matches("b/")
                .to_string();
            new_line = 0;
            continue;
        }
        if let Some(rest) = line.strip_prefix("@@") {
            if let Some(plus) = rest.split_whitespace().find(|p| p.starts_with('+')) {
                let n = plus.trim_start_matches('+').split(',').next().unwrap_or("0");
                new_line = n.parse().unwrap_or(0);
            }
            continue;
        }
        if line.starts_with('+') && !line.starts_with("+++") {
            let body = &line[1..];
            if body.ends_with(' ') || body.ends_with('\t') {
                notes.push(format!("lint: {file}:{new_line}: trailing whitespace"));
            }
            if body.chars().count() > 160 {
                notes.push(format!(
                    "lint: {file}:{new_line}: line is longer than 160 characters"
                ));
            }
            new_line = new_line.saturating_add(1);
        } else if line.starts_with('-') && !line.starts_with("---") {
        } else if !line.starts_with('\\') && new_line > 0 {
            new_line = new_line.saturating_add(1);
        }
        if notes.len() >= 20 {
            break;
        }
    }
    notes
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::storage::pb::NodeTaskRequest;

    #[test]
    fn push_classification_records_force_and_ref() {
        let argv = vec![
            "git".into(),
            "push".into(),
            "--force-with-lease".into(),
            "origin".into(),
            "HEAD:main".into(),
        ];
        let rec = classify_push(&argv, false, "").unwrap();
        assert!(rec.force);
        assert_eq!(rec.refspec, "HEAD:main");
        let plain = classify_push(&["git".into(), "push".into(), "origin".into()], false, "main").unwrap();
        assert!(!plain.force);
        assert_eq!(plain.refspec, "main");
        assert!(classify_push(&["cargo".into(), "test".into()], false, "").is_none());
    }

    #[test]
    fn diff_inspection_flags_conflict_markers() {
        let notes = inspect_diff("diff --git a/a.rs b/a.rs\n+<<<<<<< ours\n+ok\n");
        assert!(notes.iter().any(|n| n.contains("conflict")));
        assert!(inspect_diff("").iter().any(|n| n.contains("empty")));
        let lint = lint_added_lines(
            "diff --git a/a.rs b/a.rs\n@@ -1 +1 @@\n-old\n+kept \n+short\n",
        );
        assert!(lint.iter().any(|n| n.contains("trailing whitespace")), "{lint:?}");
        assert!(!lint.iter().any(|n| n.contains("short")));
    }

    fn signed(req: &mut NodeTaskRequest, key: &[u8; 32]) {
        let payload = format!(
            "{}:{}:{}:{}:{}",
            req.task_id, req.target_repo, req.unified_diff, req.deploy_pipeline, req.expires_at_ms
        );
        req.master_hmac_sig = blake3::keyed_hash(key, payload.as_bytes())
            .as_bytes()
            .to_vec();
    }

    #[test]
    fn hmac_accepts_a_live_token_and_rejects_tampering() {
        let key = [7u8; 32];
        let mut req = NodeTaskRequest {
            task_id: "t1".into(),
            target_repo: "/srv/app".into(),
            unified_diff: "diff".into(),
            deploy_pipeline: "test".into(),
            expires_at_ms: crate::storage::now_ms() + 60_000,
            ..Default::default()
        };
        signed(&mut req, &key);
        assert!(verify_task_sig(&req, &key).is_ok());
        req.unified_diff = "other".into();
        assert!(verify_task_sig(&req, &key).is_err());
        signed(&mut req, &key);
        req.expires_at_ms = 1;
        signed(&mut req, &key);
        assert!(verify_task_sig(&req, &key).is_err());
    }

    #[test]
    fn workspace_must_stay_inside_an_allowed_root() {
        let root = std::env::temp_dir().join(format!("xmesh-allow-{}", std::process::id()));
        let nested = root.join("proj");
        let outside = std::env::temp_dir().join(format!("xmesh-deny-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(&outside);
        std::fs::create_dir_all(&nested).unwrap();
        std::fs::create_dir_all(&outside).unwrap();
        let allowed = vec![root.to_string_lossy().to_string()];
        assert!(resolve_safe_workspace(nested.to_str().unwrap(), &allowed).is_ok());
        assert!(resolve_safe_workspace(outside.to_str().unwrap(), &allowed).is_err());
        let _ = std::fs::remove_dir_all(&root);
        let _ = std::fs::remove_dir_all(&outside);
    }
}
