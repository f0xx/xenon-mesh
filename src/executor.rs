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
    bail!("Path traversal blocked: {:?} is outside allowed_workspaces", canon);
}

pub async fn run_node_task(cfg: &AppConfig, req: &NodeTaskRequest, key: &[u8; 32]) -> Result<NodeTaskResponse> {
    verify_task_sig(req, key)?;

    if !req.unified_diff.is_empty() {
        let repo_dir = resolve_safe_workspace(&req.target_repo, &cfg.node.allowed_workspaces)?;
        let _ = Command::new("git").args(["stash", "push", "-m", &req.task_id]).current_dir(&repo_dir).output().await;

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
        let pipe = cfg.deploy_pipelines.get(&req.deploy_pipeline)
            .context("Unknown deploy pipeline")?;
        return exec_pipeline(&req.task_id, pipe).await;
    }

    Ok(NodeTaskResponse { task_id: req.task_id.clone(), success: true, exit_code: 0, ..Default::default() })
}

async fn exec_pipeline(task_id: &str, pipe: &DeployPipeline) -> Result<NodeTaskResponse> {
    let mut cmd = if pipe.use_bwrap {
        let mut c = Command::new("bwrap");
        c.args(["--ro-bind", "/", "/", "--dev", "/dev", "--proc", "/proc", "--tmpfs", "/tmp"]);
        c.args(["--bind", &pipe.workdir, &pipe.workdir, "--chdir", &pipe.workdir]);
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

    let out = cmd.output().await?;
    Ok(NodeTaskResponse {
        task_id: task_id.to_string(),
        success: out.status.success(),
        exit_code: out.status.code().unwrap_or(-1),
        stdout_log: String::from_utf8_lossy(&out.stdout).into_owned(),
        stderr_log: String::from_utf8_lossy(&out.stderr).into_owned(),
    })
}

