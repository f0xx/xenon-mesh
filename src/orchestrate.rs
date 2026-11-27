//! Who may run a sub-agent.
//!
//! There is one scheduler, not a vote. Each daemon publishes how many agent
//! replies it is already running and which model ids it can serve. The hub
//! keeps those reports. A node with no hub is its own hub and uses the local
//! number only.
//!
//! A report older than 60 seconds is ignored. The scheduler sends the task to
//! the least busy host that still has room and that advertised the model. If
//! nobody advertised it, the local daemon accepts the task while it is under
//! its own cap. A host at its cap is a refusal. The supervisor is told and
//! continues. Load is in-flight replies, not GPU utilisation.

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct HostLoad {
    pub node: String,
    pub inflight: u32,
    pub capacity: u32,
    pub models: Vec<String>,
    pub updated_ms: i64,
}

pub fn admit(
    local_node: &str,
    local_inflight: u32,
    local_capacity: u32,
    model: &str,
    board: &[HostLoad],
    now_ms: i64,
) -> Result<String, String> {
    let fresh: Vec<&HostLoad> = board
        .iter()
        .filter(|host| now_ms.saturating_sub(host.updated_ms) < 60_000)
        .filter(|host| host.capacity > 0)
        .collect();
    let owners: Vec<&&HostLoad> = fresh
        .iter()
        .filter(|host| host.models.iter().any(|id| id == model))
        .collect();
    if !owners.is_empty() {
        if let Some(best) = owners
            .iter()
            .filter(|host| host.inflight < host.capacity)
            .min_by_key(|host| host.inflight)
        {
            return Ok((*best).node.clone());
        }
        return Err(format!(
            "every host offering `{model}` is at capacity"
        ));
    }
    if local_inflight >= local_capacity {
        Err(format!(
            "{local_node} is at capacity ({local_inflight}/{local_capacity})"
        ))
    } else {
        Ok(local_node.to_string())
    }
}

/// First line is `provider/model` when it contains a slash and no spaces.
pub fn parse_agent_task(body: &str) -> Option<(String, String)> {
    let mut lines = body.lines().map(str::trim).filter(|l| !l.is_empty());
    let first = lines.next()?;
    let (model, rest_first) = if let Some(rest) = first.strip_prefix("model:") {
        (rest.trim(), "")
    } else if first.contains('/') && !first.contains(' ') {
        (first, "")
    } else {
        return None;
    };
    if model.is_empty() || model.len() > 120 {
        return None;
    }
    let mut task = String::new();
    if !rest_first.is_empty() {
        task.push_str(rest_first);
    }
    for line in lines {
        if !task.is_empty() {
            task.push('\n');
        }
        task.push_str(line);
    }
    let task = task.trim();
    if task.is_empty() {
        return None;
    }
    Some((model.to_string(), task.chars().take(2_000).collect()))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn host(node: &str, inflight: u32, models: &[&str], age_ms: i64) -> HostLoad {
        HostLoad {
            node: node.into(),
            inflight,
            capacity: 2,
            models: models.iter().map(|m| (*m).to_string()).collect(),
            updated_ms: 1_000_000 - age_ms,
        }
    }

    #[test]
    fn the_least_busy_advertised_host_wins() {
        let board = vec![
            host("huge", 1, &["huge/qwen"], 0),
            host("creative", 0, &["huge/qwen"], 0),
        ];
        let picked = admit("hub", 0, 4, "huge/qwen", &board, 1_000_000).unwrap();
        assert_eq!(picked, "creative");
    }

    #[test]
    fn a_full_host_is_refused_and_an_unknown_model_stays_local() {
        let board = vec![host("huge", 2, &["huge/qwen"], 0)];
        let err = admit("hub", 0, 4, "huge/qwen", &board, 1_000_000).unwrap_err();
        assert!(err.contains("capacity"), "{err}");
        let stale = vec![host("huge", 0, &["huge/qwen"], 120_000)];
        assert_eq!(
            admit("hub", 0, 4, "huge/qwen", &stale, 1_000_000).unwrap(),
            "hub"
        );
        assert!(admit("hub", 4, 4, "other/model", &[], 1_000_000).is_err());
    }

    #[test]
    fn an_agent_fence_splits_model_and_task() {
        let (model, task) = parse_agent_task("huge/qwen\ncheck the parser\n").unwrap();
        assert_eq!(model, "huge/qwen");
        assert_eq!(task, "check the parser");
        assert!(parse_agent_task("just a sentence").is_none());
    }
}
