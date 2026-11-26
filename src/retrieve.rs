//! Prompt assembly for collaborative coding.
//!
//! Embeddings are used when both the query and a source chunk have the same
//! dimension. Otherwise chunks are ranked by token overlap so a deployment
//! without an embedding model still feeds the repository to the assistant.

use std::collections::HashSet;

pub const ENTRY_DYNAMIC: i32 = 1;
pub const ENTRY_PINNED: i32 = 2;
pub const ENTRY_LEARNED: i32 = 3;

#[derive(Clone, Debug)]
pub struct PromptEntry {
    pub entry_type: i32,
    pub source_path: String,
    pub origin: String,
    pub content: String,
    pub embedding: Vec<f32>,
}

pub fn cosine_sim(a: &[f32], b: &[f32]) -> f32 {
    if a.is_empty() || a.len() != b.len() {
        return 0.0;
    }
    let (mut dot, mut na, mut nb) = (0.0f32, 0.0f32, 0.0f32);
    for (x, y) in a.iter().zip(b.iter()) {
        dot += x * y;
        na += x * x;
        nb += y * y;
    }
    if na == 0.0 || nb == 0.0 {
        0.0
    } else {
        dot / (na.sqrt() * nb.sqrt())
    }
}

pub fn tokens(text: &str) -> HashSet<String> {
    text.split(|c: char| !c.is_ascii_alphanumeric())
        .filter(|t| t.len() >= 2)
        .map(|t| t.to_ascii_lowercase())
        .collect()
}

pub fn lexical_score(query: &str, content: &str) -> f32 {
    let q = tokens(query);
    if q.is_empty() {
        return 0.0;
    }
    let c = tokens(content);
    let hit = q.iter().filter(|t| c.contains(*t)).count();
    hit as f32 / q.len() as f32
}

pub fn coding_preamble(channel: &str) -> String {
    format!(
        "You are a collaborative coding assistant in channel `{channel}`.\n\
         The path index and source blocks below are this channel's project. Read them and answer from them, including a general question about what the project is.\n\
         Do not claim the source is unavailable when those blocks are present.\n\
         When you change code, include one unified diff inside a ```diff fence that `git apply` can consume, then explain the change briefly.\n\
         Prefer small reviewable edits. Pinned and learned notes are invariants.\n\
         The daemon process runs as its operating-system user. You decide whether an action is safe to propose. The operating system enforces that user's rights."
    )
}

fn is_overview(path: &str) -> bool {
    let lower = path.to_ascii_lowercase();
    let name = lower.rsplit('/').next().unwrap_or(&lower);
    name.starts_with("readme")
        || name == "cargo.toml"
        || name == "package.json"
        || name == "go.mod"
        || name == "pyproject.toml"
        || name == "agents.md"
        || lower.starts_with("docs/")
        || lower.contains("/docs/")
}

fn truncate(text: &str, max_chars: usize) -> String {
    if text.chars().count() <= max_chars {
        return text.to_string();
    }
    let mut out: String = text.chars().take(max_chars).collect();
    out.push_str("\n…[truncated]");
    out
}

fn is_always_included(entry: &PromptEntry) -> bool {
    entry.entry_type == ENTRY_PINNED
        || entry.entry_type == ENTRY_LEARNED
        || entry.source_path.starts_with("@diff:")
}

/// Build the system context block.
///
/// `top_k` limits ranked source files. Pinned notes, learned notes, and
/// `@diff:` snapshots are always included and do not consume `top_k`.
pub fn assemble_context(
    entries: &[PromptEntry],
    query: &str,
    query_emb: &[f32],
    top_k: usize,
    char_budget: usize,
) -> String {
    let mut blocks: Vec<String> = Vec::new();
    let mut used = 0usize;

    let push = |blocks: &mut Vec<String>, used: &mut usize, text: String| {
        if text.is_empty() {
            return;
        }
        *used += text.len();
        blocks.push(text);
    };

    for e in entries.iter().filter(|e| is_always_included(e)) {
        let label = if e.source_path.starts_with("@diff:") {
            "DIFF"
        } else if e.entry_type == ENTRY_PINNED {
            "PINNED"
        } else {
            "LEARNED"
        };
        let body = truncate(&e.content, 8_000);
        push(
            &mut blocks,
            &mut used,
            format!("### [{label}] {}\n{body}", e.source_path),
        );
    }

    let mut ranked: Vec<(f32, &PromptEntry)> = Vec::new();
    let embeddings_ok = !query_emb.is_empty();
    for e in entries
        .iter()
        .filter(|e| e.entry_type == ENTRY_DYNAMIC && !is_always_included(e))
    {
        let lexical = lexical_score(query, &e.content).max(lexical_score(query, &e.source_path));
        let score =
            if embeddings_ok && e.embedding.len() == query_emb.len() && !e.embedding.is_empty() {
                cosine_sim(&e.embedding, query_emb)
            } else {
                lexical
            };
        if score > 0.0 || query.trim().is_empty() {
            ranked.push((score, e));
        }
    }
    ranked.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));

    for e in entries
        .iter()
        .filter(|e| e.entry_type == ENTRY_DYNAMIC && is_overview(&e.source_path))
    {
        if used >= char_budget {
            break;
        }
        let room = char_budget.saturating_sub(used).min(6_000);
        if room < 200 {
            break;
        }
        let body = truncate(&e.content, room);
        push(
            &mut blocks,
            &mut used,
            format!("### [CODE: {} @ {}]\n{body}", e.source_path, e.origin),
        );
    }

    let positive = ranked.iter().any(|(score, _)| *score > 0.0);
    let ranked = if positive {
        ranked
    } else {
        // A question that shares no tokens with the source, such as a
        // description request in another language, still needs the tree.
        let mut fill: Vec<(f32, &PromptEntry)> = entries
            .iter()
            .filter(|e| e.entry_type == ENTRY_DYNAMIC && !is_always_included(e))
            .map(|e| {
                let prefer = if e.source_path.contains("src/") || e.source_path.starts_with("src/")
                {
                    1.0
                } else {
                    0.1
                };
                (prefer, e)
            })
            .collect();
        fill.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
        fill
    };

    for (_, e) in ranked
        .into_iter()
        .filter(|(_, entry)| !is_overview(&entry.source_path))
        .take(top_k)
    {
        if used >= char_budget {
            break;
        }
        let room = char_budget.saturating_sub(used).min(6_000);
        if room < 200 {
            break;
        }
        let body = truncate(&e.content, room);
        push(
            &mut blocks,
            &mut used,
            format!("### [CODE: {} @ {}]\n{body}", e.source_path, e.origin),
        );
    }

    blocks.join("\n\n")
}

pub fn map_entries<'a>(
    entries: impl IntoIterator<Item = &'a crate::storage::pb::ContextEntry>,
) -> Vec<PromptEntry> {
    entries
        .into_iter()
        .map(|e| PromptEntry {
            entry_type: e.entry_type,
            source_path: e.source_path.clone(),
            origin: e.origin_node.clone(),
            content: e.content.clone(),
            embedding: e.embedding.clone(),
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(kind: i32, path: &str, content: &str) -> PromptEntry {
        PromptEntry {
            entry_type: kind,
            source_path: path.into(),
            origin: "node".into(),
            content: content.into(),
            embedding: vec![],
        }
    }

    #[test]
    fn cosine_rejects_mismatched_dimensions() {
        assert_eq!(ENTRY_DYNAMIC, 1);
        assert_eq!(cosine_sim(&[1.0, 0.0], &[1.0, 0.0, 0.0]), 0.0);
        assert!((cosine_sim(&[1.0, 0.0], &[1.0, 0.0]) - 1.0).abs() < 1e-6);
        assert_eq!(cosine_sim(&[], &[1.0]), 0.0);
    }

    #[test]
    fn lexical_overlap_is_a_fraction_of_query_terms() {
        let score = lexical_score(
            "apply unified diff",
            "the executor can apply a unified diff",
        );
        assert!(score > 0.5, "{score}");
        assert_eq!(lexical_score("a", "nothing"), 0.0);
    }

    #[test]
    fn assemble_keeps_pins_and_ranks_source_by_tokens() {
        let entries = vec![
            entry(
                ENTRY_PINNED,
                "memory/invariants",
                "never disable auth checks",
            ),
            entry(ENTRY_DYNAMIC, "src/unrelated.rs", "fn paint_pixels() {}"),
            entry(
                ENTRY_DYNAMIC,
                "src/executor.rs",
                "fn apply_unified_diff() {}",
            ),
            entry(ENTRY_DYNAMIC, "@diff:node", "diff --git a/src/executor.rs"),
        ];
        let ctx = assemble_context(&entries, "apply unified diff in executor", &[], 1, 20_000);
        assert!(ctx.contains("PINNED"));
        assert!(ctx.contains("never disable auth"));
        assert!(ctx.contains("DIFF"));
        assert!(ctx.contains("src/executor.rs"));
        assert!(!ctx.contains("paint_pixels"));
    }

    #[test]
    fn a_question_with_no_shared_tokens_still_sees_the_project() {
        let entries = vec![
            entry(ENTRY_DYNAMIC, "README.md", "xenon mesh hub and node"),
            entry(ENTRY_DYNAMIC, "src/main.rs", "fn main() {}"),
        ];
        let ctx = assemble_context(&entries, "дай описание о чем этот проект", &[], 4, 20_000);
        assert!(ctx.contains("README.md"), "{ctx}");
        assert!(ctx.contains("src/main.rs"), "{ctx}");
    }

    #[test]
    fn embeddings_outrank_a_lexical_miss_when_dimensions_match() {
        let mut relevant = entry(ENTRY_DYNAMIC, "src/a.rs", "zzzz unrelated words");
        relevant.embedding = vec![1.0, 0.0];
        let mut distractor = entry(ENTRY_DYNAMIC, "src/b.rs", "executor apply diff");
        distractor.embedding = vec![0.0, 1.0];
        let ctx = assemble_context(
            &[relevant, distractor],
            "executor apply diff",
            &[1.0, 0.0],
            1,
            20_000,
        );
        assert!(ctx.contains("src/a.rs"));
        assert!(!ctx.contains("src/b.rs"));
    }
}
