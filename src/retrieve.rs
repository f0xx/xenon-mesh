//! Prompt assembly for collaborative coding.
//!
//! Embeddings are used when both the query and a source chunk have the same
//! dimension. Otherwise chunks are ranked by token overlap so a deployment
//! without an embedding model still feeds the repository to the assistant.

use std::collections::HashSet;

pub const ENTRY_DYNAMIC: i32 = 1;
pub const ENTRY_PINNED: i32 = 2;
pub const ENTRY_LEARNED: i32 = 3;
pub const ENTRY_EXEMPLAR: i32 = 4;

#[derive(Clone, Debug)]
pub struct PromptEntry {
    pub id: String,
    pub entry_type: i32,
    pub source_path: String,
    pub origin: String,
    pub content: String,
    pub embedding: Vec<f32>,
    /// Multiplier for a learned note. Below 0.2 the note is omitted.
    pub utility: f32,
    pub updated_at_ms: i64,
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

/// Cosine when dimensions match, mixed with token overlap. Lexical only otherwise.
pub fn rank_score(
    query: &str,
    content: &str,
    path: &str,
    embedding: &[f32],
    query_emb: &[f32],
) -> f32 {
    let lexical = lexical_score(query, content).max(lexical_score(query, path));
    if !query_emb.is_empty() && embedding.len() == query_emb.len() && !embedding.is_empty() {
        let dense = cosine_sim(embedding, query_emb).max(0.0);
        0.7 * dense + 0.3 * lexical
    } else {
        lexical
    }
}

/// `half_life_days` of 0, or a missing timestamp, leaves utility unchanged.
pub fn decayed_utility(utility: f32, updated_at_ms: i64, now_ms: i64, half_life_days: f32) -> f32 {
    if half_life_days <= 0.0 || updated_at_ms <= 0 || now_ms <= updated_at_ms {
        return utility;
    }
    let days = (now_ms - updated_at_ms) as f32 / 86_400_000.0;
    utility * (-std::f32::consts::LN_2 * days / half_life_days).exp()
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
         Source blocks are numbered `NNNNN|`. That number is the line in the file. Use those numbers in the @@ hunk header.\n\
         Do not copy the `NNNNN|` prefix into the diff or into other code fences. A hunk with invented line numbers will not apply.\n\
         Prefer small reviewable edits. Pinned and learned notes are invariants.\n\
         The daemon process runs as its operating-system user. You decide whether an action is safe to propose. The operating system enforces that user's rights.\n\
         Facts that are not in the project blocks or in a Web results block are not known to you. Do not invent web pages, versions, or current events.\n\
         The first context is a slice of the project, not the whole tree. Paths you were not shown are still in the repository.\n\
         When you need another file or a symbol before answering, end the reply with fenced blocks:\n\
         - ```read with `<path>` or `<path>:<start>-<end>` (up to 3 paths per step). Paths stay inside the project.\n\
         - ```grep with a substring to search across the repository. This is not a regular expression.\n\
         - ```web with a search query, or ```fetch with one http(s) URL\n\
         - ```agent with a first line `provider/model` and the task below it, when a sub-agent should do that part. The scheduler may refuse a busy host. Then continue yourself.\n\
         The daemon fetches the numbered lines and calls you again. Stop asking once you can answer. A clarification from the operator during this reply is extra instruction, not a new project context."
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

fn remember(touched: &mut Vec<String>, entry: &PromptEntry) {
    if !entry.id.is_empty() {
        touched.push(entry.id.clone());
    }
}

fn is_always_included(entry: &PromptEntry) -> bool {
    entry.entry_type == ENTRY_PINNED || entry.source_path.starts_with("@diff:")
}

/// Build the system context block.
///
/// `top_k` limits ranked source files. Pinned notes and `@diff:` snapshots
/// are always included and do not consume `top_k`. Learned notes are ranked
/// against the question and at most five that overlap it are included.
/// `chunk_chars` is the per-file cap for numbered source.
/// Learned notes are at most five. Exemplars, verified diffs from an
/// upvoted reply, are at most two. `used` receives the ids of those notes.
pub fn assemble_context(
    entries: &[PromptEntry],
    query: &str,
    query_emb: &[f32],
    top_k: usize,
    char_budget: usize,
    chunk_chars: usize,
    touched: &mut Vec<String>,
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
        } else {
            "PINNED"
        };
        let body = truncate(&e.content, 8_000);
        push(
            &mut blocks,
            &mut used,
            format!("### [{label}] {}\n{body}", e.source_path),
        );
    }

    let mut learned: Vec<(f32, &PromptEntry)> = entries
        .iter()
        .filter(|e| e.entry_type == ENTRY_LEARNED && e.utility >= 0.2)
        .map(|e| {
            let s = rank_score(query, &e.content, &e.source_path, &e.embedding, query_emb)
                * e.utility;
            (s, e)
        })
        .collect();
    learned.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    let query_blank = query.trim().is_empty();
    for (_, e) in learned
        .into_iter()
        .filter(|(score, _)| query_blank || *score > 0.0)
        .take(if query_blank { 3 } else { 5 })
    {
        if used >= char_budget {
            break;
        }
        let body = truncate(&e.content, 1_500);
        let heading = if e.content.trim_start().starts_with("[AVOID]") {
            "LEARNED ANTI-PATTERN"
        } else {
            "LEARNED PATTERN"
        };
        push(
            &mut blocks,
            &mut used,
            format!("### [{heading}] {}\n{body}", e.source_path),
        );
        remember(touched, e);
    }

    let mut exemplars: Vec<(f32, &PromptEntry)> = entries
        .iter()
        .filter(|e| e.entry_type == ENTRY_EXEMPLAR && e.utility >= 0.2)
        .map(|e| {
            let s = rank_score(query, &e.content, &e.source_path, &e.embedding, query_emb)
                * e.utility;
            (s, e)
        })
        .collect();
    exemplars.sort_by(|a, b| b.0.partial_cmp(&a.0).unwrap_or(std::cmp::Ordering::Equal));
    for (_, e) in exemplars
        .into_iter()
        .filter(|(score, _)| query_blank || *score > 0.0)
        .take(if query_blank { 1 } else { 2 })
    {
        if used >= char_budget {
            break;
        }
        let body = truncate(&e.content, 2_000);
        push(
            &mut blocks,
            &mut used,
            format!("### [EXEMPLAR] {}\n{body}", e.source_path),
        );
        remember(touched, e);
    }

    let mut ranked: Vec<(f32, &PromptEntry)> = Vec::new();
    let q_lower = query.to_ascii_lowercase();
    for e in entries
        .iter()
        .filter(|e| e.entry_type == ENTRY_DYNAMIC && !is_always_included(e))
    {
        let path_lower = e.source_path.to_ascii_lowercase();
        let file_name = path_lower.rsplit('/').next().unwrap_or(&path_lower);
        let explicit_mention = (!path_lower.is_empty() && q_lower.contains(&path_lower))
            || (file_name.len() >= 4 && file_name.contains('.') && q_lower.contains(file_name));
        let base_score = rank_score(query, &e.content, &e.source_path, &e.embedding, query_emb);
        let score = if explicit_mention {
            base_score + 2.0
        } else {
            base_score
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
        let room = char_budget.saturating_sub(used).min(chunk_chars);
        if room < 200 {
            break;
        }
        let body = number_source(&e.content, room);
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
        let room = char_budget.saturating_sub(used).min(chunk_chars);
        if room < 200 {
            break;
        }
        let body = number_source(&e.content, room);
        push(
            &mut blocks,
            &mut used,
            format!("### [CODE: {} @ {}]\n{body}", e.source_path, e.origin),
        );
    }

    blocks.join("\n\n")
}

/// File line numbers, kept inside `max_chars`. The prefix is `NNNNN|`.
pub fn number_source(text: &str, max_chars: usize) -> String {
    let mut out = String::new();
    for (i, line) in text.lines().enumerate() {
        let row = format!("{:>5}|{line}\n", i + 1);
        if out.len() + row.len() > max_chars && !out.is_empty() {
            out.push_str("…[truncated]\n");
            break;
        }
        out.push_str(&row);
    }
    out
}

pub fn map_entries<'a>(
    entries: impl IntoIterator<Item = &'a crate::storage::pb::ContextEntry>,
) -> Vec<PromptEntry> {
    entries
        .into_iter()
        .map(|e| PromptEntry {
            id: e.id.clone(),
            entry_type: e.entry_type,
            source_path: e.source_path.clone(),
            origin: e.origin_node.clone(),
            content: e.content.clone(),
            embedding: e.embedding.clone(),
            utility: 1.0,
            updated_at_ms: e.updated_at_ms,
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn entry(kind: i32, path: &str, content: &str) -> PromptEntry {
        PromptEntry {
            id: String::new(),
            entry_type: kind,
            source_path: path.into(),
            origin: "node".into(),
            content: content.into(),
            embedding: vec![],
            utility: 1.0,
            updated_at_ms: 0,
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
        let ctx = assemble_context(
            &entries,
            "apply unified diff in executor",
            &[],
            1,
            20_000,
            6_000,
            &mut Vec::new(),
        );
        assert!(ctx.contains("PINNED"));
        assert!(ctx.contains("never disable auth"));
        assert!(ctx.contains("DIFF"));
        assert!(ctx.contains("src/executor.rs"));
        assert!(ctx.contains("|fn apply_unified_diff"));
        assert!(!ctx.contains("paint_pixels"));
    }

    #[test]
    fn a_question_with_no_shared_tokens_still_sees_the_project() {
        let entries = vec![
            entry(ENTRY_DYNAMIC, "README.md", "xenon mesh hub and node"),
            entry(ENTRY_DYNAMIC, "src/main.rs", "fn main() {}"),
        ];
        let ctx = assemble_context(
            &entries,
            "дай описание о чем этот проект",
            &[],
            4,
            20_000,
            6_000,
            &mut Vec::new(),
        );
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
            6_000,
            &mut Vec::new(),
        );
        assert!(ctx.contains("src/a.rs"));
        assert!(!ctx.contains("src/b.rs"));
    }

    #[test]
    fn a_named_file_outranks_a_token_match_elsewhere() {
        let entries = vec![
            entry(ENTRY_DYNAMIC, "src/other.rs", "executor apply diff"),
            entry(ENTRY_DYNAMIC, "src/executor.rs", "zzzz"),
        ];
        let ctx = assemble_context(&entries, "look at executor.rs", &[], 1, 20_000, 6_000, &mut Vec::new());
        assert!(ctx.contains("src/executor.rs"), "{ctx}");
        assert!(!ctx.contains("src/other.rs"), "{ctx}");
    }

    #[test]
    fn a_learned_note_is_kept_only_when_it_overlaps_the_question() {
        let entries = vec![
            entry(ENTRY_LEARNED, "memory/auth", "never disable auth checks"),
            entry(ENTRY_LEARNED, "memory/paint", "pixels use a linear buffer"),
        ];
        let ctx = assemble_context(&entries, "auth checks", &[], 5, 20_000, 6_000, &mut Vec::new());
        assert!(ctx.contains("LEARNED PATTERN"), "{ctx}");
        assert!(ctx.contains("never disable auth"), "{ctx}");
        assert!(!ctx.contains("linear buffer"), "{ctx}");
    }

    #[test]
    fn an_identifier_outranks_a_weak_embedding() {
        let mut conceptual = entry(ENTRY_DYNAMIC, "src/a.rs", "zzzz unrelated words");
        conceptual.embedding = vec![0.4, 1.0];
        let mut named = entry(
            ENTRY_DYNAMIC,
            "src/b.rs",
            "verify task sig checks the hmac",
        );
        named.embedding = vec![0.0, 1.0];
        let ctx = assemble_context(
            &[conceptual, named],
            "verify task sig",
            &[1.0, 0.0],
            1,
            20_000,
            6_000,
            &mut Vec::new(),
        );
        assert!(ctx.contains("src/b.rs"), "{ctx}");
        assert!(!ctx.contains("src/a.rs"), "{ctx}");
    }

    #[test]
    fn an_avoid_note_is_labeled_and_a_weak_note_is_dropped() {
        let mut weak = entry(ENTRY_LEARNED, "memory/old", "auth checks stay on");
        weak.utility = 0.1;
        let avoid = entry(ENTRY_LEARNED, "memory/fail", "[AVOID] do not invent headers");
        let ctx = assemble_context(
            &[weak, avoid],
            "invent headers",
            &[],
            5,
            20_000,
            6_000,
            &mut Vec::new(),
        );
        assert!(ctx.contains("LEARNED ANTI-PATTERN"), "{ctx}");
        assert!(!ctx.contains("auth checks stay"), "{ctx}");
    }

    #[test]
    fn utility_halves_after_one_half_life() {
        let updated = 1_000_000_i64;
        let later = updated + 10 * 86_400_000;
        let score = decayed_utility(1.0, updated, later, 10.0);
        assert!((score - 0.5).abs() < 1e-5, "{score}");
        assert_eq!(decayed_utility(1.0, updated, later, 0.0), 1.0);
    }

    #[test]
    fn an_upvoted_diff_is_an_exemplar_when_the_question_overlaps() {
        let mut kept = entry(
            ENTRY_EXEMPLAR,
            "memory/exemplar-3",
            "Task: fix the parser\n\ndiff --git a/src/parser.rs",
        );
        kept.id = "ex1".into();
        let other = entry(ENTRY_EXEMPLAR, "memory/exemplar-9", "Task: paint pixels");
        let mut used = Vec::new();
        let ctx = assemble_context(
            &[kept, other],
            "fix the parser",
            &[],
            2,
            20_000,
            6_000,
            &mut used,
        );
        assert!(ctx.contains("EXEMPLAR"), "{ctx}");
        assert!(ctx.contains("src/parser.rs"), "{ctx}");
        assert!(!ctx.contains("paint pixels"), "{ctx}");
        assert_eq!(used, vec!["ex1".to_string()]);
    }
}
