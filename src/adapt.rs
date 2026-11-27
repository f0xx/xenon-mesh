//! Operator preference pairs and the marker a training pipeline prints.
//!
//! Weight updates stay outside this process. The daemon only records a vote,
//! pairs a preferred reply with a rejected one for the same prompt, and
//! exports JSONL. A model-vs-model debate is not a pair.

use serde_json::Value;

#[derive(Clone, Debug)]
pub struct RatedTurn {
    pub seq: u64,
    pub human: bool,
    pub content: String,
    pub vote: i32,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PreferenceDraft {
    pub prompt_seq: u64,
    pub prompt: String,
    pub chosen_seq: u64,
    pub chosen: String,
    pub rejected_seq: u64,
    pub rejected: String,
}

pub fn pair_id(channel: &str, thread: &str, prompt_seq: u64) -> String {
    blake3::hash(format!("{channel}:{thread}:{prompt_seq}:operator_vote").as_bytes())
        .to_hex()
        .to_string()
}

/// One pair per human prompt that has both a positive and a negative vote
/// on later non-human replies. The latest vote of each sign wins.
pub fn synthesize_pairs(turns: &[RatedTurn]) -> Vec<PreferenceDraft> {
    let mut prompt_seq = 0u64;
    let mut prompt = String::new();
    let mut have_prompt = false;
    let mut groups: Vec<Group> = Vec::new();

    for turn in turns {
        if turn.human {
            prompt_seq = turn.seq;
            prompt = turn.content.clone();
            have_prompt = true;
            continue;
        }
        if !have_prompt || turn.vote == 0 {
            continue;
        }
        let group = match groups.iter_mut().find(|g| g.prompt_seq == prompt_seq) {
            Some(group) => group,
            None => {
                groups.push(Group {
                    prompt_seq,
                    prompt: prompt.clone(),
                    pos: None,
                    neg: None,
                });
                groups.last_mut().unwrap()
            }
        };
        let slot = if turn.vote > 0 {
            &mut group.pos
        } else {
            &mut group.neg
        };
        if slot.as_ref().map(|(seq, _)| turn.seq >= *seq).unwrap_or(true) {
            *slot = Some((turn.seq, turn.content.clone()));
        }
    }

    groups
        .into_iter()
        .filter_map(|group| {
            let (chosen_seq, chosen) = group.pos?;
            let (rejected_seq, rejected) = group.neg?;
            if group.prompt.trim().is_empty()
                || chosen.trim().is_empty()
                || rejected.trim().is_empty()
                || chosen.trim() == rejected.trim()
            {
                return None;
            }
            Some(PreferenceDraft {
                prompt_seq: group.prompt_seq,
                prompt: group.prompt,
                chosen_seq,
                chosen,
                rejected_seq,
                rejected,
            })
        })
        .collect()
}

struct Group {
    prompt_seq: u64,
    prompt: String,
    pos: Option<(u64, String)>,
    neg: Option<(u64, String)>,
}

/// An upvoted reply that contains a diff fence becomes an exemplar.
/// The human prompt is the task. No model call is involved.
pub fn exemplar_from_reply(prompt: &str, reply: &str) -> Option<String> {
    let start = reply.find("```diff\n")?;
    let rest = &reply[start + "```diff\n".len()..];
    let end = rest.find("```")?;
    let diff = rest[..end].trim();
    if diff.is_empty() {
        return None;
    }
    let task: String = prompt.trim().chars().take(300).collect();
    let diff: String = diff.chars().take(3_500).collect();
    if task.is_empty() {
        return None;
    }
    Some(format!("Task: {task}\n\n{diff}"))
}

/// First vote starts at 1. A later upvote adds 1. A later downvote subtracts 2.
pub fn next_score(current: Option<i32>, vote: i32) -> i32 {
    match current {
        None => 1,
        Some(score) => score + if vote > 0 { 1 } else { -2 },
    }
}

/// Scores below -3 are deleted and leave the prompt.
pub fn score_survives(score: i32) -> bool {
    score >= -3
}

/// A note attached to a vote becomes a learned entry. A downvote is an
/// anti-pattern. An empty note stores nothing.
pub fn feedback_note(vote: i32, note: &str) -> Option<String> {
    let note = note.trim();
    if note.is_empty() {
        return None;
    }
    if vote < 0 {
        Some(format!("[AVOID] {note}"))
    } else {
        Some(note.to_string())
    }
}

pub fn preference_line(
    channel: &str,
    thread: &str,
    prompt: &str,
    chosen: &str,
    rejected: &str,
) -> String {
    serde_json::json!({
        "prompt": prompt,
        "chosen": chosen,
        "rejected": rejected,
        "chosen_weight": 1.0,
        "rejected_weight": 0.0,
        "source": "operator_vote",
        "channel": channel,
        "thread": thread,
    })
    .to_string()
}

/// Last `XMESH_LORA {json}` line in a pipeline's stdout.
pub fn parse_lora_marker(stdout: &str) -> Option<Value> {
    stdout.lines().rev().find_map(|line| {
        let rest = line.trim().strip_prefix("XMESH_LORA ")?;
        serde_json::from_str(rest).ok()
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::process::Command;

    fn turn(seq: u64, human: bool, content: &str, vote: i32) -> RatedTurn {
        RatedTurn {
            seq,
            human,
            content: content.into(),
            vote,
        }
    }

    #[test]
    fn a_pair_needs_both_signs_on_the_same_prompt() {
        let turns = vec![
            turn(1, true, "fix the parser", 0),
            turn(2, false, "bad diff", -1),
            turn(3, false, "good diff", 1),
            turn(4, true, "another question", 0),
            turn(5, false, "only one reply", 1),
            turn(6, false, "same text", -1),
            turn(7, false, "same text", 1),
        ];
        let pairs = synthesize_pairs(&turns);
        assert_eq!(pairs.len(), 1);
        assert_eq!(pairs[0].prompt_seq, 1);
        assert_eq!(pairs[0].chosen, "good diff");
        assert_eq!(pairs[0].rejected, "bad diff");
        assert!(synthesize_pairs(&[turn(1, true, "q", 1)]).is_empty());
    }

    #[test]
    fn the_later_vote_of_the_same_sign_replaces_the_earlier_one() {
        let turns = vec![
            turn(1, true, "prompt", 0),
            turn(2, false, "first bad", -1),
            turn(3, false, "second bad", -1),
            turn(4, false, "good", 1),
        ];
        let pairs = synthesize_pairs(&turns);
        assert_eq!(pairs[0].rejected, "second bad");
    }

    #[test]
    fn an_upvoted_diff_fence_is_an_exemplar() {
        let body = exemplar_from_reply(
            "fix the parser",
            "here\n```diff\ndiff --git a/src/parser.rs b/src/parser.rs\n+ok\n```\n",
        );
        assert!(body.unwrap().contains("diff --git a/src/parser.rs"));
        assert!(exemplar_from_reply("fix", "no fence").is_none());
    }

    #[test]
    fn a_downvote_note_is_an_avoid_entry() {
        assert_eq!(
            feedback_note(-1, " do not invent headers ").as_deref(),
            Some("[AVOID] do not invent headers")
        );
        assert_eq!(
            feedback_note(1, "match existing errors").as_deref(),
            Some("match existing errors")
        );
        assert!(feedback_note(1, "  ").is_none());
        assert_eq!(next_score(None, 1), 1);
        assert_eq!(next_score(Some(1), -1), -1);
        assert!(score_survives(-3));
        assert!(!score_survives(next_score(Some(-3), -1)));
    }

    #[test]
    fn the_export_line_names_the_pair_and_not_a_debate() {
        let line = preference_line("demo", "main", "prompt", "yes", "no");
        let v: Value = serde_json::from_str(&line).unwrap();
        assert_eq!(v["source"], "operator_vote");
        assert_eq!(v["chosen_weight"], 1.0);
        assert_eq!(v["rejected"], "no");
        assert!(parse_lora_marker("noise\nXMESH_LORA {\"adapter_name\":\"xmesh-lora\",\"pairs_used\":4}\n").is_some());
        assert!(parse_lora_marker("no marker").is_none());
    }

    #[test]
    fn the_training_script_filters_pairs_without_a_gpu() {
        let script = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("scripts/train_dpo_lora.py");
        let out = Command::new("python3")
            .arg(&script)
            .arg("--self-test")
            .output()
            .expect("python3");
        assert!(
            out.status.success(),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
}
