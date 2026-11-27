# Continuous adaptation

A lab with little or no free VRAM still adapts. The daemon keeps what an operator kept: a short note, a verified diff, and a preference pair. The next prompt reads those rows. No gradient step runs inside `xmeshd`, and no model is called to invent the memory. Weight training is a separate pipeline the operator starts when a machine has the base weights and the spare time.

## DR-42. What is adapted

| Tier | Where it lives | What changes |
| --- | --- | --- |
| Notes and exemplars | This process, on each prompt | A learned note, or a diff the operator marked useful |
| Preference log | This process's database | Pairs of a preferred reply and a rejected reply |
| Weights | A machine the operator chooses, outside the daemon | An optional LoRA run from the exported file |

A private channel's pairs stay on the node that recorded the vote. They are not part of the sync envelope and they are not copied to the hub. A public channel's vote is still stored on the daemon that received it. Export is a local HTTP call.

## DR-43. Retrieval

When a chunk and the question share an embedding, the rank is `0.7 * max(0, cosine) + 0.3 * lexical`. Lexical overlap counts the file path as well as the text, so an exact identifier still matters when the embedding is only loosely related. If either side has no embedding, or the lengths differ, the rank is lexical only. Naming a file still adds the existing path bonus.

A learned note whose text starts with `[AVOID]` is inserted as an anti-pattern. Any other learned note is inserted as a pattern. Notes are still limited to the ones that overlap the question.

An upvoted reply that contains a `diff` fence is stored as an exemplar (`entry_type` 4). The human prompt is the task text, capped at 300 characters, and the fence is the diff. The next prompt includes at most two exemplars that overlap the question, under `### [EXEMPLAR]`. A downvote of that reply deletes the exemplar. This is the zero-VRAM path: every configured model sees the diff on the next question, and nothing is loaded into a GPU to store it.

`entry_stats` holds `score` and `hits` for those rows. The first vote sets `score` to 1. A later upvote of the same reply adds 1. A later downvote subtracts 2. A score below -3 deletes the row. While a row is assembled into a prompt, `hits` increases by one. A score below -3 is omitted from ranking; any other stored score ranks at least at utility `0.2`, and a higher score ranks higher. `adapt.learned_half_life_days` above zero then multiplies that utility by exponential decay from `updated_at_ms`. The default is `0`, which does not decay.

The operator writes the note, either in the memory box (start the text with `[AVOID]` for an anti-pattern) or as `note` on the vote. The daemon does not summarize a debate into a note.

## DR-44. Preference pairs

`POST /api/chat/feedback` with `{channel, thread, seq, vote, note}` stores the vote on that message. `vote` is `1` or `-1`. The message must be a model reply. The latest positive vote and the latest negative vote under the same human prompt become one pair. Two votes of the same sign, or two replies with the same text, do not. Debate transcripts are not pairs. A pipeline exit code is not a pair: the approve call does not name the message that produced the diff, and a failed apply already has its own learned-note path.

A non-empty `note` is also stored as a learned entry. A downvote is prefixed with `[AVOID]`.

`POST /api/preferences/export` with `{channel}` returns every pair for that channel as JSONL and stamps `exported_at_ms` on rows that were not yet exported. Repeating the same votes does not clear that stamp. Changing which reply is chosen or rejected does. `GET /api/preferences?channel=` lists the pairs and does not stamp them.

Each JSONL line has `prompt`, `chosen`, `rejected`, `chosen_weight` `1`, `rejected_weight` `0`, and `source` `operator_vote`.

## DR-45. Training stays a pipeline

`scripts/train_dpo_lora.py` filters that JSONL. `--dry-run` and `--self-test` do not load a model. A training run is Unsloth and TRL against a local Hugging Face snapshot passed as `--base-model`. An Ollama tag is a GGUF the script cannot fine-tune. The last stdout line is `XMESH_LORA` and a JSON object. The hub audit action is `adapt.lora`, with the adapter name and the pair count. The channel's selected model is left as the operator set it. Pointing chat at the new adapter is a later edit of the model server and of `chat_model_prefs`, after someone has looked at the result.

An example pipeline, with placeholders, belongs in `/etc/xmeshd/local.yaml` on the machine that holds the base weights. It is not part of the repo because the paths and the model id are local:

```yaml
deploy_pipelines:
  train-lora:
    workdir: "/var/lib/xmeshd/training"
    use_bwrap: true
    allow_network: false
    command:
      - "python3"
      - "/usr/local/libexec/xmeshd/train_dpo_lora.py"
      - "--dataset"
      - "preferences.jsonl"
      - "--base-model"
      - "/var/lib/models/Qwen2.5-Coder-7B-Instruct"
      - "--output-dir"
      - "/var/lib/xmeshd/training/out"
```

The script refuses a training run with fewer than four pairs. Weights are not downloaded by the daemon. A lab GPU that already serves GGUF through Ollama still needs a separate snapshot, and a small GPU should use a small base model.

A CPU-only host can run the same export through `llama-finetune` from llama.cpp, at `nice -n 19`, on a small GGUF such as a 1.5B or 3B coder. That binary is not shipped here. The pipeline's last line should still be `XMESH_LORA` plus JSON if the audit row should record it. The channel's selected model stays where the operator left it. The daemon does not pick a debate leader from win/loss counts, does not parse source with a syntax tree, and does not embed queries with an in-process ONNX model.
