#!/usr/bin/env python3
"""Train a DPO LoRA from xmeshd preference JSONL, or only filter that file.

The daemon exports one JSON object per line: prompt, chosen, rejected,
chosen_weight, rejected_weight. This script does not run inside xmeshd.
`--self-test` and `--dry-run` never import a training stack.

A real run needs Unsloth, TRL, and a local Hugging Face snapshot of the base
model. An Ollama tag is not that snapshot. On success the last stdout line is
`XMESH_LORA` plus a JSON object. xmeshd records the adapter name. It does not
change the channel's selected model.
"""

import argparse
import json
import sys
import tempfile
from pathlib import Path


def parse_args(argv=None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Train a DPO LoRA adapter from xmeshd preference pairs."
    )
    parser.add_argument("--dataset", type=Path)
    parser.add_argument("--base-model", type=str, default="")
    parser.add_argument("--output-dir", type=Path)
    parser.add_argument("--adapter-name", type=str, default="xmesh-lora")
    parser.add_argument("--min-weight-delta", type=float, default=0.2)
    parser.add_argument("--max-seq-length", type=int, default=4096)
    parser.add_argument("--lora-r", type=int, default=16)
    parser.add_argument("--lora-alpha", type=int, default=32)
    parser.add_argument("--beta", type=float, default=0.1)
    parser.add_argument("--epochs", type=int, default=1)
    parser.add_argument("--batch-size", type=int, default=1)
    parser.add_argument("--grad-accum", type=int, default=4)
    parser.add_argument("--lr", type=float, default=5e-5)
    parser.add_argument("--export-gguf", action="store_true")
    parser.add_argument("--llama-cpp-dir", type=Path, default=Path("/opt/llama.cpp"))
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Print how many pairs would train, and do not load a model.",
    )
    parser.add_argument(
        "--self-test",
        action="store_true",
        help="Check the pair filter and exit.",
    )
    return parser.parse_args(argv)


def filter_records(rows, min_weight_delta):
    kept = []
    for item in rows:
        prompt = str(item.get("prompt", "")).strip()
        chosen = str(item.get("chosen", "")).strip()
        rejected = str(item.get("rejected", "")).strip()
        try:
            delta = float(item.get("chosen_weight", 1.0)) - float(
                item.get("rejected_weight", 0.0)
            )
        except (TypeError, ValueError):
            continue
        if not prompt or not chosen or not rejected:
            continue
        if chosen == rejected:
            continue
        if delta < min_weight_delta:
            continue
        kept.append({"prompt": prompt, "chosen": chosen, "rejected": rejected})
    return kept


def load_jsonl(path: Path):
    rows = []
    with path.open("r", encoding="utf-8") as handle:
        for line_num, raw in enumerate(handle, start=1):
            line = raw.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
            except json.JSONDecodeError as exc:
                print(
                    f"[warn] skipping malformed JSON at line {line_num}: {exc}",
                    file=sys.stderr,
                )
                continue
            if isinstance(item, dict):
                rows.append(item)
    return rows


def self_test() -> None:
    with tempfile.TemporaryDirectory() as tmp:
        path = Path(tmp) / "pairs.jsonl"
        path.write_text(
            "\n".join(
                [
                    json.dumps(
                        {
                            "prompt": "fix the parser",
                            "chosen": "good diff",
                            "rejected": "bad diff",
                        }
                    ),
                    json.dumps(
                        {
                            "prompt": "fix the parser",
                            "chosen": "same",
                            "rejected": "same",
                        }
                    ),
                    json.dumps(
                        {
                            "prompt": "fix the parser",
                            "chosen": "a",
                            "rejected": "b",
                            "chosen_weight": 0.1,
                            "rejected_weight": 0.0,
                        }
                    ),
                    json.dumps({"prompt": "", "chosen": "a", "rejected": "b"}),
                    "{not json",
                ]
            ),
            encoding="utf-8",
        )
        kept = filter_records(load_jsonl(path), 0.2)
    if len(kept) != 1 or kept[0]["chosen"] != "good diff":
        print(f"[error] filter kept {kept}", file=sys.stderr)
        sys.exit(1)


def train(args, records) -> None:
    from datasets import Dataset
    import torch
    from trl import DPOConfig, DPOTrainer
    from unsloth import FastLanguageModel, PatchDPOTrainer

    PatchDPOTrainer()
    model, tokenizer = FastLanguageModel.from_pretrained(
        model_name=args.base_model,
        max_seq_length=args.max_seq_length,
        dtype=None,
        load_in_4bit=True,
    )
    formatted = []
    for row in records:
        prompt = row["prompt"]
        if hasattr(tokenizer, "apply_chat_template"):
            prompt = tokenizer.apply_chat_template(
                [{"role": "user", "content": prompt}],
                tokenize=False,
                add_generation_prompt=True,
            )
        formatted.append(
            {"prompt": prompt, "chosen": row["chosen"], "rejected": row["rejected"]}
        )
    if len(formatted) < 4:
        print(
            f"[error] insufficient valid preference pairs ({len(formatted)} < 4); aborting.",
            file=sys.stderr,
        )
        sys.exit(2)

    model = FastLanguageModel.get_peft_model(
        model,
        r=args.lora_r,
        target_modules=[
            "q_proj",
            "k_proj",
            "v_proj",
            "o_proj",
            "gate_proj",
            "up_proj",
            "down_proj",
        ],
        lora_alpha=args.lora_alpha,
        lora_dropout=0.0,
        bias="none",
        use_gradient_checkpointing="unsloth",
        random_state=3407,
    )
    args.output_dir.mkdir(parents=True, exist_ok=True)
    adapter_dir = args.output_dir / args.adapter_name
    use_bf16 = torch.cuda.is_available() and torch.cuda.is_bf16_supported()
    trainer = DPOTrainer(
        model=model,
        ref_model=None,
        args=DPOConfig(
            output_dir=str(args.output_dir / "checkpoints"),
            per_device_train_batch_size=args.batch_size,
            gradient_accumulation_steps=args.grad_accum,
            warmup_ratio=0.1,
            num_train_epochs=args.epochs,
            learning_rate=args.lr,
            fp16=not use_bf16,
            bf16=use_bf16,
            logging_steps=5,
            optim="paged_adamw_8bit",
            beta=args.beta,
            max_length=args.max_seq_length,
            max_prompt_length=args.max_seq_length // 2,
            save_strategy="no",
            report_to="none",
        ),
        train_dataset=Dataset.from_list(formatted),
        processing_class=tokenizer,
    )
    trainer.train()
    model.save_pretrained(str(adapter_dir))
    tokenizer.save_pretrained(str(adapter_dir))
    summary = {
        "adapter_name": args.adapter_name,
        "base_model": args.base_model,
        "pairs_used": len(formatted),
        "safetensors_dir": str(adapter_dir),
    }
    print(f"XMESH_LORA {json.dumps(summary)}")


def main(argv=None) -> None:
    args = parse_args(argv)
    if args.self_test:
        self_test()
        return
    if args.dataset is None:
        print("[error] --dataset is required", file=sys.stderr)
        sys.exit(2)
    if not args.dataset.is_file():
        print(f"[error] dataset not found: {args.dataset}", file=sys.stderr)
        sys.exit(2)
    records = filter_records(load_jsonl(args.dataset), args.min_weight_delta)
    if args.dry_run:
        print(len(records))
        return
    if not args.base_model or args.output_dir is None:
        print("[error] --base-model and --output-dir are required", file=sys.stderr)
        sys.exit(2)
    train(args, records)


if __name__ == "__main__":
    main()
