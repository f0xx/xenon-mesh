# Model providers

The daemon talks to models over HTTP. A provider is one endpoint plus the model names it serves. Chat requests name a model as `provider/model`. A bare model name is resolved by searching providers, then by using the first configured provider.

## DR-18. Kinds

| `kind` | Protocol | Discovery | Embeddings |
| --- | --- | --- | --- |
| `openai`, `openapi`, `openai-compat` | `POST {base}/chat/completions` and `POST {base}/embeddings`. `{base}` is the root ending in `/v1` | `GET {base}/models` | `data[0].embedding` |
| `ollama` | `POST {root}/api/chat` and `POST {root}/api/embed` | `GET {root}/api/tags` | `embeddings[0]` |
| `gemini` | Generative Language API. The key is sent as `x-goog-api-key`, not in the query string | Configured list only | `embedContent` |
| `auto` | Probe, then one of the rows above | Same as the detected kind | Same as the detected kind |

`auto` looks for an OpenAPI document and reads `paths`:

- a path containing `chat/completions` selects `openai`
- otherwise a path containing `/api/chat` selects `ollama`
- if no document is published, `GET /v1/models` then `GET /api/tags` decide

Ollama 0.40 publishes the OpenAI routes and does not publish an OpenAPI file, so `auto` and `openai` both land on `/v1`. Native `ollama` remains available when you want `/api/chat` specifically. Streaming thinking models report text in `delta.reasoning` (OpenAI) or `message.thinking` (native). The UI shows that as a thought trace. Only the answer is stored.

Images are sent as OpenAI `image_url` parts. Other attachments are decoded and inlined as text when they are valid UTF-8.

## DR-19. Coding priority

If `chat_model` is empty and `coding_bias` is true, discovery sorts names and picks the highest rank:

| Substring | Rank |
| --- | --- |
| `coder` | 100 |
| `code` (and not `coder`) | 50 |
| `instruct` | +10 |
| `reason`, `think`, or `-r1` | +5 |

`models.default` wins when it is set. The UI lists every discovered model with the coding models first. Fallback after a 404, 429, or 5xx tries at most two more names from the same provider. Authentication failures and connection failures do not fall through.

## DR-20. Embeddings are optional

Set `embed_model` (and `embed_provider` when it is not the first provider that has one) to turn embeddings on. The hub indexer stores a vector beside each changed file and paces itself only when a vector came back.

If the server answers that embeddings are disabled, the process stops calling that endpoint and ranks chunks by token overlap instead. Overlap counts the file path, so a question that names `executor` still selects `src/executor.rs`.

To enable embeddings on an Ollama server that was started without them, restart it with `--embeddings` and set `embed_model` to a model that supports the call. Until then, leave `embed_model` empty.

## DR-21. What the model is told

The system text is, in order:

1. `models.system_prompt`, when set
2. A short coding preamble that names the channel and asks for a single unified diff when code changes
3. Pinned notes, learned notes, and `@diff:` snapshots
4. Up to 8 ranked source chunks, inside a character budget

Roles stored in history are the literal author (`user` or `provider/model`). They are mapped per protocol: Gemini wants `user` and `model`; OpenAI-compatible and Ollama want `user` and `assistant`.

## DR-22. Probe

```sh
xmeshd models http://127.0.0.1:11434
```

Prints JSON: `kind`, `suggested_default`, and `models[]` with `coding_rank`. Use the suggested id as `models.default` only in local configuration.
