# Model providers

The daemon talks to models over HTTP. A provider is one host plus the model names it serves. Chat requests name a model as `provider/model`. The same model name on two hosts stays two choices, labeled `model : host`. A bare name is used only when exactly one configured host serves it. A node with `node.master_http` takes the hub's catalog and sends each completion to the hub as its own request. The hub drops its provider lock before the HTTP call, so one slow host does not pause chats aimed at another host.

A Gemini provider is announced by the hub under the hub's own node id, with `proxied: true`. The API key stays on the hub. Nodes select `gemini/<model>` and the hub performs the call. The menu groups those models under the hub so they are not sorted to the bottom of a flat list.

## DR-18. Kinds

| `kind` | Protocol | Discovery | Embeddings |
| --- | --- | --- | --- |
| `openai`, `openapi`, `openai-compat` | `POST {base}/chat/completions` and `POST {base}/embeddings`. `{base}` is the root ending in `/v1` | `GET {base}/models` | `data[0].embedding` |
| `ollama` | `POST {root}/api/chat` and `POST {root}/api/embed` | `GET {root}/api/tags` | `embeddings[0]` |
| `gemini` | Generative Language API, `generateContent`. The key is sent as `x-goog-api-key`, not in the query string. A 503 is retried a few times | Configured list only | `embedContent` |
| `google`, `web` | Runs `command`. The query is stdin. `user_agent` and `base_url` (the search URL, with `{query}`) are exported as `XMESH_UA` and `XMESH_SEARCH_URL`. The script in `scripts/google-ask.sh` is the page client | Configured list only | None. No repository source is attached |
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

`models.default` wins when it is set. The UI lists every discovered model with the coding models first. A chat with no saved choice uses that default. The last model chosen in a channel and thread is stored on that node and restored when the chat is opened again. The multi switch next to the model list asks every selected model; each reply is stored on its own and they are not merged, so they can disagree. Fallback after a 404, 429, or 5xx tries at most two more names from the same provider. Authentication failures and connection failures do not fall through.

## Sampling

`models.sampling` and a provider's own `sampling` set `temperature`, `top_p`, and `repeat_penalty`. A provider field overrides the shared one. OpenAI-compatible and Gemini calls use temperature 0.2 when it is unset. An Ollama request omits `temperature` when it is unset, and the server then uses the model file. That file is often 0.8, which is loose for coding. Set `temperature: 0.2` on the Ollama provider to override it for these chats without editing the model file.

`repeat_penalty` is sent only to native Ollama, as `options.repeat_penalty`.

## External providers

`models.providers_file` points at a JSON object whose keys are provider ids. The shape matches `configs/providers.example.json`: `type` `openai_chat`, `base_url`, `models`, `api_key` or `api_key_env`, and `auth_optional`. A placeholder key (`PASTE_…`) or an empty key drops that provider unless `auth_optional` is true. `api_key_env` keeps the provider only when that variable is set at startup. Providers loaded this way are not written into the source tree. On the hub they are proxied: a node selects `provider/model` and the hub performs the call, the same as Gemini.

## DR-20. Embeddings are optional

Set `embed_model` (and `embed_provider` when it is not the first provider that has one) to turn embeddings on. The hub indexer stores a vector beside each changed file and paces itself only when a vector came back.

If the server answers that embeddings are disabled, the process stops calling that endpoint and ranks chunks by token overlap instead. Overlap counts the file path, so a question that names `executor` still selects `src/executor.rs`.

To enable embeddings on an Ollama server that was started without them, restart it with `--embeddings` and set `embed_model` to a model that supports the call. Until then, leave `embed_model` empty.

## DR-21. What the model is told

The system text is, in order:

1. `models.system_prompt`, when set
2. A short coding preamble that names the channel and asks for a single unified diff when code changes
3. Pinned notes and `@diff:` snapshots. Learned notes are ranked against the question; at most five that share tokens (or a vector) are included, each truncated to 1,500 characters
4. A path index of the associated project, plus overview files, then a first slice of ranked source. The slice is about half the full budget: 4 chunks and 16,000 characters for a local model, 5 chunks and 32,000 for Gemini. A file the question names is boosted ahead of a looser token match. A question that shares no tokens with the source still receives that outline. `git.harvest_command` replaces the built-in walk with a script; the project path is the last argument. A `google` model receives a one-line note and a short summary of the chat instead of source.

The model can ask for the rest. A reply that ends with a `read` fence (`path` or `path:start-end`), a `grep` fence (a substring, not a regular expression), a `web` fence, or a `fetch` fence is not stored as the final answer. The daemon runs up to four of those lookups, then calls the model again. `read` refuses `..` and absolute paths and returns numbered lines from the channel's indexed files. `grep` returns at most 40 matching lines. After four steps the daemon stops and keeps the text.

Roles stored in history are the literal author (`user` or `provider/model`). They are mapped per protocol: Gemini wants `user` and `model`, and consecutive turns of the same role are merged; OpenAI-compatible and Ollama want `user` and `assistant`. A non-generic author is prefixed on the Gemini turn so the collapsed role still shows who spoke.

When `models.debate_max_rounds` is greater than 0 and the UI debate box is checked, the selected models answer in parallel and then critique each other, one at a time, for that many rounds. A round ends the debate only when every surviving model starts its reply with `CONSENSUS:`. That text can be distilled into a learned note. A failed model drops out. The checkbox stays hidden while the cap is 0.

## DR-22. Probe

```sh
xmeshd models http://127.0.0.1:11434
```

Prints JSON: `kind`, `suggested_default`, and `models[]` with `coding_rank`. Use the suggested id as `models.default` only in local configuration.
