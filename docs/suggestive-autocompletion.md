# Suggestive autocompletion

The composer can show ghost text for the channel you are in. The checkbox is **suggest**, next to **agentic review**. It defaults to on. The choice is stored per channel (`sg:<channel>` in `node_settings`, missing means on). Tab inserts the hint. Esc clears it. A hint is requested only when the caret is at the end of the composer, 350 ms after the last key, and the previous request is cancelled.

`POST /api/suggest` takes `{channel, thread, prefix}` and returns `{completion, source}`. `completion` is only the suffix to append. `source` is `index`, `llm`, or `none`.

`GET /api/suggest-pref?channel=` and `POST /api/suggest-pref` with `{channel, enabled}` are the switch. `xmeshd cli` has `/suggest`, `/suggest on`, `/suggest off`, and `/suggest <text>`.

The index tier does not call a model:

1. A line that starts with `/` completes a command, or a model id after `/model `.
2. A path token of at least 3 characters completes a `source_path` in the channel's context.
3. An identifier of at least 4 characters with `_` or a camelCase hump completes the most common matching token in those files.
4. A prefix of at least 6 characters completes the newest matching human prompt or exemplar task.

The model tier runs only when the index missed, the prefix is at least 10 characters, `models.autocomplete_model` is set, and this daemon is not already generating a reply. The call is dropped at `models.autocomplete_timeout_ms` (default 800). An empty `autocomplete_model` stays on the index, which is the lab default so a completion never loads another set of weights beside a chat.
