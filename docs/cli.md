# Command line

`xmeshd cli` talks to a daemon that is already running. It does not bind the HTTP or gRPC port.

```sh
xmeshd cli
xmeshd cli --url http://127.0.0.1:8091
```

With no `--url`, the address comes from `node.http_addr` in the local config. `0.0.0.0` is rewritten to `127.0.0.1`.

A line that starts with `/` is a command. Any other line is sent as a chat message on the current channel and thread.

| Command | Effect |
| --- | --- |
| `/help` | List the commands |
| `/channels` | List channels |
| `/use <channel>` | Switch channel and return to its `main` thread |
| `/thread [name]` | Show or switch the thread |
| `/threads` | List threads in the current channel |
| `/models` | List model ids from `/api/config` |
| `/model [provider/model]` | Show or select the model. Empty uses the daemon default |
| `/history [count]` | Print the latest messages. A finished reply includes `generation took …` |
| `/alias [name]` | Show or set the name stored on this node. An empty box falls back to `node.id` |
| `/send <text>` | Send one message |
| `/quit` | Leave the prompt |

On a public channel's `main` thread, the daemon stores the question there and the model reply in a new thread named `m` plus the question's sequence. The client prints that thread name when the reply is saved. `/thread m12` opens it. A question asked while already inside a thread stays in that thread.

The name from `/alias` is the author on messages sent from this daemon, including the web UI on the same machine. It is one row in the node database, so it survives a new browser window.
