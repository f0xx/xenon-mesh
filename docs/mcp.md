# MCP

The running daemon speaks [Model Context Protocol](https://modelcontextprotocol.io) JSON-RPC on `POST /api/mcp`. Chat, the web UI, the CLI, and MCP share that process and the same database. `xmeshd mcp` does not start a second daemon. It reads JSON-RPC from stdin and posts each frame to the daemon already bound to `node.http_addr`. Stdout is only the response frames. A daemon that is down answers with a JSON-RPC error.

`scripts/mcp-call.sh` posts one frame. `scripts/mcp-demo.sh` lists the tools, saves a pin in the channel `mcp-demo`, reads it back, and posts a message there.

| Tool | Effect |
| --- | --- |
| `xmesh_search_context` | Ranked source, pins, and notes for a channel and a query |
| `xmesh_save_memory` | Pin or learned note. `kind` is `pin` or `learn` |
| `xmesh_read_thread` | Recent messages. `thread` defaults to `main` |
| `xmesh_post_message` | Append one message. The author is the node id plus `(mcp)` |

Resources use `xmesh://channel/<name>/memory`. `initialize` reports protocol `2024-11-05`. A notification has no `id` and the HTTP route answers `204`.

The route is on the same HTTP port as the UI. It is the same trust boundary: whoever can open the UI can call MCP.
