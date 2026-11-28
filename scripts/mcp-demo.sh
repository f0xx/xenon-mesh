#!/bin/sh
# Exercise MCP on a running daemon: list tools, save a pin, read it, post a note.
# Usage: mcp-demo.sh [base-url]
# This writes into the channel mcp-demo on that daemon.
set -eu
base=${1:-http://127.0.0.1:8080}
here=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
call() {
  printf '%s\n' "$1" | "$here/mcp-call.sh" "$base"
}
call '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}'
call '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}'
call '{"jsonrpc":"2.0","id":3,"method":"tools/call","params":{"name":"xmesh_save_memory","arguments":{"channel":"mcp-demo","kind":"pin","topic":"memory/demo","content":"MCP runs beside chat"}}}'
call '{"jsonrpc":"2.0","id":4,"method":"resources/read","params":{"uri":"xmesh://channel/mcp-demo/memory"}}'
call '{"jsonrpc":"2.0","id":5,"method":"tools/call","params":{"name":"xmesh_post_message","arguments":{"channel":"mcp-demo","thread":"main","content":"hello from mcp-demo.sh"}}}'
call '{"jsonrpc":"2.0","id":6,"method":"tools/call","params":{"name":"xmesh_read_thread","arguments":{"channel":"mcp-demo","thread":"main","limit":5}}}'
