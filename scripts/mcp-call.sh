#!/bin/sh
# Post one MCP JSON-RPC frame to a running daemon.
# Usage: mcp-call.sh [base-url] <<'JSON'
# The base URL defaults to http://127.0.0.1:8080. Stdout is the response frame.
set -eu
base=${1:-http://127.0.0.1:8080}
base=${base%/}
curl -sS -H 'content-type: application/json' --data-binary @- "$base/api/mcp"
echo
