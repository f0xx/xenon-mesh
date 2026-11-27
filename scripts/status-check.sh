#!/bin/sh
# Read-only status probe. Prints one finding per line and always exits 0.
# Set XMESH_STATUS_URL to an HTTP(S) endpoint that returns plain text.
# Nothing is written. A failure here does not disable approve.
set -eu
if [ -z "${XMESH_STATUS_URL:-}" ]; then
  echo "status: no XMESH_STATUS_URL configured"
  exit 0
fi
if out=$(curl -fsS --max-time 15 "$XMESH_STATUS_URL" 2>/dev/null); then
  printf '%s\n' "$out" | awk 'NF && NR<=20 { print "status: " $0 }'
else
  echo "status: check failed"
fi
exit 0
