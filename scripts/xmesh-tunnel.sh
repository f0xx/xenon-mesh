#!/bin/sh
# Open SSH local forwards so a node off the hub LAN can use the hub.
# The script does not rewrite xmeshd config. It prints the overlay to apply.
set -eu

cmd="${1:-up}"
hub="${XMESH_HUB_SSH:-}"
opts="${XMESH_SSH_OPTS:-}"
socket="${XMESH_TUNNEL_SOCKET:-${TMPDIR:-/tmp}/xmesh-tunnel.sock}"
# local:remote pairs. Remote is the hub's own listener.
forwards="${XMESH_FORWARDS:-8443:8443 8080:8080 8448:8448}"

if [ -z "$hub" ]; then
  echo "Set XMESH_HUB_SSH to the SSH destination, for example hub.example" >&2
  exit 1
fi

case "$cmd" in
  up)
    if [ -S "$socket" ] && ssh -S "$socket" -O check "$hub" >/dev/null 2>&1; then
      echo "tunnel already up ($socket)"
    else
      # shellcheck disable=SC2086
      ssh $opts -f -N -M -S "$socket" \
        $(for pair in $forwards; do
            local_port="${pair%%:*}"
            remote_port="${pair##*:}"
            printf '%s\n' "-L" "127.0.0.1:${local_port}:127.0.0.1:${remote_port}"
          done) \
        "$hub"
      echo "tunnel up ($socket)"
    fi
    cat <<EOF
# Overlay for the node that is using the forwards. Merge into local.yaml and restart xmeshd.
node:
  master_url: "http://127.0.0.1:8443"
  master_http: "http://127.0.0.1:8080"
EOF
    ;;
  down)
    ssh -S "$socket" -O exit "$hub"
    echo "tunnel down"
    ;;
  status)
    if ssh -S "$socket" -O check "$hub" >/dev/null 2>&1; then
      echo "up $socket"
    else
      echo "down"
      exit 1
    fi
    ;;
  *)
    echo "usage: XMESH_HUB_SSH=host $0 up|down|status" >&2
    exit 1
    ;;
esac
