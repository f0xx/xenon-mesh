#!/usr/bin/env bash
# Install a node.
#   ./scripts/provision_node_example.sh <xmeshd_bin> <hmac.key> <master_url>
# master_url is a full gRPC URL, for example http://hub.example:8443
set -euo pipefail

BIN_SRC="${1:?usage: $0 <xmeshd_bin> <hmac.key> <master_url>}"
KEY_SRC="${2:?usage: $0 <xmeshd_bin> <hmac.key> <master_url>}"
MASTER_URL="${3:-${XMESHD_MASTER_URL:-}}"
if [ -z "$MASTER_URL" ]; then
    echo "master URL is required (argument 3 or XMESHD_MASTER_URL)" >&2
    exit 1
fi
NODE_ID="${NODE_ID:-$(hostname -s)}"
OPENAI_BASE="${XMESHD_OPENAI_BASE_URL:-http://127.0.0.1:11434/v1}"

getent group xmeshd >/dev/null || groupadd -r xmeshd
getent passwd xmeshd >/dev/null || useradd -r -g xmeshd -d /var/lib/xmeshd -s /sbin/nologin xmeshd

install -d -m 0750 -o xmeshd -g xmeshd /etc/xmeshd /var/lib/xmeshd /var/log/xmeshd /srv/projects
install -m 0755 -o root -g root "$BIN_SRC" /usr/local/bin/xmeshd
install -m 0640 -o root -g xmeshd "$KEY_SRC" /etc/xmeshd/hmac.key

cat > /etc/xmeshd/local.yaml <<EOF
node:
  id: "${NODE_ID}"
  mode: "node"
  listen_addr: "0.0.0.0:8443"
  http_addr: "0.0.0.0:8080"
  master_url: "${MASTER_URL}"
  hmac_secret_path: "/etc/xmeshd/hmac.key"
  allowed_workspaces:
    - "/srv/projects"

storage:
  primary_url: null
  fallback_sqlite: "/var/lib/xmeshd/node.db"

models:
  default: ""
  providers:
    - id: local
      kind: auto
      base_url: "${OPENAI_BASE}"
      discover: true
      coding_bias: true
      timeout_secs: 300

git:
  watched_repos: []

deploy_pipelines:
  git-status:
    workdir: "/srv/projects"
    command: ["git", "status"]
    use_bwrap: false
    allow_network: false
EOF
chown root:xmeshd /etc/xmeshd/local.yaml
chmod 0640 /etc/xmeshd/local.yaml

if command -v rc-update >/dev/null 2>&1; then
    cat > /etc/init.d/xmeshd <<'EOF'
#!/sbin/openrc-run
name="xmeshd"
command="/usr/local/bin/xmeshd"
command_user="xmeshd"
supervisor="supervise-daemon"
respawn_delay=3
output_log="/var/log/xmeshd/stdout.log"
error_log="/var/log/xmeshd/stderr.log"
depend() { need net; }
EOF
    chmod 0755 /etc/init.d/xmeshd
    rc-update add xmeshd default || true
    rc-service xmeshd restart || true
elif command -v systemctl >/dev/null 2>&1; then
    cat > /etc/systemd/system/xmeshd.service <<'EOF'
[Unit]
Description=xmeshd node
After=network-online.target

[Service]
User=xmeshd
Group=xmeshd
ExecStart=/usr/local/bin/xmeshd
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
EOF
    systemctl daemon-reload
    systemctl enable --now xmeshd
fi
echo "Node '${NODE_ID}' provisioned against ${MASTER_URL}"
