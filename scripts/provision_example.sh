#!/usr/bin/env bash
# Install a hub. Passwords and API keys come from the environment.
#   BIN_SRC=./target/release/xmeshd GIT_ROOT=/srv/git ./scripts/provision_example.sh
set -euo pipefail

BIN_SRC="${1:-${BIN_SRC:-./target/release/xmeshd}}"
GIT_ROOT="${GIT_ROOT:-/srv/git}"
NODE_ID="${NODE_ID:-$(hostname -s)}"
INSTALL_PREFIX="${INSTALL_PREFIX:-/usr/local/bin}"
OPENAI_BASE="${XMESHD_OPENAI_BASE_URL:-}"
DB_URL="${XMESH_DATABASE_URL:-}"

getent group xmeshd >/dev/null || groupadd -r xmeshd
getent passwd xmeshd >/dev/null || useradd -r -g xmeshd -d /var/lib/xmeshd -s /sbin/nologin xmeshd

install -d -m 0750 -o root -g xmeshd /etc/xmeshd
install -d -m 0750 -o xmeshd -g xmeshd /var/lib/xmeshd /var/log/xmeshd

if [ -d "$GIT_ROOT" ]; then
    chgrp -R xmeshd "$GIT_ROOT" || true
    chmod -R g+rX "$GIT_ROOT" || true
fi

if [ -f "$BIN_SRC" ]; then
    install -m 0755 -o root -g root "$BIN_SRC" "${INSTALL_PREFIX}/xmeshd"
else
    echo "WARN: binary $BIN_SRC not found; copy it to ${INSTALL_PREFIX}/xmeshd manually."
fi

if [ ! -f /etc/xmeshd/hmac.key ]; then
    head -c 32 /dev/urandom > /etc/xmeshd/hmac.key
    chown root:xmeshd /etc/xmeshd/hmac.key
    chmod 0640 /etc/xmeshd/hmac.key
    echo "Generated /etc/xmeshd/hmac.key. Copy this file to every node."
fi

PRIMARY="null"
if [ -n "$DB_URL" ]; then
    PRIMARY="\"${DB_URL}\""
fi

MODELS=""
if [ -n "$OPENAI_BASE" ]; then
    MODELS=$(cat <<EOF

models:
  default: ""
  providers:
    - id: local
      kind: auto
      base_url: "${OPENAI_BASE}"
      discover: true
      coding_bias: true
      timeout_secs: 300
EOF
)
fi

cat > /etc/xmeshd/local.yaml <<EOF
node:
  id: "${NODE_ID}"
  mode: "master"
  listen_addr: "0.0.0.0:8443"
  http_addr: "0.0.0.0:8080"
  hmac_secret_path: "/etc/xmeshd/hmac.key"

storage:
  primary_url: ${PRIMARY}
  fallback_sqlite: "/var/lib/xmeshd/master_fallback.db"

gemini:
  chat_model: "gemini-2.5-flash"
  embed_model: "gemini-embedding-001"
  api_key_env: "GEMINI_API_KEY"
${MODELS}
git:
  bare_repos_root: "${GIT_ROOT}"
EOF

chown root:xmeshd /etc/xmeshd/local.yaml
chmod 0640 /etc/xmeshd/local.yaml

if [ ! -f /etc/conf.d/xmeshd ]; then
    cat > /etc/conf.d/xmeshd <<'EOF'
# Export provider credentials here. Do not commit this file.
# export GEMINI_API_KEY=
# export OLLAMA_API_KEY=
EOF
    chmod 0600 /etc/conf.d/xmeshd
fi

if [ ! -f /etc/init.d/xmeshd ]; then
    cp "$(dirname "$0")/openrc-xmeshd" /etc/init.d/xmeshd
    chmod 0755 /etc/init.d/xmeshd
fi

echo "Hub '${NODE_ID}' files installed. Start with: rc-service xmeshd restart"
