#!/usr/bin/env bash
set -euo pipefail

BIN_SRC="${1:-./target/release/xmeshd}"
GIT_ROOT=${GIT_ROOT:-"/svc/git"}
NODE_ID=${NODE_ID:-"euphoria"}
INSTALL_PREFIX=${INSTALL_PREFIX:-"/usr/local/bin"}
GEMINI_MODEL=${GEMINI_MODEL:-"gemini-2.5-flash"}

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
    echo "WARN: Binary $BIN_SRC not found, copy it to ${INSTALL_PREFIX}/xmeshd manually."
fi

if [ ! -f /etc/xmeshd/hmac.key ]; then
    head -c 32 /dev/urandom > /etc/xmeshd/hmac.key
    chown root:xmeshd /etc/xmeshd/hmac.key
    chmod 0640 /etc/xmeshd/hmac.key
    echo "Generated new /etc/xmeshd/hmac.key (copy this file to all lab nodes!)"
fi

cat > /etc/xmeshd/local.yaml <<'EOF'
node:
  id: "${NODE_ID}"
  mode: "master"
  listen_addr: "0.0.0.0:8443"
  hmac_secret_path: "/etc/xmeshd/hmac.key"

storage:
  primary_url: "postgres://xmesh:labpass@127.0.0.1:5432/xmesh_lab"
  fallback_sqlite: "/var/lib/xmeshd/master_fallback.db"

gemini:
  chat_model: "${GEMINI_MODEL}"
  embed_model: "gemini-embedding-001"
  api_key_env: "MY_GEMINI_CONFIG"

git:
  bare_repos_root: "/mnt/lvmd0/lvmd0p0/"
EOF
chown root:xmeshd /etc/xmeshd/local.yaml
chmod 0640 /etc/xmeshd/local.yaml

