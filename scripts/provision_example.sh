#!/usr/bin/env bash
set -euo pipefail

BIN_SRC="${1:-./target/release/xmeshd}"
GIT_ROOT=${GIT_ROOT:-"/svc/git"}
NODE_ID=${NODE_ID:-"euphoria"}
INSTALL_PREFIX=${INSTALL_PREFIX:-"/usr/local/bin/"}
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

cat > /etc/xmeshd/local.yaml <<EOF
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
  api_key_env: "MY_GEMINI_API_KEY"

git:
  bare_repos_root: "${GIT_ROOT}"
EOF

chown root:xmeshd /etc/xmeshd/local.yaml
chmod 0640 /etc/xmeshd/local.yaml

if [ ! -f /etc/conf.d/xmeshd ]; then
    cat > /etc/conf.d/xmeshd <<'EOF'
      export MY_GEMINI_API_KEY="REPLACE_WITH_YOUR_GEMINI_KEY"
EOF
    chmod 0600 /etc/conf.d/xmeshd
fi

cat > /etc/init.d/xmeshd <<EOF
#!/sbin/openrc-run

name="xmeshd"
description="Xenon Mesh Distributed AI & Lab Orchestration Daemon"
command="${INSTALL_PREFIX}/xmeshd"
command_user="xmeshd"

supervisor="supervise-daemon"
respawn_delay=3
respawn_max=0

output_log="/var/log/xmeshd/stdout.log"
error_log="/var/log/xmeshd/stderr.log"

depend() {
    need net
    use postgresql mariadb
    after firewall wireguard
}
EOF
chmod 0755 /etc/init.d/xmeshd

if command -v psql >/dev/null 2>&1 && su - postgres -c "psql -c '\q'" >/dev/null 2>&1; then
    su - postgres -c "psql -v ON_ERROR_STOP=1" <<'EOF'
DO $$ BEGIN
    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = 'xmesh') THEN
        CREATE ROLE xmesh WITH LOGIN PASSWORD 'labpass';
    END IF;
END $$;
SELECT 'CREATE DATABASE xmesh_lab OWNER xmesh ENCODING ''UTF8'''
WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'xmesh_lab')\gexec
\c xmesh_lab
GRANT ALL PRIVILEGES ON DATABASE xmesh_lab TO xmesh;
GRANT ALL ON SCHEMA public TO xmesh;
EOF
    echo "PostgreSQL role 'xmesh' and database 'xmesh_lab' initialized."
else
    echo "PostgreSQL not running/installed — xmeshd will use SQLite fallback."
fi

#rc-update add xmeshd default
echo "Done! Set MY_GEMINI_API_KEY in /etc/conf.d/xmeshd and run: rc-service xmeshd start"

