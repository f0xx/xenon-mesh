#!/bin/sh
# Create a PostgreSQL role and database from the environment, then apply the schema.
# Required: XMESH_DB_USER XMESH_DB_PASSWORD XMESH_DB_NAME
set -eu

: "${XMESH_DB_USER:?set XMESH_DB_USER}"
: "${XMESH_DB_PASSWORD:?set XMESH_DB_PASSWORD}"
: "${XMESH_DB_NAME:?set XMESH_DB_NAME}"

ROOT=$(CDPATH= cd -- "$(dirname "$0")" && pwd)

psql -v ON_ERROR_STOP=1 \
  -v db_user="$XMESH_DB_USER" \
  -v db_password="$XMESH_DB_PASSWORD" \
  -v db_name="$XMESH_DB_NAME" <<'EOF'
SELECT format('CREATE ROLE %I LOGIN PASSWORD %L', :'db_user', :'db_password')
WHERE NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = :'db_user') \gexec
SELECT format('CREATE DATABASE %I OWNER %I ENCODING ''UTF8''', :'db_name', :'db_user')
WHERE NOT EXISTS (SELECT 1 FROM pg_database WHERE datname = :'db_name') \gexec
EOF

psql -v ON_ERROR_STOP=1 -d "$XMESH_DB_NAME" -v db_user="$XMESH_DB_USER" <<EOF
GRANT ALL ON SCHEMA public TO :"db_user";
SET ROLE :"db_user";
\\i '${ROOT}/init_postgres.sql'
EOF
