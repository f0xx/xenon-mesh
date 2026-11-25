#!/bin/sh
# Create a MariaDB/MySQL account from the environment, then apply the schema.
# Required: XMESH_DB_USER XMESH_DB_PASSWORD XMESH_DB_NAME
# The password is interpolated into SQL. Do not put quotes in it.
set -eu

: "${XMESH_DB_USER:?set XMESH_DB_USER}"
: "${XMESH_DB_PASSWORD:?set XMESH_DB_PASSWORD}"
: "${XMESH_DB_NAME:?set XMESH_DB_NAME}"

ROOT=$(CDPATH= cd -- "$(dirname "$0")" && pwd)
mysql <<EOF
CREATE DATABASE IF NOT EXISTS \`${XMESH_DB_NAME}\` CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
CREATE USER IF NOT EXISTS '${XMESH_DB_USER}'@'127.0.0.1' IDENTIFIED BY '${XMESH_DB_PASSWORD}';
CREATE USER IF NOT EXISTS '${XMESH_DB_USER}'@'localhost' IDENTIFIED BY '${XMESH_DB_PASSWORD}';
GRANT ALL PRIVILEGES ON \`${XMESH_DB_NAME}\`.* TO '${XMESH_DB_USER}'@'127.0.0.1';
GRANT ALL PRIVILEGES ON \`${XMESH_DB_NAME}\`.* TO '${XMESH_DB_USER}'@'localhost';
FLUSH PRIVILEGES;
EOF
mysql "${XMESH_DB_NAME}" < "${ROOT}/init_mysql.sql"
