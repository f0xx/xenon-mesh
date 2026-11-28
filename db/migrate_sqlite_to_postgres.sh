#!/bin/sh
# Copy one node's SQLite history into PostgreSQL and leave the SQLite file in place.
# Usage: migrate_sqlite_to_postgres.sh <sqlite-file> <postgres-url>
# The URL is passed to xmeshd and is not printed.
set -eu
src=${1:?sqlite file}
dst=${2:?postgres url}
bin=${XMESHD:-xmeshd}
if [ ! -f "$src" ]; then
  echo "sqlite file is missing" >&2
  exit 1
fi
case $dst in
  postgres://*|postgresql://*) ;;
  *) echo "destination must be a postgres url" >&2; exit 1 ;;
esac
"$bin" migrate "sqlite://$src" "$dst"
echo "sqlite file kept at $src"
