#!/bin/sh
set -eu

NEW_RULES="${NFT_NEW_RULES:-/srv/projects/app/nftables.conf}"
ACTIVE_RULES="${NFT_ACTIVE_RULES:-/etc/nftables/main.nft}"
BACKUP_RULES="${NFT_BACKUP_RULES:-/var/lib/xmeshd/main.nft.bak}"
HEALTHCHECK="${XMESHD_HEALTHCHECK_HOST:?set XMESHD_HEALTHCHECK_HOST to a host that must stay reachable}"

nft -c -f "$NEW_RULES"

cp "$ACTIVE_RULES" "$BACKUP_RULES"
cp "$NEW_RULES" "$ACTIVE_RULES"
nft -f "$ACTIVE_RULES"

if ! ping -c 2 -W 3 "$HEALTHCHECK" >/dev/null 2>&1; then
    cp "$BACKUP_RULES" "$ACTIVE_RULES"
    nft -f "$ACTIVE_RULES"
    echo "ROLLBACK: healthcheck host unreachable after applying new rules" >&2
    exit 1
fi

echo "nftables updated and healthcheck host still reachable."
