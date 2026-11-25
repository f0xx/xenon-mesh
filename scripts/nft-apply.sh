#!/bin/sh
set -eu

NEW_RULES="/srv/projects/router-fw/nftables.conf"
ACTIVE_RULES="/etc/nftables/main.nft"
BACKUP_RULES="/var/lib/xmeshd/main.nft.bak"
MASTER_IP="10.10.0.1"

nft -c -f "$NEW_RULES"

cp "$ACTIVE_RULES" "$BACKUP_RULES"
cp "$NEW_RULES" "$ACTIVE_RULES"
nft -f "$ACTIVE_RULES"

if ! ping -c 2 -W 3 "$MASTER_IP" >/dev/null 2>&1; then
    cp "$BACKUP_RULES" "$ACTIVE_RULES"
    nft -f "$ACTIVE_RULES"
    echo "ROLLBACK: Master unreachable after applying new rules!" >&2
    exit 1
fi

echo "SUCCESS: nftables updated and Master connectivity verified."

