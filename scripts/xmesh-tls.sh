#!/bin/sh
# Write a self-signed certificate for the local xmeshd HTTPS listener.
# The browser will ask once. After it is allowed, the page is a secure context
# and desktop notifications can be requested.
set -eu

dest="${1:-/etc/xmeshd/tls}"
name="${2:-$(hostname -s 2>/dev/null || echo xmeshd)}"
ips="${XMESH_TLS_IPS:-127.0.0.1}"
sans="DNS:localhost,DNS:${name}"
for ip in $ips; do
  sans="${sans},IP:${ip}"
done

mkdir -p "$dest"
if [ -f "$dest/cert.pem" ] && [ "${XMESH_TLS_FORCE:-}" != 1 ]; then
  echo "certificate already exists at $dest (set XMESH_TLS_FORCE=1 to replace)"
  exit 0
fi

openssl req -x509 -newkey rsa:2048 -nodes \
  -keyout "$dest/key.pem" \
  -out "$dest/cert.pem" \
  -days 825 \
  -subj "/CN=${name}" \
  -addext "subjectAltName=${sans}"
chmod 640 "$dest/key.pem"
chmod 644 "$dest/cert.pem"
echo "wrote $dest/cert.pem"
echo "node.tls_cert_path: \"$dest/cert.pem\""
echo "node.tls_key_path: \"$dest/key.pem\""
