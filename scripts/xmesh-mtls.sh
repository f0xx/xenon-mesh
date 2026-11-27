#!/bin/sh
# Create a lab CA and one node certificate for mutual TLS.
# The CA signs gRPC and the mesh HTTP listener. Each machine gets its own
# cert. The application MAC in node_key_path is a separate check.
set -eu

dest="${1:-/etc/xmeshd/mtls}"
name="${2:-node}"
mkdir -p "$dest"
chmod 700 "$dest"

if [ ! -f "$dest/ca.key" ]; then
  openssl req -x509 -newkey rsa:2048 -nodes \
    -keyout "$dest/ca.key" \
    -out "$dest/ca.pem" \
    -days 825 \
    -subj "/CN=xmeshd-lab"
fi

openssl req -newkey rsa:2048 -nodes \
  -keyout "$dest/${name}.key" \
  -out "$dest/${name}.csr" \
  -subj "/CN=${name}"
openssl x509 -req -in "$dest/${name}.csr" \
  -CA "$dest/ca.pem" -CAkey "$dest/ca.key" -CAcreateserial \
  -out "$dest/${name}.pem" -days 825
rm -f "$dest/${name}.csr"
chmod 640 "$dest/${name}.key" "$dest/ca.key"
chmod 644 "$dest/ca.pem" "$dest/${name}.pem"
echo "node.mtls_ca_path: \"$dest/ca.pem\""
echo "node.mtls_cert_path: \"$dest/${name}.pem\""
echo "node.mtls_key_path: \"$dest/${name}.key\""
echo "Copy ca.pem to every peer. Keep ca.key on the machine that issues certs."
