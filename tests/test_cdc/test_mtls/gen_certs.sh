#!/bin/bash
# Generate CA + 3 sets of server/client certs for mTLS testing.
# All certs share the same CA. SAN=localhost for local testing.
#
# Usage: ./gen_certs.sh [output_dir]
#   Default output: ./certs/

set -e

CERT_DIR="${1:-$(dirname "$0")/certs}"
DAYS=3650

mkdir -p "${CERT_DIR}"

echo "=== Generating certificates in ${CERT_DIR} ==="

# --- CA ---
echo "Generating CA..."
openssl genrsa -out "${CERT_DIR}/ca.key" 4096 2>/dev/null
openssl req -new -x509 -key "${CERT_DIR}/ca.key" -out "${CERT_DIR}/ca.pem" \
    -days ${DAYS} -subj "/CN=MilvusTestCA"

# --- Per-cluster certs ---
for CLUSTER in cluster-a cluster-b cluster-c; do
    echo "Generating certs for ${CLUSTER}..."

    # Server cert (with SAN for localhost)
    openssl genrsa -out "${CERT_DIR}/${CLUSTER}-server.key" 2048 2>/dev/null
    openssl req -new -key "${CERT_DIR}/${CLUSTER}-server.key" \
        -out "${CERT_DIR}/${CLUSTER}-server.csr" -subj "/CN=${CLUSTER}-server"
    openssl x509 -req -in "${CERT_DIR}/${CLUSTER}-server.csr" \
        -CA "${CERT_DIR}/ca.pem" -CAkey "${CERT_DIR}/ca.key" -CAcreateserial \
        -out "${CERT_DIR}/${CLUSTER}-server.pem" -days ${DAYS} \
        -extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1")

    # Client cert
    openssl genrsa -out "${CERT_DIR}/${CLUSTER}-client.key" 2048 2>/dev/null
    openssl req -new -key "${CERT_DIR}/${CLUSTER}-client.key" \
        -out "${CERT_DIR}/${CLUSTER}-client.csr" -subj "/CN=${CLUSTER}-client"
    openssl x509 -req -in "${CERT_DIR}/${CLUSTER}-client.csr" \
        -CA "${CERT_DIR}/ca.pem" -CAkey "${CERT_DIR}/ca.key" -CAcreateserial \
        -out "${CERT_DIR}/${CLUSTER}-client.pem" -days ${DAYS}

    # Cleanup CSRs
    rm -f "${CERT_DIR}/${CLUSTER}-server.csr" "${CERT_DIR}/${CLUSTER}-client.csr"
done

rm -f "${CERT_DIR}/ca.srl"

echo ""
echo "=== Certificates generated ==="
ls -la "${CERT_DIR}"
