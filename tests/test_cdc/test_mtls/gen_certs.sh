#!/bin/bash
# Generate per-cluster CA + server cert + client certs for mTLS testing.
# Each cluster has its own CA so cross-cluster misuse is detected.
#
# Per cluster (i = 1, 2, 3):
#   ca-dev{i}.{pem,key}        — CA for cluster by-dev{i}
#   server-dev{i}.{pem,key}    — server cert for by-dev{i} (SAN=localhost)
#   client-dev{i}.{pem,key}    — CDC client cert for connecting TO by-dev{i} (CN=cdc-to-dev{i})
#   pymilvus-dev{i}.{pem,key}  — PyMilvus client cert for connecting TO by-dev{i} (CN=pymilvus-to-dev{i})
#
# Usage: ./gen_certs.sh [output_dir]
#   Default output: ./certs/

set -e

CERT_DIR="${1:-$(dirname "$0")/certs}"
DAYS=3650

mkdir -p "${CERT_DIR}"

echo "=== Generating per-cluster certificates in ${CERT_DIR} ==="

for i in 1 2 3; do
    echo "--- Cluster by-dev${i} ---"

    # CA
    echo "  Generating CA (ca-dev${i})..."
    openssl genrsa -out "${CERT_DIR}/ca-dev${i}.key" 4096 2>/dev/null
    openssl req -new -x509 -key "${CERT_DIR}/ca-dev${i}.key" -out "${CERT_DIR}/ca-dev${i}.pem" \
        -days ${DAYS} -subj "/CN=MilvusTestCA-dev${i}"

    # Server cert (SAN=localhost)
    echo "  Generating server cert (server-dev${i})..."
    openssl genrsa -out "${CERT_DIR}/server-dev${i}.key" 2048 2>/dev/null
    openssl req -new -key "${CERT_DIR}/server-dev${i}.key" \
        -out "${CERT_DIR}/server-dev${i}.csr" -subj "/CN=milvus-server-dev${i}"
    openssl x509 -req -in "${CERT_DIR}/server-dev${i}.csr" \
        -CA "${CERT_DIR}/ca-dev${i}.pem" -CAkey "${CERT_DIR}/ca-dev${i}.key" -CAcreateserial \
        -out "${CERT_DIR}/server-dev${i}.pem" -days ${DAYS} \
        -extfile <(printf "subjectAltName=DNS:localhost,IP:127.0.0.1")

    # CDC client cert (for CDC connecting TO this cluster)
    echo "  Generating CDC client cert (client-dev${i}, CN=cdc-to-dev${i})..."
    openssl genrsa -out "${CERT_DIR}/client-dev${i}.key" 2048 2>/dev/null
    openssl req -new -key "${CERT_DIR}/client-dev${i}.key" \
        -out "${CERT_DIR}/client-dev${i}.csr" -subj "/CN=cdc-to-dev${i}"
    openssl x509 -req -in "${CERT_DIR}/client-dev${i}.csr" \
        -CA "${CERT_DIR}/ca-dev${i}.pem" -CAkey "${CERT_DIR}/ca-dev${i}.key" -CAcreateserial \
        -out "${CERT_DIR}/client-dev${i}.pem" -days ${DAYS}

    # PyMilvus client cert (for PyMilvus connecting TO this cluster)
    echo "  Generating PyMilvus client cert (pymilvus-dev${i}, CN=pymilvus-to-dev${i})..."
    openssl genrsa -out "${CERT_DIR}/pymilvus-dev${i}.key" 2048 2>/dev/null
    openssl req -new -key "${CERT_DIR}/pymilvus-dev${i}.key" \
        -out "${CERT_DIR}/pymilvus-dev${i}.csr" -subj "/CN=pymilvus-to-dev${i}"
    openssl x509 -req -in "${CERT_DIR}/pymilvus-dev${i}.csr" \
        -CA "${CERT_DIR}/ca-dev${i}.pem" -CAkey "${CERT_DIR}/ca-dev${i}.key" -CAcreateserial \
        -out "${CERT_DIR}/pymilvus-dev${i}.pem" -days ${DAYS}
done

# Cleanup
rm -f "${CERT_DIR}"/*.csr "${CERT_DIR}"/*.srl

echo ""
echo "=== Per-cluster certificates generated ==="
ls -la "${CERT_DIR}"
