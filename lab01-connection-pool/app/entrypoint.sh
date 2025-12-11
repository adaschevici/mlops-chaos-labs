#!/bin/bash
# entrypoint.sh

set -e

# Get unique identifier for this container
# HOSTNAME is set by Docker to the container ID (e.g., "abc123def456")
WORKER_ID="${WORKER_NAME:-${HOSTNAME}}"

# Create worker-specific metrics directory
METRICS_DIR="/tmp/prometheus-multiproc/${WORKER_ID}"
echo "[entrypoint] Creating metrics directory at: ${METRICS_DIR}"
mkdir -p "${METRICS_DIR}"

# Export for prometheus_client
export PROMETHEUS_MULTIPROC_DIR="${METRICS_DIR}"

echo "[entrypoint] Worker ID: ${WORKER_ID}"
echo "[entrypoint] Metrics directory: ${METRICS_DIR}"

# Execute the command passed to the container
exec "$@"
