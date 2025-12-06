import os
import sys
from typing import Dict, Any

from prometheus_client import CollectorRegistry, multiprocess, generate_latest
from prometheus_client.exposition import CONTENT_TYPE_LATEST
from fastapi import FastAPI, Response, HTTPException
import uvicorn

# ----------------------------------------------------------------------
# Prometheus FastAPI Metrics Exporter for Multiprocess Celery Workers
# ----------------------------------------------------------------------

# --- Configuration ---
# Get the directory from environment variable, fall back to default
PROMETHEUS_MULTIPROC_DIR = os.environ.get(
    "PROMETHEUS_MULTIPROC_DIR", "/tmp/prometheus_multiproc"
)
HOST = "0.0.0.0"
PORT = 9080
# ---------------------

app = FastAPI(
    title="Prometheus Metrics Aggregator",
    description="Aggregates metrics from multi-process workers via file system.",
)


def collect_multiprocess_metrics(path: str) -> bytes:
    """
    Synchronously collects and aggregates metrics from all processes
    in the given directory path.

    Note: This is a synchronous, I/O-bound function (file access)
    that FastAPI will automatically run in its thread pool.
    """
    try:
        registry = CollectorRegistry()
        multiprocess.MultiProcessCollector(registry, path=path)
        # generate_latest returns bytes in the Prometheus text format
        return generate_latest(registry)
    except Exception as e:
        # Log the error and return a 500 status
        print(f"Error during metric aggregation: {e}", file=sys.stderr)
        return b""


@app.get("/metrics")
# This route is defined as synchronous. FastAPI automatically runs it
# in the thread pool, ensuring the main Uvicorn event loop is non-blocking.
def metrics() -> Response:
    """
    Endpoint that aggregates and exposes Prometheus metrics from all worker files.
    """
    output = collect_multiprocess_metrics(PROMETHEUS_MULTIPROC_DIR)

    if not output:
        # If aggregation failed (e.g., no files or permission error), return a 500 error
        raise HTTPException(
            status_code=500,
            detail="Metric aggregation failed. Check PROMETHEUS_MULTIPROC_DIR permissions.",
        )

    # Use the official CONTENT_TYPE_LATEST constant from prometheus_client
    return Response(content=output, media_type=CONTENT_TYPE_LATEST)


@app.get("/healthz", response_model=Dict[str, Any])
# This route is also defined as synchronous and runs in the thread pool.
def health() -> Dict[str, Any]:
    """
    Lightweight health check endpoint to verify the exporter process is alive
    and can access the metrics directory.
    """
    # 1. Check if metrics directory exists and is accessible
    if not os.path.exists(PROMETHEUS_MULTIPROC_DIR):
        raise HTTPException(
            status_code=500,
            detail={
                "status": "error",
                "message": f"Metrics directory not found: {PROMETHEUS_MULTIPROC_DIR}",
            },
        )

    # 2. Check if we can list files (synchronous I/O)
    try:
        files = os.listdir(PROMETHEUS_MULTIPROC_DIR)
    except OSError as e:
        # Handles permission denied errors ([Errno 13])
        raise HTTPException(
            status_code=500,
            detail={
                "status": "error",
                "message": f"Permission denied to list files in metrics directory: {e}",
            },
        )

    return {
        "status": "healthy",
        "metrics_dir": PROMETHEUS_MULTIPROC_DIR,
        "metric_files": len(files),
        "files": files[:10],  # Show first 10 files
    }


if __name__ == "__main__":
    print("🚀 Starting Prometheus metrics exporter (FastAPI/Uvicorn)")
    print(f"📂 Metrics directory: {PROMETHEUS_MULTIPROC_DIR}")
    print(f"🌐 Serving metrics at http://{HOST}:{PORT}/metrics")
    print(f"💚 Serving health check at http://{HOST}:{PORT}/healthz")

    # Make sure directory exists before starting
    try:
        os.makedirs(PROMETHEUS_MULTIPROC_DIR, exist_ok=True)
    except OSError as e:
        print(
            f"FATAL ERROR: Could not create directory {PROMETHEUS_MULTIPROC_DIR}. Check permissions. Error: {e}",
            file=sys.stderr,
        )
        sys.exit(1)

    # Use uvicorn to serve the FastAPI app asynchronously
    uvicorn.run(app, host=HOST, port=PORT)
