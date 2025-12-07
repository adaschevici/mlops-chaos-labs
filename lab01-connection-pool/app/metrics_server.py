import os
import sys
from pathlib import Path

from prometheus_client import CollectorRegistry, multiprocess, generate_latest
from prometheus_client.exposition import CONTENT_TYPE_LATEST
from fastapi import FastAPI, Response, HTTPException
import structlog
import logging
import uvicorn

# Configure once at module level
structlog.configure(
    processors=[
        structlog.stdlib.filter_by_level,
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    context_class=dict,
    logger_factory=structlog.stdlib.LoggerFactory(),
    cache_logger_on_first_use=True,
)

# Set log level
logging.basicConfig(level=logging.INFO)

logger = structlog.get_logger()
# ----------------------------------------------------------------------
# Prometheus FastAPI Metrics Exporter for Multiprocess Celery Workers
# ----------------------------------------------------------------------

# --- Configuration ---
# Get the directory from environment variable, fall back to default
PROMETHEUS_MULTIPROC_DIR = os.environ.get(
    "PROMETHEUS_MULTIPROC_DIR", "/tmp/prometheus-multiproc"
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


PROMETHEUS_MULTIPROC_DIR = Path(
    os.environ.get("PROMETHEUS_MULTIPROC_DIR", "/tmp/prometheus-multiproc")
)


@app.get("/metrics")
async def metrics():
    try:
        registry = CollectorRegistry()
        multiprocess.MultiProcessCollector(registry)
        return Response(generate_latest(registry), media_type=CONTENT_TYPE_LATEST)
    except Exception as e:
        logger.error(
            "metric_aggregation_failed", error=str(e), error_type=type(e).__name__
        )

        for f in PROMETHEUS_MULTIPROC_DIR.glob("*.db"):
            try:
                f.unlink()
            except Exception as _e:
                pass

        logger.info("metrics_reset", reason="corruption")
        return Response(
            "# Metrics reset due to corruption\n", media_type=CONTENT_TYPE_LATEST
        )


@app.get("/healthz")
def health() -> dict:
    """Health check endpoint."""
    if not PROMETHEUS_MULTIPROC_DIR.exists():
        raise HTTPException(
            status_code=500,
            detail={
                "status": "error",
                "message": f"Directory not found: {PROMETHEUS_MULTIPROC_DIR}",
            },
        )

    try:
        files = list(PROMETHEUS_MULTIPROC_DIR.iterdir())
    except PermissionError as e:
        raise HTTPException(
            status_code=500,
            detail={"status": "error", "message": f"Permission denied: {e}"},
        )

    return {
        "status": "healthy",
        "metrics_dir": str(PROMETHEUS_MULTIPROC_DIR),
        "metric_files": len(files),
        "files": [f.name for f in files[:10]],
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
