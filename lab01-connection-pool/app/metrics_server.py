import os
import sys
import time
from pathlib import Path
from collections import defaultdict
from contextlib import asynccontextmanager

from prometheus_client import CollectorRegistry, multiprocess, generate_latest
from prometheus_client.parser import text_string_to_metric_families
from prometheus_client.exposition import CONTENT_TYPE_LATEST
from fastapi import FastAPI, Response, HTTPException, Request

import uvicorn
from common import get_multiproc_dir, configure_logging
import structlog

# 1. Call the configuration function FIRST
configure_logging()

# 2. Get the main application logger
# Use the module's __name__ for a properly named stdlib logger
logger = structlog.get_logger(__name__)
# ----------------------------------------------------------------------
# Prometheus FastAPI Metrics Exporter for Multiprocess Celery Workers
# ----------------------------------------------------------------------

# --- Configuration ---
# Get the directory from environment variable, fall back to default
METRICS_BASE_DIR = Path(get_multiproc_dir())
HOST = "0.0.0.0"
PORT = 9080
# ---------------------


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Lifespan context manager for startup and shutdown events"""
    # Startup
    logger.info(
        "server_starting",
        metrics_base_dir=str(METRICS_BASE_DIR),
        pid=os.getpid(),
    )

    worker_dirs = get_worker_dirs()
    if worker_dirs:
        for worker_dir in worker_dirs:
            file_count = len(list(worker_dir.glob("*.db")))
            logger.info(
                "worker_dir_found",
                worker=worker_dir.name,
                file_count=file_count,
            )
    else:
        logger.warning("no_worker_dirs_at_startup")

    yield  # App runs here

    # Shutdown
    logger.info("server_shutting_down")


app = FastAPI(
    title="Prometheus Metrics Aggregator",
    description="Aggregates metrics from multi-process workers via file system.",
    lifespan=lifespan,
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
        logger.info(f"Error during metric aggregation: {e}", file=sys.stderr)
        return b""


def get_worker_dirs() -> list[Path]:
    """Find all worker subdirectories containing metrics files"""
    worker_dirs = []

    if not METRICS_BASE_DIR.exists():
        logger.warning("metrics_dir_missing", path=str(METRICS_BASE_DIR))
        return worker_dirs

    for entry in METRICS_BASE_DIR.iterdir():
        if entry.is_dir() and list(entry.glob("*.db")):
            worker_dirs.append(entry)

    # Fallback: check if .db files are in base dir (old setup)
    if not worker_dirs and list(METRICS_BASE_DIR.glob("*.db")):
        worker_dirs = [METRICS_BASE_DIR]
        logger.info("using_base_dir_fallback", path=str(METRICS_BASE_DIR))

    return worker_dirs


def collect_metrics_from_dir(metrics_dir: Path) -> str | None:
    """Collect metrics from a single directory"""
    try:
        registry = CollectorRegistry()
        multiprocess.MultiProcessCollector(registry, path=str(metrics_dir))
        metrics_text = generate_latest(registry).decode("utf-8")

        # Count metrics for logging
        line_count = len(
            [_l for _l in metrics_text.split("\n") if _l and not _l.startswith("#")]
        )
        logger.debug(
            "collected_metrics",
            worker_dir=metrics_dir.name,
            metric_lines=line_count,
        )

        return metrics_text
    except Exception as e:
        logger.error(
            "metrics_collection_failed",
            worker_dir=str(metrics_dir),
            error=str(e),
            error_type=type(e).__name__,
        )
        return None


def merge_metrics(metrics_texts: list[str]) -> bytes:
    """
    Merge metrics from multiple sources.

    Counters and histograms are summed.
    Gauges take the latest value per label set.
    """
    merged_data: dict[str, dict[tuple, float]] = defaultdict(lambda: defaultdict(float))
    metric_types: dict[str, str] = {}
    metric_help: dict[str, str] = {}
    histogram_buckets: dict[tuple, dict[str, float]] = defaultdict(
        lambda: defaultdict(float)
    )
    histogram_sums: dict[str, dict[tuple, float]] = defaultdict(
        lambda: defaultdict(float)
    )
    histogram_counts: dict[str, dict[tuple, float]] = defaultdict(
        lambda: defaultdict(float)
    )

    for text in metrics_texts:
        for family in text_string_to_metric_families(text):
            metric_types[family.name] = family.type
            if family.documentation:
                metric_help[family.name] = family.documentation

            for sample in family.samples:
                labels = tuple(sorted(sample.labels.items()))

                if family.type == "histogram":
                    if sample.name.endswith("_bucket"):
                        base_name = sample.name[:-7]
                        le = sample.labels.get("le", "")
                        other_labels = tuple(
                            (k, v)
                            for k, v in sorted(sample.labels.items())
                            if k != "le"
                        )
                        histogram_buckets[(base_name, other_labels)][le] += sample.value
                    elif sample.name.endswith("_sum"):
                        base_name = sample.name[:-4]
                        other_labels = tuple(sorted(sample.labels.items()))
                        histogram_sums[base_name][other_labels] += sample.value
                    elif sample.name.endswith("_count"):
                        base_name = sample.name[:-6]
                        other_labels = tuple(sorted(sample.labels.items()))
                        histogram_counts[base_name][other_labels] += sample.value
                elif family.type in ("counter", "gauge", "summary"):
                    merged_data[sample.name][labels] += sample.value

    # Generate merged output
    lines: list[str] = []
    seen_metrics: set[str] = set()

    # Output regular metrics
    for metric_name in sorted(merged_data.keys()):
        base_name = metric_name.replace("_total", "").replace("_created", "")
        if base_name not in seen_metrics:
            seen_metrics.add(base_name)
            if base_name in metric_help:
                lines.append(f"# HELP {base_name} {metric_help[base_name]}")
            if base_name in metric_types:
                lines.append(f"# TYPE {base_name} {metric_types[base_name]}")

        for labels, value in sorted(merged_data[metric_name].items()):
            if labels:
                label_str = ",".join(f'{k}="{v}"' for k, v in labels)
                lines.append(f"{metric_name}{{{label_str}}} {value}")
            else:
                lines.append(f"{metric_name} {value}")

    # Output histograms
    for (base_name, labels), buckets in sorted(histogram_buckets.items()):
        if base_name not in seen_metrics:
            seen_metrics.add(base_name)
            if base_name in metric_help:
                lines.append(f"# HELP {base_name} {metric_help[base_name]}")
            lines.append(f"# TYPE {base_name} histogram")

        label_str = ",".join(f'{k}="{v}"' for k, v in labels) if labels else ""

        sorted_buckets = sorted(
            buckets.items(),
            key=lambda x: float(x[0]) if x[0] != "+Inf" else float("inf"),
        )

        for le, count in sorted_buckets:
            if label_str:
                lines.append(f'{base_name}_bucket{{{label_str},le="{le}"}} {count}')
            else:
                lines.append(f'{base_name}_bucket{{le="{le}"}} {count}')

        if labels in histogram_sums.get(base_name, {}):
            sum_value = histogram_sums[base_name][labels]
            if label_str:
                lines.append(f"{base_name}_sum{{{label_str}}} {sum_value}")
            else:
                lines.append(f"{base_name}_sum {sum_value}")

        if labels in histogram_counts.get(base_name, {}):
            count_value = histogram_counts[base_name][labels]
            if label_str:
                lines.append(f"{base_name}_count{{{label_str}}} {count_value}")
            else:
                lines.append(f"{base_name}_count {count_value}")

    logger.debug(
        "metrics_merged",
        unique_metrics=len(seen_metrics),
        total_lines=len(lines),
    )

    return ("\n".join(lines) + "\n").encode("utf-8")


# Request logging middleware
@app.middleware("http")
async def log_requests(request: Request, call_next):
    """Log all incoming requests with timing"""
    start_time = time.monotonic()

    # Process request
    response = await call_next(request)

    # Calculate duration
    duration_ms = (time.monotonic() - start_time) * 1000

    # Log based on path (reduce noise for health checks)
    if request.url.path == "/health":
        # Only log health checks at debug level or if slow
        if duration_ms > 100:
            logger.warning(
                "slow_health_check",
                path=request.url.path,
                method=request.method,
                status_code=response.status_code,
                duration_ms=round(duration_ms, 2),
            )
    else:
        logger.info(
            "request_completed",
            path=request.url.path,
            method=request.method,
            status_code=response.status_code,
            duration_ms=round(duration_ms, 2),
        )

    return response


@app.get("/metrics")
async def metrics():
    worker_dirs = get_worker_dirs()
    if not worker_dirs:
        logger.warning("no_worker_dirs_found")
        return Response(
            content=b"# No worker metrics found\n", media_type=CONTENT_TYPE_LATEST
        )

    all_metrics = []
    for worker_dir in worker_dirs:
        metrics_text = collect_metrics_from_dir(worker_dir)
        if metrics_text:
            all_metrics.append(metrics_text)
    if not all_metrics:
        logger.warning(
            "no_metrics_collected", worker_dirs=[d.name for d in worker_dirs]
        )
        return Response(
            content=b"# No metrics collected\n", media_type=CONTENT_TYPE_LATEST
        )

    merged = merge_metrics(all_metrics)

    logger.info(
        "metrics_served",
        worker_count=len(worker_dirs),
        sources_collected=len(all_metrics),
        response_bytes=len(merged),
    )

    return Response(content=merged, media_type=CONTENT_TYPE_LATEST)


@app.get("/healthz")
def health() -> dict:
    """Health check endpoint."""
    if not METRICS_BASE_DIR.exists():
        raise HTTPException(
            status_code=500,
            detail={
                "status": "error",
                "message": f"Directory not found: {METRICS_BASE_DIR}",
            },
        )

    try:
        files = list(METRICS_BASE_DIR.iterdir())
    except PermissionError as e:
        raise HTTPException(
            status_code=500,
            detail={"status": "error", "message": f"Permission denied: {e}"},
        )

    return {
        "status": "healthy",
        "metrics_dir": str(METRICS_BASE_DIR),
        "metric_files": len(files),
        "files": [f.name for f in files[:10]],
    }


@app.get("/debug")
async def debug():
    """Debug endpoint showing directories and files"""
    debug_info = {
        "base_dir": str(METRICS_BASE_DIR),
        "base_dir_exists": METRICS_BASE_DIR.exists(),
        "subdirs": {},
        "base_dir_files": [],
    }

    if not METRICS_BASE_DIR.exists():
        logger.warning("debug_metrics_dir_missing", path=str(METRICS_BASE_DIR))
        return debug_info

    for entry in METRICS_BASE_DIR.iterdir():
        if entry.is_dir():
            db_files = list(entry.glob("*.db"))
            debug_info["subdirs"][entry.name] = {
                "files": [f.name for f in db_files],
                "file_count": len(db_files),
            }

    base_files = list(METRICS_BASE_DIR.glob("*.db"))
    debug_info["base_dir_files"] = [f.name for f in base_files]

    logger.info(
        "debug_info_served",
        subdir_count=len(debug_info["subdirs"]),
        base_file_count=len(debug_info["base_dir_files"]),
    )

    return debug_info


@app.get("/debug/totals")
async def debug_totals():
    """Show per-worker metric totals for debugging"""
    worker_dirs = get_worker_dirs()

    result = {
        "workers": {},
        "totals": {
            "task_total": 0,
            "success": 0,
            "failed": 0,
        },
    }

    for worker_dir in worker_dirs:
        metrics_text = collect_metrics_from_dir(worker_dir)
        if not metrics_text:
            continue

        worker_stats = {
            "task_total": 0,
            "success": 0,
            "failed": 0,
        }

        for family in text_string_to_metric_families(metrics_text):
            for sample in family.samples:
                if "celery_task_total" in sample.name:
                    status = sample.labels.get("status", "unknown")
                    if status == "success":
                        worker_stats["success"] += sample.value
                    elif status == "failure":
                        worker_stats["failed"] += sample.value
                    worker_stats["task_total"] += sample.value

        result["workers"][worker_dir.name] = worker_stats
        result["totals"]["task_total"] += worker_stats["task_total"]
        result["totals"]["success"] += worker_stats["success"]
        result["totals"]["failed"] += worker_stats["failed"]

    return result


if __name__ == "__main__":
    logger.info("🚀 Starting Prometheus metrics exporter (FastAPI/Uvicorn)")
    logger.info(f"📂 Metrics directory: {METRICS_BASE_DIR}")
    logger.info(f"🌐 Serving metrics at http://{HOST}:{PORT}/metrics")
    logger.info(f"💚 Serving health check at http://{HOST}:{PORT}/healthz")

    # Make sure directory exists before starting
    try:
        os.makedirs(METRICS_BASE_DIR, exist_ok=True)
    except OSError as e:
        logger.info(
            f"FATAL ERROR: Could not create directory {METRICS_BASE_DIR}. Check permissions. Error: {e}",
            file=sys.stderr,
        )
        sys.exit(1)
    log_level = os.environ.get("LOG_LEVEL", "info").lower()

    logger.info(
        "starting_uvicorn",
        port=PORT,
        log_level=log_level,
    )
    # Use uvicorn to serve the FastAPI app asynchronously
    uvicorn.run(app, host=HOST, port=PORT)
