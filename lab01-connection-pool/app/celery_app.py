# ruff: noqa: E402
from gevent import monkey

monkey.patch_all()

from celery import Celery, bootsteps

import os
import socket
from threading import Lock

from commmon import configure_logging, get_multiproc_dir

import structlog

# 1. Call the configuration function FIRST
configure_logging()

# 2. Get the main application logger
# Use the module's __name__ for a properly named stdlib logger
logger = structlog.get_logger(__name__)


PROMETHEUS_MULTIPROC_DIR = get_multiproc_dir()

# Get worker name from environment or hostname
WORKER_NAME = os.getenv("WORKER_NAME", socket.gethostname())

# Track publish timing
publish_times = {}
publish_lock = Lock()

app = Celery("chaos_lab")
app.config_from_object("celeryconfig_redis")
app.autodiscover_tasks(
    ["tasks.redis_exhaustion", "tasks.broker_pool_contention"], force=True
)
# class MonitoringBootstep(bootsteps.StartStopStep):
#     """Start monitoring greenlets for gevent pool workers."""
#
#     requires = {"celery.worker.components:Pool"}
#
#     def __init__(self, worker, **kwargs):
#         self.greenlets: list[gevent.Greenlet] = []
#
#     def start(self, worker):
#         multiproc_dir = Path(PROMETHEUS_MULTIPROC_DIR)
#
#         # Clean stale metric files for this PID
#         if multiproc_dir.exists():
#             for f in multiproc_dir.glob(f"*_{os.getpid()}.db"):
#                 try:
#                     f.unlink()
#                 except Exception:
#                     pass
#         else:
#             multiproc_dir.mkdir(parents=True, exist_ok=True)
#
#         # Initialize metrics with zero values
#         task_counter.labels(task_name="init", status="success", worker=WORKER_NAME)
#         tasks_in_progress.labels(task_name="init", worker=WORKER_NAME).set(0)
#         celery_task_queue_depth.labels(queue_name="celery", worker=WORKER_NAME).set(0)
#
#         # Spawn monitoring greenlets
#         self.greenlets = [
#             gevent.spawn(monitor_redis_pools),
#             gevent.spawn(monitor_broker_pool_saturation),
#         ]
#
#         print(f"✅ Worker {os.getpid()} monitoring started via bootstep")
#
#     def stop(self, worker):
#         if self.greenlets:
#             gevent.killall(self.greenlets, timeout=5)
#             self.greenlets = []
#         print(f"🧹 Worker {os.getpid()} monitoring stopped")
#


# Register AFTER app is created
# app.steps["worker"].add(MonitoringBootstep)
#
# @before_task_publish.connect
# def track_publish_start(sender=None, headers=None, body=None, **kwargs):
#     """Track when task publishing starts"""
#     task_id = headers.get("id") if headers else None
#     if task_id:
#         # with concurrent_publishes_lock:
#         #     global concurrent_publishes_count
#         #     concurrent_publishes_count += 1
#         with publish_lock:
#             publish_times[task_id] = time.time()
#

#
#
# #
# #
# @after_task_publish.connect
# def track_publish_end(sender=None, headers=None, body=None, **kwargs):
#     """Track when task publishing completes - measure pool wait time"""
#     task_id = headers.get("id") if headers else None
#     if task_id:
#         end_time = time.time()
#         # with concurrent_publishes_lock:
#         #     global concurrent_publishes_count
#         #     # Ensure the count doesn't go below zero due to potential edge cases
#         #     concurrent_publishes_count = max(0, concurrent_publishes_count - 1)
#         with publish_lock:
#             start_time = publish_times.pop(task_id, None)
#
#             if start_time:
#                 duration = end_time - start_time
#
#                 # Record in Prometheus
#                 celery_publish_duration.labels(worker=WORKER_NAME).observe(duration)
#                 celery_publish_total.labels(worker=WORKER_NAME).inc()
#
#                 # Enhanced logging with visual indicators
#                 if duration > 5.0:
#                     print(
#                         f"🔴🔴🔴 CRITICAL PUBLISH DELAY: {duration:.3f}s - BROKER POOL EXHAUSTED!"
#                     )
#                 elif duration > 1.0:
#                     print(
#                         f"🔴 SEVERE: Publish took {duration:.3f}s (broker pool contention)"
#                     )
#                 elif duration > 0.5:
#                     print(f"🟡 WARNING: Publish took {duration:.3f}s (pool pressure)")
#                 elif duration > 0.1:
#                     print(f"⚠️  Slow publish: {duration:.3f}s")


#
# # Autodiscover tasks in these modules
