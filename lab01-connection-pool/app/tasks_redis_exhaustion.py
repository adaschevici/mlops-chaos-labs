# ruff: noqa: E402
from gevent import monkey, spawn
from gevent.pywsgi import WSGIServer

monkey.patch_all()

from celery import Celery, bootsteps
from celery.signals import (
    before_task_publish,
    after_task_publish,
)
import time
import random
import redis
import json
from prometheus_client import Counter, Histogram, Gauge, make_wsgi_app
import os
import socket
import gevent
from threading import Lock

app = Celery("chaos_lab")
app.config_from_object("celeryconfig_redis")

# Get worker name from environment or hostname
WORKER_NAME = os.getenv("WORKER_NAME", socket.gethostname())

# Prometheus metrics
task_counter = Counter(
    "celery_task_total", "Total number of tasks", ["task_name", "status", "worker"]
)

task_duration = Histogram(
    "celery_task_duration_seconds",
    "Task execution duration",
    ["task_name", "worker"],
    buckets=[0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300],
)

tasks_in_progress = Gauge(
    "celery_tasks_in_progress",
    "Number of tasks currently executing",
    ["task_name", "worker"],
)

# Queue and broker metrics
celery_task_queue_depth = Gauge(
    "celery_task_queue_depth", "Tasks waiting in queue", ["queue_name", "worker"]
)

celery_broker_operations = Counter(
    "celery_broker_operations_total", "Total broker operations", ["operation", "worker"]
)

# Redis connection pool metrics (from redis-py directly)
redis_pool_size = Gauge(
    "redis_pool_size", "Redis connection pool max size", ["pool_type", "worker"]
)

redis_pool_available = Gauge(
    "redis_pool_available",
    "Available connections in Redis pool",
    ["pool_type", "worker"],
)

redis_pool_in_use = Gauge(
    "redis_pool_in_use", "In-use connections in Redis pool", ["pool_type", "worker"]
)
# Add metric
# NEW: Publish duration metric
celery_publish_duration = Histogram(
    "celery_publish_duration_seconds",
    "Time to publish task to broker (indicates pool contention)",
    ["worker"],
    buckets=[0.001, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0],
)

celery_publish_total = Counter(
    "celery_publish_total", "Total tasks published", ["worker"]
)

# Track publish timing
publish_times = {}
publish_lock = Lock()


@before_task_publish.connect
def track_publish_start(sender=None, headers=None, body=None, **kwargs):
    """Track when task publishing starts"""
    task_id = headers.get("id") if headers else None
    if task_id:
        with publish_lock:
            publish_times[task_id] = time.time()


@after_task_publish.connect
def track_publish_end(sender=None, headers=None, body=None, **kwargs):
    """Track when task publishing completes - measure pool wait time"""
    task_id = headers.get("id") if headers else None
    if task_id:
        end_time = time.time()
        with publish_lock:
            start_time = publish_times.pop(task_id, None)

            if start_time:
                duration = end_time - start_time

                # Record in Prometheus
                celery_publish_duration.labels(worker=WORKER_NAME).observe(duration)
                celery_publish_total.labels(worker=WORKER_NAME).inc()

                # Log slow publishes (indicates pool contention)
                if duration > 0.1:
                    print(
                        f"⚠️  Slow publish: task took {duration:.3f}s to publish (pool contention!)"
                    )
                elif duration > 1.0:
                    print(
                        f"🔴 CRITICAL: task took {duration:.3f}s to publish (severe pool exhaustion!)"
                    )


def monitor_redis_pools():
    """
    Monitor Redis connection pools directly.
    This tracks both application-level and Celery-managed pools.
    """
    while True:
        try:
            # Method 1: Monitor the broker connection pool
            # Get a connection from Celery's pool
            with app.pool.acquire(block=True) as conn:
                # Access the underlying Redis connection pool
                if hasattr(conn, "default_channel"):
                    channel = conn.default_channel
                    if hasattr(channel, "client") and hasattr(
                        channel.client, "connection_pool"
                    ):
                        pool = channel.client.connection_pool

                        # Track broker pool
                        max_conn = pool.max_connections
                        created = pool._created_connections
                        available = len(pool._available_connections)
                        in_use = created - available

                        redis_pool_size.labels(
                            pool_type="broker", worker=WORKER_NAME
                        ).set(max_conn)
                        redis_pool_available.labels(
                            pool_type="broker", worker=WORKER_NAME
                        ).set(available)
                        redis_pool_in_use.labels(
                            pool_type="broker", worker=WORKER_NAME
                        ).set(in_use)

                        print(
                            f"📊 Broker Pool: {in_use}/{max_conn} in use, {available} available, {created} created"
                        )

            # Method 2: Monitor task-created connections (your application connections)
            # This requires tracking via Redis INFO command
            r = redis.from_url("redis://redis:6379/0")
            info = r.info("clients")
            connected_clients = info.get("connected_clients", 0)

            # Also get queue depth
            queue_depth = r.llen("celery")
            celery_task_queue_depth.labels(queue_name="celery", worker=WORKER_NAME).set(
                queue_depth
            )

            r.close()

            if queue_depth > 0:
                print(f"📬 Queue: {queue_depth} tasks waiting")

        except Exception as e:
            print(f"⚠️ Error monitoring pools: {e}")
            import traceback

            traceback.print_exc()

        gevent.sleep(5)


class PrometheusServerStep(bootsteps.StartStopStep):
    """
    A custom Bootstep that launches the Prometheus server
    when the Celery Worker starts.
    """

    requires = {"celery.worker.components:Timer"}

    def __init__(self, worker, **kwargs):
        self.server = None
        self.pool_monitor = None
        super().__init__(worker, **kwargs)

    def start(self, worker):
        print("🚀 Bootstep: initializing Prometheus metrics server...")
        try:
            # Create the WSGI app
            metrics_app = make_wsgi_app()

            # Bind the server to 0.0.0.0:8000
            self.server = WSGIServer(("0.0.0.0", 8000), metrics_app, log=None)

            # Start listening (Non-blocking in Gevent)
            self.server.start()
            print("📊 Metrics Server listening on 0.0.0.0:8000")

            # Start pool monitoring greenlet
            print("🔍 Starting Redis pool monitoring greenlet...")
            self.pool_monitor = spawn(monitor_redis_pools)
            print("✅ Pool monitoring started")

        except Exception as e:
            print(f"❌ Failed to start metrics bootstep: {e}")

    def stop(self, worker):
        if self.server:
            print("🛑 Stopping metrics server...")
            self.server.stop()

        if self.pool_monitor:
            print("🛑 Stopping pool monitor...")
            self.pool_monitor.kill()


# Register the bootstep with the worker
app.steps["worker"].add(PrometheusServerStep)


# Each task opens its OWN Redis connection (outside Celery's pool)
def get_redis():
    """Tasks open their own connections - EXHAUSTS POOL"""
    return redis.from_url("redis://redis:6379/0")


@app.task(bind=True)
def task_with_extra_connections(self, task_id, operations=10):
    """
    This task opens additional Redis connections
    beyond what Celery manages - THIS is what breaks it
    """
    print(f"Task {task_id} starting - will open {operations} connections")

    task_name = "task_with_extra_connections"

    # Track task start
    tasks_in_progress.labels(task_name=task_name, worker=WORKER_NAME).inc()
    start_time = time.time()

    connections = []

    try:
        # Open multiple connections (simulates: caching, session storage, etc.)
        for i in range(operations):
            r = get_redis()  # ← Each opens a NEW connection!
            connections.append(r)

            # Do work that holds the connection
            r.set(
                f"task:{task_id}:step:{i}",
                json.dumps(
                    {
                        "status": "processing",
                        "timestamp": time.time(),
                        "worker": WORKER_NAME,
                    }
                ),
            )
            gevent.sleep(0.5)

        gevent.sleep(random.uniform(1, 10))  # Simulate processing

        # Record success
        duration = time.time() - start_time
        task_counter.labels(
            task_name=task_name, status="success", worker=WORKER_NAME
        ).inc()
        task_duration.labels(task_name=task_name, worker=WORKER_NAME).observe(duration)

        print(f"Task {task_id} completed {operations} operations")
        return {
            "task_id": task_id,
            "operations": operations,
            "worker": WORKER_NAME,
            "duration": duration,
        }

    except redis.exceptions.ConnectionError as e:
        print(f"Task {task_id} FAILED: Connection error - {e}")

        # Record failure
        duration = time.time() - start_time
        task_counter.labels(
            task_name=task_name, status="failure", worker=WORKER_NAME
        ).inc()
        task_duration.labels(task_name=task_name, worker=WORKER_NAME).observe(duration)

        raise
    except Exception as e:
        print(f"Task {task_id} FAILED: {e}")

        # Record failure
        duration = time.time() - start_time
        task_counter.labels(
            task_name=task_name, status="failure", worker=WORKER_NAME
        ).inc()
        task_duration.labels(task_name=task_name, worker=WORKER_NAME).observe(duration)

        raise
    finally:
        # ✅ CRITICAL: Decrement the gauge when task finishes
        tasks_in_progress.labels(task_name=task_name, worker=WORKER_NAME).dec()

        # Clean up connections (but damage is done)
        for r in connections:
            try:
                r.close()
            except Exception:
                pass


@app.task(bind=True)
def task_with_broker_pool_contention(self, task_id, subtasks=20):
    """
    This task spawns many subtasks, exhausting Celery's broker pool.
    Each subtask needs a broker connection to be sent/received.
    """
    print(f"Task {task_id} spawning {subtasks} subtasks - will exhaust broker pool")

    task_name = "task_with_broker_pool_contention"

    # Track task start
    tasks_in_progress.labels(task_name=task_name, worker=WORKER_NAME).inc()
    start_time = time.time()

    try:
        # Spawn many subtasks (each needs broker connection)
        job = []
        for i in range(subtasks):
            result = worker_task.apply_async(args=[task_id, i], countdown=0)
            job.append(result)

        # Wait for all subtasks
        results = [r.get(timeout=30) for r in job]

        # Record success
        duration = time.time() - start_time
        task_counter.labels(
            task_name=task_name, status="success", worker=WORKER_NAME
        ).inc()
        task_duration.labels(task_name=task_name, worker=WORKER_NAME).observe(duration)

        print(f"Task {task_id} completed with {len(results)} subtasks")
        return {
            "task_id": task_id,
            "subtasks": len(results),
            "worker": WORKER_NAME,
            "duration": duration,
        }

    except Exception as e:
        print(f"Task {task_id} FAILED: {e}")

        duration = time.time() - start_time
        task_counter.labels(
            task_name=task_name, status="failure", worker=WORKER_NAME
        ).inc()
        task_duration.labels(task_name=task_name, worker=WORKER_NAME).observe(duration)

        raise

    finally:
        tasks_in_progress.labels(task_name=task_name, worker=WORKER_NAME).dec()


@app.task(bind=True)
def worker_task(self, parent_id, step_id):
    """
    Simple worker task that does minimal work.
    The contention comes from having MANY of these.
    """
    task_name = "worker_task"

    tasks_in_progress.labels(task_name=task_name, worker=WORKER_NAME).inc()
    start_time = time.time()

    try:
        # Simulate some work
        gevent.sleep(random.uniform(1, 3))

        # Maybe touch Redis (adds to contention)
        r = get_redis()
        r.set(f"subtask:{parent_id}:{step_id}", "completed")
        r.close()

        duration = time.time() - start_time
        task_counter.labels(
            task_name=task_name, status="success", worker=WORKER_NAME
        ).inc()
        task_duration.labels(task_name=task_name, worker=WORKER_NAME).observe(duration)

        return {"step": step_id, "status": "completed"}

    except Exception as e:
        duration = time.time() - start_time
        task_counter.labels(
            task_name=task_name, status="failure", worker=WORKER_NAME
        ).inc()
        task_duration.labels(task_name=task_name, worker=WORKER_NAME).observe(duration)
        raise

    finally:
        tasks_in_progress.labels(task_name=task_name, worker=WORKER_NAME).dec()
