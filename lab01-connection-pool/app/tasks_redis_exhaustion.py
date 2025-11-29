# ruff: noqa: E402
import gevent
from gevent import monkey, spawn
from gevent.pywsgi import WSGIServer  # Import the Native Gevent Server

monkey.patch_all()


from celery import Celery, bootsteps

# Start metrics when worker initializes
import time
import random
import redis
import json
from prometheus_client import Counter, Histogram, Gauge, make_wsgi_app
import os
import socket

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

# Add new metrics for Celery's broker pool
celery_broker_pool_size = Gauge(
    "celery_broker_pool_size", "Celery broker connection pool size", ["worker"]
)

celery_broker_pool_available = Gauge(
    "celery_broker_pool_available", "Available connections in broker pool", ["worker"]
)

celery_broker_pool_in_use = Gauge(
    "celery_broker_pool_in_use",
    "Connections currently in use from broker pool",
    ["worker"],
)


def monitor_celery_pools():
    """
    Gevent greenlet to monitor Celery's internal connection pools.
    This runs continuously and updates Prometheus metrics.
    """
    while True:
        try:
            # Access the connection pool from the broker connection
            # For Redis broker, the pool is at app.connection().pool
            conn = app.connection_for_read()
            pool = (
                conn.default_channel.client.connection_pool
                if hasattr(conn.default_channel.client, "connection_pool")
                else None
            )

            if pool:
                # For redis-py connection pool
                if hasattr(pool, "_available_connections"):
                    # Redis-py 4.x structure
                    available = len(pool._available_connections)
                    in_use = (
                        len(pool._in_use_connections)
                        if hasattr(pool, "_in_use_connections")
                        else 0
                    )
                    pool_limit = pool.max_connections
                elif hasattr(pool, "pool"):
                    # Older structure or Kombu pool
                    available = pool.pool.qsize() if hasattr(pool.pool, "qsize") else 0
                    pool_limit = getattr(pool, "limit", 10)
                    in_use = max(0, pool_limit - available)
                else:
                    # Fallback - try to get from Kombu pool
                    pool_limit = getattr(pool, "limit", 10)
                    available = 0
                    in_use = 0

                celery_broker_pool_size.labels(worker=WORKER_NAME).set(pool_limit)
                celery_broker_pool_available.labels(worker=WORKER_NAME).set(available)
                celery_broker_pool_in_use.labels(worker=WORKER_NAME).set(in_use)

                print(
                    f"📊 Broker Pool: {in_use}/{pool_limit} in use, {available} available"
                )
            else:
                print("⚠️ Could not access broker pool")

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
        super().__init__(worker, **kwargs)

    def start(self, worker):
        print("🚀 Bootstep: initializing Prometheus metrics server...")
        try:
            # Create the WSGI app (No threading involved)
            app = make_wsgi_app()

            # Bind the server to 0.0.0.0:8000
            # log=None quiets the access logs
            self.server = WSGIServer(("0.0.0.0", 8000), app, log=None)

            # Start listening (Non-blocking in Gevent)
            self.server.start()
            print("📊 Metrics Server listening on 0.0.0.0:8000")
            # Start pool monitoring greenlet
            print("🔍 Starting Celery pool monitoring greenlet...")
            self.pool_monitor = spawn(monitor_celery_pools)
            print("✅ Pool monitoring started")

        except Exception as e:
            print(f"❌ Failed to start metrics bootstep: {e}")

    def stop(self, worker):
        # Clean shutdown when worker exits
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
            gevent.sleep(0.5)  # Use gevent.sleep instead of time.sleep

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
        tasks_in_progress.labels(task_name=task_name, worker=WORKER_NAME).dec()
        # Clean up connections (but damage is done)
        for r in connections:
            try:
                r.close()
            except Exception as _e:
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
            # Each .apply_async() grabs a connection from Celery's pool
            result = worker_task.apply_async(
                args=[task_id, i],
                countdown=0,  # Execute immediately
            )
            job.append(result)

        # Wait for all subtasks (holds connections)
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

    except Exception as _e:
        duration = time.time() - start_time
        task_counter.labels(
            task_name=task_name, status="failure", worker=WORKER_NAME
        ).inc()
        task_duration.labels(task_name=task_name, worker=WORKER_NAME).observe(duration)
        raise

    finally:
        tasks_in_progress.labels(task_name=task_name, worker=WORKER_NAME).dec()
