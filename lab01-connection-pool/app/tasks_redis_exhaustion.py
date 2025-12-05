# ruff: noqa: E402
from gevent import monkey

monkey.patch_all()

from celery import Celery, Task
from celery.signals import (
    before_task_publish,
    after_task_publish,
)
import time
import random
import redis
import json
from prometheus_client import Counter, Histogram, Gauge
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

# Add new metrics for publish failures
celery_publish_failed = Counter(
    "celery_publish_failed_total", "Failed task publishes", ["worker", "error_type"]
)

celery_publish_connection_errors = Counter(
    "celery_publish_connection_errors_total",
    "Connection errors during publish",
    ["worker"],
)

celery_result_get_duration = Histogram(
    "celery_result_get_duration_seconds",
    "Time waiting for task results (indicates pool blocking)",
    ["worker", "status"],
    buckets=[0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 120.0],
)
celery_broker_pool_max = Gauge(
    "celery_broker_pool_max",
    "Maximum broker pool connections (from config)",
    ["worker"],
)

celery_broker_pool_active = Gauge(
    "celery_broker_pool_active", "Currently active broker pool connections", ["worker"]
)

celery_broker_pool_saturation = Gauge(
    "celery_broker_pool_saturation_percent",
    "Broker pool saturation percentage",
    ["worker"],
)

# Track concurrent publishes (proxy for pool usage)
concurrent_publishes = Gauge(
    "celery_concurrent_publishes",
    "Number of publishes happening concurrently",
    ["worker"],
)

# Track publish queue (tasks waiting for pool connection)
publish_queue_depth = Gauge(
    "celery_publish_queue_depth", "Tasks waiting for broker pool connection", ["worker"]
)
# Track publish timing
publish_times = {}
publish_lock = Lock()

# FIXED: Manual counter for tracking concurrent publishes
_concurrent_publishes_count = 0
_concurrent_publishes_lock = Lock()


class InstrumentedTask(Task):
    """Custom task class that tracks publish failures"""

    def apply_async(self, args=None, kwargs=None, **options):
        """Override apply_async to track publishing"""
        global _concurrent_publishes_count

        # Track that we're TRYING to publish
        with _concurrent_publishes_lock:
            _concurrent_publishes_count += 1
            publish_queue_depth.labels(worker=WORKER_NAME).inc()
            concurrent_publishes.labels(worker=WORKER_NAME).inc()
        start_time = time.time()

        duration = None

        try:
            # Try to publish
            result = super().apply_async(args=args, kwargs=kwargs, **options)
            # This is where it blocks waiting for a pool connection!
            result = super().apply_async(args=args, kwargs=kwargs, **options)

            # Track successful publish duration
            duration = time.time() - start_time
            celery_publish_duration.labels(worker=WORKER_NAME).observe(duration)
            celery_publish_total.labels(worker=WORKER_NAME).inc()

            # Log pool contention
            if duration > 5.0:
                print(
                    f"🔴🔴🔴 CRITICAL POOL WAIT: {duration:.3f}s - BROKER POOL SATURATED!"
                )
            elif duration > 1.0:
                print(
                    f"🔴 Severe pool contention: {duration:.3f}s waiting for broker connection"
                )
            elif duration > 0.5:
                print(f"🟡 Pool pressure: {duration:.3f}s")

            return result

        except redis.exceptions.ConnectionError as _e:
            # Track connection error
            duration = time.time() - start_time
            celery_publish_connection_errors.labels(worker=WORKER_NAME).inc()
            celery_publish_failed.labels(
                worker=WORKER_NAME, error_type="ConnectionError"
            ).inc()

            print(f"🔴🔴🔴 PUBLISH FAILED: ConnectionError after {duration:.3f}s")
            print("           Cannot get Redis connection to publish task!")
            raise

        except Exception as e:
            # Track other errors
            duration = time.time() - start_time
            error_type = type(e).__name__
            celery_publish_failed.labels(
                worker=WORKER_NAME, error_type=error_type
            ).inc()

            print(f"🔴 PUBLISH FAILED: {error_type} after {duration:.3f}s")
            raise
        finally:
            # Done with publish attempt
            publish_queue_depth.labels(worker=WORKER_NAME).dec()
            concurrent_publishes.labels(worker=WORKER_NAME).dec()


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

                # Enhanced logging with visual indicators
                if duration > 5.0:
                    print(
                        f"🔴🔴🔴 CRITICAL PUBLISH DELAY: {duration:.3f}s - BROKER POOL EXHAUSTED!"
                    )
                elif duration > 1.0:
                    print(
                        f"🔴 SEVERE: Publish took {duration:.3f}s (broker pool contention)"
                    )
                elif duration > 0.5:
                    print(f"🟡 WARNING: Publish took {duration:.3f}s (pool pressure)")
                elif duration > 0.1:
                    print(f"⚠️  Slow publish: {duration:.3f}s")


def monitor_broker_pool_saturation():
    """
    Monitor Celery's broker pool saturation.
    The broker pool is what limits concurrent publishes.
    """
    while True:
        try:
            # Get broker_pool_limit from Celery config
            broker_pool_limit = app.conf.get("broker_pool_limit", 10)  # Default is 10

            # Set the max gauge
            celery_broker_pool_max.labels(worker=WORKER_NAME).set(broker_pool_limit)

            # Get current concurrent publishes as proxy for active connections
            # This is a proxy because Celery doesn't expose pool._in_use directly
            current_concurrent = concurrent_publishes._metrics.get(
                (WORKER_NAME,), concurrent_publishes._metric_init()
            )._value._value

            celery_broker_pool_active.labels(worker=WORKER_NAME).set(current_concurrent)

            # Calculate saturation
            saturation = (
                (current_concurrent / broker_pool_limit * 100)
                if broker_pool_limit > 0
                else 0
            )
            celery_broker_pool_saturation.labels(worker=WORKER_NAME).set(saturation)

            # Log when saturated
            if saturation >= 100:
                print(
                    f"🔴 BROKER POOL SATURATED: {current_concurrent}/{broker_pool_limit} ({saturation:.0f}%)"
                )
            elif saturation >= 80:
                print(
                    f"🟡 Broker pool pressure: {current_concurrent}/{broker_pool_limit} ({saturation:.0f}%)"
                )

        except Exception as e:
            print(f"⚠️ Error monitoring broker pool: {e}")

        gevent.sleep(1)  # Check every second


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
            _connected_clients = info.get("connected_clients", 0)

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


# Each task opens its OWN Redis connection (outside Celery's pool)
def get_redis():
    """Tasks open their own connections - EXHAUSTS POOL"""
    return redis.from_url("redis://redis:6379/0")


@app.task(bind=True, base=InstrumentedTask)
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


@app.task(bind=True, base=InstrumentedTask)
def task_with_broker_pool_contention(self, task_id, subtasks=50):
    """
    This task spawns subtasks, stressing Celery's broker pool.
    """
    print(f"📤 Task {task_id} spawning {subtasks} subtasks - will exhaust broker pool")

    task_name = "task_with_broker_pool_contention"
    tasks_in_progress.labels(task_name=task_name, worker=WORKER_NAME).inc()
    start_time = time.time()

    try:
        # Spawn many subtasks
        jobs = []
        publish_start = time.time()

        for i in range(subtasks):
            job_start = time.time()
            result = worker_task.apply_async(args=[task_id, i], countdown=0)
            job_duration = time.time() - job_start

            # Log slow publishes
            if job_duration > 0.5:
                print(
                    f"⚠️  Slow subtask publish #{i}: {job_duration:.2f}s (pool contention!)"
                )

            jobs.append(result)

        publish_duration = time.time() - publish_start
        print(
            f"✅ Published {subtasks} subtasks in {publish_duration:.2f}s (avg {publish_duration / subtasks:.3f}s each)"
        )

        # CRITICAL: Wait for results
        print(f"⏳ Waiting for {len(jobs)} subtask results (timeout=60s each)...")
        results = []
        get_start = time.time()

        for i, job in enumerate(jobs):
            try:
                result_start = time.time()
                result = job.get(timeout=60)  # Increased timeout
                result_duration = time.time() - result_start

                celery_result_get_duration.labels(
                    worker=WORKER_NAME, status="success"
                ).observe(result_duration)
                if result_duration > 5.0:
                    print(
                        f"🐌 Subtask #{i} took {result_duration:.1f}s to return (slow!)"
                    )

                results.append(result)

                # Show progress every 10 subtasks
                if (i + 1) % 10 == 0:
                    print(f"   Progress: {i + 1}/{len(jobs)} subtasks completed")

            except Exception as e:
                get_duration = time.time() - result_start
                print(
                    f"❌ Subtask #{i} FAILED after {get_duration:.1f}s: {type(e).__name__}: {e}"
                )
                celery_result_get_duration.labels(
                    worker=WORKER_NAME, status="timeout"
                ).observe(result_duration)
                raise
                # Continue trying other subtasks instead of failing immediately

        get_duration = time.time() - get_start
        print(f"✅ Collected {len(results)}/{len(jobs)} results in {get_duration:.2f}s")

        # Record success
        duration = time.time() - start_time
        task_counter.labels(
            task_name=task_name, status="success", worker=WORKER_NAME
        ).inc()
        task_duration.labels(task_name=task_name, worker=WORKER_NAME).observe(duration)

        success_rate = len(results) / len(jobs) * 100
        print(
            f"🎯 Task {task_id} completed: {len(results)}/{len(jobs)} subtasks ({success_rate:.0f}%) in {duration:.1f}s total"
        )

        return {
            "task_id": task_id,
            "subtasks_spawned": len(jobs),
            "subtasks_completed": len(results),
            "success_rate": success_rate,
            "worker": WORKER_NAME,
            "duration": duration,
            "publish_duration": publish_duration,
            "get_duration": get_duration,
        }

    except Exception as e:
        print(f"💥 Task {task_id} CATASTROPHIC FAILURE: {e}")
        duration = time.time() - start_time
        task_counter.labels(
            task_name=task_name, status="failure", worker=WORKER_NAME
        ).inc()
        task_duration.labels(task_name=task_name, worker=WORKER_NAME).observe(duration)
        raise

    finally:
        tasks_in_progress.labels(task_name=task_name, worker=WORKER_NAME).dec()


@app.task(bind=True, base=InstrumentedTask)
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
