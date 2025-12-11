# ruff: noqa: E402
from gevent import monkey

monkey.patch_all()

import time
import random
import redis
import json
from prometheus_client import Counter, Gauge
from instrumented_task import InstrumentedTask
import gevent
from threading import Lock
from celery_app import app, WORKER_NAME
from tasks.common import get_redis
from tasks.metrics import tasks_in_progress, task_counter, task_duration


# Queue and broker metrics
celery_task_queue_depth = Gauge(
    "celery_task_queue_depth",
    "Tasks waiting in queue",
    ["queue_name", "worker"],
    multiprocess_mode="livesum",
)

celery_broker_operations = Counter(
    "celery_broker_operations", "Total broker operations", ["operation", "worker"]
)

# Redis connection pool metrics (from redis-py directly)
redis_pool_size = Gauge(
    "redis_pool_size",
    "Redis connection pool max size",
    ["pool_type", "worker"],
    multiprocess_mode="liveall",
)

redis_pool_available = Gauge(
    "redis_pool_available",
    "Available connections in Redis pool",
    ["pool_type", "worker"],
    multiprocess_mode="livesum",
)

redis_pool_in_use = Gauge(
    "redis_pool_in_use",
    "In-use connections in Redis pool",
    ["pool_type", "worker"],
    multiprocess_mode="livesum",
)


celery_broker_pool_max = Gauge(
    "celery_broker_pool_max",
    "Maximum broker pool connections (from config)",
    ["worker"],
    multiprocess_mode="liveall",
)

celery_broker_pool_active = Gauge(
    "celery_broker_pool_active",
    "Currently active broker pool connections",
    ["worker"],
    multiprocess_mode="livesum",
)

celery_broker_pool_saturation = Gauge(
    "celery_broker_pool_saturation_percent",
    "Broker pool saturation percentage",
    ["worker"],
    multiprocess_mode="livesum",
)


# FIXED: Manual counter for tracking concurrent publishes
concurrent_publishes_count = 0
concurrent_publishes_lock = Lock()


# def monitor_broker_pool_saturation():
#     """
#     Monitor Celery's broker pool saturation.
#     The broker pool is what limits concurrent publishes.
#     """
#     while True:
#         try:
#             # Get broker_pool_limit from Celery config
#             broker_pool_limit = app.conf.get("broker_pool_limit", 10)  # Default is 10
#
#             # Set the max gauge
#             celery_broker_pool_max.labels(worker=WORKER_NAME).set(broker_pool_limit)
#
#             with concurrent_publishes_lock:
#                 current_concurrent = concurrent_publishes_count
#
#             celery_broker_pool_active.labels(worker=WORKER_NAME).set(current_concurrent)
#
#             # Calculate saturation
#             saturation = (
#                 (current_concurrent / broker_pool_limit * 100)
#                 if broker_pool_limit > 0
#                 else 0
#             )
#             celery_broker_pool_saturation.labels(worker=WORKER_NAME).set(saturation)
#             concurrent_publishes.labels(worker=WORKER_NAME).set(current_concurrent)
#             celery_broker_pool_saturation.labels(worker=WORKER_NAME).set(saturation)
#
#             # Log when saturated
#             if saturation >= 100:
#                 print(
#                     f"🔴 BROKER POOL SATURATED: {current_concurrent}/{broker_pool_limit} ({saturation:.0f}%)"
#                 )
#             elif saturation >= 80:
#                 print(
#                     f"🟡 Broker pool pressure: {current_concurrent}/{broker_pool_limit} ({saturation:.0f}%)"
#                 )
#
#         except Exception as e:
#             print(f"⚠️ Error monitoring broker pool: {e}")
#
#         gevent.sleep(1)  # Check every second
#
#
# def monitor_redis_pools():
#     """
#     Monitor Redis connection pools directly.
#     This tracks both application-level and Celery-managed pools.
#     """
#     while True:
#         try:
#             # Method 1: Monitor the broker connection pool
#             # Get a connection from Celery's pool
#             with app.pool.acquire(block=True) as conn:
#                 # Access the underlying Redis connection pool
#                 if hasattr(conn, "default_channel"):
#                     channel = conn.default_channel
#                     if hasattr(channel, "client") and hasattr(
#                         channel.client, "connection_pool"
#                     ):
#                         pool = channel.client.connection_pool
#
#                         # Track broker pool
#                         max_conn = pool.max_connections
#                         created = pool._created_connections
#                         available = len(pool._available_connections)
#                         in_use = created - available
#
#                         redis_pool_size.labels(
#                             pool_type="broker", worker=WORKER_NAME
#                         ).set(max_conn)
#                         redis_pool_available.labels(
#                             pool_type="broker", worker=WORKER_NAME
#                         ).set(available)
#                         redis_pool_in_use.labels(
#                             pool_type="broker", worker=WORKER_NAME
#                         ).set(in_use)
#
#                         print(
#                             f"📊 Broker Pool: {in_use}/{max_conn} in use, {available} available, {created} created"
#                         )
#
#             # Method 2: Monitor task-created connections (your application connections)
#             # This requires tracking via Redis INFO command
#             r = redis.from_url("redis://redis:6379/0")
#             info = r.info("clients")
#             _connected_clients = info.get("connected_clients", 0)
#
#             # Also get queue depth
#             queue_depth = r.llen("celery")
#             celery_task_queue_depth.labels(queue_name="celery", worker=WORKER_NAME).set(
#                 queue_depth
#             )
#
#             r.close()
#
#             if queue_depth > 0:
#                 print(f"📬 Queue: {queue_depth} tasks waiting")
#
#         except Exception as e:
#             print(f"⚠️ Error monitoring pools: {e}")
#             import traceback
#
#             traceback.print_exc()
#
#         gevent.sleep(5)


@app.task(bind=True, base=InstrumentedTask)
def task_with_extra_connections(self, task_id, operations=10):
    """
    This task opens additional Redis connections
    beyond what Celery manages - THIS is what breaks it
    """
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
