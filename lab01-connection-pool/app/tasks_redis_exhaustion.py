from gevent import monkey, spawn
from gevent.pywsgi import WSGIServer  # Import the Native Gevent Server

monkey.patch_all()


from celery import Celery, bootsteps

# Start metrics when worker initializes
from celery.signals import worker_process_init, worker_process_shutdown
import time
import random
import redis
import json
from prometheus_client import Counter, Histogram, Gauge, make_wsgi_app
import threading
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

        except Exception as e:
            print(f"❌ Failed to start metrics bootstep: {e}")

    def stop(self, worker):
        # Clean shutdown when worker exits
        if self.server:
            print("🛑 Stopping metrics server...")
            self.server.stop()


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
            time.sleep(0.5)

        time.sleep(random.uniform(1, 10))  # Simulate processing

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
        # Clean up connections (but damage is done)
        for r in connections:
            try:
                r.close()
            except Exception as _e:
                pass


@app.task(bind=True)
def task_with_result_backend_pressure(self, task_id, result_size_mb=5):
    """
    Store large results - exhausts result backend connections
    """
    print(f"Task {task_id} generating {result_size_mb}MB result")

    # Generate large result
    large_data = {
        "task_id": task_id,
        "predictions": [random.random() for _ in range(result_size_mb * 100000)],
        "metadata": {"model": "test-model", "version": "1.0", "timestamp": time.time()},
    }

    time.sleep(1)  # Simulate processing

    # This stores to result backend (uses connection from pool)
    return large_data


@app.task(bind=True)
def task_with_streaming_results(self, task_id, updates=20):
    """
    Tasks that update state multiple times (each uses connection)
    """
    print(f"Task {task_id} will send {updates} progress updates")

    for i in range(updates):
        # Each update uses a result backend connection
        self.update_state(
            state="PROGRESS",
            meta={
                "current": i,
                "total": updates,
                "status": f"Processing step {i + 1}/{updates}",
            },
        )
        time.sleep(0.2)  # Work while connection might be held

    return {"task_id": task_id, "updates": updates}


@app.task(bind=True)
def blocking_task(self, task_id, duration=10):
    """
    Long-running task that holds connections
    """
    print(f"Task {task_id} blocking for {duration}s")

    # Hold connection for entire duration
    r = get_redis()

    duration = time.sleep(
        random.uniform(1, 50)
    )  # Simulate work while holding connection
    try:
        # Set a key and hold the connection
        r.set(f"blocking:{task_id}", "locked")
        time.sleep(duration)
        r.delete(f"blocking:{task_id}")

        return {"task_id": task_id, "duration": duration}
    finally:
        r.close()
