# ruff: noqa: E402
from gevent import monkey

monkey.patch_all()

import time
import redis
from celery import Task
from prometheus_client import Histogram, Counter, Gauge
from celery_app import WORKER_NAME


celery_publish_total = Counter("celery_publish", "Total tasks published", ["worker"])
# Publish duration metric
celery_publish_duration = Histogram(
    "celery_publish_duration_seconds",
    "Time to publish task to broker (indicates pool contention)",
    ["worker"],
    buckets=[0.001, 0.01, 0.05, 0.1, 0.5, 1.0, 5.0, 10.0, 30.0],
)

# Add new metrics for publish failures
celery_publish_failed = Counter(
    "celery_publish_failed", "Failed task publishes", ["worker", "error_type"]
)

celery_publish_connection_errors = Counter(
    "celery_publish_connection_errors",
    "Connection errors during publish",
    ["worker"],
)

# Track concurrent publishes (proxy for pool usage)
concurrent_publishes = Gauge(
    "celery_concurrent_publishes",
    "Number of publishes happening concurrently",
    ["worker"],
    multiprocess_mode="livesum",
)

# Track publish queue (tasks waiting for pool connection)
publish_queue_depth = Gauge(
    "celery_publish_queue_depth",
    "Tasks waiting for broker pool connection",
    ["worker"],
    multiprocess_mode="livesum",
)


class InstrumentedTask(Task):
    """Custom task class that tracks publish failures"""

    def apply_async(self, args=None, kwargs=None, **options):
        """Override apply_async to track publishing"""
        # global _concurrent_publishes_count
        #
        # # Track that we're TRYING to publish
        # with concurrent_publishes_lock:
        #     global concurrent_publishes_count
        #     concurrent_publishes_count += 1
        #     publish_queue_depth.labels(worker=WORKER_NAME).inc()
        #     concurrent_publishes.labels(worker=WORKER_NAME).inc()
        start_time = time.time()

        duration = None

        try:
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
