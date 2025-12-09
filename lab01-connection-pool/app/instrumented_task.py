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
    """
    Gevent-compatible task class that tracks publish timing.

    Measures the full apply_async duration including:
    - Connection pool acquisition wait
    - Serialization
    - Network I/O to broker
    """

    # Class-level thresholds (can be overridden per-task)
    CRITICAL_THRESHOLD = 5.0
    SEVERE_THRESHOLD = 1.0
    WARNING_THRESHOLD = 0.5
    SLOW_THRESHOLD = 0.1

    def apply_async(self, args=None, kwargs=None, **options):
        """
        Override apply_async to track publishing duration.

        This captures the actual blocking time, which with gevent
        means time waiting for broker connection from pool.
        """
        start_time = time.monotonic()

        duration = None

        try:
            # This is where blocking happens:
            # 1. Get connection from pool (can block if exhausted)
            # 2. Serialize task
            # 3. Send to broker
            result = super().apply_async(args=args, kwargs=kwargs, **options)

            # successful publish
            duration = time.monotonic() - start_time
            self._record_publish_success(duration)

            return result

        except redis.exceptions.ConnectionError as e:
            # Track connection error
            duration = time.monotonic() - start_time
            self._record_connection_error(duration, e)
            raise

        except redis.exceptions.TimeoutError as e:
            duration = time.monotonic() - start_time
            self._record_timeout_error(duration, e)
            raise

        except Exception as e:
            # Track other errors
            duration = time.monotonic() - start_time
            self._record_error(duration, e)
            raise

        finally:
            # Done with publish attempt
            publish_queue_depth.labels(worker=WORKER_NAME).dec()
            concurrent_publishes.labels(worker=WORKER_NAME).dec()

    def _record_timeout_error(self, duration: float, error: Exception):
        """Record timeout error metrics"""
        celery_publish_failed.labels(
            worker=WORKER_NAME, error_type="TimeoutError"
        ).inc()
        print(f"🔴🔴🔴 [{WORKER_NAME}] PUBLISH TIMEOUT after {duration:.3f}s: {error}")

    def _record_connection_error(self, duration, error):
        celery_publish_connection_errors.labels(worker=WORKER_NAME).inc()
        celery_publish_failed.labels(
            worker=WORKER_NAME, error_type="ConnectionError"
        ).inc()
        print(
            f"🔴🔴🔴 [{WORKER_NAME}] PUBLISH FAILED: ConnectionError after {duration:.3f}s"
        )
        print(f"           Cannot get Redis connection: {error}")

    def _record_publish_success(self, duration):
        """Record successful publish metrics"""
        celery_publish_duration.labels(worker=WORKER_NAME).observe(duration)
        celery_publish_total.labels(worker=WORKER_NAME).inc()

    def _record_error(self, duration: float, error: Exception):
        error_type = type(error).__name__
        celery_publish_failed.labels(worker=WORKER_NAME, error_type=error_type).inc()

        print(f"🔴 PUBLISH FAILED: {error_type} after {duration:.3f}s")

    def _log_duration(self, duration: float):
        """Log publish duration with severity indicators"""
        if duration > self.CRITICAL_THRESHOLD:
            print(
                f"🔴🔴🔴 [{WORKER_NAME}] CRITICAL POOL WAIT: {duration:.3f}s - BROKER POOL SATURATED!"
            )
        elif duration > self.SEVERE_THRESHOLD:
            print(f"🔴 [{WORKER_NAME}] Severe pool contention: {duration:.3f}s")
        elif duration > self.WARNING_THRESHOLD:
            print(f"🟡 [{WORKER_NAME}] Pool pressure: {duration:.3f}s")
        elif duration > self.SLOW_THRESHOLD:
            print(f"⚠️  [{WORKER_NAME}] Slow publish: {duration:.3f}s")
