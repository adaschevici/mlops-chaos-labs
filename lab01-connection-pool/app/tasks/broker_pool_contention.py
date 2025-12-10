# ruff: noqa: E402
from gevent import monkey
import gevent

monkey.patch_all()

import time
import random
from prometheus_client import Histogram
from celery_app import app, WORKER_NAME
from tasks.common import get_redis
from tasks.metrics import tasks_in_progress, task_counter, task_duration


celery_result_get_duration = Histogram(
    "celery_result_get_duration_seconds",
    "Time waiting for task results (indicates pool blocking)",
    ["worker", "status"],
    buckets=[0.1, 0.5, 1.0, 5.0, 10.0, 30.0, 60.0, 120.0],
)


@app.task(bind=True)
def task_with_broker_pool_contention(self, task_id, subtasks=50):
    """Task that spawns subtasks, stressing Celery's broker pool"""
    print(f"📤 Task {task_id} spawning {subtasks} subtasks - will exhaust broker pool")

    task_name = "task_with_broker_pool_contention"
    tasks_in_progress.labels(task_name=task_name, worker=WORKER_NAME).inc()
    start_time = time.time()

    try:
        jobs = []
        publish_start = time.time()

        for i in range(subtasks):
            job_start = time.time()
            result = worker_task.apply_async(args=[task_id, i], countdown=0)
            job_duration = time.time() - job_start

            if job_duration > 0.5:
                print(
                    f"⚠️  Slow subtask publish #{i}: {job_duration:.2f}s (pool contention!)"
                )

            jobs.append(result)

        publish_duration = time.time() - publish_start
        print(f"✅ Published {subtasks} subtasks in {publish_duration:.2f}s")

        # Wait for results
        print(f"⏳ Waiting for {len(jobs)} subtask results...")
        results = []
        get_start = time.time()

        for i, job in enumerate(jobs):
            try:
                result_start = time.time()
                result = job.get(timeout=60)
                result_duration = time.time() - result_start

                celery_result_get_duration.labels(
                    worker=WORKER_NAME, status="success"
                ).observe(result_duration)

                if result_duration > 5.0:
                    print(f"🐌 Subtask #{i} took {result_duration:.1f}s to return")

                results.append(result)

                if (i + 1) % 10 == 0:
                    print(f"   Progress: {i + 1}/{len(jobs)} subtasks completed")

            except Exception as e:
                get_duration = time.time() - result_start
                print(f"❌ Subtask #{i} FAILED after {get_duration:.1f}s: {e}")
                celery_result_get_duration.labels(
                    worker=WORKER_NAME, status="timeout"
                ).observe(get_duration)
                raise

        get_duration = time.time() - get_start
        print(f"✅ Collected {len(results)}/{len(jobs)} results in {get_duration:.2f}s")

        duration = time.time() - start_time
        task_counter.labels(
            task_name=task_name, status="success", worker=WORKER_NAME
        ).inc()
        task_duration.labels(task_name=task_name, worker=WORKER_NAME).observe(duration)

        success_rate = len(results) / len(jobs) * 100
        print(
            f"🎯 Task {task_id} completed: {len(results)}/{len(jobs)} subtasks ({success_rate:.0f}%)"
        )

        return {
            "task_id": task_id,
            "subtasks_spawned": len(jobs),
            "subtasks_completed": len(results),
            "success_rate": success_rate,
            "worker": WORKER_NAME,
            "duration": duration,
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
