from prometheus_client import Gauge, Counter, Histogram

tasks_in_progress = Gauge(
    "celery_tasks_in_progress",
    "Number of tasks currently executing",
    ["task_name", "worker"],
    multiprocess_mode="livesum",
)

# Prometheus metrics
task_counter = Counter(
    "celery_task", "Total number of tasks", ["task_name", "status", "worker"]
)

task_duration = Histogram(
    "celery_task_duration_seconds",
    "Task execution duration",
    ["task_name", "worker"],
    buckets=[0.1, 0.5, 1, 2, 5, 10, 30, 60, 120, 300],
)
