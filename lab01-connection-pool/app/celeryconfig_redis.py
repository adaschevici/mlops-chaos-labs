broker_url = "redis://redis:6379/0"
result_backend = "redis://redis:6379/0"


# THE REAL PROBLEM: Pool too small + high concurrency
broker_pool_limit = 20  # ← Even smaller!
result_backend_transport_options = {
    "max_connections": 20  # ← Only 1 connection for results!
}

# Make it worse
worker_prefetch_multiplier = 50  # Workers grab LOTS of tasks
task_acks_late = True  # Don't ack until complete
worker_max_tasks_per_child = None  # Never restart

# This will cause the breakdown
result_expires = None  # Results never expire (memory pressure)
task_track_started = True  # Extra Redis writes
