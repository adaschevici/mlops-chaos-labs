import redis


# Each task opens its OWN Redis connection (outside Celery's pool)
def get_redis():
    """Tasks open their own connections - EXHAUSTS POOL"""
    return redis.from_url("redis://redis:6379/0")
