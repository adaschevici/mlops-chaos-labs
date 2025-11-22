from celery import Celery
import time
import random
import redis
import json

app = Celery('chaos_lab')
app.config_from_object('celeryconfig')

# Each task opens its OWN Redis connection (outside Celery's pool)
def get_redis():
    """Tasks open their own connections - EXHAUSTS POOL"""
    return redis.from_url('redis://redis:6379/0')

@app.task(bind=True)
def task_with_extra_connections(self, task_id, operations=10):
    """
    This task opens additional Redis connections
    beyond what Celery manages - THIS is what breaks it
    """
    print(f"Task {task_id} starting - will open {operations} connections")
    
    connections = []
    
    try:
        # Open multiple connections (simulates: caching, session storage, etc.)
        for i in range(operations):
            r = get_redis()  # ← Each opens a NEW connection!
            connections.append(r)

            # Do work that holds the connection
            r.set(f"task:{task_id}:step:{i}", json.dumps({
                'status': 'processing',
                'timestamp': time.time()
            }))
            time.sleep(0.5)

        time.sleep(random.uniform(1, 50))  # Simulate work while holding connection
        print(f"Task {task_id} completed {operations} operations")
        return {"task_id": task_id, "operations": operations}

    except redis.exceptions.ConnectionError as e:
        print(f"Task {task_id} FAILED: Connection error - {e}")
        raise
    except Exception as e:
        print(f"Task {task_id} FAILED: {e}")
        raise
    finally:
        # Clean up connections (but damage is done)
        for r in connections:
            try:
                r.close()
            except:
                pass

@app.task(bind=True)
def task_with_result_backend_pressure(self, task_id, result_size_mb=5):
    """
    Store large results - exhausts result backend connections
    """
    print(f"Task {task_id} generating {result_size_mb}MB result")
    
    # Generate large result
    large_data = {
        'task_id': task_id,
        'predictions': [random.random() for _ in range(result_size_mb * 100000)],
        'metadata': {
            'model': 'test-model',
            'version': '1.0',
            'timestamp': time.time()
        }
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
            state='PROGRESS',
            meta={
                'current': i,
                'total': updates,
                'status': f'Processing step {i+1}/{updates}'
            }
        )
        time.sleep(0.2)  # Work while connection might be held
    
    return {'task_id': task_id, 'updates': updates}

@app.task(bind=True)
def blocking_task(self, task_id, duration=10):
    """
    Long-running task that holds connections
    """
    print(f"Task {task_id} blocking for {duration}s")
    
    # Hold connection for entire duration
    r = get_redis()
    
    try:
        # Set a key and hold the connection
        r.set(f"blocking:{task_id}", "locked")
        time.sleep(duration)
        r.delete(f"blocking:{task_id}")
        
        return {'task_id': task_id, 'duration': duration}
    finally:
        r.close()
