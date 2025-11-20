# app/tasks.py
from celery import Celery
import time
import random

app = Celery('chaos_lab')
app.config_from_object('celeryconfig')

# # Each task opens its OWN Redis connection (outside Celery's pool)
# def get_redis():
#     """Tasks open their own connections - EXHAUSTS POOL"""
#     return redis.from_url('redis://redis:6379/0')

@app.task(bind=True)
def simulate_work(self, task_id, duration=2):
    """Simulates a typical ML task with Redis interaction"""
    print(f"Task {task_id} starting (duration: {duration}s)")
    
    # Simulate multiple Redis operations during task
    for i in range(5):
        time.sleep(duration / 5)
        # Each iteration might need a connection
        result = self.app.backend.get(f"dummy_key_{task_id}_{i}")
    
    print(f"Task {task_id} completed")
    return {"task_id": task_id, "status": "completed"}

@app.task(bind=True)
def ml_inference(self, data_size=100):
    """Simulates ML inference with result storage"""
    print(f"ML inference starting (data_size: {data_size})")
    
    # Simulate model prediction
    predictions = [random.random() for _ in range(data_size)]
    
    # Store results (uses connection)
    time.sleep(1)
    
    return {"predictions": predictions[:10], "count": len(predictions)}
