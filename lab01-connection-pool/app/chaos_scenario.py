from click import echo, command, argument, Choice
from tasks import (
    task_with_extra_connections,
    task_with_result_backend_pressure,
    task_with_streaming_results,
    blocking_task
)
from celery import group
import time
import sys
from datetime import datetime

def print_header(title):
    echo("\n" + "=" * 70)
    echo(f" {title}")
    echo("=" * 70 + "\n")

def scenario_1_connection_explosion(num_tasks=1800):
    """
    Each task opens 10 connections
    With 3 workers × 8 concurrency = 24 tasks
    24 tasks × 10 connections = 240 connections needed
    But pool only has 1 connection!
    """
    print_header("SCENARIO 1: Connection Explosion")
    
    echo("📊 Setup:")
    echo("  Workers: 3")
    echo("  Concurrency per worker: 8")
    echo("  Total concurrent tasks: 24")
    echo("  Connections per task: 10")
    echo("  Total connections needed: 240")
    echo("  Pool size: 1 ← THIS WILL BREAK!\n")
    
    echo(f"🚀 Submitting {num_tasks} tasks...\n")
    
    job = group(
        task_with_extra_connections.s(task_id=i, operations=10)
        for i in range(num_tasks)
    )
    
    start = time.time()
    result = job.apply_async()
    
    echo("⏳ Watch it fail in real-time...")
    echo("\n🔍 Monitor with:")
    echo("  watch -n 1 'docker exec lab01-redis redis-cli INFO clients'")
    echo("  docker-compose logs -f celery-worker-1\n")
    
    # Monitor
    completed = 0
    errors = 0
    timeout = 120
    
    try:
        poll_start = time.time()
        while not result.ready() and (time.time() - poll_start) < timeout:
            ready_count = sum(1 for r in result.results if r.ready())
            failed_count = sum(1 for r in result.results if r.failed())
            
            if ready_count != completed or failed_count != errors:
                completed = ready_count
                errors = failed_count
                elapsed = time.time() - start
                echo(f"  [{elapsed:5.1f}s] Completed: {completed:3d} | Failed: {errors:3d} | Pending: {num_tasks-completed-errors:3d}")
            
            time.sleep(2)
        
        duration = time.time() - start
        successful = sum(1 for r in result.results if r.successful())
        failed = sum(1 for r in result.results if r.failed())
        
        echo(f"\n" + "=" * 70)
        echo(f"📈 RESULTS after {duration:.1f}s:")
        echo(f"   Successful: {successful}/{num_tasks}")
        echo(f"   Failed: {failed}/{num_tasks}")
        echo(f"   Success rate: {successful/num_tasks*100:.1f}%")
        echo("=" * 70)
        
        if failed > 0:
            echo("\n✅ Connection pool exhaustion demonstrated!")
            echo("   Run diagnostics to confirm root cause")
        else:
            echo("\n⚠️  All tasks succeeded - pool might be larger than expected")
            
    except Exception as e:
        echo(f"\n❌ Exception: {e}")

def scenario_2_result_backend_pressure(num_tasks=180):
    """
    Tasks store 5MB results each
    Result backend pool = 1 connection
    = Massive bottleneck
    """
    print_header("SCENARIO 2: Result Backend Exhaustion")
    
    echo("📊 Each task stores 5MB result")
    echo("  Result backend pool: 1 connection")
    echo("  All tasks compete for 1 connection!\n")
    
    echo(f"🚀 Submitting {num_tasks} tasks...\n")
    
    job = group(
        task_with_result_backend_pressure.s(task_id=i, result_size_mb=5)
        for i in range(num_tasks)
    )
    
    start = time.time()
    result = job.apply_async()
    
    echo("⏳ Watching result backend saturation...\n")
    
    try:
        poll_start = time.time()
        while not result.ready() and (time.time() - poll_start) < 120:
            ready_count = sum(1 for r in result.results if r.ready())
            echo(f"  Progress: {ready_count}/{num_tasks}\n")
            time.sleep(2)
        
        duration = time.time() - start
        successful = sum(1 for r in result.results if r.successful())
        
        echo(f"\n\n✓ Completed: {successful}/{num_tasks} in {duration:.1f}s")
        echo(f"  Average: {duration/num_tasks:.1f}s per task")
        
        if duration > 150:
            echo("\n⚠️  Very slow - result backend bottleneck likely!")
            
    except Exception as e:
        echo(f"\n❌ Failed: {e}")

def scenario_3_streaming_updates():
    """
    Tasks that update state 20 times
    Each update = result backend write
    = Connection churn
    """
    print_header("SCENARIO 3: Streaming Updates Overload")
    
    echo("📊 Each task sends 20 progress updates")
    echo("  20 updates × 50 tasks = 1000 result backend writes")
    echo("  Pool size: 1 connection\n")
    
    echo("🚀 Submitting 50 tasks...\n")
    
    job = group(
        task_with_streaming_results.s(task_id=i, updates=20)
        for i in range(50)
    )
    
    start = time.time()
    result = job.apply_async()
    
    try:
        result.get(timeout=120)
        duration = time.time() - start
        echo(f"\n✓ Completed in {duration:.1f}s")
    except Exception as e:
        duration = time.time() - start
        echo(f"\n❌ Failed after {duration:.1f}s: {e}")

def scenario_4_mixed_load():
    """
    Combination: blocking tasks + connection-heavy tasks
    = Complete chaos
    """
    print_header("SCENARIO 4: Mixed Chaos Load")
    
    echo("📊 Mix of:")
    echo("  - 10 blocking tasks (10s each, hold connections)")
    echo("  - 40 connection-heavy tasks (10 connections each)")
    echo("  - 30 result backend pressure tasks")
    echo("  Total: 80 tasks competing for pool of 1!\n")
    
    echo("🚀 Submitting mixed workload...\n")
    
    blocking = group(
        blocking_task.s(task_id=f"block-{i}", duration=10)
        for i in range(10)
    )
    
    connection_heavy = group(
        task_with_extra_connections.s(task_id=f"conn-{i}", operations=10)
        for i in range(40)
    )
    
    result_heavy = group(
        task_with_result_backend_pressure.s(task_id=f"result-{i}", result_size_mb=3)
        for i in range(30)
    )
    
    start = time.time()
    
    # Submit all at once
    r1 = blocking.apply_async()
    r2 = connection_heavy.apply_async()
    r3 = result_heavy.apply_async()
    
    echo("⏳ Maximum chaos in progress...\n")
    
    try:
        time.sleep(60)  # Let it run for a minute
        
        b_done = sum(1 for r in r1.results if r.ready())
        c_done = sum(1 for r in r2.results if r.ready())
        r_done = sum(1 for r in r3.results if r.ready())
        
        echo(f"\n📊 After 60s:")
        echo(f"  Blocking tasks: {b_done}/10")
        echo(f"  Connection tasks: {c_done}/40")
        echo(f"  Result tasks: {r_done}/30")
        echo(f"  Total: {b_done+c_done+r_done}/80")
        
    except Exception as e:
        echo(f"\n❌ {e}")

# Map scenario numbers to the actual functions
SCENARIOS = {
    '1': scenario_1_connection_explosion,
    '2': scenario_2_result_backend_pressure,
    '3': scenario_3_streaming_updates,
    '4': scenario_4_mixed_load
}
@command(help="Run test scenarios for Celery with Redis backend")
@argument('scenario', default='1', type=Choice(list(SCENARIOS.keys())), required=False)
def cli(scenario):
    """
    Runs one of four defined test scenarios.

    SCENARIO must be '1', '2', '3', or '4'. Defaults to '1'.

    1: Connection explosion (tasks open many connections)
    2: Result backend pressure (large results)
    3: Streaming updates (many small writes)
    4: Mixed chaos (combination of all)
    """
    echo(f"Selected scenario: {scenario}\n")

    SCENARIOS[scenario]()

if __name__ == '__main__':
    cli()
