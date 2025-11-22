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
import socket

def get_container_id_via_hostname():
    """Retrieves the container ID, which is set as the hostname."""
    try:
        # Get the hostname, which is the full container ID inside Docker
        hostname = socket.gethostname()
        return hostname
    except Exception as e:
        return f"Error reading hostname: {e}"

def print_header(title):
    echo("\n" + "=" * 70)
    echo(f" {title}")
    echo("=" * 70 + "\n")

def get_task_states(result):
    """
    Safely get task states with error handling
    Returns: (ready, successful, failed, pending, error_msgs)
    """
    ready = 0
    successful = 0
    failed = 0
    error_msgs = []
    
    total = len(result.results)
    
    for idx, r in enumerate(result.results):
        try:
            if r.ready():
                ready += 1
                try:
                    if r.successful():
                        successful += 1
                    elif r.failed():
                        failed += 1
                except Exception as e:
                    # Task in weird state
                    error_msgs.append(f"Task {idx}: {type(e).__name__}")
        except Exception as e:
            # Can't even check if ready
            error_msgs.append(f"Task {idx} check failed: {e}")
    
    pending = total - ready
    
    return ready, successful, failed, pending, error_msgs

def scenario_1_connection_explosion(num_tasks=1800):
    """Version with robust error handling"""
    print_header("SCENARIO 1: Connection Explosion")
    
    echo(f"🚀 Submitting {num_tasks} tasks...\n")
    
    job = group(
        task_with_extra_connections.s(task_id=i, operations=10)
        for i in range(num_tasks)
    )
    
    start = time.time()
    result = job.apply_async()
    
    echo("⏳ Monitoring...\n")
    
    timeout = 60 * 10
    poll_start = time.time()
    last_ready = 0
    stall_count = 0
    container_id =  get_container_id_via_hostname()
    
    try:
        while (time.time() - poll_start) < timeout:
            elapsed = time.time() - start
            
            # Get states safely
            ready, successful, failed, pending, errors = get_task_states(result)
            
            # Print progress
            echo(f"[container-id: {container_id}] [{elapsed:6.1f}s] Ready: {ready:4d} | "
                 f"Success: {successful:4d} | "
                 f"Failed: {failed:4d} | "
                 f"Pending: {pending:4d}")
            
            if errors:
                echo(f"  ⚠️  Errors checking tasks: {len(errors)}")
            
            # Check for completion
            if ready >= num_tasks:
                echo("\n✓ All tasks processed")
                break
            
            # Check for stall
            if ready == last_ready:
                stall_count += 1
                if stall_count >= 30:  # No progress for 60s (30 × 2s)
                    echo("\n⚠️  No progress for 60s - tasks appear stuck")
                    echo(f"  Last state: {ready}/{num_tasks} ready")
                    break
            else:
                stall_count = 0
            
            last_ready = ready
            time.sleep(2)
        
        # Final report
        duration = time.time() - start
        ready, successful, failed, pending, errors = get_task_states(result)
        
        echo(f"\n" + "=" * 70)
        echo(f"📈 FINAL RESULTS after {duration:.1f}s:")
        echo(f"   Total: {num_tasks}")
        echo(f"   Successful: {successful} ({successful/num_tasks*100:.1f}%)")
        echo(f"   Failed: {failed} ({failed/num_tasks*100:.1f}%)")
        echo(f"   Pending: {pending} ({pending/num_tasks*100:.1f}%)")
        
        if errors:
            echo(f"   Check errors: {len(errors)}")
        
        echo("=" * 70)
        
        # Diagnosis
        if failed > num_tasks * 0.1:
            echo("\n✅ Connection exhaustion demonstrated!")
            echo(f"   {failed} tasks failed (~{failed/num_tasks*100:.0f}%)")
        elif pending > num_tasks * 0.1:
            echo("\n⚠️  Many tasks stuck/pending")
            echo(f"   {pending} tasks never completed")
            echo("   Possible: deadlock, worker crash, task timeout")
        else:
            echo("\n✓ Most tasks completed successfully")
            
    except KeyboardInterrupt:
        echo("\n⏸  Interrupted")
    except Exception as e:
        echo(f"\n❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()

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
