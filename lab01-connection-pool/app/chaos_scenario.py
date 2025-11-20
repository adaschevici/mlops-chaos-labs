from click import echo, command, argument, Choice
from tasks import simulate_work, ml_inference
from celery import group
import time
import sys
from datetime import datetime

def print_header(title):
    echo("\n" + "=" * 70)
    echo(f" {title}")
    echo("=" * 70 + "\n")

def scenario_1_sudden_spike(num_tasks=250):
    """50 tasks at once - will exhaust connection pool"""
    print_header(f"SCENARIO 1: Sudden Spike ({num_tasks} concurrent tasks)")
    
    echo("📊 Expected behavior with broken config:")
    echo("  ❌ Connection timeouts")
    echo("  ❌ Tasks stuck in pending")
    echo("  ❌ Sporadic failures\n")
    
    echo("📊 Expected behavior with fixed config:")
    echo("  ✅ All tasks complete")
    echo("  ✅ No connection errors")
    echo("  ✅ Even load distribution\n")
    
    echo(f"⏰ Starting at: {datetime.now().strftime('%H:%M:%S')}")
    echo(f"🚀 Submitting {num_tasks} tasks...\n")
    
    # Create and submit task group
    job = group(
        simulate_work.s(task_id=i, duration=2) 
        for i in range(num_tasks)
    )
    
    start = time.time()
    result = job.apply_async()
    
    echo(f"✓ Tasks submitted: {len(result.results)}")
    echo("\n" + "-" * 70)
    echo("🔍 DIAGNOSTIC COMMANDS (run in another terminal):")
    echo("-" * 70)
    echo("  docker exec lab01-redis redis-cli INFO clients")
    echo("  docker exec lab01-redis redis-cli CLIENT LIST | wc -l")
    echo("  docker exec lab01-celery-worker celery -A tasks inspect active")
    echo("  docker exec lab01-celery-worker python diagnose.py")
    echo("-" * 70 + "\n")
    
    echo("⏳ Waiting for completion (timeout: 60s)...\n")
    
    # Monitor progress
    completed = 0
    timeout = 60
    poll_start = time.time()
    
    try:
        while not result.ready() and (time.time() - poll_start) < timeout:
            try:
                # Try to get partial results
                ready_count = sum(1 for r in result.results if r.ready())
                if ready_count != completed:
                    completed = ready_count
                    elapsed = time.time() - start
                    echo(f"  Progress: {completed}/{num_tasks} tasks ({completed/num_tasks*100:.0f}%) - {elapsed:.1f}s elapsed")
            except:
                pass
            time.sleep(2)
        
        # Final result
        if result.ready():
            result.get(timeout=5)
            duration = time.time() - start
            echo(f"\n✅ SUCCESS: All tasks completed in {duration:.1f}s")
            echo(f"   Average: {duration/num_tasks:.2f}s per task")
        else:
            duration = time.time() - start
            echo(f"\n⏱️  TIMEOUT: Not all tasks completed in {duration:.1f}s")
            completed = sum(1 for r in result.results if r.ready())
            echo(f"   Completed: {completed}/50 tasks ({completed/num_tasks*100:.0f}%)")
            
    except Exception as e:
        duration = time.time() - start
        echo(f"\n❌ FAILED after {duration:.1f}s")
        echo(f"   Error: {type(e).__name__}: {e}")
        echo("\n💡 TIP: This is expected with broken config!")
        echo("   Run 'docker exec lab01-celery-worker python diagnose.py'")
        echo("   Then: 'make fix' to apply the solution")

def scenario_2_sustained_load():
    """Continuous stream - gradual degradation"""
    print_header("SCENARIO 2: Sustained Load")
    
    echo("📊 Submitting tasks continuously for 30 seconds...")
    echo("🎯 Goal: Observe gradual degradation with broken config\n")
    
    start = time.time()
    submitted = 0
    errors = 0
    
    while time.time() - start < 30:
        try:
            simulate_work.apply_async(
                args=(submitted,),
                kwargs={'duration': 3}
            )
            submitted += 1
            
            if submitted % 5 == 0:
                elapsed = time.time() - start
                rate = submitted / elapsed
                echo(f"  [{elapsed:5.1f}s] Submitted: {submitted:3d} | Rate: {rate:.1f}/s | Errors: {errors}")
                
        except Exception as e:
            errors += 1
            if errors <= 3:  # Only print first few errors
                echo(f"\n  ❌ Error on task {submitted}: {type(e).__name__}")
        
        time.sleep(0.5)
    
    duration = time.time() - start
    success_rate = (submitted - errors) / submitted * 100 if submitted > 0 else 0
    
    echo(f"\n" + "=" * 70)
    echo(f"📈 RESULTS:")
    echo(f"   Duration: {duration:.1f}s")
    echo(f"   Submitted: {submitted} tasks")
    echo(f"   Errors: {errors} tasks")
    echo(f"   Success rate: {success_rate:.1f}%")
    echo(f"   Average rate: {submitted/duration:.1f} tasks/sec")
    echo("=" * 70)
    
    if errors > submitted * 0.1:
        echo("\n⚠️  High error rate detected!")
        echo("   Run diagnostics: docker exec lab01-celery-worker python diagnose.py")

@command(help="Run test scenarios for Celery with Redis backend")
@argument('scenario', default='1', type=Choice(['1', '2']), required=False)
def cli(scenario):
    """
    Runs one of two defined test scenarios.
    
    SCENARIO must be '1' or '2'. Defaults to '1' if not provided.
    
    1: Sudden spike (50 concurrent tasks)
    2: Sustained load (continuous for 30s)
    """
    echo(f"Selected scenario: {scenario}\n")

    if scenario == '1':
        scenario_1_sudden_spike()
    elif scenario == '2':
        scenario_2_sustained_load()

if __name__ == '__main__':
    cli()
