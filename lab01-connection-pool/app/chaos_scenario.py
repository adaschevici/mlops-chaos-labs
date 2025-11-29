from collections import deque
from click import echo, command, argument, Choice
from tasks_redis_exhaustion import (
    task_with_extra_connections,
    task_with_broker_pool_contention,
)
from metrics import (
    get_container_id_via_hostname,
    get_task_states,
    get_redis_stats,
)
from celery import group
import time


def print_header(title):
    echo("\n" + "=" * 70)
    echo(f" {title}")
    echo("=" * 70 + "\n")


def scenario_1_connection_explosion(num_tasks=1800):
    """Enhanced with throughput tracking"""
    print_header("SCENARIO 1: Connection Explosion")
    hostname = get_container_id_via_hostname()

    echo("📊 Setup:")
    echo(f"  Total tasks: {num_tasks}")
    echo("  Workers: 3")
    echo("  Concurrency per worker: 150\n")

    echo(f"🚀 Submitting {num_tasks} tasks...\n")

    # Check initial Redis state
    initial_stats = get_redis_stats()
    if initial_stats:
        echo("📊 Initial Redis State:")
        echo(f"   maxclients: {initial_stats['maxclients']}")
        echo(f"   connected_clients: {initial_stats['connected_clients']}")
        echo(f"   Usage: {initial_stats['usage_percent']:.1f}%\n")

    job = group(
        task_with_extra_connections.s(task_id=i, operations=10)
        for i in range(num_tasks)
    )

    start = time.time()
    result = job.apply_async()

    echo("⏳ Monitoring progress...\n")

    # Tracking variables
    timeout = 60 * 10
    poll_start = time.time()
    last_ready = 0
    last_check_time = poll_start
    stall_count = 0

    # For throughput calculation
    throughput_window = deque(maxlen=10)  # Last 10 measurements
    checkpoint_interval = 5  # Check every 5 seconds
    last_checkpoint = poll_start
    checkpoint_completed = 0
    max_connections_seen = 0
    connection_samples = []

    try:
        while (time.time() - poll_start) < timeout:
            current_time = time.time()
            elapsed = current_time - start

            # Get states safely
            ready, successful, failed, pending, errors = get_task_states(result)

            # Calculate throughput metrics
            time_since_last = current_time - last_check_time
            tasks_completed_since_last = ready - last_ready

            # Instantaneous rate (tasks/sec over last interval)
            instant_rate = (
                tasks_completed_since_last / time_since_last
                if time_since_last > 0
                else 0
            )

            # Average rate (tasks/sec overall)
            avg_rate = ready / elapsed if elapsed > 0 else 0
            # Redis
            # Get Redis stats
            redis_stats = get_redis_stats()
            if redis_stats:
                connected = redis_stats["connected_clients"]
                maxclients = redis_stats["maxclients"]
                usage_pct = redis_stats["usage_percent"]
                rejected = redis_stats.get("rejected_connections", 0)

                max_connections_seen = max(max_connections_seen, connected)
                connection_samples.append(
                    {
                        "time": elapsed,
                        "connected": connected,
                        "usage_pct": usage_pct,
                        "tasks_ready": ready,
                        "rate": avg_rate,
                    }
                )
            else:
                connected = 0
                maxclients = 0
                usage_pct = 0
                rejected = 0

            # Connection status with visual indicator
            conn_str = f"{connected:4d}/{maxclients} ({usage_pct:5.1f}%)"

            # Track throughput every checkpoint interval
            if current_time - last_checkpoint >= checkpoint_interval:
                checkpoint_tasks = ready - checkpoint_completed
                checkpoint_rate = checkpoint_tasks / checkpoint_interval
                throughput_window.append(checkpoint_rate)

                checkpoint_completed = ready
                last_checkpoint = current_time

            # Rolling average throughput (over last N checkpoints)
            rolling_avg_rate = (
                sum(throughput_window) / len(throughput_window)
                if throughput_window
                else 0
            )

            # Estimated time remaining
            if rolling_avg_rate > 0 and pending > 0:
                eta_seconds = pending / rolling_avg_rate
                eta_minutes = eta_seconds / 60
                eta_str = (
                    f"{eta_minutes:.1f}m" if eta_minutes >= 1 else f"{eta_seconds:.0f}s"
                )
            else:
                eta_str = "unknown"

            # Progress percentage
            progress_pct = (ready / num_tasks * 100) if num_tasks > 0 else 0

            # Print progress with throughput
            echo(f"Container ID: {hostname}                         ")
            echo(f"Redis Connections: {conn_str} | Rejected: {rejected}     ")
            echo(
                f"[{elapsed:6.1f}s] "
                f"Progress: {ready:4d}/{num_tasks} ({progress_pct:5.1f}%) | "
                f"Success: {successful:4d} | "
                f"Failed: {failed:4d} | "
                f"Pending: {pending:4d}"
            )

            echo(
                f"           "
                f"Rate: {instant_rate:5.1f} t/s (instant) | "
                f"{rolling_avg_rate:5.1f} t/s (rolling) | "
                f"{avg_rate:5.1f} t/s (avg) | "
                f"ETA: {eta_str}"
            )

            # Detect throughput degradation
            if len(throughput_window) >= 5:
                recent_rate = sum(list(throughput_window)[-3:]) / 3
                older_rate = sum(list(throughput_window)[:3]) / 3

                if older_rate > 0 and recent_rate < older_rate * 0.5:
                    echo(
                        f"           ⚠️  Throughput dropped {older_rate:.1f} → {recent_rate:.1f} t/s (50% reduction!)"
                    )

            # Detect stall
            if ready == last_ready:
                stall_count += 1
                if stall_count >= 3:
                    echo(f"           ⚠️  No progress for {stall_count * 2}s")
                if stall_count >= 30:  # 60s stall
                    echo("\n⚠️  No progress for 60s - tasks appear stuck")
                    break
            else:
                if stall_count > 0:
                    echo(f"           ✓ Resumed after {stall_count * 2}s stall")
                stall_count = 0

            # Check for completion
            if ready >= num_tasks:
                echo("\n✓ All tasks processed")
                break

            last_ready = ready
            last_check_time = current_time
            time.sleep(2)

        # Check timeout
        if (time.time() - poll_start) >= timeout:
            echo(f"\n⏱️  Timeout reached after {timeout}s")

        # Final statistics
        duration = time.time() - start
        ready, successful, failed, pending, errors = get_task_states(result)

        echo("\n" + "=" * 70)
        echo(f"📈 FINAL RESULTS after {duration:.1f}s:")
        echo(f"   Total tasks: {num_tasks}")
        echo(f"   Completed: {ready} ({ready / num_tasks * 100:.1f}%)")
        echo(f"   ├─ Successful: {successful} ({successful / num_tasks * 100:.1f}%)")
        echo(f"   └─ Failed: {failed} ({failed / num_tasks * 100:.1f}%)")
        echo(f"   Pending/Lost: {pending} ({pending / num_tasks * 100:.1f}%)")
        echo(f"\n   Overall throughput: {ready / duration:.2f} tasks/sec")
        if ready > 0:
            echo(f"   Average duration: {duration / ready:.2f}s per task")
        elif ready == 0:
            echo("   Average duration: Inf(s) per task")

        final_redis = get_redis_stats()
        if final_redis:
            echo("\n🔌 Redis Connection Statistics:")
            echo(
                f"   Initial connections: {initial_stats['connected_clients'] if initial_stats else 'unknown'}"
            )
            echo(f"   Peak connections: {max_connections_seen}")
            echo(f"   Final connections: {final_redis['connected_clients']}")
            echo(f"   maxclients limit: {final_redis['maxclients']}")
            echo(
                f"   Peak usage: {max_connections_seen / final_redis['maxclients'] * 100:.1f}%"
            )

            if final_redis.get("rejected_connections", 0) > 0:
                echo(
                    f"   ❌ Rejected connections: {final_redis['rejected_connections']}"
                )
                echo("      This confirms connection pool exhaustion!")

            # Connection growth rate
            if initial_stats and duration > 0:
                growth = max_connections_seen - initial_stats["connected_clients"]
                growth_rate = growth / duration
                echo(
                    f"   Connection growth: +{growth} ({growth_rate:.1f} connections/sec)"
                )

        if successful > 0:
            echo(f"   Success rate: {successful / ready * 100:.1f}% (of completed)")

        echo("=" * 70)

        # Diagnosis
        if failed > num_tasks * 0.1:
            echo("\n✅ Connection pool exhaustion demonstrated!")
            echo(f"   {failed} tasks failed (~{failed / num_tasks * 100:.0f}%)")
        elif pending > num_tasks * 0.1:
            echo("\n⚠️  Many tasks stuck/pending")
            echo(f"   {pending} tasks never completed")
        else:
            echo("\n✓ Most tasks completed successfully")

    except KeyboardInterrupt:
        echo("\n⏸  Interrupted")
        duration = time.time() - start
        ready, successful, failed, pending, _ = get_task_states(result)
        echo(f"\nCompleted {ready}/{num_tasks} tasks in {duration:.1f}s")
        echo(f"Average rate: {ready / duration:.2f} tasks/sec")

    except Exception as e:
        echo(f"\n❌ Exception: {type(e).__name__}: {e}")
        import traceback

        traceback.print_exc()


def scenario_2_managed_pool_degradation(num_parent_tasks=100, subtasks_per_parent=50):
    """
    SCENARIO 2: Celery Broker Pool Exhaustion

    Demonstrates what happens when tasks spawn many subtasks, exhausting
    Celery's internal broker connection pool used for task publishing.

    Unlike Scenario 1 (app opens Redis connections), this exhausts the
    pool Celery uses to send/receive tasks from the broker.
    """
    print_header("SCENARIO 2: Broker Pool Exhaustion (Celery-Managed)")
    hostname = get_container_id_via_hostname()

    total_tasks = num_parent_tasks * subtasks_per_parent

    echo("📊 Setup:")
    echo(f"  Parent tasks: {num_parent_tasks}")
    echo(f"  Subtasks per parent: {subtasks_per_parent}")
    echo(f"  Total subtasks: {total_tasks}")
    echo("  Workers: 3")
    echo("  Broker pool limit: ~10 connections (Celery default)\n")

    echo(f"🚀 Submitting {num_parent_tasks} parent tasks...\n")
    echo("💡 Each parent spawns {subtasks_per_parent} subtasks simultaneously")
    echo("   This floods Celery's broker pool with publish requests\n")

    # Check initial state
    initial_redis = get_redis_stats()
    initial_queue_depth = get_queue_depth()

    if initial_redis:
        echo("📊 Initial State:")
        echo(f"   Redis connections: {initial_redis['connected_clients']}")
        echo(f"   Queue depth: {initial_queue_depth}")
        echo("")

    # Submit parent tasks
    job = group(
        task_with_broker_pool_contention.s(
            task_id=f"parent-{i}", subtasks=subtasks_per_parent
        )
        for i in range(num_parent_tasks)
    )

    start = time.time()
    result = job.apply_async()

    echo("⏳ Monitoring broker pool contention...\n")

    # Tracking
    timeout = 60 * 15  # 15 minutes
    poll_start = time.time()
    last_ready = 0
    last_check_time = poll_start
    stall_count = 0

    throughput_window = deque(maxlen=10)
    checkpoint_interval = 5
    last_checkpoint = poll_start
    checkpoint_completed = 0

    max_queue_depth = 0
    max_broker_pool_usage = 0
    broker_samples = []
    queue_samples = []

    try:
        while (time.time() - poll_start) < timeout:
            current_time = time.time()
            elapsed = current_time - start

            # Get task states
            ready, successful, failed, pending, errors = get_task_states(result)

            # Throughput calculation
            time_since_last = current_time - last_check_time
            tasks_completed_since_last = ready - last_ready
            instant_rate = (
                tasks_completed_since_last / time_since_last
                if time_since_last > 0
                else 0
            )
            avg_rate = ready / elapsed if elapsed > 0 else 0

            # Get broker pool metrics from Prometheus
            broker_stats = get_broker_pool_stats()  # New function
            queue_depth = get_queue_depth()

            if broker_stats:
                pool_size = broker_stats.get("pool_size", 10)
                pool_in_use = broker_stats.get("in_use", 0)
                pool_available = broker_stats.get("available", pool_size)
                pool_usage_pct = (pool_in_use / pool_size * 100) if pool_size > 0 else 0

                max_broker_pool_usage = max(max_broker_pool_usage, pool_in_use)
                broker_samples.append(
                    {
                        "time": elapsed,
                        "in_use": pool_in_use,
                        "available": pool_available,
                        "usage_pct": pool_usage_pct,
                    }
                )
            else:
                pool_size = 10
                pool_in_use = 0
                pool_available = pool_size
                pool_usage_pct = 0

            max_queue_depth = max(max_queue_depth, queue_depth)
            queue_samples.append(
                {
                    "time": elapsed,
                    "depth": queue_depth,
                    "tasks_ready": ready,
                }
            )

            # Broker pool status
            broker_str = f"{pool_in_use:2d}/{pool_size} ({pool_usage_pct:5.1f}%)"

            # Track throughput
            if current_time - last_checkpoint >= checkpoint_interval:
                checkpoint_tasks = ready - checkpoint_completed
                checkpoint_rate = checkpoint_tasks / checkpoint_interval
                throughput_window.append(checkpoint_rate)
                checkpoint_completed = ready
                last_checkpoint = current_time

            rolling_avg_rate = (
                sum(throughput_window) / len(throughput_window)
                if throughput_window
                else 0
            )

            # ETA
            if rolling_avg_rate > 0 and pending > 0:
                eta_seconds = pending / rolling_avg_rate
                eta_minutes = eta_seconds / 60
                eta_str = (
                    f"{eta_minutes:.1f}m" if eta_minutes >= 1 else f"{eta_seconds:.0f}s"
                )
            else:
                eta_str = "unknown"

            progress_pct = (
                (ready / num_parent_tasks * 100) if num_parent_tasks > 0 else 0
            )

            # Print progress
            echo(f"Container ID: {hostname}                         ")
            echo(f"Broker Pool: {broker_str} | Queue Depth: {queue_depth:5d}     ")
            echo(
                f"[{elapsed:6.1f}s] "
                f"Parents: {ready:4d}/{num_parent_tasks} ({progress_pct:5.1f}%) | "
                f"Success: {successful:4d} | "
                f"Failed: {failed:4d} | "
                f"Pending: {pending:4d}"
            )
            echo(
                f"           "
                f"Rate: {instant_rate:5.1f} t/s (instant) | "
                f"{rolling_avg_rate:5.1f} t/s (rolling) | "
                f"{avg_rate:5.1f} t/s (avg) | "
                f"ETA: {eta_str}"
            )

            # Detect broker pool saturation
            if pool_usage_pct >= 90:
                echo(
                    f"           🔴 BROKER POOL SATURATED: {pool_in_use}/{pool_size} connections in use!"
                )
            elif pool_usage_pct >= 70:
                echo(
                    f"           🟡 Broker pool under pressure: {pool_usage_pct:.0f}% utilized"
                )

            # Detect queue backup
            if queue_depth > 500:
                echo(f"           🔴 QUEUE BACKUP: {queue_depth} tasks waiting!")
            elif queue_depth > 200:
                echo(f"           🟡 Queue growing: {queue_depth} tasks waiting")

            # Detect throughput collapse
            if len(throughput_window) >= 5:
                recent_rate = sum(list(throughput_window)[-3:]) / 3
                older_rate = sum(list(throughput_window)[:3]) / 3

                if older_rate > 0 and recent_rate < older_rate * 0.3:
                    echo(
                        f"           ⚠️  THROUGHPUT COLLAPSED: {older_rate:.1f} → {recent_rate:.1f} t/s (70% drop!)"
                    )
                    echo(
                        "              Likely cause: Broker pool exhaustion blocking task publishing"
                    )

            # Detect stall
            if ready == last_ready:
                stall_count += 1
                if stall_count >= 3:
                    echo(f"           ⚠️  No progress for {stall_count * 2}s")
                if stall_count >= 30:
                    echo("\n⚠️  No progress for 60s - broker pool likely exhausted")
                    break
            else:
                if stall_count > 0:
                    echo(f"           ✓ Resumed after {stall_count * 2}s stall")
                stall_count = 0

            # Check completion
            if ready >= num_parent_tasks:
                echo("\n✓ All parent tasks completed")
                break

            last_ready = ready
            last_check_time = current_time
            time.sleep(2)

        # Timeout check
        if (time.time() - poll_start) >= timeout:
            echo(f"\n⏱️  Timeout reached after {timeout}s")

        # Final statistics
        duration = time.time() - start
        ready, successful, failed, pending, errors = get_task_states(result)

        echo("\n" + "=" * 70)
        echo(f"📈 FINAL RESULTS after {duration:.1f}s:")
        echo(f"   Parent tasks submitted: {num_parent_tasks}")
        echo(f"   Total subtasks spawned: ~{num_parent_tasks * subtasks_per_parent}")
        echo(f"   Parents completed: {ready} ({ready / num_parent_tasks * 100:.1f}%)")
        echo(
            f"   ├─ Successful: {successful} ({successful / num_parent_tasks * 100:.1f}%)"
        )
        echo(f"   └─ Failed: {failed} ({failed / num_parent_tasks * 100:.1f}%)")
        echo(f"   Pending/Lost: {pending} ({pending / num_parent_tasks * 100:.1f}%)")
        echo(f"\n   Overall throughput: {ready / duration:.2f} parent tasks/sec")

        # Broker pool statistics
        final_broker = get_broker_pool_stats()
        if final_broker or broker_samples:
            echo("\n🔌 Broker Pool Statistics:")
            echo(f"   Pool size (limit): {pool_size} connections")
            echo(f"   Peak usage: {max_broker_pool_usage}/{pool_size} connections")
            echo(f"   Peak utilization: {max_broker_pool_usage / pool_size * 100:.1f}%")

            if max_broker_pool_usage >= pool_size * 0.9:
                echo("   ❌ Pool reached 90%+ utilization - EXHAUSTION CONFIRMED!")
                echo("      This blocked new task publishing")

        # Queue statistics
        final_queue = get_queue_depth()
        echo("\n📬 Queue Statistics:")
        echo(f"   Initial queue depth: {initial_queue_depth}")
        echo(f"   Peak queue depth: {max_queue_depth}")
        echo(f"   Final queue depth: {final_queue}")

        if max_queue_depth > 1000:
            echo(f"   ❌ Queue backed up to {max_queue_depth} tasks!")
            echo("      Broker pool couldn't keep up with publishing rate")

        # Redis connection stats (for comparison)
        final_redis = get_redis_stats()
        if initial_redis and final_redis:
            echo("\n🔗 Redis Connection Comparison:")
            echo(f"   Initial: {initial_redis['connected_clients']} connections")
            echo(f"   Final: {final_redis['connected_clients']} connections")
            echo(
                f"   Change: +{final_redis['connected_clients'] - initial_redis['connected_clients']}"
            )
            echo("   (Note: Smaller increase than Scenario 1 - using managed pool)")

        echo("=" * 70)

        # Diagnosis
        if max_broker_pool_usage >= pool_size * 0.9:
            echo("\n✅ BROKER POOL EXHAUSTION DEMONSTRATED!")
            echo(
                f"   Pool saturated at {max_broker_pool_usage}/{pool_size} connections"
            )
            echo(f"   Queue backed up to {max_queue_depth} tasks")
            if ready < num_parent_tasks * 0.5:
                echo(
                    f"   Only {ready / num_parent_tasks * 100:.0f}% of tasks completed"
                )
        elif max_queue_depth > 500:
            echo("\n⚠️  Significant queue backup detected")
            echo(f"   Peak queue: {max_queue_depth} tasks")
        else:
            echo("\n✓ System handled load (no broker pool exhaustion)")

        # Key differences from Scenario 1
        echo("\n🔍 Key Difference from Scenario 1:")
        echo("   Scenario 1: Tasks create their own Redis connections")
        echo("              → Exhausts Redis maxclients limit")
        echo("   Scenario 2: Tasks publish subtasks via Celery's pool")
        echo("              → Exhausts Celery's broker_pool_limit")
        echo("\n   Symptom in Scenario 2:")
        echo("   - Queue depth grows (tasks waiting to be published)")
        echo("   - Throughput drops (can't publish new tasks fast enough)")
        echo("   - Broker pool shows 100% utilization")

    except KeyboardInterrupt:
        echo("\n⏸  Interrupted")
        duration = time.time() - start
        ready, successful, failed, pending, _ = get_task_states(result)
        echo(f"\nCompleted {ready}/{num_parent_tasks} parent tasks in {duration:.1f}s")

    except Exception as e:
        echo(f"\n❌ Exception: {type(e).__name__}: {e}")
        import traceback

        traceback.print_exc()


def get_broker_pool_stats():
    """
    Query Prometheus for broker pool metrics.
    Returns dict with pool_size, in_use, available.
    """
    try:
        import requests

        # Query Prometheus for broker pool metrics
        queries = {
            "pool_size": 'redis_pool_size{pool_type="broker"}',
            "in_use": 'redis_pool_in_use{pool_type="broker"}',
            "available": 'redis_pool_available{pool_type="broker"}',
        }

        results = {}
        for key, query in queries.items():
            resp = requests.get(
                "http://prometheus:9090/api/v1/query",
                params={"query": query},
                timeout=2,
            )
            if resp.status_code == 200:
                data = resp.json()
                if data["data"]["result"]:
                    value = float(data["data"]["result"][0]["value"][1])
                    results[key] = int(value)

        return results if results else None

    except Exception as _e:
        # Silently fail if Prometheus unavailable
        return None


def get_queue_depth(queue_name="celery"):
    """
    Get current queue depth from Redis.
    """
    try:
        import redis

        r = redis.from_url("redis://redis:6379/0")
        depth = r.llen(queue_name)
        r.close()
        return depth
    except Exception as _e:
        return 0


# def scenario_3_result_backend_pressure(num_tasks=180):
#     """
#     Tasks store 5MB results each
#     Result backend pool = 1 connection
#     = Massive bottleneck
#     """
#     print_header("SCENARIO 2: Result Backend Exhaustion")
#
#     echo("📊 Each task stores 5MB result")
#     echo("  Result backend pool: 1 connection")
#     echo("  All tasks compete for 1 connection!\n")
#
#     echo(f"🚀 Submitting {num_tasks} tasks...\n")
#
#     job = group(
#         task_with_result_backend_pressure.s(task_id=i, result_size_mb=5)
#         for i in range(num_tasks)
#     )
#
#     start = time.time()
#     result = job.apply_async()
#
#     echo("⏳ Watching result backend saturation...\n")
#
#     try:
#         poll_start = time.time()
#         while not result.ready() and (time.time() - poll_start) < 120:
#             ready_count = sum(1 for r in result.results if r.ready())
#             echo(f"  Progress: {ready_count}/{num_tasks}\n")
#             time.sleep(2)
#
#         duration = time.time() - start
#         successful = sum(1 for r in result.results if r.successful())
#
#         echo(f"\n\n✓ Completed: {successful}/{num_tasks} in {duration:.1f}s")
#         echo(f"  Average: {duration / num_tasks:.1f}s per task")
#
#         if duration > 150:
#             echo("\n⚠️  Very slow - result backend bottleneck likely!")
#
#     except Exception as e:
#         echo(f"\n❌ Failed: {e}")


# Map scenario numbers to the actual functions
SCENARIOS = {
    "1": scenario_1_connection_explosion,
    "2": scenario_2_managed_pool_degradation,
}


@command(help="Run test scenarios for Celery with Redis backend")
@argument("scenario", default="1", type=Choice(list(SCENARIOS.keys())), required=False)
def cli(scenario):
    """
    Runs one of four defined test scenarios.

    SCENARIO must be '1', '2',  or '3'. Defaults to '1'.

    1: Connection explosion (tasks open many connections)
    2: Connection explosion on the managed pool
    """
    echo(f"Selected scenario: {scenario}\n")

    SCENARIOS[scenario]()


if __name__ == "__main__":
    cli()
