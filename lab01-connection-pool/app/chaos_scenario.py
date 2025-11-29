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
        echo(f"   Average duration: {duration / ready:.2f}s per task")

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


def scenario_2_managed_pool_degradation(num_tasks=100):
    print_header("SCENARIO 2: Result Backend Exhaustion")

    echo(
        "📊 Each task blocks the celery managed pool for a random time lapse between 1 and 50 seconds"
    )
    echo("  Result backend pool: 2 connection")
    echo("  All tasks compete for 2 connection!\n")


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
