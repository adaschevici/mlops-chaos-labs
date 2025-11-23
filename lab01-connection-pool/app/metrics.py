import socket
import subprocess
import redis


def get_container_id_via_hostname():
    """Retrieves the container ID, which is set as the hostname."""
    try:
        # Get the hostname, which is the full container ID inside Docker
        hostname = socket.gethostname()
        return hostname
    except Exception as e:
        return f"Error reading hostname: {e}"


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


def get_redis_stats():
    """
    Get Redis connection statistics
    Returns dict with connection info or None on error
    """
    try:
        # Method 1: Using redis-py (if you have direct access)
        r = redis.from_url("redis://redis:6379/0")

        # Get client info
        clients_info = r.info("clients")

        # Get maxclients config
        config = r.config_get("maxclients")
        maxclients = int(config.get("maxclients", 10000))

        # Get stats
        stats_info = r.info("stats")

        return {
            "connected_clients": clients_info["connected_clients"],
            "blocked_clients": clients_info["blocked_clients"],
            "maxclients": maxclients,
            "total_connections_received": stats_info["total_connections_received"],
            "rejected_connections": stats_info["rejected_connections"],
            "usage_percent": (clients_info["connected_clients"] / maxclients * 100)
            if maxclients > 0
            else 0,
        }
    except Exception as _e:
        # Method 2: Using docker exec (fallback)
        try:
            # Get client info
            clients_output = subprocess.check_output(
                ["docker", "exec", "lab01-redis", "redis-cli", "INFO", "clients"],
                stderr=subprocess.DEVNULL,
            ).decode()

            # Get maxclients
            maxclients_output = (
                subprocess.check_output(
                    [
                        "docker",
                        "exec",
                        "lab01-redis",
                        "redis-cli",
                        "CONFIG",
                        "GET",
                        "maxclients",
                    ],
                    stderr=subprocess.DEVNULL,
                )
                .decode()
                .strip()
                .split("\n")
            )

            # Get stats
            stats_output = subprocess.check_output(
                ["docker", "exec", "lab01-redis", "redis-cli", "INFO", "stats"],
                stderr=subprocess.DEVNULL,
            ).decode()

            # Parse outputs
            stats = {}
            for line in clients_output.split("\n"):
                if ":" in line and not line.startswith("#"):
                    key, value = line.strip().split(":", 1)
                    if key in ["connected_clients", "blocked_clients"]:
                        stats[key] = int(value)

            for line in stats_output.split("\n"):
                if ":" in line and not line.startswith("#"):
                    key, value = line.strip().split(":", 1)
                    if key in ["total_connections_received", "rejected_connections"]:
                        stats[key] = int(value)

            maxclients = (
                int(maxclients_output[1]) if len(maxclients_output) > 1 else 10000
            )
            stats["maxclients"] = maxclients
            stats["usage_percent"] = (
                (stats.get("connected_clients", 0) / maxclients * 100)
                if maxclients > 0
                else 0
            )

            return stats
        except Exception as e2:
            print(f"Warning: Could not get Redis stats: {e2}")
            return None


def get_redis_connection_details():
    """Get detailed connection list from Redis"""
    try:
        r = redis.from_url("redis://redis:6379/0")
        clients = r.client_list()

        # Analyze connections
        by_name = {}
        idle_stats = {"<1s": 0, "1-10s": 0, "10-60s": 0, ">60s": 0}

        for client in clients:
            # Count by name
            name = client.get("name", "unnamed")
            by_name[name] = by_name.get(name, 0) + 1

            # Count by idle time
            idle = client.get("idle", 0)
            if idle < 1:
                idle_stats["<1s"] += 1
            elif idle < 10:
                idle_stats["1-10s"] += 1
            elif idle < 60:
                idle_stats["10-60s"] += 1
            else:
                idle_stats[">60s"] += 1

        return {"total": len(clients), "by_name": by_name, "idle_stats": idle_stats}
    except Exception as _e:
        return None
