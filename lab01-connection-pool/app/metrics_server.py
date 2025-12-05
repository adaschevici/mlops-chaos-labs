import os
from prometheus_client import CollectorRegistry
from prometheus_client.wsgi import make_wsgi_app
from gevent.pywsgi import WSGIServer
import sys
from gevent import monkey

# ----------------------------------------------------------------------
# Metrics Server Script
#
# This script is designed to run as the dedicated metrics exporter
# (the 'prometheus_exporter' service in your docker-compose.yml).
#
# It uses gevent's WSGIServer to serve the aggregated metrics, ensuring
# the server uses cooperative concurrency.
# ----------------------------------------------------------------------

# --- Configuration ---
PORT = 8000
HOST = "0.0.0.0"
# The registry used for aggregation when PROMETHEUS_MULTIPROC_DIR is set.
# We explicitly create a registry that will read from the environment directory.
# The `make_wsgi_app` function will use this registry.
REGISTRY = CollectorRegistry()
# ---------------------


def run_metrics_server():
    """
    Initializes and starts the gevent WSGI server to expose aggregated metrics.
    """

    # 1. Check for the mandatory environment variable
    multiproc_dir = os.environ.get("PROMETHEUS_MULTIPROC_DIR")
    if not multiproc_dir:
        print("FATAL ERROR: PROMETHEUS_MULTIPROC_DIR environment variable is not set.")
        print(
            "This variable must point to the shared volume where metric files are stored."
        )
        sys.exit(1)

    print(f"✅ Starting Prometheus Exporter on {HOST}:{PORT}")
    print(f"✅ Aggregating metrics from directory: {multiproc_dir}")

    # 2. Create the WSGI application that generates the Prometheus exposition format
    # The registry will automatically handle reading and aggregating metric files
    # from the PROMETHEUS_MULTIPROC_DIR path on every scrape.
    metrics_app = make_wsgi_app(REGISTRY)

    # 3. Create and start the gevent WSGI server
    try:
        # Use gevent's WSGIServer for non-blocking serving
        server = WSGIServer((HOST, PORT), metrics_app)

        # Start the server and block the main greenlet indefinitely
        # This keeps the container running and serving requests cooperatively.
        print("📊 Gevent Metrics Server listening...")
        server.serve_forever()

    except OSError as e:
        print(f"FATAL ERROR: Could not bind to port {PORT}. Error: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        print("\n🛑 Shutting down server...")
    except Exception as e:
        print(f"❌ An unexpected error occurred: {e}")


if __name__ == "__main__":
    # You MUST patch gevent before using its features like WSGIServer
    monkey.patch_all()

    run_metrics_server()
