from pathlib import Path
import structlog
import logging
import os


def configure_logging():
    """
    Configures structlog to use the standard library logging system.
    """

    # --- 1. Standard Library Logging Configuration ---
    # Set the root logger's level and basic handler (e.g., to console)
    log_level_str = os.environ.get("LOG_LEVEL", "INFO").upper()
    log_level = getattr(logging, log_level_str, logging.INFO)

    # We use a basic handler for outputting logs
    # Note: We DON'T set a formatter here, as structlog will handle formatting.
    logging.basicConfig(
        level=log_level,
        format="%(message)s",  # Crucial: Let structlog handle the formatting
        handlers=[logging.StreamHandler()],  # Outputs to stderr by default
    )

    # --- 2. structlog Configuration ---
    # Define the processors for structured logging
    # See  for a visual representation of how processors work.
    processors = [
        # Filter: First processor to drop logs below the configured level
        structlog.stdlib.filter_by_level,
        # Context: Add context information
        structlog.stdlib.add_logger_name,  # Adds the logger name (e.g., "my_app.module_a")
        structlog.stdlib.add_log_level,  # Adds the log level (e.g., "info", "warning")
        structlog.processors.TimeStamper(fmt="iso"),
        # Exception Handling & Stack Info
        structlog.processors.StackInfoRenderer(),
        structlog.processors.format_exc_info,
        # Final Renderer: Turns the event dict into a string/bytes
        # Use ConsoleRenderer for development (readable output)
        # Use JSONRenderer for production (machine-readable)
        structlog.processors.JSONRenderer()
        if os.environ.get("ENV") == "prod"
        else structlog.dev.ConsoleRenderer(),
    ]

    structlog.configure(
        processors=processors,
        # The BoundLogger is how structlog integrates with the stdlib logging
        wrapper_class=structlog.stdlib.BoundLogger,
        # The standard stdlib logging object will be the actual backend
        logger_factory=structlog.stdlib.LoggerFactory(),
        # Cache for performance, typically always True
        cache_logger_on_first_use=True,
    )

    # Optional: Log the configuration for verification
    structlog.get_logger(__name__).info(
        "Logging configured successfully", level=log_level_str
    )


# 1. Call the configuration function FIRST
configure_logging()

# 2. Get the main application logger
# Use the module's __name__ for a properly named stdlib logger
logger = structlog.get_logger(__name__)


def get_multiproc_dir() -> Path:
    """Initialize on module import"""
    metrics_dir = os.environ.get("PROMETHEUS_MULTIPROC_DIR")

    if not metrics_dir:
        hostname = os.environ.get("HOSTNAME", "unknown")
        metrics_dir = f"/tmp/prometheus-multiproc/{hostname}"
        os.environ["PROMETHEUS_MULTIPROC_DIR"] = metrics_dir

    Path(metrics_dir).mkdir(parents=True, exist_ok=True)
    logger.info(f"[metrics] Dir: {metrics_dir}")
    return metrics_dir


METRICS_DIR = get_multiproc_dir()

# Call this function once when your application starts
if __name__ != "__main__":
    # Typically, you call this in your main application entry point
    # or immediately in your package's __init__.py.
    # We call it here just for this example, but usually you'd call it elsewhere.
    pass  # Will be called manually in the example below
