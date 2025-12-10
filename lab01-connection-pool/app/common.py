from pathlib import Path
import os
# We still import os just for os.environ.get, as it's the standard way
# to read environment variables.


def get_multiproc_dir() -> Path:
    """Get or create multiprocess metrics directory"""

    # 1. Get the directory path from environment variable or use the default
    # os.environ.get is the standard way to retrieve env vars.
    metrics_dir_str = os.environ.get(
        "PROMETHEUS_MULTIPROC_DIR", "/tmp/prometheus_multiproc"
    )

    # 2. Convert the string path to a Path object
    metrics_dir = Path(metrics_dir_str)

    # 3. Check if the directory exists and create it if it doesn't
    # Path.mkdir() with parents=True and exist_ok=True is the
    # equivalent of os.makedirs(..., exist_ok=True)
    metrics_dir.mkdir(parents=True, exist_ok=True)

    # 4. Return the Path object
    return metrics_dir
