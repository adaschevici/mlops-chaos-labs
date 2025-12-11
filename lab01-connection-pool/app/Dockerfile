FROM python:3.14-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    vim \
    curl \
    iproute2 \
    net-tools \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements first (better caching)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application code
COPY . .


# Create non-root user for security
RUN useradd -m -u 1000 celeryuser && \
    chown -R celeryuser:celeryuser /app

RUN mkdir -p /tmp/prometheus-multiproc

RUN chown -R celeryuser:celeryuser /tmp/prometheus-multiproc


USER celeryuser

# Make entrypoint executable
RUN chmod +x entrypoint.sh

# Default command (can be overridden in docker-compose)
CMD ["celery", "-A", "tasks", "worker", "--loglevel=info"]
