SHELL := /bin/bash
SCENARIO ?= 1
NODE_COUNT ?= 3
DAEMON ?= false
SERVICE_PATTERN ?= lab01-
COMPOSE_FILES := -f docker-compose.infra.yml \
								 -f docker-compose.monitoring.yml \
								 -f docker-compose.app.yml

.PHONY: build up down logs shell chaos-test diagnostics diagnose fix clean docker-stats

ifeq ($(DAEMON), true)
	DAEMON_FLAG := -d
else
	DAEMON_FLAG :=
endif

up: clean
	docker compose $(COMPOSE_FILES) up $(DAEMON_FLAG) --build --scale celery-worker=$(NODE_COUNT)
	@echo "Waiting for services to be ready..."
	@sleep 5
	@echo "Services available:"
	@echo "  - Prometheus: http://localhost:9090"
	@echo "  - Grafana: http://localhost:3000"


chaos-test:
	docker ps -q --filter "name=lab01-connection-pool-celery-worker" |  \
		xargs -I {} -P 0 docker exec {} python chaos_scenario.py $(SCENARIO)

down:
	docker compose $(COMPOSE_FILES) down --volumes --remove-orphans

logs:
	docker compose $(COMPOSE_FILES) logs -f


clean:
	docker compose $(COMPOSE_FILES) down --volumes --remove-orphans
	docker compose $(COMPOSE_FILES) rm -f

diagnostics:
	docker exec lab01-redis redis-cli INFO clients
	# docker exec lab01-redis redis-cli CLIENT LIST
	# docker exec lab01-redis redis-cli INFO memory
	# docker exec lab01-redis redis-cli MEMORY STATS
	# watch -n 1 'docker exec lab01-redis redis-cli INFO clients'
	# docker exec lab01-redis redis-cli CLIENT LIST | wc -l
	# docker ps -q --filter "name=lab01-connection-pool-celery-worker" |  xargs -I {} docker exec {} celery -A tasks inspect active
	# docker exec lab01-celery-worker python diagnose.py
	#
pending-tasks:
	docker exec lab01-redis redis-cli LLEN celery

docker-stats:
	@echo "Monitoring stats for services matching: $(SERVICE_PATTERN)... (Ctrl+C to stop)"
	@CONTAINERS=$$(docker ps --format "{{.Names}}" --filter "name=$(SERVICE_PATTERN)"); \
	if [ -z "$$CONTAINERS" ]; then \
		echo "No containers found matching pattern: $(SERVICE_PATTERN)"; \
	else \
		exec docker stats $$CONTAINERS; \
	fi
