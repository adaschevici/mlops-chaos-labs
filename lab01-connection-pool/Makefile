SCENARIO ?= 1
NODE_COUNT ?= 3
DAEMON ?= false

.PHONY: build up down logs shell chaos-test diagnostics diagnose fix clean

ifeq ($(DAEMON), true)
	DAEMON_FLAG := -d
else
	DAEMON_FLAG :=
endif

up:
	docker compose up $(DAEMON_FLAG) --build --scale celery-worker=$(NODE_COUNT)
	@echo "Waiting for services to be ready..."
	@sleep 5
	@echo "Services available:"
	@echo "  - Flower: http://localhost:5555"
	@echo "  - Prometheus: http://localhost:9090"
	@echo "  - Grafana: http://localhost:3000"


chaos-test:
	docker ps -q --filter "name=lab01-connection-pool-celery-worker" |  \
		xargs -I {} docker exec {} python chaos_scenario.py $(SCENARIO)

down:
	docker compose down --volumes --remove-orphans

logs:
	docker compose logs -f


clean:
	docker compose down --volumes --remove-orphans
	docker compose rm -f

diagnostics:
	docker exec lab01-redis redis-cli INFO clients
	docker exec lab01-redis redis-cli CLIENT LIST | wc -l
	# docker ps -q --filter "name=lab01-connection-pool-celery-worker" |  xargs -I {} docker exec {} celery -A tasks inspect active
	# docker exec lab01-celery-worker python diagnose.py
	
