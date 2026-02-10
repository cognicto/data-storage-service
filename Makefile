# Sensor Data Storage Service Makefile

.PHONY: help build run dev test clean install lint format docker-build docker-push

# Default target
help: ## Show this help message
	@echo "Sensor Data Storage Service"
	@echo "Available targets:"
	@awk 'BEGIN {FS = ":.*?## "} /^[a-zA-Z_-]+:.*?## / {printf "  %-15s %s\n", $$1, $$2}' $(MAKEFILE_LIST)

# Development
install: ## Install dependencies
	pip install -r requirements.txt

dev: ## Start development environment
	./scripts/start-dev.sh

run: ## Run service locally
	python -m app.main

test: ## Run tests
	pytest tests/ -v

lint: ## Run linting
	flake8 app/ tests/
	black --check app/ tests/

format: ## Format code
	black app/ tests/
	isort app/ tests/

# Docker
docker-build: ## Build Docker image
	docker build -t sensor-storage-service:latest .

docker-run: ## Run Docker container
	docker run --env-file .env -p 8080:8080 sensor-storage-service:latest

docker-push: ## Push to registry (set REGISTRY variable)
	@if [ -z "$(REGISTRY)" ]; then echo "Set REGISTRY variable"; exit 1; fi
	docker tag sensor-storage-service:latest $(REGISTRY)/sensor-storage-service:latest
	docker push $(REGISTRY)/sensor-storage-service:latest

# Compose
compose-up: ## Start with docker-compose
	docker-compose up -d

compose-dev: ## Start development stack
	docker-compose --profile dev up -d

compose-down: ## Stop docker-compose
	docker-compose down

compose-logs: ## View logs
	docker-compose logs -f sensor-storage-service

# Testing
test-kafka: ## Run Kafka test producer
	python scripts/test-kafka-producer.py

# Kafka Connect
kafka-connect-status: ## Check Kafka Connect status
	curl -s http://localhost:8083/ | jq

kafka-connect-connectors: ## List Kafka Connect connectors
	curl -s http://localhost:8083/connectors | jq

kafka-connect-logs: ## View Kafka Connect logs
	docker-compose logs -f kafka-connect

# TimescaleDB Connector Management
timescaledb-deploy: ## Deploy TimescaleDB source connector
	curl -X POST http://localhost:8083/connectors \
		-H "Content-Type: application/json" \
		-d @kafka-connect/config/timescaledb-source.json

timescaledb-status: ## Check TimescaleDB connector status
	curl -s http://localhost:8083/connectors/timescaledb-source-connector/status | jq

timescaledb-restart: ## Restart TimescaleDB connector
	curl -X POST http://localhost:8083/connectors/timescaledb-source-connector/restart

timescaledb-pause: ## Pause TimescaleDB connector
	curl -X PUT http://localhost:8083/connectors/timescaledb-source-connector/pause

timescaledb-resume: ## Resume TimescaleDB connector
	curl -X PUT http://localhost:8083/connectors/timescaledb-source-connector/resume

timescaledb-delete: ## Delete TimescaleDB connector
	curl -X DELETE http://localhost:8083/connectors/timescaledb-source-connector

timescaledb-config: ## Show TimescaleDB connector config
	curl -s http://localhost:8083/connectors/timescaledb-source-connector/config | jq

# Cleanup
clean: ## Clean up containers and images
	docker-compose down -v
	docker system prune -f

# Kubernetes
k8s-deploy: ## Deploy to Kubernetes
	kubectl apply -f deployment/kubernetes/

k8s-delete: ## Delete from Kubernetes
	kubectl delete -f deployment/kubernetes/

k8s-logs: ## View Kubernetes logs
	kubectl logs -f deployment/sensor-storage-service -n sensor-storage

# Monitoring
health: ## Check service health
	curl -f http://localhost:8080/health | jq

metrics: ## Get service metrics
	curl http://localhost:8080/metrics | jq