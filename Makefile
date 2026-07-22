.PHONY: up down logs topics lint test load chaos clean

up:            ## Start the full stack (Redpanda, Postgres, Redis, gateway, reconciler, Grafana)
	docker compose up -d --build
	$(MAKE) topics

down:
	docker compose down -v

logs:
	docker compose logs -f gateway reconciler

topics:        ## Create Kafka topics with the right partitioning and compaction
	docker compose exec -T redpanda rpk topic create \
		requests.started requests.metered provider.usage settlements drift \
		-p 6 -r 1 || true

lint:
	ruff check .

test:
	pytest -q

load:          ## Poisson-burst load generator against the gateway
	python -m chaos.load --rps 40 --duration 120

chaos:         ## Run the failure-injection suite and assert drift converges to zero
	python -m chaos.run

clean: down
	docker compose rm -f
