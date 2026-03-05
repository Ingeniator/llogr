.PHONY: dev test lint perf perf-ui up up-seed down seed

dev:
	uv run uvicorn llogr.main:app --reload

test:
	uv run pytest -v

lint:
	uv run ruff check src/ tests/

perf:
	@mkdir -p perf/report
	uv run locust -f tests/locustfile.py --host http://localhost:8000 \
		--users 20 --spawn-rate 20 --run-time 30s --headless \
		--csv perf/report/results
	@echo ""
	@echo "=== Results ==="
	@uv run python perf/collect.py
	@if [ -f perf/history.csv ] && [ $$(wc -l < perf/history.csv) -gt 2 ]; then \
		echo ""; \
		echo "=== Baseline comparison ==="; \
		echo "Baseline (first run):"; \
		head -2 perf/history.csv | tail -1 | awk -F, '{printf "  rps=%-8s avg=%-6sms p95=%-6sms p99=%-6sms\n", $$4, $$5, $$7, $$8}'; \
		echo "Current  (this run):"; \
		tail -1 perf/history.csv | awk -F, '{printf "  rps=%-8s avg=%-6sms p95=%-6sms p99=%-6sms\n", $$4, $$5, $$7, $$8}'; \
	fi

perf-ui:
	uv run locust -f tests/locustfile.py --host http://localhost:8000

up:
	docker compose up -d --build

up-seed:
	docker compose --profile seed up -d --build

down:
	docker compose --profile seed down

seed:
	uv run python scripts/seed.py
