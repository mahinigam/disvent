ifneq (,$(wildcard ./.env))
    include .env
    export
endif

.PHONY: infra-up app-up pipeline-up observability-up infra-down api dashboard generator stream test smoke migrate load-test

infra-up:
	docker compose up -d redpanda redpanda-console clickhouse

app-up:
	docker compose --profile app up -d

pipeline-up:
	docker compose --profile pipeline up -d

observability-up:
	docker compose --profile app --profile observability up -d

infra-down:
	docker compose down

api:
	uv run --project apps/fast-api-engine uvicorn main:app --host 0.0.0.0 --port 8001 --reload

dashboard:
	uv run --project apps/dashboard streamlit run main.py --server.port 8501

generator:
	uv run --project apps/mock-generator python main.py

stream:
	uv run --project apps/streaming-engine python main.py

test:
	uv run python -m py_compile apps/mock-generator/main.py apps/fast-api-engine/main.py apps/streaming-engine/main.py apps/dashboard/main.py scripts/migrate.py apps/load-tester/locustfile.py
	uv run pytest tests/

smoke:
	bash scripts/smoke.sh

chaos:
	bash scripts/chaos.sh

migrate:
	uv run python scripts/migrate.py

load-test:
	export $$(grep -v '^#' .env | xargs) && uv run locust -f apps/load-tester/locustfile.py --host=http://localhost:8001

stress-test:
	export $$(grep -v '^#' .env | xargs) && uv run locust -f apps/load-tester/locustfile.py --host=http://localhost:8001 --users 500 --spawn-rate 50
