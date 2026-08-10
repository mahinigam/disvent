.PHONY: infra-up app-up pipeline-up observability-up infra-down api dashboard generator stream test smoke

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
	python3 -m py_compile apps/mock-generator/main.py apps/fast-api-engine/main.py apps/streaming-engine/main.py apps/dashboard/main.py
	python3 -m unittest discover -s tests

smoke:
	bash scripts/smoke.sh
