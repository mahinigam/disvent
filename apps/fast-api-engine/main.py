import os
import re
import time
from pathlib import Path
from typing import Any

import clickhouse_connect
import duckdb
from fastapi import Depends, FastAPI, HTTPException, Path as ApiPath, Query, Request
from fastapi.responses import Response
from fastapi.security import APIKeyHeader
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, generate_latest
from pydantic import BaseModel, Field


ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,80}$")


class Settings(BaseModel):
    clickhouse_host: str = Field(default_factory=lambda: os.getenv("CLICKHOUSE_HOST", "localhost"))
    clickhouse_port: int = Field(default_factory=lambda: int(os.getenv("CLICKHOUSE_PORT", "8123")))
    clickhouse_database: str = Field(default_factory=lambda: os.getenv("CLICKHOUSE_DATABASE", "disvent"))
    clickhouse_user: str = Field(default_factory=lambda: os.getenv("CLICKHOUSE_USER", "default"))
    clickhouse_password: str = Field(default_factory=lambda: os.getenv("CLICKHOUSE_PASSWORD", ""))
    parquet_dir: Path = Field(default_factory=lambda: Path(os.getenv("PARQUET_DIR", "/tmp/disvent-historical")))
    api_key: str | None = Field(default_factory=lambda: os.getenv("DISVENT_API_KEY"))
    auth_enabled: bool = Field(default_factory=lambda: os.getenv("DISVENT_AUTH_ENABLED", "false").lower() == "true")


class MerchantStats(BaseModel):
    merchant_id: str
    hour: str
    total_amount: float
    transaction_count: int


class RiskScore(BaseModel):
    user_id: str
    window_start: str
    window_end: str
    total_amount_60s: float
    transaction_count_60s: int
    distinct_devices_60s: int
    geo_spread_km: float
    risk_score: float
    reason: str
    calculated_at: str


class ThroughputMetrics(BaseModel):
    transactions_total: int
    transactions_last_minute: int
    anomalies_last_hour: int
    latest_event_time: str | None


settings = Settings()
api_key_header = APIKeyHeader(name="X-API-Key", auto_error=False)
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Histogram, Gauge, generate_latest
import asyncio
import logging

logger = logging.getLogger(__name__)

REQUEST_COUNT = Counter("disvent_api_requests_total", "Total API requests.", ["method", "path", "status"])
REQUEST_LATENCY = Histogram("disvent_api_request_duration_seconds", "API request latency.", ["method", "path"])

PIPELINE_TX_TOTAL = Gauge("disvent_pipeline_transactions_total", "Total transactions ingested into ClickHouse")
PIPELINE_TX_RATE = Gauge("disvent_pipeline_transactions_last_minute", "Transactions ingested in the last minute")
PIPELINE_ANOMALIES = Gauge("disvent_pipeline_anomalies_last_hour", "Fraud anomalies detected in the last hour")

app = FastAPI(
    title="Disvent API Engine",
    version="0.2.0",
    description="Dual-route API for real-time ClickHouse analytics and DuckDB historical audits.",
)

def _fetch_pipeline_metrics():
    client = clickhouse_client()
    tx_total = client.query("SELECT count() FROM transactions").result_rows[0][0]
    tx_rate = client.query("SELECT count() FROM transactions WHERE event_time >= now() - INTERVAL 1 MINUTE").result_rows[0][0]
    anomalies = client.query("SELECT count() FROM risk_scores WHERE calculated_at >= now() - INTERVAL 1 HOUR").result_rows[0][0]
    return tx_total, tx_rate, anomalies

async def update_pipeline_metrics():
    while True:
        try:
            tx_total, tx_rate, anomalies = await asyncio.to_thread(_fetch_pipeline_metrics)
            PIPELINE_TX_TOTAL.set(tx_total)
            PIPELINE_TX_RATE.set(tx_rate)
            PIPELINE_ANOMALIES.set(anomalies)
        except Exception as e:
            logger.error(f"Failed to update pipeline metrics: {e}")
        await asyncio.sleep(10)

@app.on_event("startup")
async def startup_event():
    asyncio.create_task(update_pipeline_metrics())


@app.middleware("http")
async def metrics_middleware(request: Request, call_next: Any) -> Response:
    started_at = time.perf_counter()
    response = await call_next(request)
    route = request.scope.get("route")
    path = getattr(route, "path", request.url.path)
    elapsed = time.perf_counter() - started_at
    REQUEST_COUNT.labels(request.method, path, str(response.status_code)).inc()
    REQUEST_LATENCY.labels(request.method, path).observe(elapsed)
    return response


def require_api_key(api_key: str | None = Depends(api_key_header)) -> None:
    if not settings.auth_enabled:
        return
    if not settings.api_key:
        raise HTTPException(status_code=503, detail="API auth is enabled but DISVENT_API_KEY is not configured.")
    if api_key != settings.api_key:
        raise HTTPException(status_code=401, detail="Invalid or missing API key.")


def sql_string(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def valid_id(value: str) -> str:
    if not ID_PATTERN.fullmatch(value):
        raise HTTPException(status_code=422, detail="ID must contain only letters, numbers, underscores, or hyphens.")
    return value


def clickhouse_client() -> Any:
    try:
        return clickhouse_connect.get_client(
            host=settings.clickhouse_host,
            port=settings.clickhouse_port,
            database=settings.clickhouse_database,
            username=settings.clickhouse_user,
            password=settings.clickhouse_password,
        )
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"ClickHouse unavailable: {exc}") from exc


def duckdb_connection() -> duckdb.DuckDBPyConnection:
    settings.parquet_dir.mkdir(parents=True, exist_ok=True)
    seed_path = settings.parquet_dir / "sample_transactions_v2.parquet"
    conn = duckdb.connect(database=":memory:")

    if not seed_path.exists():
        conn.execute(
            """
            CREATE TABLE seed AS
            SELECT *
            FROM (
                VALUES
                    ('user_1', 'merchant_1', 127.50, 'USD', 'New York', 40.7128, -74.0060, TIMESTAMP '2026-08-09 10:00:00'),
                    ('user_1', 'merchant_7', 903.20, 'USD', 'London', 51.5074, -0.1278, TIMESTAMP '2026-08-09 10:15:00'),
                    ('user_42', 'merchant_1', 58.10, 'USD', 'Singapore', 1.3521, 103.8198, TIMESTAMP '2026-08-09 11:00:00')
            ) AS t(user_id, merchant_id, amount, currency, location, latitude, longitude, event_time)
            """
        )
        conn.execute(f"COPY seed TO {sql_string(str(seed_path))} (FORMAT PARQUET)")

    return conn


@app.get("/metrics")
async def prometheus_metrics() -> Response:
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/api/v1/health")
async def health_check() -> dict[str, Any]:
    checks: dict[str, Any] = {"api": "ok", "clickhouse": "unknown", "duckdb": "unknown"}

    try:
        clickhouse_client().query("SELECT 1")
        checks["clickhouse"] = "ok"
    except HTTPException as exc:
        checks["clickhouse"] = exc.detail

    try:
        conn = duckdb_connection()
        conn.execute("SELECT 1").fetchone()
        conn.close()
        checks["duckdb"] = "ok"
    except Exception as exc:
        checks["duckdb"] = str(exc)

    checks["status"] = "ok" if checks["clickhouse"] == "ok" and checks["duckdb"] == "ok" else "degraded"
    return checks


@app.get("/api/v1/metrics/realtime-throughput", response_model=ThroughputMetrics, dependencies=[Depends(require_api_key)])
async def realtime_throughput() -> ThroughputMetrics:
    result = clickhouse_client().query(
        """
        SELECT
            count() AS transactions_total,
            countIf(event_time >= now() - INTERVAL 1 MINUTE) AS transactions_last_minute,
            (SELECT count() FROM risk_scores WHERE calculated_at >= now() - INTERVAL 1 HOUR) AS anomalies_last_hour,
            toString(max(event_time)) AS latest_event_time
        FROM transactions
        """
    )
    row = result.result_rows[0]
    return ThroughputMetrics(
        transactions_total=row[0],
        transactions_last_minute=row[1],
        anomalies_last_hour=row[2],
        latest_event_time=row[3] or None,
    )


@app.get("/api/v1/merchant/{merchant_id}/stats", response_model=list[MerchantStats], dependencies=[Depends(require_api_key)])
async def get_merchant_stats(
    merchant_id: str = ApiPath(...),
    limit: int = Query(default=24, ge=1, le=168),
) -> list[MerchantStats]:
    merchant_id = valid_id(merchant_id)
    result = clickhouse_client().query(
        """
        SELECT merchant_id, toString(hour) AS hour, total_amount, transaction_count
        FROM merchant_hourly_stats
        WHERE merchant_id = {merchant_id:String}
        ORDER BY hour DESC
        LIMIT {limit:UInt32}
        """,
        parameters={"merchant_id": merchant_id, "limit": limit},
    )
    return [
        MerchantStats(
            merchant_id=row[0],
            hour=row[1],
            total_amount=row[2],
            transaction_count=row[3],
        )
        for row in result.result_rows
    ]


@app.get("/api/v1/risk-score/{user_id}", response_model=list[RiskScore], dependencies=[Depends(require_api_key)])
async def get_risk_scores(
    user_id: str = ApiPath(...),
    limit: int = Query(default=20, ge=1, le=100),
) -> list[RiskScore]:
    user_id = valid_id(user_id)
    result = clickhouse_client().query(
        """
        SELECT
            user_id,
            toString(window_start),
            toString(window_end),
            total_amount_60s,
            transaction_count_60s,
            distinct_devices_60s,
            geo_spread_km,
            risk_score,
            reason,
            toString(calculated_at)
        FROM risk_scores
        WHERE user_id = {user_id:String}
        ORDER BY calculated_at DESC
        LIMIT {limit:UInt32}
        """,
        parameters={"user_id": user_id, "limit": limit},
    )
    return [
        RiskScore(
            user_id=row[0],
            window_start=row[1],
            window_end=row[2],
            total_amount_60s=row[3],
            transaction_count_60s=row[4],
            distinct_devices_60s=row[5],
            geo_spread_km=row[6],
            risk_score=row[7],
            reason=row[8],
            calculated_at=row[9],
        )
        for row in result.result_rows
    ]


@app.get("/api/v1/historical/user/{user_id}", dependencies=[Depends(require_api_key)])
async def get_historical_user_data(user_id: str = ApiPath(...)) -> list[dict[str, Any]]:
    user_id = valid_id(user_id)
    conn = duckdb_connection()
    try:
        result = conn.execute(
            """
            SELECT
                user_id,
                merchant_id,
                amount,
                currency,
                location,
                latitude,
                longitude,
                event_time
            FROM read_parquet(?, union_by_name = true)
            WHERE user_id = ?
            ORDER BY event_time DESC
            """,
            [str(settings.parquet_dir / "*.parquet"), user_id],
        )
        columns = [description[0] for description in result.description]
        records: list[dict[str, Any]] = []
        for row in result.fetchall():
            record = dict(zip(columns, row, strict=True))
            if record.get("event_time") is not None:
                record["event_time"] = record["event_time"].isoformat()
            records.append(record)
        return records
    except Exception as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    finally:
        conn.close()


@app.post("/api/v1/archive/recent", dependencies=[Depends(require_api_key)])
async def archive_recent_transactions(
    minutes: int = Query(default=60, ge=1, le=1440),
) -> dict[str, Any]:
    settings.parquet_dir.mkdir(parents=True, exist_ok=True)
    output_path = settings.parquet_dir / f"transactions_last_{minutes}m.parquet"
    client = clickhouse_client()
    result = client.query(
        """
        SELECT
            user_id,
            merchant_id,
            amount,
            currency,
            location,
            latitude,
            longitude,
            event_time
        FROM transactions
        WHERE event_time >= now() - INTERVAL {minutes:UInt32} MINUTE
        ORDER BY event_time DESC
        """,
        parameters={"minutes": minutes},
    )
    conn = duckdb.connect(database=":memory:")
    try:
        conn.execute(
            """
            CREATE TABLE recent_transactions (
                user_id VARCHAR,
                merchant_id VARCHAR,
                amount DOUBLE,
                currency VARCHAR,
                location VARCHAR,
                latitude DOUBLE,
                longitude DOUBLE,
                event_time TIMESTAMP
            )
            """
        )
        if result.result_rows:
            conn.executemany("INSERT INTO recent_transactions VALUES (?, ?, ?, ?, ?, ?, ?, ?)", result.result_rows)
        conn.execute(f"COPY recent_transactions TO {sql_string(str(output_path))} (FORMAT PARQUET)")
    finally:
        conn.close()

    return {
        "path": str(output_path),
        "records": len(result.result_rows),
        "minutes": minutes,
    }
