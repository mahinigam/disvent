import os
import sys
import pytest
from unittest.mock import MagicMock, patch
from fastapi.testclient import TestClient

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Must set auth environment variables before importing main to enable auth in settings
os.environ["DISVENT_AUTH_ENABLED"] = "true"
os.environ["DISVENT_API_KEY"] = "test-secret-key"

from main import app, settings, get_historical_user_data, archive_recent_transactions


@pytest.fixture
def client():
    return TestClient(app)


@pytest.fixture
def valid_headers():
    return {"X-API-Key": "test-secret-key"}


@pytest.fixture
def mock_clickhouse():
    with patch("main.clickhouse_client") as mock:
        yield mock


@pytest.fixture
def mock_duckdb():
    with patch("main.duckdb_connection") as mock:
        yield mock


def test_prometheus_metrics(client):
    response = client.get("/metrics")
    assert response.status_code == 200
    assert b"disvent_api_requests_total" in response.content


def test_health_check_ok(client, mock_clickhouse, mock_duckdb):
    mock_ch_instance = MagicMock()
    mock_clickhouse.return_value = mock_ch_instance

    mock_db_instance = MagicMock()
    mock_duckdb.return_value = mock_db_instance
    mock_db_instance.execute.return_value.fetchone.return_value = (1,)

    response = client.get("/api/v1/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "ok"
    assert data["clickhouse"] == "ok"
    assert data["duckdb"] == "ok"


from fastapi import HTTPException

def test_health_check_degraded(client, mock_clickhouse, mock_duckdb):
    mock_clickhouse.side_effect = HTTPException(status_code=503, detail="ClickHouse unavailable")
    mock_db_instance = MagicMock()
    mock_duckdb.return_value = mock_db_instance
    mock_db_instance.execute.return_value.fetchone.return_value = (1,)

    response = client.get("/api/v1/health")
    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "degraded"
    assert data["duckdb"] == "ok"


def test_auth_missing(client):
    response = client.get("/api/v1/metrics/realtime-throughput")
    assert response.status_code == 401
    assert response.json()["detail"] == "Invalid or missing API key."


def test_auth_invalid(client):
    response = client.get(
        "/api/v1/metrics/realtime-throughput", headers={"X-API-Key": "wrong-key"}
    )
    assert response.status_code == 401


def test_realtime_throughput(client, valid_headers, mock_clickhouse):
    mock_ch_instance = MagicMock()
    mock_clickhouse.return_value = mock_ch_instance

    # Mock the return value of result_rows[0]
    mock_ch_instance.query.return_value.result_rows = [
        (1000, 50, 5, "2026-08-09 10:00:00")
    ]

    response = client.get("/api/v1/metrics/realtime-throughput", headers=valid_headers)
    assert response.status_code == 200
    data = response.json()
    assert data["transactions_total"] == 1000
    assert data["transactions_last_minute"] == 50
    assert data["anomalies_last_hour"] == 5
    assert data["latest_event_time"] == "2026-08-09 10:00:00"


def test_get_merchant_stats(client, valid_headers, mock_clickhouse):
    mock_ch_instance = MagicMock()
    mock_clickhouse.return_value = mock_ch_instance
    mock_ch_instance.query.return_value.result_rows = [
        ("merchant_1", "2026-08-09 10:00:00", 5000.50, 150)
    ]

    response = client.get("/api/v1/merchant/merchant_1/stats", headers=valid_headers)
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["merchant_id"] == "merchant_1"
    assert data[0]["total_amount"] == 5000.50
    assert data[0]["transaction_count"] == 150


def test_get_risk_scores(client, valid_headers, mock_clickhouse):
    mock_ch_instance = MagicMock()
    mock_clickhouse.return_value = mock_ch_instance
    mock_ch_instance.query.return_value.result_rows = [
        (
            "user_1",
            "2026-08-09 10:00:00",
            "2026-08-09 10:01:00",
            12000.0,
            15,
            3,
            2500.5,
            0.95,
            "High velocity and amount",
            "2026-08-09 10:01:05",
        )
    ]

    response = client.get("/api/v1/risk-score/user_1", headers=valid_headers)
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["user_id"] == "user_1"
    assert data[0]["geo_spread_km"] == 2500.5
    assert data[0]["risk_score"] == 0.95


def test_get_historical_user_data(client, valid_headers, mock_duckdb):
    mock_db_instance = MagicMock()
    mock_duckdb.return_value = mock_db_instance
    
    mock_result = MagicMock()
    mock_result.description = [
        ("user_id",), ("merchant_id",), ("amount",), ("currency",), 
        ("location",), ("latitude",), ("longitude",), ("event_time",)
    ]
    
    import datetime
    mock_result.fetchall.return_value = [
        ("user_1", "merchant_1", 150.0, "USD", "New York", 40.71, -74.00, datetime.datetime(2026, 8, 9, 10, 0, 0))
    ]
    mock_db_instance.execute.return_value = mock_result

    response = client.get("/api/v1/historical/user/user_1", headers=valid_headers)
    assert response.status_code == 200
    data = response.json()
    assert len(data) == 1
    assert data[0]["user_id"] == "user_1"
    assert data[0]["amount"] == 150.0
    assert data[0]["event_time"] == "2026-08-09T10:00:00"


@patch("main.duckdb")
def test_archive_recent_transactions(mock_duckdb_module, client, valid_headers, mock_clickhouse):
    mock_ch_instance = MagicMock()
    mock_clickhouse.return_value = mock_ch_instance
    mock_ch_instance.query.return_value.result_rows = [
        ("user_1", "merchant_1", 150.0, "USD", "New York", 40.71, -74.00, "2026-08-09 10:00:00")
    ]

    mock_db_conn = MagicMock()
    mock_duckdb_module.connect.return_value = mock_db_conn

    response = client.post("/api/v1/archive/recent?minutes=30", headers=valid_headers)
    
    assert response.status_code == 200
    data = response.json()
    assert data["records"] == 1
    assert data["minutes"] == 30
    assert data["path"].endswith("transactions_last_30m.parquet")
    
    # Verify DuckDB was called to write the parquet
    mock_db_conn.executemany.assert_called_once()
    
    # Verify the CREATE TABLE call
    create_call = mock_db_conn.execute.call_args_list[0]
    assert "CREATE TABLE recent_transactions" in create_call[0][0]
