from fastapi import FastAPI, HTTPException
import clickhouse_connect
import duckdb
import os
from pydantic import BaseModel
from typing import List, Optional

app = FastAPI(title="Disvent API Engine")

CLICKHOUSE_HOST = os.getenv("CLICKHOUSE_HOST", "localhost")
CLICKHOUSE_PORT = int(os.getenv("CLICKHOUSE_PORT", "8123"))

# Initialize ClickHouse Client
ch_client = clickhouse_connect.get_client(host=CLICKHOUSE_HOST, port=CLICKHOUSE_PORT, database='disvent')

# Initialize DuckDB for ad-hoc parquet querying
duckdb_conn = duckdb.connect(database=':memory:')

class MerchantStats(BaseModel):
    merchant_id: str
    hour: str
    total_amount: float
    transaction_count: int

@app.get("/api/v1/health")
async def health_check():
    return {"status": "ok"}

@app.get("/api/v1/merchant/{merchant_id}/stats", response_model=List[MerchantStats])
async def get_merchant_stats(merchant_id: str):
    """
    Query ClickHouse Materialized View for sub-10ms aggregations
    """
    query = f"""
        SELECT merchant_id, toString(hour) as hour, total_amount, transaction_count
        FROM merchant_hourly_stats
        WHERE merchant_id = '{merchant_id}'
        ORDER BY hour DESC
        LIMIT 24
    """
    try:
        result = ch_client.query(query)
        stats = []
        for row in result.result_rows:
            stats.append(MerchantStats(
                merchant_id=row[0],
                hour=row[1],
                total_amount=row[2],
                transaction_count=row[3]
            ))
        return stats
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/api/v1/historical/user/{user_id}")
async def get_historical_user_data(user_id: str):
    """
    Query DuckDB for ad-hoc queries over historical Parquet files.
    Demonstrates Dual Query Routing.
    """
    # Create dummy parquet file if not exists for demo purposes
    if not os.path.exists("/tmp/historical_data"):
        os.makedirs("/tmp/historical_data")
        duckdb_conn.execute("CREATE TABLE IF NOT EXISTS dummy AS SELECT 'user_1' as user_id, 100 as amount, '2023-01-01' as date")
        duckdb_conn.execute("COPY dummy TO '/tmp/historical_data/data.parquet' (FORMAT PARQUET)")
        
    try:
        query = f"SELECT * FROM read_parquet('/tmp/historical_data/*.parquet') WHERE user_id = '{user_id}'"
        df = duckdb_conn.execute(query).df()
        return df.to_dict(orient="records")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
