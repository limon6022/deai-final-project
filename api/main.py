"""
FastAPI Analytics Layer
Reads gold-layer Parquet files via DuckDB and exposes REST endpoints.
"""

from __future__ import annotations
import os
from functools import lru_cache
from typing import Optional

import duckdb
from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware

app = FastAPI(
    title="NYC Taxi Analytics API",
    description="Analytical REST API over the NYC TLC trip data pipeline.",
    version="1.0.0",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://localhost:9000").replace("http://", "")
MINIO_ACCESS   = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET   = os.getenv("MINIO_SECRET_KEY", "minioadmin")

def gold(table: str) -> str:
    return f"read_parquet('s3://taxi/gold/{table}/*.parquet')"

@lru_cache(maxsize=1)
def db() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect(database=":memory:")
    con.execute(f"""
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_endpoint='{MINIO_ENDPOINT}';
        SET s3_access_key_id='{MINIO_ACCESS}';
        SET s3_secret_access_key='{MINIO_SECRET}';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
    """)
    return con

@app.get("/", tags=["Health"])
def root():
    return {"status": "ok", "docs": "/docs"}

@app.get("/rides-per-day", tags=["Demand"])
def rides_per_day(
    provider: Optional[str] = Query(None),
    start_date: Optional[str] = Query(None),
    end_date: Optional[str] = Query(None),
):
    """Analytical Question 1: How does daily ride demand vary over time?"""
    filters = []
    if provider:
        filters.append(f"provider = '{provider}'")
    if start_date:
        filters.append(f"trip_date >= '{start_date}'")
    if end_date:
        filters.append(f"trip_date <= '{end_date}'")
    where = ("WHERE " + " AND ".join(filters)) if filters else ""
    sql = f"""
        SELECT trip_date, provider,
               SUM(ride_count) AS total_rides,
               ROUND(AVG(avg_fare), 2) AS avg_fare_usd,
               ROUND(SUM(total_revenue), 2) AS total_revenue_usd
        FROM {gold("rides_per_hour")}
        {where}
        GROUP BY trip_date, provider
        ORDER BY trip_date, provider
    """
    rows = db().execute(sql).fetchdf().to_dict(orient="records")
    return {"count": len(rows), "data": rows}

@app.get("/rides-per-hour", tags=["Demand"])
def rides_per_hour(
    provider: Optional[str] = Query(None),
    date: Optional[str] = Query(None),
):
    """Analytical Question 1 detail: What are the peak hours for demand?"""
    filters = []
    if provider:
        filters.append(f"provider = '{provider}'")
    if date:
        filters.append(f"trip_date = '{date}'")
    where = ("WHERE " + " AND ".join(filters)) if filters else ""
    sql = f"""
        SELECT pickup_hour, provider,
               SUM(ride_count) AS total_rides,
               ROUND(AVG(avg_fare), 2) AS avg_fare_usd,
               ROUND(AVG(avg_duration_min), 1) AS avg_duration_min
        FROM {gold("rides_per_hour")}
        {where}
        GROUP BY pickup_hour, provider
        ORDER BY pickup_hour, provider
    """
    rows = db().execute(sql).fetchdf().to_dict(orient="records")
    return {"count": len(rows), "data": rows}

@app.get("/provider-summary", tags=["Providers"])
def provider_summary():
    """Analytical Question 2: How do providers compare?"""
    sql = f"""
        SELECT provider,
               total_rides,
               ROUND(avg_fare, 2) AS avg_fare_usd,
               ROUND(avg_tip, 2) AS avg_tip_usd,
               ROUND(avg_distance_miles, 2) AS avg_distance_miles,
               ROUND(avg_duration_min, 1) AS avg_duration_min,
               ROUND(total_revenue, 2) AS total_revenue_usd,
               ROUND(avg_speed_mph, 1) AS avg_speed_mph
        FROM {gold("provider_summary")}
        ORDER BY total_rides DESC
    """
    rows = db().execute(sql).fetchdf().to_dict(orient="records")
    return {"count": len(rows), "data": rows}

@app.get("/uber-vs-lyft", tags=["Providers"])
def uber_vs_lyft():
    """Analytical Question 2 focused: Direct Uber vs Lyft comparison."""
    sql = f"""
        SELECT provider,
               total_rides,
               ROUND(avg_fare, 2) AS avg_fare_usd,
               ROUND(avg_tip / avg_fare * 100, 1) AS tip_rate_pct,
               ROUND(avg_distance_miles, 2) AS avg_distance_miles,
               ROUND(avg_fare / avg_distance_miles, 2) AS fare_per_mile,
               ROUND(avg_duration_min, 1) AS avg_duration_min,
               ROUND(total_revenue, 2) AS total_revenue_usd
        FROM {gold("provider_summary")}
        WHERE provider IN ('uber', 'lyft')
        ORDER BY provider
    """
    rows = db().execute(sql).fetchdf().to_dict(orient="records")
    if not rows:
        raise HTTPException(status_code=404, detail="Uber/Lyft data not found.")
    return {"data": rows}

@app.get("/top-routes", tags=["Routes"])
def top_routes(
    provider: Optional[str] = Query(None),
    limit: int = Query(20, ge=1, le=200),
):
    """Analytical Question 3: Which routes are most popular?"""
    filters = []
    if provider:
        filters.append(f"provider = '{provider}'")
    where = ("WHERE " + " AND ".join(filters)) if filters else ""
    sql = f"""
        SELECT pickup_location_id, dropoff_location_id, provider,
               SUM(ride_count) AS total_rides,
               ROUND(AVG(avg_fare), 2) AS avg_fare_usd,
               ROUND(AVG(avg_duration_min), 1) AS avg_duration_min
        FROM {gold("top_routes")}
        {where}
        GROUP BY pickup_location_id, dropoff_location_id, provider
        ORDER BY total_rides DESC
        LIMIT {limit}
    """
    rows = db().execute(sql).fetchdf().to_dict(orient="records")
    return {"count": len(rows), "data": rows}

@app.get("/day-of-week", tags=["Demand"])
def day_of_week(provider: Optional[str] = Query(None)):
    """Analytical Question 4: How does demand vary by day of week?"""
    DAY_NAMES = {1:"Sunday", 2:"Monday", 3:"Tuesday", 4:"Wednesday",
                 5:"Thursday", 6:"Friday", 7:"Saturday"}
    filters = []
    if provider:
        filters.append(f"provider = '{provider}'")
    where = ("WHERE " + " AND ".join(filters)) if filters else ""
    sql = f"""
        SELECT pickup_day_of_week, provider,
               SUM(ride_count) AS total_rides,
               ROUND(AVG(avg_fare), 2) AS avg_fare_usd
        FROM {gold("dow_demand")}
        {where}
        GROUP BY pickup_day_of_week, provider
        ORDER BY pickup_day_of_week, provider
    """
    rows = db().execute(sql).fetchdf().to_dict(orient="records")
    for r in rows:
        r["day_name"] = DAY_NAMES.get(r["pickup_day_of_week"], "?")
    return {"count": len(rows), "data": rows}

@app.get("/health", tags=["Health"])
def health():
    try:
        db().execute("SELECT 1").fetchone()
        return {"status": "healthy", "duckdb": "ok"}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))