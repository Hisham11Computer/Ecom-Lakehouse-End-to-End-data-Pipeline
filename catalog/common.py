"""Shared helpers for the simulated Glue Crawler and the simulated Athena."""
import os

import duckdb


S3_HOST = os.getenv("S3_ENDPOINT_HOST", "localhost:4566")
BUCKET = os.getenv("LAKE_BUCKET", "ecom-raw-lake")
DATABASE = "ecom_lakehouse"  


def connect() -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    con.execute("INSTALL httpfs; LOAD httpfs;")
    con.execute("INSTALL delta; LOAD delta;")
    con.execute(f"""
        CREATE OR REPLACE SECRET lake (
            TYPE S3,
            KEY_ID 'test',
            SECRET 'test',
            ENDPOINT '{S3_HOST}',
            REGION 'us-east-1',
            URL_STYLE 'path',
            USE_SSL false
        );
    """)
    return con


def source_sql(table: dict) -> str:
    """DuckDB expression that reads a catalog table (Delta or Parquet)."""
    location = table["location"]
    if table["format"] == "delta":
        return f"delta_scan('{location}')"
    return f"read_parquet('{location}/*.parquet')"
