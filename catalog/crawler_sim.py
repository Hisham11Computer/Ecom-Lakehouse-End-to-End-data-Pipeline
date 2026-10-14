
import json
import os
from datetime import datetime, timezone
from pathlib import Path

import boto3

from common import BUCKET, DATABASE, connect, source_sql

S3_URL = os.getenv("S3_ENDPOINT_URL", "http://localhost:4566")
LAYERS = ["bronze", "silver", "gold"]
OUTPUT = Path(__file__).with_name("catalog.json")

s3 = boto3.client(
    "s3",
    endpoint_url=S3_URL,
    aws_access_key_id="test",
    aws_secret_access_key="test",
    region_name="us-east-1",
)


def list_subfolders(prefix: str) -> list:
    paginator = s3.get_paginator("list_objects_v2")
    folders = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix, Delimiter="/"):
        folders += [p["Prefix"] for p in page.get("CommonPrefixes", [])]
    return folders


def list_keys(prefix: str) -> list:
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        keys += [obj["Key"] for obj in page.get("Contents", [])]
    return keys


def detect_format(keys: list):
    if any("/_delta_log/" in key for key in keys):
        return "delta"
    if any(key.endswith(".parquet") for key in keys):
        return "parquet"
    return None


def main() -> None:
    con = connect()
    tables = []

    for layer in LAYERS:
        print(f"[crawler] scanning s3://{BUCKET}/{layer}/")
        for folder in list_subfolders(f"{layer}/"):
            fmt = detect_format(list_keys(folder))
            if fmt is None:
                print(f"[crawler]   skipped {folder} (no table files)")
                continue

            name = folder.rstrip("/").split("/")[-1]
            table = {
                "name": f"{layer}_{name}",
                "layer": layer,
                "format": fmt,
                "location": f"s3://{BUCKET}/{folder.rstrip('/')}",
            }

            described = con.sql(f"DESCRIBE SELECT * FROM {source_sql(table)}").fetchall()
            table["columns"] = [{"name": row[0], "type": row[1]} for row in described]
            table["record_count"] = con.sql(
                f"SELECT COUNT(*) FROM {source_sql(table)}"
            ).fetchone()[0]

            tables.append(table)
            print(
                f"[crawler]   found table {table['name']} "
                f"({fmt}, {len(table['columns'])} columns, {table['record_count']} rows)"
            )

    catalog = {
        "database": DATABASE,
        "crawled_at": datetime.now(timezone.utc).isoformat(),
        "tables": tables,
    }
    OUTPUT.write_text(json.dumps(catalog, indent=2))
    print(f"[crawler] done: {len(tables)} tables written to {OUTPUT.name}")


if __name__ == "__main__":
    main()
