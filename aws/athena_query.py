
import os
import time
from pathlib import Path

import boto3

REGION = os.getenv("AWS_REGION", "eu-west-1")
BUCKET = os.environ["LAKE_BUCKET"]
DATABASE = "ecom_lakehouse"
OUTPUT = f"s3://{BUCKET}/athena-results/"
QUERIES = Path(__file__).resolve().parent.parent / "analytics" / "queries"

athena = boto3.client("athena", region_name=REGION)


def run_query(sql: str) -> list:
    execution_id = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": DATABASE},
        ResultConfiguration={"OutputLocation": OUTPUT},
    )["QueryExecutionId"]

    while True:
        status = athena.get_query_execution(QueryExecutionId=execution_id)["QueryExecution"]["Status"]
        if status["State"] in ("SUCCEEDED", "FAILED", "CANCELLED"):
            break
        time.sleep(1)

    if status["State"] != "SUCCEEDED":
        raise RuntimeError(f"Athena query {status['State']}: {status.get('StateChangeReason')}")

    rows = athena.get_query_results(QueryExecutionId=execution_id)["ResultSet"]["Rows"]
    return [[col.get("VarCharValue") for col in row["Data"]] for row in rows]


if __name__ == "__main__":
    for sql_file in sorted(QUERIES.glob("*.sql")):
        print(f"\n=== {sql_file.name} ===")
        for row in run_query(sql_file.read_text().strip().rstrip(";")):
            print(row)
