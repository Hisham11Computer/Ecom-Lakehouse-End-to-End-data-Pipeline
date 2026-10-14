
import os
import sys

import great_expectations as gx
from deltalake import DeltaTable

SILVER_URI = os.getenv("SILVER_URI", "s3://ecom-raw-lake/silver/orders")
USE_LOCALSTACK = os.getenv("USE_LOCALSTACK", "true").lower() == "true"

storage_options = {}
if USE_LOCALSTACK:
    storage_options = {
        "AWS_ENDPOINT_URL": os.getenv("S3_ENDPOINT", "http://localstack:4566"),
        "AWS_ACCESS_KEY_ID": os.getenv("AWS_ACCESS_KEY_ID", "test"),
        "AWS_SECRET_ACCESS_KEY": os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
        "AWS_REGION": "us-east-1",
        "AWS_ALLOW_HTTP": "true",
    }

print(f"Loading Silver table: {SILVER_URI}")
df = DeltaTable(SILVER_URI, storage_options=storage_options).to_pandas()
print(f"Loaded {len(df)} rows")


df["amount_diff"] = (df["total_amount"] - (df["price"] * df["quantity"]).round(2)).abs()

context = gx.get_context(mode="ephemeral")
data_source = context.data_sources.add_pandas(name="silver_source")
asset = data_source.add_dataframe_asset(name="silver_orders")
batch_definition = asset.add_batch_definition_whole_dataframe("whole_table")
batch = batch_definition.get_batch(batch_parameters={"dataframe": df})

E = gx.expectations
expectations = [
    E.ExpectTableRowCountToBeBetween(min_value=1),
    E.ExpectColumnValuesToNotBeNull(column="order_id"),
    E.ExpectColumnValuesToNotBeNull(column="customer_id"),
    E.ExpectColumnValuesToNotBeNull(column="order_date"),
    E.ExpectColumnValuesToNotBeNull(column="total_amount"),
    E.ExpectColumnValuesToBeUnique(column="order_id"),
    E.ExpectColumnValuesToBeBetween(column="price", min_value=0, strict_min=True),
    E.ExpectColumnValuesToBeBetween(column="quantity", min_value=0, strict_min=True),
    E.ExpectColumnValuesToBeBetween(column="total_amount", min_value=0, strict_min=True),
    E.ExpectColumnValuesToBeBetween(column="amount_diff", max_value=0.01),
    E.ExpectColumnValuesToBeInSet(
        column="status",
        value_set=["completed", "refunded", "cancelled", "pending", "shipped"],
    ),
]

failed = []
print("\nGreat Expectations validation report")
for expectation in expectations:
    result = batch.validate(expectation)
    name = type(expectation).__name__
    column = getattr(expectation, "column", "") or "(table)"
    print(f"  [{'PASS' if result.success else 'FAIL'}] {name} on {column}")
    if not result.success:
        failed.append(f"{name}:{column}")

if failed:
    print(f"\nData quality gate FAILED: {failed}")
    sys.exit(1)

print("\nAll expectations passed - Gold can run.")
