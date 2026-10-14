import sys

from spark_utils import get_spark


path = sys.argv[1] if len(sys.argv) > 1 else "s3a://ecom-raw-lake/bronze/orders/"
fmt = sys.argv[2] if len(sys.argv) > 2 else "parquet"

spark = get_spark("CheckSchema", delta=(fmt == "delta"))

df = spark.read.format(fmt).load(path)
print(f"\n--- SCHEMA: {path} ({fmt}) ---")
df.printSchema()

print("\n--- SAMPLE DATA ---")
df.show(5, truncate=False)
print(f"Rows: {df.count()}")

spark.stop()
