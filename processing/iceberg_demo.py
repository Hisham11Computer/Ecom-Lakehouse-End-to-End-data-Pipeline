
from spark_utils import get_spark

SILVER_PATH = "s3a://ecom-raw-lake/silver/orders/"
TABLE = "iceberg.ecom.orders"

spark = get_spark("IcebergDemo", delta=True, iceberg=True)

try:
    silver_df = spark.read.format("delta").load(SILVER_PATH)
    if silver_df.limit(1).count() == 0:
        raise ValueError("Silver is empty - run the Airflow DAG first")

    print("\n[1] Create an Iceberg table from the Silver data (snapshot 1)")
    spark.sql("CREATE NAMESPACE IF NOT EXISTS iceberg.ecom")
    (
        silver_df.select(
            "order_id", "customer_id", "product_name", "price",
            "quantity", "order_date", "total_amount", "status",
        )
        .writeTo(TABLE).using("iceberg").createOrReplace()
    )

    target_id = spark.sql(f"SELECT MIN(order_id) AS id FROM {TABLE}").first()["id"]
    print(f"Order {target_id} before the update:")
    spark.sql(f"SELECT order_id, status FROM {TABLE} WHERE order_id = {target_id}").show()

    print("[2] MERGE INTO (upsert) - creates snapshot 2")
    spark.sql(f"""
        MERGE INTO {TABLE} t
        USING (SELECT {target_id} AS order_id, 'refunded' AS new_status) s
        ON t.order_id = s.order_id
        WHEN MATCHED THEN UPDATE SET status = s.new_status
    """)
    spark.sql(f"SELECT order_id, status FROM {TABLE} WHERE order_id = {target_id}").show()

    print("[3] Snapshots (every commit is a snapshot)")
    snapshots = spark.sql(
        f"SELECT snapshot_id, committed_at, operation FROM {TABLE}.snapshots ORDER BY committed_at"
    )
    snapshots.show(truncate=False)
    first_snapshot = snapshots.first()["snapshot_id"]

    print("[4] Time travel: the same order as of snapshot 1")
    spark.sql(
        f"SELECT order_id, status FROM {TABLE} VERSION AS OF {first_snapshot} "
        f"WHERE order_id = {target_id}"
    ).show()

    print("[5] Schema evolution: add a column without rewriting data")
    spark.sql(f"ALTER TABLE {TABLE} ADD COLUMN discount double")
    spark.sql(f"DESCRIBE {TABLE}").show(truncate=False)

    print("DONE: Iceberg demo finished")

except Exception as e:
    print(f"Error during Iceberg demo: {e}")
    raise

finally:
    spark.stop()
