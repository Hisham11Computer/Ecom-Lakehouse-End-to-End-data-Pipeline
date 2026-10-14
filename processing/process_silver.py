from delta.tables import DeltaTable
from pyspark.sql import functions as F
from pyspark.sql.window import Window

from spark_utils import get_spark

BRONZE_PATH = "s3a://ecom-raw-lake/bronze/orders/"
SILVER_PATH = "s3a://ecom-raw-lake/silver/orders/"

COLS = [
    "order_id", "customer_id", "product_name", "price", "quantity",
    "order_date", "total_amount", "status", "_lsn", "_event_ts",
]

spark = get_spark("ProcessSilverLayer", delta=True)

try:
    print("Reading raw data from Bronze layer...")
    bronze_df = spark.read.parquet(BRONZE_PATH)
    if bronze_df.limit(1).count() == 0:
        raise ValueError("Bronze layer is empty - is the spark-streaming service running?")

    
    silver_exists = DeltaTable.isDeltaTable(spark, SILVER_PATH)
    last_lsn = None
    if silver_exists:
        last_lsn = (
            spark.read.format("delta").load(SILVER_PATH)
            .agg(F.max("_lsn")).first()[0]
        )

    new_df = bronze_df if last_lsn is None else bronze_df.filter(F.col("lsn") > F.lit(last_lsn))
    new_count = new_df.count()
    print(f"Last processed LSN: {last_lsn} | new Bronze events: {new_count}")

    if new_count == 0:
        print("Nothing new to process - Silver is up to date.")
    else:
        
        w = Window.partitionBy("order_id").orderBy(F.col("lsn").desc(), F.col("event_ts").desc())
        latest = (
            new_df.withColumn("rn", F.row_number().over(w))
            .filter(F.col("rn") == 1)
            .drop("rn")
        )

        changes = (
            latest.select(
                F.col("order_id").cast("int").alias("order_id"),
                F.col("customer_id").cast("int").alias("customer_id"),
                F.col("product_name"),
                F.col("price").cast("double").alias("price"),
                F.col("quantity").cast("int").alias("quantity"),
                F.to_date(F.col("order_date")).alias("order_date"),
                F.col("total_amount").cast("double").alias("total_amount"),
                F.col("status"),
                F.col("op"),
                F.col("lsn").alias("_lsn"),
                F.col("event_ts").alias("_event_ts"),
            )
           
            .filter(
                (F.col("op") == "d")
                | (
                    F.col("customer_id").isNotNull()
                    & F.col("order_date").isNotNull()
                    & F.col("total_amount").isNotNull()
                )
            )
        )

        if not silver_exists:
            print("Creating Silver Delta table...")
            (
                changes.filter(F.col("op") != "d").select(*COLS)
                .write.format("delta").mode("overwrite").save(SILVER_PATH)
            )
        else:
            print("Applying changes with MERGE...")
            values = {c: f"s.{c}" for c in COLS}
            (
                DeltaTable.forPath(spark, SILVER_PATH).alias("t")
                .merge(changes.alias("s"), "t.order_id = s.order_id")
                .whenMatchedDelete(condition="s.op = 'd'")
                .whenMatchedUpdate(condition="s.op != 'd'", set=values)
                .whenNotMatchedInsert(condition="s.op != 'd'", values=values)
                .execute()
            )

    total = spark.read.format("delta").load(SILVER_PATH).count()
    print(f"SUCCESS: Silver layer up to date ({total} rows)")

except Exception as e:
    print(f"Error during Silver processing: {e}")
    raise 

finally:
    spark.stop()
