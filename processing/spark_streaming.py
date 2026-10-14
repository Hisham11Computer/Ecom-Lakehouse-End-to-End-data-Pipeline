import os

from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType, IntegerType, LongType, StringType, StructField, StructType,
)

from spark_utils import get_spark

KAFKA_BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:29092")

TOPIC = "ecom.public.orders"
BRONZE_PATH = "s3a://ecom-raw-lake/bronze/orders/"
CHECKPOINT_PATH = "s3a://ecom-raw-lake/checkpoints/orders/"

spark = get_spark(
    "EcomStreamingIngestion",
    extra_packages=["org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"],
)

kafka_df = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
    .option("subscribe", TOPIC)
    .option("startingOffsets", "earliest")
    .load()
)


order_schema = StructType([
    StructField("order_id", IntegerType(), True),
    StructField("customer_id", IntegerType(), True),
    StructField("product_name", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("status", StringType(), True),
    StructField("created_at", LongType(), True),
])


source_schema = StructType([
    StructField("ts_ms", LongType(), True),
    StructField("lsn", LongType(), True),
])

debezium_schema = StructType([
    StructField("payload", StructType([
        StructField("before", order_schema),   
        StructField("after", order_schema),
        StructField("source", source_schema),
        StructField("op", StringType()),
    ]))
])

parsed_df = (
    kafka_df
    .selectExpr("CAST(value AS STRING) AS json_str")
    .select(F.from_json(F.col("json_str"), debezium_schema).alias("data"))
    .select(
        
        F.coalesce(
            F.col("data.payload.after.order_id"),
            F.col("data.payload.before.order_id"),
        ).alias("order_id"),
        F.col("data.payload.after.customer_id").alias("customer_id"),
        F.col("data.payload.after.product_name").alias("product_name"),
        F.col("data.payload.after.price").alias("price"),
        F.col("data.payload.after.quantity").alias("quantity"),
        F.col("data.payload.after.status").alias("status"),
        (F.col("data.payload.after.created_at") / 1000000).cast("timestamp").alias("created_ts"),
        F.col("data.payload.op").alias("op"),
        (F.col("data.payload.source.ts_ms") / 1000).cast("timestamp").alias("event_ts"),
        F.col("data.payload.source.lsn").alias("lsn"),
    )
    .filter(F.col("order_id").isNotNull())  # يتجاهل الـ tombstones بس (الـ deletes بتفضل)
    .withColumn("order_date", F.date_format(F.col("created_ts"), "yyyy-MM-dd HH:mm:ss"))
    .withColumn("total_amount", F.round(F.col("price") * F.col("quantity"), 2))
    .select(
        "order_id", "customer_id", "order_date", "total_amount", "status",
        "product_name", "price", "quantity", "op", "event_ts", "lsn",
    )
)

s3_query = (
    parsed_df.writeStream
    .format("parquet")
    .outputMode("append")
    .option("path", BRONZE_PATH)
    .option("checkpointLocation", CHECKPOINT_PATH)
    .trigger(processingTime="10 seconds")
    .start()
)

print("Streaming pipeline writing Parquet files to S3 (ecom-raw-lake)...")

s3_query.awaitTermination()
