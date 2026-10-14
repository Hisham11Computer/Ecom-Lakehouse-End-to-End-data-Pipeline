from pyspark.sql import functions as F

from spark_utils import get_spark

SILVER_PATH = "s3a://ecom-raw-lake/silver/orders/"
DAILY_SALES_PATH = "s3a://ecom-raw-lake/gold/daily_sales/"
CUSTOMER_SUMMARY_PATH = "s3a://ecom-raw-lake/gold/customer_summary/"

spark = get_spark("ProcessGoldLayer", delta=True)

try:
    print("Reading clean data from Silver layer (Delta)...")
    silver_df = spark.read.format("delta").load(SILVER_PATH)

    print("\nCleaned Silver Data Preview:")
    silver_df.show(5, truncate=False)

    print("Calculating Daily Sales Metrics for Gold layer...")
    daily_sales_gold = (
        silver_df.groupBy("order_date")
        .agg(
            F.round(F.sum("total_amount"), 2).alias("total_daily_revenue"),
            F.count("order_id").alias("total_orders_count"),
        )
        .orderBy("order_date")
    )
    print("\nDaily Sales Gold Preview:")
    daily_sales_gold.show(5, truncate=False)
    daily_sales_gold.write.mode("overwrite").parquet(DAILY_SALES_PATH)

    print("Calculating Customer Spend Metrics for Gold layer...")
    customer_summary_gold = (
        silver_df.groupBy("customer_id")
        .agg(
            F.round(F.sum("total_amount"), 2).alias("total_spent"),
            F.count("order_id").alias("total_orders"),
        )
        .orderBy(F.col("total_spent").desc())
    )
    print("\nCustomer Summary Gold Preview:")
    customer_summary_gold.show(5, truncate=False)
    customer_summary_gold.write.mode("overwrite").parquet(CUSTOMER_SUMMARY_PATH)

    print("\nSUCCESS: Gold layer metrics created and written to S3")

except Exception as e:
    print(f"Error during Gold processing: {e}")
    raise

finally:
    spark.stop()
