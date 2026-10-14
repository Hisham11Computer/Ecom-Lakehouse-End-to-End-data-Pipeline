import os

from pyspark.sql import SparkSession


if os.name == "nt":
    os.environ["HADOOP_HOME"] = r"D:\hadoop"
    os.environ["JAVA_HOME"] = r"D:\jdk"
    os.environ["PATH"] = (
        os.environ["HADOOP_HOME"] + r"\bin;"
        + os.environ["JAVA_HOME"] + r"\bin;"
        + os.environ["PATH"]
    )


USE_LOCALSTACK = os.getenv("USE_LOCALSTACK", "true").lower() == "true"


S3_ENDPOINT = os.getenv(
    "S3_ENDPOINT",
    "http://localhost:4566" if os.name == "nt" else "http://localstack:4566",
)

WAREHOUSE = os.getenv("ICEBERG_WAREHOUSE", "s3a://ecom-raw-lake/iceberg")

HADOOP_AWS = "org.apache.hadoop:hadoop-aws:3.3.4"
DELTA = "io.delta:delta-spark_2.12:3.1.0"
ICEBERG = "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.0"


def get_spark(
    app_name: str,
    delta: bool = False,
    iceberg: bool = False,
    extra_packages=None,
) -> SparkSession:
    packages = [HADOOP_AWS]
    extensions = []

    if delta:
        packages.append(DELTA)
        extensions.append("io.delta.sql.DeltaSparkSessionExtension")
    if iceberg:
        packages.append(ICEBERG)
        extensions.append("org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    packages += extra_packages or []

    builder = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.jars.packages", ",".join(packages))
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.fast.upload", "true")
    )

    if extensions:
        builder = builder.config("spark.sql.extensions", ",".join(extensions))
    if delta:
        builder = builder.config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
        )
    if iceberg:
        
        builder = (
            builder
            .config("spark.sql.catalog.iceberg", "org.apache.iceberg.spark.SparkCatalog")
            .config("spark.sql.catalog.iceberg.type", "hadoop")
            .config("spark.sql.catalog.iceberg.warehouse", WAREHOUSE)
        )

    if USE_LOCALSTACK:
        builder = (
            builder
            .config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", os.getenv("AWS_ACCESS_KEY_ID", "test"))
            .config("spark.hadoop.fs.s3a.secret.key", os.getenv("AWS_SECRET_ACCESS_KEY", "test"))
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
        )
    else:
        builder = builder.config(
            "spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain",
        )

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    return spark
