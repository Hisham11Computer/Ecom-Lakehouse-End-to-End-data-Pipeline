# Running the lakehouse on AWS

Everything in this folder talks to **real AWS** (the rest of the repo runs locally on LocalStack).

| Local component | AWS equivalent | Script |
|---|---|---|
| LocalStack S3 | Amazon S3 | set `USE_LOCALSTACK=false` |
| `catalog/crawler_sim.py` | AWS Glue Crawler + Data Catalog | `aws/glue_crawler.py` |
| `catalog/athena_sim.py` (DuckDB) | Amazon Athena | `aws/athena_query.py` |
| Spark jobs in Airflow | AWS Glue jobs / EMR | same PySpark scripts |
| Great Expectations gate | same (runs in the orchestrator) | `quality/validate_silver_gx.py` |

## Steps
1. Create an S3 bucket and upload (or write) the `bronze/`, `silver/`, `gold/` layers.
2. Create an IAM role for Glue (`AWSGlueServiceRole` + read access to the bucket).
3. `export LAKE_BUCKET=... GLUE_ROLE_ARN=... AWS_REGION=...`
4. `python aws/glue_crawler.py` creates the `ecom_lakehouse` database and one crawler per layer.
5. `python aws/athena_query.py` runs the same SQL files used by the local simulation.

## Cost note
Athena bills per data scanned and crawlers per DPU-hour. Delete the crawlers and the
`athena-results/` prefix when you are done experimenting.

> These scripts were written against the boto3 Glue/Athena APIs but not executed in this repo's
> local environment (no AWS account attached). Treat the first run as a test.
