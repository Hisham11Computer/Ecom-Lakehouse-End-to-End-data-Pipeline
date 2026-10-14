
import os
import time

import boto3

REGION = os.getenv("AWS_REGION", "eu-west-1")
BUCKET = os.environ["LAKE_BUCKET"]
ROLE_ARN = os.environ["GLUE_ROLE_ARN"]
DATABASE = "ecom_lakehouse"

glue = boto3.client("glue", region_name=REGION)


def build_targets(layer: str) -> dict:
    if layer == "silver":
        return {
            "DeltaTargets": [{
                "DeltaTables": [f"s3://{BUCKET}/silver/orders/"],
                "WriteManifest": False,
                "CreateNativeDeltaTable": True,
            }]
        }
    return {
        "S3Targets": [{
            "Path": f"s3://{BUCKET}/{layer}/",
            "Exclusions": ["**/_spark_metadata/**", "**/_SUCCESS"],
        }]
    }


def ensure_database() -> None:
    try:
        glue.create_database(DatabaseInput={"Name": DATABASE})
        print(f"created database {DATABASE}")
    except glue.exceptions.AlreadyExistsException:
        print(f"database {DATABASE} already exists")


def ensure_crawler(layer: str) -> str:
    name = f"ecom-{layer}-crawler"
    config = dict(
        Name=name,
        Role=ROLE_ARN,
        DatabaseName=DATABASE,
        TablePrefix=f"{layer}_",  
        Targets=build_targets(layer),
        SchemaChangePolicy={"UpdateBehavior": "UPDATE_IN_DATABASE", "DeleteBehavior": "LOG"},
    )
    try:
        glue.create_crawler(**config)
        print(f"created crawler {name}")
    except glue.exceptions.AlreadyExistsException:
        glue.update_crawler(**config)
        print(f"updated crawler {name}")
    return name


def run_and_wait(name: str) -> None:
    glue.start_crawler(Name=name)
    print(f"started {name}")
    while glue.get_crawler(Name=name)["Crawler"]["State"] != "READY":
        time.sleep(10)
    last = glue.get_crawler(Name=name)["Crawler"].get("LastCrawl", {})
    print(f"{name} finished: {last.get('Status')}")


if __name__ == "__main__":
    ensure_database()
    for crawler_name in [ensure_crawler(layer) for layer in ("bronze", "silver", "gold")]:
        run_and_wait(crawler_name)
