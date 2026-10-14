import duckdb


con = duckdb.connect()

con.execute("INSTALL httpfs; LOAD httpfs;")
con.execute("INSTALL delta; LOAD delta;")
con.execute("""
    CREATE OR REPLACE SECRET localstack (
        TYPE S3,
        KEY_ID 'test',
        SECRET 'test',
        ENDPOINT 'localhost:4566',
        REGION 'us-east-1',
        URL_STYLE 'path',
        USE_SSL false
    );
""")

BUCKET = "s3://ecom-raw-lake"


def show_layer(title: str, source: str) -> None:
    print(f"\n--- {title} ---")
    try:
        count = con.sql(f"SELECT COUNT(*) FROM {source}").fetchone()[0]
    except Exception as e:
        print(f"WARNING: could not read {source}: {e}")
        return

    print(f"Rows: {count}")
    if count == 0:
        print("WARNING: no rows found in this layer.")
    else:
        con.sql(f"SELECT * FROM {source} LIMIT 10").show()


show_layer("BRONZE: Raw CDC Events", f"read_parquet('{BUCKET}/bronze/orders/*.parquet')")
print("\n" + "=" * 60)

show_layer("SILVER: Current Orders (Delta)", f"delta_scan('{BUCKET}/silver/orders')")
print("\n" + "=" * 60)
show_layer("GOLD: Customer Summary", f"read_parquet('{BUCKET}/gold/customer_summary/*.parquet')")
print("\n" + "=" * 60)
show_layer("GOLD: Daily Sales", f"read_parquet('{BUCKET}/gold/daily_sales/*.parquet')")

con.close()
