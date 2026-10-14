
import json
import sys
from pathlib import Path

from common import DATABASE, connect, source_sql

ROOT = Path(__file__).resolve().parent.parent
CATALOG = Path(__file__).with_name("catalog.json")
QUERIES = ROOT / "analytics" / "queries"


def main() -> None:
    if not CATALOG.exists():
        sys.exit("catalog.json not found - run: python catalog/crawler_sim.py")

    catalog = json.loads(CATALOG.read_text())
    con = connect()
    con.execute(f"CREATE SCHEMA IF NOT EXISTS {DATABASE}")

    for table in catalog["tables"]:
        con.execute(
            f"CREATE OR REPLACE VIEW {DATABASE}.{table['name']} AS "
            f"SELECT * FROM {source_sql(table)}"
        )
        print(f"[athena-sim] registered {DATABASE}.{table['name']}")

    files = [Path(sys.argv[1])] if len(sys.argv) > 1 else sorted(QUERIES.glob("*.sql"))
    for sql_file in files:
        print(f"\n=== {sql_file.name} ===")
        query = sql_file.read_text().strip().rstrip(";")
        print(query)
        con.sql(query).show()


if __name__ == "__main__":
    main()
