"""
Grammy source preparation (NOT the ETL Load stage).

Loads the provided Grammy CSV into grammy_source.grammy_awards and
reconciles the result against the CSV. Safe to rerun: the table content
is fully replaced inside one transaction.
"""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, "/opt/airflow")

import pandas as pd
from sqlalchemy import text

from src.db import source_engine

CSV_PATH = Path("/opt/airflow/data/raw/the_grammy_awards.csv")
DDL_PATH = Path("/opt/airflow/sql/source_setup.sql")
EVIDENCE_PATH = Path("/opt/airflow/docs/evidence/source_reconciliation.json")
TABLE = "grammy_awards"
EXPECTED_COLUMNS = [
    "year", "title", "published_at", "updated_at", "category",
    "nominee", "artist", "workers", "img", "winner",
]


def read_csv():
    # Only truly empty fields become NULL; strings like "None" or "NA" are preserved.
    df = pd.read_csv(CSV_PATH, dtype=str, keep_default_na=False, na_values=[""])
    if list(df.columns) != EXPECTED_COLUMNS:
        raise ValueError(f"Unexpected CSV columns: {list(df.columns)}")
    df.insert(0, "source_row_number", range(1, len(df) + 1))
    return df


def run_ddl(conn):
    sql = DDL_PATH.read_text(encoding="utf-8")
    for statement in [s.strip() for s in sql.split(";") if s.strip()]:
        conn.exec_driver_sql(statement)


def main():
    df = read_csv()
    records = df.astype(object).where(df.notna(), None).to_dict("records")

    columns = ["source_row_number"] + EXPECTED_COLUMNS
    col_sql = ", ".join(f'"{c}"' for c in columns)
    val_sql = ", ".join(f":{c}" for c in columns)

    engine = source_engine()
    with engine.begin() as conn:
        run_ddl(conn)
    with engine.begin() as conn:  # atomic replace
        conn.execute(text(f"DELETE FROM {TABLE}"))
        conn.execute(text(f"INSERT INTO {TABLE} ({col_sql}) VALUES ({val_sql})"), records)

    # Reconciliation: CSV vs table
    with engine.connect() as conn:
        db_rows = conn.execute(text(f"SELECT COUNT(*) FROM {TABLE}")).scalar()
        db_nulls = {
            c: conn.execute(text(f'SELECT COUNT(*) - COUNT("{c}") FROM {TABLE}')).scalar()
            for c in EXPECTED_COLUMNS
        }
        db_year_range = conn.execute(text(f'SELECT MIN("year"), MAX("year") FROM {TABLE}')).one()

    csv_nulls = {c: int(df[c].isna().sum()) for c in EXPECTED_COLUMNS}
    csv_years = pd.to_numeric(df["year"])
    checks = {
        "row_count_match": len(df) == db_rows,
        "null_counts_match": all(csv_nulls[c] == int(db_nulls[c]) for c in EXPECTED_COLUMNS),
        "year_range_match": (int(csv_years.min()), int(csv_years.max())) == tuple(db_year_range),
    }
    evidence = {
        "executed_at_utc": datetime.now(timezone.utc).isoformat(),
        "source_file": str(CSV_PATH),
        "target_table": f"grammy_source.public.{TABLE}",
        "csv_rows": len(df),
        "db_rows": db_rows,
        "csv_null_counts": csv_nulls,
        "db_null_counts": {c: int(v) for c, v in db_nulls.items()},
        "csv_year_range": [int(csv_years.min()), int(csv_years.max())],
        "db_year_range": list(db_year_range),
        "checks": checks,
        "reconciled": all(checks.values()),
    }
    EVIDENCE_PATH.parent.mkdir(parents=True, exist_ok=True)
    EVIDENCE_PATH.write_text(json.dumps(evidence, indent=2), encoding="utf-8")
    print(json.dumps(evidence, indent=2))

    if not evidence["reconciled"]:
        raise SystemExit("Reconciliation FAILED. See docs/evidence/source_reconciliation.json")
    print("Source preparation completed and reconciled.")


if __name__ == "__main__":
    main()