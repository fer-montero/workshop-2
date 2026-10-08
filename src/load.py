"""
Load stage (README section 6.10): validated prepared batch -> music_dw.

Strategy: TRANSACTIONAL TRUNCATE-AND-LOAD (full refresh).
- Both sources are complete snapshots and the surrogate keys are deterministic (rule K1),
  so every run rebuilds the whole model. Rerunning the same batch gives the same result
  (idempotent load), and nothing is duplicated.
- Everything happens inside ONE transaction:
      1. create the schema if it does not exist (dw_schema.sql, idempotent DDL)
      2. TRUNCATE the 8 model tables in a single command (they reference each other)
      3. insert parents before children: dimensions -> fact_track -> fact_award -> bridges
      4. reconcile: rows in the database == rows in the prepared files
      5. write one etl_load_audit row per table
      6. COMMIT
  If ANY step fails (FK, CHECK, UNIQUE, lost connection, reconciliation), PostgreSQL rolls
  back and the warehouse stays exactly as it was before the run: never half loaded.
"""
from __future__ import annotations

import json
import os
import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from src.db import dw_engine
from src.extract import read_csv_exact, safe_run_id

SCHEMA_SQL = Path(os.environ.get("DW_SCHEMA_PATH", "/opt/airflow/sql/dw_schema.sql"))
EVIDENCE_DIR = Path(os.environ.get("LOAD_EVIDENCE_DIR", "/opt/airflow/docs/evidence/load"))

# Rule L1 - load order: every parent is loaded before the tables that reference it.
# fact_track is a fact table, but it is also the PARENT of both bridges.
LOAD_ORDER = [
    "dim_artist", "dim_genre", "dim_category", "dim_year",   # parents of everything
    "fact_track",                                           # no FKs; parent of the bridges
    "fact_award",                                           # -> dim_artist, dim_category, dim_year
    "bridge_track_artist", "bridge_track_genre",            # -> fact_track + dim_artist / dim_genre
]

# Measures compared before and after each load (rerun evidence, Test C).
CONTROL_QUERIES = {
    "total_awards": "SELECT COALESCE(SUM(award_count), 0) FROM fact_award",
    "awards_unknown_artist": "SELECT COUNT(*) FROM fact_award WHERE artist_key = 0",
    "avg_track_popularity": "SELECT ROUND(AVG(popularity)::numeric, 4) FROM fact_track",
    "candidate_artists": "SELECT COUNT(*) FROM dim_artist WHERE segment = 'candidato'",
    "consolidated_artists": "SELECT COUNT(*) FROM dim_artist WHERE segment = 'consolidado'",
}

LOAD_FAULTS = ("none", "fail_after_facts")


class InjectedLoadFailure(RuntimeError):
    """TEST-ONLY: simulated crash in the middle of the load to prove the rollback."""


class LoadDataError(RuntimeError):
    """Deterministic data problem rejected by the database (FK, CHECK, UNIQUE, type): no retry."""


def _db_error(exc: BaseException):
    """Find the original PostgreSQL error in the exception chain (pandas/SQLAlchemy wrap it)."""
    seen = set()
    while exc is not None and id(exc) not in seen:
        seen.add(id(exc))
        orig = getattr(exc, "orig", None)
        if orig is not None and getattr(orig, "pgcode", None):
            return orig
        if getattr(exc, "pgcode", None):
            return exc
        exc = exc.__cause__ or exc.__context__
    return None


def _read_prepared(name: str, path: str) -> pd.DataFrame:
    """Read a prepared table exactly (rule X1) and fix the dtypes CSV cannot keep."""
    df = read_csv_exact(path)
    if name == "dim_artist":
        # Grammy-only artists have no tracks: popularity is NULL, not 0.
        df["max_popularity"] = df["max_popularity"].astype("Int64")
        for col in ("in_grammy", "in_spotify", "popularity_zero"):
            df[col] = df[col].astype(bool)
    return df


def _snapshot(conn) -> dict:
    """Row counts per table + control measures (empty dict if the model does not exist yet)."""
    exists = conn.execute(text("SELECT to_regclass('public.fact_award') IS NOT NULL")).scalar()
    if not exists:
        return {}
    rows = {t: int(conn.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar()) for t in LOAD_ORDER}
    measures = {k: conn.execute(text(q)).scalar() for k, q in CONTROL_QUERIES.items()}
    return {"table_rows": rows,
            "measures": {k: (float(v) if v is not None else None) for k, v in measures.items()}}


def _write_evidence(run_id: str, payload: dict) -> str:
    out_dir = EVIDENCE_DIR / safe_run_id(run_id)
    out_dir.mkdir(parents=True, exist_ok=True)
    path = out_dir / "load_summary.json"
    path.write_text(json.dumps(payload, indent=2, ensure_ascii=False, default=str), encoding="utf-8")
    return str(path)


def load_dw(tables: dict[str, str], run_id: str, fault: str = "none") -> dict:
    """
    Load the validated prepared tables into music_dw in a single transaction.

    tables: {table_name: path to the prepared CSV} (output of validate_prepared).
    fault:  TEST-ONLY. "fail_after_facts" raises after both fact tables are inserted,
            so the evidence shows that the already-inserted rows are rolled back.
    Returns an XCom-safe summary; also written to docs/evidence/load/<run_id>/load_summary.json.
    """
    if fault not in LOAD_FAULTS:
        raise ValueError(f"Unknown load fault: {fault}")
    missing = [t for t in LOAD_ORDER if t not in tables]
    if missing:
        raise ValueError(f"Prepared batch is missing tables: {missing}")

    frames = {name: _read_prepared(name, tables[name]) for name in LOAD_ORDER}
    engine = dw_engine()
    started = time.perf_counter()
    summary = {"run_id": run_id, "strategy": "transactional truncate-and-load",
               "load_order": LOAD_ORDER, "fault": fault,
               "started_at_utc": datetime.now(timezone.utc).isoformat()}

    with engine.connect() as conn:
        summary["before"] = _snapshot(conn)
        conn.rollback()  # close the read-only transaction opened by the snapshot

    try:
        with engine.begin() as conn:                        # BEGIN ... COMMIT / ROLLBACK
            conn.exec_driver_sql(SCHEMA_SQL.read_text(encoding="utf-8"))
            conn.execute(text(f"TRUNCATE TABLE {', '.join(LOAD_ORDER)}"))

            loaded = {}
            for name in LOAD_ORDER:
                frames[name].to_sql(name, conn, if_exists="append", index=False,
                                    method="multi", chunksize=5000)
                loaded[name] = len(frames[name])
                if fault == "fail_after_facts" and name == "fact_award":
                    raise InjectedLoadFailure(
                        "TEST-ONLY injected failure after loading the fact tables")

            # Rule L2 - reconciliation inside the transaction: db rows == prepared rows.
            in_db = {t: int(conn.execute(text(f"SELECT COUNT(*) FROM {t}")).scalar())
                     for t in LOAD_ORDER}
            mismatches = {t: {"prepared": loaded[t], "db": in_db[t]}
                          for t in LOAD_ORDER if loaded[t] != in_db[t]}
            if mismatches:
                # Deterministic: the same batch would mismatch again -> LoadDataError (no retry).
                raise LoadDataError(f"Load reconciliation failed (rule L2): {mismatches}")

            conn.execute(text("INSERT INTO etl_load_audit (run_id, table_name, rows_loaded) "
                              "VALUES (:run_id, :table_name, :rows)"),
                         [{"run_id": run_id, "table_name": t, "rows": n} for t, n in loaded.items()])
    except Exception as exc:
        # The transaction was rolled back: show that the warehouse did not change.
        with engine.connect() as conn:
            summary["after_rollback"] = _snapshot(conn)
        db_err = _db_error(exc)
        # Short PostgreSQL message, without the huge INSERT statement and its values.
        cause = f"{type(db_err).__name__}: {str(db_err).strip()}" if db_err else f"{type(exc).__name__}: {exc}"
        summary.update(status="ROLLED_BACK", error=cause,
                       unchanged=summary["after_rollback"] == summary["before"])
        summary["evidence_file"] = _write_evidence(run_id, summary)
        # SQLSTATE class 22 = data exception, 23 = integrity violation: deterministic, never retried.
        if db_err is not None and str(db_err.pgcode)[:2] in ("22", "23"):
            raise LoadDataError(f"Load rolled back, warehouse unchanged. {cause}") from exc
        raise

    with engine.connect() as conn:
        summary["after"] = _snapshot(conn)
    summary.update(status="COMMITTED", rows_loaded=loaded, reconciled=True,
                   duration_seconds=round(time.perf_counter() - started, 2))
    summary["evidence_file"] = _write_evidence(run_id, summary)
    return {k: summary[k] for k in ("status", "rows_loaded", "duration_seconds", "evidence_file")}