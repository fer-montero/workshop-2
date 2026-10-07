"""
Extraction stage (README section 6.7).

Principles:
- Extract WITHOUT cleaning: no column dropping, type fixing, deduplication or null handling.
  Source-quality problems must reach the raw validation gate untouched.
- Each run writes its own raw working files under data/work/<run_id>/ so the evaluated
  batch can be investigated later.
- Functions return a small dict with file paths and metadata (safe for XCom),
  never the full DataFrame.
"""
from __future__ import annotations

import hashlib
import json
import os
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
from sqlalchemy import text

from src.db import source_engine

DATA_DIR = Path(os.environ.get("PIPELINE_DATA_DIR", "/opt/airflow/data"))
SPOTIFY_SOURCE = DATA_DIR / "raw" / "spotify_dataset.csv"
GRAMMY_TABLE = "grammy_awards"


def safe_run_id(run_id: str) -> str:
    """Airflow run ids contain ':' and '+', which are invalid in Windows folder names."""
    return re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)


def run_work_dir(run_id: str) -> Path:
    path = DATA_DIR / "work" / safe_run_id(run_id)
    path.mkdir(parents=True, exist_ok=True)
    return path


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(1 << 20), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_outputs(df: pd.DataFrame, name: str, run_id: str, source: dict) -> dict:
    """Persist the raw batch and its metadata; return a small XCom-safe summary."""
    work_dir = run_work_dir(run_id)
    data_path = work_dir / f"{name}.csv"
    meta_path = work_dir / f"{name}_metadata.json"

    df.to_csv(data_path, index=False, encoding="utf-8")
    metadata = {
        "dataset": name,
        "run_id": run_id,
        "extracted_at_utc": datetime.now(timezone.utc).isoformat(),
        "source": source,
        "rows": int(len(df)),
        "columns": list(df.columns),
        "null_counts": {c: int(v) for c, v in df.isna().sum().items()},
        "output_file": str(data_path),
    }
    meta_path.write_text(json.dumps(metadata, indent=2, ensure_ascii=False), encoding="utf-8")
    return {"dataset": name, "path": str(data_path), "metadata_path": str(meta_path),
            "rows": metadata["rows"]}


def extract_spotify(run_id: str, source_path: Path | str = SPOTIFY_SOURCE) -> dict:
    """Read the Spotify CSV exactly as delivered (all columns, default parsing)."""
    source_path = Path(source_path)
    if not source_path.exists():
        raise FileNotFoundError(f"Spotify source not found: {source_path}")

    df = pd.read_csv(source_path)
    if df.shape[1] == 1:
        raise ValueError("Spotify CSV parsed into a single column: unexpected delimiter.")

    source = {"type": "csv", "path": str(source_path), "sha256": _sha256(source_path)}
    return _write_outputs(df, "spotify_raw", run_id, source)


def extract_grammys(run_id: str) -> dict:
    """Read the operational Grammy table from PostgreSQL (grammy_source), unchanged."""
    query = text(f"SELECT * FROM {GRAMMY_TABLE} ORDER BY source_row_number")
    with source_engine().connect() as conn:
        df = pd.read_sql(query, conn)
        database = conn.execute(text("SELECT current_database()")).scalar()

    source = {"type": "postgresql", "database": database, "table": GRAMMY_TABLE}
    return _write_outputs(df, "grammy_raw", run_id, source)


def read_raw(extract_result: dict) -> pd.DataFrame:
    """Load a raw working file produced by extract_spotify / extract_grammys."""
    return pd.read_csv(extract_result["path"])