"""
Manual execution of the raw-layer validation (outside the DAG).

Produces machine-readable GX evidence for README section 6.6:
    python /opt/airflow/scripts/run_raw_validation.py                   -> normal run
    python /opt/airflow/scripts/run_raw_validation.py --inject-failure  -> controlled failure

--inject-failure sets popularity = 150 in ONE row of an in-memory copy of Spotify
(the CSV on disk is never modified) to prove that DQ03 (critical) blocks the path.
"""
import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, "/opt/airflow")

import pandas as pd  # noqa: E402
from sqlalchemy import text  # noqa: E402

from src.db import source_engine  # noqa: E402
from src.validation import (  # noqa: E402
    GRAMMY_COLUMNS, SPOTIFY_COLUMNS, CriticalQualityFailure, enforce_severity_policy, run_validation,
)

SPOTIFY_PATH = Path("/opt/airflow/data/raw/spotify_dataset.csv")


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--inject-failure", action="store_true")
    args = parser.parse_args()

    spotify = pd.read_csv(SPOTIFY_PATH, usecols=SPOTIFY_COLUMNS)
    with source_engine().connect() as conn:
        grammy = pd.read_sql(text('SELECT "year", category, artist FROM grammy_awards'), conn)

    if args.inject_failure:
        spotify = spotify.copy()
        spotify.loc[spotify.index[0], "popularity"] = 150

    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_id = f"manual_{stamp}" + ("_injected_failure" if args.inject_failure else "")

    blocked = False
    for name, df in [("spotify_raw", spotify), ("grammy_raw", grammy[GRAMMY_COLUMNS])]:
        summary = run_validation({name: df}, stage=name, run_id=run_id)
        print(json.dumps({k: summary[k] for k in
                          ("stage", "policy_decision", "failed_rules", "objects", "results_file")},
                         indent=2, ensure_ascii=False))
        try:
            enforce_severity_policy(summary)
        except CriticalQualityFailure as exc:
            blocked = True
            print(f"BLOCKED -> {exc}")

    sys.exit(1 if blocked else 0)


if __name__ == "__main__":
    main()
