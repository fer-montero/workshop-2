"""
Evidence to choose the segment thresholds (README section 6.8, step 5).

    python /opt/airflow/scripts/segment_thresholds.py

Uses the same transformation code as the pipeline. Prints and saves:
  - distribution of Grammy awards among artists present in BOTH sources
  - popularity percentiles for the average AND the max track popularity
    (artists in both sources, and the whole Spotify catalog as reference)
  - thresholds proposed by the quartile rule and the segment sizes they produce
  - how many candidates other threshold combinations would produce (sensitivity)
Output: docs/evidence/integration/segment_threshold_analysis.json
"""
import json
import sys
from pathlib import Path

sys.path.insert(0, "/opt/airflow")

import pandas as pd  # noqa: E402

from src.extract import extract_grammys, extract_spotify, read_raw  # noqa: E402
from src.transform import UNKNOWN_ARTIST_KEY, integrate, prepare_grammys, prepare_spotify  # noqa: E402

OUT = Path("/opt/airflow/docs/evidence/integration/segment_threshold_analysis.json")
RUN_ID = "analisis_umbrales"
PCTS = [0.10, 0.25, 0.50, 0.75, 0.90]

spotify = prepare_spotify(read_raw(extract_spotify(RUN_ID)))
grammy = prepare_grammys(read_raw(extract_grammys(RUN_ID)))
dim = integrate(spotify, grammy)["dim_artist"]
dim = dim[dim["artist_key"] != UNKNOWN_ARTIST_KEY]

both = dim[dim["in_grammy"] & dim["in_spotify"]]
catalog = dim[dim["in_spotify"]]


def percentiles(series):
    return {f"p{int(p * 100)}": round(float(series.quantile(p)), 1) for p in PCTS}


awards_bins = pd.cut(both["total_awards"], [0, 1, 2, 3, 4, 9, 1000],
                     labels=["1", "2", "3", "4", "5-9", "10+"]).value_counts().sort_index()

# Quartile rule (same rule approved by the team, applied to the chosen measure)
min_awards = int(both["total_awards"].quantile(0.75))
low = float(catalog["max_popularity"].quantile(0.25))
high = float(catalog["max_popularity"].quantile(0.50))
top = both["total_awards"] >= min_awards
proposal = {
    "measure": "max_popularity",
    "min_awards (p75 awards, both sources)": min_awards,
    "candidate_below (p25 max popularity, catalog)": low,
    "consolidated_from (p50 max popularity, catalog)": high,
    "candidates": int((top & (both["max_popularity"] < low)).sum()),
    "consolidated": int((top & (both["max_popularity"] >= high)).sum()),
    "middle_band": int((top & both["max_popularity"].between(low, high, inclusive="left")).sum()),
    "candidates_with_all_tracks_at_zero": int((top & (both["max_popularity"] == 0)).sum()),
}

sensitivity = []
for n_awards in (2, 3, 5):
    for below in (15, 20, 25, 30):
        mask = (both["total_awards"] >= n_awards) & (both["max_popularity"] < below)
        sensitivity.append({"min_awards": n_awards, "max_popularity_below": below,
                            "candidates": int(mask.sum())})

report = {
    "artists_in_both_sources": len(both),
    "awards_distribution_both": {str(k): int(v) for k, v in awards_bins.items()},
    "awards_percentiles_both": percentiles(both["total_awards"]),
    "avg_popularity_percentiles": {"both": percentiles(both["avg_popularity"]),
                                   "catalog": percentiles(catalog["avg_popularity"])},
    "max_popularity_percentiles": {"both": percentiles(both["max_popularity"]),
                                   "catalog": percentiles(catalog["max_popularity"])},
    "quartile_rule_proposal": proposal,
    "sensitivity_max_popularity": sensitivity,
}
OUT.parent.mkdir(parents=True, exist_ok=True)
OUT.write_text(json.dumps(report, indent=2, ensure_ascii=False), encoding="utf-8")
print(json.dumps(report, indent=2, ensure_ascii=False))
print(f"-> saved {OUT}")