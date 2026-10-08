"""
Create a controlled bad-data copy of Spotify for the raw-validation failure test.

    python /opt/airflow/scripts/make_bad_spotify.py

Writes data/test/spotify_bad_popularity.csv with popularity = 150 in the first row.
The original data/raw/spotify_dataset.csv is never modified.
Violated rule: DQ03 (popularity between 0 and 100, Critical).
"""
from pathlib import Path

import sys

sys.path.insert(0, "/opt/airflow")

from src.extract import read_csv_exact  # noqa: E402

SOURCE = Path("/opt/airflow/data/raw/spotify_dataset.csv")
TARGET = Path("/opt/airflow/data/test/spotify_bad_popularity.csv")

df = read_csv_exact(SOURCE)
original = df.loc[df.index[0], "popularity"]
df.loc[df.index[0], "popularity"] = 150

TARGET.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(TARGET, index=False)
print(f"Bad copy written: {TARGET}")
print(f"Row 0 track_id={df.loc[df.index[0], 'track_id']}: popularity {original} -> 150 (violates DQ03)")