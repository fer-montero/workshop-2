"""
Export the Expectation Suites as JSON (gx/expectations/) and the
Expectation -> Rule ID map (docs/evidence/gx/expectation_rule_map.csv).

    python /opt/airflow/scripts/export_gx_suites.py

dim_genre_suite is exported once the genre family catalog exists
(it is defined in the transformation stage).
"""
import sys

sys.path.insert(0, "/opt/airflow")

from src.validation import export_suites  # noqa: E402

try:
    from src.transform import GENRE_FAMILIES  # defined in the transformation stage
except ImportError:
    GENRE_FAMILIES = None

objects = ["spotify_raw", "grammy_raw", "fact_track", "fact_award", "dim_artist"]
if GENRE_FAMILIES:
    objects.append("dim_genre")

for path in export_suites(objects, genre_families=GENRE_FAMILIES):
    print("-> written", path)
if not GENRE_FAMILIES:
    print("dim_genre_suite pending: GENRE_FAMILIES is not defined yet in src/transform.py")