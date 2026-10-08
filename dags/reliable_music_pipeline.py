"""
Resonance Records - reliable batch pipeline (Airflow 3.1.8, TaskFlow API).

Current scope (README sections 6.7 and 6.8):

    extract_spotify -> validate_spotify_raw --\
                                               >-- transform_and_integrate
    extract_grammys -> validate_grammys_raw --/

transform_and_integrate runs only when BOTH raw gates let their batch continue, and it reads
exactly the batches that were validated.

The DAG only orchestrates. Business logic lives in src/ (extract.py, validation.py).
Heavy libraries (pandas, Great Expectations) are imported inside tasks so the DAG file
parses fast in the dag-processor.
"""
from __future__ import annotations

import logging

import pendulum
from airflow.exceptions import AirflowFailException
from airflow.sdk import Param, dag, get_current_context, task

log = logging.getLogger(__name__)

DATA_ROOT = "/opt/airflow/data"
DEFAULT_SPOTIFY_SOURCE = f"{DATA_ROOT}/raw/spotify_dataset.csv"


def _raw_gate(extract_result: dict) -> dict:
    """Run the raw validation gate; a critical failure fails the task WITHOUT retries."""
    from src.validation import CriticalQualityFailure, validate_raw_batch

    run_id = get_current_context()["run_id"]
    try:
        summary = validate_raw_batch(extract_result, run_id)
    except CriticalQualityFailure as exc:
        # Deterministic data problem: retrying would fail again, so fail immediately.
        raise AirflowFailException(str(exc)) from exc

    log.info("Decision: %s | failed rules: %s | evidence: %s",
             summary["policy_decision"], summary["failed_rules"], summary["results_file"])
    out = {k: summary[k] for k in ("stage", "policy_decision", "failed_rules", "results_file")}
    out["batch"] = extract_result  # the validated batch travels to the transformation
    return out


@dag(
    dag_id="reliable_music_pipeline",
    description="Spotify (CSV) + Grammy (PostgreSQL) -> GX validation -> music_dw",
    schedule=None,
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 0},
    params={
        "spotify_source": Param(
            DEFAULT_SPOTIFY_SOURCE,
            type="string",
            description="Spotify CSV to extract. Change it only for controlled-failure tests.",
        ),
    },
    tags=["etl", "great-expectations", "resonance-records"],
)
def reliable_music_pipeline():

    # ------------------------------------------------------------------ Spotify branch
    @task(task_id="extract_spotify", retries=2, retry_delay=pendulum.duration(seconds=30))
    def extract_spotify_task() -> dict:
        from src.extract import extract_spotify

        context = get_current_context()
        source = context["params"]["spotify_source"]
        if not source.startswith(f"{DATA_ROOT}/"):
            raise AirflowFailException(f"spotify_source must be inside {DATA_ROOT}: {source}")
        return extract_spotify(context["run_id"], source)

    @task(task_id="validate_spotify_raw")
    def validate_spotify_raw(extract_result: dict) -> dict:
        return _raw_gate(extract_result)

    # ------------------------------------------------------------------ Grammy branch
    @task(task_id="extract_grammys", retries=2, retry_delay=pendulum.duration(seconds=30))
    def extract_grammys_task() -> dict:
        from src.extract import extract_grammys

        return extract_grammys(get_current_context()["run_id"])

    @task(task_id="validate_grammys_raw")
    def validate_grammys_raw(extract_result: dict) -> dict:
        return _raw_gate(extract_result)

    # ------------------------------------------------------------------ transformation
    @task(task_id="transform_and_integrate")
    def transform_and_integrate(spotify_gate: dict, grammy_gate: dict) -> dict:
        """Single transformation phase: cleaning, harmonization, integration, segments, keys."""
        from src.transform import run_transform

        log.info("Raw gate decisions: spotify=%s grammy=%s",
                 spotify_gate["policy_decision"], grammy_gate["policy_decision"])
        result = run_transform(spotify_gate["batch"], grammy_gate["batch"],
                               get_current_context()["run_id"])
        log.info("Prepared tables: %s | metrics: %s", result["table_rows"], result["metrics_path"])
        return result

    spotify_ok = validate_spotify_raw(extract_spotify_task())
    grammy_ok = validate_grammys_raw(extract_grammys_task())
    transform_and_integrate(spotify_ok, grammy_ok)


reliable_music_pipeline()