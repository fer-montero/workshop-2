"""
Great Expectations validation layer for the Resonance Records pipeline.

Design (README section 6.6):
- Expectation           -> one check that implements a declared quality rule (Rule ID in meta).
- Expectation Suite     -> group of checks for ONE data object (one suite per object).
- Validation Definition -> explicit link "this data object + this suite".
- Checkpoint            -> controlled execution; results are persisted as JSON evidence
                           and the severity policy is applied afterwards.

Data objects and suites:
    raw layer       spotify_raw  -> spotify_raw_suite   (DQ01-DQ08)
                    grammy_raw   -> grammy_raw_suite    (DQ09-DQ13)
    prepared layer  fact_track   -> fact_track_suite    (DQ14)
                    dim_artist   -> dim_artist_suite    (DQ15, DQ17, DQ18)
                    dim_genre    -> dim_genre_suite     (DQ16)

Severity policy:
    critical -> blocks the downstream path (CriticalQualityFailure is raised)
    warning  -> recorded and exposed; the pipeline continues
    info     -> recorded for monitoring; never blocks
"""
from __future__ import annotations

import csv
import json
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

import great_expectations as gx
import great_expectations.expectations as gxe

log = logging.getLogger(__name__)

RESULTS_DIR = Path(os.environ.get("GX_RESULTS_DIR", "/opt/airflow/docs/evidence/gx"))
SUITES_DIR = Path(os.environ.get("GX_SUITES_DIR", "/opt/airflow/gx/expectations"))

CRITICAL, WARNING, INFO = "critical", "warning", "info"

SPOTIFY_COLUMNS = [
    "track_id", "artists", "popularity", "explicit", "track_genre",
    "energy", "valence", "danceability", "acousticness",
]
GRAMMY_COLUMNS = ["year", "category", "artist"]
AUDIO_FEATURES = ["energy", "valence", "danceability", "acousticness"]
SEGMENTS = ["candidato", "consolidado", "no aplica"]
UNKNOWN_ARTIST_KEY = 0


class CriticalQualityFailure(Exception):
    """Raised when at least one critical rule fails. The downstream path must stop."""


def _exp(expectation_cls, rule_id: str, severity: str, **kwargs):
    """Build an Expectation tagged with its Rule ID and severity."""
    return expectation_cls(severity=severity, meta={"rule_id": rule_id}, **kwargs)


# ---------------------------------------------------------------------------
# Expectation Suites (one builder per data object)
# ---------------------------------------------------------------------------
def spotify_raw_expectations(**_):
    exps = [_exp(gxe.ExpectColumnToExist, "DQ01", CRITICAL, column=c) for c in SPOTIFY_COLUMNS]
    exps += [
        _exp(gxe.ExpectTableRowCountToBeBetween, "DQ01", CRITICAL, min_value=1),
        _exp(gxe.ExpectColumnValuesToNotBeNull, "DQ02", CRITICAL, column="track_id"),
        _exp(gxe.ExpectColumnValuesToNotBeNull, "DQ03", CRITICAL, column="popularity"),
        _exp(gxe.ExpectColumnValuesToBeBetween, "DQ03", CRITICAL, column="popularity",
             min_value=0, max_value=100),
    ]
    exps += [
        _exp(gxe.ExpectColumnValuesToBeBetween, "DQ04", CRITICAL, column=c, min_value=0, max_value=1)
        for c in AUDIO_FEATURES
    ]
    exps += [
        _exp(gxe.ExpectColumnValuesToBeInSet, "DQ05", CRITICAL, column="explicit",
             value_set=[True, False]),
        _exp(gxe.ExpectColumnValuesToNotBeNull, "DQ06", CRITICAL, column="track_genre"),
        _exp(gxe.ExpectColumnValuesToNotBeNull, "DQ07", WARNING, column="artists", mostly=0.99),
        _exp(gxe.ExpectColumnValuesToBeBetween, "DQ08", INFO, column="popularity",
             min_value=1, max_value=100, mostly=0.80),
    ]
    return exps


def grammy_raw_expectations(current_year: int | None = None, **_):
    current_year = current_year or datetime.now(timezone.utc).year
    exps = [_exp(gxe.ExpectColumnToExist, "DQ09", CRITICAL, column=c) for c in GRAMMY_COLUMNS]
    exps += [
        _exp(gxe.ExpectTableRowCountToBeBetween, "DQ09", CRITICAL, min_value=1),
        _exp(gxe.ExpectColumnValuesToNotBeNull, "DQ10", CRITICAL, column="year"),
        _exp(gxe.ExpectColumnValuesToBeBetween, "DQ10", CRITICAL, column="year",
             min_value=1958, max_value=current_year),
        _exp(gxe.ExpectColumnValuesToNotBeNull, "DQ11", CRITICAL, column="category"),
        _exp(gxe.ExpectColumnValuesToNotBeNull, "DQ12", WARNING, column="artist", mostly=0.60),
        _exp(gxe.ExpectCompoundColumnsToBeUnique, "DQ13", INFO, column_list=GRAMMY_COLUMNS,
             ignore_row_if="any_value_is_missing"),
    ]
    return exps


def fact_track_expectations(**_):
    return [
        _exp(gxe.ExpectColumnValuesToNotBeNull, "DQ14", CRITICAL, column="track_id"),
        _exp(gxe.ExpectColumnValuesToBeUnique, "DQ14", CRITICAL, column="track_id"),
    ]


def dim_artist_expectations(**_):
    return [
        _exp(gxe.ExpectColumnValuesToNotBeNull, "DQ15", CRITICAL, column="artist_norm"),
        _exp(gxe.ExpectColumnValuesToBeUnique, "DQ15", CRITICAL, column="artist_norm"),
        # Normalized: no upper-case letters and no leading/trailing spaces
        _exp(gxe.ExpectColumnValuesToMatchRegex, "DQ15", CRITICAL, column="artist_norm",
             regex=r"^[^\sA-Z](?:[^A-Z]*[^\sA-Z])?$"),
        # Normalized: no repeated inner spaces
        _exp(gxe.ExpectColumnValuesToNotMatchRegex, "DQ15", CRITICAL, column="artist_norm",
             regex=r"\s{2,}"),
        _exp(gxe.ExpectColumnValuesToBeInSet, "DQ17", CRITICAL, column="segment", value_set=SEGMENTS),
        # Share of Grammy artists (excluding the "Desconocido" member) also present in Spotify
        _exp(gxe.ExpectColumnMeanToBeBetween, "DQ18", WARNING, column="in_spotify",
             min_value=0.25, max_value=1.0, condition_parser="pandas",
             row_condition=f"in_grammy == True and artist_key != {UNKNOWN_ARTIST_KEY}"),
    ]


def dim_genre_expectations(genre_families: list[str] | None = None, **_):
    if not genre_families:
        raise ValueError("dim_genre_suite needs the genre family catalog (genre_families=...).")
    return [
        _exp(gxe.ExpectColumnValuesToNotBeNull, "DQ16", CRITICAL, column="genre_family"),
        _exp(gxe.ExpectColumnValuesToBeInSet, "DQ16", CRITICAL, column="genre_family",
             value_set=list(genre_families)),
    ]


# data object -> (suite name, layer, suite builder)
VALIDATION_TARGETS = {
    "spotify_raw": ("spotify_raw_suite", "raw", spotify_raw_expectations),
    "grammy_raw": ("grammy_raw_suite", "raw", grammy_raw_expectations),
    "fact_track": ("fact_track_suite", "prepared", fact_track_expectations),
    "dim_artist": ("dim_artist_suite", "prepared", dim_artist_expectations),
    "dim_genre": ("dim_genre_suite", "prepared", dim_genre_expectations),
}


# ---------------------------------------------------------------------------
# Execution
# ---------------------------------------------------------------------------
def _new_context():
    context = gx.get_context(mode="ephemeral")
    try:  # silence progress bars in Airflow logs
        from great_expectations.data_context.types.base import ProgressBarsConfig
        context.variables.progress_bars = ProgressBarsConfig(globally=False)
    except Exception:  # noqa: BLE001 - cosmetic only
        pass
    return context


def _jsonable(value):
    """Convert numpy / pandas scalars and containers into plain JSON types."""
    if isinstance(value, dict):
        return {str(k): _jsonable(v) for k, v in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_jsonable(v) for v in value]
    if hasattr(value, "item"):
        try:
            return value.item()
        except (ValueError, TypeError):
            pass
    if isinstance(value, (str, int, float, bool)) or value is None:
        return value
    return str(value)


def _rule_record(data_object: str, suite_name: str, res) -> dict:
    cfg = res.expectation_config
    kwargs = dict(cfg.kwargs)
    kwargs.pop("batch_id", None)
    result = res.result or {}
    severity = getattr(cfg.severity, "value", cfg.severity) or CRITICAL
    return _jsonable({
        "rule_id": (cfg.meta or {}).get("rule_id", "UNMAPPED"),
        "data_object": data_object,
        "suite": suite_name,
        "expectation": cfg.type,
        "kwargs": kwargs,
        "severity": severity,
        "success": bool(res.success),
        "observed_value": result.get("observed_value"),
        "element_count": result.get("element_count"),
        "unexpected_count": result.get("unexpected_count"),
        "unexpected_percent": result.get("unexpected_percent"),
        "partial_unexpected_list": (result.get("partial_unexpected_list") or [])[:10],
        "raised_exception": bool((res.exception_info or {}).get("raised_exception")),
    })


def run_validation(data_objects: dict, stage: str, run_id: str, **suite_kwargs) -> dict:
    """
    Validate one or more data objects with their suites and persist the evidence.

    data_objects: {"spotify_raw": dataframe, ...} using names from VALIDATION_TARGETS.
    Returns a small JSON-serializable summary (safe to pass through XCom).
    Does NOT raise on failures: call enforce_severity_policy() to apply the policy.
    """
    context = _new_context()
    records, objects = [], {}

    for name, dataframe in data_objects.items():
        suite_name, layer, builder = VALIDATION_TARGETS[name]

        data_source = context.data_sources.add_pandas(name=f"{name}_source")
        data_asset = data_source.add_dataframe_asset(name=f"{name}_asset")
        batch_definition = data_asset.add_batch_definition_whole_dataframe(f"{name}_batch")

        suite = context.suites.add(gx.ExpectationSuite(name=suite_name))
        for expectation in builder(**suite_kwargs):
            suite.add_expectation(expectation)

        validation_definition = context.validation_definitions.add(
            gx.ValidationDefinition(name=f"{name}_validation", data=batch_definition, suite=suite)
        )
        checkpoint = context.checkpoints.add(
            gx.Checkpoint(
                name=f"{name}_checkpoint",
                validation_definitions=[validation_definition],
                result_format={"result_format": "SUMMARY"},
            )
        )

        checkpoint_result = checkpoint.run(batch_parameters={"dataframe": dataframe})
        suite_result = next(iter(checkpoint_result.run_results.values()))

        object_records = [_rule_record(name, suite_name, r) for r in suite_result.results]
        records.extend(object_records)
        objects[name] = {
            "suite": suite_name,
            "layer": layer,
            "rows_validated": int(len(dataframe)),
            "success": bool(checkpoint_result.success),
            "statistics": _jsonable(suite_result.statistics),
        }

    failed = {sev: sorted({r["rule_id"] for r in records if not r["success"] and r["severity"] == sev})
              for sev in (CRITICAL, WARNING, INFO)}
    if failed[CRITICAL]:
        decision = "BLOCKED"
    elif failed[WARNING] or failed[INFO]:
        decision = "PASS_WITH_FINDINGS"
    else:
        decision = "PASS"

    out_dir = RESULTS_DIR / run_id
    out_dir.mkdir(parents=True, exist_ok=True)
    results_file = out_dir / f"{stage}.json"

    summary = {
        "run_id": run_id,
        "stage": stage,
        "executed_at_utc": datetime.now(timezone.utc).isoformat(),
        "overall_success": all(o["success"] for o in objects.values()),
        "policy_decision": decision,
        "failed_rules": failed,
        "objects": objects,
        "results_file": str(results_file),
    }
    results_file.write_text(
        json.dumps({**summary, "rule_results": records}, indent=2, ensure_ascii=False),
        encoding="utf-8",
    )
    log.info("GX %s -> %s | failed rules: %s | evidence: %s", stage, decision, failed, results_file)
    return summary


def enforce_severity_policy(summary: dict) -> None:
    """Apply the severity policy: critical failures block, warning/info are only reported."""
    failed = summary["failed_rules"]
    if failed[WARNING]:
        log.warning("Warning rules failed (pipeline continues): %s", failed[WARNING])
    if failed[INFO]:
        log.info("Informational rules failed (monitoring only): %s", failed[INFO])
    if failed[CRITICAL]:
        raise CriticalQualityFailure(
            f"[{summary['stage']}] critical rules failed: {failed[CRITICAL]}. "
            f"Evidence: {summary['results_file']}"
        )


# ---------------------------------------------------------------------------
# Documentation helpers
# ---------------------------------------------------------------------------
def export_suites(objects: list[str] | None = None, **suite_kwargs) -> list[Path]:
    """
    Write each suite as JSON (gx/expectations/) and the Expectation -> Rule ID map
    (docs/evidence/gx/expectation_rule_map.csv). Suites are versioned project assets.
    """
    context = _new_context()
    SUITES_DIR.mkdir(parents=True, exist_ok=True)
    RESULTS_DIR.mkdir(parents=True, exist_ok=True)
    written, rows = [], []

    for name in objects or list(VALIDATION_TARGETS):
        suite_name, layer, builder = VALIDATION_TARGETS[name]
        suite = context.suites.add(gx.ExpectationSuite(name=suite_name))
        for expectation in builder(**suite_kwargs):
            suite.add_expectation(expectation)
            cfg = expectation.configuration
            kwargs = {k: v for k, v in cfg.kwargs.items() if k != "batch_id"}
            rows.append({
                "rule_id": cfg.meta.get("rule_id"),
                "layer": layer,
                "data_object": name,
                "suite": suite_name,
                "expectation": cfg.type,
                "severity": getattr(cfg.severity, "value", cfg.severity),
                "kwargs": json.dumps(_jsonable(kwargs), ensure_ascii=False),
            })
        path = SUITES_DIR / f"{suite_name}.json"
        path.write_text(json.dumps(_jsonable(suite.to_json_dict()), indent=2, ensure_ascii=False),
                        encoding="utf-8")
        written.append(path)

    map_path = RESULTS_DIR / "expectation_rule_map.csv"
    with map_path.open("w", newline="", encoding="utf-8") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(rows[0].keys()))
        writer.writeheader()
        writer.writerows(rows)
    written.append(map_path)
    return written
