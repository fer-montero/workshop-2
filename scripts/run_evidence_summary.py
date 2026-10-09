"""
Summarize all the evidence a pipeline run left behind (README section 7).

Usage (from the project root, with the local .venv):
    python scripts/run_evidence_summary.py "manual__2026-10-08T16:35:48.407076+00:00"

It reads, for that run_id:
    data/work/<run>/*_metadata.json           extraction (rows, source)
    docs/evidence/gx/<run>/*.json             GX decision per stage
    docs/evidence/transform/<run>/transform_metrics.json
    docs/evidence/load/<run>/load_summary.json
prints a stage-by-stage summary and writes it to docs/evidence/runs/<run>_summary.json.
Missing files are reported as "not reached" (e.g. stages blocked by an earlier failure).
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def safe_run_id(run_id: str) -> str:
    return re.sub(r"[^A-Za-z0-9_.-]", "_", run_id)


def load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None


def main(run_id: str) -> None:
    safe = safe_run_id(run_id)
    summary = {"run_id": run_id, "stages": {}}

    # 1. Extraction
    task_names = {"spotify_raw": ("extract_spotify", "validate_spotify_raw"),
                  "grammy_raw": ("extract_grammys", "validate_grammys_raw")}
    for name, (extract_task, _) in task_names.items():
        meta = load_json(ROOT / "data" / "work" / safe / f"{name}_metadata.json")
        summary["stages"][extract_task] = (
            {"status": "OK", "rows": meta["rows"], "source": meta["source"],
             "extracted_at_utc": meta["extracted_at_utc"]} if meta else {"status": "not reached / missing"})

    # 2. GX gates (raw + prepared)
    gates = {"spotify_raw": "validate_spotify_raw", "grammy_raw": "validate_grammys_raw",
             "prepared": "validate_prepared"}
    for stage, task_name in gates.items():
        gx = load_json(ROOT / "docs" / "evidence" / "gx" / safe / f"{stage}.json")
        if gx is None:
            summary["stages"][task_name] = {"status": "not reached / missing"}
            continue
        summary["stages"][task_name] = {
            "status": gx["policy_decision"],
            "failed_rules": gx["failed_rules"],
            "rows_validated": {k: v["rows_validated"] for k, v in gx["objects"].items()},
            "evidence": f"docs/evidence/gx/{safe}/{stage}.json",
        }

    # 3. Transformation
    tm = load_json(ROOT / "docs" / "evidence" / "transform" / safe / "transform_metrics.json")
    summary["stages"]["transform_and_integrate"] = (
        {"status": "OK", "table_rows": tm["table_rows"],
         "awards_reconcile_with_source": tm["awards_reconcile_with_source"],
         "evidence": f"docs/evidence/transform/{safe}/transform_metrics.json"}
        if tm else {"status": "not reached / missing"})

    # 4. Load
    ls = load_json(ROOT / "docs" / "evidence" / "load" / safe / "load_summary.json")
    if ls is None:
        summary["stages"]["load_dw"] = {"status": "not reached / missing"}
    else:
        entry = {"status": ls["status"], "evidence": f"docs/evidence/load/{safe}/load_summary.json",
                 "before": ls.get("before"), "after": ls.get("after") or ls.get("after_rollback")}
        if ls["status"] == "COMMITTED":
            entry.update(rows_loaded=ls["rows_loaded"], reconciled=ls["reconciled"],
                         duration_seconds=ls["duration_seconds"],
                         rerun_identical=(ls.get("before") == ls.get("after")))
        else:
            entry.update(error=ls.get("error"), unchanged=ls.get("unchanged"))
        summary["stages"]["load_dw"] = entry

    order = ["extract_spotify", "extract_grammys", "validate_spotify_raw", "validate_grammys_raw",
             "transform_and_integrate", "validate_prepared", "load_dw"]
    summary["stages"] = {k: summary["stages"][k] for k in order}

    out_dir = ROOT / "docs" / "evidence" / "runs"
    out_dir.mkdir(parents=True, exist_ok=True)
    out = out_dir / f"{safe}_summary.json"
    out.write_text(json.dumps(summary, indent=2, ensure_ascii=False), encoding="utf-8")

    print(f"Run: {run_id}")
    for stage, info in summary["stages"].items():
        extra = ""
        if "failed_rules" in info:
            failed = ", ".join(f"{k}={v}" for k, v in info["failed_rules"].items() if v)
            extra = f" | failed: {failed}" if failed else ""
        if "rows" in info:
            extra = f" | rows={info['rows']}"
        if "awards_reconcile_with_source" in info:
            extra = f" | reconcile={info['awards_reconcile_with_source']}"
        if stage == "load_dw" and info["status"] == "COMMITTED":
            extra = f" | reconciled={info['reconciled']} | {info['duration_seconds']}s | rerun_identical={info['rerun_identical']}"
        if stage == "load_dw" and info["status"] == "ROLLED_BACK":
            extra = f" | unchanged={info['unchanged']}"
        print(f"  {stage:<26} {info['status']}{extra}")
    print(f"Saved: {out.relative_to(ROOT)}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit('Usage: python scripts/run_evidence_summary.py "<run_id>"')
    main(sys.argv[1])