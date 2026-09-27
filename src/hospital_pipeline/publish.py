"""Publish step: export gold tables from the warehouse to Parquet for serving.

The dashboard never opens the DuckDB warehouse file. It reads an immutable,
atomically swapped Parquet snapshot instead, which:

* avoids DuckDB single-writer lock contention while the pipeline is running
* means a failed run never leaves the dashboard half-updated
* mirrors how a warehouse would publish to a serving layer / object store
"""

from __future__ import annotations

import json
import shutil

from hospital_pipeline import ops
from hospital_pipeline.config import get_settings

GOLD_TABLES = [
    "marts.dim_hospital",
    "marts.dim_hospital_history",
    "marts.dim_measure",
    "marts.fct_measure_scores",
    "marts.fct_patient_experience",
    "marts.mart_hospital_scorecard",
    "marts.mart_state_summary",
    "marts.mart_measure_benchmarks",
    "marts.mart_rating_changes",
]
OPS_TABLES = ["ops.pipeline_runs", "ops.task_runs", "ops.ingest_manifest", "ops.dq_results"]


def _export(tables: list[str], subdir: str) -> dict[str, int]:
    s = get_settings()
    final = s.gold_dir / subdir
    tmp = final.with_name(final.name + ".tmp")
    shutil.rmtree(tmp, ignore_errors=True)
    tmp.mkdir(parents=True)
    counts = {}
    with ops.connect() as con:
        for t in tables:
            name = t.split(".", 1)[1]
            con.execute(f"copy (select * from {t}) to '{tmp / name}.parquet' (format parquet, compression zstd)")
            counts[name] = con.execute(f"select count(*) from {t}").fetchone()[0]
    (tmp / "_published.json").write_text(json.dumps(
        {"published_at": ops.now().isoformat(), "tables": counts}, indent=2))
    old = final.with_name(final.name + ".old")
    shutil.rmtree(old, ignore_errors=True)
    if final.exists():
        final.rename(old)
    tmp.rename(final)
    shutil.rmtree(old, ignore_errors=True)
    return counts


def publish_marts() -> dict[str, int]:
    return _export(GOLD_TABLES, "marts")


def publish_ops() -> dict[str, int]:
    return _export(OPS_TABLES, "ops")
