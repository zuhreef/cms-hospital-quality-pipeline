"""Pipeline steps + a CLI that runs them end to end.

The same step functions are called by the Airflow DAG (one task per step), by
``make run`` locally and by CI - so there is exactly one implementation.

    python -m hospital_pipeline.pipeline run --source sample
    python -m hospital_pipeline.pipeline run --source api
    python -m hospital_pipeline.pipeline extract --dataset hcahps --source api
"""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys

from hospital_pipeline import ops, publish
from hospital_pipeline.config import PROJECT_ROOT, get_settings
from hospital_pipeline.quality import checks

log = logging.getLogger("hospital_pipeline")
DBT_DIR = PROJECT_ROOT / "dbt"


# --------------------------------------------------------------------------- steps
def step_extract(run_id: str, source: str, datasets: list[str] | None = None, force: bool = False) -> dict:
    from hospital_pipeline.ingest.extract import extract_dataset

    names = datasets or list(get_settings().datasets)
    summary = {}
    for name in names:
        with ops.track_task(run_id, f"extract.{name}") as info:
            results = extract_dataset(name, run_id, source=source, force=force)
            info["rows_out"] = sum(r.rows for r in results)
            info["releases"] = {r.release_date: ("skipped" if r.skipped else r.rows) for r in results}
            summary[name] = info["releases"]
    return summary


def step_silver(run_id: str, full_refresh: bool = False) -> list[dict]:
    from hospital_pipeline.transform import bronze_to_silver

    with ops.track_task(run_id, "spark.bronze_to_silver") as info:
        stats = bronze_to_silver.run(full_refresh=full_refresh)
        info["rows_out"] = sum(s["rows_out"] for s in stats)
        info["tables"] = stats
    return stats


def step_quality(run_id: str, silver_stats: list[dict] | None = None) -> list[dict]:
    with ops.track_task(run_id, "dq.silver_checks") as info:
        results = checks.run_checks(silver_stats)
        ops.record_dq(run_id, results)
        info["rows_out"] = len(results)
        info["failed"] = [f"{r['dataset']}.{r['check_name']}" for r in results if not r["passed"]]
        checks.enforce(results)
    return results


def _dbt(*args: str) -> subprocess.CompletedProcess:
    env = os.environ | {"DATA_DIR": str(get_settings().data_dir.resolve())}
    cmd = [os.getenv("DBT_BIN", "dbt"), *args, "--project-dir", str(DBT_DIR), "--profiles-dir", str(DBT_DIR)]
    proc = subprocess.run(cmd, env=env, capture_output=True, text=True)
    print(proc.stdout[-8000:])
    if proc.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd[:2])} failed:\n{proc.stdout[-3000:]}\n{proc.stderr[-2000:]}")
    return proc


def step_dbt(run_id: str, full_refresh: bool = False) -> None:
    """`dbt build` = seeds + models + snapshots + tests (incl. unit tests) in DAG order.

    Runs as a subprocess: dbt needs exclusive access to the DuckDB file, so no
    connection is held by this process meanwhile.
    """
    with ops.track_task(run_id, "dbt.build") as info:
        proc = _dbt("build", *(["--full-refresh"] if full_refresh else []))
        tail = [ln for ln in proc.stdout.splitlines() if "Done. PASS=" in ln]
        info["summary"] = tail[-1].split("Done.", 1)[-1].strip() if tail else ""


def step_publish(run_id: str) -> dict:
    with ops.track_task(run_id, "publish.gold") as info:
        counts = publish.publish_marts()
        info["rows_out"] = sum(counts.values())
        info["tables"] = counts
    return counts


# --------------------------------------------------------------------------- orchestration
def run_all(source: str, trigger: str = "cli", force: bool = False, full_refresh: bool = False) -> str:
    run_id = ops.new_run_id()
    ops.start_run(run_id, source, trigger)
    log.info("run %s started (source=%s)", run_id, source)
    try:
        step_extract(run_id, source, force=force)
        stats = step_silver(run_id, full_refresh=full_refresh or force)
        step_quality(run_id, stats)
        step_dbt(run_id, full_refresh=full_refresh or force)
        step_publish(run_id)
        ops.finish_run(run_id, "success")
        log.info("run %s succeeded", run_id)
    except Exception as exc:
        ops.finish_run(run_id, "failed", repr(exc)[:1000])
        log.exception("run %s failed", run_id)
        raise
    finally:
        publish.publish_ops()      # observability data is published even when a run fails
    return run_id


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)-7s %(name)s | %(message)s")
    logging.getLogger("py4j").setLevel(logging.WARNING)
    p = argparse.ArgumentParser(prog="hospital-pipeline")
    sub = p.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("run", help="run the full pipeline")
    r.add_argument("--source", choices=["api", "sample"], default=os.getenv("PIPELINE_SOURCE", "sample"))
    r.add_argument("--force", action="store_true", help="re-ingest releases already loaded")
    r.add_argument("--full-refresh", action="store_true", help="rebuild silver and incremental models")

    e = sub.add_parser("extract", help="extract one or more datasets to bronze")
    e.add_argument("--source", choices=["api", "sample"], default=os.getenv("PIPELINE_SOURCE", "sample"))
    e.add_argument("--dataset", action="append")
    e.add_argument("--force", action="store_true")

    sub.add_parser("silver", help="run the Spark bronze->silver job")
    sub.add_parser("quality", help="run silver data quality checks")
    sub.add_parser("publish", help="export gold tables to parquet")

    a = p.parse_args(argv)
    get_settings().ensure_dirs()
    if a.cmd == "run":
        run_all(a.source, force=a.force, full_refresh=a.full_refresh)
        return 0

    run_id = ops.new_run_id()
    if a.cmd == "extract":
        print(step_extract(run_id, a.source, a.dataset, a.force))
    elif a.cmd == "silver":
        print(step_silver(run_id))
    elif a.cmd == "quality":
        step_quality(run_id)
    elif a.cmd == "publish":
        print(step_publish(run_id))
        publish.publish_ops()
    return 0


if __name__ == "__main__":
    sys.exit(main())
