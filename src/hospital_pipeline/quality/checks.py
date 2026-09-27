"""Data quality gate on the silver layer (runs before dbt).

A small declarative framework: each check is a SQL query that returns a single
observed value plus a predicate that decides pass/fail. Checks have a severity:

* ``error`` - failing stops the pipeline (the gold layer is not rebuilt, so the
  dashboard keeps serving the last good data)
* ``warn``  - recorded and surfaced on the dashboard, pipeline continues

dbt tests then guard the modelled (gold) layer - two lines of defence.
"""

from __future__ import annotations

import logging
from collections.abc import Callable
from dataclasses import dataclass
from datetime import date

import duckdb

from hospital_pipeline.config import get_settings

log = logging.getLogger(__name__)

EXPECTED_COLUMNS = {
    "hospitals": {"facility_id", "facility_name", "state", "hospital_type", "ownership",
                  "overall_rating", "has_emergency_services", "release_date"},
    "measure_scores": {"facility_id", "measure_id", "compared_to_national", "score",
                       "lower_estimate", "higher_estimate", "period_start", "period_end",
                       "source_dataset", "release_date"},
    "hcahps": {"facility_id", "measure_id", "star_rating", "answer_percent",
               "completed_surveys", "release_date"},
}
BUSINESS_KEYS = {
    "hospitals": ["facility_id", "release_date"],
    "measure_scores": ["facility_id", "measure_id", "release_date"],
    "hcahps": ["facility_id", "measure_id", "release_date"],
}


class DataQualityError(RuntimeError):
    pass


@dataclass
class Check:
    dataset: str
    name: str
    sql: str
    predicate: Callable[[object], bool]
    threshold: str
    severity: str = "error"
    message: str = ""


def _src(table: str) -> str:
    path = get_settings().silver_dir / table
    return f"read_parquet('{path}/**/*.parquet', hive_partitioning = true, union_by_name = true)"


def _latest(table: str) -> str:
    # max() over the cast value: DuckDB 1.5's statistics shortcut for max(<hive partition column>)
    # can hit an internal error with some Parquet writers; casting bypasses that code path.
    return (f"(select * from {_src(table)} where release_date::varchar = "
            f"(select max(release_date::varchar) from {_src(table)}))")


def build_checks() -> list[Check]:
    checks: list[Check] = []
    for table, keys in BUSINESS_KEYS.items():
        latest = _latest(table)
        checks += [
            Check(table, "row_count_positive", f"select count(*) from {latest}",
                  lambda v: v > 0, "> 0", message="latest release must contain rows"),
            Check(table, "business_key_unique",
                  f"select count(*) from (select {', '.join(keys)} from {_src(table)} "
                  f"group by all having count(*) > 1)",
                  lambda v: v == 0, "= 0", message=f"duplicate keys on ({', '.join(keys)})"),
            Check(table, "facility_id_not_null",
                  f"select count(*) from {_src(table)} where facility_id is null",
                  lambda v: v == 0, "= 0"),
            Check(table, "release_volume_change_pct",
                  f"""with c as (select release_date, count(*) n from {_src(table)} group by 1 order by 1 desc limit 2)
                      select coalesce(round(100.0 * abs(max_by(n, release_date) - min_by(n, release_date))
                             / nullif(min_by(n, release_date), 0), 2), 0) from c""",
                  lambda v: v <= 25, "<= 25%", message="row count swing between the last two releases"),
        ]
    checks += [
        Check("hospitals", "overall_rating_in_range",
              f"select count(*) from {_src('hospitals')} where overall_rating not between 1 and 5",
              lambda v: v == 0, "= 0"),
        Check("hospitals", "overall_rating_null_pct",
              f"select round(100.0 * count(*) filter (where overall_rating is null) / count(*), 2) from {_latest('hospitals')}",
              lambda v: v <= 60, "<= 60%", severity="warn",
              message="share of hospitals CMS did not rate"),
        Check("measure_scores", "ci_bounds_ordered",
              f"select count(*) from {_src('measure_scores')} where lower_estimate > higher_estimate",
              lambda v: v == 0, "= 0", message="lower_estimate must be <= higher_estimate"),
        Check("measure_scores", "unparseable_score_count",
              f"select count(*) from {_src('measure_scores')} where score is null and compared_to_national ilike '%national%'",
              lambda v: v <= 5, "<= 5", severity="warn",
              message="rated measures whose score could not be parsed"),
        Check("measure_scores", "orphan_facility_pct",
              f"""select round(100.0 * count(*) filter (where h.facility_id is null) / count(*), 3)
                  from {_latest('measure_scores')} m
                  left join {_src('hospitals')} h using (facility_id, release_date)""",
              lambda v: v <= 1, "<= 1%", severity="warn",
              message="measure rows whose hospital is missing from the same release"),
        Check("hcahps", "star_rating_in_range",
              f"select count(*) from {_src('hcahps')} where star_rating not between 1 and 5",
              lambda v: v == 0, "= 0"),
        Check("hcahps", "answer_percent_in_range",
              f"select count(*) from {_src('hcahps')} where answer_percent not between 0 and 100",
              lambda v: v == 0, "= 0"),
        Check("hospitals", "freshness_days",
              f"select date_diff('day', max(release_date::varchar)::date, DATE '{date.today().isoformat()}') from {_src('hospitals')}",
              lambda v: v <= 200, "<= 200 days", severity="warn",
              message="CMS refreshes quarterly; older data suggests a stalled feed"),
    ]
    return checks


def schema_checks(con: duckdb.DuckDBPyConnection) -> list[dict]:
    out = []
    for table, expected in EXPECTED_COLUMNS.items():
        cols = {r[0] for r in con.execute(f"describe select * from {_src(table)}").fetchall()}
        missing = sorted(expected - cols)
        out.append({"layer": "silver", "dataset": table, "check_name": "schema_contract",
                    "severity": "error", "passed": not missing, "observed": ",".join(missing) or "ok",
                    "threshold": "all expected columns present", "message": "missing columns" if missing else ""})
    return out


def quarantine_checks(silver_stats: list[dict]) -> list[dict]:
    out = []
    for st in silver_stats:
        if not st.get("rows_in"):
            continue
        pct = round(100.0 * st["quarantined"] / st["rows_in"], 3)
        out.append({"layer": "silver", "dataset": st["table"], "check_name": "quarantine_rate_pct",
                    "severity": "error" if pct > 5 else "warn", "passed": pct <= 1,
                    "observed": pct, "threshold": "<= 1%",
                    "message": f"{st['quarantined']} rows quarantined, {st['duplicates_removed']} duplicates removed"})
    return out


def run_checks(silver_stats: list[dict] | None = None) -> list[dict]:
    con = duckdb.connect()
    results = schema_checks(con)
    for c in build_checks():
        try:
            observed = con.execute(c.sql).fetchone()[0]
            observed = 0 if observed is None else observed
            passed = bool(c.predicate(observed))
        except duckdb.Error as exc:
            observed, passed = f"error: {exc}"[:200], False
        results.append({"layer": "silver", "dataset": c.dataset, "check_name": c.name,
                        "severity": c.severity, "passed": passed, "observed": observed,
                        "threshold": c.threshold, "message": c.message})
    results += quarantine_checks(silver_stats or [])
    for r in results:
        level = logging.INFO if r["passed"] else (logging.ERROR if r["severity"] == "error" else logging.WARNING)
        log.log(level, "DQ %-15s %-28s passed=%s observed=%s", r["dataset"], r["check_name"], r["passed"], r["observed"])
    return results


def enforce(results: list[dict]) -> None:
    failed = [r for r in results if not r["passed"] and r["severity"] == "error"]
    if failed:
        names = ", ".join(f"{r['dataset']}.{r['check_name']}" for r in failed)
        raise DataQualityError(f"{len(failed)} blocking data quality check(s) failed: {names}")
