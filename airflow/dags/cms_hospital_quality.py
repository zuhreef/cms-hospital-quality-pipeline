"""Airflow 3 DAG: CMS hospital quality lakehouse.

    start_run -> extract[dataset] (dynamic task mapping, one per CMS feed)
              -> spark_bronze_to_silver -> silver_quality_gate -> dbt_build
              -> publish_gold -> finish_run
    publish_ops always runs last (trigger_rule=all_done) so the dashboard's
    Pipeline Health page reflects failures too.

Every task calls the same step functions as the CLI (hospital_pipeline.pipeline),
so local runs, CI and Airflow share one implementation.
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta

from airflow.sdk import dag, get_current_context, task

SOURCE = os.getenv("PIPELINE_SOURCE", "api")
DATASETS = ["hospital_info", "complications_deaths", "unplanned_visits", "hcahps"]


@dag(
    dag_id="cms_hospital_quality",
    description="CMS Care Compare -> bronze -> Spark silver -> DQ gate -> dbt gold -> dashboard",
    schedule="0 6 * * 1",  # weekly; CMS refreshes quarterly and unchanged releases are skipped
    start_date=datetime(2026, 1, 1),
    catchup=False,
    max_active_runs=1,  # DuckDB is single-writer
    default_args={"retries": 2, "retry_delay": timedelta(minutes=5), "owner": "data-eng"},
    tags=["cms", "healthcare", "lakehouse"],
    doc_md=__doc__,
)
def cms_hospital_quality():
    @task
    def start_run() -> str:
        from hospital_pipeline import ops

        ctx = get_current_context()
        pipeline_run_id = ops.new_run_id()
        ops.start_run(pipeline_run_id, SOURCE, trigger=f"airflow:{ctx['dag_run'].run_id}")
        return pipeline_run_id

    @task(max_active_tis_per_dag=1)  # serialise writes to the DuckDB metadata store
    def extract(dataset: str, pipeline_run_id: str) -> dict:
        from hospital_pipeline.pipeline import step_extract

        return step_extract(pipeline_run_id, SOURCE, [dataset])

    @task
    def spark_bronze_to_silver(pipeline_run_id: str, _extracted: list) -> list[dict]:
        from hospital_pipeline.pipeline import step_silver

        return step_silver(pipeline_run_id)

    @task
    def silver_quality_gate(pipeline_run_id: str, silver_stats: list[dict]) -> int:
        from hospital_pipeline.pipeline import step_quality

        results = step_quality(pipeline_run_id, silver_stats)  # raises -> task fails on blocking checks
        return sum(not r["passed"] for r in results)

    @task
    def dbt_build(pipeline_run_id: str, _dq_failures: int) -> None:
        from hospital_pipeline.pipeline import step_dbt

        step_dbt(pipeline_run_id)

    @task
    def publish_gold(pipeline_run_id: str, _built: None) -> dict:
        from hospital_pipeline.pipeline import step_publish

        return step_publish(pipeline_run_id)

    @task
    def finish_run(pipeline_run_id: str, _published: dict) -> None:
        from hospital_pipeline import ops

        ops.finish_run(pipeline_run_id, "success")

    @task(trigger_rule="all_done")
    def publish_ops(pipeline_run_id: str) -> None:
        from hospital_pipeline import ops, publish

        with ops.connect() as con:  # mark the run failed if finish_run never happened
            con.execute("update ops.pipeline_runs set status = 'failed', finished_at = ? "
                        "where run_id = ? and status = 'running'", [ops.now(), pipeline_run_id])
        publish.publish_ops()

    pipeline_run_id = start_run()
    extracted = extract.partial(pipeline_run_id=pipeline_run_id).expand(dataset=DATASETS)
    silver = spark_bronze_to_silver(pipeline_run_id, extracted)
    dq = silver_quality_gate(pipeline_run_id, silver)
    built = dbt_build(pipeline_run_id, dq)
    published = publish_gold(pipeline_run_id, built)
    done = finish_run(pipeline_run_id, published)
    done >> publish_ops(pipeline_run_id)


cms_hospital_quality()
