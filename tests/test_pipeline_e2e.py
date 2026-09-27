"""End-to-end: sample source -> bronze -> Spark silver -> DQ -> dbt build -> gold Parquet."""

import shutil

import duckdb
import pytest

from hospital_pipeline import pipeline
from hospital_pipeline.ingest import sample_data

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(shutil.which("dbt") is None, reason="dbt not installed"),
]


@pytest.fixture()
def small_sample(monkeypatch):
    original = sample_data.generate_release.__wrapped__
    monkeypatch.setattr(sample_data, "generate_release",
                        lambda release, n_hospitals=250, seed=42: original(release, n_hospitals, seed))


def test_full_run_then_idempotent_rerun(data_dir, small_sample):
    run_id = pipeline.run_all("sample", trigger="pytest")

    gold = data_dir / "gold" / "marts"
    con = duckdb.connect()
    scorecard = con.execute(f"select count(*) from '{gold}/mart_hospital_scorecard.parquet'").fetchone()[0]
    hospitals = con.execute(f"select count(*) from '{gold}/dim_hospital.parquet'").fetchone()[0]
    assert scorecard == hospitals > 200
    history = con.execute(f"select count(*) filter (where is_current), count(*) "
                          f"from '{gold}/dim_hospital_history.parquet'").fetchone()
    assert history[1] > history[0]  # SCD2 captured changes across the two releases

    runs = con.execute(f"select run_id, status from '{data_dir}/gold/ops/pipeline_runs.parquet'").fetchall()
    assert (run_id, "success") in runs

    facts_before = con.execute(f"select count(*) from '{gold}/fct_measure_scores.parquet'").fetchone()[0]
    pipeline.run_all("sample", trigger="pytest")  # nothing new -> skips extract/silver, same result
    facts_after = con.execute(f"select count(*) from '{gold}/fct_measure_scores.parquet'").fetchone()[0]
    assert facts_after == facts_before
