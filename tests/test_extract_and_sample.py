import json

import pyarrow.parquet as pq

from hospital_pipeline import ops
from hospital_pipeline.ingest import sample_data
from hospital_pipeline.ingest.extract import extract_dataset


def test_sample_generator_is_deterministic_and_cms_shaped():
    a = sample_data.generate_release("2026-07-22", n_hospitals=50)
    b = sample_data.generate_release("2026-07-22", n_hospitals=50)
    assert a == b
    assert set(a) == {"hospital_info", "complications_deaths", "unplanned_visits", "hcahps"}
    row = a["hospital_info"][0]
    for col in ("facility_id", "hospital_overall_rating", "hospital_ownership", "emergency_services"):
        assert col in row
    assert all(isinstance(v, str) for v in row.values())  # like the API: everything is a string


def test_sample_generator_injects_defects():
    rows = sample_data.generate_release("2026-07-22", n_hospitals=50)["hospital_info"]
    ids = [r["facility_id"] for r in rows]
    assert len(ids) != len(set(ids)), "expected duplicate rows"
    assert any(r["state"] == "ZZ" for r in rows), "expected an invalid state"


def test_extract_writes_bronze_and_is_idempotent(data_dir):
    run_id = ops.new_run_id()
    first = extract_dataset("hospital_info", run_id, source="sample")
    assert [r.skipped for r in first] == [False, False]

    part = data_dir / "bronze" / "hospital_info" / "release_date=2026-07-22"
    table = pq.read_table(part / "part-0.parquet")
    manifest = json.loads((part / "_manifest.json").read_text())
    assert table.num_rows == manifest["rows"] > 0
    assert {"_run_id", "_ingested_at", "_release_date"} <= set(table.column_names)

    second = extract_dataset("hospital_info", ops.new_run_id(), source="sample")
    assert all(r.skipped for r in second), "already-ingested releases must be skipped"
    assert ops.ingested_releases("hospital_info") == set(sample_data.SAMPLE_RELEASES)
